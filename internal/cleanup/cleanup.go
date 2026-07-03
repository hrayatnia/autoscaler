// Package cleanup detects and removes ghost runners — runners we previously
// spawned whose containers are dead but whose registrations linger in
// GitHub. Ghosts form when a container is killed mid-job (Docker daemon
// crash, OOM, ungraceful host restart) before the runner can call
// `./config.sh remove`. GitHub keeps the runner registered, marked busy,
// holding the assigned job in_progress until the heartbeat timeout (~30
// minutes), wasting that capacity in the meantime.
//
// This package:
//   - Lists `auto-*` offline runners across all managed repos.
//   - Force-cancels any in_progress workflow runs assigned to dead runners.
//   - Attempts DELETE on the runner; GitHub returns 422 until heartbeat
//     timeout, so failures are expected and silently retried on the next
//     tick.
//
// Idempotent and safe to run on startup and periodically.
package cleanup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hrayatnia/autoscaler/internal/config"
	"github.com/hrayatnia/autoscaler/internal/spawner"
)

const (
	autoPrefix         = "auto-"
	managedLabel       = "gha-autoscaler=true"
	repoLabelKey       = "gha-autoscaler-repo"
	defaultLocalGrace  = 5 * time.Minute
	dockerCmdTimeout   = 60 * time.Second
)

type Cleaner struct {
	cfg     *config.Config
	spawner *spawner.Spawner
	log     *slog.Logger

	localGrace time.Duration

	totalGhostsDeleted    atomic.Uint64
	totalRunsCancelled    atomic.Uint64
	totalLocalGhostsReaped atomic.Uint64

	lastMu      sync.Mutex
	lastSummary Summary
}

func New(cfg *config.Config, sp *spawner.Spawner, log *slog.Logger) *Cleaner {
	return &Cleaner{cfg: cfg, spawner: sp, log: log, localGrace: defaultLocalGrace}
}

// SetLocalGrace overrides the minimum age a local container must reach before
// being reaped. Must be > 0; values smaller than 30s race fresh spawns and are
// rejected.
func (c *Cleaner) SetLocalGrace(d time.Duration) {
	if d < 30*time.Second {
		return
	}
	c.localGrace = d
}

// Run blocks until ctx is canceled, ticking cleanup every interval. The
// first tick fires immediately on startup so we begin draining ghosts that
// formed while the autoscaler was down.
func (c *Cleaner) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	c.log.Info("cleanup started", "interval", interval.String())
	c.tick(ctx)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.tick(ctx)
		}
	}
}

// Tick runs a single cleanup pass across all managed repos. Exposed for
// the on-demand /api/cleanup endpoint.
func (c *Cleaner) Tick(ctx context.Context) Summary {
	return c.tickWithSummary(ctx)
}

type Summary struct {
	Repos                  int    `json:"repos"`
	GhostsFound            int    `json:"ghosts_found"`
	RunsCancelled          int    `json:"runs_cancelled"`
	GhostsDeleted          int    `json:"ghosts_deleted"`
	GhostsStillStuck       int    `json:"ghosts_still_stuck"`
	LocalGhostsReaped      int    `json:"local_ghosts_reaped"`
	TotalGhostsDeleted     uint64 `json:"total_ghosts_deleted_lifetime"`
	TotalRunsCancelled     uint64 `json:"total_runs_cancelled_lifetime"`
	TotalLocalGhostsReaped uint64 `json:"total_local_ghosts_reaped_lifetime"`
}

func (c *Cleaner) tick(ctx context.Context) {
	c.tickWithSummary(ctx)
}

func (c *Cleaner) tickWithSummary(ctx context.Context) Summary {
	var (
		mu      sync.Mutex
		summary Summary
	)
	summary.Repos = len(c.cfg.Repos)

	var wg sync.WaitGroup
	for i := range c.cfg.Repos {
		wg.Add(1)
		go func(repo *config.RepoConfig) {
			defer wg.Done()
			r, err := c.cleanupRepo(ctx, repo)
			if err != nil {
				c.log.Warn("cleanup repo failed", "repo", repo.Name, "err", err)
				return
			}
			mu.Lock()
			summary.GhostsFound += r.found
			summary.RunsCancelled += r.cancelled
			summary.GhostsDeleted += r.deleted
			summary.GhostsStillStuck += r.stillStuck
			mu.Unlock()
		}(&c.cfg.Repos[i])
	}
	wg.Wait()

	// Reap orphan local containers (no GitHub registration) after the
	// per-repo passes — this runs once per tick across all repos.
	summary.LocalGhostsReaped = c.reapLocalGhosts(ctx)

	summary.TotalGhostsDeleted = c.totalGhostsDeleted.Load()
	summary.TotalRunsCancelled = c.totalRunsCancelled.Load()
	summary.TotalLocalGhostsReaped = c.totalLocalGhostsReaped.Load()
	if summary.GhostsFound > 0 || summary.GhostsDeleted > 0 || summary.LocalGhostsReaped > 0 {
		c.log.Info("cleanup tick complete",
			"repos", summary.Repos,
			"found", summary.GhostsFound,
			"cancelled", summary.RunsCancelled,
			"deleted", summary.GhostsDeleted,
			"stuck", summary.GhostsStillStuck,
			"local_reaped", summary.LocalGhostsReaped)
	}
	c.lastMu.Lock()
	c.lastSummary = summary
	c.lastMu.Unlock()
	return summary
}

// LastSummary returns the most recent tick's summary, or a zero value if no
// tick has run yet.
func (c *Cleaner) LastSummary() Summary {
	c.lastMu.Lock()
	defer c.lastMu.Unlock()
	s := c.lastSummary
	s.TotalGhostsDeleted = c.totalGhostsDeleted.Load()
	s.TotalRunsCancelled = c.totalRunsCancelled.Load()
	s.TotalLocalGhostsReaped = c.totalLocalGhostsReaped.Load()
	return s
}

type repoResult struct {
	found, cancelled, deleted, stillStuck int
}

func (c *Cleaner) cleanupRepo(ctx context.Context, repo *config.RepoConfig) (repoResult, error) {
	var r repoResult

	ghosts, err := c.listGhostRunners(ctx, repo)
	if err != nil {
		return r, fmt.Errorf("list ghosts: %w", err)
	}
	if len(ghosts) == 0 {
		return r, nil
	}
	r.found = len(ghosts)

	// Force-cancel in_progress runs that ghost runners are assigned to.
	// Cancellation alone won't free the runner (GitHub waits for the
	// runner to confirm), but it starts the timeout clock and prevents
	// new jobs from being added.
	stuckRunIDs, err := c.runIDsForGhosts(ctx, repo, ghosts)
	if err == nil {
		for _, runID := range stuckRunIDs {
			if c.forceCancelRun(ctx, repo, runID) {
				r.cancelled++
				c.totalRunsCancelled.Add(1)
			}
		}
	}

	// Try to delete each ghost. 422 ("currently running a job") is
	// expected until heartbeat timeout — silently track as stillStuck.
	for _, g := range ghosts {
		if c.deleteRunner(ctx, repo, g.id) {
			r.deleted++
			c.totalGhostsDeleted.Add(1)
		} else {
			r.stillStuck++
		}
	}
	return r, nil
}

type ghostRunner struct {
	id   int64
	name string
	busy bool
}

func (c *Cleaner) listGhostRunners(ctx context.Context, repo *config.RepoConfig) ([]ghostRunner, error) {
	url := fmt.Sprintf("%s/repos/%s/actions/runners?per_page=100", c.spawner.APIBase(), repo.Name)
	body, err := c.apiGET(ctx, url)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	var page struct {
		Runners []struct {
			ID     int64  `json:"id"`
			Name   string `json:"name"`
			Status string `json:"status"`
			Busy   bool   `json:"busy"`
		} `json:"runners"`
	}
	if err := json.NewDecoder(body).Decode(&page); err != nil {
		return nil, err
	}

	out := make([]ghostRunner, 0)
	for _, r := range page.Runners {
		if !strings.HasPrefix(r.Name, autoPrefix) {
			continue
		}
		if r.Status == "online" {
			continue // healthy live runner
		}
		out = append(out, ghostRunner{id: r.ID, name: r.Name, busy: r.Busy})
	}
	return out, nil
}

// runIDsForGhosts finds in_progress workflow runs whose runner_name matches
// any of our ghost runner names, returning unique run IDs.
func (c *Cleaner) runIDsForGhosts(ctx context.Context, repo *config.RepoConfig, ghosts []ghostRunner) ([]int64, error) {
	url := fmt.Sprintf("%s/repos/%s/actions/runs?status=in_progress&per_page=100", c.spawner.APIBase(), repo.Name)
	body, err := c.apiGET(ctx, url)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	var page struct {
		WorkflowRuns []struct {
			ID int64 `json:"id"`
		} `json:"workflow_runs"`
	}
	if err := json.NewDecoder(body).Decode(&page); err != nil {
		return nil, err
	}

	ghostNames := make(map[string]struct{}, len(ghosts))
	for _, g := range ghosts {
		ghostNames[g.name] = struct{}{}
	}

	stuckRuns := make(map[int64]struct{})
	for _, run := range page.WorkflowRuns {
		jobsURL := fmt.Sprintf("%s/repos/%s/actions/runs/%d/jobs?per_page=100", c.spawner.APIBase(), repo.Name, run.ID)
		jobsBody, err := c.apiGET(ctx, jobsURL)
		if err != nil {
			continue
		}
		var jobsPage struct {
			Jobs []struct {
				RunnerName string `json:"runner_name"`
				Status     string `json:"status"`
			} `json:"jobs"`
		}
		_ = json.NewDecoder(jobsBody).Decode(&jobsPage)
		_ = jobsBody.Close()
		for _, j := range jobsPage.Jobs {
			if j.Status != "in_progress" {
				continue
			}
			if _, isGhost := ghostNames[j.RunnerName]; isGhost {
				stuckRuns[run.ID] = struct{}{}
				break
			}
		}
	}

	out := make([]int64, 0, len(stuckRuns))
	for id := range stuckRuns {
		out = append(out, id)
	}
	return out, nil
}

func (c *Cleaner) forceCancelRun(ctx context.Context, repo *config.RepoConfig, runID int64) bool {
	url := fmt.Sprintf("%s/repos/%s/actions/runs/%d/force-cancel", c.spawner.APIBase(), repo.Name, runID)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	req.Header.Set("Authorization", "Bearer "+c.spawner.PAT())
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := c.spawner.HTTPClient().Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

func (c *Cleaner) deleteRunner(ctx context.Context, repo *config.RepoConfig, id int64) bool {
	url := fmt.Sprintf("%s/repos/%s/actions/runners/%d", c.spawner.APIBase(), repo.Name, id)
	req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	req.Header.Set("Authorization", "Bearer "+c.spawner.PAT())
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := c.spawner.HTTPClient().Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	// 204 = deleted; 422 = stuck busy (expected, retry next tick)
	return resp.StatusCode == http.StatusNoContent
}

// reapLocalGhosts removes containers we spawned (label
// `gha-autoscaler=true`) that no longer correspond to a registered runner
// on GitHub for their declared repo. These accumulate when:
//   - A runner finishes a job and exits, but Docker `--rm` hangs (Docker
//     Desktop on macOS, deep mount stacks → containers stuck in "removing").
//   - GitHub revokes/loses the registration but the container stays up
//     holding a Runner.Listener that will never receive work.
//   - The autoscaler restarted while a registration delete was racing the
//     container exit, leaving an orphan.
//
// Only containers older than `localGrace` (default 5m) are eligible — this
// avoids racing freshly spawned runners that haven't completed their initial
// registration handshake yet.
func (c *Cleaner) reapLocalGhosts(ctx context.Context) int {
	locals, err := listManagedContainers(ctx)
	if err != nil {
		c.log.Warn("local reap: docker ps failed", "err", err)
		return 0
	}
	if len(locals) == 0 {
		return 0
	}

	// Build per-repo set of currently-registered runner names. We trust this
	// snapshot — if a name is missing, GitHub does not know about the
	// container, so nothing will ever dispatch work to it.
	registered := make(map[string]map[string]struct{}, len(c.cfg.Repos))
	for i := range c.cfg.Repos {
		repo := &c.cfg.Repos[i]
		names, err := c.listAllRunnerNames(ctx, repo)
		if err != nil {
			c.log.Warn("local reap: list runners failed; skipping repo to avoid false positives",
				"repo", repo.Name, "err", err)
			continue
		}
		set := make(map[string]struct{}, len(names))
		for _, n := range names {
			set[n] = struct{}{}
		}
		registered[repo.Name] = set
	}

	grace := c.localGrace
	if grace <= 0 {
		grace = defaultLocalGrace
	}

	reaped := 0
	for _, lc := range locals {
		// Corpse path: any non-running container is unconditionally a ghost.
		// It can never accept jobs but its label still counts against the
		// per-repo cap (Docker `ps` reports cached "Up" on stuck-`--rm`
		// containers on macOS). No grace, no GitHub check needed.
		if lc.state != "" && lc.state != "running" {
			c.log.Info("reaping non-running ghost container",
				"name", lc.name, "repo", lc.repo, "state", lc.state)
			if err := dockerRun(ctx, "rm", "-f", lc.id); err != nil {
				c.log.Warn("local reap: docker rm -f failed", "name", lc.name, "err", err)
				continue
			}
			c.totalLocalGhostsReaped.Add(1)
			reaped++
			continue
		}

		// Running path: reap only if the container has no GitHub
		// registration AND it's been up long enough that we're confident
		// it's not a fresh spawn still completing its handshake.
		repoSet, ok := registered[lc.repo]
		if !ok {
			// We failed to list runners for this repo, or the container is
			// labeled with a repo not in our config. Skip — better to leak
			// than to nuke a container we can't reason about.
			continue
		}
		if _, isRegistered := repoSet[lc.name]; isRegistered {
			continue
		}
		age, err := containerAge(ctx, lc.id)
		if err != nil {
			c.log.Debug("local reap: age lookup failed", "name", lc.name, "err", err)
			continue
		}
		if age < grace {
			continue
		}
		c.log.Info("reaping unregistered running container",
			"name", lc.name, "repo", lc.repo, "age", age.Truncate(time.Second).String())
		if err := dockerRun(ctx, "rm", "-f", lc.id); err != nil {
			c.log.Warn("local reap: docker rm -f failed", "name", lc.name, "err", err)
			continue
		}
		c.totalLocalGhostsReaped.Add(1)
		reaped++
	}
	return reaped
}

type localContainer struct {
	id, name, repo, state string
}

func listManagedContainers(ctx context.Context) ([]localContainer, error) {
	out, err := dockerOutput(ctx, "ps", "-a",
		"--filter", "label="+managedLabel,
		"--format", "{{.ID}}|{{.Names}}|{{.Label \""+repoLabelKey+"\"}}",
	)
	if err != nil {
		return nil, err
	}
	var cs []localContainer
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 3)
		if len(parts) < 3 {
			continue
		}
		cs = append(cs, localContainer{id: parts[0], name: parts[1], repo: parts[2]})
	}
	if len(cs) == 0 {
		return cs, nil
	}
	// Resolve true State.Status via inspect — `docker ps` caches "Up …" on
	// containers that have actually exited but failed `--rm` (Docker Desktop
	// macOS bug). Without this we would treat corpses as live runners.
	ids := make([]string, len(cs))
	for i, c := range cs {
		ids[i] = c.id
	}
	iout, err := dockerOutput(ctx, append([]string{"inspect", "--format", "{{.State.Status}}"}, ids...)...)
	if err != nil {
		return cs, nil // soft fail — caller will skip on missing state
	}
	states := strings.Split(strings.TrimSpace(iout), "\n")
	if len(states) != len(cs) {
		return cs, nil
	}
	for i := range cs {
		cs[i].state = strings.TrimSpace(states[i])
	}
	return cs, nil
}

func containerAge(ctx context.Context, id string) (time.Duration, error) {
	out, err := dockerOutput(ctx, "inspect", "--format", "{{.Created}}", id)
	if err != nil {
		return 0, err
	}
	t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(out))
	if err != nil {
		// Fall back to second precision (older Docker versions).
		t, err = time.Parse(time.RFC3339, strings.TrimSpace(out))
		if err != nil {
			return 0, err
		}
	}
	return time.Since(t), nil
}

func dockerOutput(ctx context.Context, args ...string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, dockerCmdTimeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, "docker", args...)
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("docker %s: %w (stderr=%s)", strings.Join(args, " "), err, strings.TrimSpace(errBuf.String()))
	}
	return out.String(), nil
}

func dockerRun(ctx context.Context, args ...string) error {
	_, err := dockerOutput(ctx, args...)
	return err
}

// listAllRunnerNames returns every runner name currently registered with the
// repo, regardless of online/offline/busy status. Used to determine whether a
// local container has a corresponding GitHub registration.
func (c *Cleaner) listAllRunnerNames(ctx context.Context, repo *config.RepoConfig) ([]string, error) {
	url := fmt.Sprintf("%s/repos/%s/actions/runners?per_page=100", c.spawner.APIBase(), repo.Name)
	body, err := c.apiGET(ctx, url)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	var page struct {
		Runners []struct {
			Name string `json:"name"`
		} `json:"runners"`
	}
	if err := json.NewDecoder(body).Decode(&page); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(page.Runners))
	for _, r := range page.Runners {
		out = append(out, r.Name)
	}
	return out, nil
}

func (c *Cleaner) apiGET(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.spawner.PAT())
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := c.spawner.HTTPClient().Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("GET %s: %d %s", url, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return resp.Body, nil
}
