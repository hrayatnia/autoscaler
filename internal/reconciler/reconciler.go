// Package reconciler periodically reconciles the autoscaler's spawn state
// against GitHub's actual queue. This closes the orphan-job gap left by the
// purely event-driven smee → spawn path: events can be lost (smee SSE
// disconnect, autoscaler restart, cap-hit drop, GitHub webhook delivery
// failure), and once a job is queued in GitHub it never re-fires
// `workflow_job.queued`. The reconciler polls each repo on a tick, finds
// queued workflow_runs whose jobs match our match_labels, and spawns
// enough ephemeral runners to cover them (subject to per-repo cap and the
// spawner's idle-skip).
package reconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hrayatnia/autoscaler/internal/config"
	"github.com/hrayatnia/autoscaler/internal/spawner"
)

type Reconciler struct {
	cfg      *config.Config
	spawner  *spawner.Spawner
	log      *slog.Logger
	interval time.Duration

	// apiBaseFor returns the API base URL (scheme + host) for GitHub calls.
	// Defaults to https://api.github.com. Tests override this to point at a
	// httptest.Server.
	apiBaseFor func() string
}

func New(cfg *config.Config, sp *spawner.Spawner, log *slog.Logger, interval time.Duration) *Reconciler {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &Reconciler{
		cfg:        cfg,
		spawner:    sp,
		log:        log,
		interval:   interval,
		apiBaseFor: defaultAPIBase,
	}
}

func defaultAPIBase() string { return "https://api.github.com" }

// Run blocks until ctx is canceled, ticking the reconcile loop every interval.
// The first tick fires immediately so orphans accumulated before startup are
// picked up promptly.
func (r *Reconciler) Run(ctx context.Context) {
	r.log.Info("reconciler started", "interval", r.interval.String())
	r.tick(ctx)
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.tick(ctx)
		}
	}
}

// tick reconciles every configured repo in parallel.
func (r *Reconciler) tick(ctx context.Context) {
	r.tickFiltered(ctx, "")
}

// tickFiltered reconciles repos whose Name == repoFilter, or all repos when
// repoFilter is empty.
func (r *Reconciler) tickFiltered(ctx context.Context, repoFilter string) {
	var wg sync.WaitGroup
	for i := range r.cfg.Repos {
		if repoFilter != "" && r.cfg.Repos[i].Name != repoFilter {
			continue
		}
		wg.Add(1)
		go func(rp *config.RepoConfig) {
			defer wg.Done()
			if err := r.reconcileRepo(ctx, rp); err != nil {
				r.log.Warn("reconcile failed", "repo", rp.Name, "err", err)
			}
		}(&r.cfg.Repos[i])
	}
	wg.Wait()
}

// TriggerOnce runs a single tick on demand (from the portal /api/reconcile).
// repoFilter is "" for all repos, or "owner/repo" for one.
func (r *Reconciler) TriggerOnce(ctx context.Context, repoFilter string) {
	r.log.Info("manual reconcile triggered", "repo", repoFilter)
	r.tickFiltered(ctx, repoFilter)
}

// reconcileRepo fetches the count of queued jobs whose labels match the
// repo's match_labels, then spawns up to the available capacity.
//
// We walk runs → jobs to filter by label precisely. To bound API usage we
// stop walking jobs once we've seen enough matches to fill capacity.
func (r *Reconciler) reconcileRepo(ctx context.Context, repo *config.RepoConfig) error {
	running := r.spawner.CountRunning(repo.Name)
	available := repo.MaxConcurrency - running
	if available <= 0 {
		return nil
	}

	matched, err := r.countQueuedMatchingJobs(ctx, repo, available)
	if err != nil {
		return err
	}
	if matched == 0 {
		return nil
	}

	toSpawn := matched
	if toSpawn > available {
		toSpawn = available
	}

	r.log.Info("reconcile spawning",
		"repo", repo.Name,
		"matched_queued", matched,
		"running", running,
		"available", available,
		"to_spawn", toSpawn)

	for i := 0; i < toSpawn; i++ {
		if err := r.spawner.Spawn(ctx, repo); err != nil {
			r.log.Debug("reconcile spawn returned",
				"repo", repo.Name, "iter", i, "err", err)
			return nil
		}
		r.spawner.IncReconcile()
	}
	return nil
}

// countQueuedMatchingJobs walks queued workflow_runs of a repo, then for each
// run lists jobs and counts those that are queued and target any of the
// repo's match_labels (case-insensitive). Stops counting once `cap` matches
// are seen — we never need more than the available capacity.
func (r *Reconciler) countQueuedMatchingJobs(ctx context.Context, repo *config.RepoConfig, cap int) (int, error) {
	want := make(map[string]struct{}, len(repo.MatchLabels))
	for _, l := range repo.MatchLabels {
		want[strings.ToLower(l)] = struct{}{}
	}

	runs, err := r.listQueuedRunIDs(ctx, repo)
	if err != nil {
		return 0, err
	}
	if len(runs) == 0 {
		return 0, nil
	}

	matched := 0
	for _, runID := range runs {
		jobs, err := r.listRunJobs(ctx, repo, runID)
		if err != nil {
			r.log.Debug("list jobs failed", "repo", repo.Name, "run_id", runID, "err", err)
			continue
		}
		for _, j := range jobs {
			if j.Status != "queued" {
				continue
			}
			for _, lbl := range j.Labels {
				if _, ok := want[strings.ToLower(lbl)]; ok {
					matched++
					break
				}
			}
			if matched >= cap {
				return matched, nil
			}
		}
	}
	return matched, nil
}

func (r *Reconciler) listQueuedRunIDs(ctx context.Context, repo *config.RepoConfig) ([]int64, error) {
	url := fmt.Sprintf("%s/repos/%s/actions/runs?status=queued&per_page=100", r.apiBaseFor(), repo.Name)
	body, err := r.apiGET(ctx, url)
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
		return nil, fmt.Errorf("decode runs: %w", err)
	}
	ids := make([]int64, 0, len(page.WorkflowRuns))
	for _, w := range page.WorkflowRuns {
		ids = append(ids, w.ID)
	}
	return ids, nil
}

type apiJob struct {
	Status string   `json:"status"`
	Labels []string `json:"labels"`
}

func (r *Reconciler) listRunJobs(ctx context.Context, repo *config.RepoConfig, runID int64) ([]apiJob, error) {
	url := fmt.Sprintf("%s/repos/%s/actions/runs/%d/jobs?per_page=100", r.apiBaseFor(), repo.Name, runID)
	body, err := r.apiGET(ctx, url)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	var page struct {
		Jobs []apiJob `json:"jobs"`
	}
	if err := json.NewDecoder(body).Decode(&page); err != nil {
		return nil, fmt.Errorf("decode jobs: %w", err)
	}
	return page.Jobs, nil
}

func (r *Reconciler) apiGET(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+r.spawner.PAT())
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := r.spawner.HTTPClient().Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return resp.Body, nil
}
