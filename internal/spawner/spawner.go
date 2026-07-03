// Package spawner launches and tracks ephemeral self-hosted GitHub Actions
// runner containers. One Spawner instance is shared by the webhook handler
// and the reconciler.
//
// Hardening notes (v2):
//
//   - The GitHub PAT is NEVER passed on the docker-run argv. Instead, it is
//     written to a per-spawn 0600 env-file in os.TempDir() and referenced via
//     `docker run --env-file`. The file is unlinked immediately after
//     `docker run` returns; the kernel keeps the open fd alive long enough
//     for the runner container to read it.
//
//   - Per-repo concurrency caps are enforced under a per-repo sync.Mutex,
//     closing the TOCTOU window where two concurrent Spawn calls would both
//     observe `running < cap` and both spawn.
//
//   - Docker and GitHub interactions are routed through injectable interfaces
//     (DockerExecer, HTTPDoer) so unit tests can exercise the spawn paths
//     without a running Docker daemon.
package spawner

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hrayatnia/autoscaler/internal/config"
)

// Errors returned by Spawn.
var (
	ErrCapHit = errors.New("concurrency cap reached")
	ErrPaused = errors.New("spawner paused")
)

const (
	managedLabel   = "gha-autoscaler=true"
	repoLabelKey   = "gha-autoscaler-repo"
	dockerRunTO    = 30 * time.Second
	dockerPsTO     = 10 * time.Second
	githubAPITO    = 5 * time.Second
	defaultAPIBase = "https://api.github.com"
)

// DockerExecer runs a docker subcommand and returns (stdout, stderr).
// The default implementation shells out to `docker`; tests substitute a fake.
type DockerExecer interface {
	Exec(ctx context.Context, args ...string) (stdout, stderr []byte, err error)
}

// HTTPDoer is satisfied by *http.Client. Decoupled for tests.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// Spawner is safe for concurrent use.
type Spawner struct {
	pat         string
	runnerImage string
	dryRun      bool
	log         *slog.Logger

	docker  DockerExecer
	http    HTTPDoer
	apiBase string

	// repoMu serializes the count+spawn critical section per repo, so two
	// concurrent webhook events for the same repo can't both pass the cap.
	repoMu sync.Map // map[string]*sync.Mutex

	paused atomic.Bool

	totalSpawned   atomic.Uint64
	totalCapHit    atomic.Uint64
	totalErrors    atomic.Uint64
	totalIdleSkip  atomic.Uint64
	totalReconcile atomic.Uint64
	totalPaused    atomic.Uint64
}

// Stats is a JSON-serializable snapshot of lifetime spawn metrics.
type Stats struct {
	TotalSpawned   uint64         `json:"total_spawned"`
	TotalCapHit    uint64         `json:"total_cap_hit"`
	TotalErrors    uint64         `json:"total_errors"`
	TotalIdleSkip  uint64         `json:"total_idle_skip"`
	TotalReconcile uint64         `json:"total_reconcile"`
	TotalPaused    uint64         `json:"total_paused"`
	Paused         bool           `json:"paused"`
	RunningByRepo  map[string]int `json:"running_by_repo"`
}

// New builds a Spawner with real docker + http.Client.
func New(pat, runnerImage string, dryRun bool, log *slog.Logger) *Spawner {
	return NewWithDeps(pat, runnerImage, dryRun, log, realDocker{}, &http.Client{Timeout: githubAPITO}, defaultAPIBase)
}

// NewWithDeps is the testable constructor.
func NewWithDeps(pat, runnerImage string, dryRun bool, log *slog.Logger, d DockerExecer, h HTTPDoer, apiBase string) *Spawner {
	if apiBase == "" {
		apiBase = defaultAPIBase
	}
	return &Spawner{
		pat:         pat,
		runnerImage: runnerImage,
		dryRun:      dryRun,
		log:         log,
		docker:      d,
		http:        h,
		apiBase:     apiBase,
	}
}

func (s *Spawner) repoLock(name string) *sync.Mutex {
	m, _ := s.repoMu.LoadOrStore(name, &sync.Mutex{})
	return m.(*sync.Mutex)
}

// Spawn starts one ephemeral runner for repo, subject to per-repo cap and
// pause. Returns nil on success or idle-skip, ErrCapHit if at cap, ErrPaused
// if paused, or a wrapped error on docker/github failure.
//
// The full check-and-spawn sequence runs under the per-repo mutex so a burst
// of webhook events cannot exceed the cap.
func (s *Spawner) Spawn(ctx context.Context, repo *config.RepoConfig) error {
	if s.paused.Load() {
		s.totalPaused.Add(1)
		return ErrPaused
	}

	mu := s.repoLock(repo.Name)
	mu.Lock()
	defer mu.Unlock()

	if idle, err := s.hasIdleMatchingRunner(ctx, repo); err == nil && idle {
		s.totalIdleSkip.Add(1)
		s.log.Info("skip spawn: idle matching runner exists", "repo", repo.Name)
		return nil
	} else if err != nil {
		if isAuthOrPermissionError(err) {
			s.log.Error("github auth/permission error on idle-check — check PAT scopes", "repo", repo.Name, "err", err)
		} else {
			s.log.Warn("idle check failed; spawning anyway", "repo", repo.Name, "err", err)
		}
	}

	running, err := s.countRunning(ctx, repo.Name)
	if err != nil {
		s.totalErrors.Add(1)
		return fmt.Errorf("count running: %w", err)
	}
	if running >= repo.MaxConcurrency {
		s.totalCapHit.Add(1)
		s.log.Info("concurrency cap hit", "repo", repo.Name, "running", running, "cap", repo.MaxConcurrency)
		return ErrCapHit
	}

	tag := repo.Label
	if tag == "" && len(repo.MatchLabels) > 0 {
		tag = repo.MatchLabels[0]
	}
	name := fmt.Sprintf("auto-%s-%s", sanitize(tag), randHex(4))

	if s.dryRun {
		s.log.Info("dry-run: would spawn", "repo", repo.Name, "name", name, "labels", repo.RunnerLabels)
		s.totalSpawned.Add(1)
		return nil
	}

	envFile, cleanup, err := writePATEnvFile(s.pat)
	if err != nil {
		s.totalErrors.Add(1)
		return fmt.Errorf("write env-file: %w", err)
	}
	defer cleanup()

	args := []string{
		"run", "--rm", "-d",
		"--name", name,
		"--label", managedLabel,
		"--label", fmt.Sprintf("%s=%s", repoLabelKey, repo.Name),
		"--restart", "no",
		"--env-file", envFile,
		"-e", "EPHEMERAL=true",
		"-e", "RUN_AS_ROOT=true",
		"-e", "DISABLE_AUTO_UPDATE=true",
		"-e", "RUNNER_SCOPE=repo",
		"-e", fmt.Sprintf("REPO_URL=%s", repo.RepoURL),
		"-e", fmt.Sprintf("RUNNER_NAME=%s", name),
		"-e", fmt.Sprintf("LABELS=%s", repo.RunnerLabels),
		"-v", "/var/run/docker.sock:/var/run/docker.sock",
		s.runnerImage,
	}

	runCtx, cancel := context.WithTimeout(ctx, dockerRunTO)
	defer cancel()
	stdout, stderr, err := s.docker.Exec(runCtx, args...)
	if err != nil {
		s.totalErrors.Add(1)
		return fmt.Errorf("docker run: %w (stderr=%s)", err, strings.TrimSpace(string(stderr)))
	}

	containerID := strings.TrimSpace(string(stdout))
	s.totalSpawned.Add(1)
	short := containerID
	if len(short) > 12 {
		short = short[:12]
	}
	s.log.Info("runner spawned", "repo", repo.Name, "name", name, "container_id", short)
	return nil
}

// countRunning counts our managed containers for repo that are *actually*
// running per `docker inspect .State.Status`. Docker Desktop on macOS keeps
// dead-but-not-removed containers visible in `docker ps` with cached "Up …"
// status (deep mount stacks make `--rm` hang post-exit); trusting `ps` causes
// us to count corpses against the cap.
func (s *Spawner) countRunning(ctx context.Context, repo string) (int, error) {
	psCtx, cancel := context.WithTimeout(ctx, dockerPsTO)
	defer cancel()
	stdout, _, err := s.docker.Exec(psCtx,
		"ps", "-a",
		"--filter", "label="+managedLabel,
		"--filter", fmt.Sprintf("label=%s=%s", repoLabelKey, repo),
		"--format", "{{.ID}}",
	)
	if err != nil {
		return 0, err
	}
	ids := strings.Fields(strings.TrimSpace(string(stdout)))
	if len(ids) == 0 {
		return 0, nil
	}
	inspectArgs := append([]string{"inspect", "--format", "{{.State.Status}}"}, ids...)
	istdout, _, err := s.docker.Exec(psCtx, inspectArgs...)
	if err != nil {
		return 0, err
	}
	running := 0
	for _, line := range strings.Split(strings.TrimSpace(string(istdout)), "\n") {
		if strings.TrimSpace(line) == "running" {
			running++
		}
	}
	return running, nil
}

func (s *Spawner) Stats() Stats {
	return Stats{
		TotalSpawned:   s.totalSpawned.Load(),
		TotalCapHit:    s.totalCapHit.Load(),
		TotalErrors:    s.totalErrors.Load(),
		TotalIdleSkip:  s.totalIdleSkip.Load(),
		TotalReconcile: s.totalReconcile.Load(),
		TotalPaused:    s.totalPaused.Load(),
		Paused:         s.paused.Load(),
	}
}

func (s *Spawner) IncReconcile()                { s.totalReconcile.Add(1) }
func (s *Spawner) PAT() string                  { return s.pat }
func (s *Spawner) HTTPClient() HTTPDoer         { return s.http }
func (s *Spawner) Docker() DockerExecer         { return s.docker }
func (s *Spawner) APIBase() string              { return s.apiBase }
func (s *Spawner) SetPaused(p bool)             { s.paused.Store(p) }
func (s *Spawner) IsPaused() bool               { return s.paused.Load() }

// RepoRunnerCounts returns (online, busy, ghost) for runners whose names
// start with our `auto-` prefix.
func (s *Spawner) RepoRunnerCounts(ctx context.Context, repo *config.RepoConfig) (online, busy, ghost int) {
	url := fmt.Sprintf("%s/repos/%s/actions/runners?per_page=100", s.apiBase, repo.Name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		s.log.Warn("list runners failed", "repo", repo.Name, "err", err)
		return
	}
	s.signRequest(req)
	resp, err := s.http.Do(req)
	if err != nil {
		s.log.Warn("list runners failed", "repo", repo.Name, "err", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		s.log.Warn("list runners failed", "repo", repo.Name, "status", resp.StatusCode)
		return
	}
	var page struct {
		Runners []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
			Busy   bool   `json:"busy"`
		} `json:"runners"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		s.log.Warn("list runners failed", "repo", repo.Name, "err", err)
		return
	}
	for _, r := range page.Runners {
		if !strings.HasPrefix(r.Name, "auto-") {
			continue
		}
		if r.Status == "online" {
			online++
			if r.Busy {
				busy++
			}
		} else {
			ghost++
		}
	}
	return
}

// hasIdleMatchingRunner is ctx-aware and used under the per-repo mutex.
func (s *Spawner) hasIdleMatchingRunner(ctx context.Context, repo *config.RepoConfig) (bool, error) {
	rctx, cancel := context.WithTimeout(ctx, githubAPITO)
	defer cancel()
	url := fmt.Sprintf("%s/repos/%s/actions/runners?per_page=100", s.apiBase, repo.Name)
	req, err := http.NewRequestWithContext(rctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	s.signRequest(req)
	resp, err := s.http.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("list runners: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var page struct {
		Runners []struct {
			Status string `json:"status"`
			Busy   bool   `json:"busy"`
			Labels []struct {
				Name string `json:"name"`
			} `json:"labels"`
		} `json:"runners"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return false, err
	}
	want := make(map[string]struct{}, len(repo.MatchLabels))
	for _, l := range repo.MatchLabels {
		want[strings.ToLower(l)] = struct{}{}
	}
	for _, r := range page.Runners {
		if r.Busy || r.Status != "online" {
			continue
		}
		for _, l := range r.Labels {
			if _, ok := want[strings.ToLower(l.Name)]; ok {
				return true, nil
			}
		}
	}
	return false, nil
}

// isAuthOrPermissionError reports whether err wraps a GitHub 401/403
// response (as produced by hasIdleMatchingRunner's "status %d" wrapping).
// A 401/403 usually means the PAT is invalid or missing scopes, which is
// worth surfacing distinctly from transient (rate-limit/5xx) failures so
// on-call can grep it apart from routine noise.
func isAuthOrPermissionError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "status 401") || strings.Contains(msg, "status 403")
}

func (s *Spawner) signRequest(req *http.Request) {
	// PAT is added via header only. We do not log Authorization values.
	req.Header.Set("Authorization", "Bearer "+s.pat)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
}

// CountRunning is a convenience for metrics; suppresses ctx threading.
func (s *Spawner) CountRunning(repo string) int {
	ctx, cancel := context.WithTimeout(context.Background(), dockerPsTO)
	defer cancel()
	n, _ := s.countRunning(ctx, repo)
	return n
}

// writePATEnvFile writes the GitHub PAT to a 0600 temp file in the form
// `ACCESS_TOKEN=<pat>` and returns the path plus a cleanup func. The file is
// unlinked after `docker run` returns; the runner container reads its env
// before docker daemon closes the underlying fd.
func writePATEnvFile(pat string) (string, func(), error) {
	dir := os.TempDir()
	f, err := os.CreateTemp(dir, "autoscaler-env-*.env")
	if err != nil {
		return "", nil, err
	}
	path := f.Name()
	cleanup := func() {
		_ = os.Remove(path)
	}
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		cleanup()
		return "", nil, err
	}
	if _, err := f.WriteString("ACCESS_TOKEN=" + pat + "\n"); err != nil {
		f.Close()
		cleanup()
		return "", nil, err
	}
	if err := f.Close(); err != nil {
		cleanup()
		return "", nil, err
	}
	return filepath.Clean(path), cleanup, nil
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func sanitize(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '-' || c == '_':
			out = append(out, c)
		}
	}
	return string(out)
}

// realDocker shells out to the `docker` binary on PATH.
type realDocker struct{}

func (realDocker) Exec(ctx context.Context, args ...string) ([]byte, []byte, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	err := cmd.Run()
	return out.Bytes(), errBuf.Bytes(), err
}
