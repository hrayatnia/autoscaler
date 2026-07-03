package reconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hrayatnia/autoscaler/internal/config"
	"github.com/hrayatnia/autoscaler/internal/spawner"
)

// fakeDocker is a no-op spawner.DockerExecer that always reports zero
// running containers. The reconciler always spawns in dry-run mode in these
// tests, so `docker run` is never invoked; only `docker ps` (via
// countRunning) needs a response, and an empty one means "nothing running".
// Using this instead of the real docker binary keeps the test hermetic —
// spawner.New shells out to an actual Docker daemon, which may not be
// running on the machine executing the tests.
type fakeDocker struct{}

func (fakeDocker) Exec(_ context.Context, _ ...string) ([]byte, []byte, error) {
	return nil, nil, nil
}

// newTestSpawner builds a Spawner with a fake Docker executer (so tests
// never depend on a local Docker daemon) but a *real* HTTP client pointed at
// the given httptest.Server. The reconciler issues its own GitHub API calls
// through r.spawner.HTTPClient(), so the spawner's HTTP client and API base
// must point at the same fake server the reconciler is configured with.
func newTestSpawner(dryRun bool, srv *httptest.Server) *spawner.Spawner {
	return spawner.NewWithDeps("test-pat", "img", dryRun,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		fakeDocker{}, srv.Client(), srv.URL)
}

// fakeGitHub returns canned responses for /actions/runs and /actions/runs/{id}/jobs.
func fakeGitHub(t *testing.T, queuedRunIDs []int64, jobsPerRun map[int64][]apiJob) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasSuffix(path, "/actions/runs") {
			runs := make([]map[string]int64, 0, len(queuedRunIDs))
			for _, id := range queuedRunIDs {
				runs = append(runs, map[string]int64{"id": id})
			}
			out := map[string]any{"workflow_runs": runs}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(out)
			return
		}
		if strings.HasSuffix(path, "/jobs") {
			parts := strings.Split(path, "/")
			var runID int64
			for i := 0; i < len(parts)-1; i++ {
				if parts[i] == "runs" {
					_, _ = fmt.Sscan(parts[i+1], &runID)
					break
				}
			}
			out := map[string]any{"jobs": jobsPerRun[runID]}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(out)
			return
		}
		http.NotFound(w, r)
	})
	return httptest.NewServer(mux)
}

func TestReconcile_NoQueued_NoSpawn(t *testing.T) {
	srv := fakeGitHub(t, nil, nil)
	defer srv.Close()

	cfg := &config.Config{Repos: []config.RepoConfig{
		{Name: "acme/web", RepoURL: "x", MatchLabels: []string{"pool-a"}, RunnerLabels: "pool-a", MaxConcurrency: 4},
	}}
	sp := newTestSpawner(true, srv)
	r := New(cfg, sp, slog.New(slog.NewTextHandler(io.Discard, nil)), 30*time.Second)
	r.apiBaseFor = func() string { return srv.URL }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.reconcileRepo(ctx, &cfg.Repos[0]); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := sp.Stats().TotalSpawned; got != 0 {
		t.Errorf("spawned %d, want 0", got)
	}
}

func TestReconcile_MatchedJobs_SpawnsCapped(t *testing.T) {
	srv := fakeGitHub(t,
		[]int64{1, 2},
		map[int64][]apiJob{
			1: {{Status: "queued", Labels: []string{"self-hosted", "pool-a"}}, {Status: "queued", Labels: []string{"ubuntu-latest"}}},
			2: {{Status: "queued", Labels: []string{"self-hosted", "pool-a"}}, {Status: "queued", Labels: []string{"self-hosted", "pool-a"}}},
		},
	)
	defer srv.Close()

	cfg := &config.Config{Repos: []config.RepoConfig{
		{Name: "acme/web", RepoURL: "x", MatchLabels: []string{"pool-a"}, RunnerLabels: "pool-a", MaxConcurrency: 2},
	}}
	sp := newTestSpawner(true /* dryRun */, srv)
	r := New(cfg, sp, slog.New(slog.NewTextHandler(io.Discard, nil)), 30*time.Second)
	r.apiBaseFor = func() string { return srv.URL }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.reconcileRepo(ctx, &cfg.Repos[0]); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// 3 matching queued jobs, cap=2, dry-run spawns count toward stats.
	if got := sp.Stats().TotalSpawned; got != 2 {
		t.Errorf("spawned %d, want 2 (cap)", got)
	}
}
