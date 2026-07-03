package cleanup

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hrayatnia/autoscaler/internal/config"
	"github.com/hrayatnia/autoscaler/internal/spawner"
)

// TestListGhostRunners_Parsing exercises the JSON parsing + auto-prefix +
// online-filter logic against an in-process httptest GitHub.
func TestListGhostRunners_Parsing(t *testing.T) {
	body := map[string]any{
		"runners": []map[string]any{
			{"id": 1, "name": "auto-pool-a-aaaa", "status": "offline", "busy": true},
			{"id": 2, "name": "auto-pool-a-bbbb", "status": "online", "busy": false},  // healthy — skip
			{"id": 3, "name": "manual-runner", "status": "offline", "busy": false},    // non-prefix — skip
			{"id": 4, "name": "auto-pool-b-cccc", "status": "offline", "busy": false}, // ghost
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/actions/runners") {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer srv.Close()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &config.Config{Repos: []config.RepoConfig{
		{Name: "a/b", RepoURL: "u", MatchLabels: []string{"L"}, RunnerLabels: "L", MaxConcurrency: 1},
	}}
	sp := spawner.NewWithDeps("pat", "img", true, log, nil, srv.Client(), srv.URL)
	c := &Cleaner{cfg: cfg, spawner: sp, log: log}

	ghosts, err := c.listGhostRunners(context.Background(), &cfg.Repos[0])
	if err != nil {
		t.Fatalf("listGhostRunners: %v", err)
	}
	if len(ghosts) != 2 {
		t.Errorf("got %d ghosts, want 2 (ids 1 and 4)", len(ghosts))
	}
	names := map[string]bool{}
	for _, g := range ghosts {
		names[g.name] = true
	}
	if !names["auto-pool-a-aaaa"] || !names["auto-pool-b-cccc"] {
		t.Errorf("unexpected ghost set: %v", names)
	}
}

func TestDeleteRunner_204vs422(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch calls {
		case 1:
			w.WriteHeader(http.StatusNoContent)
		case 2:
			w.WriteHeader(http.StatusUnprocessableEntity)
		}
	}))
	defer srv.Close()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &config.Config{Repos: []config.RepoConfig{
		{Name: "a/b", RepoURL: "u", MatchLabels: []string{"L"}, RunnerLabels: "L", MaxConcurrency: 1},
	}}
	sp := spawner.NewWithDeps("pat", "img", true, log, nil, srv.Client(), srv.URL)
	c := &Cleaner{cfg: cfg, spawner: sp, log: log}

	if !c.deleteRunner(context.Background(), &cfg.Repos[0], 1) {
		t.Error("204 should report success")
	}
	if c.deleteRunner(context.Background(), &cfg.Repos[0], 2) {
		t.Error("422 should report failure (stuck busy)")
	}
}
