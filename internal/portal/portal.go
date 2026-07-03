// Package portal serves a small operator UI: live state, lifetime counters,
// per-repo queue/runner detail, and action endpoints (reconcile, cleanup,
// pause/resume, stop runner). Embedded into the binary so deployment is a
// single image.
package portal

import (
	"context"
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/hrayatnia/autoscaler/internal/cleanup"
	"github.com/hrayatnia/autoscaler/internal/config"
	"github.com/hrayatnia/autoscaler/internal/spawner"
)

//go:embed static/index.html
var indexHTML []byte

const (
	managedLabel = "gha-autoscaler=true"
	repoLabelKey = "gha-autoscaler-repo"
)

// ReconcileTrigger is implemented by reconciler.Reconciler. Decoupled here
// to avoid an import cycle.
type ReconcileTrigger interface {
	TriggerOnce(ctx context.Context, repoFilter string)
}

// Server wires the portal HTML, /api/state, and the action endpoints.
type Server struct {
	cfg       *config.Config
	spawner   *spawner.Spawner
	cleaner   *cleanup.Cleaner
	reconcile ReconcileTrigger
}

func New(cfg *config.Config, sp *spawner.Spawner, cl *cleanup.Cleaner, rec ReconcileTrigger) *Server {
	return &Server{cfg: cfg, spawner: sp, cleaner: cl, reconcile: rec}
}

func (s *Server) Mount(mux *http.ServeMux) {
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/state", s.auth(s.handleState))
	mux.HandleFunc("/api/reconcile", s.auth(s.handleReconcile))
	mux.HandleFunc("/api/cleanup", s.auth(s.handleCleanup))
	mux.HandleFunc("/api/pause", s.auth(s.handlePause))
	mux.HandleFunc("/api/resume", s.auth(s.handleResume))
	mux.HandleFunc("/api/stop", s.auth(s.handleStop))
}

// auth wraps a handler with an optional bearer-token check. If
// cfg.PortalToken is empty, the wrapper is a no-op (matches PoC behavior).
// When set, requests must carry `Authorization: Bearer <token>` (constant-
// time compared). Both the API token and a cookie named `portal_token` are
// accepted so the embedded HTML can authenticate XHR calls.
func (s *Server) auth(h http.HandlerFunc) http.HandlerFunc {
	if s.cfg.PortalToken == "" {
		return h
	}
	want := []byte(s.cfg.PortalToken)
	return func(w http.ResponseWriter, r *http.Request) {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got == "" {
			if ck, err := r.Cookie("portal_token"); err == nil {
				got = ck.Value
			}
		}
		if subtle.ConstantTimeCompare([]byte(got), want) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h(w, r)
	}
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(indexHTML)
}

type RepoState struct {
	Name              string `json:"name"`
	Cap               int    `json:"cap"`
	Queued            int    `json:"queued"`
	InProgress        int    `json:"in_progress"`
	Online            int    `json:"online"`
	Busy              int    `json:"busy"`
	Idle              int    `json:"idle"`
	Ghosts            int    `json:"ghosts"`
	RunningEphemerals int    `json:"running_ephemerals"`
}

type ContainerState struct {
	Name string `json:"name"`
	Repo string `json:"repo"`
	Busy bool   `json:"busy"`
}

type State struct {
	Stats      spawner.Stats     `json:"stats"`
	Cleanup    cleanup.Summary   `json:"cleanup"`
	Repos      []RepoState       `json:"repos"`
	Containers []ContainerState  `json:"containers"`
	Paused     bool              `json:"paused"`
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	state := State{
		Stats:   s.spawner.Stats(),
		Cleanup: s.cleaner.LastSummary(),
		Paused:  s.spawner.IsPaused(),
	}

	for i := range s.cfg.Repos {
		repo := &s.cfg.Repos[i]
		rs := s.collectRepoState(ctx, repo)
		state.Repos = append(state.Repos, rs)
	}

	state.Containers = listLiveEphemerals(ctx)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(state)
}

func (s *Server) collectRepoState(ctx context.Context, repo *config.RepoConfig) RepoState {
	rs := RepoState{Name: repo.Name, Cap: repo.MaxConcurrency}
	rs.RunningEphemerals = s.spawner.CountRunning(repo.Name)

	rs.Queued = s.queryRunCount(ctx, repo.Name, "queued")
	rs.InProgress = s.queryRunCount(ctx, repo.Name, "in_progress")

	online, busy, ghosts := s.spawner.RepoRunnerCounts(ctx, repo)
	rs.Online = online
	rs.Busy = busy
	rs.Idle = online - busy
	rs.Ghosts = ghosts
	return rs
}

func (s *Server) queryRunCount(ctx context.Context, repoName, status string) int {
	url := fmt.Sprintf("%s/repos/%s/actions/runs?status=%s&per_page=1", s.spawner.APIBase(), repoName, status)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer "+s.spawner.PAT())
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := s.spawner.HTTPClient().Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return 0
	}
	var page struct {
		TotalCount int `json:"total_count"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&page)
	return page.TotalCount
}

func (s *Server) handleReconcile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	repoFilter := r.URL.Query().Get("repo")
	// Manual reconcile is fire-and-forget but bounded — don't leak past 5m.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		s.reconcile.TriggerOnce(ctx, repoFilter)
	}()
	writeJSON(w, map[string]string{"status": "triggered"})
}

func (s *Server) handleCleanup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	summary := s.cleaner.Tick(ctx)
	writeJSON(w, summary)
}

func (s *Server) handlePause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	s.spawner.SetPaused(true)
	writeJSON(w, map[string]bool{"paused": true})
}

func (s *Server) handleResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	s.spawner.SetPaused(false)
	writeJSON(w, map[string]bool{"paused": false})
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}
	if !strings.HasPrefix(name, "auto-") {
		http.Error(w, "only auto-* containers can be stopped via portal", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "stop", name)
	if out, err := cmd.CombinedOutput(); err != nil {
		http.Error(w, fmt.Sprintf("docker stop: %v: %s", err, strings.TrimSpace(string(out))), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"stopped": name})
}

// listLiveEphemerals enumerates managed ephemeral containers via `docker ps`.
func listLiveEphemerals(parent context.Context) []ContainerState {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "ps",
		"--filter", "label="+managedLabel,
		"--format", "{{.Names}}\t{{.Label \""+repoLabelKey+"\"}}")
	out, err := cmd.Output()
	if err != nil {
		return nil
	}
	var states []ContainerState
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		s := ContainerState{Name: parts[0]}
		if len(parts) == 2 {
			s.Repo = parts[1]
		}
		states = append(states, s)
	}
	return states
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
