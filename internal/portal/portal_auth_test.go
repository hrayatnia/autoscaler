package portal

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hrayatnia/autoscaler/internal/cleanup"
	"github.com/hrayatnia/autoscaler/internal/config"
	"github.com/hrayatnia/autoscaler/internal/spawner"
)

// recStub satisfies ReconcileTrigger.
type recStub struct{}

func (recStub) TriggerOnce(_ context.Context, _ string) {}

func newMux(t *testing.T, token string) http.Handler {
	t.Helper()
	cfg := &config.Config{
		Listen:      ":0",
		PortalToken: token,
		Repos: []config.RepoConfig{{
			Name: "a/b", RepoURL: "u", MatchLabels: []string{"L"}, RunnerLabels: "L", MaxConcurrency: 1,
		}},
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	sp := spawner.New("pat", "img", true, log)
	cl := cleanup.New(cfg, sp, log)
	srv := New(cfg, sp, cl, recStub{})
	mux := http.NewServeMux()
	srv.Mount(mux)
	return mux
}

func TestAuth_NoTokenAllowsAll(t *testing.T) {
	mux := newMux(t, "")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("POST", "/api/pause", nil))
	if w.Code != 200 {
		t.Errorf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
}

func TestAuth_TokenRequiredRejectsMissing(t *testing.T) {
	mux := newMux(t, "secret-token")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("POST", "/api/pause", nil))
	if w.Code != 401 {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestAuth_TokenAcceptsBearer(t *testing.T) {
	mux := newMux(t, "secret-token")
	req := httptest.NewRequest("POST", "/api/pause", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
}

func TestAuth_TokenAcceptsCookie(t *testing.T) {
	mux := newMux(t, "secret-token")
	req := httptest.NewRequest("POST", "/api/resume", nil)
	req.AddCookie(&http.Cookie{Name: "portal_token", Value: "secret-token"})
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
}

func TestAuth_TokenRejectsWrong(t *testing.T) {
	mux := newMux(t, "secret-token")
	req := httptest.NewRequest("POST", "/api/pause", nil)
	req.Header.Set("Authorization", "Bearer nope")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Errorf("status = %d, want 401", w.Code)
	}
	if !strings.Contains(w.Body.String(), "unauthorized") {
		t.Errorf("body = %q", w.Body.String())
	}
}
