package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/hrayatnia/autoscaler/internal/config"
	"github.com/hrayatnia/autoscaler/internal/spawner"
)

var (
	ErrBadSignature = errors.New("invalid signature")
	ErrIgnored      = errors.New("event ignored")
)

type WorkflowJob struct {
	Status      string   `json:"status"`
	Conclusion  string   `json:"conclusion"`
	Labels      []string `json:"labels"`
	RunnerName  string   `json:"runner_name"`
	WorkflowURL string   `json:"workflow_url"`
}

type Repo struct {
	FullName string `json:"full_name"`
}

type WorkflowJobEvent struct {
	Action      string      `json:"action"`
	WorkflowJob WorkflowJob `json:"workflow_job"`
	Repository  Repo        `json:"repository"`
}

type Handler struct {
	cfg     *config.Config
	spawner *spawner.Spawner
	log     *slog.Logger
}

func NewHandler(cfg *config.Config, sp *spawner.Spawner, log *slog.Logger) *Handler {
	return &Handler{cfg: cfg, spawner: sp, log: log}
}

// Dispatch validates signature, parses event, and triggers a spawn if applicable.
// Returns nil on success or "ignored" cases. Errors mean the caller should retry.
func (h *Handler) Dispatch(ctx context.Context, headers http.Header, body []byte) error {
	if h.cfg.VerifyHMAC {
		sig := headers.Get("X-Hub-Signature-256")
		if !VerifySignature(body, sig, h.cfg.WebhookSecret) {
			return ErrBadSignature
		}
	}
	event := headers.Get("X-Github-Event")
	if event != "workflow_job" {
		return ErrIgnored
	}

	var evt WorkflowJobEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return fmt.Errorf("parse event: %w", err)
	}

	if evt.Action != "queued" {
		// Only act on queued. completed/in_progress are informational.
		h.log.Debug("ignoring non-queued action", "action", evt.Action, "repo", evt.Repository.FullName)
		return ErrIgnored
	}

	repoCfg, ok := h.cfg.RepoByName(evt.Repository.FullName)
	if !ok {
		h.log.Debug("unknown repo, ignoring", "repo", evt.Repository.FullName)
		return ErrIgnored
	}

	if !labelMatches(evt.WorkflowJob.Labels, repoCfg.MatchLabels) {
		h.log.Debug("labels do not match",
			"repo", evt.Repository.FullName,
			"job_labels", evt.WorkflowJob.Labels,
			"want_any_of", repoCfg.MatchLabels)
		return ErrIgnored
	}

	h.log.Info("dispatching spawn",
		"repo", evt.Repository.FullName,
		"job_labels", evt.WorkflowJob.Labels,
		"workflow_url", evt.WorkflowJob.WorkflowURL)

	return h.spawner.Spawn(ctx, repoCfg)
}

// labelMatches returns true if any of the requested job labels equals any of
// the configured match labels. `self-hosted` is always included by GitHub on
// self-hosted jobs and is not a useful match.
func labelMatches(jobLabels []string, want []string) bool {
	wantSet := make(map[string]struct{}, len(want))
	for _, w := range want {
		wantSet[strings.ToLower(w)] = struct{}{}
	}
	for _, l := range jobLabels {
		if _, ok := wantSet[strings.ToLower(l)]; ok {
			return true
		}
	}
	return false
}
