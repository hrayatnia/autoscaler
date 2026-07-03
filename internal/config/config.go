// Package config loads and validates the autoscaler configuration from
// a JSON file (with env-var fallbacks for the three secret fields:
// SMEE_URL, WEBHOOK_SECRET, GH_PAT). Load returns a *Config or an error;
// callers should treat any error as fatal.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
)

// RepoConfig describes one GitHub repository the autoscaler watches.
type RepoConfig struct {
	Name    string `json:"name"`
	RepoURL string `json:"repo_url"`
	// MatchLabels: workflow_job is dispatched to spawn if ANY of its labels
	// matches ANY label in this list (case-insensitive). The single-string
	// `label` field is also accepted for backward-compat.
	MatchLabels    []string `json:"match_labels"`
	Label          string   `json:"label,omitempty"`
	RunnerLabels   string   `json:"runner_labels"`
	MaxConcurrency int      `json:"max_concurrency"`
}

// Config is the top-level autoscaler configuration.
type Config struct {
	SmeeURL       string `json:"smee_url"`
	WebhookSecret string `json:"webhook_secret"`
	// VerifyHMAC controls X-Hub-Signature-256 verification. With smee.io as
	// ingress, the smee URL itself is the auth secret (smee re-serializes the
	// body which loses byte-exact HMAC fidelity). Default off in smee mode.
	VerifyHMAC  bool         `json:"verify_hmac"`
	GitHubPAT   string       `json:"github_pat"`
	RunnerImage string       `json:"runner_image"`
	Listen      string       `json:"listen"`
	Repos       []RepoConfig `json:"repos"`

	// PortalToken, if non-empty, requires every /api/* request to carry
	// `Authorization: Bearer <token>` (or a portal_token cookie). The
	// embedded portal HTML uses the cookie; CLI/scripts use the header.
	// Defense-in-depth on top of binding the listen socket to 127.0.0.1.
	PortalToken string `json:"portal_token,omitempty"`
}

// Load reads, env-overrides, and validates the config at path.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	warnIfWorldReadable(path)

	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if c.SmeeURL == "" {
		if v := os.Getenv("SMEE_URL"); v != "" {
			c.SmeeURL = v
		}
	}
	if c.WebhookSecret == "" {
		if v := os.Getenv("WEBHOOK_SECRET"); v != "" {
			c.WebhookSecret = v
		}
	}
	if c.GitHubPAT == "" {
		if v := os.Getenv("GH_PAT"); v != "" {
			c.GitHubPAT = v
		}
	}
	if c.PortalToken == "" {
		if v := os.Getenv("PORTAL_TOKEN"); v != "" {
			c.PortalToken = v
		}
	}
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.RunnerImage == "" {
		c.RunnerImage = "myoung34/github-runner:latest"
	}

	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Validate enforces required fields and applies per-repo defaults.
func (c *Config) Validate() error {
	if c.SmeeURL == "" {
		return errors.New("smee_url required (or SMEE_URL env)")
	}
	if c.VerifyHMAC && c.WebhookSecret == "" {
		return errors.New("webhook_secret required when verify_hmac=true")
	}
	if c.GitHubPAT == "" {
		return errors.New("github_pat required (or GH_PAT env)")
	}
	if len(c.Repos) == 0 {
		return errors.New("at least one repo required")
	}
	for i, r := range c.Repos {
		if r.Name == "" || r.RepoURL == "" || r.RunnerLabels == "" {
			return fmt.Errorf("repo[%d] missing required field", i)
		}
		if len(r.MatchLabels) == 0 && r.Label == "" {
			return fmt.Errorf("repo[%d] needs match_labels or label", i)
		}
		if len(r.MatchLabels) == 0 {
			c.Repos[i].MatchLabels = []string{r.Label}
		}
		if r.MaxConcurrency <= 0 {
			c.Repos[i].MaxConcurrency = 4
		}
	}
	return nil
}

// RepoByName returns a pointer to the RepoConfig with the given name, or
// (nil,false) if absent.
func (c *Config) RepoByName(name string) (*RepoConfig, bool) {
	for i, r := range c.Repos {
		if r.Name == name {
			return &c.Repos[i], true
		}
	}
	return nil, false
}

// warnIfWorldReadable logs a warning if the config file is readable by
// group or other (mode & 0o077 != 0). The file contains the GitHub PAT and
// webhook secret; loose perms make a host-local privilege boundary leaky.
func warnIfWorldReadable(path string) {
	st, err := os.Stat(path)
	if err != nil {
		return
	}
	mode := st.Mode().Perm()
	if mode&0o077 != 0 {
		slog.Warn("config file is not 0600",
			"path", path,
			"mode", fmt.Sprintf("%04o", mode),
			"advice", "chmod 600 to prevent secret exposure")
	}
}
