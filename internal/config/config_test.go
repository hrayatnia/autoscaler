package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func writeTempConfig(t *testing.T, c Config) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoad_Defaults(t *testing.T) {
	path := writeTempConfig(t, Config{
		SmeeURL:   "https://smee.io/abc",
		GitHubPAT: "ghp_xxx",
		Repos: []RepoConfig{{
			Name: "a/b", RepoURL: "u", Label: "L", RunnerLabels: "L",
		}},
	})
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Listen != ":8080" {
		t.Errorf("default Listen wrong: %q", cfg.Listen)
	}
	if cfg.RunnerImage == "" {
		t.Error("default RunnerImage empty")
	}
	if cfg.Repos[0].MaxConcurrency != 4 {
		t.Errorf("default MaxConcurrency wrong: %d", cfg.Repos[0].MaxConcurrency)
	}
	// backward-compat: Label promoted into MatchLabels
	if len(cfg.Repos[0].MatchLabels) != 1 || cfg.Repos[0].MatchLabels[0] != "L" {
		t.Errorf("MatchLabels backfill failed: %v", cfg.Repos[0].MatchLabels)
	}
}

func TestLoad_EnvFallbacks(t *testing.T) {
	t.Setenv("SMEE_URL", "https://smee.io/env")
	t.Setenv("GH_PAT", "ghp_env")
	t.Setenv("PORTAL_TOKEN", "tok-env")

	path := writeTempConfig(t, Config{
		Repos: []RepoConfig{{
			Name: "a/b", RepoURL: "u", MatchLabels: []string{"L"}, RunnerLabels: "L",
		}},
	})
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.SmeeURL != "https://smee.io/env" {
		t.Errorf("SmeeURL = %q", cfg.SmeeURL)
	}
	if cfg.GitHubPAT != "ghp_env" {
		t.Errorf("GitHubPAT = %q", cfg.GitHubPAT)
	}
	if cfg.PortalToken != "tok-env" {
		t.Errorf("PortalToken = %q", cfg.PortalToken)
	}
}

func TestValidate_Failures(t *testing.T) {
	cases := []struct {
		name string
		c    Config
	}{
		{"no smee", Config{GitHubPAT: "x", Repos: []RepoConfig{{Name: "a/b", RepoURL: "u", MatchLabels: []string{"L"}, RunnerLabels: "L"}}}},
		{"no pat", Config{SmeeURL: "s", Repos: []RepoConfig{{Name: "a/b", RepoURL: "u", MatchLabels: []string{"L"}, RunnerLabels: "L"}}}},
		{"no repos", Config{SmeeURL: "s", GitHubPAT: "x"}},
		{"repo missing name", Config{SmeeURL: "s", GitHubPAT: "x", Repos: []RepoConfig{{RepoURL: "u", MatchLabels: []string{"L"}, RunnerLabels: "L"}}}},
		{"repo missing labels", Config{SmeeURL: "s", GitHubPAT: "x", Repos: []RepoConfig{{Name: "a/b", RepoURL: "u", RunnerLabels: "L"}}}},
		{"verify_hmac without secret", Config{SmeeURL: "s", GitHubPAT: "x", VerifyHMAC: true, Repos: []RepoConfig{{Name: "a/b", RepoURL: "u", MatchLabels: []string{"L"}, RunnerLabels: "L"}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.c.Validate(); err == nil {
				t.Errorf("expected validation error for %s", tc.name)
			}
		})
	}
}

func TestRepoByName(t *testing.T) {
	c := &Config{Repos: []RepoConfig{
		{Name: "a/b"}, {Name: "c/d"},
	}}
	if _, ok := c.RepoByName("c/d"); !ok {
		t.Error("expected to find c/d")
	}
	if _, ok := c.RepoByName("missing/x"); ok {
		t.Error("unexpected hit for missing/x")
	}
}

func TestLoad_NotFound(t *testing.T) {
	if _, err := Load("/does/not/exist.json"); err == nil {
		t.Error("expected error for missing file")
	}
}
