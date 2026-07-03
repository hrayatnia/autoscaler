// Command autoscaler is the entrypoint for the SubZero ephemeral
// GitHub-Actions runner autoscaler. It wires:
//
//   - smee client → webhook handler  (webhook-driven spawns)
//   - reconciler                      (catches missed/queued jobs)
//   - cleanup loop                    (drains ghost runners / dead containers)
//   - portal HTTP server              (operator UI + ops endpoints)
//
// All goroutines are tied to the root signal-cancelable context and are
// awaited on shutdown via a sync.WaitGroup. The HTTP server is gracefully
// drained with a 5s budget before the process exits.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/hrayatnia/autoscaler/internal/cleanup"
	"github.com/hrayatnia/autoscaler/internal/config"
	"github.com/hrayatnia/autoscaler/internal/portal"
	"github.com/hrayatnia/autoscaler/internal/reconciler"
	"github.com/hrayatnia/autoscaler/internal/smee"
	"github.com/hrayatnia/autoscaler/internal/spawner"
	"github.com/hrayatnia/autoscaler/internal/webhook"
)

func main() {
	var (
		cfgPath           = flag.String("config", "/etc/autoscaler/config.json", "path to config JSON")
		dryRun            = flag.Bool("dry-run", false, "log spawn intents but do not exec docker run")
		level             = flag.String("log-level", "info", "log level: debug|info|warn|error")
		reconcileInterval = flag.Duration("reconcile-interval", 30*time.Second, "interval for reconcile loop (0 disables)")
		cleanupInterval   = flag.Duration("cleanup-interval", 5*time.Minute, "interval for ghost-runner cleanup (0 disables)")
		shutdownTimeout   = flag.Duration("shutdown-timeout", 15*time.Second, "max time to wait for goroutines to drain on SIGTERM")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(*level)}))
	slog.SetDefault(log)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Error("config load failed", "err", err)
		os.Exit(2)
	}
	log.Info("config loaded",
		"smee_url", redactSmeeURL(cfg.SmeeURL),
		"repos", len(cfg.Repos),
		"runner_image", cfg.RunnerImage,
		"dry_run", *dryRun,
		"portal_auth", cfg.PortalToken != "",
	)

	sp := spawner.New(cfg.GitHubPAT, cfg.RunnerImage, *dryRun, log.With("component", "spawner"))
	h := webhook.NewHandler(cfg, sp, log.With("component", "webhook"))
	cl := cleanup.New(cfg, sp, log.With("component", "cleanup"))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup

	var rec *reconciler.Reconciler
	if *reconcileInterval > 0 {
		rec = reconciler.New(cfg, sp, log.With("component", "reconciler"), *reconcileInterval)
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec.Run(ctx)
		}()
	} else {
		log.Info("reconcile loop disabled (interval=0)")
		rec = reconciler.New(cfg, sp, log.With("component", "reconciler"), time.Hour)
	}

	if *cleanupInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl.Run(ctx, *cleanupInterval)
		}()
	} else {
		log.Info("cleanup loop disabled (interval=0)")
	}

	pt := portal.New(cfg, sp, cl, rec)
	httpSrv := buildHTTPServer(cfg, sp, pt)
	wg.Add(1)
	go func() {
		defer wg.Done()
		runHTTPServer(ctx, httpSrv, *shutdownTimeout, log.With("component", "http"))
	}()

	client := &smee.Client{
		URL:        cfg.SmeeURL,
		Handler:    h,
		Log:        log.With("component", "smee"),
		IgnoredErr: webhook.ErrIgnored,
		BadSigErr:  webhook.ErrBadSignature,
	}
	smeeErr := client.Run(ctx)

	// Wait for background goroutines to drain after ctx-cancel.
	drained := make(chan struct{})
	go func() {
		wg.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(*shutdownTimeout):
		log.Warn("shutdown timeout reached; goroutines still running", "timeout", shutdownTimeout.String())
	}

	if smeeErr != nil && !errors.Is(smeeErr, context.Canceled) {
		log.Error("smee client exited", "err", smeeErr)
		os.Exit(1)
	}
	log.Info("shutdown complete")
}

func buildHTTPServer(cfg *config.Config, sp *spawner.Spawner, pt *portal.Server) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := sp.Stats()
		stats.RunningByRepo = map[string]int{}
		for _, r := range cfg.Repos {
			stats.RunningByRepo[r.Name] = sp.CountRunning(r.Name)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stats)
	})
	pt.Mount(mux)

	return &http.Server{
		Addr:              cfg.Listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
}

func runHTTPServer(ctx context.Context, srv *http.Server, shutdownTO time.Duration, log *slog.Logger) {
	errCh := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", srv.Addr)
		errCh <- srv.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server error", "err", err)
		}
		return
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTO)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown error", "err", err)
	}
}

func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// redactSmeeURL prints only scheme+host. The smee path *is* the secret
// (the previous PoC's helper printed the first 24 chars, exposing the token
// in the common 25-char URL form).
func redactSmeeURL(u string) string {
	parsed, err := url.Parse(u)
	if err != nil || parsed.Host == "" {
		return "[redacted]"
	}
	return fmt.Sprintf("%s://%s/[redacted]", parsed.Scheme, parsed.Host)
}
