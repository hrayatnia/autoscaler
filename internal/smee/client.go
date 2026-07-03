package smee

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

const slogLevelDebug = slog.LevelDebug

func preview(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// EventHandler is implemented by webhook.Handler.
type EventHandler interface {
	Dispatch(ctx context.Context, headers http.Header, body []byte) error
}

type Client struct {
	URL     string
	Handler EventHandler
	Log     *slog.Logger

	// IgnoredErr classifies non-fatal handler returns (ignored events).
	IgnoredErr error
	BadSigErr  error

	// HTTPClient is optional; defaults to a long-lived shared client with
	// Timeout=0 (smee is a long-poll SSE stream). Tests may inject a fake.
	HTTPClient *http.Client
}

// Run blocks until ctx is canceled. On disconnect, reconnects with backoff.
func (c *Client) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		err := c.connect(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			c.Log.Warn("smee connection lost", "err", err, "backoff", backoff.String())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = capDuration(backoff*2, 30*time.Second)
	}
}

func (c *Client) connect(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.URL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("User-Agent", "subzero-runner-autoscaler/2.0")

	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 0}
		c.HTTPClient = client
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("smee status %d", resp.StatusCode)
	}
	c.Log.Info("connected to smee", "host", smeeHost(c.URL))

	return c.parseStream(ctx, resp.Body)
}

func (c *Client) parseStream(ctx context.Context, r io.Reader) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1<<20), 8<<20) // 1MB → 8MB

	// SSE spec: absent `event:` field defaults to "message". smee.io emits
	// events with `id:` + `data:` lines and no explicit `event:` field, so we
	// initialize to "message" and only override on an explicit event line.
	event := "message"
	var data string
	flush := func() {
		defer func() { event, data = "message", "" }()
		if event != "message" || data == "" {
			return
		}
		c.handleMessage(ctx, data)
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			flush()
			continue
		}
		switch {
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(line[len("event:"):])
		case strings.HasPrefix(line, "data:"):
			d := strings.TrimPrefix(line[len("data:"):], " ")
			if data != "" {
				data += "\n"
			}
			data += d
		case strings.HasPrefix(line, ":"):
			// SSE comment (smee sends ping comments). Ignore.
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return io.EOF
}

func (c *Client) handleMessage(ctx context.Context, data string) {
	c.Log.Debug("sse event received", "size", len(data))
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(data), &raw); err != nil {
		c.Log.Warn("malformed smee event", "err", err, "preview", preview(data, 120))
		return
	}
	if c.Log.Enabled(nil, slogLevelDebug) {
		var ev string
		if v, ok := raw["x-github-event"]; ok {
			_ = json.Unmarshal(v, &ev)
		}
		c.Log.Debug("sse event parsed", "x_github_event", ev, "keys", len(raw))
	}

	headers := http.Header{}
	for k, v := range raw {
		if k == "body" || k == "query" || k == "timestamp" {
			continue
		}
		var s string
		if err := json.Unmarshal(v, &s); err == nil {
			headers.Set(k, s)
		}
	}

	body, err := extractBody(raw["body"])
	if err != nil {
		c.Log.Warn("unable to extract body", "err", err)
		return
	}

	if err := c.Handler.Dispatch(ctx, headers, body); err != nil {
		if c.IgnoredErr != nil && errors.Is(err, c.IgnoredErr) {
			return
		}
		c.Log.Warn("dispatch failed", "err", err)
	}
}

// extractBody returns the original webhook body bytes. smee.io may pass either:
//   - a string (raw body as JSON-encoded string) — use as-is
//   - an object (already-parsed JSON) — re-serialize compactly
//
// HMAC verification requires byte-exact match with what GitHub signed. The
// string case is exact; the object case is best-effort and may fail HMAC.
func extractBody(raw json.RawMessage) ([]byte, error) {
	if len(raw) == 0 {
		return nil, errors.New("empty body")
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return []byte(s), nil
	}
	// it's an object — return canonical JSON bytes
	return raw, nil
}

// capDuration caps a duration at a maximum. (Renamed from `min` to avoid
// shadowing the Go 1.21 builtin.)
func capDuration(a, max time.Duration) time.Duration {
	if a < max {
		return a
	}
	return max
}

// smeeHost extracts host (without path/token) from a smee URL for logging.
// The path of a smee URL is the channel secret and must never be logged.
func smeeHost(raw string) string {
	if i := strings.Index(raw, "://"); i >= 0 {
		raw = raw[i+3:]
	}
	if i := strings.Index(raw, "/"); i >= 0 {
		raw = raw[:i]
	}
	if raw == "" {
		return "[redacted]"
	}
	return raw
}
