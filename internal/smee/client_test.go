package smee

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
)

type captureHandler struct {
	mu          sync.Mutex
	calls       []capture
	dispatchErr error
}

type capture struct {
	headers http.Header
	body    []byte
}

func (h *captureHandler) Dispatch(_ context.Context, headers http.Header, body []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	hdrCopy := http.Header{}
	for k, vs := range headers {
		for _, v := range vs {
			hdrCopy.Add(k, v)
		}
	}
	h.calls = append(h.calls, capture{headers: hdrCopy, body: append([]byte(nil), body...)})
	return h.dispatchErr
}

// buildSmeeStream constructs an SSE byte stream as smee.io actually emits it:
// id + data, no explicit event field (default "message" per SSE spec).
func buildSmeeStream(t *testing.T, payloads []map[string]any) []byte {
	var buf bytes.Buffer
	buf.WriteString(": ping\n\n") // a comment line
	for i, p := range payloads {
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		fmt.Fprintf(&buf, "id: %d\n", i+1)
		buf.WriteString("data: ")
		buf.Write(b)
		buf.WriteString("\n\n")
	}
	return buf.Bytes()
}

func TestParseStreamWithRawStringBody(t *testing.T) {
	bodyJSON := `{"action":"queued","workflow_job":{"labels":["self-hosted","mac-docker-backend"]},"repository":{"full_name":"acme/web"}}`
	stream := buildSmeeStream(t, []map[string]any{
		{
			"x-github-event":      "workflow_job",
			"x-hub-signature-256": "sha256=deadbeef",
			"content-type":        "application/json",
			"body":                bodyJSON, // string form
			"timestamp":           1700000000000,
		},
	})

	cap := &captureHandler{}
	c := &Client{Handler: cap, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := c.parseStream(context.Background(), bytes.NewReader(stream)); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if len(cap.calls) != 1 {
		t.Fatalf("got %d calls, want 1", len(cap.calls))
	}
	if got := cap.calls[0].headers.Get("X-Github-Event"); got != "workflow_job" {
		t.Errorf("event header = %q", got)
	}
	if string(cap.calls[0].body) != bodyJSON {
		t.Errorf("body mismatch:\n got: %s\nwant: %s", cap.calls[0].body, bodyJSON)
	}
}

func TestParseStreamWithObjectBody(t *testing.T) {
	stream := buildSmeeStream(t, []map[string]any{
		{
			"x-github-event": "workflow_job",
			"body": map[string]any{
				"action":     "queued",
				"repository": map[string]any{"full_name": "acme/web"},
			},
		},
	})

	cap := &captureHandler{}
	c := &Client{Handler: cap, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := c.parseStream(context.Background(), bytes.NewReader(stream)); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if len(cap.calls) != 1 {
		t.Fatalf("got %d calls, want 1", len(cap.calls))
	}
	var parsed map[string]any
	if err := json.Unmarshal(cap.calls[0].body, &parsed); err != nil {
		t.Fatalf("body not valid JSON: %v", err)
	}
	if parsed["action"] != "queued" {
		t.Errorf("action = %v", parsed["action"])
	}
}

func TestParseStreamMultipleEvents(t *testing.T) {
	stream := buildSmeeStream(t, []map[string]any{
		{"x-github-event": "ping", "body": "{}"},
		{"x-github-event": "workflow_job", "body": `{"action":"queued"}`},
		{"x-github-event": "workflow_job", "body": `{"action":"completed"}`},
	})

	cap := &captureHandler{}
	c := &Client{Handler: cap, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := c.parseStream(context.Background(), bytes.NewReader(stream)); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if len(cap.calls) != 3 {
		t.Fatalf("got %d calls, want 3", len(cap.calls))
	}
}
