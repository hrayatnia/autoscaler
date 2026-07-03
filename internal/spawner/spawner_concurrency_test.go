package spawner

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hrayatnia/autoscaler/internal/config"
)

// fakeDocker simulates docker calls. It tracks how many ephemerals exist
// per repo, so countRunning + spawn interact realistically without an
// actual Docker daemon.
type fakeDocker struct {
	mu      sync.Mutex
	running map[string]int // repo -> count
	runs    atomic.Int64
}

func (f *fakeDocker) Exec(_ context.Context, args ...string) ([]byte, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd := args[0]
	switch cmd {
	case "ps":
		// extract `label=gha-autoscaler-repo=<repo>`
		repo := ""
		for i, a := range args {
			if strings.HasPrefix(a, "label=gha-autoscaler-repo=") {
				repo = strings.TrimPrefix(a, "label=gha-autoscaler-repo=")
			}
			_ = i
		}
		n := f.running[repo]
		var sb strings.Builder
		for i := 0; i < n; i++ {
			sb.WriteString("id")
			sb.WriteByte('0' + byte(i))
			sb.WriteByte('\n')
		}
		return []byte(sb.String()), nil, nil
	case "inspect":
		// Reply "running" for each id passed.
		ids := 0
		for _, a := range args {
			if strings.HasPrefix(a, "id") {
				ids++
			}
		}
		var sb strings.Builder
		for i := 0; i < ids; i++ {
			sb.WriteString("running\n")
		}
		return []byte(sb.String()), nil, nil
	case "run":
		// Extract repo via --label flag.
		repo := ""
		for i, a := range args {
			if a == "--label" && i+1 < len(args) && strings.HasPrefix(args[i+1], "gha-autoscaler-repo=") {
				repo = strings.TrimPrefix(args[i+1], "gha-autoscaler-repo=")
			}
		}
		f.runs.Add(1)
		f.running[repo]++
		return []byte("container-" + repo + "\n"), nil, nil
	}
	return nil, nil, nil
}

// fakeHTTP returns an empty runner list so hasIdleMatchingRunner always says
// "no idle". Keeps Spawn from short-circuiting.
type fakeHTTP struct{}

func (fakeHTTP) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(`{"runners":[]}`)),
		Header:     http.Header{},
	}, nil
}

// TestSpawn_CapHonoredUnderRace fires N concurrent Spawn() calls for the
// same repo with cap=K and asserts exactly K spawns occur. This is the
// regression test for the TOCTOU window the v1 PoC had — without the
// per-repo mutex, multiple goroutines pass the count check before any of
// them increments docker state, producing over-cap spawns.
func TestSpawn_CapHonoredUnderRace(t *testing.T) {
	const cap = 3
	const goroutines = 20

	fd := &fakeDocker{running: map[string]int{}}
	sp := NewWithDeps(
		"ghp_test", "img", false,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		fd, fakeHTTP{}, "",
	)
	// We must skip the env-file / docker-run real path; dryRun does that but
	// also skips the cap check accounting because dryRun returns before
	// docker.run. So use dryRun=false but our fakeDocker fakes both ps and
	// run. However writePATEnvFile will run — that's fine, tmp file write
	// in t.TempDir() equivalent (os.TempDir) is cheap and safe.

	repo := &config.RepoConfig{
		Name: "acme/web", RepoURL: "u", MatchLabels: []string{"pool-a"},
		RunnerLabels: "pool-a", MaxConcurrency: cap,
	}

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sp.Spawn(context.Background(), repo)
		}()
	}
	wg.Wait()

	if got := fd.runs.Load(); got != int64(cap) {
		t.Fatalf("docker run invocations = %d; want exactly %d (cap)", got, cap)
	}
	if got := sp.Stats().TotalSpawned; got != uint64(cap) {
		t.Errorf("TotalSpawned = %d; want %d", got, cap)
	}
}
