package spawner

import (
	"io/fs"
	"os"
	"strings"
	"testing"
)

func TestWritePATEnvFile_PermsAndCleanup(t *testing.T) {
	path, cleanup, err := writePATEnvFile("ghp_super_secret")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if mode := st.Mode().Perm(); mode != 0o600 {
		t.Errorf("env-file mode = %o; want 0600", mode)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.HasPrefix(string(b), "ACCESS_TOKEN=ghp_super_secret") {
		t.Errorf("content unexpected: %q", b)
	}
	cleanup()
	if _, err := os.Stat(path); !errIs(err, fs.ErrNotExist) {
		t.Errorf("expected file removed after cleanup, got err=%v", err)
	}
}

func errIs(err error, target error) bool {
	if err == nil {
		return false
	}
	return os.IsNotExist(err) || err.Error() == target.Error()
}
