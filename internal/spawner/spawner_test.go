package spawner

import "testing"

func TestSanitize(t *testing.T) {
	cases := map[string]string{
		"mac-docker-backend":  "mac-docker-backend",
		"mac-docker-backend!": "mac-docker-backend",
		"backend/v2":          "backendv2",
		"a b c":               "abc",
		"":                    "",
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			if got := sanitize(in); got != want {
				t.Errorf("sanitize(%q) = %q, want %q", in, got, want)
			}
		})
	}
}

func TestRandHex(t *testing.T) {
	a := randHex(4)
	b := randHex(4)
	if len(a) != 8 || len(b) != 8 {
		t.Fatalf("expected 8 chars, got %d/%d", len(a), len(b))
	}
	if a == b {
		t.Fatalf("randHex returned identical: %q", a)
	}
}
