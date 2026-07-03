package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func sign(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestVerifySignature(t *testing.T) {
	body := []byte(`{"action":"queued"}`)
	secret := "swordfish"
	good := sign(body, secret)

	cases := []struct {
		name   string
		header string
		secret string
		want   bool
	}{
		{"valid", good, secret, true},
		{"wrong secret", good, "other", false},
		{"empty header", "", secret, false},
		{"missing prefix", strings.TrimPrefix(good, "sha256="), secret, false},
		{"malformed hex", "sha256=zz", secret, false},
		{"tampered body", sign([]byte(`{"action":"completed"}`), secret), secret, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := VerifySignature(body, tc.header, tc.secret)
			if got != tc.want {
				t.Errorf("got %v want %v", got, tc.want)
			}
		})
	}
}
