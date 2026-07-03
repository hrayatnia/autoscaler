package webhook

import "testing"

func TestLabelMatches(t *testing.T) {
	cases := []struct {
		name string
		labs []string
		want []string
		ok   bool
	}{
		{"single exact match", []string{"self-hosted", "mac-docker-backend"}, []string{"mac-docker-backend"}, true},
		{"case insensitive", []string{"Self-Hosted", "Mac-Docker-Backend"}, []string{"mac-docker-backend"}, true},
		{"any-of: matches second", []string{"self-hosted", "gha-infra"}, []string{"mac-docker-infra", "gha-infra"}, true},
		{"any-of: matches none", []string{"self-hosted", "ubuntu-latest"}, []string{"mac-docker-infra", "gha-infra"}, false},
		{"empty job labels", []string{}, []string{"mac-docker-backend"}, false},
		{"empty want", []string{"self-hosted", "gha-infra"}, []string{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := labelMatches(tc.labs, tc.want); got != tc.ok {
				t.Errorf("got %v want %v", got, tc.ok)
			}
		})
	}
}
