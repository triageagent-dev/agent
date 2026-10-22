package logexporter

import "testing"

func TestRedactSample(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"uri credentials", "dial postgres://app:s3cr3t@db:5432/orders failed", "dial postgres://app:***@db:5432/orders failed"},
		{"bearer token", "upstream 401 Authorization: Bearer abcdef0123456789xyz", "upstream 401 Authorization: ***"},
		{"bearer alone", "sent Bearer abcdef0123456789xyz to api", "sent Bearer *** to api"},
		{"jwt", "bad token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0In0.sig_abc-123 rejected", "bad token *** rejected"},
		{"logfmt password", "level=error msg=login failed password=hunter2 user=bob", "level=error msg=login failed password=*** user=bob"},
		{"json secret", `{"level":"error","api_key":"k-123456","msg":"denied"}`, `{"level":"error","api_key":"***","msg":"denied"}`},
		{"colon token", "refresh failed token: abc.def.ghi", "refresh failed token: ***"},
		{"aws key", "using AKIAABCDEFGHIJKLMNOP for s3", "using *** for s3"},
		{"email", "no account for jane.doe@example.com", "no account for ***"},
		{"plain line untouched", "connection refused to orders:8080 after 3 retries", "connection refused to orders:8080 after 3 retries"},
		{"trace id kept", "request 4bf92f3577b34da6a3ce929d0e0e4736 failed", "request 4bf92f3577b34da6a3ce929d0e0e4736 failed"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := redactSample(c.in); got != c.want {
				t.Errorf("redactSample(%q)\n got %q\nwant %q", c.in, got, c.want)
			}
		})
	}
}

// TestCounterSet_ExemplarIsRedacted checks the path the sample takes out of
// the cluster: recordError stores the redacted line as the slot's exemplar,
// and a secret near the length limit is removed whole, not cut in half.
func TestCounterSet_ExemplarIsRedacted(t *testing.T) {
	cs := &counterSet{}
	line := []byte("login failed for jane@example.com password=hunter2-very-long-secret")
	cs.recordError("fp", line, 48, DefaultMaxSamplesPerService)

	slots := cs.errorSlotsSnapshot()
	if len(slots) != 1 {
		t.Fatalf("expected 1 slot, got %d", len(slots))
	}
	if want := "login failed for *** password=***"; slots[0].exemplar != want {
		t.Errorf("exemplar = %q, want %q", slots[0].exemplar, want)
	}
}
