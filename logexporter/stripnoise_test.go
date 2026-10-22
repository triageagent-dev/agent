package logexporter

import "testing"

func TestStripLeadingNoise_Table(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "postgres-style preamble: timestamp, timezone, and pid bracket all strip",
			in:   `#-#-# #:#:#.# UTC [#] FATAL: role "root" does not exist`,
			want: `FATAL: role "root" does not exist`,
		},
		{
			name: "line starting with meaningful text is byte-unchanged",
			in:   "connection refused",
			want: "connection refused",
		},
		{
			name: "all-placeholder line survives unstripped (never returns empty)",
			in:   "# # #",
			want: "# # #",
		},
		{
			name: "mid-line placeholder is left alone — only a LEADING run strips",
			in:   "request # failed after # retries",
			want: "request # failed after # retries",
		},
		{
			name: "GMT timezone token also strips",
			in:   "# GMT server unreachable",
			want: "server unreachable",
		},
		{
			name: "bare Z (Zulu/UTC) timezone token strips (as its own space-delimited token)",
			in:   "# # Z panic: nil pointer",
			want: "panic: nil pointer",
		},
		{
			name: "a timestamp token with 'T'/'Z' GLUED on (no space) is not pure punctuation, so it stops the strip rather than being silently consumed",
			in:   "#T#:#:#.#Z panic: nil pointer",
			want: "#T#:#:#.#Z panic: nil pointer",
		},
		{
			name: "a real word that merely CONTAINS placeholder-set characters is not noise",
			in:   "error: disk full",
			want: "error: disk full",
		},
		{
			name: "leading whitespace alone still resolves to the trimmed content",
			in:   "   already trimmed by fingerprint in practice",
			want: "already trimmed by fingerprint in practice",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := stripLeadingNoise(tc.in)
			if got != tc.want {
				t.Errorf("stripLeadingNoise(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestStripLeadingNoise_PassesExtractedTextThrough verifies JSON/logfmt
// extraction outputs — whose first token is always real text — are never
// accidentally stripped by this pass.
func TestStripLeadingNoise_PassesExtractedTextThrough(t *testing.T) {
	cases := []string{
		"collector failed | dial tcp: lookup dev-ecosystem-postgres-api-broker.svc.cluster.local on #.#.#.#:#: no such host",
		"failed to acquire lock after # retries: context deadline exceeded",
		"a \"b\" c",
	}
	for _, in := range cases {
		if got := stripLeadingNoise(in); got != in {
			t.Errorf("stripLeadingNoise(%q) = %q, want unchanged", in, got)
		}
	}
}

func TestIsNoiseToken(t *testing.T) {
	noise := []string{"#", "#-#-#", "#:#:#.#", "[#]", "UTC", "GMT", "Z", "(#)", "#/#/#", "#,#"}
	for _, tok := range noise {
		if !isNoiseToken(tok) {
			t.Errorf("isNoiseToken(%q) = false, want true", tok)
		}
	}
	// "-" alone IS a legitimate noise token — it is composed entirely of a
	// single character from the punctuation set (a bare "-" placeholder, the
	// common web-server-log null-value convention), matching the literal
	// character-set rule exactly.
	notNoise := []string{"", "FATAL:", "role", "UTCX", "utc", "#a", "a#", "connection"}
	for _, tok := range notNoise {
		if isNoiseToken(tok) {
			t.Errorf("isNoiseToken(%q) = true, want false", tok)
		}
	}
}

// TestStripLeadingNoise_OperatesOnRawTextForExemplar pins round 5
// (docs/adr/0014-log-exporter-in-agent-ds.md's identity/display split):
// stripLeadingNoise now strips a leading timestamp preamble straight off
// RAW, UNMASKED text — the real digits, not fingerprint's `#` placeholders —
// which is what the round-5 exemplar label (metrics.go's
// errorSlot.exemplar) is built from. Same live Postgres sample as
// TestStripLeadingNoise_EndToEndFromFingerprint below, but fed in BEFORE
// fingerprint() ever runs.
func TestStripLeadingNoise_OperatesOnRawTextForExemplar(t *testing.T) {
	raw := `2026-08-23 07:12:01.123 UTC [4242] FATAL: role "root" does not exist`
	got := stripLeadingNoise(raw)
	want := `FATAL: role "root" does not exist`
	if got != want {
		t.Errorf("stripLeadingNoise(%q) = %q, want %q", raw, got, want)
	}
}

// TestStripLeadingNoise_AllNoiseRawLineNeverReturnsEmpty is the raw-text
// counterpart of the "all-placeholder line survives unstripped" case in
// TestStripLeadingNoise_Table above: three tokens that each MASK down to
// pure placeholder/punctuation (an 8-digit run, a bracketed 8-digit run, an
// all-zero 8-digit run) must never strip the sample away to nothing.
func TestStripLeadingNoise_AllNoiseRawLineNeverReturnsEmpty(t *testing.T) {
	raw := "12345678 [99999999] 00000000"
	got := stripLeadingNoise(raw)
	if got == "" {
		t.Fatal("stripLeadingNoise must never return empty")
	}
	if got != raw {
		t.Errorf("stripLeadingNoise(%q) = %q, want unchanged (every token is noise)", raw, got)
	}
}

// TestStripLeadingNoise_EndToEndFromFingerprint drives the real fingerprint
// output (not a hand-built fixture) through the strip pass, matching the
// exact production shape: a plain-text postgres log line with a real
// timestamp, fingerprinted, then stripped.
func TestStripLeadingNoise_EndToEndFromFingerprint(t *testing.T) {
	raw := `2026-08-23 14:22:10.483 UTC [12345] FATAL: role "root" does not exist`
	fp := fingerprint([]byte(raw), DefaultSampleMaxLen)
	got := stripLeadingNoise(fp)
	want := `FATAL: role "root" does not exist`
	if got != want {
		t.Errorf("stripLeadingNoise(fingerprint(%q)) = %q, want %q (fingerprinted: %q)", raw, got, want, fp)
	}
}
