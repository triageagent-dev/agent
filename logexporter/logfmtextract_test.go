package logexporter

import "testing"

func TestExtractErrorText_LogfmtTable(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "live production sample: envelope dropped, msg+err composed",
			in: `ts=2026-08-23T14:22:10.483123456Z caller=collector.go:142 level=error msg="collector failed" ` +
				`name=database duration_seconds=5.2 err="dial tcp: lookup dev-ecosystem-postgres-api-broker.svc.cluster.local on 10.0.0.10:53: no such host"`,
			want: "collector failed | dial tcp: lookup dev-ecosystem-postgres-api-broker.svc.cluster.local on 10.0.0.10:53: no such host",
		},
		{
			name: "escaped-quote value is unescaped",
			in:   `level=error msg="a \"b\" c"`,
			want: `a "b" c`,
		},
		{
			name: "bare (unquoted) token value",
			in:   "level=error err=timeout",
			want: "timeout",
		},
		{
			name: "msg-only",
			in:   `level=error msg="only message here"`,
			want: "only message here",
		},
		{
			name: "err-only",
			in:   `level=error err="only error here"`,
			want: "only error here",
		},
		{
			name: "long forms message= and error=",
			in:   `message="long form message" error="long form error"`,
			want: "long form message | long form error",
		},
		{
			name: "identical msg and err values compose ONCE, not duplicated",
			in:   `msg="same text" err="same text"`,
			want: "same text",
		},
		{
			name: "non-logfmt line (no target key=) passes through unchanged",
			in:   "panic: something totally different with no equals signs",
			want: "panic: something totally different with no equals signs",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractErrorText([]byte(tc.in), DefaultSampleJSONFields)
			if string(got) != tc.want {
				t.Errorf("extractErrorText(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestExtractErrorText_LogfmtDecoyInsideQuotedValueIsNotExtracted is the
// pinned decoy test: a key-shaped substring INSIDE another key's quoted
// value must never be mistaken for a real key=value pair. A naive scanner
// that independently searched for `msg="..."` would find the escaped decoy
// and return "decoy"; the real tokenizer consumes the whole err value
// (escapes and all) as one quoted string before it ever looks for the next
// key, so no separate msg pair is ever produced.
func TestExtractErrorText_LogfmtDecoyInsideQuotedValueIsNotExtracted(t *testing.T) {
	in := `level=error err="real error text msg=\"decoy\" more text"`
	got := string(extractErrorText([]byte(in), DefaultSampleJSONFields))
	if got == "decoy" {
		t.Fatalf("extracted %q — the decoy inside the quoted err value was mistaken for a real msg key", got)
	}
	want := `real error text msg="decoy" more text`
	if got != want {
		t.Errorf("extractErrorText(%q) = %q, want %q (the whole err value, decoy included as literal unescaped text)", in, got, want)
	}
}

// TestExtractErrorText_JSONLinesStillTakeTheJSONPath is a regression guard:
// JSON-shaped content must dispatch to extractFromJSON, never fall through
// to logfmt, even after round 4 restructured extractErrorText into a
// dispatcher over two extractors.
func TestExtractErrorText_JSONLinesStillTakeTheJSONPath(t *testing.T) {
	in := `{"msg":"json message","level":"error"}`
	got := string(extractErrorText([]byte(in), DefaultSampleJSONFields))
	if got != "json message" {
		t.Errorf("extractErrorText(%q) = %q, want %q (JSON path)", in, got, "json message")
	}
}

func TestParseLogfmtPairs(t *testing.T) {
	in := []byte(`a=1 b="two words" c=bare d="esc\"aped"`)
	pairs := parseLogfmtPairs(in)
	want := []logfmtPair{
		{key: "a", value: "1"},
		{key: "b", value: "two words"},
		{key: "c", value: "bare"},
		{key: "d", value: `esc"aped`},
	}
	if len(pairs) != len(want) {
		t.Fatalf("got %d pairs, want %d: %+v", len(pairs), len(want), pairs)
	}
	for i, p := range pairs {
		if p != want[i] {
			t.Errorf("pair[%d] = %+v, want %+v", i, p, want[i])
		}
	}
}

func TestLogfmtKeyCandidates(t *testing.T) {
	if !logfmtKeyCandidates([]byte("level=error msg=hi"), DefaultSampleJSONFields) {
		t.Error("expected true: msg= is present")
	}
	if logfmtKeyCandidates([]byte("panic: no keys here at all"), DefaultSampleJSONFields) {
		t.Error("expected false: no target key= substring present")
	}
}
