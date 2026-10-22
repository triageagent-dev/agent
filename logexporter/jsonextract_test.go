package logexporter

import "testing"

func TestExtractErrorText_Table(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		fields []string
		want   string
	}{
		{
			name: "pino-style: level+msg",
			in:   `{"level":"error","msg":"failed to connect to redis: dial tcp: connection refused"}`,
			want: "failed to connect to redis: dial tcp: connection refused",
		},
		{
			name: "zap-style: level+message",
			in:   `{"level":"error","message":"unable to marshal response","ts":1692700000.123}`,
			want: "unable to marshal response",
		},
		{
			name: "GCP Cloud Logging envelope: message buried after a nested operation object",
			in: `{"actor":"worker","event":"sync","jobId":42,"label":"tenant-a","level":"error",` +
				`"logging.googleapis.com/operation":{"id":"op-1","producer":"sync-worker","first":true,"last":false},` +
				`"message":"failed to acquire lock after 3 retries: context deadline exceeded"}`,
			want: "failed to acquire lock after 3 retries: context deadline exceeded",
		},
		{
			name: "nested error object: error.message sub-key",
			in:   `{"error":{"message":"db connection failed","code":500},"service":"api"}`,
			want: "db connection failed",
		},
		{
			name: "stack present: first line only appended after ' | '",
			in:   `{"message":"boom","stack":"Error: boom\n    at foo (file.js:10:5)\n    at bar (file.js:20:3)"}`,
			want: "boom | Error: boom",
		},
		{
			name: "an earlier string VALUE containing the literal text 'message' must not be mistaken for the key",
			in:   `{"jobId":"this value contains the word message in it","msg":"actual error text"}`,
			want: "actual error text",
		},
		{
			name: "an earlier string VALUE with an escaped quote around 'message' must not confuse a real parser",
			in:   `{"note":"he said \"message\": \"fake\" but that is not a real key","msg":"actual message"}`,
			want: "actual message",
		},
		{
			name: "empty extracted value falls through to the next key",
			in:   `{"message":"","msg":"real text"}`,
			want: "real text",
		},
		{
			name: "object value with no usable message sub-key falls through to the next key",
			in:   `{"error":{"code":500},"msg":"real text"}`,
			want: "real text",
		},
		{
			name: "no listed key present at all falls back to the raw line",
			in:   `{"foo":"bar","baz":123}`,
			want: `{"foo":"bar","baz":123}`,
		},
		{
			name: "malformed/truncated JSON falls back to the raw line",
			in:   `{"message": "unterminated`,
			want: `{"message": "unterminated`,
		},
		{
			name: "non-JSON content passes through unchanged",
			in:   "panic: something bad happened",
			want: "panic: something bad happened",
		},
		{
			name: "leading whitespace before '{' is still recognized as JSON",
			in:   "  \t{\"msg\":\"hi there\"}",
			want: "hi there",
		},
		{
			name:   "custom field list is respected over the default priority order",
			in:     `{"message":"default field text","detail":"custom field text"}`,
			fields: []string{"detail"},
			want:   "custom field text",
		},
		{
			name: "array value for a candidate key is skipped, not a crash",
			in:   `{"error":["one","two"],"msg":"real text"}`,
			want: "real text",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fields := tc.fields
			if fields == nil {
				fields = DefaultSampleJSONFields
			}
			got := extractErrorText([]byte(tc.in), fields)
			if string(got) != tc.want {
				t.Errorf("extractErrorText(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestExtractErrorText_FeedsIntoFingerprintUnchanged(t *testing.T) {
	// End-to-end sanity: the extracted text flows through the existing
	// fingerprint+truncate pipeline unmodified — a GCP-envelope line's
	// fingerprint should be the extracted MESSAGE's fingerprint, not the
	// envelope's.
	in := []byte(`{"actor":"worker","jobId":42,"logging.googleapis.com/operation":{"id":"op-1"},` +
		`"message":"failed to acquire lock after 3 retries"}`)
	extracted := extractErrorText(in, DefaultSampleJSONFields)
	got := fingerprint(extracted, DefaultSampleMaxLen)
	want := fingerprint([]byte("failed to acquire lock after # retries"), DefaultSampleMaxLen)
	if got != want {
		t.Errorf("fingerprint(extracted) = %q, want %q (the envelope must not leak into the fingerprint)", got, want)
	}
	if got == fingerprint(in, DefaultSampleMaxLen) {
		t.Error("fingerprinting the raw envelope produced the same result as the extracted message — extraction had no effect")
	}
}

func TestLooksLikeJSONObject(t *testing.T) {
	cases := map[string]bool{
		`{"a":1}`:        true,
		"  {\"a\":1}":    true,
		"\t\n {\"a\":1}": true,
		"not json":       false,
		"":               false,
		"[1,2,3]":        false,
		"   ":            false,
	}
	for in, want := range cases {
		if got := looksLikeJSONObject([]byte(in)); got != want {
			t.Errorf("looksLikeJSONObject(%q) = %v, want %v", in, got, want)
		}
	}
}
