package logexporter

import (
	"encoding/json"
	"strings"
)

// DefaultSampleJSONFields is the default priority-ordered list of top-level
// JSON keys checked when an error line's content is a JSON object — the
// first key present with a non-empty STRING value wins, matching
// --log-exporter-sample-json-fields' default.
var DefaultSampleJSONFields = []string{"message", "msg", "error", "err"}

// looksLikeJSONObject reports whether content, after skipping leading JSON
// (RFC 8259) whitespace, starts with '{'. A cheap byte scan with an early
// exit on the first non-whitespace byte — the real validation (and the
// fallback-on-failure) happens in extractErrorText via encoding/json. This
// is what keeps a non-JSON error line (the common case — "level=error ...",
// "panic: ...") from ever paying for a JSON parse attempt.
func looksLikeJSONObject(content []byte) bool {
	for _, b := range content {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		default:
			return b == '{'
		}
	}
	return false
}

// jsonRawAsNonEmptyString decodes raw as a JSON string and reports whether
// it is both syntactically a string AND non-empty. A present-but-empty or
// present-but-non-string (number/null/array/object) value reports ok=false,
// so the caller falls through to the next candidate — "empty extracted
// value falls through to next key" (docs/adr/0014-log-exporter-in-agent-
// ds.md's Samples section).
func jsonRawAsNonEmptyString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, s != ""
}

// extractErrorText tries to pull the human-meaningful message out of a
// structured error line — JSON object first, then logfmt (round 4:
// docs/adr/0014-log-exporter-in-agent-ds.md's Samples section) — falling
// back to content UNCHANGED (the safe, "never lose the sample" behavior)
// when neither shape yields anything. Structured loggers routinely bury the
// actual error text behind metadata a fingerprinted-then-truncated raw line
// would otherwise spend its whole sampleMaxLen budget on — two real
// production samples motivated the two extractors below: a GCP Cloud
// Logging JSON envelope, and a Prometheus-style logfmt line
// (`ts=... level=error msg="..." err="..."`).
//
// A JSON-shaped line (looksLikeJSONObject) ONLY ever takes the JSON path —
// malformed/truncated JSON falls straight to raw, it never also tries
// logfmt (JSON and logfmt are mutually exclusive envelope shapes in
// practice, and a line that merely LOOKS like JSON but fails to parse is
// more likely truncated than accidentally logfmt-shaped). A non-JSON line
// tries logfmt next.
//
// Both extractors read fields (the same configured list — reused for both
// shapes rather than adding a second flag) and are only ever called from
// tailer.countLine's verdictError branch — never on the hot (non-error)
// per-line path — and both cheap-reject before doing real parsing work.
func extractErrorText(content []byte, fields []string) []byte {
	if looksLikeJSONObject(content) {
		if extracted, ok := extractFromJSON(content, fields); ok {
			return extracted
		}
		return content
	}
	if extracted, ok := extractFromLogfmt(content, fields); ok {
		return extracted
	}
	return content
}

// extractFromJSON implements the JSON half of extractErrorText: fields is
// checked in priority order; the first key present with a non-empty string
// value wins. When that key's value is a JSON OBJECT instead, its own
// "message" sub-key is tried (covers the common Go/Node error-wrapping
// shape {"error":{"message":"..."}}). If a top-level "stack" string is also
// present, its FIRST LINE ONLY is appended as " | "+line.
//
// Uses encoding/json — a real parser, not a substring scan — so a value
// string that happens to CONTAIN the literal text of a candidate key name
// (or an escaped quote, or a nested object/array) can never be mistaken for
// that key. ok=false when the JSON is malformed/truncated or none of
// fields' keys yield a usable value.
func extractFromJSON(content []byte, fields []string) (result []byte, ok bool) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(content, &top); err != nil {
		return nil, false
	}

	var extracted string
	for _, key := range fields {
		raw, present := top[key]
		if !present {
			continue
		}
		if s, sok := jsonRawAsNonEmptyString(raw); sok {
			extracted = s
			break
		}
		// Object value: try its own "message" sub-key. json.Unmarshal into a
		// map fails (and sub stays nil) for any non-object value — numbers,
		// arrays, null — which is exactly "no usable sub-message here",
		// falling through to the next candidate key below.
		var sub map[string]json.RawMessage
		if err := json.Unmarshal(raw, &sub); err == nil {
			if s, sok := jsonRawAsNonEmptyString(sub["message"]); sok {
				extracted = s
				break
			}
		}
	}
	if extracted == "" {
		return nil, false
	}

	if stackRaw, present := top["stack"]; present {
		if stack, sok := jsonRawAsNonEmptyString(stackRaw); sok {
			firstLine := stack
			if i := strings.IndexByte(stack, '\n'); i >= 0 {
				firstLine = stack[:i]
			}
			extracted = extracted + " | " + firstLine
		}
	}
	return []byte(extracted), true
}
