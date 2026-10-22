package logexporter

import "bytes"

// logfmtMessageKeys/logfmtErrorKeys classify which of the configured
// extraction field names (the same list extractFromJSON uses, reused here
// rather than adding a second flag) compose a logfmt line's sample: the
// first CONFIGURED field belonging to each set that is actually present in
// the line wins that slot. A custom field list containing none of these
// four canonical names simply finds nothing via logfmt — extractFromJSON's
// flat first-match-wins model has no such two-slot shape and is unaffected
// by this classification.
var (
	logfmtMessageKeys = map[string]bool{"message": true, "msg": true}
	logfmtErrorKeys   = map[string]bool{"error": true, "err": true}
)

// logfmtKeyCandidates is the cheap pre-filter: true only if content
// contains at least one configured field name immediately followed by '='
// ANYWHERE in the byte slice — a plain bytes.Contains scan, no parsing. A
// line with none of these substrings can never yield anything from
// parseLogfmtPairs, so the real (quote-aware) tokenizer is skipped
// entirely for arbitrary non-logfmt error lines ("panic: ...", a bare
// message with no key=value structure at all). Mirrors classify.go's
// literal pre-filter idiom.
func logfmtKeyCandidates(content []byte, fields []string) bool {
	for _, f := range fields {
		if bytes.Contains(content, []byte(f+"=")) {
			return true
		}
	}
	return false
}

// extractFromLogfmt implements the logfmt half of extractErrorText: the
// first configured field in logfmtMessageKeys present in the line (in
// fields' configured order), and the first configured field in
// logfmtErrorKeys present, are composed as "<msg> | <err>" when both are
// present and their values differ; either alone is returned by itself;
// neither present reports ok=false (caller falls back to the raw line).
//
// Motivating production sample: `ts=... caller=collector.go:142
// level=error msg="collector failed" name=database duration_seconds=5.2
// err="dial tcp: lookup dev-ecosystem-postgres-api-broker...: no such
// host"` — envelope fields (ts/caller/level/name/duration_seconds) are
// never candidates (not in the default field list) and are correctly
// dropped; only msg/err reach the sample.
func extractFromLogfmt(content []byte, fields []string) (result []byte, ok bool) {
	if !logfmtKeyCandidates(content, fields) {
		return nil, false
	}
	pairs := parseLogfmtPairs(content)
	if len(pairs) == 0 {
		return nil, false
	}

	msgVal := firstLogfmtValue(pairs, fields, logfmtMessageKeys)
	errVal := firstLogfmtValue(pairs, fields, logfmtErrorKeys)

	switch {
	case msgVal != "" && errVal != "" && msgVal != errVal:
		return []byte(msgVal + " | " + errVal), true
	case msgVal != "":
		return []byte(msgVal), true
	case errVal != "":
		return []byte(errVal), true
	default:
		return nil, false
	}
}

// firstLogfmtValue returns the value of the first field (in fields'
// configured order) that both belongs to keySet and is present among pairs
// with a non-empty value.
func firstLogfmtValue(pairs []logfmtPair, fields []string, keySet map[string]bool) string {
	for _, f := range fields {
		if !keySet[f] {
			continue
		}
		for _, p := range pairs {
			if p.key == f && p.value != "" {
				return p.value
			}
		}
	}
	return ""
}

// logfmtPair is one key/value pair tokenized out of a logfmt line.
type logfmtPair struct {
	key   string
	value string
}

// parseLogfmtPairs tokenizes a single logfmt-formatted line into an ordered
// list of key/value pairs. A key is only ever read at a "key position" —
// immediately after whitespace, or at the start of the line — never
// re-derived from inside a quoted value's content. That is what makes a
// decoy like `err="... msg=\"fake\" ..."` safe: the tokenizer scans
// sequentially and consumes the whole quoted value (escapes and all)
// before ever looking for the next key, so text inside a quoted value can
// never be misread as a new key=value pair.
//
// A value is either a double-quoted string — backslash-escapes ANY
// following character, most commonly \" for a literal embedded quote — or
// a bare token running to the next whitespace (so it may itself contain
// '=', e.g. a bare URL value). A bare key with no '=' (a logfmt flag) is
// skipped: it can never carry a message/error value.
func parseLogfmtPairs(line []byte) []logfmtPair {
	var pairs []logfmtPair
	i, n := 0, len(line)
	for i < n {
		for i < n && isLogfmtSpace(line[i]) {
			i++
		}
		if i >= n {
			break
		}
		keyStart := i
		for i < n && line[i] != '=' && !isLogfmtSpace(line[i]) {
			i++
		}
		key := string(line[keyStart:i])
		if i >= n || line[i] != '=' {
			// Bare flag, no value — skip to the next whitespace and move on.
			for i < n && !isLogfmtSpace(line[i]) {
				i++
			}
			continue
		}
		i++ // consume '='
		if i < n && line[i] == '"' {
			i++ // consume opening quote
			value, next := readLogfmtQuotedValue(line, i)
			i = next
			pairs = append(pairs, logfmtPair{key: key, value: value})
			continue
		}
		valStart := i
		for i < n && !isLogfmtSpace(line[i]) {
			i++
		}
		pairs = append(pairs, logfmtPair{key: key, value: string(line[valStart:i])})
	}
	return pairs
}

// readLogfmtQuotedValue reads a double-quoted logfmt value starting just
// past the opening quote, unescaping "\X" to the literal character X (most
// commonly \" for an embedded quote), and returns the unescaped value plus
// the index just past the closing quote. An unterminated (truncated) quoted
// value reads to the end of the line rather than panicking or losing data.
func readLogfmtQuotedValue(line []byte, start int) (value string, next int) {
	i, n := start, len(line)
	var buf []byte
	for i < n {
		c := line[i]
		if c == '\\' && i+1 < n {
			buf = append(buf, line[i+1])
			i += 2
			continue
		}
		if c == '"' {
			i++ // consume closing quote
			break
		}
		buf = append(buf, c)
		i++
	}
	return string(buf), i
}

func isLogfmtSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n'
}
