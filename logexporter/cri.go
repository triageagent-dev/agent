package logexporter

import "bytes"

// splitCRI parses one raw log line in CRI's log format:
//
//	2026-08-22T10:15:23.123456789Z stdout F actual log content here
//
// three space-separated fields (timestamp, stream, partial/full flag) followed
// by the content, which may itself contain arbitrary bytes including spaces.
// Finds the third space by index only — no allocation, no string conversion,
// the returned content is a sub-slice of line. ok is false when line does not
// have the expected three-field prefix (not CRI-formatted, or a malformed/
// truncated read); callers fall back to treating the whole line as content in
// that case (cri.go's own doc references this fallback; see tailer.go).
func splitCRI(line []byte) (flag byte, content []byte, ok bool) {
	i := bytes.IndexByte(line, ' ')
	if i < 0 {
		return 0, nil, false
	}
	rest := line[i+1:]
	j := bytes.IndexByte(rest, ' ')
	if j < 0 {
		return 0, nil, false
	}
	rest2 := rest[j+1:]
	k := bytes.IndexByte(rest2, ' ')
	if k < 0 {
		return 0, nil, false
	}
	flagField := rest2[:k]
	if len(flagField) != 1 || (flagField[0] != 'P' && flagField[0] != 'F') {
		return 0, nil, false
	}
	return flagField[0], rest2[k+1:], true
}

// trimEOL strips a trailing "\n" or "\r\n" — a cheap sub-slice, never a copy.
func trimEOL(b []byte) []byte {
	n := len(b)
	if n > 0 && b[n-1] == '\n' {
		n--
	}
	if n > 0 && b[n-1] == '\r' {
		n--
	}
	return b[:n]
}
