package logexporter

import (
	"strings"
	"unicode"
)

// stripPlaceholderChars is the set of characters a token may be composed
// ENTIRELY of for stripLeadingNoise to treat it as structural noise (a
// timestamp/sequence fragment that would MASK to nothing but placeholders/
// punctuation) rather than message content: the fingerprint placeholder
// itself plus common timestamp/sequence punctuation. Deliberately narrow —
// nothing word-like is ever in this set, so a token containing even one real
// letter always stops the strip.
const stripPlaceholderChars = fingerprintPlaceholder + "[]():.,/+-"

// stripTimezoneTokens is the tiny fixed set of bare timezone abbreviations
// stripLeadingNoise treats as noise on their own. Unlike the
// placeholder-composed tokens above these ARE genuine words, so they are
// matched by an explicit, narrow allowlist rather than a character-class
// rule — nothing else word-like is ever stripped.
var stripTimezoneTokens = map[string]bool{"UTC": true, "GMT": true, "Z": true}

// stripLeadingNoise repeatedly trims leading structural noise — timestamp/
// sequence fragments and bare timezone tokens — off RAW, UNMASKED text: a
// plain-text log line's timestamp preamble
// ("2026-08-23 14:22:10.483 UTC [12345] FATAL: ..."), still carrying its
// REAL digits, not yet collapsed to placeholders. This is round 5's
// identity/display split (docs/adr/0014-log-exporter-in-agent-ds.md's
// Samples section): the function used to run on an ALREADY-fingerprinted
// string; it now runs on raw text so the caller can build the Prometheus
// `sample` label from a real exemplar instead of a masked template.
//
// For each LEADING token (whitespace-delimited — any Unicode space, not
// just ' ', since raw text can carry the tabs/newlines a CRI multiline
// reassembly leaves behind), applyMaskTransforms (fingerprint.go — the
// SAME UUID/hex-run/digit-run collapse fingerprint() applies to a whole
// line, applied here to one token in isolation) is what decides whether
// that token IS noise: a token that would mask down to nothing but
// placeholder+punctuation, or an exact bare timezone abbreviation, is
// noise. Stops at the first token that is not pure noise (by this same
// masked test) — mid-line placeholders (a request ID inside a real
// sentence) are left exactly where they are, and everything from that
// point to the end of s is returned UNCHANGED (not token-rejoined), so any
// internal formatting the kept text carries — multiple spaces, an embedded
// newline from a multiline value — survives verbatim. Only the LEADING run
// of noise, plus its own separating whitespace, is ever consumed.
//
// If stripping would consume the entire sample, the original input is
// returned unchanged instead — this pass never returns empty.
//
// Runs on every error line's extracted text, whichever path (JSON, logfmt,
// or raw) produced it: an extraction result's first token is always real
// text, so the strip is a correct no-op for those — pinned by
// TestStripLeadingNoise_PassesExtractedTextThrough. Also runs, unchanged in
// behavior, on the ALREADY-masked identity fingerprint (tailer.go's
// countLine) — masking an already-masked token is a no-op (there is no raw
// digit/hex/UUID substring left for applyMaskTransforms to find), so this
// produces byte-identical output to the pre-round-5 masked-text pass for
// that path (TestFingerprintIdentityUnchanged). A cheap token scan, error
// path only, never touching the non-error hot path.
func stripLeadingNoise(s string) string {
	rest := s
	for {
		trimmed := strings.TrimLeftFunc(rest, unicode.IsSpace)
		if trimmed == "" {
			break
		}
		sp := strings.IndexFunc(trimmed, unicode.IsSpace)
		token := trimmed
		if sp >= 0 {
			token = trimmed[:sp]
		}
		if !isNoiseToken(applyMaskTransforms(token)) {
			rest = trimmed
			break
		}
		if sp < 0 {
			rest = ""
			break
		}
		rest = trimmed[sp+1:]
	}
	if strings.TrimSpace(rest) == "" {
		return s
	}
	return rest
}

// isNoiseToken reports whether token is either composed ENTIRELY of
// stripPlaceholderChars, or is a bare timezone token from
// stripTimezoneTokens.
func isNoiseToken(token string) bool {
	if token == "" {
		return false
	}
	if stripTimezoneTokens[token] {
		return true
	}
	for _, r := range token {
		if !strings.ContainsRune(stripPlaceholderChars, r) {
			return false
		}
	}
	return true
}
