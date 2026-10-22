package logexporter

import (
	"regexp"
	"strings"
)

// DefaultSampleMaxLen/DefaultMaxSamplesPerService are the compiled-in defaults
// for error-line sample fingerprinting (docs/adr/0007-error-log-sample-
// fingerprint.md's behavior, restored natively — see ADR-0014's Samples
// section). DefaultSampleMaxLen matches the sample length Fluent Bit's own
// pipeline used, byte-for-byte.
const (
	DefaultSampleMaxLen         = 160
	DefaultMaxSamplesPerService = 8
)

// fingerprintPlaceholder is the token FB's own fingerprint.lua collapsed
// volatile substrings to (ADR-0007) — not renumbered per occurrence, so two
// lines differing only in a request ID or timestamp fingerprint to the
// IDENTICAL string and therefore the identical series.
const fingerprintPlaceholder = "#"

var (
	// uuidRe matches the canonical 8-4-4-4-12 hex-with-dashes UUID shape.
	// Runs BEFORE hexRunRe: four of a UUID's five dash-separated groups are
	// under hexRunRe's 8-char floor (4, 4, 4 hex chars), so hexRunRe alone
	// would only collapse the first (8) and last (12) groups, leaving the
	// three middle groups as literal, request-varying hex text — defeating
	// "make repeats collapse" for the common request-id-as-UUID case.
	uuidRe = regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	// hexRunRe matches a bare run of 8+ hex characters not already caught by
	// uuidRe above — request IDs, hashes, commit SHAs. Word-boundary anchored
	// so a mixed alnum token (e.g. "deadbeefgoose") is never partially
	// chopped: the trailing \b fails unless the ENTIRE maximal run at that
	// position is hex.
	hexRunRe = regexp.MustCompile(`(?i)\b[0-9a-f]{8,}\b`)
	// digitRunRe matches any remaining run of decimal digits (timestamps,
	// ports, counts, byte offsets, request/line numbers).
	digitRunRe = regexp.MustCompile(`[0-9]+`)
	// whitespaceRunRe collapses any run of whitespace — including embedded
	// newlines a CRI multiline reassembly can leave in the content — to a
	// single space.
	whitespaceRunRe = regexp.MustCompile(`\s+`)
)

// applyMaskTransforms collapses UUIDs, hex runs >= 8 chars, and digit runs to
// fingerprintPlaceholder, in that order — the exact three transforms
// fingerprint() applies to a whole line. Factored out so it can ALSO be
// applied to a single leading TOKEN in isolation (stripnoise.go's raw-text
// strip pass, round 5 — see the sample/identity split in
// docs/adr/0014-log-exporter-in-agent-ds.md): the series-identity masking
// rules and the noise-token-detection masking rules must never independently
// drift, and sharing this one function is what guarantees that.
func applyMaskTransforms(s string) string {
	s = uuidRe.ReplaceAllString(s, fingerprintPlaceholder)
	s = hexRunRe.ReplaceAllString(s, fingerprintPlaceholder)
	s = digitRunRe.ReplaceAllString(s, fingerprintPlaceholder)
	return s
}

// truncateSample cuts s to at most maxLen BYTES and trims any dangling
// trailing space the cut can leave behind. Shared between fingerprint()
// (the masked identity string) and the round-5 raw exemplar label
// (metrics.go's errorSlot.exemplar, computed in tailer.go) so both truncate
// exactly the same way — this is a byte-position cut, not a rune-safe one
// (matching the exact behavior fingerprint() has always had; changing that
// here would change identity output, which round 5 must not do). maxLen <=
// 0 disables truncation.
func truncateSample(s string, maxLen int) string {
	if maxLen > 0 && len(s) > maxLen {
		s = s[:maxLen]
		// A byte-position cut can land right after a space (or, rarely,
		// leave a leading one if maxLen ever gets applied before the first
		// trim in a future refactor) — trim again so truncation never
		// leaves a dangling trailing space in the label value.
		s = strings.TrimSpace(s)
	}
	return s
}

// fingerprint normalizes an error line's CONTENT (already CRI-prefix-stripped
// and EOL-trimmed by the caller) into a bounded-cardinality template:
// collapse UUIDs, hex runs >= 8 chars, and digit runs to fingerprintPlaceholder,
// collapse whitespace runs to one space, then truncate to maxLen. Two log
// lines differing only in volatile tokens (timestamps, request IDs, ports)
// normalize to the identical fingerprint and therefore the identical metric
// series — this is what bounds the error-series cardinality per (namespace,
// service) to something close to "distinct error TEMPLATES", not "distinct
// error lines ever seen".
//
// **This is IDENTITY only, since round 5 (docs/adr/0014-log-exporter-in-
// agent-ds.md's identity/display split) — the slot registry stays keyed by
// this masked+truncated string, but it is no longer what a human sees**: the
// Prometheus `sample` label is the slot's raw exemplar
// (metrics.go's errorSlot.exemplar), captured once at slot creation from the
// UNMASKED extracted text. fingerprint's own output is unchanged byte-for-
// byte from before round 5 (`TestFingerprintIdentityUnchanged` pins this) —
// only what gets exposed as the label changed.
//
// Normalizing BEFORE truncating is deliberate, and is the REVERSE of Fluent
// Bit's ORIGINAL Lua filter order: a production fix to that filter
// (docs/adr/0007-error-log-sample-fingerprint.md's 2026-07-17 note) found
// truncate-first spent the entire sampleMaxLen budget on a long JSON
// envelope and never reached the actual message text. Normalizing first (so
// volatile tokens are already collapsed to a single '#' before the length
// cut) keeps more of the budget available for the actual distinguishing text.
// This does not implement Fluent Bit's later JSON-field-extraction refinement
// (message/msg/error/err/stack) — out of scope here; see ADR-0014.
//
// maxLen <= 0 disables truncation. Only ever called from tailer.countLine's
// verdictError branch — never on the hot (non-error, ~95% of lines) per-line
// path, so its allocations (one string conversion + four regexp
// substitutions) are bounded to error lines only.
func fingerprint(content []byte, maxLen int) string {
	s := string(content)
	s = applyMaskTransforms(s)
	s = whitespaceRunRe.ReplaceAllString(s, " ")
	s = strings.TrimSpace(s)
	return truncateSample(s, maxLen)
}
