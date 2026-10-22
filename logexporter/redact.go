package logexporter

import "regexp"

// redactPlaceholder replaces every secret or personal value redactSample
// finds. It is distinct from fingerprintPlaceholder so a reader of a sample
// can tell a redaction from a masked volatile token.
const redactPlaceholder = "***"

// redactRule is one secret or PII pattern and its replacement. Rules run in
// order: URI credentials before emails, because "user:pass@host" would
// otherwise read as an email address.
type redactRule struct {
	re   *regexp.Regexp
	repl string
}

var redactRules = []redactRule{
	// scheme://user:password@host -> scheme://user:***@host (the ADR-0007
	// Fluent Bit scrub, restored natively).
	{regexp.MustCompile(`([A-Za-z][A-Za-z0-9+.-]*://[^\s:/@]+:)[^\s@/]+@`), "${1}" + redactPlaceholder + "@"},
	// JSON Web Tokens: three base64url segments, the first a JSON header.
	{regexp.MustCompile(`eyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]+`), redactPlaceholder},
	// Authorization schemes: "Bearer <token>", "Basic <credentials>".
	{regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}`), "${1} " + redactPlaceholder},
	// key=value, key: value and "key":"value" for secret-named keys, in
	// logfmt, plain text and JSON.
	{regexp.MustCompile(`(?i)("?\b(?:password|passwd|pwd|secret|client[_-]?secret|token|access[_-]?token|refresh[_-]?token|api[_-]?key|apikey|access[_-]?key|private[_-]?key|authorization)"?\s*[:=]\s*"?)(?:(?i:bearer|basic)\s+)?[^\s",;&}]+`), "${1}" + redactPlaceholder},
	// AWS access key IDs.
	{regexp.MustCompile(`\b(?:AKIA|ASIA)[0-9A-Z]{16}\b`), redactPlaceholder},
	// Email addresses.
	{regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`), redactPlaceholder},
}

// redactSample removes credentials and personal data from an error line
// before it becomes the log.sample exemplar that leaves the cluster. It runs
// on the stripped line, ahead of truncation, so a secret is never half cut
// and left partly visible at the length limit.
func redactSample(s string) string {
	for _, r := range redactRules {
		s = r.re.ReplaceAllString(s, r.repl)
	}
	return s
}
