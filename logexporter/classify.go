package logexporter

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
)

// DefaultErrorRegex/DefaultWarnRegex (built below from the word lists) started as the error/warn regex pair
// Fluent Bit's log_to_metrics filter used (docs/adr/0006-log-based-
// metrics.md) and keep every alternative of it, so a fleet that ran Fluent Bit
// for this pipeline still sees those lines classified the same. Added on top:
//
//   - pino numeric JSON levels: "level":50/60 (error/fatal), "level":40 (warn);
//     the trailing \b keeps 500 and 5 out.
//   - error only: a line STARTING with an exception-style `<Name>Error: `
//     (Node.js). Anchored so the continuation line of the same error
//     (`  details: '... Last error: Error: connect ...'`) is not counted twice.
//   - the three-letter level tokens of zerolog's console writer: ERR, FTL,
//     PNC (error) and WRN (warn), which the core and many Go services log.
//   - an OpenTelemetry severity field, JS-dump (severityText: 'error') or JSON
//     ("severityText":"ERROR") form, value case-insensitive. Deliberately no
//     `type: 'Error'` alternative: it would double-count the same record.
var (
	DefaultErrorRegex = buildErrorRegex()
	DefaultWarnRegex  = buildWarnRegex()
)

// Level words of the formats the exporter knows, each matched in lower, UPPER
// and Title case only (what the libraries print), so the literal prefilter
// below can cover every spelling the regexes accept.
var (
	errorWords = []string{"error", "eror", "err", "fatal", "crit", "critical", "panic", "dpanic", "alert", "emerg", "emergency"}
	warnWords  = []string{"warn", "warning"}
	// levelKeys are the field names a level value hides under: logfmt and JSON
	// loggers (logrus, slog, zap, zerolog JSON, go-kit, structlog, python-json-
	// logger, ECS, Stackdriver severity).
	levelKeys = `level|lvl|severity|levelname|log\.level`
)

func caseVariants(words []string) []string {
	var out []string
	for _, w := range words {
		out = append(out, w, strings.ToUpper(w), strings.ToUpper(w[:1])+w[1:])
	}
	return out
}

func buildErrorRegex() string {
	vals := strings.Join(caseVariants(errorWords), "|")
	return strings.Join([]string{
		// logfmt/slog/logrus/go-kit: level=error, lvl=eror, severity=ERROR.
		`\b(` + levelKeys + `)="?(` + vals + `)\b`,
		// JSON string levels, any spacing: {"level": "ERROR"}.
		`"(` + levelKeys + `)"\s*:\s*"(` + vals + `)"`,
		// pino/bunyan numeric: error 50, fatal 60.
		`"level"\s*:\s*(50|60)\b`,
		// Bare level words of text loggers: python, java, zap console, zerolog
		// console (ERR, FTL, PNC), serilog ([ERR] [FTL]), java.util.logging.
		`\b(ERROR|FATAL|ERR|FTL|PNC|PANIC|DPANIC|CRITICAL|SEVERE)\b`,
		// nginx and envoy: [error] [crit] [alert] [emerg].
		`\[(error|crit|critical|alert|emerg)\]`,
		// .NET console logger: "fail: " and "crit: " at line start.
		`^(fail|crit): `,
		`panic:`,
		// a line STARTING with an exception-style `<Name>Error: ` (Node.js).
		`^\s*\w*Error: `,
		// OpenTelemetry severity field, JS-dump or JSON form.
		`severityText"?:\s*["'](?i:error|fatal)["']`,
	}, "|")
}

func buildWarnRegex() string {
	vals := strings.Join(caseVariants(warnWords), "|")
	return strings.Join([]string{
		`\b(` + levelKeys + `)="?(` + vals + `)\b`,
		`"(` + levelKeys + `)"\s*:\s*"(` + vals + `)"`,
		`"level"\s*:\s*40\b`,
		`\b(WARN|WARNING|WRN)\b`,
		`\[warn(ing)?\]`,
		`^warn: `,
		`severityText"?:\s*["'](?i:warn(ing)?)["']`,
	}, "|")
}

// verdict is the outcome of classifying one line's content.
type verdict int

const (
	verdictNone verdict = iota
	verdictError
	verdictWarn
)

// errorLiterals/warnLiterals are the exact contiguous substrings that MUST be
// present in a line for it to have any chance of matching DefaultErrorRegex /
// DefaultWarnRegex respectively — hand-derived, per-regex (not one combined
// set), as a conservative (never-false-negative) superset test:
//
//   - "level=error" / "level=fatal"   — level=(error|fatal)
//   - `"level":"error"` / `"level":"fatal"` — "level":"(error|fatal)"
//   - "ERROR" / "FATAL"               — \bERROR\b / \bFATAL\b (the word-
//     boundary anchors only narrow further; the substring itself is a safe
//     superset)
//   - "ERR" / "FTL" / "PNC"           — \bERR\b / \bFTL\b / \bPNC\b
//   - "panic:"                        — panic:
//   - `"level":50` / `"level":60`     — "level":(50|60)\b (\b only narrows)
//   - "Error: "                       — ^\s*\w*Error:  (the line-start anchor
//     only narrows; "TypeError: " contains "Error: ")
//   - "severityText"                  — severityText"?:\s*["'](?i:error|fatal)["'].
//     Not expressible as a cheap literal with the value (the value is case-
//     insensitive and the separator varies), so the key alone is the literal:
//     a line with severityText 'info' pays one failed regex call, nothing else.
//   - "level=warn"                    — level=warn(ing)? ("warn" is always a
//     substring whether or not "ing" follows)
//   - `"level":"warn"`                — "level":"warn(ing)?" (same reasoning)
//   - "WARN"                          — \bWARN(ING)?\b (covers WARN and
//     WARNING alike)
//   - "WRN"                           — \bWRN\b
//   - `"level":40`                    — "level":40\b
//   - "severityText"                  — same reasoning as the error side
//
// Splitting into two sets (rather than one combined "might match either"
// list) matters: a line containing only "WARN" has no reason to pay for an
// errorRe.Match attempt first, and a JSON line carrying `"level":"info"`
// contains NEITHER set's tokens at all (unlike a broader bare `"level"` key
// check would) — so it costs zero regex calls, not one or two failed ones.
// This is the dominant cost in the per-line hot path (Go's regexp backtrack
// engine, even on a guaranteed-no-match input, is not free — see
// docs/adr/0014-log-exporter-in-agent-ds.md "Capacity" section for the
// benchmark numbers this literal narrowing was measured against).
var errorLiterals, warnLiterals = buildLiterals()

// buildLiterals derives the prefilter sets from the same word lists the regexes
// use, so a spelling the regex accepts is never rejected before it runs: every
// alternative above implies one of these substrings.
func buildLiterals() (errs, warns [][]byte) {
	add := func(dst *[][]byte, ss ...string) {
		for _, x := range ss {
			*dst = append(*dst, []byte(x))
		}
	}
	for _, w := range caseVariants(errorWords) {
		// `"error` covers every JSON value, `=error` every logfmt one.
		add(&errs, `"`+w, "="+w)
	}
	for _, w := range caseVariants(warnWords) {
		add(&warns, `"`+w, "="+w)
	}
	add(&errs, "ERROR", "FATAL", "ERR", "FTL", "PNC", "PANIC", "DPANIC", "CRITICAL", "SEVERE",
		"[error]", "[crit", "[alert]", "[emerg]", "fail: ", "crit: ", "panic:", "Error: ", "severityText",
		`"level":50`, `"level":60`, `"level": 50`, `"level": 60`, `"level" : 50`, `"level" : 60`, `"level" :50`, `"level" :60`)
	add(&warns, "WARN", "WRN", "[warn", "warn: ", "severityText",
		`"level":40`, `"level": 40`, `"level" : 40`, `"level" :40`)
	return errs, warns
}

func containsAny(content []byte, literals [][]byte) bool {
	for _, lit := range literals {
		if bytes.Contains(content, lit) {
			return true
		}
	}
	return false
}

// classifier confirms candidate lines against the configured error/warn
// regexes. Compiled once at exporter startup, reused across every file/line —
// regexp.Regexp is safe for concurrent use by multiple goroutines.
type classifier struct {
	errorRe *regexp.Regexp
	warnRe  *regexp.Regexp
	// prefilter is only enabled when both configured regex strings are
	// byte-identical to the compiled-in defaults above: candidateLiterals was
	// hand-derived as a conservative match for those two SPECIFIC patterns, and
	// there is no general, safe way to reduce an arbitrary operator-supplied
	// regex to a literal superset. With a custom regex, classify falls back to
	// running both regexes on every line — still fully correct, just without the
	// fast-reject speedup this package's benchmark measures.
	prefilter bool

	// sampleMaxLen/maxSamplesPerService configure error-line fingerprinting
	// (fingerprint.go, docs/adr/0007-error-log-sample-fingerprint.md's
	// behavior restored natively — see ADR-0014's Samples section). Bundled
	// onto classifier rather than threading two more parameters through
	// every tick/drain/processLine/countLine call: both are "what to do once
	// a line classifies as verdictError", the same scope classify() already
	// owns. Unused by classify() itself — read only by tailer.countLine's
	// verdictError branch.
	sampleMaxLen         int
	maxSamplesPerService int
	// sampleFields is the priority-ordered key list extractErrorText
	// (jsonextract.go, logfmtextract.go) checks for BOTH structured shapes
	// it understands — JSON object keys and logfmt key=value pairs — same
	// "config lives on classifier" reasoning as the two fields above. Named
	// generically (not "sampleJSONFields") since round 4 added the logfmt
	// extractor onto the exact same configured list; the public flag/chart
	// value names stay --log-exporter-sample-json-fields /
	// logExporter.sampleJsonFields for compatibility with what already
	// shipped, even though the list now drives more than JSON.
	sampleFields []string
}

func newClassifier(errorRegex, warnRegex string, sampleMaxLen, maxSamplesPerService int, sampleFields []string) (*classifier, error) {
	ere, err := regexp.Compile(errorRegex)
	if err != nil {
		return nil, fmt.Errorf("compile error regex %q: %w", errorRegex, err)
	}
	wre, err := regexp.Compile(warnRegex)
	if err != nil {
		return nil, fmt.Errorf("compile warn regex %q: %w", warnRegex, err)
	}
	if len(sampleFields) == 0 {
		sampleFields = DefaultSampleJSONFields
	}
	return &classifier{
		errorRe:              ere,
		warnRe:               wre,
		prefilter:            errorRegex == DefaultErrorRegex && warnRegex == DefaultWarnRegex,
		sampleMaxLen:         sampleMaxLen,
		maxSamplesPerService: maxSamplesPerService,
		sampleFields:         sampleFields,
	}, nil
}

// classify runs on []byte end-to-end — never converts content to a string
// (regexp.Regexp.Match takes []byte directly) — and allocates nothing on any
// path. Error is checked before warn: a line matching both classifies as error
// only (mirrors the Fluent Bit pipeline's errorRegex/warnRegex being evaluated
// as two independent, mutually-exclusive classifications in that priority
// order). Each regex is skipped individually when the prefilter is enabled and
// that regex's own literal set is absent — not just gated on "either set
// present", which would still pay for a doomed errorRe.Match on a warn-only
// line.
func (c *classifier) classify(content []byte) verdict {
	// A logger that writes to a terminal-style stream colours its level
	// (zerolog's console writer prints ESC[33mWRN ESC[0m), and the m before the
	// W leaves no word boundary for the level regexes. Strip the colour codes
	// first; a line without an ESC byte, nearly all of them, is untouched and
	// costs one byte scan.
	if bytes.IndexByte(content, 0x1b) >= 0 {
		content = ansiColor.ReplaceAll(content, nil)
	}
	// klog/glog lines carry the level as their first letter: E0102 15:04:05.
	if c.prefilter {
		switch klogLevel(content) {
		case verdictError:
			return verdictError
		case verdictWarn:
			return verdictWarn
		}
	}
	if !c.prefilter || containsAny(content, errorLiterals) {
		if c.errorRe.Match(content) {
			return verdictError
		}
	}
	if !c.prefilter || containsAny(content, warnLiterals) {
		if c.warnRe.Match(content) {
			return verdictWarn
		}
	}
	return verdictNone
}

// ansiColor matches an ANSI SGR escape such as ESC[33m or ESC[1;31m.
var ansiColor = regexp.MustCompile("\x1b\\[[0-9;]*m")

// klogLevel recognises klog/glog's header, `E0102 15:04:05.000000 ` with E or F
// for error and W for warning, which no word regex can express cheaply.
func klogLevel(b []byte) verdict {
	if len(b) < 21 || b[5] != ' ' || b[8] != ':' || b[11] != ':' {
		return verdictNone
	}
	for _, i := range []int{1, 2, 3, 4, 6, 7, 9, 10, 12, 13} {
		if b[i] < '0' || b[i] > '9' {
			return verdictNone
		}
	}
	switch b[0] {
	case 'E', 'F':
		return verdictError
	case 'W':
		return verdictWarn
	}
	return verdictNone
}
