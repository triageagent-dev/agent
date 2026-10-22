package logexporter

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// buildBenchLines constructs n realistic, CRI-formatted, 'F'-terminated log
// lines in the mix docs/adr/0014-log-exporter-in-agent-ds.md's "Capacity"
// section documents and measures against: 70% plain no-match, 15% JSON with
// "level":"info", 10% matching WARN variants, 5% matching error/fatal/panic
// variants, line lengths generally 80-400 bytes (deterministic seed for a
// reproducible benchmark number run to run).
//
// The error category is deliberately NOT run through the generic
// pad-then-truncate-to-a-random-target scheme every other category uses: that
// scheme truncates the RAW (pre-fingerprint) string at a byte position that
// can land below fingerprint's own sampleMaxLen (160) cap, and since
// buildBenchLines' truncation happens upstream of fingerprint's digit/hex
// normalization, two occurrences of the "same" template with a
// different-length embedded random number would get cut at different byte
// offsets and normalize to DIFFERENT fingerprints — exactly the instability
// fingerprinting exists to prevent, and not representative of real
// production traffic (a real error template recurs verbatim except for its
// volatile fields). The error category instead uses a small, fixed set of
// complete templates with an embedded varying digit run, which normalizes to
// a STABLE, small set of fingerprints — see buildErrorBenchLines.
func buildBenchLines(n int) [][]byte {
	r := rand.New(rand.NewSource(1))
	lines := make([][]byte, n)
	errIdx := 0
	for i := range lines {
		if i%20 >= 19 { // 5%: error/fatal/panic variants — see doc comment above
			lines[i] = errorBenchLine(r, errIdx)
			errIdx++
			continue
		}
		target := 80 + r.Intn(321) // 80..400 inclusive-ish
		var content string
		switch {
		case i%20 < 14: // 70%: plain, no error/warn tokens at all
			content = fmt.Sprintf("handling request id=%d path=/api/v1/widgets method=GET status=200 duration_ms=%d", r.Int63(), r.Intn(500))
		case i%20 < 17: // 15%: JSON, level info — hits the "level" literal prefilter, confirms as no-match
			content = `{"level":"info","msg":"request completed","path":"/api/v1/widgets","status":200,"duration_ms":12}`
		default: // 10%: WARN variants
			switch i % 3 {
			case 0:
				content = "level=warning disk usage approaching threshold on volume /data"
			case 1:
				content = `{"level":"warn","msg":"retrying upstream call","attempt":2}`
			default:
				content = "WARNING: connection pool nearing capacity"
			}
		}
		for len(content) < target {
			content += " extra padding text to reach the target line length"
		}
		if len(content) > target {
			content = content[:target]
		}
		lines[i] = []byte(fmt.Sprintf("2026-08-22T10:00:00.000000000Z stdout F %s\n", content))
	}
	return lines
}

// errorBenchTemplates are the small, fixed set of complete error-line shapes
// errorBenchLine cycles through — representative of a real fleet's error
// volume being dominated by a handful of recurring failure templates, not an
// unbounded variety of distinct messages. The GCP-envelope template mirrors
// the live production sample that motivated JSON message extraction
// (docs/adr/0014-log-exporter-in-agent-ds.md's Samples section): a
// multi-field envelope, including a nested "logging.googleapis.com/
// operation" object, BEFORE the actual "message" key — this is what
// exercises extractErrorText's real cost (a full json.Unmarshal into a
// map), not just the three plain-text templates below it. The logfmt
// envelope template (round 4) mirrors ITS OWN live production sample —
// `ts=... caller=... level=error msg="..." ... err="..."` — exercising
// parseLogfmtPairs' full tokenizer cost the same way the GCP template
// exercises json.Unmarshal's.
var errorBenchTemplates = []string{
	"level=error failed to write to database: connection reset by peer (attempt %d)",
	`{"level":"fatal","msg":"unrecoverable state, exiting","pid":%d}`,
	"panic: runtime error: invalid memory address or nil pointer dereference [goroutine %d]",
	"ERROR: upstream timeout after %dms calling payments-service",
	`{"actor":"worker","event":"sync","jobId":%d,"label":"tenant-a","level":"error",` +
		`"logging.googleapis.com/operation":{"id":"abcdef1234567890","producer":"sync-worker","first":true,"last":false},` +
		`"message":"failed to acquire lock after 3 retries: context deadline exceeded"}`,
	`ts=2026-08-23T14:22:10.483123456Z caller=collector.go:%d level=error msg="collector failed" ` +
		`name=database duration_seconds=5.2 err="dial tcp: lookup dev-ecosystem-postgres-api-broker.svc.cluster.local on 10.0.0.10:53: no such host"`,
}

// errorBenchLine renders one CRI-formatted error line from a cycling
// template with an embedded varying digit run — fingerprint's digit
// normalization collapses that run, so every occurrence of template i
// normalizes to the SAME fingerprint regardless of the embedded value,
// exercising the realistic "same template, different request" case.
func errorBenchLine(r *rand.Rand, i int) []byte {
	tmpl := errorBenchTemplates[i%len(errorBenchTemplates)]
	content := fmt.Sprintf(tmpl, r.Intn(1_000_000))
	return []byte(fmt.Sprintf("2026-08-22T10:00:00.000000000Z stdout F %s\n", content))
}

// buildErrorBenchLines constructs n error-only CRI lines, cycling through
// errorBenchTemplates (well under DefaultMaxSamplesPerService), for
// BenchmarkErrorLineWithSample — isolates the error branch's own cost
// (fingerprint + recordError) from the ~95%-of-mix non-error path.
func buildErrorBenchLines(n int) [][]byte {
	r := rand.New(rand.NewSource(2))
	lines := make([][]byte, n)
	for i := range lines {
		lines[i] = errorBenchLine(r, i)
	}
	return lines
}

// nonErrorBenchLines returns only the plain/JSON-info/WARN lines from a
// buildBenchLines-shaped mix (i.e. every category except the 5% error one) —
// used to prove the non-error path allocates nothing, isolated from the
// error branch's own (expected, bounded) allocations.
func nonErrorBenchLines(n int) [][]byte {
	full := buildBenchLines(n)
	out := full[:0:0]
	for i, line := range full {
		if i%20 >= 19 {
			continue
		}
		out = append(out, line)
	}
	return out
}

// BenchmarkPerLineHotPath exercises the full per-line path exactly as
// tailer.drain calls it for a complete line: CRI-prefix skip (splitCRI) + EOL
// trim + classify (literal prefilter, then regex confirm on candidates) +
// the atomic total/error/warn adds, PLUS — for the 5% of lines that classify
// as errors — fingerprint + recordError (docs/adr/0014-log-exporter-in-
// agent-ds.md "Samples" section). Everything EXCEPT the file-read syscall
// itself and the once-per-file (namespace, service) resolution, both of
// which are explicitly outside the per-line hot path this benchmark
// measures. Run with:
//
//	go test ./logexporter/... -run '^$' -bench BenchmarkPerLineHotPath -benchtime=2s -cpu=1
func BenchmarkPerLineHotPath(b *testing.B) {
	const n = 20000
	lines := buildBenchLines(n)

	cls, err := newClassifier(DefaultErrorRegex, DefaultWarnRegex, DefaultSampleMaxLen, DefaultMaxSamplesPerService, DefaultSampleJSONFields)
	if err != nil {
		b.Fatal(err)
	}
	cs := &counterSet{}
	tl := &tailer{}
	tl.counters.Store(cs)
	self := &selfMetrics{}

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		tl.processLine(lines[i%n], cls, self)
	}
	elapsed := time.Since(start)
	b.StopTimer()

	linesPerSec := float64(b.N) / elapsed.Seconds()
	b.ReportMetric(linesPerSec, "lines/sec")

	// Sanity: every line in the mix was attributed and classified (total ==
	// b.N) — a benchmark that silently classified nothing would report an
	// inflated, meaningless rate.
	if got := cs.total.Load(); int(got) != b.N {
		b.Fatalf("total = %d, want %d (every generated line must be counted)", got, b.N)
	}
}

// BenchmarkErrorLineWithSample isolates the error-only path — classify +
// fingerprint + recordError — from the mixed benchmark above. Every line
// cycles through a small, fixed set of templates (well under
// DefaultMaxSamplesPerService), so this measures the realistic steady-state
// cost: a lock-free existing-slot match on nearly every call, not the rare
// new-fingerprint append path.
func BenchmarkErrorLineWithSample(b *testing.B) {
	const n = 4000
	lines := buildErrorBenchLines(n)

	cls, err := newClassifier(DefaultErrorRegex, DefaultWarnRegex, DefaultSampleMaxLen, DefaultMaxSamplesPerService, DefaultSampleJSONFields)
	if err != nil {
		b.Fatal(err)
	}
	cs := &counterSet{}
	tl := &tailer{}
	tl.counters.Store(cs)
	self := &selfMetrics{}

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		tl.processLine(lines[i%n], cls, self)
	}
	elapsed := time.Since(start)
	b.StopTimer()

	linesPerSec := float64(b.N) / elapsed.Seconds()
	b.ReportMetric(linesPerSec, "lines/sec")

	if got := cs.total.Load(); int(got) != b.N {
		b.Fatalf("total = %d, want %d", got, b.N)
	}
	// Sanity: fingerprinting must have stabilized to the small, fixed
	// template count — not one slot per line, which would mean
	// normalization isn't collapsing the embedded varying digits at all.
	if got := len(cs.errorSlotsSnapshot()); got == 0 || got > len(errorBenchTemplates) {
		b.Fatalf("error slots = %d, want a stable count in (0, %d]", got, len(errorBenchTemplates))
	}
}

// BenchmarkNonErrorLinePath measures the ~95%-of-mix path (plain + JSON-info
// + WARN — everything that never calls fingerprint) in isolation, and
// TestNonErrorLinePath_ZeroAllocations (below) hard-asserts it allocates
// nothing — requirement: "the non-error path must be completely untouched"
// by the sample-fingerprinting work added on top of the error path.
func BenchmarkNonErrorLinePath(b *testing.B) {
	const n = 19000
	lines := nonErrorBenchLines(n)

	cls, err := newClassifier(DefaultErrorRegex, DefaultWarnRegex, DefaultSampleMaxLen, DefaultMaxSamplesPerService, DefaultSampleJSONFields)
	if err != nil {
		b.Fatal(err)
	}
	cs := &counterSet{}
	tl := &tailer{}
	tl.counters.Store(cs)
	self := &selfMetrics{}

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		tl.processLine(lines[i%len(lines)], cls, self)
	}
	elapsed := time.Since(start)
	b.StopTimer()

	linesPerSec := float64(b.N) / elapsed.Seconds()
	b.ReportMetric(linesPerSec, "lines/sec")

	if got := len(cs.errorSlotsSnapshot()); got != 0 || cs.errCatchAll.Load() != 0 {
		b.Fatalf("expected zero error attribution on a non-error-only fixture, got slots=%d catchAll=%d", got, cs.errCatchAll.Load())
	}
}

// TestNonErrorLinePath_ZeroAllocations hard-asserts (not just eyeballs
// benchmark output) that processing a non-error line allocates NOTHING —
// testing.AllocsPerRun gives an exact average, unlike -benchmem's rounded
// per-op figure. Covers plain, JSON-info, and WARN lines (WARN classifies
// but never fingerprints — cs.warns.Add(1) only).
func TestNonErrorLinePath_ZeroAllocations(t *testing.T) {
	lines := nonErrorBenchLines(1900)
	if len(lines) == 0 {
		t.Fatal("fixture produced no non-error lines")
	}
	cls, err := newClassifier(DefaultErrorRegex, DefaultWarnRegex, DefaultSampleMaxLen, DefaultMaxSamplesPerService, DefaultSampleJSONFields)
	if err != nil {
		t.Fatal(err)
	}
	cs := &counterSet{}
	tl := &tailer{}
	tl.counters.Store(cs)
	self := &selfMetrics{}

	i := 0
	avg := testing.AllocsPerRun(1000, func() {
		tl.processLine(lines[i%len(lines)], cls, self)
		i++
	})
	if avg != 0 {
		t.Errorf("non-error line path allocates %.2f allocs/op on average, want 0", avg)
	}
}
