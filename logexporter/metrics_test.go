package logexporter

import (
	"fmt"
	"testing"

	"github.com/triageagent-dev/agent/metricset"
)

func buildSet(reg *seriesRegistry, self *selfMetrics) metricset.Set {
	b := metricset.NewBuilder("k8s.node.name", "node-a")
	buildMetrics(b, reg, self)
	return metricset.FromOTLP(b.ResourceMetrics())
}

func TestBuildMetrics_CarriesTheContract(t *testing.T) {
	reg := newSeriesRegistry()
	cs := reg.getOrCreate("ops", "api")
	cs.total.Store(10)
	for i := 0; i < 3; i++ {
		cs.recordError("boom", []byte("boom"), DefaultSampleMaxLen, DefaultMaxSamplesPerService)
	}
	cs.warns.Store(2)

	self := &selfMetrics{}
	self.linesProcessed.Store(100)
	self.bytesProcessed.Store(4096)
	self.filesTracked.Store(5)
	self.linesDropped.Store(1)

	set := buildSet(reg, self)
	for _, name := range []string{
		MetricLogErrorsTotal, MetricLogWarnsTotal, MetricLogLinesTotal,
		MetricLinesProcessedTotal, MetricBytesProcessedTotal, MetricFilesTracked, MetricLinesDroppedTotal,
	} {
		if set.Get(name) == nil {
			t.Errorf("missing metric %q", name)
		}
	}

	// One fingerprint slot (sample="boom", value 3) plus the always-present
	// catch-all without a sample (value 0): two series for this one
	// (namespace, service) pair, matching the ADR-0007 model.
	errs := set.Get(MetricLogErrorsTotal)
	if errs.Kind != metricset.Counter || len(errs.Points) != 2 {
		t.Fatalf("%s = %+v, want 2 counter points", MetricLogErrorsTotal, errs)
	}
	var sawSample, sawCatchAll bool
	for _, p := range errs.Points {
		if p.Attr(LabelNamespace) != "ops" || p.Attr(LabelService) != "api" {
			t.Errorf("point attributes = %v, want ops/api", p.Attrs)
		}
		if p.Attr("k8s.node.name") != "node-a" {
			t.Errorf("resource attribute missing: %v", p.Attrs)
		}
		if sample := p.Attr(LabelSample); sample != "" {
			sawSample = true
			if sample != "boom" || p.Value != 3 {
				t.Errorf("fingerprint series = %v value=%v, want sample=boom value=3", p.Attrs, p.Value)
			}
		} else {
			sawCatchAll = true
			if p.Value != 0 {
				t.Errorf("catch-all value = %v, want 0", p.Value)
			}
		}
	}
	if !sawSample || !sawCatchAll {
		t.Fatalf("sawSample=%v sawCatchAll=%v", sawSample, sawCatchAll)
	}
	if got := set.Get(MetricLogLinesTotal).Points[0].Value; got != 10 {
		t.Errorf("total = %v, want 10", got)
	}
	if got := set.Get(MetricLogWarnsTotal).Points[0].Value; got != 2 {
		t.Errorf("warns = %v, want 2", got)
	}
	if m := set.Get(MetricFilesTracked); m.Kind != metricset.Gauge || m.Points[0].Value != 5 {
		t.Errorf("filesTracked = %+v, want gauge 5", m)
	}
}

// TestBuildMetrics_SampleRoundTripsVerbatim plants a sample holding a quote,
// a backslash and a raw newline: OTLP carries it as a plain string, so it
// must come back exactly.
func TestBuildMetrics_SampleRoundTripsVerbatim(t *testing.T) {
	reg := newSeriesRegistry()
	cs := reg.getOrCreate("ops", "api")
	tricky := "boom: path=\"C:\\data\\log\"\nsecond line after a raw newline"
	cs.recordError(tricky, []byte(tricky), DefaultSampleMaxLen, DefaultMaxSamplesPerService)

	var found bool
	for _, p := range buildSet(reg, &selfMetrics{}).Get(MetricLogErrorsTotal).Points {
		if v := p.Attr(LabelSample); v != "" {
			found = true
			if v != tricky {
				t.Errorf("sample = %q, want %q", v, tricky)
			}
		}
	}
	if !found {
		t.Fatal("no sample series")
	}
}

func TestSeriesRegistry_GetOrCreateIsIdempotent(t *testing.T) {
	reg := newSeriesRegistry()
	a := reg.getOrCreate("ns", "svc")
	b := reg.getOrCreate("ns", "svc")
	if a != b {
		t.Fatal("expected the same *counterSet for the same (namespace, service)")
	}
	c := reg.getOrCreate("ns", "other")
	if a == c {
		t.Fatal("expected a distinct *counterSet for a different service")
	}
}

// TestFingerprint_Table pins fingerprint's normalization behavior: digit
// runs, hex runs >= 8 chars, UUIDs, and whitespace runs all collapse, and
// the result truncates to maxLen.
func TestFingerprint_Table(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		maxLen int
		want   string
	}{
		{
			name: "digit run collapses",
			in:   "request took 12345 ms on port 8080",
			want: "request took # ms on port #",
		},
		{
			name: "short digit runs also collapse (not just long ones)",
			in:   "retry 1 of 3",
			want: "retry # of #",
		},
		{
			name: "hex run >= 8 chars collapses",
			in:   "session deadbeefcafe0123 expired",
			want: "session # expired",
		},
		{
			name: "short hex-looking word under 8 chars is left alone",
			in:   "status abcd failed",
			want: "status abcd failed",
		},
		{
			name: "uuid collapses as one placeholder, not fragments",
			in:   "request 550e8400-e29b-41d4-a716-446655440000 failed",
			want: "request # failed",
		},
		{
			name: "whitespace runs (including embedded newlines) collapse to one space",
			in:   "line one\n\n   line two\ttabbed",
			want: "line one line two tabbed",
		},
		{
			name:   "truncates to maxLen after normalizing",
			in:     "the quick brown fox jumps over the lazy dog 1234567890",
			maxLen: 20,
			want:   "the quick brown fox",
		},
		{
			name:   "maxLen <= 0 disables truncation",
			in:     "abcdefghij123456789",
			maxLen: 0,
			want:   "abcdefghij#",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			maxLen := tc.maxLen
			if maxLen == 0 && tc.name != "maxLen <= 0 disables truncation" {
				maxLen = DefaultSampleMaxLen
			}
			got := fingerprint([]byte(tc.in), maxLen)
			if got != tc.want {
				t.Errorf("fingerprint(%q, %d) = %q, want %q", tc.in, maxLen, got, tc.want)
			}
		})
	}
}

// TestFingerprintIdentityUnchanged pins round 5's core promise
// (docs/adr/0014-log-exporter-in-agent-ds.md's identity/display split):
// reworking stripLeadingNoise to operate on raw text (stripnoise.go) must
// not change what the IDENTITY pipeline — fingerprint() then
// stripLeadingNoise() on the masked result, tailer.go's countLine — produces
// for existing inputs. Each want value here was the fingerprint produced
// before round 5; masking an already-masked token is a no-op, so
// stripLeadingNoise's generalized (mask-aware) noise test degrades to
// exactly its old behavior once the input is already fully masked.
func TestFingerprintIdentityUnchanged(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "plain error line, no leading noise to strip",
			in:   "level=error failed to write to database: connection reset by peer (attempt 42)",
			want: "level=error failed to write to database: connection reset by peer (attempt #)",
		},
		{
			name: "hex run collapses, no leading noise",
			in:   "session deadbeefcafe0123 expired",
			want: "session # expired",
		},
		{
			name: "postgres timestamp preamble strips",
			in:   `2026-08-23 14:22:10.483 UTC [12345] FATAL: role "root" does not exist`,
			want: `FATAL: role "root" does not exist`,
		},
		{
			name: "short digit runs collapse, no leading noise",
			in:   "retry 1 of 3",
			want: "retry # of #",
		},
		{
			name: "uuid collapses, no leading noise",
			in:   "request 550e8400-e29b-41d4-a716-446655440000 failed",
			want: "request # failed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fp := fingerprint([]byte(tc.in), DefaultSampleMaxLen)
			got := stripLeadingNoise(fp)
			if got != tc.want {
				t.Errorf("identity(%q) = %q, want %q (fingerprinted: %q)", tc.in, got, tc.want, fp)
			}
		})
	}
}

// TestCounterSet_ExemplarCollisionFallsBackToFingerprint pins round 5's
// collision guard (docs/adr/0014-log-exporter-in-agent-ds.md's Samples
// section): two DIFFERENT fingerprints in the same (namespace, service)
// whose exemplars coincide after truncation must never mint two series
// sharing one label set. textA/textB share an identical first 17 raw bytes
// (through the trailing space right after "text", well before the AAA/BBB
// divergence) so truncating at maxLen=17 produces the SAME candidate
// exemplar for both — the second slot created must fall back to its own
// fingerprint instead.
func TestCounterSet_ExemplarCollisionFallsBackToFingerprint(t *testing.T) {
	cs := &counterSet{}
	const maxLen = 17
	textA := []byte("same prefix text AAA tail one")
	textB := []byte("same prefix text BBB tail two")

	cs.recordError("fp-a", textA, maxLen, DefaultMaxSamplesPerService)
	cs.recordError("fp-b", textB, maxLen, DefaultMaxSamplesPerService)

	slots := cs.errorSlotsSnapshot()
	if len(slots) != 2 {
		t.Fatalf("expected 2 slots (2 distinct fingerprints), got %d: %+v", len(slots), slots)
	}
	var a, b *errorSlot
	for _, s := range slots {
		switch s.fingerprint {
		case "fp-a":
			a = s
		case "fp-b":
			b = s
		}
	}
	if a == nil || b == nil {
		t.Fatalf("missing expected slot(s): %+v", slots)
	}
	if want := "same prefix text"; a.exemplar != want {
		t.Fatalf("first slot's exemplar = %q, want %q (no collision yet, nothing to fall back from)", a.exemplar, want)
	}
	if want := "fp-b"; b.exemplar != want {
		t.Errorf("second slot's exemplar = %q, want its own fingerprint %q (collision fallback)", b.exemplar, want)
	}
	if a.exemplar == b.exemplar {
		t.Fatal("the two series' sample label values must differ")
	}
}

// TestCounterSet_SeriesStability verifies the same error template repeating
// increments exactly ONE series (one errorSlot), never mints a second one,
// and the slot's own counter is monotonic.
func TestCounterSet_SeriesStability(t *testing.T) {
	cs := &counterSet{}
	for i := 0; i < 50; i++ {
		cs.recordError("connection refused", []byte("connection refused"), DefaultSampleMaxLen, DefaultMaxSamplesPerService)
	}
	slots := cs.errorSlotsSnapshot()
	if len(slots) != 1 {
		t.Fatalf("expected exactly 1 slot for one repeating template, got %d: %+v", len(slots), slots)
	}
	if slots[0].fingerprint != "connection refused" {
		t.Errorf("slot fingerprint = %q, want %q", slots[0].fingerprint, "connection refused")
	}
	if got := slots[0].count.Load(); got != 50 {
		t.Errorf("slot count = %d, want 50", got)
	}
	if cs.errCatchAll.Load() != 0 {
		t.Errorf("catch-all = %d, want 0 (no cap was reached)", cs.errCatchAll.Load())
	}
}

// TestCounterSet_CardinalityCap feeds 9 distinct fingerprints (one more than
// DefaultMaxSamplesPerService=8) at a service that has never seen an error
// before, and verifies: exactly 8 slots are minted (never more), the 9th
// distinct template falls into the catch-all, every slot's counter is
// correct and monotonic, and re-sending an already-capped template still
// hits its OWN existing slot rather than the catch-all.
func TestCounterSet_CardinalityCap(t *testing.T) {
	cs := &counterSet{}
	const maxSlots = 8
	for i := 0; i < 9; i++ {
		fp := fmt.Sprintf("template-%d", i)
		cs.recordError(fp, []byte(fp), DefaultSampleMaxLen, maxSlots)
	}
	slots := cs.errorSlotsSnapshot()
	if len(slots) != maxSlots {
		t.Fatalf("expected exactly %d slots, got %d: %+v", maxSlots, len(slots), slots)
	}
	seen := map[string]bool{}
	for _, s := range slots {
		if s.count.Load() != 1 {
			t.Errorf("slot %q count = %d, want 1", s.fingerprint, s.count.Load())
		}
		seen[s.fingerprint] = true
	}
	for i := 0; i < maxSlots; i++ {
		fp := fmt.Sprintf("template-%d", i)
		if !seen[fp] {
			t.Errorf("expected slot for %q (one of the first %d distinct templates)", fp, maxSlots)
		}
	}
	if seen["template-8"] {
		t.Error("the 9th distinct template must NOT get its own slot past the cap")
	}
	if got := cs.errCatchAll.Load(); got != 1 {
		t.Fatalf("catch-all = %d, want 1 (the 9th distinct template)", got)
	}

	// Re-sending an existing (pre-cap) template must still hit its own slot,
	// not the catch-all — the cap only blocks NEW fingerprints.
	cs.recordError("template-0", []byte("template-0"), DefaultSampleMaxLen, maxSlots)
	if got := cs.errorSlotsSnapshot()[indexOfFingerprint(t, cs, "template-0")].count.Load(); got != 2 {
		t.Errorf("template-0 slot count = %d, want 2", got)
	}
	if got := cs.errCatchAll.Load(); got != 1 {
		t.Errorf("catch-all = %d, want still 1 (re-send of an existing template must not touch it)", got)
	}

	// A further NEW distinct template (past the cap) also falls into the
	// catch-all, and slot count never exceeds the cap.
	cs.recordError("template-9-also-new", []byte("template-9-also-new"), DefaultSampleMaxLen, maxSlots)
	if got := len(cs.errorSlotsSnapshot()); got != maxSlots {
		t.Fatalf("slot count = %d, want it to stay at the cap %d", got, maxSlots)
	}
	if got := cs.errCatchAll.Load(); got != 2 {
		t.Errorf("catch-all = %d, want 2", got)
	}
}

func indexOfFingerprint(t *testing.T, cs *counterSet, fp string) int {
	t.Helper()
	for i, s := range cs.errorSlotsSnapshot() {
		if s.fingerprint == fp {
			return i
		}
	}
	t.Fatalf("no slot found for fingerprint %q", fp)
	return -1
}

// TestCounterSet_EmptyFingerprintGoesToCatchAll verifies a line that
// normalizes away to nothing (all digits/hex/whitespace) never mints a slot
// with an empty sample label — pickSamples on the read side would silently
// ignore such a series anyway, so giving it a slot would waste a cap entry.
func TestCounterSet_EmptyFingerprintGoesToCatchAll(t *testing.T) {
	cs := &counterSet{}
	cs.recordError("", nil, DefaultSampleMaxLen, DefaultMaxSamplesPerService)
	if got := len(cs.errorSlotsSnapshot()); got != 0 {
		t.Fatalf("expected no slot for an empty fingerprint, got %d", got)
	}
	if got := cs.errCatchAll.Load(); got != 1 {
		t.Fatalf("catch-all = %d, want 1", got)
	}
}
