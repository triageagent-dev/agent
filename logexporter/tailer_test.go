package logexporter

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"
)

func newTestTailerFile(t *testing.T, dir string) (path string, cs *counterSet) {
	t.Helper()
	path = filepath.Join(dir, "pod_ns_container-cid.log")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	cs = &counterSet{}
	return path, cs
}

func testClassifier(t *testing.T) *classifier {
	t.Helper()
	cls, err := newClassifier(DefaultErrorRegex, DefaultWarnRegex, DefaultSampleMaxLen, DefaultMaxSamplesPerService, DefaultSampleJSONFields)
	if err != nil {
		t.Fatal(err)
	}
	return cls
}

func criLine(flag byte, content string) string {
	return "2026-08-22T10:00:00.000000000Z stdout " + string(flag) + " " + content + "\n"
}

func appendToFile(t *testing.T, path, s string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteString(s); err != nil {
		t.Fatal(err)
	}
}

func TestTailer_TailsFromEndNeverReadsHistory(t *testing.T) {
	dir := t.TempDir()
	path, cs := newTestTailerFile(t, dir)
	// Pre-existing content BEFORE the tailer ever looks at this file.
	if err := os.WriteFile(path, []byte(criLine('F', "level=error pre-existing")), 0o644); err != nil {
		t.Fatal(err)
	}

	tl := newTailer(path, "ns", "pod", "container", logr.Discard())
	tl.counters.Store(cs)
	cls := testClassifier(t)
	self := &selfMetrics{}

	tl.tick(cls, self) // first tick: opens at END, must not count the pre-existing line

	if got := cs.total.Load(); got != 0 {
		t.Fatalf("total = %d after first tick, want 0 (history must not be read)", got)
	}

	appendToFile(t, path, criLine('F', "level=error fresh"))
	tl.tick(cls, self)

	if got := cs.total.Load(); got != 1 {
		t.Fatalf("total = %d, want 1", got)
	}
	if got := cs.totalErrors(); got != 1 {
		t.Fatalf("errors = %d, want 1", got)
	}
}

func TestTailer_ClassifiesErrorWarnAndTotal(t *testing.T) {
	dir := t.TempDir()
	path, cs := newTestTailerFile(t, dir)
	tl := newTailer(path, "ns", "pod", "container", logr.Discard())
	tl.counters.Store(cs)
	cls := testClassifier(t)
	self := &selfMetrics{}
	tl.tick(cls, self) // establish end-of-empty-file position

	appendToFile(t, path,
		criLine('F', "just some info")+
			criLine('F', "level=error something broke")+
			criLine('F', "level=warn getting close to limit")+
			criLine('F', `{"level":"info","msg":"fine"}`))
	tl.tick(cls, self)

	if got := cs.total.Load(); got != 4 {
		t.Fatalf("total = %d, want 4", got)
	}
	if got := cs.totalErrors(); got != 1 {
		t.Fatalf("errors = %d, want 1", got)
	}
	if got := cs.warns.Load(); got != 1 {
		t.Fatalf("warns = %d, want 1", got)
	}
	// error must not ALSO bump warns, and vice versa.
	appendToFile(t, path, criLine('F', "level=error and level=warn in the same line"))
	tl.tick(cls, self)
	if got := cs.totalErrors(); got != 2 {
		t.Fatalf("errors = %d, want 2 (error takes priority)", got)
	}
	if got := cs.warns.Load(); got != 1 {
		t.Fatalf("warns = %d, want still 1 (must not double count)", got)
	}
}

func TestTailer_SplitLineCountedOnce(t *testing.T) {
	dir := t.TempDir()
	path, cs := newTestTailerFile(t, dir)
	tl := newTailer(path, "ns", "pod", "container", logr.Discard())
	tl.counters.Store(cs)
	cls := testClassifier(t)
	self := &selfMetrics{}
	tl.tick(cls, self)

	// A CRI split sequence for ONE logical line: P, P, F. Must count as exactly
	// one line total, classified from the FIRST chunk's content.
	appendToFile(t, path,
		criLine('P', "level=error part one ")+
			criLine('P', "part two ")+
			criLine('F', "part three"))
	tl.tick(cls, self)

	if got := cs.total.Load(); got != 1 {
		t.Fatalf("total = %d, want 1 for one split logical line", got)
	}
	if got := cs.totalErrors(); got != 1 {
		t.Fatalf("errors = %d, want 1 (classified from the first chunk)", got)
	}

	// A second, unrelated complete line afterward must count normally.
	appendToFile(t, path, criLine('F', "level=warn afterwards"))
	tl.tick(cls, self)
	if got := cs.total.Load(); got != 2 {
		t.Fatalf("total = %d, want 2", got)
	}
	if got := cs.warns.Load(); got != 1 {
		t.Fatalf("warns = %d, want 1", got)
	}
}

func TestTailer_NonCRIFallsBackToWholeLine(t *testing.T) {
	dir := t.TempDir()
	path, cs := newTestTailerFile(t, dir)
	tl := newTailer(path, "ns", "pod", "container", logr.Discard())
	tl.counters.Store(cs)
	cls := testClassifier(t)
	self := &selfMetrics{}
	tl.tick(cls, self)

	appendToFile(t, path, "not a cri formatted line at all, just level=error text\n")
	tl.tick(cls, self)

	if got := cs.total.Load(); got != 1 {
		t.Fatalf("total = %d, want 1", got)
	}
	if got := cs.totalErrors(); got != 1 {
		t.Fatalf("errors = %d, want 1", got)
	}
}

func TestTailer_RotationByRenameAndRecreate(t *testing.T) {
	dir := t.TempDir()
	path, cs := newTestTailerFile(t, dir)
	tl := newTailer(path, "ns", "pod", "container", logr.Discard())
	tl.counters.Store(cs)
	cls := testClassifier(t)
	self := &selfMetrics{}
	tl.tick(cls, self)

	appendToFile(t, path, criLine('F', "level=error before rotation"))
	tl.tick(cls, self)
	if got := cs.totalErrors(); got != 1 {
		t.Fatalf("errors = %d, want 1 before rotation", got)
	}

	// Simulate log rotation: rename the old file away, create a brand-new file
	// at the same path (new inode).
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(criLine('F', "level=error after rotation")), 0o644); err != nil {
		t.Fatal(err)
	}

	tl.tick(cls, self) // must detect the inode change and read the NEW file from its start

	if got := cs.totalErrors(); got != 2 {
		t.Fatalf("errors = %d, want 2 after rotation (new file read from its own start)", got)
	}
}

func TestTailer_TruncationInPlace(t *testing.T) {
	dir := t.TempDir()
	path, cs := newTestTailerFile(t, dir)
	tl := newTailer(path, "ns", "pod", "container", logr.Discard())
	tl.counters.Store(cs)
	cls := testClassifier(t)
	self := &selfMetrics{}
	tl.tick(cls, self)

	appendToFile(t, path, criLine('F', "level=error one")+criLine('F', "level=error two"))
	tl.tick(cls, self)
	if got := cs.totalErrors(); got != 2 {
		t.Fatalf("errors = %d, want 2", got)
	}

	// copytruncate-style rotation: same inode, file truncated then re-written
	// from empty.
	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(criLine('F', "level=warn after truncate")), 0o644); err != nil {
		t.Fatal(err)
	}

	tl.tick(cls, self)

	if got := cs.warns.Load(); got != 1 {
		t.Fatalf("warns = %d, want 1 after truncation recovery", got)
	}
}

// TestTailer_ExemplarShowsRawValues pins round 5's core fix
// (docs/adr/0014-log-exporter-in-agent-ds.md's identity/display split): the
// Prometheus `sample` label must carry the REAL duration from the first
// occurrence, not the masked fingerprint ("slot wait timed out after
// #ms") — the bug this round exists to fix. A second line with the SAME
// template but a DIFFERENT embedded value must land in the SAME slot
// (identity is still masked/deduped) and bump its count, while the
// exemplar — captured only at slot creation — stays the first line's text.
func TestTailer_ExemplarShowsRawValues(t *testing.T) {
	dir := t.TempDir()
	path, cs := newTestTailerFile(t, dir)
	tl := newTailer(path, "ns", "pod", "container", logr.Discard())
	tl.counters.Store(cs)
	cls := testClassifier(t)
	self := &selfMetrics{}
	tl.tick(cls, self)

	appendToFile(t, path, criLine('F', "ERROR: slot wait timed out after 30000ms"))
	tl.tick(cls, self)

	slots := cs.errorSlotsSnapshot()
	if len(slots) != 1 {
		t.Fatalf("expected exactly 1 slot, got %d: %+v", len(slots), slots)
	}
	want := "ERROR: slot wait timed out after 30000ms"
	if slots[0].exemplar != want {
		t.Errorf("exemplar = %q, want %q (raw value, not the masked fingerprint)", slots[0].exemplar, want)
	}
	if slots[0].fingerprint == want {
		t.Errorf("fingerprint = %q, want it masked (distinct from the raw exemplar)", slots[0].fingerprint)
	}

	// Same template, different embedded duration — must absorb into the
	// SAME slot (unchanged identity behavior), and the exemplar must NOT be
	// overwritten by the second line's value.
	appendToFile(t, path, criLine('F', "ERROR: slot wait timed out after 45000ms"))
	tl.tick(cls, self)

	slots = cs.errorSlotsSnapshot()
	if len(slots) != 1 {
		t.Fatalf("expected still exactly 1 slot (same fingerprint absorbs), got %d: %+v", len(slots), slots)
	}
	if slots[0].exemplar != want {
		t.Errorf("exemplar changed on repeat = %q, want unchanged %q (exemplar is captured once, at slot creation)", slots[0].exemplar, want)
	}
	if got := slots[0].count.Load(); got != 2 {
		t.Errorf("count = %d, want 2", got)
	}
}

// TestTailer_ExemplarPreservesRawValuesFromLogfmtExtraction drives the exact
// round-4 live production logfmt sample (logfmtextract_test.go's
// TestExtractErrorText_LogfmtTable) through the full tailer path and checks
// the composed "msg | err" exemplar keeps its REAL values (the IP:port,
// the hostname) rather than the masked fingerprint form.
func TestTailer_ExemplarPreservesRawValuesFromLogfmtExtraction(t *testing.T) {
	dir := t.TempDir()
	path, cs := newTestTailerFile(t, dir)
	tl := newTailer(path, "ns", "pod", "container", logr.Discard())
	tl.counters.Store(cs)
	cls := testClassifier(t)
	self := &selfMetrics{}
	tl.tick(cls, self)

	line := `ts=2026-08-23T14:22:10.483123456Z caller=collector.go:142 level=error msg="collector failed" ` +
		`name=database duration_seconds=5.2 err="dial tcp: lookup dev-ecosystem-postgres-api-broker.svc.cluster.local on 10.0.0.10:53: no such host"`
	appendToFile(t, path, criLine('F', line))
	tl.tick(cls, self)

	slots := cs.errorSlotsSnapshot()
	if len(slots) != 1 {
		t.Fatalf("expected exactly 1 slot, got %d: %+v", len(slots), slots)
	}
	want := "collector failed | dial tcp: lookup dev-ecosystem-postgres-api-broker.svc.cluster.local on 10.0.0.10:53: no such host"
	if slots[0].exemplar != want {
		t.Errorf("exemplar = %q, want %q (raw values preserved end to end)", slots[0].exemplar, want)
	}
}

func TestTailer_UnattributableCountedNowhereButSelfMetricsStillMove(t *testing.T) {
	dir := t.TempDir()
	path, _ := newTestTailerFile(t, dir)
	tl := newTailer(path, "ns", "pod", "container", logr.Discard())
	// Deliberately never call tl.counters.Store — unattributable file.
	cls := testClassifier(t)
	self := &selfMetrics{}
	tl.tick(cls, self)

	appendToFile(t, path, criLine('F', "level=error nobody owns this"))
	tl.tick(cls, self)

	if got := self.linesProcessed.Load(); got != 1 {
		t.Fatalf("linesProcessed = %d, want 1 (self-metrics track throughput regardless of attribution)", got)
	}
	if tl.counters.Load() != nil {
		t.Fatal("counters must stay nil for an unattributable file")
	}
}

// totalErrors sums every error slot's counter plus the catch-all, what
// logmetrics.Collector's sumByServicePair computes on the read side.
func (cs *counterSet) totalErrors() uint64 {
	sum := cs.errCatchAll.Load()
	for _, s := range cs.errorSlotsSnapshot() {
		sum += s.count.Load()
	}
	return sum
}
