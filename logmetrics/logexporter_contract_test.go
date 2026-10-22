package logmetrics

import (
	"context"
	"sort"
	"testing"

	"github.com/triageagent-dev/agent/logexporter"
	"github.com/triageagent-dev/agent/metricset"
)

// logExporterSet builds one (namespace, service) pair's counters exactly the
// way logexporter.Exporter's own OTLP path does (logexporter/metrics.go's
// buildSet — see logexporter/metrics_test.go), via metricset.NewBuilder and
// metricset.FromOTLP rather than hand-built metricset.Set literals. Built
// from logexporter's EXPORTED metric/label constants, not hand-typed
// strings, so a rename on either side breaks this test at compile time
// (unknown identifier) rather than silently drifting.
//
// errSamples renders one error-counter series per (fingerprint, count) pair
// carrying LabelSample, sorted by fingerprint for reproducible test output,
// plus errCatchAll as an always-present, unlabeled series — matching
// logexporter's real errorSlot model (ADR-0007).
func logExporterSet(namespace, service string, errCatchAll int64, errSamples map[string]int64, warns, total int64) metricset.Set {
	b := metricset.NewBuilder()

	keys := make([]string, 0, len(errSamples))
	for k := range errSamples {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, sample := range keys {
		b.Counter(logexporter.MetricLogErrorsTotal,
			metricset.KV(logexporter.LabelNamespace, namespace, logexporter.LabelService, service, logexporter.LabelSample, sample),
			float64(errSamples[sample]))
	}
	b.Counter(logexporter.MetricLogErrorsTotal,
		metricset.KV(logexporter.LabelNamespace, namespace, logexporter.LabelService, service),
		float64(errCatchAll))
	b.Counter(logexporter.MetricLogWarnsTotal,
		metricset.KV(logexporter.LabelNamespace, namespace, logexporter.LabelService, service),
		float64(warns))
	b.Counter(logexporter.MetricLogLinesTotal,
		metricset.KV(logexporter.LabelNamespace, namespace, logexporter.LabelService, service),
		float64(total))

	return metricset.FromOTLP(b.ResourceMetrics())
}

// TestCollector_ReadsLogExporterContractEndToEnd proves logmetrics.Collector —
// completely unmodified — correctly reads, deltas, gates, and sends a frame from
// a metricset.Set built the same way logexporter's real OTLP snapshot is
// (docs/adr/0015-sensor-stream-grpc.md). This is the reverse-direction pin to
// logexporter/metrics_test.go's own contract test: that one proves the exporter
// EMITS the contract correctly; this one proves the collector CONSUMES it
// correctly.
func TestCollector_ReadsLogExporterContractEndToEnd(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", logExporterSet("ops", "api", 5, nil, 0, 100))
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})

	ctx := context.Background()
	c.runOnce(ctx) // baseline read: first-seen cumulative counts as the delta
	if sender.callCount() != 0 {
		t.Fatalf("after baseline read (delta=5 < threshold=10): callCount = %d, want 0", sender.callCount())
	}

	// errors 5->20 (+15), warns 0->3, total 100->400 (+300) — the exporter's
	// counters are cumulative-since-process-start, so the collector's existing
	// delta logic applies unmodified.
	src.set("sensor/node-a", logExporterSet("ops", "api", 20, nil, 3, 400))
	c.runOnce(ctx)

	svcs := sender.allServices()
	if len(svcs) != 1 {
		t.Fatalf("services sent = %d, want 1", len(svcs))
	}
	got := svcs[0]
	if got.Namespace != "ops" || got.Service != "api" {
		t.Fatalf("service = %+v, want namespace=ops service=api", got)
	}
	if got.ErrorCount != 15 {
		t.Errorf("ErrorCount = %d, want 15", got.ErrorCount)
	}
	if got.WarnCount != 3 {
		t.Errorf("WarnCount = %d, want 3", got.WarnCount)
	}
	if got.TotalCount != 300 {
		t.Errorf("TotalCount = %d, want 300", got.TotalCount)
	}
}

// TestCollector_ReadsLogExporterSampleContractEndToEnd proves the unmodified
// collector — same pickSamples logic (logmetrics/deltas.go, ADR-0007) —
// correctly surfaces the Sample field from logexporter's real multi-series-
// per-fingerprint shape: two distinct error fingerprints firing at (ops, api),
// a THIRD, cold catch-all series always present, and the collector must pick
// the fingerprint with the LARGEST POSITIVE DELTA THIS WINDOW as the frame's
// Sample — "the template actually firing now", not whichever fingerprint has
// the highest lifetime total or was built first (ADR-0007's decision point 5).
func TestCollector_ReadsLogExporterSampleContractEndToEnd(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", logExporterSet("ops", "api", 0, map[string]int64{
		"connection reset by peer": 5,
		"upstream timeout":         2,
	}, 0, 100))
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})

	ctx := context.Background()
	c.runOnce(ctx) // baseline read — establishes prevSamples, no send (delta=7 < threshold=10)
	if sender.callCount() != 0 {
		t.Fatalf("after baseline read: callCount = %d, want 0", sender.callCount())
	}

	// "connection reset by peer" +3 (5->8), "upstream timeout" +18 (2->20) —
	// "upstream timeout" is the larger delta THIS window, even though
	// "connection reset by peer" has the higher lifetime total.
	src.set("sensor/node-a", logExporterSet("ops", "api", 0, map[string]int64{
		"connection reset by peer": 8,
		"upstream timeout":         20,
	}, 3, 400))
	c.runOnce(ctx)

	svcs := sender.allServices()
	if len(svcs) != 1 {
		t.Fatalf("services sent = %d, want 1", len(svcs))
	}
	got := svcs[0]
	if got.ErrorCount != 21 { // (8-5) + (20-2)
		t.Errorf("ErrorCount = %d, want 21 (summed across every fingerprint series, regardless of the sample label)", got.ErrorCount)
	}
	if got.WarnCount != 3 {
		t.Errorf("WarnCount = %d, want 3", got.WarnCount)
	}
	if got.Sample != "upstream timeout" {
		t.Errorf("Sample = %q, want %q (the fingerprint with the largest positive delta this window)", got.Sample, "upstream timeout")
	}
}

// TestCollector_LogExporterCatchAllNeverBecomesTheSample verifies the unlabeled
// catch-all series (logexporter/metrics.go's errCatchAll — lines past the
// per-service fingerprint cap, or an empty fingerprint) never becomes the frame's
// Sample even when it has the largest raw delta: pickSamples skips any series
// whose sample label is absent/empty (logmetrics/deltas.go), so the catch-all
// only ever contributes to ErrorCount, matching ADR-0007's "series without the
// label are ignored".
func TestCollector_LogExporterCatchAllNeverBecomesTheSample(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", logExporterSet("ops", "api", 1, map[string]int64{
		"rare template": 1,
	}, 0, 50))
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})

	ctx := context.Background()
	c.runOnce(ctx)

	// The catch-all's delta (1->500, +499) dwarfs the one fingerprint slot's
	// (1->2, +1) — if pickSamples ever let an unlabeled series win, Sample
	// would come back empty (its sample label value is "").
	src.set("sensor/node-a", logExporterSet("ops", "api", 500, map[string]int64{
		"rare template": 2,
	}, 0, 600))
	c.runOnce(ctx)

	svcs := sender.allServices()
	if len(svcs) != 1 {
		t.Fatalf("services sent = %d, want 1", len(svcs))
	}
	if got := svcs[0].Sample; got != "rare template" {
		t.Errorf("Sample = %q, want %q (the only labeled series — the catch-all must never win)", got, "rare template")
	}
}

// A service the log-exporter counted is not counted again from an OTLP
// source.
func TestCollector_SensorWinsOverOTLPPerService(t *testing.T) {
	src := newFakeSource()
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})
	ctx := context.Background()
	src.set("sensor/node-a", logExporterSet("ops", "api", 5, nil, 0, 100))
	src.set("sensor/otlp/abc", logExporterSet("ops", "api", 5, nil, 0, 100))
	c.runOnce(ctx)
	src.set("sensor/node-a", logExporterSet("ops", "api", 20, nil, 0, 400))
	src.set("sensor/otlp/abc", logExporterSet("ops", "api", 20, nil, 0, 400))
	c.runOnce(ctx)
	svcs := sender.allServices()
	if len(svcs) != 1 || svcs[0].ErrorCount != 15 {
		t.Fatalf("services = %+v, want api with 15 errors, not 30", svcs)
	}
}
