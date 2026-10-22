package logexporter

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/triageagent-dev/agent/metricset"
)

// OTLP metric names this exporter emits and logmetrics.Collector reads
// (docs/adr/0015-sensor-stream-grpc.md). All three are cumulative counters
// per (namespace, service); the error counter also splits by sample.
const (
	MetricLogErrorsTotal = "log.errors"
	MetricLogWarnsTotal  = "log.warns"
	MetricLogLinesTotal  = "log.lines"

	LabelNamespace = "k8s.namespace.name"
	LabelService   = "service.name"
	// LabelSample carries a RAW EXEMPLAR of the first log line that minted
	// this error series (round 5, docs/adr/0014-log-exporter-in-agent-ds.md's
	// identity/display split) — the masked+truncated fingerprint is still
	// what SELECTS the series (errorSlot.fingerprint, the map key delta math
	// depends on), but it is no longer what a human sees: this label carries
	// real values (durations, hostnames, counts) instead of `#` placeholders.
	// ERROR series only, never warn/total.
	LabelSample = "log.sample"
)

// Self-metrics: this exporter's own throughput/health, sent with the same
// snapshot so capacity is measurable in production rather than merely asserted
// (docs/adr/0014-log-exporter-in-agent-ds.md "Capacity" section). Not read by
// logmetrics.Collector (it only reads the three counters above).
const (
	MetricLinesProcessedTotal = "log.exporter.lines.processed"
	MetricBytesProcessedTotal = "log.exporter.bytes.processed"
	MetricFilesTracked        = "log.exporter.files.tracked"
	MetricLinesDroppedTotal   = "log.exporter.lines.dropped"
)

// errorSlot is ONE distinct error fingerprint's own permanent, monotonic
// Prometheus series for a (namespace, service) pair — never evicted, never
// relabeled, only ever incremented (docs/adr/0007-error-log-sample-
// fingerprint.md's model, restored natively — ADR-0014's Samples section:
// evicting or renaming a series would reset ITS counter and corrupt
// logmetrics.Collector's delta math, since a Prometheus series identity is
// its full label set including `sample`).
//
// fingerprint is IDENTITY — the masked+truncated template this slot is keyed
// and matched by, unchanged since round 1. exemplar is DISPLAY — round 5's
// split (docs/adr/0014-log-exporter-in-agent-ds.md's Samples section): the
// raw (unmasked) representative text captured ONCE, when the slot is
// created, and never touched again — a later line matching the same
// fingerprint only increments count, exactly like before. exemplar, not
// fingerprint, is what renderMetrics puts in the `sample` label.
type errorSlot struct {
	fingerprint string
	exemplar    string
	count       atomic.Uint64
}

// counterSet is one (namespace, service) pair's cumulative counters. total/
// warns/errCatchAll are atomic — Collect's read (buildMetrics) reads
// them via Load with no lock held, and every tailer goroutine that has this
// counterSet resolved writes them via Add with no lock held. This is what
// makes the hot path (per log line: CRI-prefix skip + classify + one or two
// atomic adds) never contend with either another file's tailer or a
// concurrent collect (docs/adr/0014-log-exporter-in-agent-ds.md "Capacity"
// section, isolation requirement).
//
// Error counting is richer: one errorSlot PER DISTINCT fingerprint, up to
// maxSamplesPerService (see recordError) — errSlots is a copy-on-write
// []*errorSlot behind an atomic.Pointer, so a lookup that finds an existing
// slot (the steady-state common case once a service's error templates have
// stabilized) is entirely lock-free: one Load of the current slice, a short
// linear scan (bounded by the cap), one atomic Add. errMu guards only the
// RARE path — appending a genuinely new fingerprint's slot — so the hot path
// never takes it.
type counterSet struct {
	total atomic.Uint64
	warns atomic.Uint64

	errMu       sync.Mutex
	errSlots    atomic.Pointer[[]*errorSlot]
	errCatchAll atomic.Uint64 // unmatched-after-cap (or empty-fingerprint) error lines; NO sample label
}

// recordError attributes one error line's fingerprint fp to its series: an
// existing slot's counter if fp already has one, a brand-new slot if fp is
// genuinely new and the (namespace, service) pair hasn't yet reached
// maxSlots distinct fingerprints, or the unlabeled catch-all otherwise. An
// empty fingerprint (the line normalized away to nothing — e.g. all-digit/
// all-hex/all-whitespace content) also routes straight to the catch-all: a
// slot whose sample label value is "" would be indistinguishable from "no
// sample label" to logmetrics.Collector's pickSamples (which explicitly
// skips sample=="" series), so giving it its own slot would just waste a
// cap slot for no benefit.
//
// text/sampleMaxLen are ONLY consulted when a genuinely new slot is about to
// be minted (round 5's exemplar capture, "at slot creation only" —
// docs/adr/0014-log-exporter-in-agent-ds.md's Samples section) — the
// steady-state fast path (an existing fingerprint, both the lock-free read
// above and the re-check under the lock) never touches them, so the
// per-line cost of the common case is unchanged from before round 5: this
// is what keeps BenchmarkErrorLineWithSample's steady-state throughput from
// regressing. text is the RAW extracted error text (jsonextract.go/
// logfmtextract.go's output) — callers pass it as []byte so building the
// exemplar (a string conversion + stripLeadingNoise + truncateSample) costs
// nothing unless this call actually reaches the new-slot branch.
func (cs *counterSet) recordError(fp string, text []byte, sampleMaxLen, maxSlots int) {
	if fp == "" {
		cs.errCatchAll.Add(1)
		return
	}
	if slots := cs.errSlots.Load(); slots != nil {
		for _, s := range *slots {
			if s.fingerprint == fp {
				s.count.Add(1)
				return
			}
		}
	}

	cs.errMu.Lock()
	defer cs.errMu.Unlock()
	// Re-check under the lock: another goroutine (a different file/pod
	// mapping to the same service) may have added this exact fingerprint, or
	// filled the cap, between the lock-free read above and now.
	var cur []*errorSlot
	if slots := cs.errSlots.Load(); slots != nil {
		cur = *slots
		for _, s := range cur {
			if s.fingerprint == fp {
				s.count.Add(1)
				return
			}
		}
	}
	if maxSlots > 0 && len(cur) >= maxSlots {
		cs.errCatchAll.Add(1)
		return
	}
	exemplar := truncateSample(redactSample(stripLeadingNoise(string(text))), sampleMaxLen)
	ns := &errorSlot{fingerprint: fp, exemplar: resolveExemplarLabel(cur, exemplar, fp)}
	ns.count.Add(1)
	next := append(append([]*errorSlot(nil), cur...), ns)
	cs.errSlots.Store(&next)
}

// resolveExemplarLabel implements round 5's collision guard
// (docs/adr/0014-log-exporter-in-agent-ds.md's Samples section): two
// DIFFERENT fingerprints in the same (namespace, service) can produce the
// SAME exemplar after truncation (e.g. identical first sampleMaxLen raw
// chars, divergence only past the cut) — left unguarded, that would mint
// two Prometheus series sharing one label set, which Prometheus treats as
// one series with a nondeterministic value. The candidate exemplar is
// preferred; on collision with any EXISTING slot's label in this
// (namespace, service) pair, the new slot falls back to its own masked
// fingerprint instead — guaranteed unique among existing slots' labels
// because fingerprint values are unique by construction (they are the
// dedup key, checked immediately above in recordError); if even THAT
// collides (an existing slot's exemplar happens to equal this fingerprint
// string verbatim — a taxonomy collision, not the common case), " ·2" is
// appended, which is unique because no other slot has ever needed the
// suffix for its own fingerprint. Evaluated once, at creation, so the
// choice is deterministic and stable for the slot's whole lifetime.
func resolveExemplarLabel(existing []*errorSlot, exemplar, fp string) string {
	if !exemplarLabelInUse(existing, exemplar) {
		return exemplar
	}
	if !exemplarLabelInUse(existing, fp) {
		return fp
	}
	return fp + " ·2"
}

func exemplarLabelInUse(existing []*errorSlot, label string) bool {
	for _, s := range existing {
		if s.exemplar == label {
			return true
		}
	}
	return false
}

// errorSlotsSnapshot returns the current, append-only slot slice — safe to
// range over without holding errMu (each slot's own counter is read via
// Load, and the slice itself is never mutated in place, only atomically
// swapped for a longer copy).
func (cs *counterSet) errorSlotsSnapshot() []*errorSlot {
	slots := cs.errSlots.Load()
	if slots == nil {
		return nil
	}
	return *slots
}

// seriesKey identifies one (namespace, service) counter series.
type seriesKey struct {
	namespace string
	service   string
}

// seriesRegistry is the process-wide map from (namespace, service) to its
// counterSet. Resolution happens ONCE per file, at file-open/discovery time (see
// discovery.go/tailer.go) — never per line — so the mutex here is touched at
// file-churn rate (pod starts/stops, directory rescans), not at log-line rate.
type seriesRegistry struct {
	mu   sync.Mutex
	sets map[seriesKey]*counterSet
}

func newSeriesRegistry() *seriesRegistry {
	return &seriesRegistry{sets: make(map[seriesKey]*counterSet)}
}

// getOrCreate returns the counterSet for (namespace, service), creating it on
// first reference. Safe to call concurrently from multiple discovery/rescan
// goroutines; never called from a tailer's per-line hot path.
func (r *seriesRegistry) getOrCreate(namespace, service string) *counterSet {
	k := seriesKey{namespace: namespace, service: service}
	r.mu.Lock()
	defer r.mu.Unlock()
	cs := r.sets[k]
	if cs == nil {
		cs = &counterSet{}
		r.sets[k] = cs
	}
	return cs
}

// snapshot returns every currently-known series, sorted for deterministic
// exposition output (matches this repo's existing determinism discipline:
// nodeexporter.handleMetrics's pod sort, redsource's sample tie-break).
func (r *seriesRegistry) snapshot() []seriesEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]seriesEntry, 0, len(r.sets))
	for k, cs := range r.sets {
		out = append(out, seriesEntry{key: k, cs: cs})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].key.namespace != out[j].key.namespace {
			return out[i].key.namespace < out[j].key.namespace
		}
		return out[i].key.service < out[j].key.service
	})
	return out
}

type seriesEntry struct {
	key seriesKey
	cs  *counterSet
}

// selfMetrics tracks this exporter's own throughput/health. All fields atomic —
// written from tailer/discovery goroutines, read from the exposition handler,
// same lock-free discipline as counterSet.
type selfMetrics struct {
	linesProcessed atomic.Uint64
	bytesProcessed atomic.Uint64
	filesTracked   atomic.Int64
	linesDropped   atomic.Uint64
}

// buildMetrics records the three counters and this exporter's self-metrics
// into b. Every counter is read with an atomic Load: never blocks on, and is
// never blocked by, a tailer goroutine (requirement 1: isolation).
func buildMetrics(b *metricset.Builder, reg *seriesRegistry, self *selfMetrics) {
	for _, e := range reg.snapshot() {
		// One series per distinct fingerprint — each its own permanent,
		// monotonic counter (never evicted/relabeled, see errorSlot's doc
		// comment), plus the catch-all series without a sample, ALWAYS
		// recorded (even at 0) for a stable, predictable shape.
		for _, slot := range e.cs.errorSlotsSnapshot() {
			b.Counter(MetricLogErrorsTotal, metricset.KV(LabelNamespace, e.key.namespace, LabelService, e.key.service, LabelSample, slot.exemplar), float64(slot.count.Load()))
		}
		base := metricset.KV(LabelNamespace, e.key.namespace, LabelService, e.key.service)
		b.Counter(MetricLogErrorsTotal, base, float64(e.cs.errCatchAll.Load()))
		b.Counter(MetricLogWarnsTotal, base, float64(e.cs.warns.Load()))
		b.Counter(MetricLogLinesTotal, base, float64(e.cs.total.Load()))
	}
	b.Counter(MetricLinesProcessedTotal, nil, float64(self.linesProcessed.Load()))
	b.Counter(MetricBytesProcessedTotal, nil, float64(self.bytesProcessed.Load()))
	b.Gauge(MetricFilesTracked, nil, float64(self.filesTracked.Load()))
	b.Counter(MetricLinesDroppedTotal, nil, float64(self.linesDropped.Load()))
}
