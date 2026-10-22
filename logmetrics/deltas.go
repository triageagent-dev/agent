package logmetrics

import (
	"regexp"
	"sort"

	"github.com/triageagent-dev/agent/logexporter"
	"github.com/triageagent-dev/agent/metricset"
)

// delta is one (namespace, service) pair's counter movement for one sensor's
// set, relative to that same sensor's previous set.
type delta struct {
	errorDelta int64
	warnDelta  int64
	totalDelta int64
	sample     string   // value of the qualifying pair's fastest-growing error series' SampleLabel this window
	samples    []string // up to push.MaxLogMetricsSamples distinct sample lines, fastest-growing first; samples[0] == sample
}

// sampleSeries identifies one error-counter series that carries a sample:
// the (namespace, service) pair plus the sample text itself. With the chart's
// fingerprint filter each distinct error template is its own series, so the
// per-series delta says which template is firing NOW.
type sampleSeries struct {
	pair   servicePair
	sample string
}

// computeDeltas reads the log-exporter's three counters from one sensor's
// set, aggregates them per (namespace, service), and returns the delta
// against that same sensor's previous set.
//
// Deltas are computed per sensor (keyed by target), not globally, so that:
//   - a sensor restart resetting its counters to 0 produces a negative raw delta, which
//     is treated as "current value is the delta" (the counter's lifetime-since-restart
//     total), instead of silently going negative or being dropped;
//   - one sensor coming or going never perturbs another sensor's baseline.
//
// The current set's absolute values replace the stored snapshot unconditionally
// (whether this was a reset or a normal increment), so the next call always diffs
// against exactly this set.
func (c *Collector) computeDeltas(target string, set metricset.Set) map[servicePair]delta {
	cur := targetCounters{
		logexporter.MetricLogErrorsTotal: sumByServicePair(set.Get(logexporter.MetricLogErrorsTotal)),
		logexporter.MetricLogWarnsTotal:  sumByServicePair(set.Get(logexporter.MetricLogWarnsTotal)),
		logexporter.MetricLogLinesTotal:  sumByServicePair(set.Get(logexporter.MetricLogLinesTotal)),
	}

	prev := c.prev[target]
	out := make(map[servicePair]delta)
	mergeMetric := func(metricName string, assign func(*delta, int64)) {
		for pair, v := range cur[metricName] {
			d := out[pair]
			assign(&d, counterDelta(prev[metricName][pair], v))
			out[pair] = d
		}
	}
	mergeMetric(logexporter.MetricLogErrorsTotal, func(d *delta, v int64) { d.errorDelta = v })
	mergeMetric(logexporter.MetricLogWarnsTotal, func(d *delta, v int64) { d.warnDelta = v })
	mergeMetric(logexporter.MetricLogLinesTotal, func(d *delta, v int64) { d.totalDelta = v })

	bestSamples, topSamples, curSamples := pickSamples(set.Get(logexporter.MetricLogErrorsTotal), c.prevSamples[target])
	for pair, sample := range bestSamples {
		d := out[pair]
		d.sample = sample
		d.samples = topSamples[pair]
		out[pair] = d
	}
	c.prevSamples[target] = curSamples

	c.prev[target] = cur
	return out
}

// ansiEscape matches an ANSI CSI escape sequence (colour codes and the like).
var ansiEscape = regexp.MustCompile(`\x1b\[[0-9;]*[A-Za-z]`)

// stripANSI removes ANSI escape sequences from s, so a coloured log line
// reads as plain text on the wire.
func stripANSI(s string) string {
	return ansiEscape.ReplaceAllString(s, "")
}

// maxSamples is how many distinct sample lines are kept per pair.
const maxSamples = 3

// pickSamples returns, per (namespace, service) pair, the sample of the error
// series with the largest positive delta since prev — the error template
// actually firing this window — falling back to the first non-empty sample in
// point order when no sampled series moved (e.g. the pair qualified on warns
// alone). top holds, per pair, up to three distinct samples ordered by delta
// descending (ties in point order); when no series moved it is the fallback
// alone, and top[pair][0] == best[pair] always. Every sample is ANSI-stripped
// before it is used, so two series differing only in colour codes are one.
// Series without a sample are ignored. Also returns the current per-series
// values, which the caller stores as the next baseline.
func pickSamples(m *metricset.Metric, prev map[sampleSeries]float64) (best map[servicePair]string, top map[servicePair][]string, cur map[sampleSeries]float64) {
	best = make(map[servicePair]string)
	top = make(map[servicePair][]string)
	cur = make(map[sampleSeries]float64)
	if m == nil {
		return best, top, cur
	}

	type ranked struct {
		sample string
		delta  int64
	}
	type pairBest struct {
		fallback string   // first non-empty sample seen for this pair, in family order
		series   []string // distinct samples, in first-seen point order
	}
	pairs := make(map[servicePair]*pairBest)

	// Iterate in slice order (not map order) so "first in point order" is
	// well-defined for both the fallback and the tie-break below.
	for _, p := range m.Points {
		sample := stripANSI(p.Attr(logexporter.LabelSample))
		if sample == "" {
			continue
		}
		pair := pairOf(p)
		series := sampleSeries{pair: pair, sample: sample}
		if _, seen := cur[series]; !seen {
			pb := pairs[pair]
			if pb == nil {
				pb = &pairBest{fallback: sample}
				pairs[pair] = pb
			}
			pb.series = append(pb.series, sample)
		}
		cur[series] += p.Value
	}

	for pair, pb := range pairs {
		moved := make([]ranked, 0, len(pb.series))
		for _, s := range pb.series {
			series := sampleSeries{pair: pair, sample: s}
			if d := counterDelta(prev[series], cur[series]); d > 0 {
				moved = append(moved, ranked{sample: s, delta: d})
			}
		}
		sort.SliceStable(moved, func(i, j int) bool { return moved[i].delta > moved[j].delta })
		if len(moved) == 0 {
			best[pair] = pb.fallback
			top[pair] = []string{pb.fallback}
			continue
		}
		if len(moved) > maxSamples {
			moved = moved[:maxSamples]
		}
		lines := make([]string, len(moved))
		for i, r := range moved {
			lines[i] = r.sample
		}
		best[pair] = lines[0]
		top[pair] = lines
	}
	return best, top, cur
}

func pairOf(p metricset.Point) servicePair {
	return servicePair{Namespace: p.Attr(logexporter.LabelNamespace), Service: p.Attr(logexporter.LabelService)}
}

// counterDelta returns cur-prev, except when that would be negative (a counter reset
// — the log-exporter sensor restarting, or the series is new this read with prev==0
// already handled by the zero value) in which case cur itself is treated as the delta: the
// counter has been accumulating since the reset, so its current value IS the count of
// events since then, none of which were reported yet.
func counterDelta(prev, cur float64) int64 {
	d := cur - prev
	if d < 0 {
		d = cur
	}
	return int64(d)
}

// sumByServicePair sums every point of a metric by its (namespace, service).
// m may be nil (absent from this set), in which case an empty map is
// returned.
func sumByServicePair(m *metricset.Metric) map[servicePair]float64 {
	out := make(map[servicePair]float64)
	if m == nil {
		return out
	}
	for _, p := range m.Points {
		pair := pairOf(p)
		if pair.Namespace == "" && pair.Service == "" {
			continue // point carries neither identifying attribute; nothing to attribute it to
		}
		out[pair] += p.Value
	}
	return out
}
