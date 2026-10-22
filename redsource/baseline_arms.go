package redsource

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/triageagent-dev/agent/baseline"
	"github.com/triageagent-dev/agent/internal/agentmetrics"
	"github.com/triageagent-dev/agent/push"
)

// BaselineOptions configures the baseline arms (docs/todo/baseline-gates.md):
// each tick the service's, and each of its operations', error rate and p99 are
// observed into per-key trackers (package baseline), and a key breaches when
// its value over the last EvidenceWindows ticks (summed until the evidence
// minimum is reached) is at least Multiplier x the median of its own recent
// non-breaching readings and over a floor. Zero numeric fields take the
// documented default.
type BaselineOptions struct {
	// Enabled switches the baseline arms on; nil means on (default). It is a
	// pointer so a zero BaselineOptions{} is enabled while a caller can still
	// pass a false.
	Enabled *bool
	// EvidenceWindows is how many ticks of data one verdict may sum
	// (default 5; capped at baseline.MaxWindows).
	EvidenceWindows int
	// MinBaselineWindows is how many baseline readings a key needs before it is
	// judged against its median (default 5). Fewer is cold: the key is judged
	// against ErrColdFloorPct or LatColdFloorMs instead.
	MinBaselineWindows int
	// Multiplier: breach needs observed >= Multiplier x baseline (default 3).
	Multiplier float64
	// ErrMinRequests is the summed requests an error verdict needs (default 20).
	ErrMinRequests int64
	// ErrMinErrors is the summed errors an error verdict needs (default 3).
	ErrMinErrors int64
	// ErrFloorPct is the minimum error rate, in percent, for a warm key
	// (default 5).
	ErrFloorPct float64
	// LatMinRequests is the summed requests a latency verdict needs
	// (default 10).
	LatMinRequests int64
	// LatFloorMs is the minimum p99 in milliseconds (default 300).
	LatFloorMs float64

	// ErrColdFloorPct and LatColdFloorMs are the conservative absolute arms a
	// key with NO baseline yet is judged against (defaults 15 and 5000; never
	// under the warm floor). Baselines are in memory, so every key is cold
	// after an agent restart: a cold reading at or above the cold floor is
	// reported and is NOT learned, so a restart during an outage reports the
	// outage instead of taking it for normal. A cold reading under it is
	// learned as usual. They are higher than the warm floors on purpose: with
	// no history, 5% errors could be a chronic rate. A key that stays at or
	// above its cold floor never forms a baseline and keeps firing, like the
	// absolute gates. Negative disables the arm (a cold key is then silent
	// and everything it reads is learned).
	ErrColdFloorPct float64
	LatColdFloorMs  float64
	// ErrSparseMinErrors and ErrSparseRatePct are the low-volume rule: a key
	// that does not reach ErrMinRequests over all EvidenceWindows (under about
	// 4 requests a minute by default) still breaches when it has at least
	// ErrSparseMinErrors errors and a failure share of at least
	// ErrSparseRatePct percent over them (defaults 5 and 50). Negative
	// ErrSparseMinErrors disables the rule.
	ErrSparseMinErrors int64
	ErrSparseRatePct   float64
	// LatChronicMinRequests is the summed requests a breaching p99 needs to
	// count toward chronic learning (default 100): a p99 over about ten
	// requests is their maximum, so a slow request every few windows would
	// otherwise get the key muted as chronically spiky. A breach on fewer
	// requests is still reported. Negative disables the check.
	LatChronicMinRequests int64
	// MaxOperations caps how many operations of one service are tracked one by
	// one (default 100); the rest are judged together as one overflow
	// operation. Negative removes the cap.
	MaxOperations int
}

func (b *BaselineOptions) setDefaults() {
	if b.EvidenceWindows <= 0 {
		b.EvidenceWindows = 5
	}
	if b.EvidenceWindows > baseline.MaxWindows {
		b.EvidenceWindows = baseline.MaxWindows
	}
	if b.MinBaselineWindows <= 0 {
		b.MinBaselineWindows = 5
	}
	if b.Multiplier <= 0 {
		b.Multiplier = 3
	}
	if b.ErrMinRequests <= 0 {
		b.ErrMinRequests = 20
	}
	if b.ErrMinErrors <= 0 {
		b.ErrMinErrors = 3
	}
	if b.ErrFloorPct <= 0 {
		b.ErrFloorPct = 5
	}
	if b.LatMinRequests <= 0 {
		b.LatMinRequests = 10
	}
	if b.LatFloorMs <= 0 {
		b.LatFloorMs = 300
	}
	if b.ErrColdFloorPct == 0 {
		b.ErrColdFloorPct = 15
	}
	if b.ErrColdFloorPct > 0 && b.ErrColdFloorPct < b.ErrFloorPct {
		b.ErrColdFloorPct = b.ErrFloorPct
	}
	if b.LatColdFloorMs == 0 {
		b.LatColdFloorMs = 5000
	}
	if b.LatColdFloorMs > 0 && b.LatColdFloorMs < b.LatFloorMs {
		b.LatColdFloorMs = b.LatFloorMs
	}
	if b.ErrSparseMinErrors == 0 {
		b.ErrSparseMinErrors = 5
	}
	if b.ErrSparseRatePct <= 0 {
		b.ErrSparseRatePct = 50
	}
	if b.LatChronicMinRequests == 0 {
		b.LatChronicMinRequests = 100
	}
	if b.MaxOperations == 0 {
		b.MaxOperations = 100
	}
}

func (b BaselineOptions) enabled() bool { return b.Enabled == nil || *b.Enabled }

// newTracker returns a Tracker, or nil when the baseline arms are off.
func newTracker(b BaselineOptions) *baseline.Tracker {
	if !b.enabled() {
		return nil
	}
	return baseline.New()
}

func (c *Collector) baselineOn() bool { return c.opts.Baseline.enabled() }

// errGate / latGate build the baseline.Gate for each arm.
func (c *Collector) errGate() baseline.Gate {
	b := c.opts.Baseline
	return baseline.Gate{
		Windows: b.EvidenceWindows, WindowSec: int(c.opts.Interval / time.Second),
		MinEvidence: float64(b.ErrMinRequests), MinNum: float64(b.ErrMinErrors),
		MinBaseline: b.MinBaselineWindows, Multiplier: b.Multiplier,
		Floor: b.ErrFloorPct, Value: baseline.Percent,
		// Negative (disabled) values are a zero Gate field: the arm is off.
		ColdFloor:    max(b.ErrColdFloorPct, 0),
		SparseMinNum: float64(max(b.ErrSparseMinErrors, 0)), SparseFloor: b.ErrSparseRatePct,
	}
}

func (c *Collector) latGate() baseline.Gate {
	b := c.opts.Baseline
	return baseline.Gate{
		Windows: b.EvidenceWindows, WindowSec: int(c.opts.Interval / time.Second),
		MinEvidence: float64(b.LatMinRequests),
		MinBaseline: b.MinBaselineWindows, Multiplier: b.Multiplier,
		Floor: b.LatFloorMs, Scale: 1000,
		ColdFloor:          max(b.LatColdFloorMs, 0),
		ChronicMinEvidence: float64(max(b.LatChronicMinRequests, 0)),
	}
}

// judgeErrors judges one error key and settles it. A cold key (no baseline
// yet) is judged against ErrColdFloorPct, so a brand-new operation failing
// outright, or an outage in progress when the agent started, fires without a
// baseline and is not learned, while a chronic stream under the cold floor
// stays silent until its baseline forms. A key that keeps breaching while cold
// never forms a baseline (breaches are not settled), so a chronic stream at or
// above the cold floor keeps firing, like the absolute gates. A key with too
// few requests to judge fires on the low-volume rule (ErrSparseMinErrors).
func (c *Collector) judgeErrors(t *baseline.Tracker, arm, key, op string) baseline.Verdict {
	v := t.Judge(key, c.errGate())
	c.settle(t, arm, key, op, v)
	return v
}

func (c *Collector) judgeLatency(t *baseline.Tracker, arm, key, op string) baseline.Verdict {
	v := t.JudgeQuantile(key, c.latGate(), 0.99)
	c.settle(t, arm, key, op, v)
	return v
}

// settle settles the verdict and logs a change of the key's muted state, so a
// mute is never silent: the log names the key when it starts and when it ends,
// and agentmetrics.BaselineMutedKeys counts the muted keys.
func (c *Collector) settle(t *baseline.Tracker, arm, key, op string, v baseline.Verdict) {
	svc, _, _ := strings.Cut(key, "\x00")
	switch t.Settle(key, v) {
	case baseline.MuteStarted:
		c.log.Info("baseline key muted: its breaches recur, so it now reports every few windows instead of every window",
			"arm", arm, "key", svc, "operation", op, "observed", v.Observed, "median", v.Baseline, "reference", v.Reference,
			"reportEveryWindows", baseline.StillBreachingEvery)
	case baseline.MuteEnded:
		c.log.Info("baseline key no longer muted: its breaches stopped recurring",
			"arm", arm, "key", svc, "operation", op)
	}
}

// baselineHit is one service's strongest baseline-arm breach for a window.
type baselineHit struct {
	arm         string // push.Breach*
	op          string
	observed    float64
	baseline    float64
	evidenceSec int
	// still marks the periodic report of a muted key (Verdict.StillBreaching).
	still bool
}

// hitFrom builds a hit from a breaching verdict.
func hitFrom(arm, op string, v baseline.Verdict) *baselineHit {
	return &baselineHit{arm: arm, op: op, observed: v.Observed, baseline: v.Baseline, evidenceSec: v.EvidenceSec, still: v.StillBreaching}
}

var armRank = map[string]int{push.BreachOpErrors: 0, push.BreachErrors: 1, push.BreachOpLatency: 2, push.BreachLatency: 3}

// ratio is how many times over its baseline the hit is (baseline floored at
// 1 so a zero baseline ranks by the observed value).
func (h *baselineHit) ratio() float64 { return h.observed / math.Max(h.baseline, 1) }

// beats reports whether h should replace cur as a service's reported breach:
// a new breach before the periodic report of a muted key, then arm precedence
// op-errors > errors > op-latency > latency, then the larger
// observed/baseline ratio, then the lexically smaller operation.
func (h *baselineHit) beats(cur *baselineHit) bool {
	if cur == nil {
		return true
	}
	if h.still != cur.still {
		return !h.still
	}
	if armRank[h.arm] != armRank[cur.arm] {
		return armRank[h.arm] < armRank[cur.arm]
	}
	if h.ratio() != cur.ratio() {
		return h.ratio() > cur.ratio()
	}
	return h.op < cur.op
}

// describe renders the breach for an otherwise empty Sample.
func (h *baselineHit) describe() string {
	var what string
	switch h.arm {
	case push.BreachErrors, push.BreachOpErrors:
		what = fmt.Sprintf("%.0f%% errors over %ds vs baseline %.0f%%", h.observed, h.evidenceSec, h.baseline)
	default:
		what = fmt.Sprintf("p99 %.0fms over %ds vs baseline %.0fms", h.observed, h.evidenceSec, h.baseline)
	}
	if h.still {
		what += " (chronic, still breaching)"
	}
	if h.op != "" {
		return push.TruncateOperation(h.op) + " " + what
	}
	return what
}

// apply fills the wire fields of a qualifying entry from the hit.
func (h *baselineHit) apply(m *push.ServiceRedMetrics) {
	m.Breach, m.Operation = h.arm, push.TruncateOperation(h.op)
	m.Observed, m.Baseline, m.EvidenceSec = h.observed, h.baseline, h.evidenceSec
	m.StillBreaching = h.still
	if h.arm == push.BreachLatency {
		m.LatencyBreach = true
		if m.BaselineP99Ms == 0 {
			m.BaselineP99Ms = h.baseline
		}
	}
	if m.Sample == "" {
		m.Sample = push.TruncateRedSample(h.describe())
	}
}

// evalBaseline runs the four baseline arms for every attributable service
// read this window and returns each breaching service's strongest breach. It
// observes, judges and settles every key, then garbage-collects and advances
// the window round of all four trackers. Nil when the arms are off.
func (c *Collector) evalBaseline(redTotals map[servicePair]*push.ServiceRedMetrics, redOps map[servicePair]map[string]*opTotals, bucketTotals map[servicePair]map[float64]int64) map[servicePair]*baselineHit {
	if !c.baselineOn() {
		return nil
	}
	var out map[servicePair]*baselineHit
	consider := func(pair servicePair, h *baselineHit, key string) {
		c.log.V(1).Info("baseline breach", "arm", h.arm, "key", key, "operation", h.op,
			"observed", h.observed, "baseline", h.baseline, "evidenceSec", h.evidenceSec)
		if out == nil {
			out = make(map[servicePair]*baselineHit)
		}
		if h.beats(out[pair]) {
			out[pair] = h
		}
	}
	// Deterministic iteration keeps the log order stable.
	pairs := make([]servicePair, 0, len(redTotals))
	for pair, m := range redTotals {
		if m.Service != "" {
			pairs = append(pairs, pair)
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].Namespace != pairs[j].Namespace {
			return pairs[i].Namespace < pairs[j].Namespace
		}
		return pairs[i].Workload < pairs[j].Workload
	})
	for _, pair := range pairs {
		m := redTotals[pair]
		svcKey := pair.Namespace + "/" + pair.Workload
		// One reading per service per window, so a fault that never trips
		// an arm can still be read off the log (requests, errors, p99).
		c.log.V(1).Info("red reading", "key", svcKey, "requests", m.RequestCount, "errors", m.ErrorCount,
			"ops", len(redOps[pair]))
		c.errSvc.Observe(svcKey, float64(m.ErrorCount), float64(m.RequestCount))
		if byLe, ok := bucketTotals[pair]; ok {
			c.latSvc.ObserveBuckets(svcKey, byLe, m.RequestCount)
		}
		if v := c.judgeErrors(c.errSvc, push.BreachErrors, svcKey, ""); v.Breach {
			consider(pair, hitFrom(push.BreachErrors, "", v), svcKey)
		}
		if v := c.judgeLatency(c.latSvc, push.BreachLatency, svcKey, ""); v.Breach {
			consider(pair, hitFrom(push.BreachLatency, "", v), svcKey)
		}
		keys := make([]string, 0, len(redOps[pair]))
		for k := range redOps[pair] {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			op := redOps[pair][k]
			opKey := svcKey + "\x00" + k
			if op.errors > 0 {
				c.log.V(1).Info("red op reading", "key", svcKey, "operation", op.label,
					"requests", op.requests, "errors", op.errors)
			}
			c.errOp.Observe(opKey, float64(op.errors), float64(op.requests))
			if op.buckets != nil {
				c.latOp.ObserveBuckets(opKey, op.buckets, op.requests)
			}
			if v := c.judgeErrors(c.errOp, push.BreachOpErrors, opKey, op.label); v.Breach {
				consider(pair, hitFrom(push.BreachOpErrors, op.label, v), opKey)
			}
			if v := c.judgeLatency(c.latOp, push.BreachOpLatency, opKey, op.label); v.Breach {
				consider(pair, hitFrom(push.BreachOpLatency, op.label, v), opKey)
			}
		}
	}
	eg, lg := c.errGate(), c.latGate()
	c.errSvc.GC(eg)
	c.errOp.GC(eg)
	c.latSvc.GC(lg)
	c.latOp.GC(lg)
	c.errSvc.Tick()
	c.errOp.Tick()
	c.latSvc.Tick()
	c.latOp.Tick()
	for arm, t := range map[string]*baseline.Tracker{
		push.BreachErrors: c.errSvc, push.BreachOpErrors: c.errOp,
		push.BreachLatency: c.latSvc, push.BreachOpLatency: c.latOp,
	} {
		agentmetrics.BaselineMutedKeys.WithLabelValues(agentmetrics.SensorRED, arm).Set(float64(t.Muted()))
	}
	return out
}
