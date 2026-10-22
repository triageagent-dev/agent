// Package baseline judges a metric against the key's own recent normal, over
// enough evidence (docs/todo/baseline-gates.md).
//
// A key (a string the caller builds, e.g. "ns/svc" or "ns/svc\x00op") is
// observed once per collection window as a numerator and a denominator
// (errors/requests, error log lines/1, cpu-seconds/window-seconds), or, for
// latency, as a bucket-count delta map. Judge sums the newest windows until
// there is enough evidence, then compares the resulting value with the median of
// the key's past non-breaching readings. Settle records the reading into that
// baseline unless it breached, so a sustained fault never becomes normal.
//
// A key with no baseline yet (cold) is judged against Gate.ColdFloor, a
// conservative absolute level: a reading at or above it breaches and is not
// settled, so an agent that starts during an outage reports it instead of
// learning it. A key whose breaches recur (chronic) is muted: its usual spikes
// stop breaching every window, but the mute is explicit state (Verdict.Muted,
// Settle's Transition, Tracker.Muted) and bounded, since a muted key that is
// still breaching reports again every StillBreachingEvery windows.
//
// Per window, a collector does, in this order:
//
//	t.Observe(key, num, den)         // or ObserveBuckets, once per key
//	v := t.Judge(key, gate)          // or JudgeQuantile
//	t.Settle(key, v)                 // once per Judge, at most once per Observe
//	...                              // for every key, then
//	t.GC(gate)                       // drop long-absent keys
//	t.Tick()                         // advance the window round
//
// The package is pure: no clock, no I/O, no logging. A Tracker is NOT safe for
// concurrent use; each collector owns one per metric and calls it from its
// single tick goroutine.
package baseline

import (
	"math"
	"sort"
)

// MaxWindows bounds Gate.Windows: a Tracker keeps at most this many window
// samples per key.
const MaxWindows = 16

// absentRoundsFactor times Gate.Windows is how many consecutive rounds a key
// may go unobserved before GC drops it.
const absentRoundsFactor = 4

// Buckets maps a histogram's upper bound (le, +Inf included) to its cumulative
// count delta for one window: the shape of redsource's computeBucketDeltas
// output.
type Buckets map[float64]int64

// Gate configures one arm's judgement. The zero value is not useful; set at
// least MinEvidence, Floor and WindowSec. Zero Windows, Keep, MinBaseline and
// Multiplier take the defaults (5, 30, 5, 3).
type Gate struct {
	// Windows is how many of the newest windows Judge may sum (default 5,
	// capped at MaxWindows).
	Windows int
	// WindowSec is one window's length in seconds; Verdict.EvidenceSec is
	// windows used x WindowSec.
	WindowSec int
	// MinEvidence is the denominator (requests, log lines, cpu-seconds) the
	// summed windows must reach. Judge stops adding windows once it does, so a
	// busy key decides on one window. Below it over every kept window, the
	// verdict is not Ready.
	MinEvidence float64
	// MinNum is a floor on the summed numerator (e.g. errors >= 3): below it
	// there is no breach. Zero disables it.
	MinNum float64
	// MinBaseline is how many baseline readings a key needs before it is judged
	// against its median (default 5). Fewer is "cold".
	MinBaseline int
	// Keep is the baseline ring capacity (default 30).
	Keep int
	// Multiplier: breach needs Observed >= Multiplier x median (default 3).
	Multiplier float64
	// Floor is the absolute minimum Observed for a breach, in the arm's unit.
	Floor float64
	// ColdFloor is the conservative absolute level a cold key (fewer than
	// MinBaseline readings) is judged against: a reading at or above it (and
	// meeting MinNum) breaches and is not settled into the baseline, so an
	// outage in progress when the agent starts is reported, not learned as
	// normal. A cold reading under it is settled as usual. Zero means no cold
	// arm: a cold key never breaches and every ready reading is learned
	// (unless ColdBreach).
	ColdFloor float64
	// ColdBreach is ColdFloor = Floor: a cold key breaches on the floor alone.
	// Ignored when ColdFloor is set.
	ColdBreach bool
	// SparseMinNum and SparseFloor are the low-volume rule. A key whose summed
	// denominator never reaches MinEvidence over all Windows is normally not
	// judged at all; with SparseMinNum > 0 it still breaches when its summed
	// numerator is at least SparseMinNum and its value at least SparseFloor
	// (5 errors and a 50% failure share over the lookback, say). Such a
	// verdict is not Ready, so it is never recorded into the baseline.
	SparseMinNum float64
	SparseFloor  float64
	// ChronicMinEvidence is the summed denominator a breaching reading needs to
	// count toward chronic learning (see chronicWindows). A p99 over about ten
	// requests is their maximum, so one slow request in each of a few windows
	// would otherwise teach the tracker that the key is chronically spiky.
	// A breach under it is still reported. Zero disables the check.
	ChronicMinEvidence float64
	// Value turns the summed numerator and denominator into the observed value.
	// Nil means Ratio. Ignored by JudgeQuantile.
	Value func(num, den float64) float64
	// Scale multiplies the quantile in JudgeQuantile (1000 turns seconds into
	// ms). Zero means 1.
	Scale float64
}

// Ratio is num/den, 0 when den is 0.
func Ratio(num, den float64) float64 {
	if den == 0 {
		return 0
	}
	return num / den
}

// Percent is 100*num/den, 0 when den is 0.
func Percent(num, den float64) float64 { return 100 * Ratio(num, den) }

// Rate is num/den (e.g. cpu-seconds per window-second = cores), 0 when den is 0.
func Rate(num, den float64) float64 { return Ratio(num, den) }

func (g Gate) withDefaults() Gate {
	if g.Windows <= 0 {
		g.Windows = 5
	}
	if g.Windows > MaxWindows {
		g.Windows = MaxWindows
	}
	if g.Keep <= 0 {
		g.Keep = 30
	}
	if g.MinBaseline <= 0 {
		g.MinBaseline = 5
	}
	if g.Multiplier <= 0 {
		g.Multiplier = 3
	}
	if g.Value == nil {
		g.Value = Ratio
	}
	if g.Scale <= 0 {
		g.Scale = 1
	}
	return g
}

// Verdict is one Judge result. Pass it to Settle unchanged.
type Verdict struct {
	// Ready is false when the summed denominator never reached MinEvidence (or
	// the key has no data). A not-ready verdict never breaches and is never
	// recorded into the baseline.
	Ready bool
	// Breach reports a deviation from the baseline (or, when Cold, from
	// Gate.ColdFloor; or, when Sparse, the low-volume rule).
	Breach bool
	// Muted is true when this reading deviates from the key's median but is
	// one of its chronic spikes (see chronicWindows). A muted reading does not
	// breach, except every StillBreachingEvery windows (StillBreaching).
	Muted bool
	// StillBreaching marks a breach that is the periodic report of a muted
	// key: nothing new, but the key is still deviating from its median.
	StillBreaching bool
	// Sparse marks a breach from the low-volume rule (Gate.SparseMinNum):
	// the evidence minimum was not reached. Ready is false.
	Sparse bool
	// Cold is true while the key has fewer than MinBaseline readings.
	Cold bool
	// Observed is the judged value, in the arm's unit.
	Observed float64
	// Baseline is the median of the key's non-breaching readings; 0 when Cold.
	// It is the median for a chronic key too, so that "observed vs baseline"
	// means the same thing on every breach (it is what goes on the wire).
	Baseline float64
	// Reference is what a chronic key was judged against instead of its
	// median: the upper quantile of all its readings, spikes included. 0 when
	// the key is not chronic.
	Reference float64
	// Windows is how many windows were summed.
	Windows int
	// EvidenceSec is Windows x Gate.WindowSec.
	EvidenceSec int

	keep int // Gate.Keep, for Settle
	// raw is the plain rule's verdict (multiplier x median, or the floor on
	// a zero baseline) before any chronic reference: Settle tracks spikes and
	// the chronic test on it, so suppressing a chronic spike does not flip
	// the key back to calm.
	raw bool
	// noChronic is set when the reading had less than Gate.ChronicMinEvidence:
	// if it breached, Settle keeps it out of the chronic test and the spikes.
	noChronic bool
}

// StillBreachingEvery is the interval, in windows, at which a muted key that
// keeps deviating from its median reports again (Verdict.StillBreaching).
const StillBreachingEvery = 10

// Transition is a change of a key's muted state, returned by Settle so the
// caller can log it (this package does no logging).
type Transition int

const (
	// NoTransition: the key's muted state did not change.
	NoTransition Transition = iota
	// MuteStarted: the key became chronic and its usual spikes stop breaching
	// every window from now on.
	MuteStarted
	// MuteEnded: the key is no longer chronic and is judged against its median
	// alone again.
	MuteEnded
)

// sample is one window's observation for a key.
type sample struct {
	round    int
	num, den float64
	buckets  Buckets
}

type keyState struct {
	samples  []sample  // oldest first
	hist     []float64 // baseline readings, oldest first
	lastSeen int       // round of the latest Observe
	gen      int       // bumped by every Observe
	settled  int       // gen of the last Settle
	// spikes holds breaching readings and recent the breach flags of the
	// last chronicWindows settled verdicts: a key that breaches often is
	// chronically spiky, and is judged against what it actually does.
	spikes []float64
	recent []bool
	// muted is the explicit mute state (see Transition) and lastReport the
	// round of the key's latest settled breach, which paces the
	// StillBreaching reports.
	muted      bool
	lastReport int
}

// chronicWindows, chronicShare and chronicQuantile define a chronically
// spiky key: one that breached in at least chronicShare of its last
// chronicWindows settled windows (a p99 that is under 5 ms most minutes and
// 2 s every few, as a low-traffic PHP service shows). Such a key is judged
// against the chronicQuantile of all its readings, breaches included, as well
// as its median: its regular spikes stop firing, a step above them still
// fires. A breach run counts once however long it lasts, so a sustained
// fault (one run) is never taken for chronic; it takes chronicMinRuns
// separate runs.
const (
	chronicWindows  = 20
	chronicShare    = 0.3
	chronicQuantile = 0.9
	chronicMinRuns  = 3
)

// chronic reports whether ks breached in at least chronicShare of its recent
// settled windows.
func (ks *keyState) chronic() bool {
	if len(ks.recent) < chronicWindows/2 {
		return false
	}
	n, runs := 0, 0
	for i, b := range ks.recent {
		if b {
			n++
			if i == 0 || !ks.recent[i-1] {
				runs++
			}
		}
	}
	// A sustained fault is one run of breaches and must keep firing; a
	// chronically spiky key breaches in several separate runs.
	return runs >= chronicMinRuns && float64(n) >= chronicShare*float64(len(ks.recent))
}

// quantileOf returns the q-quantile (nearest rank) of vals; vals is not
// modified.
func quantileOf(vals []float64, q float64) float64 {
	cp := append([]float64(nil), vals...)
	sort.Float64s(cp)
	i := int(q*float64(len(cp)-1) + 0.5)
	return cp[i]
}

// Tracker holds per-key window samples and baselines. The zero value is not
// usable; call New.
type Tracker struct {
	round int
	keys  map[string]*keyState
	muted int // keys with keyState.muted
}

// New returns an empty Tracker.
func New() *Tracker { return &Tracker{keys: make(map[string]*keyState)} }

// Tick advances the window round. Call it once per window, after Judge/Settle
// and GC. Observe calls between two Ticks belong to the same window and are
// merged per key.
func (t *Tracker) Tick() { t.round++ }

// Len is the number of tracked keys.
func (t *Tracker) Len() int { return len(t.keys) }

// Muted is the number of tracked keys that are currently muted (chronic):
// see Transition.
func (t *Tracker) Muted() int { return t.muted }

func (t *Tracker) state(key string) *keyState {
	ks := t.keys[key]
	if ks == nil {
		ks = &keyState{}
		t.keys[key] = ks
	}
	return ks
}

// cur returns this window's sample for ks, creating it. It also bumps the
// generation and trims samples older than MaxWindows rounds.
func (t *Tracker) cur(ks *keyState) *sample {
	ks.gen++
	ks.lastSeen = t.round
	cut := 0
	for cut < len(ks.samples) && ks.samples[cut].round <= t.round-MaxWindows {
		cut++
	}
	ks.samples = ks.samples[cut:]
	if n := len(ks.samples); n > 0 && ks.samples[n-1].round == t.round {
		return &ks.samples[n-1]
	}
	ks.samples = append(ks.samples, sample{round: t.round})
	return &ks.samples[len(ks.samples)-1]
}

// Observe adds this window's numerator and denominator for key. Repeated calls
// for one key within a window add up.
func (t *Tracker) Observe(key string, num, den float64) {
	s := t.cur(t.state(key))
	s.num += num
	s.den += den
}

// ObserveBuckets adds this window's histogram bucket deltas and request count
// for key (see JudgeQuantile). b is copied. Repeated calls within a window add
// up.
func (t *Tracker) ObserveBuckets(key string, b Buckets, requests int64) {
	s := t.cur(t.state(key))
	if s.buckets == nil {
		s.buckets = make(Buckets, len(b))
	}
	for le, n := range b {
		s.buckets[le] += n
	}
	s.den += float64(requests)
}

// windowsOf returns the key's samples within the newest g.Windows rounds,
// newest first.
func (t *Tracker) windowsOf(ks *keyState, g Gate) []sample {
	var out []sample
	for i := len(ks.samples) - 1; i >= 0; i-- {
		s := ks.samples[i]
		if s.round <= t.round-g.Windows {
			break
		}
		out = append(out, s)
	}
	return out
}

// Judge sums num and den over the key's newest windows, newest first, stopping
// as soon as den >= g.MinEvidence, and judges g.Value(sumNum, sumDen).
func (t *Tracker) Judge(key string, g Gate) Verdict {
	g = g.withDefaults()
	ks := t.keys[key]
	if ks == nil {
		return Verdict{}
	}
	var num, den float64
	used := 0
	for _, s := range t.windowsOf(ks, g) {
		num += s.num
		den += s.den
		used++
		if den >= g.MinEvidence {
			break
		}
	}
	return t.verdict(ks, g, used, num, den, g.Value(num, den))
}

// JudgeQuantile is Judge for latency: it sums the key's bucket maps over the
// newest windows until their request count reaches g.MinEvidence, and judges
// the q-quantile (0.99 for p99) of the sum, times g.Scale. A p99 cannot be
// derived from per-window p99s, hence the buckets. MinNum applies to the summed
// request count.
func (t *Tracker) JudgeQuantile(key string, g Gate, q float64) Verdict {
	g = g.withDefaults()
	ks := t.keys[key]
	if ks == nil {
		return Verdict{}
	}
	sum := make(Buckets)
	var den float64
	used := 0
	for _, s := range t.windowsOf(ks, g) {
		for le, n := range s.buckets {
			sum[le] += n
		}
		den += s.den
		used++
		if den >= g.MinEvidence {
			break
		}
	}
	v, ok := Quantile(sum, q)
	vd := t.verdict(ks, g, used, den, den, v*g.Scale)
	if !ok {
		vd.Ready, vd.Breach = false, false
	}
	return vd
}

// verdict applies the evidence, floor, numerator-floor and baseline rules.
func (t *Tracker) verdict(ks *keyState, g Gate, used int, num, den, observed float64) Verdict {
	v := Verdict{keep: g.Keep, Windows: used, EvidenceSec: used * g.WindowSec, Observed: observed}
	if used == 0 {
		return v
	}
	if den < g.MinEvidence {
		// Too few requests over the whole lookback to judge a rate against a
		// baseline. A high failure share with a few errors is still a fault.
		if g.SparseMinNum > 0 && num >= g.SparseMinNum && observed >= g.SparseFloor {
			v.Breach, v.Sparse = true, true
		}
		return v
	}
	v.Ready = true
	v.noChronic = den < g.ChronicMinEvidence
	v.Cold = len(ks.hist) < g.MinBaseline
	if !v.Cold {
		v.Baseline = medianOf(ks.hist)
	}
	if observed < g.Floor || num < g.MinNum {
		return v
	}
	if v.Cold {
		coldFloor := g.ColdFloor
		if coldFloor <= 0 && g.ColdBreach {
			coldFloor = g.Floor
		}
		v.Breach = coldFloor > 0 && observed >= coldFloor
		return v
	}
	v.raw = observed >= g.Multiplier*v.Baseline || (v.Baseline == 0 && observed >= g.Floor)
	if ks.chronic() {
		// Judged against what the key actually does: its median, or the
		// upper quantile of every reading, spikes included, whichever is
		// higher. Reference reports it; Baseline stays the median.
		all := append(append([]float64(nil), ks.hist...), ks.spikes...)
		if ref := quantileOf(all, chronicQuantile); ref > v.Baseline {
			median := v.Baseline
			v.Reference = ref
			switch {
			case !v.raw:
				// Within the key's median: calm.
			case observed > ref && observed >= g.Multiplier*median:
				v.Breach = true // a step above its usual spikes
			default:
				// One of its usual spikes: muted, but not silent for good.
				v.Muted = true
				if t.round-ks.lastReport >= StillBreachingEvery {
					v.Breach, v.StillBreaching = true, true
				}
			}
			return v
		}
	}
	v.Breach = v.raw
	return v
}

// Settle records v.Observed into the key's baseline (capped at the Gate.Keep
// the verdict was judged with) unless v breached or was not ready. It is a
// no-op if the key was already settled since its last Observe, or is unknown,
// so calling it twice for one window cannot double-count. It returns the
// change of the key's muted state, if any, for the caller to log.
func (t *Tracker) Settle(key string, v Verdict) Transition {
	ks := t.keys[key]
	if ks == nil || ks.settled == ks.gen {
		return NoTransition
	}
	ks.settled = ks.gen
	if !v.Ready {
		return NoTransition
	}
	if v.Breach {
		ks.lastReport = t.round
	}
	deviates := v.raw || v.Breach
	if deviates && v.noChronic {
		// Too few samples to say anything about the key's habits: reported,
		// never absorbed, and kept out of the chronic test.
		return NoTransition
	}
	ks.recent = append(ks.recent, v.raw)
	if len(ks.recent) > chronicWindows {
		ks.recent = ks.recent[len(ks.recent)-chronicWindows:]
	}
	if deviates {
		ks.spikes = append(ks.spikes, v.Observed)
		if keep := v.keep; keep > 0 && len(ks.spikes) > keep {
			ks.spikes = ks.spikes[len(ks.spikes)-keep:]
		}
	} else {
		ks.hist = append(ks.hist, v.Observed)
		if keep := v.keep; keep > 0 && len(ks.hist) > keep {
			ks.hist = ks.hist[len(ks.hist)-keep:]
		}
	}
	switch {
	case v.Muted && !ks.muted:
		ks.muted = true
		t.muted++
		return MuteStarted
	case ks.muted && !ks.chronic():
		ks.muted = false
		t.muted--
		return MuteEnded
	}
	return NoTransition
}

// AbsentRounds is how many consecutive rounds a key may go unobserved before
// GC drops it: Windows x 4.
func (g Gate) AbsentRounds() int { return g.withDefaults().Windows * absentRoundsFactor }

// GC drops keys that have gone unobserved for g.Windows x 4 consecutive rounds.
// A shorter gap keeps the key and its baseline, so a low-volume operation that
// is absent from a window or two does not lose its history (unlike a service
// vanishing, which the original latency arm dropped at once). Call it once per
// window, before Tick.
func (t *Tracker) GC(g Gate) {
	limit := g.AbsentRounds()
	for k, ks := range t.keys {
		if t.round-ks.lastSeen >= limit {
			if ks.muted {
				t.muted--
			}
			delete(t.keys, k)
		}
	}
}

// medianOf returns the median of vals (non-empty); the mean of the two middle
// values for an even count. vals is not modified.
func medianOf(vals []float64) float64 {
	cp := append([]float64(nil), vals...)
	sort.Float64s(cp)
	n := len(cp)
	if n%2 == 1 {
		return cp[n/2]
	}
	return (cp[n/2-1] + cp[n/2]) / 2
}

// Quantile linearly interpolates the q-quantile (same unit as the bucket upper
// bounds) of cumulative bucket counts, falling back to the largest finite
// boundary when the crossing lies in +Inf. ok is false when there is nothing to
// compute from. It is redsource's p99FromBucketDeltas generalized over q.
func Quantile(b Buckets, q float64) (v float64, ok bool) {
	var les []float64
	for le := range b {
		if !math.IsInf(le, 1) {
			les = append(les, le)
		}
	}
	if len(les) == 0 {
		return 0, false
	}
	sort.Float64s(les)

	total, hasInf := b[math.Inf(1)]
	if !hasInf {
		total = b[les[len(les)-1]]
	}
	if total <= 0 {
		return 0, false
	}
	target := float64(total) * q

	var prevLe, prevCum float64
	for _, le := range les {
		cum := float64(b[le])
		if cum >= target {
			if cum == prevCum {
				return le, true
			}
			return prevLe + (target-prevCum)/(cum-prevCum)*(le-prevLe), true
		}
		prevLe, prevCum = le, cum
	}
	return les[len(les)-1], true
}
