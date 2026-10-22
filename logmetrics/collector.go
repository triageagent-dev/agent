// Package logmetrics implements the sending side of the log-based metrics pipeline
// (docs/adr/0006-log-based-metrics.md): a periodic loop that reads each sensor's
// log-exporter counters from the sensor stream (docs/adr/0015-sensor-stream-grpc.md),
// sums error/warn/total log-line counters per (namespace, service), computes a delta
// against the previous set from the same sensor, and sends a log_metrics frame (push/protocol.go) through the existing Pusher for
// services whose error or warn delta trips a threshold. Off by default; a no-op
// when push mode itself is disabled, since the frame has nowhere to go.
package logmetrics

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"github.com/triageagent-dev/agent/baseline"
	"github.com/triageagent-dev/agent/internal/agentmetrics"
	"github.com/triageagent-dev/agent/internal/ledger"
	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/push"
)

// Sender is the subset of *push.Pusher the collector needs. A small interface (rather
// than importing *push.Pusher directly into every call site) so tests can fake it, and
// so a nil *push.Pusher (push mode disabled) satisfies it via a plain nil interface
// check in New.
type Sender interface {
	SendLogMetrics(windowStart int64, windowSec int, services []push.ServiceLogMetrics)
}

// Options configures the Collector. All fields have defaults applied in New except
// ServiceNamespace/ServiceName, which are required.
type Options struct {
	// Source delivers each sensor's log-exporter metrics
	// (docs/adr/0015-sensor-stream-grpc.md).
	Source metricset.Source
	// Namespaces, when non-empty, restricts forwarded metrics to samples whose
	// namespace is in this set. Empty forwards every namespace
	// (unchanged default).
	//
	// This is what lets ONE sensor DaemonSet serve several tenants. A push
	// connection carries a single tenant, bound into its signed handshake, so
	// every frame it sends is stamped with that tenant. Without this filter a
	// shared sensor would hand a controller log metrics for namespaces
	// belonging to other tenants and they would be forwarded under this
	// connection's tenant — silently mis-attributed, and classified against the
	// wrong service graph. Set it to the same list as --target-namespaces.
	Namespaces []string
	// NamespaceAllowed, when non-nil, OVERRIDES the static Namespaces set
	// entirely: it is consulted per payload build for every sample's
	// namespace, so it hot-follows dynamic scope changes — main.go wires it
	// to push.Pusher.NamespaceInScope, whose scope union includes namespaces
	// pulled in live by the triageagent.dev/tenant Namespace annotation
	// (push/namespace_scope.go), which a static list frozen at process start
	// can never see. Nil keeps the static Namespaces behavior unchanged
	// (empty list = forward every namespace).
	NamespaceAllowed func(string) bool
	// Interval is how often the collector evaluates the gate. Default 60s.
	Interval time.Duration
	// ErrorThreshold is the minimum errorDelta OR warnDelta for a service to
	// qualify for the frame — either counter crossing it independently qualifies.
	// Default 10.
	ErrorThreshold int64
	// Baseline configures the log-step arm (docs/todo/baseline-gates.md).
	Baseline BaselineOptions
	// now is injectable in tests. Defaults to time.Now.
	now func() time.Time
}

// BaselineOptions configures the baseline gate. Once a service's error (or warn)
// stream has a baseline, it is reported only when a window steps up to at least
// Multiplier x its own median and at least FloorLines; a stream sitting at its
// baseline (chronic noise) is muted. Until a baseline exists (fewer than
// MinBaselineWindows readings) the plain ErrorThreshold presence gate applies,
// so a fresh agent is not blind.
type BaselineOptions struct {
	// Enabled is the kill switch. Nil (unset) means true; set it to a pointer to
	// false for the old presence-only gate. A pointer because the zero value of a
	// bool cannot be told apart from "not configured".
	Enabled *bool
	// EvidenceWindows is how many windows a verdict may sum. Logs are dense, so a
	// step is judged on one window (MinEvidence is 1 line-window); the field is
	// kept for symmetry with the other families. Default 1.
	EvidenceWindows int
	// MinBaselineWindows is how many non-breaching windows a service needs before
	// it is judged against its median. Default 5.
	MinBaselineWindows int
	// Multiplier: a step needs lines >= Multiplier x median. Default 3.
	Multiplier float64
	// FloorLines is the minimum lines per window for a step. Default
	// Options.ErrorThreshold.
	FloorLines int64
	// ColdFloorLines is the lines per window at or above which a stream with
	// NO baseline yet is reported as a step and is not learned, so an agent
	// that starts during an error storm does not take the storm for the
	// service's normal and mute it. 0 (default) disables it: a cold stream is
	// reported on the presence gate and every window is learned. No default,
	// because a stream that is chronically above it would never form a
	// baseline and would report every window, which is the noise this gate
	// exists to mute.
	ColdFloorLines int64
}

func (b BaselineOptions) enabled() bool { return b.Enabled == nil || *b.Enabled }

func (o *Options) setDefaults() {
	if o.Interval <= 0 {
		o.Interval = 60 * time.Second
	}
	if o.ErrorThreshold <= 0 {
		o.ErrorThreshold = 10
	}
	if o.Baseline.Enabled == nil {
		on := true
		o.Baseline.Enabled = &on
	}
	if o.Baseline.EvidenceWindows <= 0 {
		o.Baseline.EvidenceWindows = 1
	}
	if o.Baseline.MinBaselineWindows <= 0 {
		o.Baseline.MinBaselineWindows = 5
	}
	if o.Baseline.Multiplier <= 0 {
		o.Baseline.Multiplier = 3
	}
	if o.Baseline.FloorLines <= 0 {
		o.Baseline.FloorLines = o.ErrorThreshold
	}
	if o.now == nil {
		o.now = time.Now
	}
}

// Collector is a runnable that periodically reads the sensors' log
// counters and forwards qualifying services to a Sender. Gated behind leader
// election so only one replica sends.
type Collector struct {
	opts   Options
	sender Sender
	log    logr.Logger

	// nsAllow is Options.Namespaces as a set; nil means "forward every namespace".
	nsAllow map[string]bool

	// prev holds the last-seen cumulative counters per (sensor, servicePair),
	// so a delta survives individual pod restarts/scaling independently — see
	// computeDeltas.
	prev map[string]targetCounters
	// prevSamples holds the last-seen cumulative value per (sensor, error-series-
	// with-sample-label), so computeDeltas/pickSamples can tell which series actually
	// moved this window rather than just which was first in family order.
	prevSamples map[string]map[sampleSeries]float64

	// errTr and warnTr track each service's error and warn lines per window
	// (key "namespace/service"); only the tick goroutine touches them.
	errTr, warnTr *baseline.Tracker
}

// targetCounters is one sensor's last set: metric -> servicePair -> value.
type targetCounters map[string]map[servicePair]float64

type servicePair struct {
	Namespace string
	Service   string
}

// New creates a Collector. Returns nil if opts.ServiceName is empty (collector
// disabled) or sender is nil (push mode disabled — nothing to send to).
// namespaceSet turns Options.Namespaces into a lookup set. Returns nil for an
// empty/blank-only list, which the collector reads as "forward every namespace".
func namespaceSet(list []string) map[string]bool {
	var set map[string]bool
	for _, ns := range list {
		if ns = strings.TrimSpace(ns); ns != "" {
			if set == nil {
				set = make(map[string]bool, len(list))
			}
			set[ns] = true
		}
	}
	return set
}

func New(opts Options, sender Sender, log logr.Logger) *Collector {
	if opts.Source == nil || sender == nil {
		return nil
	}
	opts.setDefaults()
	return &Collector{
		opts:        opts,
		nsAllow:     namespaceSet(opts.Namespaces),
		sender:      sender,
		log:         log.WithName("logmetrics"),
		prev:        make(map[string]targetCounters),
		prevSamples: make(map[string]map[sampleSeries]float64),
		errTr:       baseline.New(),
		warnTr:      baseline.New(),
	}
}

// NeedLeaderElection ensures only the leader replica sends.
func (c *Collector) NeedLeaderElection() bool { return true }

// namespaceAllowed reports whether ns passes the collector's namespace filter:
// Options.NamespaceAllowed when non-nil (dynamic — hot-follows annotation-driven
// push-scope changes), else the static nsAllow set (nil set = every namespace).
func (c *Collector) namespaceAllowed(ns string) bool {
	if c.opts.NamespaceAllowed != nil {
		return c.opts.NamespaceAllowed(ns)
	}
	return c.nsAllow == nil || c.nsAllow[ns]
}

// Start runs evaluate on a fixed interval until ctx ends.
func (c *Collector) Start(ctx context.Context) error {
	ticker := time.NewTicker(c.opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			c.runOnce(ctx)
		}
	}
}

// runOnce reads every sensor's latest log-exporter set, aggregates deltas, applies
// the gate, and sends. A sensor without a set is logged and skipped — one missing
// sensor must not block the others' counters from being reported.
func (c *Collector) runOnce(_ context.Context) {
	windowStart := c.opts.now()
	targets := c.opts.Source.Targets(metricset.RoleLogExporter)
	if len(targets) == 0 {
		ledger.Default.Forget(metricset.RoleLogExporter, nil)
		c.log.V(1).Info("no sensor has sent log-exporter metrics; nothing to evaluate")
		return
	}

	// Garbage-collect baselines for sensors that are gone. Without this,
	// DaemonSet pod churn (rollouts, node drains) accretes dead baselines in
	// c.prev/c.prevSamples forever.
	live := make(map[string]bool, len(targets))
	for _, t := range targets {
		live[t] = true
	}
	for t := range c.prev {
		if !live[t] {
			delete(c.prev, t)
			delete(c.prevSamples, t)
		}
	}

	ledger.Default.Forget(metricset.RoleLogExporter, live)
	totals := make(map[servicePair]*push.ServiceLogMetrics)
	// A service the log-exporter counted this tick is not counted again
	// from an OTLP source (redsource does the same).
	sensorSeen := make(map[servicePair]bool)
	for _, target := range metricset.SensorsFirst(targets) {
		otlp := metricset.IsOTLPTarget(target)
		set, err := c.opts.Source.Set(metricset.RoleLogExporter, target)
		if err != nil {
			c.log.V(1).Info("no log-exporter metrics from sensor; skipping", "target", target, "reason", err.Error())
			ledger.Default.Add(metricset.RoleLogExporter, target, ledger.NoSnapshot, 1)
			continue
		}
		deltas := c.computeDeltas(target, set)
		ledger.Default.Add(metricset.RoleLogExporter, target, ledger.Ticks, 1)
		ledger.Default.Add(metricset.RoleLogExporter, target, ledger.Points, int64(set.PointCount()))
		ledger.Default.Add(metricset.RoleLogExporter, target, ledger.Services, int64(len(deltas)))
		for pair, d := range deltas {
			if otlp && sensorSeen[pair] {
				continue
			}
			if !otlp {
				sensorSeen[pair] = true
			}
			// Drop other tenants' namespaces before they can be aggregated — this
			// connection stamps every frame with its own single handshake-bound
			// tenant, so forwarding them would mis-attribute them.
			if !c.namespaceAllowed(pair.Namespace) {
				ledger.Default.Add(metricset.RoleLogExporter, target, ledger.Filtered, 1)
				continue
			}
			agg := totals[pair]
			if agg == nil {
				agg = &push.ServiceLogMetrics{Namespace: pair.Namespace, Service: pair.Service}
				totals[pair] = agg
			}
			agg.ErrorCount += d.errorDelta
			agg.WarnCount += d.warnDelta
			agg.TotalCount += d.totalDelta
			if d.sample != "" && agg.Sample == "" {
				agg.Sample = push.TruncateSample(d.sample)
			}
			for _, s := range d.samples {
				if len(agg.Samples) >= push.MaxLogMetricsSamples {
					break
				}
				if s = push.TruncateSample(s); !slices.Contains(agg.Samples, s) {
					agg.Samples = append(agg.Samples, s)
				}
			}
		}
	}

	windowSec := int(c.opts.Interval / time.Second)
	var qualifying []push.ServiceLogMetrics
	unattributed := int64(0)
	for _, m := range totals {
		// pass is the presence gate (cold key, or baseline disabled); step is the
		// baseline arm's verdict for the stream that tripped it.
		pass, step, stream := c.gate(m, windowSec)
		if !pass {
			ledger.Default.Add(metricset.RoleLogExporter, ledger.All, ledger.Gated, 1)
			continue
		}
		// Pods with neither service label leave Service empty. Skip them: the
		// receiver rejects the whole frame over one empty entry, and an
		// unattributable service could never resolve in the hub's graph anyway.
		if m.Service == "" {
			unattributed += m.ErrorCount + m.WarnCount
			ledger.Default.Add(metricset.RoleLogExporter, ledger.All, ledger.Unattributed, m.ErrorCount+m.WarnCount)
			continue
		}
		if step != nil {
			m.Breach = push.BreachLogStep
			m.Observed = step.Observed
			m.Baseline = step.Baseline
			m.EvidenceSec = step.EvidenceSec
			m.StillBreaching = step.StillBreaching
			m.Stream = stream
			c.log.V(1).Info("log step over baseline", "namespace", m.Namespace, "service", m.Service,
				"stream", stream, "observed", step.Observed, "baseline", step.Baseline)
		}
		qualifying = append(qualifying, *m)
	}
	if c.opts.Baseline.enabled() {
		c.errTr.GC(c.gateFor(windowSec))
		c.warnTr.GC(c.gateFor(windowSec))
		c.errTr.Tick()
		c.warnTr.Tick()
		// The two streams share the log-step arm on the wire, so they share
		// the count.
		agentmetrics.BaselineMutedKeys.WithLabelValues(agentmetrics.SensorLog, push.BreachLogStep).
			Set(float64(c.errTr.Muted() + c.warnTr.Muted()))
	}
	if unattributed > 0 {
		c.log.Info("skipped unattributable error/warn logs (pods without a service label)",
			"count", unattributed)
	}
	ledger.Default.Add(metricset.RoleLogExporter, ledger.All, ledger.Ticks, 1)
	ledger.Default.Add(metricset.RoleLogExporter, ledger.All, ledger.Services, int64(len(totals)))
	ledger.Default.Add(metricset.RoleLogExporter, ledger.All, ledger.Sent, int64(len(qualifying)))
	if len(qualifying) == 0 {
		c.log.V(1).Info("no services crossed the error/warn threshold; no frame sent",
			"threshold", c.opts.ErrorThreshold, "servicesRead", len(totals))
		return
	}

	c.sender.SendLogMetrics(windowStart.Unix(), windowSec, qualifying)
	c.log.Info("sent log_metrics frame", "services", len(qualifying), "windowSec", windowSec)
}

// gateFor builds the baseline Gate: one window of evidence (a line count of at
// least 1, i.e. any window), the configured floor, and no breach while cold
// unless ColdFloorLines is set.
func (c *Collector) gateFor(windowSec int) baseline.Gate {
	b := c.opts.Baseline
	return baseline.Gate{
		Windows:     b.EvidenceWindows,
		WindowSec:   windowSec,
		MinEvidence: 1,
		MinBaseline: b.MinBaselineWindows,
		Multiplier:  b.Multiplier,
		Floor:       float64(b.FloorLines),
		ColdFloor:   float64(b.ColdFloorLines),
		Value:       baseline.Ratio,
	}
}

// gate decides whether service m is reported this window. It observes the
// service's error and warn lines, judges each stream against its own baseline
// and settles the readings (a breach is never absorbed).
//
// Per stream (errors and warns each have their own tracker, same rule):
//   - cold (no baseline yet) or baseline disabled: presence, count >= ErrorThreshold;
//     a cold window at or above ColdFloorLines (when set) is a breach, not learned;
//   - warm: report only on a breach, count >= Multiplier x median and >= FloorLines.
//
// A stream whose steps recur is muted (package baseline): it then reports every
// baseline.StillBreachingEvery windows, and the mute is logged when it starts
// and ends.
//
// The returned verdict is the error step if errors stepped, else the warn step,
// nil when the service passed on presence only; stream names which one it is
// (push.LogStreamError or push.LogStreamWarn), since its Observed and Baseline
// are that stream's lines.
func (c *Collector) gate(m *push.ServiceLogMetrics, windowSec int) (pass bool, step *baseline.Verdict, stream string) {
	thr := c.opts.ErrorThreshold
	if !c.opts.Baseline.enabled() || m.Service == "" {
		return m.ErrorCount >= thr || m.WarnCount >= thr, nil, ""
	}
	g := c.gateFor(windowSec)
	key := m.Namespace + "/" + m.Service
	for _, s := range []struct {
		tr     *baseline.Tracker
		count  int64
		stream string
	}{{c.errTr, m.ErrorCount, push.LogStreamError}, {c.warnTr, m.WarnCount, push.LogStreamWarn}} {
		s.tr.Observe(key, float64(s.count), 1)
		v := s.tr.Judge(key, g)
		switch s.tr.Settle(key, v) {
		case baseline.MuteStarted:
			c.log.Info("baseline key muted: its steps recur, so it now reports every few windows instead of every window",
				"arm", push.BreachLogStep, "key", key, "stream", s.stream, "observed", v.Observed, "median", v.Baseline, "reference", v.Reference,
				"reportEveryWindows", baseline.StillBreachingEvery)
		case baseline.MuteEnded:
			c.log.Info("baseline key no longer muted: its steps stopped recurring",
				"arm", push.BreachLogStep, "key", key, "stream", s.stream)
		}
		switch {
		case v.Breach:
			pass = true
			if step == nil {
				vv := v
				step, stream = &vv, s.stream
			}
		case v.Ready && v.Cold:
			pass = pass || s.count >= thr
		case v.Ready && s.count >= thr:
			c.log.V(2).Info("chronic log stream at its baseline; muted", "namespace", m.Namespace,
				"service", m.Service, "lines", s.count, "baseline", v.Baseline)
		}
	}
	return pass, step, stream
}
