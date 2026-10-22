# Baseline gates: every detector judges a deviation from the service's own normal, over enough evidence

**Status: proposed 2026-10-01.**

## Why

Eval run fast2 (demo tenant, 2026-09-30) had five telemetry faults that never
reached the core as a signal, or reached it only as background:

| Fault | Where it lives in the demo | Why no gate fired |
|---|---|---|
| payment 100% failures | `Charge`, once per order | a few errors per minute; `errorThreshold` 10 per 60 s window |
| cart 25% errors | `EmptyCart` only, once per checkout | about 1 error per minute: under the count AND under the 50% operation rate |
| shipping 10 s delay | `ShipOrder`, international orders only | service-wide p99 is dominated by `GetQuote`; no per-operation latency |
| ad CPU burn | no CPU limit on the pod | only PSI gates CPU; no limit means no throttling and no utilization % |
| recommendation bad config | error logs already flowing before the fault | the log gate is presence (≥10 lines), so a step from 12 to 60 looks like the stream before it |

These are one problem, not five edge cases. Every gate asks "is this one
60-second window above a fixed absolute number?". That misses low-volume
operations, where checkout-path failures live. It misses resources with no
limit to divide by. It misses steps on top of a chronic stream. And it keeps
re-reporting chronic streams that are not news.

## The rule, one for all detectors

A (service[, operation], metric) **breaches** when, over the evidence window:

1. **enough evidence**: the summed denominator over the last `evidenceWindows`
   windows (default 5, i.e. 5 minutes) reaches a minimum (requests, log lines,
   CPU seconds). A busy service decides in one window; a quiet operation
   accumulates up to five;
2. **deviation**: the observed value is at least `multiplier` × the median of
   its own recent **non-breaching** windows (a breach is never absorbed, so a
   sustained fault does not become normal), or, when the baseline is about
   zero, above an absolute **floor**;
3. **floor**: the observed value is above an absolute floor, so 1 ms → 4 ms,
   0.01 → 0.05 cores, or 0 → 1 error never fires.

### Cold keys

Baselines are in memory, so every key is cold (fewer than
`minBaselineWindows` readings) after an agent start or restart. A cold key is
judged against a conservative absolute **cold floor**: a reading at or above
it breaches and is **not settled** into the baseline. A restart during an
outage therefore reports the outage instead of learning it as the key's
normal. A cold reading under the cold floor does not breach and is learned as
usual, so a cold key stays quiet on ordinary readings.

A key that stays at or above its cold floor never forms a baseline and fires
every window, like the fixed gates do. An operation that is above a cold floor
by nature (a long poll, a slow report) needs the floor raised.

### Chronic keys: an explicit, bounded mute

A key that breached in at least 30% of its last 20 settled windows, in at
least 3 separate runs, is chronically spiky. It is judged against the 90th
percentile of all its readings, spikes included, so its usual spikes stop
breaching every window while a step above them still does. This is a mute, and
it is explicit and bounded:

- the collector logs at info level when a key becomes muted and when the mute
  ends (the key calmed down), and the gauge
  `triage_agent_baseline_muted_keys{sensor,arm}` counts the muted keys;
- a muted key that is still deviating from its median reports again every 10
  windows, with `stillBreaching: true` on the wire. A sustained fault at the
  spike level on a chronic key is therefore reported within 10 windows;
- a p99 over about ten requests is their maximum, so one slow request every
  few windows looks chronic. A breaching latency reading on fewer than
  `latChronicMinRequests` requests (default 100) is still reported, but does
  not count toward the chronic test.

### Low-volume keys

A key under about 4 requests per minute never reaches the 20-request
evidence minimum within 5 windows. It is judged by a second rule instead: at
least `errSparseMinErrors` errors (default 5) and a failure share of at least
`errSparseRatePct` (default 50%) over the whole lookback. Such a verdict is
never settled into the baseline.

## Applied

| Detector | Key | Observed | Evidence | Floor (default) | Cold floor (default) | Multiplier |
|---|---|---|---|---|---|---|
| RED errors | service and each operation | error rate % | requests ≥ 20 over ≤5 windows, errors ≥ 3; or the low-volume rule | 5% | 15% | 3× baseline rate (or baseline < 1%) |
| RED latency | service and each operation | p99 ms from the summed bucket deltas | requests ≥ 10 over ≤5 windows | 300 ms | 5000 ms | 3× |
| USE CPU | service | cores used (usage_usec delta / the real time between the two snapshots) | every window with a fresh snapshot | 0.25 core | off | 3× |
| USE CPU throttling | service, when a limit exists | throttled-period share | periods ≥ 50 | 10% | 50% | 3× |
| Log errors | service | error lines per minute | every window | `errorThreshold` | off | 3× (a step) |

The CPU and log cold floors are off by default: no number of cores, and no
number of log lines, is abnormal for every service, and a stream chronically
above a log cold floor would report every window, which is the noise the log
gate exists to mute. With the floor off, a cold CPU key is silent and a cold
log stream is reported on the presence gate, and both learn what they read.
Set `useMetrics.baseline.cpuColdFloorCores` or
`logMetrics.baseline.coldFloorLines` to turn them on.

At most `maxOperations` operations of one service (default 100) are tracked
one by one. Further ones are judged together as `(other operations)`, so a
service whose routes are unbounded cannot grow the trackers without limit. A
slot is freed once its operation has been absent for 4 × `evidenceWindows`
windows.

The fixed latency arm (`latencyMultiplier`) uses the same latency cold floor
while it has fewer than `latencyMinBaselineWindows` readings.

The existing arms stay, for a large single-window burst: the service-wide error gate,
PSI, OOM, memory %, and the absolute log count on a service with no
baseline. The baseline arm is an additional OR arm, except for logs: there,
once a baseline exists, a stream that stays at its baseline stops
re-reporting every minute. That stream is the chronic email and
recommendation noise.

## Wire (additive, omitempty, byte-identical on the core mirror)

The gated entries in `ServiceRedMetrics`, `ServiceUseMetrics` and
`ServiceLogMetrics` each gain the same six fields:

```go
Breach         string  `json:"breach,omitempty"`         // which arm: errors | op-errors | latency | op-latency | cpu | throttle | log-step
Operation      string  `json:"operation,omitempty"`      // "POST /oteldemo.PaymentService/Charge" for op-* arms; at most 128 bytes
Observed       float64 `json:"observed,omitempty"`       // the value judged, in the arm's unit
Baseline       float64 `json:"baseline,omitempty"`       // the key's median (also for a chronic key); 0 = floor-only
EvidenceSec    int     `json:"evidenceSec,omitempty"`    // seconds of windows summed for the verdict
StillBreaching bool    `json:"stillBreaching,omitempty"` // the periodic report of a muted (chronic) key, not a new breach
```

`baseline` is always the median of the key's non-breaching readings. A
chronic key is judged against a higher reference (the 90th percentile of all
its readings), which is logged but not sent, so "observed vs baseline" means
the same thing on every breach.

`ServiceLogMetrics` also has `stream` (`error` or `warn`, omitempty): which
stream a `log-step` is about. `observed` and `baseline` are that stream's
lines, so without it a warn step reads as error lines.

The core composes its event message from these when present: "op-errors
POST …/Charge 100% vs baseline 0% over 300s". It also keeps the counts.

## Shared primitive: `baseline/` (new top-level package)

```go
type Gate struct {
    Windows, MinBaseline, Keep int
    MinEvidence, MinNum, Multiplier, Floor float64
    ColdFloor                 float64 // cold keys: breach at or above it, and do not learn it
    SparseMinNum, SparseFloor float64 // the low-volume rule
    ChronicMinEvidence        float64 // evidence a breach needs to count as a chronic spike
}
type Tracker struct { /* per-key ring of window samples + ring of baseline readings */ }
func (t *Tracker) Observe(key string, num, den float64) // adds one window's numerator/denominator
func (t *Tracker) Judge(key string, g Gate) Verdict
type Verdict struct { Ready, Breach, Cold, Muted, StillBreaching, Sparse bool; Observed, Baseline, Reference float64; EvidenceSec int }
func (t *Tracker) Settle(key string, v Verdict) Transition // records the reading unless it breached; reports MuteStarted/MuteEnded
func (t *Tracker) Muted() int                              // keys currently muted
func (t *Tracker) GC(g Gate)                               // drops keys absent for 4 x Windows rounds
```

Pure, no clock, no I/O, no logging, table-tested. The collectors log the mute
transitions and set the gauge. Each collector owns one Tracker per
metric. p99 needs the bucket deltas summed across the evidence windows, so the
latency arm stores bucket maps rather than num/den, behind the same Verdict.

## Order of work (one commit each)

1. `baseline/` package and the wire fields in `push/protocol.go`.
2. `redsource`: per-operation request, error and bucket accumulation over the
   evidence window. Baseline arms for op-errors, errors, op-latency and latency.
   The existing gates stay as fast arms.
3. `usemetrics`: CPU cores and throttling arms on the baseline.
4. `logmetrics`: the log-step arm. Chronic streams are muted once a baseline exists.
5. Core (`internal/remoteagent`): mirror the fields. Messages name the arm, the
   operation and the baseline. `RequestErrorAnomaly`/`RequestLatencyAnomaly`
   carry the operation in the sample.

Flags (chart `redMetrics.baseline.*`, `useMetrics.baseline.*`,
`logMetrics.baseline.*`): `evidenceWindows`, `minBaselineWindows`,
`multiplier`, the floors. Each family has `enabled` (default true) as a kill
switch. RED also has the evidence minimums (`errMinRequests`, `errMinErrors`,
`latMinRequests`), the cold floors (`errColdFloorPct`, `latColdFloorMs`), the
low-volume rule (`errSparseMinErrors`, `errSparseRatePct`),
`latChronicMinRequests` and `maxOperations`; USE has `cpuColdFloorCores` and
logs `coldFloorLines`. An unset chart key renders no argument, and a flag at 0
takes the binary's default.

## Deltas the gates rely on

- **RED.** The first read of a sensor's metric yields no delta: every series
  carries its lifetime count. On later reads a series that was not there
  before is new on the sensor, where it started from zero, so its whole value
  is the window's delta. A route's first 5xx, and a series the exporter dropped
  while idle and started again, are therefore counted in the window they
  happen in.
- **USE CPU.** The usage delta is divided by the real time between the two
  snapshots it spans, per pod, so a sensor or a pod that was away for a few
  reads does not read as several times its CPU. A read with no fresh snapshot
  is skipped.
- **RED frame cap.** When more than 100 services qualify, entries a latency or
  baseline arm qualified are kept first, then the largest error deltas, then
  namespace and service order.
