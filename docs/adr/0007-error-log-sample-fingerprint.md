# ADR-0007: Error-Log Sample via Fingerprint Label

- **Status:** Implemented
- **Date:** 2026-07-17
- **Deciders:** Denis (Principal SRE)
- **Related:** ADR-0006 (log-based metrics — the pipeline this extends)

> **Note (2026-07-17):** production on `cluster-a` surfaced two follow-ups (chart
> `0.2.1`). First, `fingerprint.lua`'s plain head-truncation-then-normalize
> order was wrong for structured (GCP-style JSON) logs: truncating to
> `sampleMaxLen` first spent the entire budget on the envelope
> (`{"label":...,"logging.googleapis.com/operation":{...}`) and never reached
> `message`, so `sample` carried metadata instead of the error text. The
> filter now extracts `message`/`msg`/`error`/`err` string fields from
> JSON-shaped lines *before* truncating (object-valued fields like
> `"error":{"code":5}` don't match and fall back to whole-head fingerprinting,
> same as today). Second, a URI-credential scrub
> (`user://user:***@host` — password never reaches the wire) was added since
> apps sometimes log full connection strings in error text and digit/hex
> normalization alone doesn't catch an alphanumeric password. Third, an
> unrelated hardening rides along: `--reuse-values` on `helm upgrade` drops
> chart defaults introduced *after* the values a release was last installed
> with, which rendered `emitterMemBufLimit` empty and crash-looped the
> DaemonSet on `cluster-a`; the three `Emitter_Mem_Buf_Limit` template lines
> (Decision point 3) and `sampleMaxLen` now carry `| default` so a nil under
> `--reuse-values` falls back to the chart's intended default instead of
> rendering empty. `sampleEnabled` (the gate discussed throughout this ADR)
> intentionally keeps no `| default` — a nil under `--reuse-values` leaves the
> feature silently off, which is safe since it's exactly the ADR-0006
> counters-only fallback; this is now annotated in the template itself.
> **Update (chart `0.2.2`):** the message-extraction window is bounded at 2048
> chars, so a long `message`/`stack` value (a multi-line stack trace) can run
> past it with no closing quote in `head`, making every terminated pattern
> fail and silently falling back to the whole-head envelope; the filter now
> also tries `stack` (winston-style loggers) and, only when `#head == 2048`
> (the window was actually cut, not just a short/empty value), an unterminated
> capture of whichever field opened but didn't close — giving `sample` the
> start of the real message/stack text instead of JSON metadata.

## Context

ADR-0006 shipped error/warn/total counters per (namespace, service), but explicitly
deferred `Sample`: "Fluent Bit's `log_to_metrics` doesn't natively carry a
representative log line (it only counts), so populating `Sample` requires a custom
label a fleet's own pipeline adds ... out of scope for the default chart pipeline in
this ADR." Triage now sees error/warn volume *rising* for a service, but not *what*
is erroring — the exact line that incremented the counter is the context an
on-call engineer actually wants first, before ever opening a log viewer.

The obvious fix — label the counter with the raw `log` field — is not safe to ship
by default. Fluent Bit's `log_to_metrics` filter has **no series expiry**: per its
own documentation, metric state "is held for the lifetime of the process, with no
eviction or expiry." Every unique label-set becomes a permanent series in memory
until the pod restarts. A raw-message label means every distinct log line —
timestamps, request IDs, stack trace addresses, all included — mints a new series
forever; that is an unbounded memory leak by design, not a tuning problem. The
filter's `emitter_mem_buf_limit` is not an eviction mechanism either: when the
emitter's internal buffer fills, it **pauses** (drops updates) rather than evicting
existing series, so a cardinality blowup degrades into silently stale/missing
counters, not a bounded working set.

## Decision

Ship a bounded, opt-in sample label on the error counter only:

1. **Lua fingerprint filter**, inserted between the `kubernetes` filter and the
   `log_to_metrics` filters (gated on `fluentbit.pipeline.sampleEnabled`, default
   `true`). It truncates each record's `log` field to `sampleMaxLen` (160 chars
   default) **first** — bounding the cost of every subsequent `gsub` — then
   collapses volatile tokens to `#`: control characters (CRI multiline
   reassembly can leave newlines/stack traces in `log`) to spaces, `0x`-prefixed
   hex, bare hex runs of 8+ characters (request IDs, hashes, UUIDs), and any
   remaining digit run (timestamps, ports, counts). The result is written to
   `record["err_sig"]`. Two log lines that differ only in their timestamp and
   request ID normalize to the identical fingerprint and therefore the identical
   metric series.
2. The **ERROR** `log_to_metrics` filter only gets `Add_Label sample $err_sig`.
   Warn and total counters are untouched — Sample is specifically "what error is
   firing," not a general log-sampling feature, and doubling the label surface on
   all three filters would triple the cardinality risk for no corresponding
   benefit.
3. **`Emitter_Mem_Buf_Limit` raised to 50M** (from Fluent Bit's 10M default) on
   all three `log_to_metrics` filters, because the sample label multiplies the
   error filter's series count (one per distinct template, not one per service)
   and a starved emitter buffer pauses that filter's updates.
4. **Accepted failure mode**: if cardinality still outgrows the pod's 128Mi
   memory limit despite fingerprinting, the OOMKill + restart *is* the reset
   mechanism. Fluent Bit's counters restart at zero on process restart, and the
   collector's `counterDelta` already treats a reset as "current value is the
   delta" (ADR-0006) — so the practical cost of a cardinality blowup is a gap in
   the next scrape window, not incorrect counts or an outage. This is a
   deliberate trade rather than an attempt to make cardinality strictly bounded:
   fingerprinting keeps the common case (retried errors, templated messages)
   collapsed to one series, and the memory limit is the backstop for whatever
   still gets through.
5. The collector (`logmetrics.Collector`) picks, per (namespace, service) pair,
   the `sample` label value from the error series with the **largest delta this
   window** — the template actually firing *now*, not whichever series happened
   to scrape first or have the highest lifetime total — falling back to the
   first non-empty value in family order when no labeled series moved (e.g. the
   pair qualified for the frame on warns alone).
6. **Endpoint-baseline garbage collection** in `Collector.runOnce`: endpoints
   that leave the Fluent Bit `Endpoints` list (DaemonSet pod churn) have their
   `prev`/`prevSamples` baselines deleted, so per-pod rollouts don't accrete
   dead map entries in the collector's own memory indefinitely.
7. **No hub-side change needed.** Unlike ADR-0006's `KindLogMetrics` frame kind
   (which required a hub-first rollout because the receiver rejects unknown
   frame kinds), `Sample` has been a field on `ServiceLogMetrics` since v1 of the
   `log_metrics` frame. This ADR only changes what populates that
   already-defined field — no receiver-side schema change, no rollout sequencing.

## Alternatives considered

1. **Raw `log` field as a `label_field`.** Rejected outright: unbounded
   cardinality with no expiry mechanism on the Fluent Bit side, as established in
   Context above.
2. **On-gate-trip `pods/log` tail fetch by the controller**, i.e. when a service
   crosses the error threshold, have the controller fetch recent log lines
   directly via the Kubernetes API instead of via a Fluent Bit label. Rejected
   for now: it requires new `pods/log` RBAC and pod-selection logic (which pod,
   which container, how far back), and the actual requirement — the line that
   matched the error regex — is exactly what the fingerprint label already
   provides with none of that machinery. Revisit if multi-line context or full
   stack traces become a real requirement; the label approach only ever carries
   one truncated line.

## Consequences

**Positive**
- Triage gets the erroring log line itself, not just a count, closing the gap
  ADR-0006 explicitly left open.
- Fingerprinting is normalization, not sampling — every window's dominant error
  template is represented, and retries of the same underlying failure collapse
  to one series instead of fragmenting the counter space.
- No hub-side rollout coordination required (contrast ADR-0006).

**Negative / risks**
- The error filter's series count is no longer bounded by (namespace, service)
  pairs alone — it's (namespace, service, error template), so a service logging
  many distinct error shapes produces proportionally more series. Fingerprinting
  bounds this in the common case; it does not make it impossible in principle.
- `sampleMaxLen` truncation happens before normalization, so a template whose
  distinguishing text falls after byte 160 collapses indistinguishably with a
  different template that shares the same first 160 bytes. Accepted: the
  troubleshooting value of "an error line" outweighs perfect template
  separation.
- Adds one more Lua filter to the per-record pipeline cost, gated off entirely
  when `sampleEnabled=false`.

**Neutral**
- `fluentbit.pipeline.sampleEnabled`/`sampleMaxLen`/`emitterMemBufLimit` and
  `logMetrics.sampleLabel` are all independently configurable; setting
  `sampleEnabled=false` restores exact ADR-0006 counters-only behavior with no
  other code path change.

## Implementation

Sender side (this repo): `charts/triage-agent/templates/fluentbit-configmap.yaml`
(Lua filter, `fingerprint.lua` ConfigMap key, `Add_Label sample`,
`Emitter_Mem_Buf_Limit`), `logmetrics/deltas.go` (`sampleSeries`,
`Collector.pickSamples`), `logmetrics/collector.go` (`prevSamples`, endpoint-baseline
GC in `runOnce`), `main.go` (`--log-metrics-sample-label`),
`charts/triage-agent/templates/deployment.yaml` (unconditional
`--log-metrics-sample-label` arg), `charts/triage-agent/values.yaml`. No receiver-
side change — `Sample` has carried this data since the `log_metrics` frame's v1
shape (ADR-0006).
