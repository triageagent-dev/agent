# TODO: Error-Log Sample via Fingerprint Label (application controller / chart side)

**Status:** Done
**ADR:** [ADR-0007](../adr/0007-error-log-sample-fingerprint.md)

Populate `ServiceLogMetrics.Sample` (deferred out of scope in ADR-0006) by having the
chart's Fluent Bit pipeline fingerprint each error line into a bounded `sample` label,
and have the collector pick the sample from whichever error series moved the most
each window. Off-by-default counters-only behavior is preserved when
`fluentbit.pipeline.sampleEnabled=false`.

## Design constraints

- **No unbounded cardinality.** `log_to_metrics` has no series expiry — a raw-message
  label would leak memory forever. The Lua filter truncates then normalizes
  (timestamps/ids/hex → `#`) so retried instances of one error template collapse to
  one series.
- **Errors only.** `Add_Label sample $err_sig` goes on the error counter only; warn
  and total counters are untouched.
- **Accepted pressure valve is OOMKill, not eviction.** If cardinality still outgrows
  the pod's memory limit, the restart resets Fluent Bit's counters to zero, and the
  collector's existing `counterDelta` reset-handling (ADR-0006) already treats that
  correctly — no new collector-side failure mode.
- **No hub-side change.** `Sample` has existed on `ServiceLogMetrics` since the
  `log_metrics` frame's v1 shape; this only changes what populates it.

## Steps

### 1. Chart: fingerprint filter + labels
- [x] `fluentbit-configmap.yaml`: Lua `[FILTER]` (fingerprint truncate+normalize into
      `err_sig`), gated on `fluentbit.pipeline.sampleEnabled`.
- [x] `Add_Label sample $err_sig` on the error `log_to_metrics` filter only.
- [x] `Emitter_Mem_Buf_Limit` on all three `log_to_metrics` filters (default `50M`,
      `fluentbit.pipeline.emitterMemBufLimit`).
- [x] New `fingerprint.lua` ConfigMap data key, gated on `sampleEnabled`, templating
      `sampleMaxLen` into the truncation length.

### 2. Chart: values + deployment args
- [x] `values.yaml`: `fluentbit.pipeline.sampleEnabled` (default `true`),
      `sampleMaxLen` (default `160`), `emitterMemBufLimit` (default `"50M"`);
      `logMetrics.sampleLabel` (default `"sample"`).
- [x] `deployment.yaml`: `--log-metrics-sample-label` passed unconditionally (not
      gated on truthiness) so an empty override actually disables Sample instead of
      falling back to the binary's non-empty flag default.

### 3. main.go
- [x] `--log-metrics-sample-label` flag (default `"sample"`).
- [x] Wired into `logmetrics.Options.SampleLabel`.

### 4. Collector: largest-delta sample selection
- [x] `logmetrics/deltas.go`: `sampleSeries` type; `Collector.pickSamples` replaces
      `sampleByServicePair` — per pair, the label value of the series with the
      largest positive delta this window wins, falling back to first-non-empty when
      nothing moved.
- [x] `logmetrics/collector.go`: `prevSamples` field (per-endpoint baseline for
      per-series sample values), initialized alongside `prev`.
- [x] Endpoint-baseline GC in `runOnce`: `prev`/`prevSamples` entries for endpoints no
      longer in the Endpoints list are deleted each cycle.

### 5. Tests
- [x] `TestCollector_SamplePicksLargestDeltaSeries`
- [x] `TestCollector_SampleFallsBackWhenNoLabeledSeriesMoves`
- [x] `TestCollector_SampleLabelDisabledSumsAcrossSampleSeries`
- [x] `TestCollector_ForgetsGoneEndpoints`
- [x] `go test ./logmetrics/... ./push/...` green.
- [x] `gofmt -l .` / `go vet ./...` / `go build ./...` clean.
- [x] `helm lint charts/triage-agent` clean; `helm template` renders assert
      `fingerprint.lua`, `Add_Label sample $err_sig`, `Emitter_Mem_Buf_Limit 50M` (x3),
      `--log-metrics-sample-label=sample` present when `sampleEnabled=true`, and all
      absent (buffer limit still present) when `sampleEnabled=false`.
- [x] `fingerprint.lua` syntax-checked with `luac -p` (template expression manually
      substituted with `160`).

## Out of scope (still)

- Multi-line stack-trace context beyond one truncated, fingerprinted line — revisit
  via an on-gate-trip `pods/log` fetch if this becomes a real need (see ADR-0007
  Alternatives).
- Making cardinality strictly bounded rather than practically bounded — the design
  accepts OOMKill+restart as the backstop, not a hard cap.

## Follow-up: JSON extraction + credential scrub + reuse-values hardening (chart 0.2.1/0.2.2)

Production on `cluster-a` showed plain head-truncation wasted the whole `sampleMaxLen`
budget on a structured (GCP JSON) log's envelope, and `--reuse-values` dropping
new chart defaults crash-looped the DaemonSet. See the ADR-0007 note above.

- [x] `fingerprint.lua`: extract `message`/`msg`/`error`/`err` string fields from
      JSON-shaped lines (`head:match('^%s*{')`) before truncating; object-valued
      fields fall back to whole-head fingerprinting unchanged.
- [x] `fingerprint.lua`: unescape `\n`/`\t`/`\"`/`\\` in the extracted text for
      readability.
- [x] `fingerprint.lua`: URI userinfo credential scrub (`user:***@host`) on the
      chosen source, ahead of the existing digit/hex normalization.
- [x] Truncation to `sampleMaxLen` moved to the end of the pipeline (was first).
- [x] `Emitter_Mem_Buf_Limit` (all three `log_to_metrics` filters) and
      `sampleMaxLen` now carry `| default` so `--reuse-values` rendering them
      empty falls back to the chart's default instead of crash-looping the
      DaemonSet. `sampleEnabled` intentionally left without `| default` (nil
      under `--reuse-values` = feature silently off = safe fallback).
- [x] `helm lint` + `helm template` assertions: extraction patterns present in
      rendered `fingerprint.lua`; `Emitter_Mem_Buf_Limit 50M` (x3) present both
      with `emitterMemBufLimit` unset and with it explicitly set to `""`.
- [x] Lua fingerprint() exercised end-to-end (real `lua` interpreter, not just
      `luac -p`) against GCP-structured, pino-style, logfmt+DSN, plain-text,
      JSON-error-object, and empty-log inputs.
- [x] Chart version bumped 0.2.0 → 0.2.1 (chart-only; `appVersion` unchanged).
- [x] `fingerprint.lua` (0.2.2): `stack` added as a last-resort terminated key
      (winston-style loggers), plus an unterminated fallback — gated on
      `#head == 2048` so only an actually-cut window falls through to it —
      for a `message`/`msg`/`error`/`err`/`stack` value longer than the
      2048-char search window (a long stack trace previously fell back to the
      whole-head JSON envelope since its closing quote was never in `head`).
      Chart version bumped 0.2.1 → 0.2.2, chart-only.
