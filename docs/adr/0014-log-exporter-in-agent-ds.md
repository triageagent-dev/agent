# ADR-0014: Native Log Exporter in the Agent DaemonSet (retiring Fluent Bit)

- **Status:** Implemented
- **Date:** 2026-08-22 (rounds 1-3); round 4, 2026-08-23
- **Deciders:** Denis (Principal SRE), operator direction 2026-08-22 (capacity/
  isolation requirements added mid-implementation — see "Capacity" below; error-
  line sample restoration added in a second round the same day — see "Samples"
  below; JSON message extraction added in a third round the same day, after a
  live sample showed a GCP envelope eating the whole sample budget — see
  "Samples" below), operator direction 2026-08-23 (round 4: logfmt extraction
  and a leading-noise strip pass, after two more live samples — a Prometheus-
  style logfmt line and a plain-text Postgres line — showed the same
  envelope-eats-the-budget failure mode in two more shapes JSON extraction
  alone didn't cover — see "Samples" below)
- **Related:** ADR-0006 (log-based metrics — the pipeline this ADR replaces the
  *source* for; the scrape/delta/gate/frame side, `logmetrics/`, is completely
  unchanged), ADR-0007 (error-log sample via fingerprint label — the model the
  "Samples" section below restores natively, on the exporter side only), ADR-0010
  (USE metrics node-exporter mode — the exact "second/third container on the
  Beyla DaemonSet pod, same image, new `--mode`" pattern this ADR repeats a
  third time), ADR-0013 (profile-driven DaemonSet decoupling — the `or
  beyla.enabled useMetrics.enabled ...` gate shape this extends)

## Context

ADR-0006 gave triage its log-volume signal (error/warn/total line counts per
service) by running Fluent Bit as a DaemonSet and having `logmetrics.Collector`
scrape its `log_to_metrics` Prometheus exporter. That works, but it is a whole
second agent — its own image, its own configuration language (Lua-adjacent
Onigmo regex, a multi-stage `[INPUT]`/`[FILTER]`/`[OUTPUT]` pipeline in
`fluentbit-configmap.yaml`), its own upgrade cadence, and a general-purpose log
pipeline doing one narrow job: count lines matching two regexes, per
(namespace, service). Every other signal family in this repo (RED, USE,
topology) is now produced by this SAME binary in a different `--mode`
(ADR-0010, ADR-0013) — Fluent Bit was the one remaining foreign agent in the
DaemonSet.

The receiving side of ADR-0006 — `logmetrics.Collector`'s scrape, delta,
counter-reset handling, and gate — has no idea what produced the Prometheus
counters it reads. Reproducing Fluent Bit's exact metric/label-name contract on
our own side is a strictly smaller problem than reproducing its `log_to_metrics`
filter engine.

## Decision

Add a fourth `--mode` (`node-exporter`, `log-exporter` now sit alongside the
default `controller` mode) implementing the counting half of the pipeline
natively:

```
Beyla DaemonSet pod, third container (logExporter.enabled)
  triage-agent --mode=log-exporter          (same image, no manager/reconciler)
    node-scoped pod informer (spec.nodeName=$NODE_NAME field selector) — same
      idiom as node-exporter mode, resolves (namespace, pod) -> service label
    directory scan of /var/log/containers/*.log every 10s (new/removed files)
    one goroutine PER FILE: tail from the file's current END at first sight
      ("never re-read history"), track (inode, offset), reopen+recover on
      rotation (rename+recreate) or truncation (copytruncate)
    per line: CRI-prefix skip -> literal pre-filter -> regex confirm ->
      atomic increment on a counter resolved ONCE per file, not per line;
      an ERROR line additionally fingerprints + attributes to its own
      per-template series (see "Samples" below)
    -> Prometheus text endpoint :9466, log_metric_counter_log_{errors,warns,
       lines}_total{namespace,service[,sample]} — IDENTICAL names/labels to
       Fluent Bit's log_to_metrics output (logexporter/metrics.go)

triage-agent logmetrics.Collector (controller Deployment, logMetrics.enabled)
  UNCHANGED — scrapes triage-log's Endpoints instead of triage-fluentbit's,
  same delta/counter-reset/gate/frame logic, zero code changes
```

`logmetrics/` — the scraper, the delta math, the gate, the `log_metrics` frame
— is not touched by this ADR at all. The whole change is upstream of it: a new
producer emitting the same shape.

### Metric/label contract: reproduced, not renamed

`logexporter.MetricLogErrorsTotal`/`MetricLogWarnsTotal`/`MetricLogLinesTotal`
equal `log_metric_counter_log_{errors,warns,lines}_total` — the exact strings
`values.yaml`'s `logMetrics.errorMetric`/`warnMetric`/`totalMetric` defaults
already were — and `LabelNamespace`/`LabelService` equal `"namespace"`/
`"service"`, `logMetrics.namespaceLabel`/`serviceLabel`'s defaults.
`LabelSample` equals `"sample"`, `logMetrics.sampleLabel`'s default — the
ERROR family carries it (see "Samples" below); warn/total never do. The chart
auto-wires `logmetrics.Collector` at the native exporter whenever
`logExporter.enabled` is true and `logMetrics.serviceName` is left unset
(`triage-log`:`9466` instead of the Fluent-Bit-Service default, and
`--log-metrics-sample-label=sample` by default) —
`charts/triage-agent/templates/deployment.yaml`'s `$logExporterIsSource`
branch.
`logmetrics/logexporter_contract_test.go` pins both directions of this
contract: a static equality check between the collector's own `Options`
defaults and the exporter's exported constants, plus end-to-end scrapes
against a fixture server emitting exactly that contract (including the
multi-series-per-fingerprint error shape — see "Samples" below).

### Classification: byte-identical regexes, narrower engine

`DefaultErrorRegex`/`DefaultWarnRegex` in `logexporter/classify.go` are
character-for-character `fluentbit.pipeline.errorRegex`/`warnRegex`'s
defaults. A fleet migrating a service from Fluent Bit to this exporter sees
the identical error/warn split for the identical log stream — this was a
requirement, not a nice-to-have, since a differing split would look like a
regression in whatever downstream trusts the count. `--log-exporter-error-
regex`/`--log-exporter-warn-regex` (chart: `logExporter.errorRegex`/
`warnRegex`) let an operator override, same override pattern as every other
collector's naming-table flags in this repo.

**Update 2026-10-10: service identity.** Attribution no longer reads the labels
first. A log signal must land on the graph node the pod's USE and RED signals
use, or the core drops it as an unknown service (it did, for a DaemonSet whose
`app.kubernetes.io/name` label differs from its name). The exporter now takes the
`triageagent.dev/virtual-app-group` annotation, then the owning workload's name
(a Deployment behind its ReplicaSet, the pod-template-hash trimmed), then the
labels for a pod with no owner. A namespace in `virtual-app-identity: labels`
mode names apps by label, which a node cannot see; there the owner name stays.

**Update 2026-10-10: well-known libraries.** The regexes grew past Fluent
Bit's pair, which missed what real services print: zerolog's console levels
(`ERR`, `WRN`, `FTL`, `PNC`), upper- and title-case and spaced JSON levels
(`"level": "ERROR"`), `severity`, `levelname` and `log.level` keys, logfmt
`lvl=eror` and `level=panic`, `CRITICAL`/`SEVERE`, nginx and envoy `[error]`
and `[warn]`, the .NET `fail:`/`warn:` prefix, and klog/glog's `E0102 15:04:05`
header. The word lists live in `classify.go` and build both the regexes and
the literal prefilter, so the two cannot drift; `classify_formats_test.go` holds
one sample line per library and checks the prefilter against the bare regex.
The Fluent Bit equivalence above holds for the Fluent Bit alternatives only.

### Samples: bounded, permanent, per-fingerprint series

**Round 2 of this ADR** (operator direction, same day): the first implementation
shipped counters-only, matching ADR-0006's original v1 scope but leaving a real
gap — a triage event read "service logged 18 error lines in 60s" with no sample
text, where the Fluent Bit path (ADR-0007) delivered the actual erroring line.
This section restores that natively.

**The correctness core is series identity, not fingerprinting.** A Prometheus
series is its metric name PLUS its full label set. ADR-0007 already worked this
out for Fluent Bit and this exporter reproduces the same model exactly:
`logmetrics.Collector`'s delta math (`counterDelta`, `logmetrics/deltas.go`) is
per-series — if a `sample` label's VALUE ever changed on what the collector
treats as "the same series," the counter would appear to reset (or worse,
silently mis-attribute a delta to the wrong template). So a `sample` label
value can never be relabeled or reused once minted, and a series can never be
evicted while the exporter process lives — eviction is indistinguishable from a
reset to the read side. The model:

- **One `errorSlot` per DISTINCT fingerprint per (namespace, service)**
  (`logexporter/metrics.go`) — its own permanent, monotonic
  `log_metric_counter_log_errors_total{namespace,service,sample="<fp>"}`
  series, created once, only ever incremented, never touched again except by
  `atomic.Uint64.Add`.
- **Capped at `maxSamplesPerService` (default 8, chart:
  `logExporter.maxSamplesPerService`, flag: `--log-exporter-max-samples-per-
  service`).** Past the cap, a genuinely NEW fingerprint — one that never
  matched an existing slot — increments an always-present, UNLABELED
  catch-all series instead of minting a 9th slot. `logmetrics.Collector`'s
  `pickSamples` already skips any series whose sample label is empty
  (`logmetrics/deltas.go`), so the catch-all correctly never becomes a
  frame's `Sample` while still contributing to `ErrorCount` (every series
  matching a `(namespace, service)` pair sums, via `sumByServicePair`,
  regardless of its `sample` label — this is the one existing collector-side
  mechanism that makes the whole per-fingerprint-series model work with ZERO
  collector code changes). An empty fingerprint (a line that normalizes away
  to nothing — all-digit/all-hex/all-whitespace content) also routes straight
  to the catch-all, for the same reason: a slot whose label value is `""`
  would be indistinguishable from "no sample label" on the read side, wasting
  a cap slot for no benefit.
- **Lock-free on the common path.** `errSlots` is a copy-on-write
  `[]*errorSlot` behind an `atomic.Pointer` — an error line matching an
  ALREADY-KNOWN fingerprint (the steady-state case once a service's error
  templates have stabilized, which is most of the time in practice) does one
  `Load`, a short linear scan bounded by the cap, and one atomic `Add` — no
  lock. `errMu` guards only the rare path — minting a genuinely new
  fingerprint's slot — bounded to at most `maxSamplesPerService` occurrences
  per (namespace, service) over the exporter's whole lifetime.
- **Why no eviction, ever, at any cardinality**: this is ADR-0007's own
  accepted trade, restated for this exporter — Fluent Bit's `log_to_metrics`
  has no series expiry either, and evicting here would corrupt exactly the
  delta math series-identity depends on. The backstop is the same: the
  container's memory limit plus a restart, which resets ALL counters to zero
  together (never a partial, series-selective reset) and which
  `counterDelta`'s "current value is the delta" reset handling already
  absorbs cleanly.

**Fingerprinting** (`logexporter/fingerprint.go`) collapses volatile tokens —
UUIDs (matched whole, before the generic hex-run pass — four of a UUID's five
dash-separated groups are under the hex-run floor and would otherwise leave
literal, request-varying hex fragments), hex runs ≥ 8 chars, digit runs, and
whitespace runs (including embedded newlines from CRI multiline reassembly) —
to a single `#` placeholder, matching Fluent Bit's own `fingerprint.lua`
convention (ADR-0007), then truncates to `sampleMaxLen` (default 160, chart:
`logExporter.sampleMaxLen`, matches `fluentbit.pipeline.sampleMaxLen`
byte-for-byte). **Normalizing BEFORE truncating is the deliberate reverse of
Fluent Bit's ORIGINAL Lua filter order** — ADR-0007's own 2026-07-17 production
note found truncate-first spent the whole length budget on a long JSON
envelope and never reached the actual message text; normalizing first (so
volatile tokens are already collapsed before the length cut) avoids
reintroducing that exact bug.

**Round 3 (same day): JSON message extraction.** Normalize-before-truncate
alone was not enough — a live sample came through as `{"actor":"worker",
"event":"sync","jobId":"#","label":"acme","level":"error","logging.
googleapis.com/operation":{...` — 160 bytes entirely spent on envelope
metadata, the actual message truncated away before fingerprinting ever ran.
`logexporter/jsonextract.go`'s `extractErrorText` runs BEFORE fingerprinting,
on the FULL untruncated content, closing exactly this gap:

- If the content, after skipping leading JSON whitespace, does not start
  with `{`, extraction is skipped entirely — a cheap byte scan
  (`looksLikeJSONObject`) that costs nothing for the (still-common)
  non-JSON error line, before any `encoding/json` call is attempted.
- Otherwise the content is decoded once into `map[string]json.RawMessage`
  and `sampleJsonFields` (default `message,msg,error,err`, chart:
  `logExporter.sampleJsonFields`, flag: `--log-exporter-sample-json-
  fields`) is checked in priority order: the first key present with a
  non-empty STRING value wins; a present OBJECT value additionally tries
  ITS OWN `"message"` sub-key (covers `{"error":{"message":"..."}}`, the
  common Go/Node error-wrapping shape); a present-but-empty or
  present-but-non-string value falls through to the next candidate key. A
  top-level `"stack"` string, if present, has its FIRST LINE ONLY appended
  as `" | "+line` to whatever was extracted.
- **Uses `encoding/json` — a real parser, never a substring scan** — this
  was a hard requirement, not a style preference: a naive `bytes.Contains`
  search for `"message"` would be fooled by an EARLIER key's string value
  that happens to contain that literal text (`{"jobId":"contains the word
  message in it","msg":"real text"}`), or by an escaped quote inside a
  string value shaped like fake JSON structure. A real parser distinguishes
  keys from string content unconditionally; `TestExtractErrorText_Table`
  pins both cases.
- Falls back to the raw line UNCHANGED — the existing, already-safe
  behavior — whenever: the content isn't a JSON object, the JSON is
  malformed or truncated (`json.Unmarshal` erroring), or none of the
  priority keys yielded a usable value. The sample is never lost, only ever
  improved.
- The extracted text then flows through `fingerprint` exactly as before —
  extraction and normalization are two separate, composed steps, not a
  rewrite of either.

**Round 4: logfmt extraction, then a leading-noise strip pass.** JSON was
only ever half the structured-logging landscape. A second live sample —
`ts=... caller=collector.go:142 level=error msg="collector failed"
name=database duration_seconds=5.2 err="dial tcp: lookup dev-ecosystem-
postgres-api-b...` (truncated mid-hostname) — showed the exact same
envelope-eats-the-budget failure mode for **logfmt**, the
`key=value key="quoted value" ...` shape Prometheus's own ecosystem (and
much of the Go logging world — `log/slog`, `go-kit/log`, `logrus`'s text
formatter) emits by default.

`extractErrorText` now tries JSON first (unchanged), then — ONLY for
content that is NOT a JSON object — logfmt, before finally falling back to
the raw line:

- **The cheap gate**: `logfmtKeyCandidates` checks whether content contains
  ANY configured field name immediately followed by `=`, via plain
  `bytes.Contains` — no parsing at all. A line with none of those
  substrings (`panic: ...`, any message with no `key=` structure) skips the
  real tokenizer entirely, at the same near-zero cost as `looksLikeJSONObject`
  rejecting a non-`{` line.
- **The same configured field list drives both formats.** Rather than add
  a second flag, `--log-exporter-sample-json-fields` /
  `logExporter.sampleJsonFields` (names kept as-is for compatibility with
  what round 3 already shipped, even though the list now configures more
  than JSON) is reused verbatim; the classifier's internal field was renamed
  `sampleFields` to reflect that.
- **A real tokenizer, not independent per-key searches.** `parseLogfmtPairs`
  scans the line once, left to right: a key is only ever read at a "key
  position" (line start, or immediately after whitespace); a double-quoted
  value is consumed in full — backslash-unescaping any `\X` to the literal
  character `X`, most commonly `\"` for an embedded quote — before the
  scanner ever looks for the next key. This is what makes a decoy safe: in
  `err="real text msg=\"decoy\" more text"`, the whole quoted `err` value
  (decoy included, now unescaped) is consumed as ONE value, and no
  independent `msg` pair is ever produced — a naive scanner searching for
  `msg="..."` anywhere in the byte slice would have returned `"decoy"`.
  `TestExtractErrorText_LogfmtDecoyInsideQuotedValueIsNotExtracted` pins
  this.
- **Composition differs from JSON's flat priority list**: logfmt composes
  UP TO TWO values — the first configured field belonging to
  `{message, msg}` present in the line, and the first belonging to
  `{error, err}` — joined as `"<msg> | <err>"` when both are present and
  distinct (identical values compose once, not duplicated); either alone is
  used by itself; neither present falls back to the raw line. This mirrors
  logfmt's own convention of carrying a human message AND a distinct error
  detail as separate fields, unlike JSON where one field usually already
  holds the whole message.
- Falls back to the raw line UNCHANGED under the same "never lose the
  sample" contract as the JSON path: no candidate key present, or the cheap
  gate found nothing to even attempt.

**The leading-noise strip pass** (`stripnoise.go`) closes a residual gap
neither extractor can: a PLAIN-TEXT log line with no JSON, no logfmt
structure, and therefore nothing for either extractor to claim — a
Postgres-style line, live sample: `2026-08-23 14:22:10.483 UTC [12345]
FATAL: role "root" does not exist`. Fingerprinting still collapses its
timestamp preamble to placeholders (`#-#-# #:#:#.# UTC [#] FATAL: ...`), but
does not remove it, so it still ate `sampleMaxLen` budget ahead of the
actual `FATAL: ...` text. `stripLeadingNoise` runs AFTER `fingerprint`
(after truncation too — this is a post-processing pass over the final
fingerprinted text, not a change to `fingerprint` itself) and repeatedly
trims from the START: whitespace; any SPACE-DELIMITED TOKEN composed
ENTIRELY of the fingerprint placeholder plus `[ ] ( ) : . , / + -`
(`#-#-#`, `#:#:#.#`, `[#]`); and a bare token from a tiny fixed timezone set
(`UTC`, `GMT`, `Z`) — nothing else word-like is ever stripped, since the
rule requires either pure punctuation/placeholder composition or an exact
match against that three-entry allowlist. It stops at the first token that
fails both tests — `role "root" does not exist`'s own placeholders (none,
here) or any mid-message placeholder in general are left exactly where they
are, since only a LEADING run is ever removed. If stripping would consume
the entire sample (an all-placeholder line), the unstripped fingerprinted
form is returned instead — this pass can never return empty. It runs
unconditionally on every error line's fingerprinted output, JSON/logfmt-
extracted or raw: an extraction result's first token is always real
message text, so the pass is a provable no-op for those (`TestStripLeading
Noise_PassesExtractedTextThrough`), and it only does real work for the raw
fallback case its own doc comment describes as "mopping up".

**Round 5 (operator-reported): identity vs. display split.** Masking the
fingerprint is correct for series identity but was also being shown to the
operator — a sample read `slot wait timed out after #ms`, destroying the one
value (the real duration) the sample exists to surface. The fix splits the
two concerns: `errorSlot` still selects/dedupes on the masked+truncated
fingerprint exactly as above (never evicted, never relabeled), but each slot
ALSO captures, once, at creation, a raw (unmasked) exemplar — the extracted
error text, leading-noise-stripped and truncated the same way, just never
masked — and the Prometheus `sample` label is now that exemplar, not the
fingerprint. `stripLeadingNoise` moved from running on already-masked text to
running on raw text, masking each leading token in isolation (sharing
`fingerprint()`'s own masking transforms via one helper, so identity and
noise-detection can never disagree about what counts as noise) purely to
decide whether to drop it, then returning the surviving RAW text unchanged —
which is also why running the SAME function on an already-masked fingerprint
still produces byte-identical output to before (masking a masked token is a
no-op). A collision guard covers the one new failure mode: two different
fingerprints in one (namespace, service) whose exemplars coincide after
truncation now fall back — the newer slot's label becomes its own
fingerprint (unique by construction), and `" ·2"` is appended in the
vanishingly rare case even that string is already taken.

### Hot-path isolation, restated for rounds 3 and 4

Extraction (JSON, then logfmt), fingerprinting, AND the leading-noise strip
are only ever reached from `tailer.countLine`'s `verdictError` branch
(`extractErrorText` → `fingerprint` → `stripLeadingNoise` →
`counterSet.recordError`); the non-error path (plain lines, JSON info
lines, and WARN lines — which get a sample-label-free single series,
unchanged since round 1) never allocates and never touches any of this
code — `TestNonErrorLinePath_ZeroAllocations` (`testing.AllocsPerRun`, an
exact average) keeps passing unmodified across all four rounds. See
"Capacity" below for the measured numbers.

### DaemonSet placement, gating, and security posture

Same pattern ADR-0010 established and ADR-0013 generalized: this is a third
container on the Beyla DaemonSet pod, gated independently.
`beyla-daemonset.yaml`'s top-level render gate becomes `or beyla.enabled
useMetrics.enabled logExporter.enabled` — a log-exporter-only cluster (no
Beyla, no USE metrics) gets a DaemonSet pod with exactly one container. `hostPID`
stays gated on `beyla.enabled` alone, unaffected by this change — the
log-exporter container never inspects another process, only files under a
`hostPath` mount, so it has no more reason to share the host PID namespace than
node-exporter mode does.

The container's `securityContext` asks for the host `/var/log` directory
**read-only** and nothing else beyond that: `privileged: false`,
`allowPrivilegeEscalation: false`, `readOnlyRootFilesystem: true`,
`capabilities: {drop: [ALL]}` with **no capabilities added back** (unlike
Beyla's scoped-but-nonzero eBPF set), `seccompProfile: RuntimeDefault`.
`runAsUser: 0`/`runAsNonRoot: false` are required — the container-log
symlinks under `/var/log/containers` resolve to files under
`/var/log/pods/...` whose ownership is the kubelet's (root), the same
constraint Fluent Bit's own container has always run under. No log line
content ever leaves the process boundary — only counts cross the `/metrics`
endpoint, the same "counts only, no bodies" property ADR-0006 designed Fluent
Bit's pipeline around.

RBAC: the `-beyla` ServiceAccount's existing ClusterRole already grants `pods`
`get`/`list`/`watch` (originally for Beyla's kubernetes attribute decorator
and node-exporter's pod informer) — the log-exporter container's own
node-scoped pod informer needs exactly this and nothing more, so **no new RBAC
rule was added**; the ClusterRole's doc comment was extended to name the third
consumer.

### Fail-guards: one log source per cluster

`logExporter.enabled` and `fluentbit.enabled` together would double-count and
double-report every error/warn/total line from the same log stream —
`deployment.yaml` fails the render on that combination
(docs/adr/0013's "Double-RED-source guard" is the direct precedent for this
shape). `logMetrics.enabled` with neither `fluentbit.enabled` nor
`logExporter.enabled` NOR an explicit `logMetrics.serviceName` also fails —
the scraper would have nothing to scrape. The `logMetrics.serviceName`
exception preserves the pre-existing, documented "point at an externally-run
Fluent Bit this chart does not deploy" escape hatch (`values.yaml`'s
`logMetrics.serviceName` comment) — an operator who has explicitly named a
scrape target has already answered "what do I scrape", so the guard only
fires on the genuinely-nothing-configured case.

## Capacity

The operator's explicit bar for this exporter: **same-or-more throughput than
the Fluent Bit (C core + Lua fingerprinting) pipeline it replaces**, with
counting as an independent, non-blocking pipeline — a hot or stuck file must
never stall another file's counters, and the `/metrics` scrape must never wait
on a tailer.

**Isolation.** Every file gets its own goroutine, its own `bufio.Reader`, its
own rotation/truncation state (`logexporter/tailer.go`). The only state a
tailer shares with anything else is a `*counterSet` resolved ONCE at
file-open/discovery time and held via `atomic.Pointer[counterSet]` — the
per-line hot path never takes a lock for total/warn, and the Prometheus
exposition handler reads every counter via `Load()`, never blocking on, or
blocked by, any tailer. Error attribution (see "Samples" above) is ALSO
lock-free on the common (already-known-fingerprint) path — its one mutex
guards only the rare new-fingerprint-slot append, never the per-line read.

**Per-line hot-path design.** `(namespace, service)` is resolved once per
file, not per line — it is a property of the file, not the line. Per line the
work is: CRI-prefix skip (three `bytes.IndexByte` calls, no allocation) +
classify + one or two atomic adds on the pre-resolved counter pointers. No map
lookups, no string conversion, no line copies in the common case: the tailer
reads with a 256KB `bufio.Reader` and uses `ReadSlice` (not `ReadBytes`), which
returns a slice into the reader's own internal buffer with zero allocation
whenever a line completes within one read — the benchmark below confirms 0
allocs/op end to end.

Classification runs a **cheap literal pre-filter before the real regex**: most
lines match neither pattern, and Go's `regexp` (RE2 — safe, linear-time, no
catastrophic backtracking) still costs real per-byte overhead even to confirm
a non-match. The first cut — one combined literal set gating BOTH regexes —
measured 230K lines/sec, dominated by `regexp.(*Regexp).tryBacktrack` at 76%
of CPU (profiled with `go tool pprof`): a JSON line carrying `"level":"info"`
contains the literal `"level"` and so still paid for a full (failed) regex
confirm on both patterns. Narrowing the literal sets to the FULL contiguous
substring each regex alternative actually requires — `` `"level":"error"` ``
rather than bare `` `"level"` ``, `` `level=warn` `` (no trailing quote —
`"warning"` defers the closing quote past "warn", a real false-negative bug
this exact narrowing caught, see `TestClassify_LiteralPrefilterNeverRejects
ARealMatch`) — and splitting into independent error/warn literal sets (a
line with only a `WARN` token has no reason to attempt `errorRe.Match` first)
eliminated the regex call entirely for the JSON-info case and for
same-family-only matches. Every literal is still a provably safe superset —
`bytes.Contains(line, literal)` can never reject a line the corresponding
regex would have matched, verified by
`TestClassify_LiteralPrefilterNeverRejectsARealMatch` against every
alternative branch of both default patterns.

**Measured throughput.** Three benchmarks in `logexporter/bench_test.go`
separate the overall mixed cost from the two paths that compose it. Numbers
below are post-round-4 (`errorBenchTemplates` now includes one GCP-envelope
JSON template AND one logfmt-envelope template alongside the four
plain-text ones, so `BenchmarkErrorLineWithSample` measures the real cost
of BOTH extractors plus the leading-noise strip pass, not just
fingerprinting):

```
go test ./logexporter/... -run '^$' -bench . -benchtime=2s -cpu=1

BenchmarkPerLineHotPath        3276386   741.2 ns/op   1349082 lines/sec    75 B/op    1 allocs/op
BenchmarkErrorLineWithSample    321717  7417   ns/op    134817 lines/sec   1506 B/op  29 allocs/op
BenchmarkNonErrorLinePath      6117216   395.0 ns/op   2531952 lines/sec     0 B/op    0 allocs/op
```

- **`BenchmarkPerLineHotPath`** (the full mix — CRI skip, pre-filter, regex
  confirm, and for the 5% error lines, structured extraction + fingerprint +
  the strip pass + `recordError`) over a 20,000-line fixture: 70% plain
  no-match / 15% JSON `"level":"info"` / 10% WARN variants / 5%
  error-fatal-panic-JSON-logfmt variants (a fixed, small set of templates
  with an embedded varying digit run — representative of a real fleet's
  error volume, not synthetic per-line noise; see the fixture's own doc
  comment for why the error category is generated differently from the
  other three). **~1.35M lines/sec on one core** — 2.7× the 500K floor.
  `allocs/op`/`B/op` are both `total / b.N` using INTEGER division, and only
  5% of iterations allocate (`BenchmarkErrorLineWithSample`'s own 29
  allocs/op × 0.05 ≈ 1.45, which floors to 1; 1506 B/op × 0.05 ≈ 75,
  matching the reported 75) — the same "non-error path costs nothing, error
  path costs what it costs, weighted by its 5% share" arithmetic as every
  prior round, now with logfmt extraction and the strip pass folded into
  that 5% alongside JSON extraction.
- **`BenchmarkErrorLineWithSample`** isolates the error-only path (classify +
  structured-extraction attempt + fingerprint + strip + `recordError`),
  cycling a small, fixed template set — four plain-text templates, one GCP
  JSON-envelope template, and one logfmt-envelope template (the exact shape
  of round 4's own live sample: `ts=... caller=... level=error
  msg="collector failed" ... err="dial tcp: lookup ..."`) — well under the
  default cap, so this measures the realistic STEADY STATE. **~135K
  lines/sec/core** — clears round 3/4's shared 100K/sec floor for this path
  by 1.35×, down from round 3's ~163K (`parseLogfmtPairs`' full tokenizer
  cost, paid on 1 of 6 templates, plus the strip pass's now-unconditional
  token scan on every error line, is the entire difference) but still far
  above what any realistic error-line volume needs, since error lines are a
  small share of total log volume.
- **`BenchmarkNonErrorLinePath`** proves the ~95%-of-mix path (plain + JSON-
  info + WARN — everything that never calls `extractErrorText`,
  `fingerprint`, or `stripLeadingNoise`) is untouched by the structured-
  extraction and strip-pass work added on top of the error path: **~2.53M
  lines/sec, 0 B/op, 0 allocs/op** — statistically unchanged across all four
  rounds. `TestNonErrorLinePath_ZeroAllocations` hard-asserts this via
  `testing.AllocsPerRun` (an exact average, not a rounded per-op figure)
  rather than relying on eyeballing benchmark output, and keeps passing
  unmodified across all four rounds.

The literal-narrowing pass from round 1 (below) remains the entire difference
between 230K/sec (below floor) and the ~2.6M/sec non-error-path number;
nothing else in that path showed up in the profile at a level worth
optimizing further. The round-2 error path's own cost (fingerprinting: one
string conversion + four `regexp.ReplaceAllString` calls) was not profiled
for further reduction — it clears every stated bar as measured.

**Overload semantics.** The exporter never buffers unboundedly and never OOMs.
An undelimited (no trailing `\n` yet) line accumulates up to
`maxPendingLineBytes` (1MiB) before being dropped and counted — the tailer
resyncs at the next newline it finds rather than growing without bound on a
pathological or truncated stream. If a tailer falls behind under CPU pressure,
counters simply lag — no queue, no backpressure signal needed, because a
poll-based per-file goroutine has nothing to overflow. If rotation outruns the
next poll tick (the old file is gone before its last undelimited partial line
completes), that partial is lost and counted in
`log_exporter_lines_dropped_total` — accepted, observable, not silently
swallowed. Self-metrics (`log_exporter_lines_processed_total`,
`log_exporter_bytes_processed_total`, `log_exporter_files_tracked`,
`log_exporter_lines_dropped_total`) ride the same `/metrics` endpoint so
capacity is measurable in production, never merely asserted.

**Honest comparison to Fluent Bit.** Fluent Bit is a mature, general-purpose C
log pipeline handling arbitrary multi-stage `[INPUT]`/`[FILTER]`/`[OUTPUT]`
routing, multiple output backends, and its own buffering/backpressure model —
its counting throughput is not something a narrower Go implementation should
expect to categorically beat in the general case. What makes parity achievable
here is that this exporter's job is much narrower than Fluent Bit's: one
input shape (CRI container logs), one operation (two-regex classify), one
output (in-memory atomic counters, no on-disk buffering, no multi-backend
routing). Go's RE2 engine plus a literal pre-filter closes the ~5-10x
constant-factor gap a naive per-line regex-only implementation would have
carried against a compiled C matcher, without needing to beat Fluent Bit at
its own, larger job.

## Consequences

**Positive**
- One fewer foreign agent in the DaemonSet; the log-metrics signal family now
  follows the exact same "new `--mode` on the same image" pattern as USE and
  RED metrics (ADR-0009/0010/0013) — one image to build/push/scan for every
  signal family.
- No log line content ever leaves the process — counts only, matching the
  privacy posture ADR-0006 already established for Fluent Bit.
- `logmetrics/` (the scraper, delta math, gate, frame) needed zero code
  changes across ALL FOUR rounds — the entire migration, counters-only, then
  samples, then JSON extraction, then logfmt extraction plus the strip
  pass, is a producer swap; `pickSamples`' worst-offender selection
  (ADR-0007) works unmodified against this exporter's real multi-series-per-
  fingerprint shape, and every extraction/normalization step happening
  upstream of the wire contract means it never moved.
- Measured ~2.53M lines/sec/core on the non-error path (statistically
  unchanged across all four rounds) and ~1.35M lines/sec/core on the
  realistic mixed path including sample fingerprinting, JSON extraction,
  logfmt extraction, and the strip pass, both well above the stated parity
  floor, with isolation (per-file goroutines, lock-free hot path on both the
  counting AND the error-attribution side) that Fluent Bit's own internal
  architecture does not need to expose as a Go-level guarantee. The
  error-only path (~135K lines/sec/core) clears round 3/4's shared
  100K/sec floor for that path specifically.
- A THIRD structured envelope shape (plain-text, no JSON, no logfmt — a
  Postgres-style timestamp preamble) now also gets its budget back, via the
  leading-noise strip pass rather than a third extractor — it needs no
  key-based extraction at all, just recognizing and dropping the
  already-fingerprinted noise ahead of the real message.
- Triage events regain the erroring line itself, not just a count — the gap
  round 1 deliberately left open, closed without reopening the unbounded-
  cardinality risk ADR-0007 identified (the per-fingerprint-series-with-a-
  cap-and-catch-all model bounds it the same way Fluent Bit's fingerprinting
  did).

**Negative / risks**
- The CRI split-line dedup (P/P/F sequences) classifies from the FIRST chunk
  only — a marker appearing only in a LATER chunk of a line split across
  containerd's 16KB boundary is not seen. This is the same class of
  limitation Fluent Bit's own multiline handling carries, not a regression
  introduced here, but it is a real, documented gap either way.
- `runAsUser: 0`/`runAsNonRoot: false` is a real (if narrow) privilege — the
  container can read anything under `/var/log` on its node, mounted
  read-only. No capability beyond that; no process inspection, no eBPF, no
  write access.
- v1 has no fsnotify-based file-change detection — new/removed files are
  discovered on a 10s poll (`RescanInterval`), not instantly. A brand-new
  container's first several seconds of logs are covered (the tailer starts
  from the file's end at discovery, same as Fluent Bit's own non-`Read_from_
  Head` default), but the discovery latency itself is a small, deliberate
  simplicity trade — the DaemonSet's existing 10s posture elsewhere (Beyla
  discovery intervals) made this an easy default, not a forced one.
- **Resolved in round 3**: JSON message extraction (`logexporter/
  jsonextract.go`) now matches Fluent Bit's own field-extraction refinement
  (ADR-0007's `message`/`msg`/`error`/`err` priority list, plus a top-level
  `"stack"` first line) — a JSON-heavy fleet no longer sees error samples
  dominated by envelope text. The one thing still not reproduced from
  Fluent Bit's evolution is trying `"stack"` on a NESTED object value (only
  the top-level key is checked) — accepted as a narrower, deliberately
  simpler scope than chasing every field Fluent Bit's Lua filter has grown
  over time.
- **Resolved in round 4**: logfmt extraction (`logexporter/
  logfmtextract.go`) covers the other half of structured logging JSON
  didn't — Prometheus-ecosystem and Go-`slog`/`go-kit`/`logrus`-text-style
  `key=value` lines — and the leading-noise strip pass
  (`logexporter/stripnoise.go`) covers plain-text lines with no structured
  envelope at all. There is no known fourth structured shape left
  unaddressed as of this round; a genuinely new one (a different structured
  logging convention entirely) would need its own extractor, same as the
  first two.

**Neutral**
- `fluentbit.*` values remain fully functional — this is an additive path,
  not a breaking change. `fluentbit.*` is now the LEGACY option; retirement
  from the chart (removing the templates/values entirely) is a follow-up
  gated on fleet migration, not part of this ADR.

**Addendum (chart 0.7.0, 2026-08-23):** the fleet migration referenced above is
complete — the last Fluent Bit install was migrated to the native exporter — and
the chart no longer bundles Fluent Bit at all as of chart 0.7.0:
`fluentbit-daemonset.yaml`, `fluentbit-configmap.yaml`, and the `fluentbit:`
values block are removed, along with the guard that kept the two log sources from
running together. `logExporter.enabled=true` is now the only built-in source; an
external Prometheus-scrape source — including a fleet-run Fluent Bit — remains
supported unchanged via an explicit `logMetrics.serviceName`.

## Rollout

Enable `logExporter.enabled=true` and `logMetrics.enabled=true` with
`fluentbit.enabled=false` (the fail-guard enforces this exclusivity). No hub
change is required — this is a producer swap behind an unchanged
`log_metrics` frame kind and payload shape (`push/protocol.go` untouched), so
there is no ADR-046-style hub-first ordering constraint the way ADR-0010's
`use_metrics` frame needed one. A rolling migration (enable `logExporter` on
one cluster/release, `fluentbit` on another) is safe: the two never coexist on
the same release (the fail-guard), but different releases in different
namespaces are fully independent.

## Testing

`logexporter/`: filename parsing (`TestParseLogFilename`), tail-from-end
(`TestTailer_TailsFromEndNeverReadsHistory`), rotation-by-rename
(`TestTailer_RotationByRenameAndRecreate`), in-place truncation
(`TestTailer_TruncationInPlace`), CRI split-line dedup
(`TestTailer_SplitLineCountedOnce`), non-CRI fallback
(`TestTailer_NonCRIFallsBackToWholeLine`), classification correctness and the
literal-prefilter safety property (`classify_test.go`), namespace filtering
and unattributable-pod handling (`discovery_test.go`, `exporter_test.go`),
Prometheus-text-exposition validity and the exact metric/label-name contract
INCLUDING escaping (`metrics_test.go`'s
`TestRenderMetrics_EscapesLabelValues`, a quote+backslash+newline sample
value round-tripped through `expfmt.NewTextParser`), fingerprint
normalization (`TestFingerprint_Table` — digits, hex runs, UUIDs, whitespace,
truncation), series stability under a repeating template
(`TestCounterSet_SeriesStability`), the cardinality cap and catch-all
behavior with all counters monotonic (`TestCounterSet_CardinalityCap`), the
empty-fingerprint-to-catch-all rule (`TestCounterSet_
EmptyFingerprintGoesToCatchAll`), a hard zero-allocations assertion on the
non-error path via `testing.AllocsPerRun`
(`TestNonErrorLinePath_ZeroAllocations`), and three benchmarks isolating the
mixed/error-only/non-error-only per-line costs (`bench_test.go`). JSON
message extraction (`jsonextract_test.go`'s `TestExtractErrorText_Table`,
table-driven): pino-style (`level`+`msg`), zap-style (`level`+`message`), the
exact GCP Cloud Logging envelope shape from the live production sample
(message buried after a nested `"logging.googleapis.com/operation"` object —
proves key-based lookup is position-independent), a nested `{"error":
{"message":"..."}}` object, stack-append (a multiline stack, first line
only), an earlier string VALUE that literally contains the text `"message"`
(plain, and with an escaped quote) — proving `encoding/json` is being used,
not a substring scan, malformed/truncated JSON and non-JSON content both
falling back to the raw line, and an empty extracted value falling through
to the next candidate key — plus `TestExtractErrorText_
FeedsIntoFingerprintUnchanged` proving the extracted text (not the raw
envelope) is what reaches `fingerprint`. Logfmt extraction
(`logfmtextract_test.go`'s `TestExtractErrorText_LogfmtTable`,
table-driven): the exact round-4 live production line pinned end to end
(envelope dropped, `msg`+`err` composed), an escaped-quote value, a bare
(unquoted) token value, `msg`-only, `err`-only, the `message=`/`error=`
long forms, identical `msg`/`err` values composing ONCE rather than
duplicating, and a non-logfmt line (no target `key=`) passing through
unchanged — plus `TestExtractErrorText_
LogfmtDecoyInsideQuotedValueIsNotExtracted` (a `key="..."` shaped substring
INSIDE another key's quoted value must never be read as a real pair —
`parseLogfmtPairs` is a real sequential tokenizer, not per-key independent
searches) and `TestExtractErrorText_JSONLinesStillTakeTheJSONPath` (a
regression guard: JSON-shaped content must never fall through to logfmt).
The leading-noise strip pass (`stripnoise_test.go`'s
`TestStripLeadingNoise_Table`): the exact round-4 Postgres live sample
pinned end to end (`TestStripLeadingNoise_EndToEndFromFingerprint`, driving
the REAL `fingerprint` output through the strip, not a hand-built
fixture), a line starting with meaningful text left byte-unchanged, an
all-placeholder line surviving unstripped (never returns empty), a
mid-line placeholder left alone (only a LEADING run strips), and
`TestStripLeadingNoise_PassesExtractedTextThrough` proving JSON/logfmt
extraction outputs are never accidentally stripped (their first token is
always real text). `-race` clean across the full package. `logmetrics/
logexporter_contract_test.go` proves the unmodified collector scrapes the
exporter's real contract end to end (fixture server → scrape → delta → gate
→ frame) for both the counters-only shape and the multi-series-per-
fingerprint sample shape (`TestCollector_
ScrapesLogExporterSampleContractEndToEnd`, `TestCollector_
LogExporterCatchAllNeverBecomesTheSample`), and statically pins the
collector's default `Options` against the exporter's exported constants.

## Implementation

`logexporter/` (`exporter.go` — `Options`/`Exporter`/`New`/`Run`, directory
rescan, HTTP server; `tailer.go` — per-file tail/rotate/truncate/drain state
machine; `classify.go` — the regex classifier + literal pre-filter, plus
`sampleFields` (the config list, renamed from `sampleJSONFields` in round 4
now that it drives both structured-extraction shapes — the public flag/
chart value names are unchanged for compatibility); `fingerprint.go` —
error-line normalization (UUID/hex/digit/whitespace collapse + truncation);
`jsonextract.go` — the extraction dispatcher (`extractErrorText`) + JSON
extraction (`extractFromJSON`, `looksLikeJSONObject`,
`jsonRawAsNonEmptyString`); `logfmtextract.go` — logfmt extraction
(`extractFromLogfmt`, `parseLogfmtPairs`, `readLogfmtQuotedValue`,
`logfmtKeyCandidates`); `stripnoise.go` — the leading-noise strip pass
(`stripLeadingNoise`, `isNoiseToken`); `cri.go` — CRI-format line splitting;
`discovery.go` — filename parsing, namespace filtering, pod→service
resolution; `metrics.go` — the counter registry (including `errorSlot`'s
per-fingerprint series + catch-all) + Prometheus text exposition with label
escaping); `main.go` (`--mode=log-exporter` dispatch branch + its flags,
mirroring the `node-exporter` branch exactly — unchanged in round 4, the
same `--log-exporter-sample-json-fields` flag now drives logfmt too);
`charts/triage-agent/values.yaml` (`logExporter.*`, including
`sampleJsonFields` — unchanged in round 4, no new chart surface was needed);
`charts/triage-agent/templates/beyla-daemonset.yaml` (third container +
`triage-log` Service + top-level gate + RBAC comment);
`charts/triage-agent/templates/deployment.yaml` (the two fail-guards + the
`$logExporterIsSource` scraper auto-wiring); `logmetrics/
logexporter_contract_test.go` (the cross-package contract pin — untouched
since round 3, since every extraction/normalization step happens upstream
of the wire contract). `logmetrics/` itself is unmodified across all four
rounds.
