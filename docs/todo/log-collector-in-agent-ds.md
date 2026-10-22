# Native log collection in the agent DaemonSet (retire Fluent Bit)

Status: direction set 2026-08-21 (operator decision), not designed in detail,
not started. Supersedes the Fluent Bit dependency for the log signal family.

## Decision

The log family (per-service error/warn/total counts feeding the `log_metrics`
frame) should be collected by the agent's own DaemonSet, the same DS pod that
already carries Beyla (RED, topology) and the node-exporter container (USE).
Fluent Bit is avoided going forward: no separate DaemonSet to install, no
log_to_metrics pipeline to template, one fewer moving part per cluster.

## Shape (to be designed)

- New collection mode in the DS pod, sibling of `--mode=node-exporter`: tail
  the kubelet-written container log files (`/var/log/containers/*.log`,
  read-only hostPath, unprivileged) and count severity matches per
  (namespace, service), resolving service from pod labels the same way the
  other collectors do. Counts only; log lines never leave the node.
- The wire contract does not change: the same `log_metrics` frame, the same
  agent-side threshold gate (>=10 errors/window today), the same first-seen
  baseline discipline. The hub needs no changes at all.
- The existing `logmetrics/` scrape-Fluent-Bit collector stays until the DS
  mode ships, then both the collector and the `fluentbit.*` chart values are
  retired.

## Rollout

1. Ship the DS log mode in the agent (chart toggle, off by default).
2. Enable it first on deployments that run no Fluent Bit today — there it
   simply turns the log family on for the first time, nothing to compare
   against.
3. On deployments running the Fluent Bit DS: upgrade the agent, enable the
   DS mode, verify counts match the Fluent Bit path, then remove the
   `triage-agent-fluentbit` DaemonSets.

## Open questions

- Severity detection: reuse the level-field parsing Fluent Bit's pipeline did
  (JSON `level`/`severity` fields plus plain-text ERROR/WARN match) or start
  stricter. Needs a decision before implementation.
- Multiline/stack traces: counting raw lines overcounts Java stacks; the
  Fluent Bit pipeline had the same weakness, so parity is acceptable for v1.
- File rotation and node restart: reuse the first-seen/counter-reset handling
  from `deltas.go`.
