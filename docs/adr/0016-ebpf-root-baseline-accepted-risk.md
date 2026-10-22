# ADR-0016: The eBPF DaemonSet Runs as Root with hostPID (Accepted Risk)

- **Status:** Accepted.
- **Date:** 2026-10-07
- **Deciders:** Denis (Principal SRE)
- **Related:** ADR-0009 (RED metrics), ADR-0013 (red-source profiles),
  ADR-0014 (log exporter in the agent DaemonSet), ADR-0015 (sensor stream),
  `PASSPORT.md` §5

## Context

The DaemonSet runs every sensor role (`ebpf`, `node-exporter`,
`log-exporter`) in one container, `agent`, so all three share one security
context. The `ebpf` role embeds OBI, which attaches uprobes and kprobes to
the HTTP and gRPC code of other containers' processes on the node.

That needs:

- `hostPID: true`, to see and hook other containers' processes. A pod-level
  namespace share has no capability substitute.
- `runAsUser: 0` with the capabilities `BPF`, `PERFMON`, `NET_RAW`,
  `SYS_PTRACE`, `DAC_READ_SEARCH`, `CHECKPOINT_RESTORE` to load programs,
  read other processes' executables and attach to them.

The chart's default (`ebpf.privileged: false`) narrows the rest:
`privileged: false`, `allowPrivilegeEscalation: false`,
`readOnlyRootFilesystem: true`, seccomp `RuntimeDefault`, `drop: ALL` before
the list above.

## Decision

Accept user 0, `hostPID` and the six capabilities above as the baseline for
the DaemonSet. Anything wider is an exception an operator opts into, under
the rules below.

| Setting | Allowed when | Why |
|---|---|---|
| `ebpf.extraCapabilities: [SYS_ADMIN]` | Go services must be instrumented on a kernel that refuses Go uprobes without it. Known case: GKE COS 6.12, where every Go uprobe fails with "opening perf event: permission denied" and Go services report no RED. | Restores RED for Go services. Narrower than `privileged: true` |
| `ebpf.extraCapabilities: [SYS_RESOURCE]` | Kernels before 5.11, where the BPF verifier limits are not lifted by the baseline set | Lets OBI's programs load |
| `ebpf.privileged: true` | Only when the kernel cannot load OBI's programs under the baseline set plus the extra capabilities above, and the cluster owner agrees in writing | Drops every restriction above except the read-only root filesystem. Last resort |

Try the baseline first. Add one extra capability at a time and confirm
the failure it fixes before adding the next.

## Consequences

**Accepted:** a compromise of the `agent` container on a node can read and
trace other processes on that node. The read-only root filesystem, the
seccomp profile and the dropped capabilities limit what it can change.

**Accepted:** the `node-exporter` and `log-exporter` roles run with the same
context as `ebpf`, although they need less. Splitting them into their own
container would narrow them; it is not done today.

**Required:** an operator who enables `SYS_ADMIN` or `ebpf.privileged`
records why, per cluster.

**Detection:** the ebpf role reports executables whose uprobes failed to
attach as its `lastError` in `sensor_state`, naming the `SYS_ADMIN` fix, so
the case shows without OBI's debug logging.
