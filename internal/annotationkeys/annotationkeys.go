// Package annotationkeys declares the annotation keys this agent reads off
// namespaces and workloads: tenant, tier, weight, domain, description,
// virtual-app, virtual-app-group, virtual-app-identity, instrument and
// triage. Every key lives
// under the triageagent.dev/ prefix. This is the one place that prefix is
// spelled out as a string literal; every reader and writer uses these
// constants instead of a literal of its own.
package annotationkeys

const (
	Tenant             = "triageagent.dev/tenant"
	Tier               = "triageagent.dev/tier"
	Weight             = "triageagent.dev/weight"
	Domain             = "triageagent.dev/domain"
	Description        = "triageagent.dev/description"
	VirtualApp         = "triageagent.dev/virtual-app"
	VirtualAppGroup    = "triageagent.dev/virtual-app-group"
	VirtualAppIdentity = "triageagent.dev/virtual-app-identity"
)

// Instrument is "false" to exclude a pod's processes from eBPF
// instrumentation entirely. Set on the pod template's own annotations, not
// on the workload's own metadata: OBI's exclude_instrument matches it on the
// pod (the chart's ebpf ConfigMap and obiEssentialsYAML render it).
const Instrument = "triageagent.dev/instrument"

// Triage is "false" to take a workload, or every workload in a namespace,
// out of triage entirely: the agent sends the core nothing about it — no
// Kubernetes events for its pods or owners, no log, RED, USE or topology
// signal, and no virtual app. It also implies triageagent.dev/instrument:
// "false" for eBPF. Set on the workload's own metadata.annotations
// (Deployment, StatefulSet, DaemonSet, Rollout, Job, CronJob), on its pod
// template's annotations, or on the Namespace. A Namespace "false" covers
// every workload in it; a workload "true" does not override it. Any value
// other than exactly "false", or absence, keeps the workload in.
const Triage = "triageagent.dev/triage"
