# ADR-0021: OTLP-only agent watches no Kubernetes objects

- **Status:** Accepted. Implemented 2026-10-15; not yet run on a cluster.
- **Date:** 2026-10-15
- **Deciders:** Denis (Principal SRE)
- **Related:** ADR-0005 (push mode), ADR-0015 (sensor stream), ADR-0017 (OTLP
  receiver), ADR-0018 (Linux host mode)

## Context

An agent fed only by OpenTelemetry Collectors (ADR-0017) has no eBPF
DaemonSet and no log exporter, yet the controller still ran every Kubernetes
input of push mode: a cluster-wide Namespace informer (for the tenant
annotation), Warning Events, Services and Ingresses (topology edge
destinations), Deployments, StatefulSets and DaemonSets (virtual apps) and
owner reads for pod identity. The chart therefore always created a ClusterRole
and ClusterRoleBinding, and a second ClusterRole for TokenReview. On a cluster
where the installer holds only namespace rights, an agent that never needed to
look at the cluster could not be installed.

Linux host mode (ADR-0018) already shows the pusher can run with none of those
inputs. It cannot serve here: its sensors are local (cgroups, journald, OBI)
and it has no OTLP receiver, no sensor-stream listener and no replica mesh.

## Decision

`--kubernetes=false` (chart `kubernetes.watch: false`) runs the controller path
without Kubernetes inputs:

- `push.Options.NoKubernetes` makes the cluster Pusher skip `startKubernetes`:
  no Namespace, Service, Ingress, workload or Event informer, and virtual apps
  are forced off. Everything else is unchanged: the OTLP receiver and its
  mutual TLS, the sensor hub, the collectors, the replica mesh, leader election
  and the join certificate.
- The tenant cannot come from a namespace annotation, so `--target-tenant` is
  required (the chart fails the render without `target.tenant`), and every
  frame files under it. `--triage-endpoint` is required too.
- Scope: with no annotation watcher, an empty `--target-namespaces` means every
  namespace an authenticated sender reports, not the annotated set. A list
  still narrows it. The senders' client certificates are what bound the data.
- The collectors take no cluster resolver: USE uses `NoIdentityResolver` and
  keeps the exporter's service label, RED and topology get no Service/Ingress
  or workload resolver, and the pod-exclusion hook answers false without
  reading a pod.
- Applications come from OTLP, through the same `AppSource` host mode uses
  (`AppEvent` gains `Namespace`, `Replicas` and `ReadyReplicas`;
  `SetAppSource` accepts a `NoKubernetes` Pusher, whose scope is then every
  namespace the source names). The source (`cmd/triage-agent/otlp_apps.go`)
  reads the workload metrics an OpenTelemetry Collector's `k8s_cluster`
  receiver sends: `k8s.deployment.desired/available`,
  `k8s.statefulset.desired_pods/ready_pods` and
  `k8s.daemonset.desired_scheduled_nodes/ready_nodes`. A workload is one app,
  ready when ready pods reach desired, with both counts on the component; one
  scaled to zero makes none. A Collector that sends none of them leaves no
  readiness to read, so each (namespace, service) an OTLP sender reports is an
  app instead, ready while its metrics are fresh. The two are never mixed.
  The Collector must send those metrics (add `k8s_cluster` to the triage
  pipeline and let the names through its filter), and scope them to the
  tenant's namespaces: the agent files everything under one tenant.
- The manager's cache is scoped to the pod's namespace and opens no watch.
- Chart: no ClusterRole or ClusterRoleBinding. The TokenReview ClusterRole is
  now rendered only when a token authenticates something (eBPF DaemonSet roles,
  or `otlp.allowedServiceAccounts`), in every mode. What remains is namespaced:
  the leader-election Role, the join, sensor-TLS and OTLP client-CA Secrets.

## Consequences

- Installable with namespace-scoped rights only.
- No Warning events and no label-derived virtual apps (no
  `triageagent.dev/virtual-app*` grouping). Applications are single workloads
  or services, readiness is what the Collector reports, and topology edges
  resolve their destination from the host name alone.
- One tenant per agent. A multi-tenant cluster-mode agent still needs the
  annotation watcher.
- A default install (`kubernetes.watch: true`) renders as before, except that
  an OTLP-only release no longer gets the TokenReview ClusterRole.

## Tests

`TestNoKubernetes_StartRegistersNoInformers` (a Pusher with no cache starts and
speaks the protocol), `TestNoKubernetes_RoutesByTenantAndReadsNothing`,
`TestNoIdentityResolver_ResolvesNothing`, `TestNoKubernetes_AppSourceBuildsAppsPerNamespace`,
`TestWorkloadApps_ReadinessFromDesiredAndReady`, `TestOTLPApps_*`; chart renders checked by hand for
watch on and off, a missing tenant and the TokenReview conditions.
