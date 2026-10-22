# ADR-0017: OTLP Metrics Receiver on the Controller

- **Status:** Accepted. Implemented 2026-10-07.
- **Date:** 2026-10-07
- **Deciders:** Denis (Principal SRE)
- **Related:** ADR-0009 (RED metrics), ADR-0010 (USE metrics), ADR-0006
  (log-based metrics), ADR-0011/0013 (topology), ADR-0015 (sensor stream)

## Context

Every collector in the controller reads the sensor stream hub, and only the
eBPF DaemonSet feeds it. A cluster that already runs OpenTelemetry, with
SDKs in the services and a Collector in front of them, cannot run the
DaemonSet (managed nodes, sandboxed runtimes, no `hostPID`) or does not
want to. It gets virtual apps and Warning events from the Kubernetes API
and no metrics.

The RED profile already reads OpenTelemetry semantic conventions:
`http.server.request.duration`, `rpc.server.call.duration`, the client
histograms, `service.name`, `k8s.namespace.name`, `server.address`. OBI
emits them, and so do the SDKs. A Collector can also produce the USE and
LOG families from its own receivers (kubeletstats, filelog with the count
connector) once they are named the way the collectors read them.

## Decision

The controller serves the OTLP metrics service over gRPC
(`--otlp-listen`, chart `otlp.*`, port 4317 on the `<fullname>-sensors`
Service) and feeds what it receives into the same hub.

- **Accumulate per resource.** A sender splits one resource's metrics
  across batches. The receiver keeps each resource's newest cumulative point
  per series and hands the whole resource to the hub every 5 s when it
  changed. A resource is keyed by a hash of its attributes, so a restarted
  process (new `service.instance.id`) is a new source, as a restarted
  sensor's counters are.
- **Route by name.** `use.*` goes to the node-exporter role, `log.errors`,
  `log.warns` and `log.lines` to the log-exporter role, everything else
  (RED and client histograms, `obi.network.flow.bytes`) to the ebpf role.
  The collectors read them unchanged.
- **Normalize older conventions.** `http.server.duration`,
  `rpc.server.duration` and the client forms are renamed to the current
  names, millisecond bounds become seconds, `http.status_code` and
  `http.method` gain their current names, a numeric `rpc.grpc.status_code`
  becomes the code name on `rpc.response.status_code`, and
  `k8s.owner.name` is set from `k8s.deployment.name` (or the StatefulSet,
  DaemonSet, CronJob or Job name) that the Collector's k8sattributes
  processor adds.
- **Refuse what the collectors cannot diff.** Delta temporality and
  exponential histograms are refused per point and reported in the
  response's partial success.
- **Bound memory.** By default 2000 resources, 5000 series per resource,
  500 resources per authenticated sender and 4 MiB per request, each set by
  a flag (`--otlp-max-*`, chart `otlp.limits`); a resource silent for 10
  minutes is dropped. Past a limit, points are refused and reported, and
  counted per sender.
- **One hub, replicated.** Every replica receives. An OTLP source's entry
  is not tied to a replica the way a sensor's stream is: the newest
  snapshot wins on every replica and is gossiped like a sensor's, so the
  leader sees every source whichever replica a sender reached. OTLP
  sources stay out of `sensor_state`.
- **TLS on the sensor certificate.** The receiver serves the controller's
  self-signed certificate; senders pin it from the `<fullname>-sensor-ca`
  ConfigMap (TLS 1.2 or later).
- **Authentication, either of:**
  - a client certificate that chains to `otlp.clientCASecret` (mutual
    TLS), the OTel exporter's own `tls.cert_file`/`key_file`; or
  - a projected ServiceAccount token, audience `triage-agent-otlp`, of an
    account in `otlp.allowedServiceAccounts`, checked by TokenReview. The
    sensors' audience is not accepted.

  With neither configured the chart refuses to render.

## Consequences

- A cluster without the DaemonSet gets RED, SIG and L7 topology from its
  SDKs. USE and LOG arrive when its Collector emits the `use.*` and
  `log.*` names, and L4 flows when something emits
  `obi.network.flow.bytes` (OBI run elsewhere, for example).
- The receiver is the first port that applications reach. It is closed
  unless `otlp.enabled`, and every request authenticates.
- A source whose batches reach two replicas at once alternates between
  partial snapshots on the leader. A sender holds one gRPC connection, so
  this is rare; the deltas of the series it does carry stay correct.
- Error samples (`log.sample`) need the exemplar the log exporter adds; a
  Collector's count connector does not produce one, so LOG frames from it
  carry counts only.

## Amendment 2026-10-13: tenant client certificates

A Collector in another cluster cannot present a ServiceAccount token (this
cluster's API server cannot review it), and its namespaces do not exist here,
so the namespace annotation that routes a local namespace cannot place them.
The mutual-TLS client certificate carries the tenant instead.

- **Certificate.** URI SANs `spiffe://<domain>/tenant/<name>`, one per tenant.
  A name is lower case letters, digits and `-`, `_`, `.`. Such a certificate
  is a *tenant sender*: it may publish for any namespace, but only inside
  its tenants. Tenant URIs win over the `ns/<ns>/sa/<name>` form; a
  certificate with neither stays a forwarder named by its common name.
- **Which tenant.** The resource's `triage.tenant` attribute, or the
  certificate's only tenant when it names one. A certificate naming several
  tenants and a resource without the attribute is refused, as is a tenant
  the certificate does not name.
- **Bounded by the join.** A tenant must also be one the agent joined the
  core with (`--join-tenant`, `--target-tenant`). The core drops frames of a
  tenant outside the approved set, so the receiver refuses the resource
  with the reason instead. Adding a tenant is a join tenant and an approval,
  as for an annotated namespace; the certificate cannot widen it.
- **Isolation inside the agent.** Every namespace of a tenant sender, on the
  resource and on each point (and the Prometheus `namespace` label of
  cAdvisor series), is rewritten to `<tenant>~<namespace>` (package
  `internal/remotens`). A Kubernetes namespace cannot contain `~`, so a
  local namespace cannot be mistaken for one. The collectors keep working
  on the string, and two tenants never merge.
- **Push.** `tenantForNamespace` reads the tenant out of a qualified name.
  A tenant stays active for 15 minutes after its last data; its first data
  queues its snapshot sequence ahead of the data frame, so the core
  brackets the tenant's state. Every outbound frame and every OTLP source
  status has the prefix removed. An item whose namespace is empty after the
  prefix (a node-level series) is not forwarded.
- **Topology.** The destination of a remote tenant's client span is
  resolved from the host name: a bare name is a Service of the source's
  namespace, `<svc>.<ns>.svc[.<domain>]` names that namespace. The
  two-label `<svc>.<ns>` shorthand is not accepted, since with no index to
  confirm the Service `example.com` would be a Service in a namespace
  `com`. Other hosts are external.
- **Not covered.** Network reachability of the listener from outside the
  cluster, and issuing and rotating the certificates, are the installer's.
  Without join tenants (standalone) a tenant certificate is refused.

## Amendment 2026-10-13: the agent is its own client CA

A remote sender needs a client certificate, and asking each installer to run a
CA of their own put key custody outside the agent. With `otlp.ownCA` the
controller creates an ECDSA P-256 CA on first start in the Secret
`<release>-otlp-client-ca` (`ca.crt`, `ca.key`; replicas that start together
race on the first write and the loser reads the winner's CA) and adds it to the
client CA pool next to `otlp.clientCASecret`.

`triage-agent otlp-cert --tenant <a,b> --cn <name>` runs inside the controller
pod with its ServiceAccount, reads the CA from the Secret and issues a client
certificate with one `spiffe://<domain>/tenant/<tenant>` URI SAN per tenant. It
prints a Secret manifest (`tls.crt`, `tls.key`, and the agent's serving
certificate as `ca.crt` for the sender to pin), or the three PEM blocks. The
command does not check the tenants against the join tenants: the receiver does
that on every export, so a certificate for a tenant the agent does not serve is
refused with a reason.

The controller renews the CA when it starts inside 90 days of expiry. It keeps
the replaced one in the Secret (`ca.prev.crt`) and trusts both until the old
one expires, so certificates issued before a renewal keep working. A running
replica keeps the pool it loaded, so a certificate from the new CA is refused
by a replica that has not restarted yet; issue certificates after a renewal
has rolled through every replica. `otlp-cert` only reads the CA: it never
creates or renews one. The controller logs whether the CA was created,
renewed or already there.

The output of `otlp-cert` pins the agent's serving certificate. Changing
`otlp.extraNames` reissues that certificate, so set the names before issuing
the first sender certificate.
