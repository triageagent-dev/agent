//go:build obi

// OS-agnostic on purpose: obi.LoadConfig/obi.Config
// (go.opentelemetry.io/obi/pkg/obi) have no eBPF dependency and build on
// every OS the vendored module supports, unlike pkg/instrumenter
// (ebpf_obi.go, `obi && linux`-tagged). Keeping the config-essentials
// builder here — mirroring ebpf_reload.go's own "kept out of ebpf_obi.go's
// build tag so it is testable" precedent — lets ebpf_config_test.go run
// natively on the darwin dev host instead of only under a linux CI runner.
package main

import (
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/obi/pkg/obi"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
)

// applyOBILogLevel sets the level of OBI's logger from the config's
// log_level. OBI logs through the default slog logger, and only its own
// cmd/obi main applies log_level, so embedded here the key was ignored and
// OBI's debug lines (why a probe did not attach) could never be seen.
func applyOBILogLevel(cfg *obi.Config) error {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		return fmt.Errorf("OBI log_level %q: %w", cfg.LogLevel, err)
	}
	slog.SetLogLoggerLevel(lvl)
	return nil
}

// obiEssentialsYAML builds the OBI config this binary loads when no
// OBI_CONFIG_PATH file is mounted (ebpf_obi.go's runEBPF): the chartless/dev
// fallback for the same essentials the chart's ConfigMap otherwise supplies.
// excludes are k8s_owner_name GLOBS (Deployment/StatefulSet/DaemonSet owner
// name, e.g. "*synthetic*") emitted as discovery.exclude_instrument entries:
// high-churn workloads like synthetic-test runners spawn short-lived browser/
// ffmpeg processes whose attach/detach churn burns CPU+memory for RED metrics
// nobody reads.
//
// networkFlows gates the L4 network-flow metric family
// (docs/todo/service-graph-from-obi.md, OBI_NETWORK_FLOWS env var in
// ebpf_obi.go) — `network: {enable: true}` is the DEPRECATED toggle
// (third_party/obi/pkg/obi/config.go's Config.normalize), used deliberately:
// it ORs export.FeatureNetwork onto whatever Metrics.Features already is
// (DefaultConfig's own FeatureApplicationRED, which is what makes today's
// RED metrics work), where the "modern" metrics.features list would REPLACE
// it and silently turn RED metrics off unless re-enumerated. The k8s owner
// attributes (k8s.src/dst.owner.name/.namespace/.owner.type) and the
// `direction` attribute the flow-edge derivation needs are already
// default-enabled by the vendored OBI whenever attributes.kubernetes.enable
// is true (already set below, unconditionally) — see
// third_party/obi/pkg/export/attributes/attr_defs.go's networkKubeAttributes/
// networkAttributes groups — so no further attributes.select override is
// strictly required; one is emitted anyway, for explicitness against future
// OBI attribute-default drift.
func obiEssentialsYAML(namespaces, excludes []string, networkFlows bool) string {
	var sb strings.Builder
	sb.WriteString("discovery:\n  instrument:\n")
	for _, ns := range namespaces {
		fmt.Fprintf(&sb, "    - k8s_namespace: %q\n", ns)
	}
	// The pod-annotation excludes are always rendered, as the chart's
	// ConfigMap does: a pod whose template carries
	// triageagent.dev/instrument: "false" or triageagent.dev/triage: "false"
	// is never instrumented. Two entries, not one: the keys of a single
	// k8s_pod_annotations map must ALL match.
	sb.WriteString("  exclude_instrument:\n")
	for _, key := range []string{annotationkeys.Instrument, annotationkeys.Triage} {
		fmt.Fprintf(&sb, "    - k8s_pod_annotations:\n        %s: \"false\"\n", key)
	}
	for _, ex := range excludes {
		fmt.Fprintf(&sb, "    - k8s_owner_name: %q\n", ex)
	}
	// One "attributes:" top-level key — go-yaml keeps only the LAST of a
	// duplicate top-level key, so kubernetes/select must be siblings under
	// it, never emitted as two separate "attributes:" blocks.
	sb.WriteString("attributes:\n")
	sb.WriteString("  kubernetes:\n    enable: true\n    meta_restrict_local_node: true\n    disable_informers: [node]\n")
	if networkFlows {
		sb.WriteString("  select:\n    obi.network.flow:\n      include:\n")
		for _, attr := range []string{
			"k8s.src.owner.name", "k8s.src.namespace", "k8s.src.owner.type",
			"k8s.dst.owner.name", "k8s.dst.namespace", "k8s.dst.owner.type",
			"direction",
		} {
			fmt.Fprintf(&sb, "        - %s\n", attr)
		}
	}
	// name_resolver.sources: [k8s] is already the vendored default
	// (DefaultConfig.NameResolver) — made explicit here so this config
	// doesn't silently depend on that default surviving a future OBI
	// version bump. It rewrites an unresolved client-span server.address IP
	// to a k8s Service name where the k8s informer can attribute one; it is
	// NOT relied on for correctness — the ClusterIP index
	// (k8sresolver.go's ServiceByClusterIP, in the sibling redsource
	// package) resolves a raw IP address independently, and an address OBI
	// could not attribute at all exports as the literal "outgoing"
	// (RenameUnresolvedHostsOutgoing's default), which this repo's resolver
	// always treats as External.
	sb.WriteString("name_resolver:\n  sources: [k8s]\n")
	if networkFlows {
		// guess_ports: ordinal — a TCP connection opened before the eBPF
		// DaemonSet pod started has no observed SYN, so OBI cannot tell the
		// initiator and would otherwise export it with direction=unknown
		// (dropped by redsource.filterFlowSamples, which keeps only
		// direction=request). The ordinal guess (highest port = client) is
		// correct here because Linux ephemeral ports are >= 32768.
		sb.WriteString("network:\n  enable: true\n  source: socket_filter\n  guess_ports: ordinal\n")
	}
	// heuristic (OBI's own default) keeps real routes, collapsing the
	// segments it recognises as IDs to "*", so RED can judge each HTTP
	// operation on its own; the earlier wildcard folded every route into
	// "/**" and hid a slow or failing minority route
	// (docs/todo/baseline-gates.md). It reduces route cardinality but does
	// not bound it: the controller caps the operations it tracks per service
	// (redsource.BaselineOptions.MaxOperations).
	sb.WriteString("routes:\n  unmatched: heuristic\n")
	// Exported in process to the sensor stream (ebpf_metrics.go); the
	// interval is how often OBI hands the store a fresh cumulative snapshot.
	sb.WriteString("otel_metrics_export:\n  interval: 15s\n")
	return sb.String()
}
