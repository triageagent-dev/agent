package redsource

import "fmt"

// ProfileOBI targets the embedded OBI (OpenTelemetry eBPF Instrumentation)
// engine's application-mode instrumentation (docs/adr/0009-red-metrics-beyla.md,
// docs/adr/0013-red-source-profiles.md). The metric names, label keys, error
// rule, and client-family/server-address edge shape below match OBI's own
// naming exactly. The server-side RED metric names only the measured
// service itself, so topology edges are derived from three client-side
// metrics, each decorated with the calling process's own identity (the SAME
// namespace/service attribute keys) plus a server.address attribute naming
// the callee, resolved through Options.Resolver.
//
// Name is "ebpf": the user-visible profile selector names the signal family
// (eBPF-sourced RED metrics) this profile produces, matching the --red-source
// flag and --mode=ebpf role that produce it.
var ProfileOBI = Profile{
	Name: "ebpf",

	// Server-side RED: one family per protocol, summed per service. The
	// histogram carries both the request count and the duration buckets, so
	// each family names the same metric twice. gRPC reports the status as
	// the code NAME on rpc.response.status_code (IsGRPCServerError).
	RequestMetrics: []RequestMetric{
		{
			MetricName:           "http.server.request.duration",
			DurationBucketMetric: "http.server.request.duration",
			StatusLabel:          "http.response.status_code",
			SampleLabels:         []string{"http.request.method", "http.route"},
			IsError:              IsHTTP5xxError,
			FormatSample:         formatSampleOBI,
		},
		{
			MetricName:           "rpc.server.call.duration",
			DurationBucketMetric: "rpc.server.call.duration",
			StatusLabel:          "rpc.response.status_code",
			SampleLabels:         []string{"rpc.method"},
			IsError:              IsGRPCServerError,
			FormatSample:         formatSampleGRPC,
		},
	},

	DestNamespaceLabel: "k8s.namespace.name",
	DestWorkloadLabel:  "service.name",
	// SourceNamespaceLabel/SourceWorkloadLabel are the SAME attribute keys
	// as DestNamespaceLabel/DestWorkloadLabel above: OBI decorates every
	// span, server or client, with the emitting process's own identity under
	// these names. On the client-side metrics below they carry the caller
	// identity edges are built from — see computeClientEdgeDeltas.
	SourceNamespaceLabel: "k8s.namespace.name",
	SourceWorkloadLabel:  "service.name",

	ClientMetrics: []ClientMetric{
		{MetricName: "http.client.request.duration", Protocol: "http"},
		{MetricName: "rpc.client.call.duration", Protocol: "grpc"},
		{MetricName: "db.client.operation.duration", Protocol: "db"},
	},
	ServerAddressLabel: "server.address",

	// SourceOwnerLabel: the emitting workload's own Deployment/StatefulSet/
	// DaemonSet name — resolved through Options.Identity into the virtual-
	// app identity that workload groups into (docs/todo/service-graph-from-obi.md),
	// with a fallback to the plain service.name value above when the owner
	// isn't a tracked workload or no Identity resolver is wired.
	SourceOwnerLabel: "k8s.owner.name",

	// Network-flow (L4) edge derivation — see FlowMetric's doc comment.
	// Label keys match the vendored OBI's default attribute names
	// (third_party/obi/pkg/export/attributes/names/attrs.go) once
	// `network: {enable: true}` is set on the OBI instance (chart value
	// ebpf.networkFlows.enabled) — absent that, the metric is not in the
	// snapshot and this whole path is a no-op.
	FlowMetric:            "obi.network.flow.bytes",
	FlowSrcNamespaceLabel: "k8s.src.namespace",
	FlowSrcOwnerLabel:     "k8s.src.owner.name",
	FlowSrcOwnerTypeLabel: "k8s.src.owner.type",
	FlowDstNamespaceLabel: "k8s.dst.namespace",
	FlowDstOwnerLabel:     "k8s.dst.owner.name",
	FlowDstOwnerTypeLabel: "k8s.dst.owner.type",
	FlowDstPortLabel:      "dst.port",
	FlowDirectionLabel:    "direction",
}

// formatSampleOBI renders "METHOD ROUTE -> STATUS xN", including the "?"
// fallback for a missing method/route.
func formatSampleOBI(ctx SampleContext) string {
	method := ctx.Extra["http.request.method"]
	route := ctx.Extra["http.route"]
	if method == "" {
		method = "?"
	}
	if route == "" {
		route = "?"
	}
	return fmt.Sprintf("%s %s -> %s x%d", method, route, ctx.Status, ctx.Delta)
}

// formatSampleGRPC renders "grpc METHOD -> STATUS xN", including the "?"
// fallback for a missing method.
func formatSampleGRPC(ctx SampleContext) string {
	method := ctx.Extra["rpc.method"]
	if method == "" {
		method = "?"
	}
	return fmt.Sprintf("grpc %s -> %s x%d", method, ctx.Status, ctx.Delta)
}
