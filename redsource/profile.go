package redsource

import "strconv"

// ClientMetric names one client-side metric topology edges are derived
// from — see Profile.ClientMetrics.
type ClientMetric struct {
	// MetricName is the metric name: a counter, or a histogram whose point
	// value is its count.
	MetricName string
	// Protocol is stamped onto every TopologyEdge this family produces.
	Protocol string
}

// SampleContext carries the fields available when a Profile formats one
// destination's representative failing sample (RequestMetric.FormatSample) — the
// winning (largest-delta) series for that destination this window.
type SampleContext struct {
	DstNamespace, DstWorkload string
	Status                    string
	Delta                     int64
	// Extra holds the values of RequestMetric.SampleLabels, captured from the
	// exact series that produced Delta, e.g. the OBI profile's
	// http_request_method/http_route, which the requests-metric grouping
	// itself deliberately ignores (see requestGroup's doc comment in
	// deltas.go) but a sample still wants to show.
	Extra map[string]string
}

// RequestMetric names one server-side request metric RED and topology nodes
// are derived from — see Profile.RequestMetrics. Each protocol family the
// instrumenter reports (HTTP, gRPC, ...) is one RequestMetric, with its own
// status label and its own notion of which status values are errors; the
// engine sums them per destination service.
type RequestMetric struct {
	// MetricName is the server-side request metric: a counter, or a
	// histogram whose point value is its count.
	MetricName string
	// DurationBucketMetric is the histogram P99Ms is interpolated from
	// (display-only, never gates).
	DurationBucketMetric string
	// StatusLabel is the attribute key IsError classifies the value of.
	StatusLabel string
	// SampleLabels are extra label keys captured per MetricName sample and
	// made available to FormatSample via SampleContext.Extra, e.g. the OBI
	// HTTP metric's method+route. Empty for a metric with nothing extra to
	// show.
	SampleLabels []string
	// IsError classifies a StatusLabel VALUE as an error. Required.
	IsError func(status string) bool
	// FormatSample renders one destination's representative failing sample
	// for the window. Required.
	FormatSample func(SampleContext) string
}

// Profile is the instrumenter's naming table a redsource.Collector is driven
// by — metric names, attribute keys, an error predicate and a sample
// formatter. The engine (collector.go/deltas.go) reads Profile fields and
// never branches on a profile's name. RequestMetrics are server-side and
// name only the measured service; edges come from ClientMetrics, whose
// points carry the caller's identity and the callee as server-address text,
// and from FlowMetric. See profiles.go for ProfileOBI.
type Profile struct {
	// Name identifies the profile for logging and the --red-source/
	// redSource.profile value.
	Name string

	// RequestMetrics are the server-side request metrics RED and topology
	// nodes are derived from, e.g. HTTP and gRPC. A destination's request
	// and error counts, p99 buckets and node presence are the sum over all
	// of them; each keeps its own delta baselines.
	RequestMetrics []RequestMetric

	// Attribute keys read off each RequestMetrics point (every request
	// metric shares the app identity attributes; the status label is per
	// RequestMetric).
	DestNamespaceLabel string
	DestWorkloadLabel  string
	// SourceNamespaceLabel/SourceWorkloadLabel: the caller's identity on
	// each ClientMetrics point.
	SourceNamespaceLabel string
	SourceWorkloadLabel  string

	// ClientMetrics are the client-side metrics topology edges are derived
	// from; ServerAddressLabel names the callee on each of their points,
	// resolved through Options.Resolver.
	ClientMetrics      []ClientMetric
	ServerAddressLabel string

	// SourceOwnerLabel names the k8s owner-object (Deployment/StatefulSet/
	// DaemonSet) name label the vendored eBPF instrumenter attaches to every
	// span describing the SPAN'S OWN emitting workload — read on
	// RequestMetrics for the destination/node identity (they are
	// server-side, so "dest" here IS "self") and on each ClientMetrics family for the source identity (same
	// key, same "self" semantics, a different family). Resolved through
	// Options.Identity (push.Pusher.IdentityFor) into the virtual-app
	// identity that workload's Deployment/StatefulSet/DaemonSet groups into;
	// "" (no Identity resolver wired, or the owner is not a tracked
	// workload) falls back to the plain DestWorkloadLabel/SourceWorkloadLabel
	// value (docs/todo/service-graph-from-obi.md).
	SourceOwnerLabel string

	// FlowMetric names an OBI network-flow BYTES counter topology
	// edges are ALSO derived from, independent of RequestMetrics/
	// ClientMetrics above — an L4 signal that observes protocols no L7
	// client family ever does (raw TCP, TLS, NATS, ...). "" disables
	// flow-derived edges entirely; with the chart's network-flows toggle off
	// the metric is simply absent from the snapshot, and
	// computeFlowEdgeDeltas is a no-op on a nil family. A flow point names
	// both ends directly, under the Flow*Label keys below.
	FlowMetric string
	// Flow{Src,Dst}{Namespace,Owner,OwnerType}Label name the network-flow
	// family's k8s attribute labels — see computeFlowEdgeDeltas/
	// filterFlowSamples for exactly how they gate/dedupe a row.
	FlowSrcNamespaceLabel string
	FlowSrcOwnerLabel     string
	FlowSrcOwnerTypeLabel string
	FlowDstNamespaceLabel string
	FlowDstOwnerLabel     string
	FlowDstOwnerTypeLabel string
	// FlowDstPortLabel, when set, names the label carrying the flow's
	// destination port. "" (or a row without it) leaves the edge's DstPort 0.
	FlowDstPortLabel string
	// FlowDirectionLabel, when set, names the label whose "request" value
	// marks the row captured from the flow's INITIATOR side (src). ""
	// means the vendored OBI build in use does not expose the attribute, so
	// filterFlowSamples keeps both directions instead of filtering — a
	// documented v1 limitation (docs/todo/service-graph-from-obi.md), not a
	// silently-wrong count: a bidirectional flow with no direction signal
	// would otherwise have exactly half its byte count dropped by whichever
	// convention filtering guessed wrong.
	FlowDirectionLabel string
}

// IsHTTP5xxError is the "HTTP 5xx is an error" predicate ProfileOBI uses for
// the http.server.request.duration IsError.
func IsHTTP5xxError(status string) bool {
	code, err := strconv.Atoi(status)
	if err != nil {
		return false
	}
	return code >= 500
}

// IsGRPCServerError is the "gRPC server error" predicate ProfileOBI uses for
// the rpc.server.call.duration IsError. It follows the OpenTelemetry semantic
// convention for gRPC SERVER spans (semantic-conventions, gRPC spans: "for
// server spans ... only UNKNOWN, DEADLINE_EXCEEDED, UNIMPLEMENTED, INTERNAL,
// UNAVAILABLE and DATA_LOSS set the span status to Error"): every other code
// is the caller's fault or an expected outcome, not a server failure. status
// is the code NAME OBI exports on rpc.response.status_code (third_party/obi/
// pkg/appolly/app/request/grpc_status.go's GRPCStatusCodeString); a missing
// or unrecognised value is not an error.
func IsGRPCServerError(status string) bool {
	switch status {
	case "UNKNOWN", "DEADLINE_EXCEEDED", "UNIMPLEMENTED", "INTERNAL", "UNAVAILABLE", "DATA_LOSS":
		return true
	default:
		return false
	}
}
