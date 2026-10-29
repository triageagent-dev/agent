// Package push implements outbound push mode: the controller dials a triage agent
// over an authenticated WebSocket and streams its Application inventory + deltas +
// Kubernetes Warning events + log-derived error/warn metrics + eBPF-derived RED
// metrics + cgroup/PSI-derived USE metrics + a periodic observed-topology snapshot,
// for closed clusters that triage cannot reach via the API (see
// docs/adr/0005-outbound-push-mode.md, docs/adr/0006-log-based-metrics.md,
// docs/adr/0009-red-metrics-beyla.md, docs/adr/0010-use-metrics-node-exporter.md,
// docs/adr/0011-topology-snapshot.md; triage side ADR-029/ADR-045/ADR-046).
//
// The frame types here MUST stay wire-compatible with the triage receiver's
// internal/remoteagent/protocol.go — they are separate Go modules, so the types are
// duplicated, not imported. Both sides marshal a VirtualApp and *corev1.Event; their
// JSON field tags are identical.
package push

import (
	"encoding/json"
	"fmt"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
)

// ProtocolVersion is the wire protocol major version (must match the receiver).
const ProtocolVersion = 1

// Frame kinds (agent→hub unless noted).
const (
	KindHello            = "hello"
	KindSnapshot         = "snapshot"
	KindSnapshotEnd      = "snapshot_end"
	KindAppDelta         = "app_delta"
	KindK8sEvent         = "k8s_event"
	KindLogMetrics       = "log_metrics"
	KindRedMetrics       = "red_metrics"
	KindRedSummary       = "red_summary"
	KindUseMetrics       = "use_metrics"
	KindUseSummary       = "use_summary"
	KindTopologySnapshot = "topology_snapshot"
	// KindSensorState carries each sensor pod's own health (ADR-0015 phase
	// 1), not application data: goroutines, memory, gRPC send/ack timings and
	// per-role gather stats, mapped from sensorstream.Hub.Nodes(). Routed
	// specially (see splitFrameByTenant) since it has no namespace to
	// resolve a tenant from.
	KindSensorState = "sensor_state"
	KindHeartbeat   = "heartbeat"
	KindPong        = "pong" // hub→agent
	// KindResync is hub→agent: {v, kind:"resync", tenant}. The core asks this
	// agent to resend one tenant's full snapshot — its service graph reloaded
	// and it wants the same resync the agent already sends after a fresh
	// handshake, without forcing a reconnect. Tenant names which tenant;
	// empty means the session's handshake tenant (see
	// Pusher.handleResyncFrame, push/tenant_routing.go). No payload struct —
	// the top-level V/Kind/Tenant fields are the whole frame.
	KindResync = "resync"
	// KindLimits is hub→agent: {v, kind:"limits", limits:{eventsPerDay:
	// {tenant: n}}}, the daily event cap of each tenant this agent serves (0
	// = uncapped), sent when the session opens and when the core's policy
	// changes. The agent holds its Kubernetes events to it (event_caps.go);
	// the core caps every event it derives either way.
	KindLimits = "limits"

	// Certificate join (core docs/todo/agent-join.md). KindChallenge is
	// hub→agent, the first frame when the core runs the join flow; the
	// agent's first frame signs its nonce. KindJoin and KindRenew ask for a
	// certificate; KindJoinResult is the hub's answer, after which it closes.
	KindChallenge  = "challenge"
	KindJoin       = "join"
	KindRenew      = "renew"
	KindJoinResult = "join_result"
)

// Join result statuses.
const (
	JoinPending = "pending"
	JoinIssued  = "issued"
	JoinRefused = "refused"
)

// MaxLogMetricsServices caps the number of ServiceLogMetrics entries in a single
// log_metrics frame. A window that qualifies more services than this is split into
// multiple frames (see logmetrics.Collector).
const MaxLogMetricsServices = 100

// MaxLogMetricsSampleLen caps ServiceLogMetrics.Sample; longer samples are
// truncated before send (see logmetrics.Collector).
const MaxLogMetricsSampleLen = 256

// MaxLogMetricsSamples caps ServiceLogMetrics.Samples.
const MaxLogMetricsSamples = 3

// MaxRedMetricsServices caps the number of ServiceRedMetrics entries in a single
// red_metrics frame (docs/adr/0009-red-metrics-beyla.md). Unlike log_metrics, a
// window that qualifies more services than this is NOT split into multiple frames:
// redsource.Collector keeps only the top services by error delta and drops the
// rest, since a wire-level error storm can qualify far more services in one window
// than a log-line counter typically does.
const MaxRedMetricsServices = 100

// MaxRedMetricsSampleLen caps ServiceRedMetrics.Sample; longer samples are
// truncated before send (see redsource.Collector).
const MaxRedMetricsSampleLen = 256

// MaxOperationLen caps ServiceRedMetrics.Operation. An operation is a method
// plus a route or RPC name, which the instrumented service controls, so it is
// clipped before send like Sample.
const MaxOperationLen = 128

// MaxUseMetricsServices caps the number of ServiceUseMetrics entries in a single
// use_metrics frame (docs/adr/0010-use-metrics-node-exporter.md). Same posture as
// red_metrics: usemetrics.Collector keeps only the top services (by memPsiAvg60)
// and drops the rest for that window rather than chunking.
const MaxUseMetricsServices = 100

// MaxUseMetricsSampleLen caps ServiceUseMetrics.Sample; longer samples are
// truncated before send (see usemetrics.Collector).
const MaxUseMetricsSampleLen = 256

// MaxTopologyNodes / MaxTopologyEdges cap one topology_snapshot frame
// (docs/adr/0011-topology-snapshot.md). Unlike red_metrics/use_metrics, there is no
// gate deciding which nodes/edges qualify — every node/edge observed this window is
// a candidate — so redsource.Collector keeps the top MaxTopologyEdges by Calls and
// the first MaxTopologyNodes alphabetically (a stable, deterministic choice, not a
// magnitude-ranked one — a node has no natural "score"), logging dropped counts at
// Info when either cap trims the set. No silent truncation.
const MaxTopologyNodes = 200
const MaxTopologyEdges = 500

// MaxTopologyExternalLen caps TopologyEdge.External; longer values are truncated
// before send (see redsource.Collector).
const MaxTopologyExternalLen = 128

// Delta operations for KindAppDelta.
const (
	OpAdd    = "add"
	OpUpdate = "update"
	OpDelete = "delete"
)

// Frame is the on-the-wire envelope. Field names/tags mirror the receiver exactly.
type Frame struct {
	V       int    `json:"v"`
	Kind    string `json:"kind"`
	Cluster string `json:"cluster,omitempty"`
	// Tenant is the per-frame tenant stamp (multi-tenant push over ONE
	// connection — see tenant_routing.go). Absent/empty means the session's
	// handshake tenant (Options.Tenant, declared in the signed hello), which
	// keeps a plain single-tenant deployment's frames byte-identical to
	// before this field existed. A non-empty value names a DIFFERENT tenant
	// this frame belongs to; the receiver validates it against the cluster's
	// pinned tenant set and files the frame under it. Mirror of the core's
	// optional envelope field — same JSON name, do not deviate.
	//
	// In CLUSTER MODE (Options.Tenant == "", no session/handshake tenant —
	// see the Pusher-side doc addendum in pusher.go/tenant_routing.go) this
	// field is never left absent/empty on a frame this agent actually sends:
	// every frame is stamped with its real, namespace-resolved tenant.
	Tenant string `json:"tenant,omitempty"`

	Hello            *HelloPayload            `json:"hello,omitempty"`
	Snapshot         *SnapshotPayload         `json:"snapshot,omitempty"`
	AppDelta         *AppDeltaPayload         `json:"appDelta,omitempty"`
	K8sEvent         *K8sEventPayload         `json:"k8sEvent,omitempty"`
	LogMetrics       *LogMetricsPayload       `json:"logMetrics,omitempty"`
	RedMetrics       *RedMetricsPayload       `json:"redMetrics,omitempty"`
	RedSummary       *RedSummaryPayload       `json:"redSummary,omitempty"`
	UseMetrics       *UseMetricsPayload       `json:"useMetrics,omitempty"`
	UseSummary       *UseSummaryPayload       `json:"useSummary,omitempty"`
	TopologySnapshot *TopologySnapshotPayload `json:"topologySnapshot,omitempty"`
	SensorState      *SensorStatePayload      `json:"sensorState,omitempty"`

	Challenge  *ChallengePayload  `json:"challenge,omitempty"`
	Join       *JoinPayload       `json:"join,omitempty"`
	JoinResult *JoinResultPayload `json:"joinResult,omitempty"`
	Limits     *LimitsPayload     `json:"limits,omitempty"`
}

// LimitsPayload carries the caps of a limits frame.
type LimitsPayload struct {
	EventsPerDay map[string]int64 `json:"eventsPerDay"`
	// Namespaces maps a tenant ID to the namespaces the agent may send for
	// it. A tenant that is absent, or has an empty list, takes every
	// namespace. The core sends it at every session open and when the
	// bindings change; each frame replaces the whole scope.
	Namespaces map[string][]string `json:"namespaces,omitempty"`
}

// ChallengePayload mirrors the core's: a nonce, the core's agent CA
// certificate, the cross certificate during a CA rotation, and the CA key's
// signature over the nonce. A core that binds the challenge to the connection
// echoes the agent's X-Triage-Agent-Nonce in AgentNonce and signs it too; one
// that also binds the TLS channel sets ChannelBound and signs the connection's
// exporter keying material as well (ChannelBindingLabel, ChannelBindingLen).
type ChallengePayload struct {
	Nonce      []byte `json:"nonce"`
	AgentNonce []byte `json:"agentNonce,omitempty"`
	CACert     []byte `json:"caCert"`
	CrossCert  []byte `json:"crossCert,omitempty"`
	Sig        []byte `json:"sig"`

	ChannelBound bool `json:"channelBound,omitempty"`
}

// Channel binding of the join challenge: both sides derive ChannelBindingLen
// bytes of TLS exporter keying material under ChannelBindingLabel (RFC 5705,
// no context).
const (
	ChannelBindingLabel = "EXPORTER-triage-agent-join"
	ChannelBindingLen   = 32
)

// JoinPayload carries a join request (CSR, Proof) or a renewal (also Cert).
type JoinPayload struct {
	CSR   []byte `json:"csr"`
	Cert  []byte `json:"cert,omitempty"`
	Proof []byte `json:"proof"`
	// EnrollKey is a one-time agent key (core ADR-090): a join that carries
	// one valid for its cluster and tenants is issued at once.
	EnrollKey string `json:"enrollKey,omitempty"`
}

// JoinBinding is one tenant binding of a key-joined agent: the namespaces
// the core accepts from it for that tenant (empty means every namespace).
type JoinBinding struct {
	Tenant     string   `json:"tenant"`
	Namespaces []string `json:"namespaces,omitempty"`
}

// JoinResultPayload is the core's answer to a join or renewal.
type JoinResultPayload struct {
	Status  string `json:"status"`
	Cert    []byte `json:"cert,omitempty"`
	CACert  []byte `json:"caCert,omitempty"`
	Message string `json:"message,omitempty"`
	// Bindings are the agent's tenant bindings when it joined with a key.
	Bindings []JoinBinding `json:"bindings,omitempty"`
}

// HelloPayload is the first frame sent after the WebSocket upgrade. It carries
// the agent's certificate and its proof of key possession (Cert, Proof).
// Tenant selects the service graph on
// the triage side; ClusterName is metadata for source-path stamping. Field names/tags
// MUST match the receiver's HelloPayload exactly.
type HelloPayload struct {
	AgentVersion string   `json:"agentVersion,omitempty"`
	ClusterName  string   `json:"clusterName"`
	Tenant       string   `json:"tenant"`
	Namespaces   []string `json:"namespaces,omitempty"`
	Capabilities []string `json:"capabilities,omitempty"`
	// Cert and Proof authenticate the hello: the agent's certificate and its
	// key's signature over the challenge nonce, cluster and tenant.
	Cert  []byte `json:"cert,omitempty"`
	Proof []byte `json:"proof,omitempty"`
}

// SnapshotPayload carries a (possibly partial) list of Application objects.
type SnapshotPayload struct {
	Apps []*VirtualApp `json:"apps"`
}

// AppDeltaPayload carries a single live Application change.
type AppDeltaPayload struct {
	Op  string      `json:"op"`
	App *VirtualApp `json:"app"`
}

// K8sEventPayload carries a single Kubernetes event (Warning type expected).
type K8sEventPayload struct {
	Event *corev1.Event `json:"event"`
}

// LogMetricsPayload carries error/warn/total log-line counts, aggregated per
// (namespace, service) over one collection window, for services whose error delta
// tripped the collector's gate (see logmetrics.Collector). Field names/tags MUST
// match the receiver's LogMetricsPayload exactly (internal/remoteagent/protocol.go).
type LogMetricsPayload struct {
	WindowStart int64               `json:"windowStart"` // unix seconds, window open
	WindowSec   int                 `json:"windowSec"`   // window length in seconds
	Services    []ServiceLogMetrics `json:"services"`
}

// Breach values for the Breach field of ServiceRedMetrics, ServiceUseMetrics and
// ServiceLogMetrics: which baseline arm qualified the entry.
const (
	BreachErrors    = "errors"     // RED: service error rate vs its baseline
	BreachOpErrors  = "op-errors"  // RED: one operation's error rate
	BreachLatency   = "latency"    // RED: service p99 over the evidence windows
	BreachOpLatency = "op-latency" // RED: one operation's p99
	BreachCPU       = "cpu"        // USE: cores used vs baseline
	BreachThrottle  = "throttle"   // USE: throttled-period share, when a limit exists
	BreachLogStep   = "log-step"   // logs: error or warn lines per minute stepped up
)

// Values of ServiceLogMetrics.Stream: which stream a log step is about.
const (
	LogStreamError = "error"
	LogStreamWarn  = "warn"
)

// ServiceLogMetrics is one (namespace, service) pair's counters for the window.
type ServiceLogMetrics struct {
	Namespace  string `json:"namespace"`
	Service    string `json:"service"`
	ErrorCount int64  `json:"errorCount"`
	WarnCount  int64  `json:"warnCount,omitempty"`
	TotalCount int64  `json:"totalCount,omitempty"`
	Sample     string `json:"sample,omitempty"` // ≤256 chars, truncated before send
	// Samples are up to MaxLogMetricsSamples distinct sample lines for the
	// service in this window, the fastest-growing error series first, each
	// ANSI-stripped and ≤256 chars. Sample stays the first of them (or the
	// fallback line) so an older receiver reads what it always read.
	Samples []string `json:"samples,omitempty"`
	// Baseline-gate fields (docs/todo/baseline-gates.md), set only when a
	// baseline arm qualified the entry. All additive and omitempty, so an older
	// receiver ignores them. The core's mirror type (internal/remoteagent/
	// protocol.go in the core repo) must gain the same six fields with the same
	// tags.
	//
	// Breach names the arm (the Breach* constants). Operation is the operation
	// the op-* arms judged, e.g. "POST /oteldemo.PaymentService/Charge". Observed
	// is the value judged, in the arm's unit. Baseline is the median of the
	// key's recent non-breaching readings; 0 means floor-only (no baseline
	// yet, or a zero one). It is the median for a chronic key as well, which
	// is judged against a higher reference that is not sent. EvidenceSec is the seconds of windows summed for the verdict.
	Breach      string  `json:"breach,omitempty"`
	Operation   string  `json:"operation,omitempty"`
	Observed    float64 `json:"observed,omitempty"`
	Baseline    float64 `json:"baseline,omitempty"`
	EvidenceSec int     `json:"evidenceSec,omitempty"`
	// StillBreaching marks the periodic report of a muted (chronically
	// breaching) key: nothing new happened, the key is still deviating from
	// its median. Additive and omitempty like the fields above; an older
	// receiver reads the entry as a plain breach.
	StillBreaching bool `json:"stillBreaching,omitempty"`
	// Stream names the stream a log step (Breach "log-step") is about:
	// LogStreamError or LogStreamWarn. Observed and Baseline are that
	// stream's lines per window, so without it a warn step reads as error
	// lines. Additive and omitempty; an entry with no step leaves it empty.
	Stream string `json:"stream,omitempty"`
}

// RedMetricsPayload carries per-(namespace, service) HTTP request/error counts,
// aggregated over one collection window from the embedded OBI eBPF application-mode
// DaemonSet's sensor stream, for services whose error delta tripped the
// collector's gate (see redsource.Collector, docs/adr/0009-red-metrics-beyla.md).
// Field names/tags MUST stay byte-identical with the receiver's RedMetricsPayload
// (internal/remoteagent/protocol.go on the triage side) — this is a pinned wire
// contract, do not deviate.
type RedMetricsPayload struct {
	WindowStart int64               `json:"windowStart"` // unix seconds, window open
	WindowSec   int                 `json:"windowSec"`   // window length in seconds
	Services    []ServiceRedMetrics `json:"services"`
}

// ServiceRedMetrics is one (namespace, service) pair's RED counters for the window.
type ServiceRedMetrics struct {
	Namespace    string `json:"namespace"`
	Service      string `json:"service"`
	RequestCount int64  `json:"requestCount"`
	ErrorCount   int64  `json:"errorCount"`
	Sample       string `json:"sample,omitempty"` // "GET /api/orders -> 502 x41", ≤256 chars, truncated before send
	// P99Ms is the 99th-percentile request duration in milliseconds, linearly
	// interpolated from OBI's duration-histogram bucket deltas for this window
	// (redsource.Collector.computeBucketDeltas/p99FromBucketDeltas). Attached
	// AFTER the error-threshold/rate gate; it feeds only the separate latency
	// arm (see LatencyBreach below), never the error gates. Zero/absent
	// (omitempty) when the collector's resolved metric family carries no bucket
	// data to derive it from (e.g. an exporter emitting a bare Counter under the
	// _count name with no accompanying histogram buckets).
	P99Ms float64 `json:"p99Ms,omitempty"`
	// BaselineP99Ms is the median of this service's recent normal-window p99
	// readings (milliseconds), set only when the collector's latency arm
	// breached this window (LatencyBreach). LatencyBreach is true when the
	// latency arm (p99 abnormally high against the service's own baseline,
	// redsource.Options.LatencyMultiplier) is what qualified the service for
	// this frame, or also qualified it alongside an error arm; a latency-only
	// entry may have ErrorCount 0 and a Sample like "p99 10000ms vs baseline
	// 5ms (x2000)". Both are additive and omitempty, so an older receiver
	// simply ignores them. The core's mirror type
	// (internal/remoteagent/protocol.go in the core repo) must gain the same
	// two fields with the same tags.
	BaselineP99Ms float64 `json:"baselineP99Ms,omitempty"`
	LatencyBreach bool    `json:"latencyBreach,omitempty"`
	// Baseline-gate fields (docs/todo/baseline-gates.md), set only when a
	// baseline arm qualified the entry. All additive and omitempty, so an older
	// receiver ignores them. The core's mirror type (internal/remoteagent/
	// protocol.go in the core repo) must gain the same six fields with the same
	// tags.
	//
	// Breach names the arm (the Breach* constants). Operation is the operation
	// the op-* arms judged, e.g. "POST /oteldemo.PaymentService/Charge". Observed
	// is the value judged, in the arm's unit. Baseline is the median of the
	// key's recent non-breaching readings; 0 means floor-only (no baseline
	// yet, or a zero one). It is the median for a chronic key as well, which
	// is judged against a higher reference that is not sent. EvidenceSec is the seconds of windows summed for the verdict.
	Breach      string  `json:"breach,omitempty"`
	Operation   string  `json:"operation,omitempty"`
	Observed    float64 `json:"observed,omitempty"`
	Baseline    float64 `json:"baseline,omitempty"`
	EvidenceSec int     `json:"evidenceSec,omitempty"`
	// StillBreaching marks the periodic report of a muted (chronically
	// breaching) key: nothing new happened, the key is still deviating from
	// its median. Additive and omitempty like the fields above; an older
	// receiver reads the entry as a plain breach.
	StillBreaching bool `json:"stillBreaching,omitempty"`
}

// RedSummaryPayload carries per-(namespace, service) HTTP request/error counts for
// EVERY destination read this window — not just the ones whose error delta
// tripped the red_metrics gate above (see RedMetricsPayload). This frame is
// UNGATED and summary-class: it exists so the hub can show golden-signal (rate/
// errors/p99) data for every service redsource.Collector observes, not only the
// ones anomalous enough to be gated into a red_metrics frame. Sent once per
// collection window whenever the collector has RED enabled and read at least one
// attributable service — an empty window sends nothing (see
// redsource.Collector.sendRedSummary), and a lost frame is not retried: the next
// window's frame simply supersedes it, mirroring red_metrics' own
// counters-are-cumulative recovery story. ServiceRedMetrics is reused verbatim
// from the gated frame above; Sample is always left empty here — a summary
// carries no anomaly sample. Field names/tags MUST stay byte-identical with the
// receiver's RedSummaryPayload (internal/remoteagent/protocol.go on the triage
// side) — this is a pinned wire contract, do not deviate.
type RedSummaryPayload struct {
	WindowStart int64               `json:"windowStart"` // unix seconds, window open
	WindowSec   int                 `json:"windowSec"`   // window length in seconds
	Services    []ServiceRedMetrics `json:"services"`
}

// UseMetricsPayload carries per-(namespace, service) USE-method counters —
// utilization, saturation (PSI), and error signals read from cgroup v2/PSI on each
// node, aggregated over one collection window from the node-exporter mode's sensor
// stream (see usemetrics.Collector, docs/adr/0010-use-metrics-node-exporter.md).
// Field names/tags MUST stay byte-identical with the receiver's UseMetricsPayload
// (internal/remoteagent/protocol.go on the triage side) — this is a pinned wire
// contract, do not deviate.
type UseMetricsPayload struct {
	WindowStart int64               `json:"windowStart"` // unix seconds, window open
	WindowSec   int                 `json:"windowSec"`   // window length in seconds
	Services    []ServiceUseMetrics `json:"services"`
}

// ServiceUseMetrics is one (namespace, service) pair's USE counters for the window,
// aggregated across the service's pods (max for pressure/utilization gauges, sum
// for delta counters — see usemetrics.Collector.runOnce).
type ServiceUseMetrics struct {
	Namespace    string  `json:"namespace"`
	Service      string  `json:"service"`
	CPUPsiAvg60  float64 `json:"cpuPsiAvg60"` // some avg60 %, max across the service's pods
	MemPsiAvg60  float64 `json:"memPsiAvg60"`
	IOPsiAvg60   float64 `json:"ioPsiAvg60,omitempty"`
	CPUPct       float64 `json:"cpuPct,omitempty"` // utilization vs limit; 0 when no limit
	MemPct       float64 `json:"memPct,omitempty"`
	OOMKills     int64   `json:"oomKills,omitempty"`     // delta this window
	ThrottledPct float64 `json:"throttledPct,omitempty"` // share of cpu.stat periods throttled this window
	Sample       string  `json:"sample,omitempty"`       // worst pod, e.g. "pod api-7f9: mem psi 34%, oom_kill +1", ≤256
	// Baseline-gate fields (docs/todo/baseline-gates.md), set only when a
	// baseline arm qualified the entry. All additive and omitempty, so an older
	// receiver ignores them. The core's mirror type (internal/remoteagent/
	// protocol.go in the core repo) must gain the same six fields with the same
	// tags.
	//
	// Breach names the arm (the Breach* constants). Operation is the operation
	// the op-* arms judged, e.g. "POST /oteldemo.PaymentService/Charge". Observed
	// is the value judged, in the arm's unit. Baseline is the median of the
	// key's recent non-breaching readings; 0 means floor-only (no baseline
	// yet, or a zero one). It is the median for a chronic key as well, which
	// is judged against a higher reference that is not sent. EvidenceSec is the seconds of windows summed for the verdict.
	Breach      string  `json:"breach,omitempty"`
	Operation   string  `json:"operation,omitempty"`
	Observed    float64 `json:"observed,omitempty"`
	Baseline    float64 `json:"baseline,omitempty"`
	EvidenceSec int     `json:"evidenceSec,omitempty"`
	// StillBreaching marks the periodic report of a muted (chronically
	// breaching) key: nothing new happened, the key is still deviating from
	// its median. Additive and omitempty like the fields above; an older
	// receiver reads the entry as a plain breach.
	StillBreaching bool `json:"stillBreaching,omitempty"`
}

// UseSummaryPayload carries per-(namespace, service) SATURATION values — the 4th
// golden signal (RED covers rate/errors/duration; USE covers utilization/
// saturation/errors, and saturation is the piece nothing else on the wire
// carries ungated) — for EVERY destination read this window, not just the
// ones whose PSI/OOM reading tripped the use_metrics gate above (see
// UseMetricsPayload). This frame is UNGATED and summary-class, mirroring
// RedSummaryPayload exactly: it exists so the hub can show saturation data for
// every service usemetrics.Collector observes, not only the ones anomalous
// enough to be gated into a use_metrics frame. Sent once per collection window
// whenever the collector read at least one attributable service — an empty
// window sends nothing (see usemetrics.Collector.sendUseSummary), and a lost
// frame is not retried: the next window's frame simply supersedes it, mirroring
// use_metrics' own counters-are-cumulative recovery story. Field names/tags
// MUST stay byte-identical with the receiver's UseSummaryPayload (internal/
// remoteagent/protocol.go on the triage side) — this is a pinned wire contract,
// do not deviate.
type UseSummaryPayload struct {
	WindowStart int64               `json:"windowStart"` // unix seconds, window open
	WindowSec   int                 `json:"windowSec"`   // window length in seconds
	Services    []ServiceUseSummary `json:"services"`
}

// ServiceUseSummary is one (namespace, service) pair's UTILIZATION and
// SATURATION values for the window, max-aggregated across the service's pods
// (see usemetrics.Collector.sendUseSummary). Deliberately a SEPARATE struct
// from ServiceUseMetrics, not a reuse: the gated frame's CPUPct/ThrottledPct/
// Sample fields are gated-only and must never appear on a summary, and the
// pinned wire field names here (cpuPsiPct/memPsiPct/ioPsiPct) differ from the
// gated frame's (cpuPsiAvg60/memPsiAvg60/ioPsiAvg60). CPUPsiPct/MemPsiPct/
// IOPsiPct are NOT omitempty — a genuine 0% reading is itself the data point,
// same reasoning as ServiceRedMetrics.ErrorCount staying non-omitempty on the
// red_summary frame. OOMKills is omitempty: zero is the overwhelmingly common
// case.
//
// MemPct is the U of USE and rides this frame because SATURATION IS NOT
// UTILIZATION and neither substitutes for the other (operator report,
// 2026-08-29). A pod pinned at 96% of its memory limit with its working set
// resident stalls in reclaim exactly never, so every PSI dimension reads a
// truthful 0% while the pod sits one allocation away from an OOM kill —
// observed live on tenant-a/neo4j-0 (1976Mi of a 2Gi limit, mem PSI 0%).
// Utilization was previously computed on this same code path every window
// (aggregate.memPct, populated from the node exporter's use_mem_pct series)
// and then discarded twice over: dropped from this summary, and reachable on
// the gated use_metrics frame only for a service that had ALREADY crossed a
// PSI/OOM threshold — i.e. reported only once it had stopped being the early
// warning it exists to be.
//
// It is omitempty, and a zero/absent value means "this service has no memory
// limit to be a percentage OF", not "this service uses no memory": the node
// exporter omits its use_mem_pct series entirely when memory.max reads "max"
// (nodeexporter.readMemPct returns ok=false), and any running container under
// a real limit consumes enough to land above zero. That is what makes the
// receiver's MemPct>0 presence test sound — see UseSummaryPayload's mirror on
// the triage side.
type ServiceUseSummary struct {
	Namespace string  `json:"namespace"`
	Service   string  `json:"service"`
	CPUPsiPct float64 `json:"cpuPsiPct"`
	MemPsiPct float64 `json:"memPsiPct"`
	IOPsiPct  float64 `json:"ioPsiPct"`
	OOMKills  int64   `json:"oomKills,omitempty"`
	MemPct    float64 `json:"memPct,omitempty"`
	// CPUPct is CPU utilization as a percentage of the service's own quota
	// (worst pod), derived from a usage_usec delta over the collection window
	// rather than read as a gauge — which is why v1 shipped without it and USE
	// reported only one of its two utilization dimensions. 0 means no CPU limit
	// is set, or the pod was seen for the first time this window.
	CPUPct float64 `json:"cpuPct,omitempty"`
	// MemBytes/MemLimitBytes/CPULimitCores are the absolute figures behind the
	// percentages, taken from the same worst pod. A percentage answers "how
	// full"; sizing a limit, or reconciling against `kubectl top`, needs "how
	// much". All omitempty — an older exporter simply omits them.
	MemBytes      float64 `json:"memBytes,omitempty"`
	MemLimitBytes float64 `json:"memLimitBytes,omitempty"`
	CPULimitCores float64 `json:"cpuLimitCores,omitempty"`

	// MemNodePct/CPUNodePct (2026-08-29) are utilization against the NODE's
	// total capacity rather than a container limit — the portable denominator
	// that always exists, unlike MemPct/CPUPct, which report 0 for any pod
	// carrying no limit at all. This answers "is this one pod eating the box"
	// on bare metal, a plain container, or anything else with cgroups, none of
	// which need a limit configured for the question to make sense. Worst pod
	// by memPct, same selection as every other absolute/percentage on this
	// struct (see usemetrics.aggregate's doc comment). 0/omitted when the
	// node-capacity reading itself was unavailable (see
	// nodeexporter.resolveNodeMemTotalBytes); CPUNodePct is essentially always
	// present, since its own denominator's resolution chain ends in
	// runtime.NumCPU(), which never fails.
	//
	// Deliberately no node TOTAL (bytes/cores) on this struct: this frame
	// aggregates a service across every pod reporting it, which may span
	// several different nodes, so there is no single correct "node total" to
	// report at that granularity — only the ratio each pod computed against
	// its OWN node is. Neither percentage is clamped to 100; in practice a
	// node percentage cannot exceed it, but nothing here enforces that.
	MemNodePct float64 `json:"memNodePct,omitempty"`
	CPUNodePct float64 `json:"cpuNodePct,omitempty"`

	// MemReqPct/CPUReqPct (2026-08-29) are utilization against the pod spec's
	// own declared REQUEST — the Kubernetes right-sizing axis, distinct from
	// both MemPct (vs limit) and MemNodePct (vs node capacity). Usage well
	// above request means the pod is a noisy neighbour and the first
	// candidate evicted under node pressure; usage well below request means
	// reserved capacity going to waste. Worst pod by memPct, same selection as
	// the rest of this struct.
	//
	// NOT clamped to 100 — exceeding your own request is normal and is
	// exactly the signal this field exists to carry; clamping would erase it.
	// 0/omitted when no regular container declared the corresponding request
	// (see nodeexporter.regularContainerMemRequests/
	// regularContainerCPURequestsMilli) — never a fabricated 0% for a pod that
	// simply wasn't given a request to measure against.
	MemReqPct float64 `json:"memReqPct,omitempty"`
	CPUReqPct float64 `json:"cpuReqPct,omitempty"`

	// MemRequestBytes/CPURequestCores are the absolute form of the two request
	// percentages above — the resolved denominator MemReqPct/CPUReqPct divided
	// by, so a surprising percentage can be traced to the request it was
	// measured against, same reasoning as MemLimitBytes/CPULimitCores for the
	// limit-relative percentages. 0/omitted on the same "nothing declared"
	// terms as MemReqPct/CPUReqPct.
	MemRequestBytes float64 `json:"memRequestBytes,omitempty"`
	CPURequestCores float64 `json:"cpuRequestCores,omitempty"`
}

// TopologySnapshotPayload carries a FULL observed service-topology snapshot —
// level state, not a delta, sent every ~10 minutes regardless of change (see
// redsource.Collector, docs/adr/0011-topology-snapshot.md). A lost frame simply
// heals on the next tick, unlike red_metrics/use_metrics/log_metrics, which only
// send when something crosses a gate. Field names/tags MUST stay byte-identical
// with the receiver's TopologySnapshotPayload (internal/remoteagent/protocol.go on
// the triage side) — this is a pinned wire contract, do not deviate.
type TopologySnapshotPayload struct {
	CapturedAt int64          `json:"capturedAt"` // unix seconds, snapshot time
	WindowSec  int            `json:"windowSec"`  // observation window covered by Calls
	Nodes      []TopologyNode `json:"nodes"`
	Edges      []TopologyEdge `json:"edges"`
}

// TopologyNode is one observed workload — workload granularity, never pods.
type TopologyNode struct {
	Namespace string `json:"namespace"`
	Workload  string `json:"workload"`
}

// TopologyEdge is one observed caller→callee relationship, aggregated per (src,
// dst, protocol) for the window. Dst and External are mutually exclusive: Dst is
// set when server_address resolves to an in-scope k8s workload, External is set
// (host[:port], ≤128 chars, truncated before send) otherwise — an IP literal, a
// host outside the k8s DNS shape, or a namespace outside Options.Namespaces (see
// redsource.Collector's server_address resolution).
type TopologyEdge struct {
	SrcNamespace string `json:"srcNamespace"`
	SrcWorkload  string `json:"srcWorkload"`
	DstNamespace string `json:"dstNamespace,omitempty"` // set for in-scope k8s targets
	DstWorkload  string `json:"dstWorkload,omitempty"`
	External     string `json:"external,omitempty"` // set for out-of-scope targets (host[:port]); mutually exclusive with Dst pair; <=128 chars, truncated before send
	Calls        int64  `json:"calls"`              // window call-count delta, >=0
	// DstPort is the destination port of the client span's server address (or
	// of an L4 flow row that carries one); omitted (0) when unknown. The same
	// destination on two ports is two edges.
	DstPort int `json:"dstPort,omitempty"`
	// Bytes is the window byte-count delta from an L4 network-flow edge
	// (docs/todo/service-graph-from-obi.md) — >=0, omitted (0) for an edge
	// derived purely from L7 client spans, which carry Calls instead. The
	// two are not mutually exclusive in the wire contract (a future profile
	// could emit both for the same src/dst/protocol), only in what today's
	// derivation actually produces per Protocol value.
	Bytes    int64  `json:"bytes,omitempty"`
	Protocol string `json:"protocol,omitempty"` // "http" | "grpc" | "sql" | "redis" | "tcp" | "other"
}

// SensorStatePayload carries every sensor's own health as the controller last
// read it from sensorstream.Hub.Nodes() (ADR-0015 phase 1). At is the unix
// second the controller built the payload, not any sensor's own clock.
type SensorStatePayload struct {
	At      int64          `json:"at"`
	Sensors []SensorStatus `json:"sensors"`
	// OTLP lists the sources the controller's OTLP receiver holds (ADR-0017),
	// across every replica: one per pushed resource. Omitted when none.
	OTLP []OTLPSourceStatus `json:"otlp,omitempty"`
	// Intake is what each OTLP sender offered the receivers in their last
	// window, summed over the otlp role's replicas and the controller's own
	// receiver (ADR-0022). Omitted when there is none.
	Intake []OTLPIntakeStatus `json:"intake,omitempty"`
}

// OTLPIntakeStatus is what one OTLP sender offered the receivers in their last
// one-minute window, and what they hold for it now. It tells the operator what
// share of a Collector's export is read and what to filter out at the
// Collector. Refused is inside Points; RefusedExports is outside both, since
// an export refused before it was read has no points to count.
type OTLPIntakeStatus struct {
	// Sender is the principal as the receiver's refusal log names it.
	Sender string `json:"sender"`
	// Window is the seconds the windowed counters below cover.
	Window int64 `json:"window"`
	// Exports is the exports the receivers took from the sender.
	Exports int64 `json:"exports"`
	// Points is the points (and spans, and log records) offered.
	Points int64 `json:"points"`
	// Unread is the points dropped at intake because no collector reads their
	// metric.
	Unread int64 `json:"unread"`
	// Refused is the points refused (limits, no namespace, tenant checks); it
	// is inside Points.
	Refused int64 `json:"refused"`
	// RefusedExports is the exports refused before they were read
	// (backpressure); outside Exports and Points.
	RefusedExports int64 `json:"refusedExports"`
	// Held is the state entries held for the sender's sources now, and Sources
	// the sources: neither is windowed.
	Held    int64 `json:"held"`
	Sources int32 `json:"sources"`
	// UnreadByName is the unread points by metric name, the top 20 by points,
	// most first. A name "other" stands for names the receiver did not count
	// one by one.
	UnreadByName []NameCount `json:"unreadByName,omitempty"`
}

// NameCount is the points of one metric name.
type NameCount struct {
	Name   string `json:"name"`
	Points int64  `json:"points"`
}

// OTLPSourceStatus is one OTLP source: one resource an OpenTelemetry SDK or
// Collector pushes, and what of it reaches each collector family.
type OTLPSourceStatus struct {
	Key       string `json:"key"`
	Service   string `json:"service,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	// Instance is service.instance.id, or the scrape target for a kubelet
	// cAdvisor source.
	Instance string `json:"instance,omitempty"`
	// Tenant is the tenant Namespace belongs to, resolved the way a frame's
	// tenant is; empty for a source with no namespace (kubelet cAdvisor) or
	// an unmapped one. Core shows a tenant only the sources stamped with it.
	Tenant   string             `json:"tenant,omitempty"`
	LastSeen int64              `json:"lastSeen"`
	Families []OTLPFamilyStatus `json:"families"`
}

// OTLPFamilyStatus is what one OTLP source feeds one collector family.
type OTLPFamilyStatus struct {
	// Family is "red" (RED, SIG and topology), "use" or "log".
	Family     string `json:"family"`
	Metrics    uint32 `json:"metrics"`
	DataPoints uint32 `json:"dataPoints"`
	// Pods counts the distinct pods a "use" family covers (cAdvisor).
	Pods uint32 `json:"pods,omitempty"`
}

// SensorStatus is one sensor pod's status, mapped from sensorstream.NodeStatus
// and its *sensorpb.SensorState. Fields past Node/Pod/Version/Connected/LastSeen
// are omitempty: a sensor that has not sent a SensorState message yet (just
// past Hello) reports none of them.
type SensorStatus struct {
	Node          string             `json:"node"`
	Pod           string             `json:"pod"`
	Version       string             `json:"version,omitempty"`
	Connected     bool               `json:"connected"`
	LastSeen      int64              `json:"lastSeen"` // unix seconds
	Gaps          uint64             `json:"gaps,omitempty"`
	Goroutines    uint32             `json:"goroutines,omitempty"`
	HeapBytes     uint64             `json:"heapBytes,omitempty"`
	SysBytes      uint64             `json:"sysBytes,omitempty"`
	CPUSeconds    float64            `json:"cpuSeconds,omitempty"`
	Reconnects    uint64             `json:"reconnects,omitempty"`
	SendErrors    uint64             `json:"sendErrors,omitempty"`
	SendMicros    int64              `json:"sendMicros,omitempty"`
	AckRTTMicros  int64              `json:"ackRttMicros,omitempty"`
	TickBytes     uint64             `json:"tickBytes,omitempty"`
	ConnectMicros int64              `json:"connectMicros,omitempty"`
	Roles         []SensorRoleStatus `json:"roles,omitempty"`
}

// SensorRoleStatus is one role's gather stats on a sensor. A role the sensor
// declared in its Hello but has not reported a SensorState for yet gets an
// entry with only Role set.
type SensorRoleStatus struct {
	Role         string `json:"role"`
	GatherMicros int64  `json:"gatherMicros,omitempty"`
	Metrics      uint32 `json:"metrics,omitempty"`
	DataPoints   uint32 `json:"dataPoints,omitempty"`
	GatherErrors uint64 `json:"gatherErrors,omitempty"`
	LastError    string `json:"lastError,omitempty"`
}

// newCertHello builds a certificate hello (agent join): cert is the agent's
// certificate and proof its key's signature over the challenge nonce,
// cluster and tenant.
func newCertHello(cluster, tenant, agentVersion string, cert, proof []byte, namespaces []string, extraCapabilities ...string) *Frame {
	caps := append([]string{"applications", "k8s_events"}, extraCapabilities...)
	return &Frame{V: ProtocolVersion, Kind: KindHello, Cluster: cluster, Hello: &HelloPayload{
		AgentVersion: agentVersion,
		ClusterName:  cluster,
		Tenant:       tenant,
		Namespaces:   namespaces,
		Capabilities: caps,
		Cert:         cert,
		Proof:        proof,
	}}
}

// newSnapshot builds a snapshot frame for a chunk of apps.
func newSnapshot(cluster string, apps []*VirtualApp) *Frame {
	return &Frame{V: ProtocolVersion, Kind: KindSnapshot, Cluster: cluster, Snapshot: &SnapshotPayload{Apps: apps}}
}

func newSnapshotEnd(cluster string) *Frame {
	return &Frame{V: ProtocolVersion, Kind: KindSnapshotEnd, Cluster: cluster}
}

func newAppDelta(cluster, op string, app *VirtualApp) *Frame {
	return &Frame{V: ProtocolVersion, Kind: KindAppDelta, Cluster: cluster, AppDelta: &AppDeltaPayload{Op: op, App: app}}
}

func newK8sEvent(cluster string, e *corev1.Event) *Frame {
	return &Frame{V: ProtocolVersion, Kind: KindK8sEvent, Cluster: cluster, K8sEvent: &K8sEventPayload{Event: e}}
}

// newLogMetrics builds a log_metrics frame for one window. Samples are defensively
// re-truncated here (callers, e.g. logmetrics.Collector, are expected to have already
// truncated via TruncateSample, but this is the last line of defense before the wire).
func newLogMetrics(cluster string, windowStart int64, windowSec int, services []ServiceLogMetrics) *Frame {
	for i := range services {
		services[i].Sample = TruncateSample(services[i].Sample)
		if len(services[i].Samples) > MaxLogMetricsSamples {
			services[i].Samples = services[i].Samples[:MaxLogMetricsSamples]
		}
		for j := range services[i].Samples {
			services[i].Samples[j] = TruncateSample(services[i].Samples[j])
		}
	}
	return &Frame{V: ProtocolVersion, Kind: KindLogMetrics, Cluster: cluster, LogMetrics: &LogMetricsPayload{
		WindowStart: windowStart,
		WindowSec:   windowSec,
		Services:    services,
	}}
}

// TruncateSample clips s to MaxLogMetricsSampleLen, matching the receiver's field
// limit so it never has to reject/trim a sample itself.
func TruncateSample(s string) string {
	if len(s) <= MaxLogMetricsSampleLen {
		return s
	}
	return s[:MaxLogMetricsSampleLen]
}

// chunkServices splits services into groups of at most MaxLogMetricsServices, so a
// window that qualifies more services than the per-frame cap is sent as multiple
// log_metrics frames for the same window.
func chunkServices(services []ServiceLogMetrics) [][]ServiceLogMetrics {
	if len(services) == 0 {
		return nil
	}
	var out [][]ServiceLogMetrics
	for i := 0; i < len(services); i += MaxLogMetricsServices {
		end := i + MaxLogMetricsServices
		if end > len(services) {
			end = len(services)
		}
		out = append(out, services[i:end])
	}
	return out
}

// newRedMetrics builds a red_metrics frame for one window. Samples are defensively
// re-truncated and the service list defensively re-capped at MaxRedMetricsServices
// here (redsource.Collector is expected to have already truncated/capped, but this
// is the last line of defense before the wire: a plain head-truncate, not a
// re-sort-by-error-delta, since the collector already picked the top services).
func newRedMetrics(cluster string, windowStart int64, windowSec int, services []ServiceRedMetrics) *Frame {
	if len(services) > MaxRedMetricsServices {
		services = services[:MaxRedMetricsServices]
	}
	for i := range services {
		services[i].Sample = TruncateRedSample(services[i].Sample)
		services[i].Operation = TruncateOperation(services[i].Operation)
	}
	return &Frame{V: ProtocolVersion, Kind: KindRedMetrics, Cluster: cluster, RedMetrics: &RedMetricsPayload{
		WindowStart: windowStart,
		WindowSec:   windowSec,
		Services:    services,
	}}
}

// TruncateRedSample clips s to MaxRedMetricsSampleLen, matching the receiver's field
// limit so it never has to reject/trim a sample itself.
func TruncateRedSample(s string) string {
	if len(s) <= MaxRedMetricsSampleLen {
		return s
	}
	return s[:MaxRedMetricsSampleLen]
}

// TruncateOperation clips s to MaxOperationLen bytes, on a rune boundary.
func TruncateOperation(s string) string {
	if len(s) <= MaxOperationLen {
		return s
	}
	n := MaxOperationLen
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// newRedSummary builds a red_summary frame for one window. Same defensive
// re-cap/re-truncate posture as newRedMetrics — a plain head-truncate, not a
// re-sort, since redsource.Collector has already picked which services to keep
// and in what order before calling this. The per-entry TruncateRedSample call is
// a no-op in practice (the wire contract leaves Sample empty for a summary), kept
// only as the same last-line-of-defense discipline every other frame builder here
// applies to its Sample-bearing services.
func newRedSummary(cluster string, windowStart int64, windowSec int, services []ServiceRedMetrics) *Frame {
	if len(services) > MaxRedMetricsServices {
		services = services[:MaxRedMetricsServices]
	}
	for i := range services {
		services[i].Sample = TruncateRedSample(services[i].Sample)
	}
	return &Frame{V: ProtocolVersion, Kind: KindRedSummary, Cluster: cluster, RedSummary: &RedSummaryPayload{
		WindowStart: windowStart,
		WindowSec:   windowSec,
		Services:    services,
	}}
}

// newUseMetrics builds a use_metrics frame for one window. Samples are defensively
// re-truncated and the service list defensively re-capped at MaxUseMetricsServices
// here (usemetrics.Collector is expected to have already truncated/capped, but this
// is the last line of defense before the wire — a plain head-truncate, not a
// re-sort-by-memPsi, since the collector already picked the top services).
func newUseMetrics(cluster string, windowStart int64, windowSec int, services []ServiceUseMetrics) *Frame {
	if len(services) > MaxUseMetricsServices {
		services = services[:MaxUseMetricsServices]
	}
	for i := range services {
		services[i].Sample = TruncateUseSample(services[i].Sample)
	}
	return &Frame{V: ProtocolVersion, Kind: KindUseMetrics, Cluster: cluster, UseMetrics: &UseMetricsPayload{
		WindowStart: windowStart,
		WindowSec:   windowSec,
		Services:    services,
	}}
}

// TruncateUseSample clips s to MaxUseMetricsSampleLen, matching the receiver's field
// limit so it never has to reject/trim a sample itself.
func TruncateUseSample(s string) string {
	if len(s) <= MaxUseMetricsSampleLen {
		return s
	}
	return s[:MaxUseMetricsSampleLen]
}

// newUseSummary builds a use_summary frame for one window. Same defensive
// re-cap posture as newRedSummary — a plain head-truncate, not a re-sort, since
// usemetrics.Collector has already picked which services to keep and in what
// order before calling this. Unlike newUseMetrics/newRedSummary there is no
// Sample field to re-truncate — the wire contract for this frame carries none.
func newUseSummary(cluster string, windowStart int64, windowSec int, services []ServiceUseSummary) *Frame {
	if len(services) > MaxUseMetricsServices {
		services = services[:MaxUseMetricsServices]
	}
	return &Frame{V: ProtocolVersion, Kind: KindUseSummary, Cluster: cluster, UseSummary: &UseSummaryPayload{
		WindowStart: windowStart,
		WindowSec:   windowSec,
		Services:    services,
	}}
}

// newTopologySnapshot builds a topology_snapshot frame for one snapshot. Nodes and
// edges are defensively re-capped at MaxTopologyNodes/MaxTopologyEdges and every
// edge's External is defensively re-truncated here (redsource.Collector is expected
// to have already capped/truncated, but this is the last line of defense before
// the wire — a plain head-truncate, not a re-sort, since the collector already
// picked the top edges by Calls and the alphabetically-first nodes).
func newTopologySnapshot(cluster string, capturedAt int64, windowSec int, nodes []TopologyNode, edges []TopologyEdge) *Frame {
	if len(nodes) > MaxTopologyNodes {
		nodes = nodes[:MaxTopologyNodes]
	}
	if len(edges) > MaxTopologyEdges {
		edges = edges[:MaxTopologyEdges]
	}
	for i := range edges {
		edges[i].External = TruncateTopologyExternal(edges[i].External)
	}
	return &Frame{V: ProtocolVersion, Kind: KindTopologySnapshot, Cluster: cluster, TopologySnapshot: &TopologySnapshotPayload{
		CapturedAt: capturedAt,
		WindowSec:  windowSec,
		Nodes:      nodes,
		Edges:      edges,
	}}
}

// TruncateTopologyExternal clips s to MaxTopologyExternalLen, matching the
// receiver's field limit so it never has to reject/trim the value itself.
func TruncateTopologyExternal(s string) string {
	if len(s) <= MaxTopologyExternalLen {
		return s
	}
	return s[:MaxTopologyExternalLen]
}

func newHeartbeat(cluster string) *Frame {
	return &Frame{V: ProtocolVersion, Kind: KindHeartbeat, Cluster: cluster}
}

// newSensorState builds a sensor_state frame for one report. No capping: the
// number of sensors is the size of the DaemonSet, never large enough to need
// one.
func newSensorState(cluster string, at int64, sensors []SensorStatus, otlp []OTLPSourceStatus, intake []OTLPIntakeStatus) *Frame {
	return &Frame{V: ProtocolVersion, Kind: KindSensorState, Cluster: cluster, SensorState: &SensorStatePayload{
		At:      at,
		Sensors: sensors,
		OTLP:    otlp,
		Intake:  intake,
	}}
}

// encode marshals a frame to its wire bytes.
func encode(f *Frame) ([]byte, error) {
	return json.Marshal(f)
}

// decode parses wire bytes into a Frame (used to read hub→agent pong/resync frames).
func decode(data []byte) (*Frame, error) {
	var f Frame
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("decode frame: %w", err)
	}
	return &f, nil
}
