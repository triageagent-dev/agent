// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/triageagent-dev/agent/agentui"
	"github.com/triageagent-dev/agent/internal/annotationkeys"
	"github.com/triageagent-dev/agent/logexporter"
	"github.com/triageagent-dev/agent/logmetrics"
	"github.com/triageagent-dev/agent/push"
	"github.com/triageagent-dev/agent/redsource"
	"github.com/triageagent-dev/agent/usemetrics"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
	// Build stamps, injected via -ldflags by the Makefile's LDFLAGS, by .ko.yaml
	// for container images, and by the CI release job. The defaults below are what
	// a bare `go build` with no -ldflags produces.
	version   = "dev"
	gitCommit = "unknown"
	buildDate = "unknown"
)

// appVersion returns the controller version reported to triage in the push hello frame.
func appVersion() string { return version }

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	// +kubebuilder:scaffold:scheme
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "otlp-cert" {
		runOTLPCert()
	}
	var modes modeList
	var nodeExporterCgroupRoot string
	var nodeExporterProcRoot, nodeExporterSysRoot string
	var logExporterIncludeNamespaces namespaceList
	var logExporterErrorRegex, logExporterWarnRegex string
	var logExporterSampleMaxLen, logExporterMaxSamplesPerService int
	var logExporterSampleJSONFields string
	var namespace string
	var metricsAddr string
	var syncPeriod int64
	var enableLeaderElection bool
	var leaderElectionID string
	var uiMaxEvents int
	var kubernetesWatch bool
	var triageEndpoint, clusterName, targetTenant, uiListen, uiAdminKeyFile, uiAdminKey, apiListen, apiTokensFile, apiTLSCert, apiTLSKey string
	var apiMCP, apiDocs, apiSAAuth bool
	var targetNamespaces namespaceList
	var joinCAFingerprint, joinStateDir, joinSecret string
	var joinTenants namespaceList
	var workloadResources workloadResourceList
	var coreHeartbeat int64
	var coreInsecure, coreAllowPlaintext bool
	var virtualApps string
	var logMetricsEnabled bool
	var logMetricsIntervalSeconds int64
	var logMetricsErrorThreshold int64
	var redMetricsEnabled bool
	var redMetricsIntervalSeconds int64
	var redMetricsErrorThreshold int64
	var redMetricsMinErrorRatePct float64
	var redMetricsOperationErrorRatePct float64
	var redLatencyMultiplier, redLatencyFloorMs float64
	var redLatencyMinRequests int64
	var redLatencyMinBaselineWindows int
	// Baseline gates (docs/todo/baseline-gates.md): one kill switch and the
	// main knobs per family; 0 for a number means the package default.
	var redBaseline, useBaseline, logBaseline bool
	var redBaselineMultiplier, redBaselineErrFloorPct, redBaselineLatFloorMs float64
	var redBaselineEvidenceWindows int
	var useBaselineMultiplier, useBaselineCPUFloorCores float64
	var logBaselineMultiplier float64
	// Cold floors, the low-volume rule, the evidence minimums and the
	// operation cap of the baseline gates; 0 means the package default.
	var redBaselineMinBaselineWindows, redBaselineMaxOperations int
	var redBaselineErrMinRequests, redBaselineErrMinErrors, redBaselineLatMinRequests int64
	var redBaselineErrSparseMinErrors, redBaselineLatChronicMinRequests int64
	var redBaselineErrColdFloorPct, redBaselineLatColdFloorMs, redBaselineErrSparseRatePct float64
	var useBaselineCPUColdFloorCores float64
	var logBaselineColdFloorLines int64
	var useMetricsEnabled bool
	var useMetricsIntervalSeconds int64
	var useMetricsMemPsiThreshold, useMetricsCPUPsiThreshold, useMetricsIOPsiThreshold, useMetricsMemPctThreshold, useMetricsMemNodePctThreshold float64
	var topologyEnabled bool
	var topologyIntervalSeconds int64
	var sensorListen, sensorTLSSecret, sensorCAConfigMap, sensorService, sensorServiceAccount string
	var sensorStale time.Duration
	var meshListen, meshService string
	var otlpListen, otlpAllowed, otlpForwarders, otlpClientCA, otlpClientCASecret, sensorExtraNames string
	var otlpMaxResources, otlpMaxPoints, otlpMaxPerSender, otlpMaxRequest, otlpMaxSeries int
	var otlpTLSCert, otlpTLSKey, otlpTenantsFlag, sensorClientCert, sensorClientKey string
	var otlpInsecure, sensorOTLPRole bool
	var sensorController, sensorCAFile, sensorTokenFile string
	var sensorTick time.Duration
	var hostNamespace, hostUnits, hostCgroupRoot string
	var hostLogFiles namespaceList

	// --mode selects what this binary process runs. "controller" (default) runs
	// push mode plus its optional collectors, unchanged. Everything else in
	// this flag set is meaningless in node-exporter/log-exporter mode and is
	// simply parsed-and-ignored — see the early branches right after
	// flag.Parse() below.
	flag.Var(&modes, "mode",
		`Role this process runs. Repeat for several: --mode=ebpf --mode=node-exporter --mode=log-exporter runs each as a goroutine in one process. Omit for "controller" (push mode plus its optional collectors).`)
	// Host mode (docs/adr/0018-linux-host-mode.md): --mode=host runs the agent
	// on a plain Linux host with no Kubernetes API.
	flag.StringVar(&hostNamespace, "host-namespace", defaultHostNamespace, "host mode: the one synthetic namespace every unit of this host reports under. Hosts running the same services share one value so their services merge in the core graph.")
	flag.StringVar(&hostUnits, "host-units", "", "host mode: comma-separated globs of the systemd units to observe (e.g. nginx.service,app-*.service). Empty selects every enabled service except a built-in denylist.")
	flag.Var(&hostLogFiles, "host-log-files", "host mode: <unit>=<glob> of a plain log file a service writes instead of journald; repeat or comma-separate. Matching files are counted under the unit's service name.")
	flag.StringVar(&hostCgroupRoot, "cgroup-root", "/sys/fs/cgroup", "host mode: root of the cgroup v2 filesystem the unit cgroups are read from.")
	flag.StringVar(&nodeExporterCgroupRoot, "node-exporter-cgroup-root", "/sys/fs/cgroup", "node-exporter mode: root of the host cgroup v2 filesystem (mounted read-only).")
	flag.StringVar(&nodeExporterProcRoot, "node-exporter-proc-root", "/proc", "node-exporter mode: root of the host /proc (mounted read-only) — used to read node total memory.")
	flag.StringVar(&nodeExporterSysRoot, "node-exporter-sys-root", "/sys", "node-exporter mode: root of the host /sys (mounted read-only) — used to read the node's online CPU core count.")

	// log-exporter mode (docs/adr/0014-log-exporter-in-agent-ds.md): a standalone
	// per-node container-log counter, the same "second container on the eBPF
	// DaemonSet pod" shape as node-exporter mode above. Its metrics reach
	// logmetrics.Collector (wired below, controller mode only) over the sensor
	// stream.
	flag.Var(&logExporterIncludeNamespaces, "log-exporter-include-namespaces", fmt.Sprintf("log-exporter mode: exact namespace name to tail; repeat the flag for each. Empty falls back to the %s-annotated set.", annotationkeys.Tenant))
	flag.StringVar(&logExporterErrorRegex, "log-exporter-error-regex", logexporter.DefaultErrorRegex, "log-exporter mode: regex matched against a line's content to classify it as an error/fatal line.")
	flag.StringVar(&logExporterWarnRegex, "log-exporter-warn-regex", logexporter.DefaultWarnRegex, "log-exporter mode: regex matched against a line's content to classify it as a warn line.")
	flag.IntVar(&logExporterSampleMaxLen, "log-exporter-sample-max-len", logexporter.DefaultSampleMaxLen, "log-exporter mode: truncate a fingerprinted error line to this many chars before it becomes a `log.sample` attribute value (docs/adr/0007-error-log-sample-fingerprint.md's behavior, restored natively).")
	flag.IntVar(&logExporterMaxSamplesPerService, "log-exporter-max-samples-per-service", logexporter.DefaultMaxSamplesPerService, "log-exporter mode: cap on DISTINCT error fingerprints tracked per (namespace, service) — each is its own permanent series; past the cap, further distinct fingerprints fall into a catch-all series without a sample.")
	flag.StringVar(&logExporterSampleJSONFields, "log-exporter-sample-json-fields", strings.Join(logexporter.DefaultSampleJSONFields, ","), "log-exporter mode: comma-separated, priority-ordered JSON keys tried when an error line's content is a JSON object — the first present, non-empty string value wins (or, for an object value, that object's own \"message\" sub-key).")

	flag.BoolVar(&kubernetesWatch, "kubernetes", true, "Watch the Kubernetes API. false runs the agent on what its OTLP receiver and sensors hand it and nothing else: no Namespace, Service, Ingress, workload or Event informers, no virtual apps, no owner reads, so it needs no list or watch rights on the cluster. Needs --triage-endpoint and --target-tenant: with no namespace annotation to read, every frame files under that one tenant (docs/adr/0021-otlp-only-no-kubernetes-watch.md).")
	flag.StringVar(&namespace, "namespace", "", "Namespace the manager cache and push mode's push cache are scoped to. Empty means cluster-wide.")
	flag.StringVar(&metricsAddr, "metrics-addr", "0",
		"Address the controller-runtime metric endpoint binds to; \"0\" disables it (the default). Expose it via an authenticating proxy rather than binding to all interfaces.")
	flag.Int64Var(&syncPeriod, "sync-period", 120, "Sync every sync-period seconds.")
	flag.BoolVar(&enableLeaderElection, "enable-leader-election", false,
		"Enable leader election for controller triage-agent. Enabling this will ensure there is only one active controller triage-agent.")
	flag.StringVar(&leaderElectionID, "leader-election-id", "triage-agent",
		"Name of the Lease the replicas elect their leader on, in the pod's namespace. Two releases in one namespace need different names, or they share one leader.")

	// Push mode (ADR-0005): stream Applications + Warning events to a triage agent
	// over an outbound WebSocket. Off unless --triage-endpoint is set.
	flag.StringVar(&triageEndpoint, "triage-endpoint", "", "Triage agent WebSocket URL (wss://host/events/ws). Empty disables push mode; with --ui-listen or --api-listen set the agent then runs standalone (no core, local TUI and monitoring API only).")
	flag.StringVar(&uiListen, "ui-listen", "", "address the local TUI listens on, e.g. 127.0.0.1:8088. Loopback only (reach it with kubectl port-forward); empty disables it. Works without a core (standalone).")
	flag.StringVar(&uiAdminKeyFile, "ui-admin-key-file", "", "file holding the admin key the local TUI asks for at login (at least 16 bytes; mount it from a Secret). Empty means no login.")
	flag.StringVar(&uiAdminKey, "ui-admin-key", "", "host mode only: the admin key the local TUI asks for at login (at least 16 bytes). The environment variable TRIAGE_AGENT_UI_ADMIN_KEY does the same and keeps it out of the process list; --ui-admin-key-file wins over both. In a cluster use --ui-admin-key-file.")
	flag.StringVar(&apiListen, "api-listen", "", "address the monitoring API listens on, e.g. :9480: Prometheus /metrics, /.well-known/triage-agent and the JSON API, all behind bearer tokens. Empty disables it. Needs --api-tokens-file. Works without a core (standalone).")
	flag.StringVar(&apiTokensFile, "api-tokens-file", "", "file of API tokens, one `name tenant token` per line (tenant * is unrestricted; mount it from a Secret). Required with --api-listen.")
	flag.StringVar(&apiTLSCert, "api-tls-cert-file", "", "certificate the monitoring API serves TLS with; with --api-tls-key-file. Empty serves plain HTTP.")
	flag.BoolVar(&apiSAAuth, "api-sa-auth", false, "cluster mode: also accept ServiceAccount tokens (audience triage-agent-api) on the monitoring API; RBAC get on agent-api.triageagent.dev decides, cluster-wide for all tenants, in a namespace for its tenant. --api-tokens-file becomes optional.")
	flag.BoolVar(&apiMCP, "api-mcp", false, "serve the Model Context Protocol endpoint (POST /mcp) on the monitoring API: read-only tools over the JSON API, same tokens and tenant scope. Off by default.")
	flag.BoolVar(&apiDocs, "api-docs", false, "serve the OpenAPI spec (/api/v1/openapi.json) and the Swagger page (/api/v1/docs) on the monitoring API. Off by default.")
	flag.StringVar(&apiTLSKey, "api-tls-key-file", "", "private key for --api-tls-cert-file.")
	flag.IntVar(&uiMaxEvents, "ui-max-events", agentui.DefaultMaxEvents, "how many of the newest events per tenant the local TUI and the monitoring API keep (10 to 5000). Older ones are dropped; the event counters keep counting.")
	flag.StringVar(&clusterName, "cluster-name", "", "Cluster identifier stamped into pushed events. Required when --triage-endpoint is set.")
	flag.StringVar(&targetTenant, "target-tenant", "", fmt.Sprintf("Primary tenant this cluster belongs to; selects the triage service graph and is declared in the hello. Optional when --triage-endpoint is set: leave it empty to run cluster-mode, identified only by --cluster-name, where every frame's tenant is derived purely from %s namespace annotations (push/tenant_routing.go) and an unmapped namespace's items are dropped rather than defaulted.", annotationkeys.Tenant))
	flag.StringVar(&joinCAFingerprint, "join-ca-fingerprint", "", "Core agent CA fingerprint (SHA256:...), from the install command or GET /v2/agents on the core. The agent authenticates to the core only by its join certificate (an admin-approved request or a one-time key), so this is required whenever --triage-endpoint is set, in a pod and in host mode.")
	flag.StringVar(&joinSecret, "join-secret", "", "Secret in this pod's namespace that keeps the agent key, its certificate and the saved core CA, so a new pod needs no new approval. Needs get and update on that Secret. Empty uses --join-state-dir.")
	flag.StringVar(&joinStateDir, "join-state-dir", "/data/join", "Directory for the agent key, its certificate and the saved core CA when --join-secret is empty. Losing it means a new approval.")
	flag.Var(&joinTenants, "join-tenant", "Tenant the join request asks for; repeat for several. Defaults to --target-tenant; a cluster-mode agent lists every tenant it serves. Also the only tenants an OTLP client certificate may name (spiffe://<domain>/tenant/<name>): a certificate naming another tenant is refused.")
	flag.Var(&targetNamespaces, "target-namespaces", fmt.Sprintf("Namespace this agent observes; repeat for several: --target-namespaces=a --target-namespaces=b. Omit to observe the %s-annotated namespaces instead — omitting it never means every namespace.", annotationkeys.Tenant))
	flag.Var(&workloadResources, "workload-resources",
		`Namespaced workload resource virtual-app synthesis discovers generically, as group/version/resource (e.g. apps/v1/deployments); repeat for several: --workload-resources=a --workload-resources=b. Omit to use the default list (apps/v1/deployments, apps/v1/statefulsets, apps/v1/daemonsets, argoproj.io/v1alpha1/rollouts) — giving the flag even once REPLACES the default list, it does not add to it. A resource the server does not serve is skipped at startup with one log line, never per namespace.`)
	flag.Int64Var(&coreHeartbeat, "core-heartbeat", 20, "Heartbeat interval in seconds.")
	flag.BoolVar(&coreInsecure, "core-insecure-skip-verify", false, "Skip TLS certificate verification for a wss:// endpoint (dev only).")
	flag.BoolVar(&coreAllowPlaintext, "core-allow-plaintext", false, "Allow a plaintext ws:// endpoint (events travel unencrypted; dev/trusted-network only).")

	// Virtual apps (docs/todo/virtual-apps.md): label-derived Application synthesis
	// layered onto push mode's existing Deployment/StatefulSet/DaemonSet visibility
	// (push/virtualapps.go). Lives entirely inside *push.Pusher, so this is a no-op
	// when push mode itself is disabled — there is no separate collector to add.
	flag.StringVar(&virtualApps, "virtual-apps", push.VirtualAppsOn,
		`Label-derived Application synthesis mode: "off" (no extra informers) or "on" (default; merge every synthesized app into the snapshot and push their deltas). Requires --triage-endpoint.`)

	// Sensor stream (docs/adr/0015-sensor-stream-grpc.md): the sensors of the
	// eBPF DaemonSet stream OTLP to the controller over gRPC; the collectors
	// below read from it.
	flag.StringVar(&sensorListen, "sensor-listen", "", "controller: address the sensor stream gRPC server listens on, e.g. :9470. Empty disables it and the collectors that read from it.")
	flag.StringVar(&sensorTLSSecret, "sensor-tls-secret", "", "controller: Secret in this pod's namespace that keeps the sensor stream's self-signed certificate (the chart creates it empty).")
	flag.StringVar(&sensorCAConfigMap, "sensor-ca-configmap", "", "controller: ConfigMap in this pod's namespace the controller publishes its certificate to as ca.crt, for the sensors to pin.")
	flag.StringVar(&sensorService, "sensor-service", "", "controller: name of the Service in front of the sensor stream; the certificate covers its DNS names.")
	flag.StringVar(&sensorServiceAccount, "sensor-service-account", "", "controller: the sensors' ServiceAccount in this pod's namespace; tokens of any other account are refused.")
	flag.StringVar(&sensorExtraNames, "sensor-extra-names", "", "controller: comma-separated host names or IP addresses the serving certificate also covers, for senders that reach a controller outside the cluster by its own address.")
	flag.DurationVar(&sensorStale, "sensor-stale", 2*time.Minute, "controller: how old a sensor's latest snapshot may be and still be read.")
	// Replica gossip (ADR-0015 phase 4): every replica shares what its own
	// sensors send, so the leader sees every node.
	flag.StringVar(&meshListen, "mesh-listen", "", "controller: address the replica gossip listens on, e.g. :9471. Empty runs no gossip (one replica). Needs --sensor-listen.")
	flag.StringVar(&meshService, "mesh-service", "", "controller: headless Service in this pod's namespace whose DNS name resolves to the peer replicas.")
	// OTLP receiver (docs/adr/0017-otlp-receiver.md): metrics pushed by
	// OpenTelemetry SDKs and Collectors, for clusters without the DaemonSet.
	flag.StringVar(&otlpListen, "otlp-listen", "", "controller: address the OTLP/gRPC metrics receiver listens on, e.g. :4317. Empty disables it. Needs --sensor-listen.")
	flag.StringVar(&otlpAllowed, "otlp-allowed-service-accounts", "", "controller: comma-separated namespace/serviceaccount (or namespace/*) whose projected tokens, audience triage-agent-otlp, may send OTLP.")
	flag.StringVar(&otlpForwarders, "otlp-forwarders", "", "controller: comma-separated namespace/serviceaccount (or namespace/*), also in --otlp-allowed-service-accounts, whose tokens may publish for any namespace (Collectors running the k8sattributes processor as a gateway). Any other token sender may publish only for its own namespace.")
	flag.IntVar(&otlpMaxResources, "otlp-max-resources", 2000, "controller: most OTLP resources (sources) the receiver holds; new ones past it are refused.")
	flag.IntVar(&otlpMaxPoints, "otlp-max-points-per-resource", 5000, "controller: most series the receiver holds for one OTLP resource.")
	flag.IntVar(&otlpMaxPerSender, "otlp-max-resources-per-sender", 500, "controller: most OTLP resources one authenticated sender may create.")
	flag.IntVar(&otlpMaxRequest, "otlp-max-request-bytes", 4<<20, "controller: largest OTLP Export request, in bytes (the gRPC receive limit).")
	flag.IntVar(&otlpMaxSeries, "otlp-max-series", 250_000, "controller: most series the receiver holds over every OTLP resource; new ones past it are refused. This is what its memory is sized by.")
	flag.StringVar(&otlpClientCASecret, "otlp-client-ca-secret", "", "controller: Secret in the pod namespace for the agent's own client CA for the OTLP receiver. The controller creates the CA on first use and trusts it for mutual TLS; issue a sender's certificate with `triage-agent otlp-cert`.")
	flag.StringVar(&otlpClientCA, "otlp-client-ca-file", "", "controller: PEM CA for mutual TLS on the OTLP receiver; a sender whose client certificate chains to it needs no token.")
	// The otlp sensor role (docs/adr/0022-otlp-sensor-role.md): --mode=otlp
	// serves the receiver above in its own pods and streams every source to
	// the controller, which then needs no OTLP listener of its own.
	flag.BoolVar(&sensorOTLPRole, "sensor-otlp-role", false, "controller: accept the otlp role on the sensor stream. A peer that presents the controller's own serving certificate (its pods mount it to serve OTLP) is the role; its streams may carry OTLP sources, and it may run no other role.")
	flag.StringVar(&sensorClientCert, "sensor-client-cert-file", "", "otlp role: PEM certificate presented to the controller's sensor stream instead of a token, the controller's serving certificate.")
	flag.StringVar(&sensorClientKey, "sensor-client-key-file", "", "otlp role: the key of --sensor-client-cert-file.")
	flag.StringVar(&otlpTLSCert, "otlp-tls-cert-file", "", "otlp role: PEM certificate the receiver serves, the controller's sensor-stream certificate (mounted from its Secret) so senders keep pinning the one in <fullname>-sensor-ca.")
	flag.StringVar(&otlpTLSKey, "otlp-tls-key-file", "", "otlp role: the key of --otlp-tls-cert-file.")
	flag.BoolVar(&otlpInsecure, "otlp-insecure", false, "otlp role: serve plain gRPC and authenticate nobody, for Collectors inside the cluster that have neither a client certificate nor a token for the agent. Every sender is an anonymous forwarder named by its address; fence the port with a NetworkPolicy (chart otlp.insecure). Excludes the certificate, client CA, token and tenant flags.")
	flag.StringVar(&otlpTenantsFlag, "otlp-tenants", "", "otlp role: comma-separated tenants a client certificate may name (the tenants the agent joined the core with). The controller checks them again.")
	flag.StringVar(&sensorController, "sensor-controller", "", "sensor roles: host:port of the controller's sensor stream Service. Empty sends nothing.")
	flag.StringVar(&sensorCAFile, "sensor-ca-file", "/etc/triage-sensor/ca.crt", "sensor roles: the controller certificate to pin.")
	flag.StringVar(&sensorTokenFile, "sensor-token-file", "/var/run/secrets/triage-sensor/token", "sensor roles: projected ServiceAccount token for the sensor stream audience.")
	flag.DurationVar(&sensorTick, "sensor-tick", 15*time.Second, "sensor roles: how often each role's metrics and the sensor's state are sent.")

	// Log-based metrics (ADR-0006, ADR-0014): read the log-exporter role's
	// counters from the sensor stream and forward error/warn/total counts to
	// triage as log_metrics frames over the push connection. Off unless
	// --log-metrics-enabled is set; a no-op if push mode itself is disabled.
	flag.BoolVar(&logMetricsEnabled, "log-metrics-enabled", false, "Enable the log-based metrics collector (reads the sensor stream, sends log_metrics frames via push mode).")
	flag.Int64Var(&logMetricsIntervalSeconds, "log-metrics-interval-seconds", 60, "Gate-evaluation interval in seconds.")
	flag.Int64Var(&logMetricsErrorThreshold, "log-metrics-error-threshold", 10, "Minimum error-count OR warn-count delta in one interval for a service to be reported (either counter crossing it independently qualifies).")

	// eBPF RED metrics via the embedded OBI engine (ADR-0009): read the ebpf
	// role's OTLP from the sensor stream and forward per-service HTTP
	// request/error counts to triage as red_metrics frames. Off unless
	// --red-metrics-enabled is set; a no-op if push mode itself is disabled.
	flag.BoolVar(&redMetricsEnabled, "red-metrics-enabled", false, "Enable the eBPF RED-metrics collector (reads the sensor stream, sends red_metrics frames via push mode).")
	flag.Int64Var(&redMetricsIntervalSeconds, "red-metrics-interval-seconds", 60, "Gate-evaluation interval in seconds.")
	flag.Int64Var(&redMetricsErrorThreshold, "red-metrics-error-threshold", 10, "Minimum error-count delta in one interval for a service to be reported.")
	flag.Float64Var(&redMetricsMinErrorRatePct, "red-metrics-min-error-rate-pct", 5, "Minimum error rate (errorCount*100 >= this*requestCount) in one interval for a service to be reported. Both this AND --red-metrics-error-threshold must hold.")
	flag.Float64Var(&redLatencyMultiplier, "red-latency-multiplier", 5, "Third way through the RED gate: a service also qualifies when its p99 in one interval is at least this many times the median p99 of its own recent normal intervals (and passes the floor, request and baseline minimums below). 0 disables.")
	flag.Float64Var(&redLatencyFloorMs, "red-latency-floor-ms", 1000, "Absolute p99 (milliseconds) the latency arm also needs, so 1 ms -> 6 ms never fires.")
	flag.Int64Var(&redLatencyMinRequests, "red-latency-min-requests", 10, "Minimum requests in an interval for its p99 to be judged by the latency arm or added to the baseline.")
	flag.IntVar(&redLatencyMinBaselineWindows, "red-latency-min-baseline-windows", 10, "Minimum prior intervals with a p99 reading a service needs before the latency arm can fire (the baseline is in memory only, so it restarts empty).")
	flag.Float64Var(&redMetricsOperationErrorRatePct, "red-operation-error-rate-pct", 50, "Second way through the RED gate: a service also qualifies when any single operation (HTTP method+route, gRPC method) has at least --red-metrics-error-threshold errors AND this error rate of its own requests in one interval, even if the service-wide rate is under --red-metrics-min-error-rate-pct. 0 disables.")
	flag.BoolVar(&redBaseline, "red-baseline", true, "Baseline arms for RED: a service or one of its operations also qualifies when its error rate or p99, summed over up to --red-baseline-evidence-windows intervals, is --red-baseline-multiplier times its own recent normal and above the floors below. false keeps only the fixed gates.")
	flag.Float64Var(&redBaselineMultiplier, "red-baseline-multiplier", 0, "Multiplier over the key's own baseline median for the RED baseline arms (0 = default 3).")
	flag.IntVar(&redBaselineEvidenceWindows, "red-baseline-evidence-windows", 0, "Most intervals a quiet operation's evidence is summed over (0 = default 5).")
	flag.Float64Var(&redBaselineErrFloorPct, "red-baseline-err-floor-pct", 0, "Error-rate floor percent for the RED baseline error arms (0 = default 5).")
	flag.Float64Var(&redBaselineLatFloorMs, "red-baseline-lat-floor-ms", 0, "p99 floor in milliseconds for the RED baseline latency arms (0 = default 300).")
	flag.IntVar(&redBaselineMinBaselineWindows, "red-baseline-min-baseline-windows", 0, "Readings a service or operation needs before it is judged against its own baseline; until then it is judged against the cold floors (0 = default 5).")
	flag.Int64Var(&redBaselineErrMinRequests, "red-baseline-err-min-requests", 0, "Requests, summed over the evidence intervals, an error-rate verdict needs (0 = default 20).")
	flag.Int64Var(&redBaselineErrMinErrors, "red-baseline-err-min-errors", 0, "Errors, summed over the evidence intervals, an error-rate breach needs (0 = default 3).")
	flag.Int64Var(&redBaselineLatMinRequests, "red-baseline-lat-min-requests", 0, "Requests, summed over the evidence intervals, a p99 verdict needs (0 = default 10).")
	flag.Float64Var(&redBaselineErrColdFloorPct, "red-baseline-err-cold-floor-pct", 0, "Error rate percent at or above which a service or operation with no baseline yet (every one after an agent restart) is reported and not learned as normal (0 = default 15; negative disables, so a cold key is silent and learns whatever it reads).")
	flag.Float64Var(&redBaselineLatColdFloorMs, "red-baseline-lat-cold-floor-ms", 0, "p99 in milliseconds at or above which a service or operation with no baseline yet is reported and not learned as normal (0 = default 5000; negative disables). Raise it when operations that are slow by nature keep firing.")
	flag.Int64Var(&redBaselineErrSparseMinErrors, "red-baseline-err-sparse-min-errors", 0, "Low-volume rule: a service or operation with fewer than --red-baseline-err-min-requests requests over the evidence intervals still qualifies with this many errors and a failure share of --red-baseline-err-sparse-rate-pct (0 = default 5; negative disables the rule).")
	flag.Float64Var(&redBaselineErrSparseRatePct, "red-baseline-err-sparse-rate-pct", 0, "Failure share percent for the low-volume rule (0 = default 50).")
	flag.Int64Var(&redBaselineLatChronicMinRequests, "red-baseline-lat-chronic-min-requests", 0, "Requests a breaching p99 needs before it can teach the gate that the key is chronically spiky and mute it; a p99 over a handful of requests is their maximum (0 = default 100; negative disables the check).")
	flag.IntVar(&redBaselineMaxOperations, "red-baseline-max-operations", 0, "Operations of one service tracked one by one; the rest are judged together as one (0 = default 100; negative removes the cap).")
	flag.BoolVar(&useBaseline, "use-baseline", true, "Baseline arms for USE: CPU cores used (and throttling, when a limit exists) against the service's own baseline. false keeps only the PSI/OOM/memory gates.")
	flag.Float64Var(&useBaselineMultiplier, "use-baseline-multiplier", 0, "Multiplier over the service's own baseline for the USE baseline arms (0 = default 3).")
	flag.Float64Var(&useBaselineCPUFloorCores, "use-baseline-cpu-floor-cores", 0, "Cores a service must use for the CPU baseline arm to fire (0 = default 0.25).")
	flag.Float64Var(&useBaselineCPUColdFloorCores, "use-baseline-cpu-cold-floor-cores", 0, "Cores (summed across a service's pods) at or above which a service with no baseline yet is reported and not learned as normal. 0 (default) disables: a cold service is silent on the CPU arm and learns whatever it reads.")
	flag.BoolVar(&logBaseline, "log-baseline", true, "Baseline gate for log metrics: once a service has a baseline, report it only on a step (--log-baseline-multiplier times its median and at least --log-metrics-error-threshold lines), so a chronic stream stops re-reporting. false keeps the presence gate.")
	flag.Float64Var(&logBaselineMultiplier, "log-baseline-multiplier", 0, "Step multiplier for the log baseline gate (0 = default 3).")
	flag.Int64Var(&logBaselineColdFloorLines, "log-baseline-cold-floor-lines", 0, "Error or warn lines per interval at or above which a service with no baseline yet is reported as a step and not learned as normal. 0 (default) disables: a cold stream is reported on the presence gate and learned.")

	// USE metrics via the node-exporter role (ADR-0010): read its per-pod
	// cgroup/PSI metrics from the sensor stream and forward per-service
	// utilization/saturation/error counters to triage as use_metrics frames.
	// Off unless --use-metrics-enabled is set; a no-op if push mode itself is
	// disabled.
	flag.BoolVar(&useMetricsEnabled, "use-metrics-enabled", false, "Enable the USE-metrics collector (reads the sensor stream, sends use_metrics frames via push mode).")
	flag.Int64Var(&useMetricsIntervalSeconds, "use-metrics-interval-seconds", 60, "Gate-evaluation interval in seconds.")
	flag.Float64Var(&useMetricsMemPsiThreshold, "use-metrics-mem-psi-threshold", 20, "Minimum memory PSI 'some avg60' percent (max across a service's pods) for a service to be reported. Any one of the mem/cpu/io PSI thresholds crossing, OR at least one OOM kill this window, qualifies (OR, not AND).")
	flag.Float64Var(&useMetricsCPUPsiThreshold, "use-metrics-cpu-psi-threshold", 40, "Minimum CPU PSI 'some avg60' percent (max across a service's pods) for a service to be reported.")
	flag.Float64Var(&useMetricsIOPsiThreshold, "use-metrics-io-psi-threshold", 40, "Minimum IO PSI 'some avg60' percent (max across a service's pods) for a service to be reported.")
	flag.Float64Var(&useMetricsMemPctThreshold, "use-metrics-mem-pct-threshold", 85, "Minimum memory UTILIZATION percent (working set vs limit, max across a service's pods) for a service to be reported — the LEADING gate arm, since PSI and OOM kills both only fire once a service is already suffering. 0 disables this arm (0% utilization means no limit is set, so a 0 threshold would match every unbounded pod).")
	flag.Float64Var(&useMetricsMemNodePctThreshold, "use-metrics-mem-node-pct-threshold", 0, "Minimum memory utilization percent of the NODE's total memory (working set vs node capacity, max across a service's pods) for a service to be reported — the portable sibling of --use-metrics-mem-pct-threshold, using a denominator that always exists even for a pod with no memory limit set. Default 0 DISABLES this arm: it is off pending observed numbers from a live fleet, not a considered threshold. Turn it on once those numbers are in.")

	// Graph-drift topology snapshot (ADR-0011, now ADR-0013): derive an observed
	// service topology from the SAME ebpf metrics --red-metrics-enabled reads —
	// nodes from presence in the requests metric, edges from the client-side
	// metrics and network flows — and forward a FULL snapshot (level state, no gate, unlike
	// red/use/log-metrics) as a topology_snapshot frame via push mode. Off unless
	// --topology-enabled is set; a no-op if push mode itself is disabled.
	flag.BoolVar(&topologyEnabled, "topology-enabled", false, "Enable the topology collector (derives an observed service topology from the ebpf role's metrics, sends topology_snapshot frames via push mode).")
	flag.Int64Var(&topologyIntervalSeconds, "topology-interval-seconds", 600, "Snapshot flush cadence in seconds — deltas accumulate across every --red-metrics-interval-seconds tick since the last flush. Not a gated collection window like red/use/log-metrics — a lost frame heals on the next flush.")

	// Bind the zap logging flags (--zap-log-level, --zap-devel, --zap-encoder, ...) so log
	// verbosity is controllable at runtime.
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	applyMemoryLimit(setupLog)

	// First line in `kubectl logs`, before the node-exporter branch below, so every
	// mode identifies which build is running. Reports "dev"/"unknown" only for a
	// bare `go build`; the Makefile, .ko.yaml and CI all stamp these.
	setupLog.Info("triage-agent starting",
		"version", appVersion(), "commit", gitCommit, "buildDate", buildDate, "mode", strings.Join(modes.roles(), ","))

	// Sensor roles (--mode=ebpf, node-exporter, log-exporter; repeat --mode
	// for several) run as goroutines in ONE process, so the DaemonSet can be a
	// single container and its layout is a chart choice. No role listens:
	// each hands its metrics as OTLP to the sensor stream client, which sends
	// them to the controller (docs/adr/0015-sensor-stream-grpc.md). The ebpf
	// role exists only in builds tagged `obi`; the untagged build fails fast
	// with a clear error (ebpf_stub.go).
	host, err := hostModeSelected(modes)
	if err != nil {
		setupLog.Error(err, "invalid --mode")
		os.Exit(1)
	}
	if roles := modes.roles(); !host && (len(roles) > 1 || roles[0] != "controller") {
		os.Exit(runSensorRoles(roles, sensorRoleOptions{
			cgroupRoot:           nodeExporterCgroupRoot,
			procRoot:             nodeExporterProcRoot,
			sysRoot:              nodeExporterSysRoot,
			logIncludeNamespaces: logExporterIncludeNamespaces,
			logErrorRegex:        logExporterErrorRegex,
			logWarnRegex:         logExporterWarnRegex,
			logSampleMaxLen:      logExporterSampleMaxLen,
			logMaxSamples:        logExporterMaxSamplesPerService,
			logSampleJSONFields:  logExporterSampleJSONFields,
			otlp: otlpSensorFlags{
				insecure: otlpInsecure,
				listen:   otlpListen, tlsCert: otlpTLSCert, tlsKey: otlpTLSKey, clientCA: otlpClientCA,
				allowed: mustParseOTLPAllowed(otlpAllowed), forwarders: mustParseOTLPAllowed(otlpForwarders),
				tenants:      splitNonEmpty(otlpTenantsFlag),
				maxResources: otlpMaxResources, maxPoints: otlpMaxPoints, maxPerSender: otlpMaxPerSender, maxRequest: otlpMaxRequest,
				maxSeries: otlpMaxSeries,
			},
			clientCert: sensorClientCert, clientKey: sensorClientKey,
			controller: sensorController,
			caFile:     sensorCAFile,
			tokenFile:  sensorTokenFile,
			tick:       sensorTick,
		}))
	}

	logMetricsOptions := func() logmetrics.Options {
		return logmetrics.Options{
			Interval:       time.Duration(logMetricsIntervalSeconds) * time.Second,
			ErrorThreshold: logMetricsErrorThreshold,
			Baseline: logmetrics.BaselineOptions{
				Enabled:        &logBaseline,
				Multiplier:     logBaselineMultiplier,
				ColdFloorLines: logBaselineColdFloorLines,
			},
		}
	}
	useMetricsOptions := func() usemetrics.Options {
		return usemetrics.Options{
			Interval:            time.Duration(useMetricsIntervalSeconds) * time.Second,
			MemPsiThreshold:     useMetricsMemPsiThreshold,
			CPUPsiThreshold:     useMetricsCPUPsiThreshold,
			IOPsiThreshold:      useMetricsIOPsiThreshold,
			MemPctThreshold:     useMetricsMemPctThreshold,
			MemNodePctThreshold: useMetricsMemNodePctThreshold,
			Baseline: usemetrics.BaselineOptions{
				Enabled:           &useBaseline,
				Multiplier:        useBaselineMultiplier,
				CPUFloorCores:     useBaselineCPUFloorCores,
				CPUColdFloorCores: useBaselineCPUColdFloorCores,
			},
		}
	}
	redMetricsOptions := func() redsource.Options {
		return redsource.Options{
			Profile:          redsource.ProfileOBI,
			Interval:         time.Duration(redMetricsIntervalSeconds) * time.Second,
			TopologyInterval: time.Duration(topologyIntervalSeconds) * time.Second,
			ErrorThreshold:   redMetricsErrorThreshold,
			MinErrorRatePct:  redMetricsMinErrorRatePct,

			OperationErrorRatePct: redMetricsOperationErrorRatePct,

			LatencyMultiplier:         redLatencyMultiplier,
			LatencyFloorMs:            redLatencyFloorMs,
			LatencyMinRequests:        redLatencyMinRequests,
			LatencyMinBaselineWindows: redLatencyMinBaselineWindows,

			Baseline: redsource.BaselineOptions{
				Enabled:         &redBaseline,
				Multiplier:      redBaselineMultiplier,
				EvidenceWindows: redBaselineEvidenceWindows,
				ErrFloorPct:     redBaselineErrFloorPct,
				LatFloorMs:      redBaselineLatFloorMs,

				MinBaselineWindows:    redBaselineMinBaselineWindows,
				ErrMinRequests:        redBaselineErrMinRequests,
				ErrMinErrors:          redBaselineErrMinErrors,
				LatMinRequests:        redBaselineLatMinRequests,
				ErrColdFloorPct:       redBaselineErrColdFloorPct,
				LatColdFloorMs:        redBaselineLatColdFloorMs,
				ErrSparseMinErrors:    redBaselineErrSparseMinErrors,
				ErrSparseRatePct:      redBaselineErrSparseRatePct,
				LatChronicMinRequests: redBaselineLatChronicMinRequests,
				MaxOperations:         redBaselineMaxOperations,
			},

			EnableRED:      redMetricsEnabled,
			EnableTopology: topologyEnabled,
		}
	}

	// No core URL: with the TUI or the monitoring API on, run standalone, the
	// pusher resolving apps and events for them and never dialling out.
	standalone := triageEndpoint == "" && (uiListen != "" || apiListen != "")
	if standalone && clusterName == "" {
		clusterName = defaultStandaloneCluster()
	}
	pushOn := triageEndpoint != "" || standalone

	// Host mode (docs/adr/0018-linux-host-mode.md): one process on a plain
	// Linux host, no Kubernetes API and no controller-runtime manager.
	if host {
		if apiSAAuth {
			setupLog.Error(nil, "--api-sa-auth needs a Kubernetes API: it is for cluster mode, use --api-tokens-file on a host")
			os.Exit(1)
		}
		joinStateDirSet := false
		flag.Visit(func(f *flag.Flag) { joinStateDirSet = joinStateDirSet || f.Name == "join-state-dir" })
		os.Exit(runHost(hostOptions{
			standalone: standalone, endpoint: triageEndpoint, allowPlaintext: coreAllowPlaintext, insecure: coreInsecure,
			cluster: clusterName, tenant: targetTenant,
			joinCAFingerprint: joinCAFingerprint, joinStateDir: hostJoinStateDir(joinStateDir, joinStateDirSet), joinSecret: joinSecret,
			joinTenants: []string(joinTenants),
			heartbeat:   time.Duration(coreHeartbeat) * time.Second,
			uiListen:    uiListen, uiAdminKeyFile: uiAdminKeyFile, uiAdminKey: hostUIAdminKey(uiAdminKey), uiMaxEvents: uiMaxEvents,
			api:       agentui.APIOptions{Addr: apiListen, TokensFile: apiTokensFile, CertFile: apiTLSCert, KeyFile: apiTLSKey, MCP: apiMCP, Docs: apiDocs},
			namespace: hostNamespace, units: splitNonEmpty(hostUnits), cgroupRoot: hostCgroupRoot, logFiles: []string(hostLogFiles),
			tick:       sensorTick,
			logMetrics: logMetricsEnabled, useMetrics: useMetricsEnabled, redMetrics: redMetricsEnabled, topology: topologyEnabled,
			logOptions: logMetricsOptions, useOptions: useMetricsOptions, redOptions: redMetricsOptions,
		}))
	}

	// No Kubernetes watching (docs/adr/0021-otlp-only-no-kubernetes-watch.md):
	// the agent forwards what its OTLP receiver and sensors send and starts no
	// informer. The tenant comes from --target-tenant, since there is no
	// namespace annotation to read one from.
	noKube := !kubernetesWatch
	if noKube {
		switch {
		case triageEndpoint == "":
			setupLog.Error(nil, "--kubernetes=false needs --triage-endpoint: the agent has nothing else to run")
			os.Exit(1)
		case targetTenant == "":
			setupLog.Error(nil, "--kubernetes=false needs --target-tenant: with no namespace annotation to read, every frame files under one tenant")
			os.Exit(1)
		}
		setupLog.Info("--kubernetes=false: no Kubernetes informers, no virtual apps; the agent forwards OTLP and sensor metrics only", "tenant", targetTenant)
	}

	syncPeriodD := time.Duration(int64(time.Second) * syncPeriod)
	cacheOpts := cache.Options{SyncPeriod: &syncPeriodD}
	// cacheNamespaces mirrors cacheOpts.DefaultNamespaces' keys (nil/empty when the
	// cache is cluster-wide) and is threaded into push.Options.CacheNamespaces below —
	// it is how the pusher tells a namespace covered by the manager cache apart from
	// one that only entered scope dynamically via a triageagent.dev/tenant Namespace annotation
	// (push/namespace_scope.go) and needs its own runtime informer factory instead
	// (controller-runtime cannot add a namespace to an already-scoped cache at
	// runtime; push/dynamic_informers.go covers that gap).
	var cacheNamespaces []string
	// clusterModeCacheNamespaces is the cluster-mode scope resolved just below;
	// empty means "not in cluster mode, or could not resolve" and leaves the
	// pre-existing --namespace behavior untouched.
	var clusterModeCacheNamespaces []string
	// CLUSTER MODE (no primary tenant) scopes the manager cache to the
	// triageagent.dev/tenant-ANNOTATED namespaces, so it does not hold every namespace in
	// the cluster in memory and does not need cluster-wide list/watch on
	// workloads. The annotated set is the rule: a namespace nobody onboarded is
	// neither read nor watched. This is
	// output-preserving, not a reduction in what reaches the core: with no
	// primary tenant, push/tenant_routing.go DROPS every item from a namespace
	// with no triageagent.dev/tenant annotation, so the cluster-wide cache was watching
	// what it could never push. Each annotated namespace gets its own runtime
	// informer set instead (push/dynamic_informers.go), attached live as the
	// annotation appears — including its Warning events, which is why that file
	// grew an Event informer alongside the workload ones.
	//
	// Namespace objects themselves are cluster-scoped, so the annotation
	// watcher still sees every namespace regardless of this setting. An
	// explicit --target-namespaces or --namespace is honored unchanged.
	if namespace == "" && pushOn && targetTenant == "" {
		annotated, err := annotatedNamespaces(ctrl.GetConfigOrDie())
		switch {
		case err != nil:
			setupLog.Error(err, "cluster-mode push: could not list namespaces; keeping a cluster-wide cache")
		case len(annotated) > 0:
			clusterModeCacheNamespaces = annotated
			setupLog.Info("cluster-mode push: scoping the manager cache to the annotated namespaces",
				"key", annotationkeys.Tenant, "namespaces", annotated)
		default:
			// Nothing annotated YET. An empty DefaultNamespaces means a
			// cluster-wide cache, so fall back to this agent's own namespace:
			// the smallest non-empty set that keeps the cache scoped. Namespaces
			// annotated later are picked up by the runtime informers either way.
			if own := currentNamespace(); own != "" {
				clusterModeCacheNamespaces = []string{own}
				setupLog.Info("cluster-mode push: no annotated namespaces yet; scoping the manager cache to this agent's own namespace until one appears",
					"key", annotationkeys.Tenant, "namespace", own)
			} else {
				setupLog.Info("cluster-mode push: could not determine this agent's own namespace; keeping a cluster-wide cache")
			}
		}
	}

	if len(clusterModeCacheNamespaces) > 0 {
		nsCache := make(map[string]cache.Config, len(clusterModeCacheNamespaces))
		for _, ns := range clusterModeCacheNamespaces {
			nsCache[ns] = cache.Config{}
		}
		cacheOpts.DefaultNamespaces = nsCache
		cacheNamespaces = append([]string(nil), clusterModeCacheNamespaces...)
	} else if namespace != "" {
		// The cache must cover every namespace ANY component in this process will LIST,
		// not just the controller's own. Push mode lists Applications across
		// --target-namespaces, so a namespace named there but missing here fails the
		// pusher's initial list with "unable to list: <ns> because of unknown namespace
		// for the cache" and the push connection reconnect-loops forever (the controller
		// itself keeps working, which is what makes this easy to miss).
		//
		// --namespace="" means a cluster-wide cache, which already covers every push
		// namespace — hence this stays inside the namespace != "" branch.
		nsCache := map[string]cache.Config{namespace: {}}
		if pushOn {
			for _, ns := range targetNamespaces {
				nsCache[ns] = cache.Config{}
			}
		}
		cacheOpts.DefaultNamespaces = nsCache
		cacheNamespaces = make([]string, 0, len(nsCache))
		for ns := range nsCache {
			cacheNamespaces = append(cacheNamespaces, ns)
		}
	}
	if noKube && len(cacheOpts.DefaultNamespaces) == 0 {
		// Nothing reads through the cache, so it opens no watch. Scope it to
		// this pod's namespace anyway: a read added later then needs only a
		// namespace Role, never cluster-wide rights by accident.
		if own := currentNamespace(); own != "" {
			cacheOpts.DefaultNamespaces = map[string]cache.Config{own: {}}
		}
	}
	// managedFields are often 30-40% of an object's serialized size and the controller
	// never reads them; stripping them before they're committed to the informer cache
	// cuts steady-state memory noticeably across every watched GVK.
	cacheOpts.DefaultTransform = cache.TransformStripManagedFields()

	// Push mode's pusher registers GetInformer(&corev1.Event{}), which by default caches
	// EVERY Event cluster-wide — the highest-churn resource in most clusters. Scope the
	// informer itself to Warning events at the cache level; the pusher's own
	// e.Type != EventTypeWarning check in onEvent stays as defense in depth. Only set this
	// in push mode: the Event informer is never started otherwise, but be explicit rather
	// than relying on that.
	if pushOn {
		cacheOpts.ByObject = map[client.Object]cache.ByObject{
			&corev1.Event{}: {Field: fields.OneTermEqualSelector("type", corev1.EventTypeWarning)},
		}
	}

	cfg := ctrl.GetConfigOrDie()

	// discoveryClient gates --workload-resources against what the server
	// actually serves (push.Options.Discovery), queried once at startup — a
	// configured resource the server does not serve is skipped rather than
	// producing an error-looping informer.
	var discoveryClient discovery.DiscoveryInterface
	if noKube {
		// No workloads are watched, so there is nothing to gate.
	} else if dc, dcErr := discovery.NewDiscoveryClientForConfig(cfg); dcErr != nil {
		setupLog.Error(dcErr, "unable to build discovery client to gate --workload-resources against server discovery")
	} else {
		discoveryClient = dc
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:           scheme,
		Metrics:          metricsserver.Options{BindAddress: metricsAddr},
		LeaderElection:   enableLeaderElection,
		LeaderElectionID: leaderElectionID,
		// In a pod this is the pod's own namespace either way; outside one
		// (a kubeconfig run) controller-runtime cannot infer it.
		LeaderElectionNamespace: podNamespace(),
		Cache:                   cacheOpts,
	})
	if err != nil {
		setupLog.Error(err, "unable to start triage-agent")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	// Push mode: stream Applications + Warning events to a triage agent (ADR-0005).
	var pusher *push.Pusher
	if pushOn {
		if triageEndpoint != "" {
			if err := push.ValidateEndpoint(triageEndpoint, coreAllowPlaintext); err != nil {
				setupLog.Error(err, "invalid --triage-endpoint")
				os.Exit(1)
			}
		} else {
			setupLog.Info("no --triage-endpoint: running standalone, the local TUI and monitoring API show this cluster and nothing is sent anywhere", "cluster", clusterName)
		}
		if clusterName == "" {
			setupLog.Error(nil, "--cluster-name is required when --triage-endpoint is set")
			os.Exit(1)
		}
		if targetTenant == "" {
			// Cluster-mode (docs/todo/virtual-apps.md "Cluster-mode push",
			// addendum 2026-08-27): no primary tenant — the agent is
			// identified only by --cluster-name (already validated above),
			// and every frame's tenant is derived purely from triageagent.dev/tenant
			// namespace annotations (push/tenant_routing.go). The core
			// accepts the empty-tenant hello for the tenants the agent's
			// certificate is bound to and drops the rest.
			setupLog.Info("--target-tenant not set; running in cluster-mode — every frame's tenant is derived from namespace annotations, and an unmapped namespace's items are dropped rather than defaulted",
				"key", annotationkeys.Tenant, "cluster", clusterName)
		}
		if joinCAFingerprint != "" && triageEndpoint != "" {
			if len(joinTenants) == 0 && targetTenant == "" {
				setupLog.Error(nil, "agent join needs --join-tenant (repeatable) or --target-tenant")
				os.Exit(1)
			}
			if joinSecret != "" && podNamespace() == "" {
				setupLog.Error(nil, "--join-secret needs the pod namespace (POD_NAMESPACE or the ServiceAccount mount)")
				os.Exit(1)
			}
			setupLog.Info("push authenticates by agent join certificate", "caFingerprint", joinCAFingerprint, "secret", joinSecret, "stateDir", joinStateDir)
		}
		if err := checkPushCredentials(triageEndpoint, joinCAFingerprint); err != nil {
			setupLog.Error(err, "invalid push credentials")
			os.Exit(1)
		}
		if err := push.ValidateVirtualAppsMode(virtualApps); err != nil {
			setupLog.Error(err, "invalid --virtual-apps")
			os.Exit(1)
		}
		parsedWorkloadResources, err := push.ParseWorkloadResourceList(workloadResources)
		if err != nil {
			setupLog.Error(err, "invalid --workload-resources")
			os.Exit(1)
		}
		nsList := []string(targetNamespaces)
		if len(nsList) == 0 && namespace != "" {
			nsList = []string{namespace}
		}
		// pushClientset backs push/dynamic_informers.go's per-namespace runtime
		// informer factories — the fallback for a triageagent.dev/tenant-annotated namespace
		// that entered DYNAMIC scope (namespace_scope.go) but isn't covered by
		// cacheNamespaces above. Built from the manager's own rest.Config, so it
		// authenticates identically to every other client in this process (same SA,
		// same RBAC — see charts/triage-agent/templates/rbac.yaml). A build failure
		// here is logged and left nil: push.Options.Clientset is documented nil-safe,
		// degrading to a per-namespace logged error rather than a crash.
		// Built in --kubernetes=false too: the join certificate is kept in a
		// Secret through this client (a get and update in the pod's own
		// namespace). A client opens no watch; only the informers do.
		pushClientset, err := kubernetes.NewForConfig(mgr.GetConfig())
		if err != nil {
			setupLog.Error(err, "unable to build clientset for out-of-cache dynamic namespace informers; namespace-scope-by-annotation will degrade to a logged error for any namespace outside the static cache scope")
			pushClientset = nil
		}
		// pushDynamicClient backs the same runtime factories' generic workload
		// informers for a resource this chart's typed clientset above cannot
		// watch (e.g. an Argo Rollout). Same nil-safe degrade contract as the
		// clientset.
		var pushDynamicClient dynamic.Interface
		if !noKube {
			pushDynamicClient, err = dynamic.NewForConfig(mgr.GetConfig())
			if err != nil {
				setupLog.Error(err, "unable to build dynamic client for out-of-cache dynamic namespace workload informers; virtual apps in such namespaces will not be observed")
				pushDynamicClient = nil
			}
		}
		pusher = push.New(push.Options{
			Endpoint:            triageEndpoint,
			Standalone:          standalone,
			ClusterName:         clusterName,
			Tenant:              targetTenant,
			JoinCAFingerprint:   joinCAFingerprint,
			JoinStateDir:        joinStateDir,
			JoinSecretName:      joinSecret,
			JoinSecretNamespace: podNamespace(),
			JoinTenants:         []string(joinTenants),
			Namespaces:          nsList,
			AgentVersion:        appVersion(),
			Heartbeat:           time.Duration(coreHeartbeat) * time.Second,
			InsecureTLS:         coreInsecure,
			LogMetrics:          logMetricsEnabled,
			RedMetrics:          redMetricsEnabled,
			UseMetrics:          useMetricsEnabled,
			Topology:            topologyEnabled,
			SensorStream:        sensorListen != "",
			VirtualApps:         virtualApps,
			NoKubernetes:        noKube,
			CacheNamespaces:     cacheNamespaces,
			Clientset:           pushClientset,
			DynamicClient:       pushDynamicClient,
			WorkloadResources:   parsedWorkloadResources,
			Discovery:           discoveryClient,
		}, mgr, ctrl.Log)
		if err := mgr.Add(pusher); err != nil {
			setupLog.Error(err, "unable to add push runnable")
			os.Exit(1)
		}
		setupLog.Info("push mode enabled", "endpoint", triageEndpoint, "cluster", clusterName, "tenant", targetTenant, "namespaces", nsList, "virtualApps", virtualApps)
	}

	// Sensor stream (docs/adr/0015-sensor-stream-grpc.md): the hub every
	// collector below reads from. Nil when --sensor-listen is empty.
	hub, meshNet := startSensorHub(mgr, sensorHubOptions{
		listen: sensorListen, tlsSecret: sensorTLSSecret, caConfigMap: sensorCAConfigMap,
		service: sensorService, serviceAccount: sensorServiceAccount, stale: sensorStale,
		extraNames: splitNonEmpty(sensorExtraNames),
		meshListen: meshListen, meshService: meshService,
		otlpListen: otlpListen, otlpAllowed: mustParseOTLPAllowed(otlpAllowed), otlpForwarders: mustParseOTLPAllowed(otlpForwarders), otlpClientCA: otlpClientCA, otlpClientCASecret: otlpClientCASecret,
		otlpMaxResources: otlpMaxResources, otlpMaxPoints: otlpMaxPoints,
		otlpMaxPerSender: otlpMaxPerSender, otlpMaxRequest: otlpMaxRequest, otlpMaxSeries: otlpMaxSeries,
		otlpTenants:    joinedTenants(joinTenants, targetTenant),
		otlpSensorRole: sensorOTLPRole,
		// The scope the core sets per workspace; a nil pusher allows all.
		otlpNamespaceInScope: pusher.WorkspaceNamespaceInScope,
	})
	if noKube && pusher != nil && hub != nil {
		// No workloads are watched, so the OTLP services are the applications.
		if err := pusher.SetAppSource(otlpApps{hub: hub}); err != nil {
			setupLog.Error(err, "unable to attach the OTLP app source")
			os.Exit(1)
		}
		setupLog.Info("--kubernetes=false: applications come from the services OTLP senders report")
	}
	if uiAdminKey != "" {
		setupLog.Error(nil, "--ui-admin-key is for host mode only: a controller takes its admin key from a Secret through --ui-admin-key-file")
		os.Exit(1)
	}
	if pusher != nil {
		apiOpts := agentui.APIOptions{Addr: apiListen, TokensFile: apiTokensFile, CertFile: apiTLSCert, KeyFile: apiTLSKey, MCP: apiMCP, Docs: apiDocs}
		if apiSAAuth {
			kc, err := kubernetes.NewForConfig(mgr.GetConfig())
			if err != nil {
				setupLog.Error(err, "--api-sa-auth: kubernetes client")
				os.Exit(1)
			}
			apiOpts.SA = &agentui.SAAuth{Client: kc}
		}
		runnables, err := newUI(uiListen, uiAdminKeyFile, "", uiMaxEvents, apiOpts, "cluster", clusterName, pusher, meshNet, sensorInfos(hub), ctrl.Log)
		if err != nil {
			setupLog.Error(err, "invalid --ui-listen")
			os.Exit(1)
		}
		for _, r := range runnables {
			if err := mgr.Add(r); err != nil {
				setupLog.Error(err, "unable to add TUI runnable")
				os.Exit(1)
			}
		}
	}
	if hub == nil && (logMetricsEnabled || useMetricsEnabled || redMetricsEnabled || topologyEnabled) {
		setupLog.Error(nil, "the log/use/red-metrics and topology collectors read the sensor stream: set --sensor-listen")
		os.Exit(1)
	}
	// Sensor status (ADR-0015 phase 1): reports every sensor's own health to
	// the core every 60s, when both the sensor stream and push are enabled.
	startSensorStatusLoop(mgr, hub, pusher)

	// Pass an explicit untyped nil Sender (not the *push.Pusher variable, even
	// though it may itself be nil) when push mode is disabled: a nil
	// *push.Pusher stored in the Sender interface would NOT compare equal to
	// nil inside New (Go's typed-nil-interface trap), so the collector would
	// start and run pointlessly.
	if logMetricsEnabled {
		var sender logmetrics.Sender
		var logNSAllowed func(string) bool
		if pusher != nil {
			sender = pusher
			// Dynamic scope hook: the pusher's live scope union (static
			// --target-namespaces + triageagent.dev/tenant-annotated namespaces,
			// push/namespace_scope.go) supersedes the static Namespaces list
			// below, so an annotation-scoped namespace's services enter
			// log_metrics payloads without a restart.
			logNSAllowed = pusher.NamespaceInScope
		}
		lo := logMetricsOptions()
		lo.Source = hub
		// Same namespace list the pusher uses: the sensors see more
		// namespaces than this connection's tenant owns, and every frame
		// this pusher sends is stamped with its own handshake-bound tenant.
		lo.Namespaces = targetNamespaces
		lo.NamespaceAllowed = logNSAllowed
		collector := logmetrics.New(lo, sender, ctrl.Log)
		if collector == nil {
			setupLog.Info("log-metrics collector not started: push mode is disabled, so there is nowhere to send frames")
		} else if err := mgr.Add(collector); err != nil {
			setupLog.Error(err, "unable to add log-metrics collector runnable")
			os.Exit(1)
		} else {
			setupLog.Info("log-metrics collector enabled",
				"interval", logMetricsIntervalSeconds, "errorThreshold", logMetricsErrorThreshold)
		}
	}

	if useMetricsEnabled {
		var sender usemetrics.Sender
		var useNSAllowed func(string) bool
		var useIdentityModeFor func(string) string
		var usePodExcluded func(context.Context, string, string) bool
		if pusher != nil {
			sender = pusher
			// Dynamic scope hook — same reasoning as the log-metrics block above.
			useNSAllowed = pusher.NamespaceInScope
			// triageagent.dev/virtual-app-identity hook: resolveServiceIdentity must
			// agree with the pusher's own virtual-app synthesis on which
			// chain names a workload (usemetrics/identity.go).
			useIdentityModeFor = pusher.IdentityModeFor
			// triageagent.dev/triage: "false" hook: an excluded pod's
			// pressure and OOM kills are never aggregated (push/exclusion.go).
			usePodExcluded = pusher.PodExcluded
		}
		var useIdentity usemetrics.IdentityResolver = &usemetrics.OwnerIdentityResolver{Client: mgr.GetClient(), ModeFor: useIdentityModeFor}
		if noKube {
			// No pod or owner reads: keep the sender's own service label.
			useIdentity = usemetrics.NoIdentityResolver{}
		}
		uo := useMetricsOptions()
		uo.Source = hub
		uo.Namespaces = targetNamespaces
		uo.NamespaceAllowed = useNSAllowed
		uo.PodExcluded = usePodExcluded
		collector := usemetrics.New(uo, useIdentity, sender, ctrl.Log)
		if collector == nil {
			setupLog.Info("use-metrics collector not started: push mode is disabled, so there is nowhere to send frames")
		} else if err := mgr.Add(collector); err != nil {
			setupLog.Error(err, "unable to add use-metrics collector runnable")
			os.Exit(1)
		} else {
			setupLog.Info("use-metrics collector enabled",
				"interval", useMetricsIntervalSeconds, "memPsiThreshold", useMetricsMemPsiThreshold,
				"cpuPsiThreshold", useMetricsCPUPsiThreshold, "ioPsiThreshold", useMetricsIOPsiThreshold,
				"memPctThreshold", useMetricsMemPctThreshold, "memNodePctThreshold", useMetricsMemNodePctThreshold)
		}
	}

	// RED + topology engine (docs/adr/0013-red-source-profiles.md): ONE
	// collector drives both red_metrics (--red-metrics-enabled) and
	// topology_snapshot (--topology-enabled) from the ebpf role's metrics.
	if redMetricsEnabled || topologyEnabled {
		var sender redsource.Sender
		var redNSAllowed func(string) bool
		var resolver redsource.Resolver
		var identity redsource.IdentityResolver
		if pusher != nil {
			sender = pusher
			// Dynamic scope hook — same reasoning as the log-metrics block above.
			redNSAllowed = pusher.NamespaceInScope
			if !noKube {
				// Cluster-object address resolution + owner-name identity
				// (docs/todo/service-graph-from-obi.md). Both read the
				// Service/Ingress and workload informers, which a
				// --kubernetes=false agent does not run.
				resolver = redsource.NewK8sResolver(pusher, redNSAllowed)
				identity = pusher
			} else {
				// No cluster index: an OTLP sender's calls are named by
				// host name alone, so its services still get edges.
				resolver = redsource.NewRemoteResolver(redNSAllowed)
			}
		}
		ro := redMetricsOptions()
		ro.Source = hub
		ro.Namespaces = targetNamespaces
		ro.NamespaceAllowed = redNSAllowed
		ro.Resolver = resolver
		ro.Identity = identity
		collector := redsource.New(ro, sender, ctrl.Log)
		if collector == nil {
			setupLog.Info("redsource collector not started: push mode is disabled, so there is nowhere to send frames")
		} else if err := mgr.Add(collector); err != nil {
			setupLog.Error(err, "unable to add redsource collector runnable")
			os.Exit(1)
		} else {
			setupLog.Info("redsource collector enabled",
				"redMetrics", redMetricsEnabled, "topology", topologyEnabled,
				"interval", redMetricsIntervalSeconds, "topologyInterval", topologyIntervalSeconds,
				"errorThreshold", redMetricsErrorThreshold, "minErrorRatePct", redMetricsMinErrorRatePct,
				"operationErrorRatePct", redMetricsOperationErrorRatePct,
				"latencyMultiplier", redLatencyMultiplier, "latencyFloorMs", redLatencyFloorMs,
				"latencyMinRequests", redLatencyMinRequests, "latencyMinBaselineWindows", redLatencyMinBaselineWindows)
		}
	}

	setupLog.Info("starting manager", "version", appVersion())
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running triage-agent")
		os.Exit(1)
	}
}

// errNeedsJoin is the startup error for an agent with a core endpoint and no
// join CA fingerprint. The join certificate is the only way to authenticate to
// the core, in a pod and on a host alike.
var errNeedsJoin = errors.New("the agent joins the core by certificate: set --join-ca-fingerprint (core.caFingerprint in the chart) whenever --triage-endpoint is set")

// checkPushCredentials enforces that an agent with a core endpoint carries the
// core CA fingerprint it joins with. Without an endpoint (standalone) nothing
// is sent anywhere, so nothing authenticates.
func checkPushCredentials(endpoint, joinCAFingerprint string) error {
	if endpoint != "" && joinCAFingerprint == "" {
		return errNeedsJoin
	}
	return nil
}

// annotatedNamespaces lists the namespaces carrying a non-empty
// annotationkeys.Tenant value, using a direct client because this runs
// BEFORE the manager (and therefore its cache) exists — the answer is what
// scopes that cache.
//
// It is a point-in-time read on purpose. A namespace annotated later cannot be
// added to an already-scoped controller-runtime cache at runtime, so it is
// picked up by push/dynamic_informers.go instead; this only decides the boot
// set. Namespace objects are cluster-scoped, so nothing here is affected by
// the cache scoping it produces.
func annotatedNamespaces(cfg *rest.Config) ([]string, error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	list, err := cs.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var out []string
	for i := range list.Items {
		if _, ok := push.NamespaceTenant(&list.Items[i]); ok {
			out = append(out, list.Items[i].Name)
		}
	}
	sort.Strings(out)
	return out, nil
}

// currentNamespace returns the namespace this agent is running in: the
// POD_NAMESPACE env var when set (the downward-API convention), otherwise the
// service-account namespace file every in-cluster pod carries. Empty when
// neither is available — running off a kubeconfig outside the cluster — and
// callers must treat that as "unknown", never as a namespace name.
func currentNamespace() string {
	if ns := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); ns != "" {
		return ns
	}
	b, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
