package push

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/gorilla/websocket"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
	"github.com/triageagent-dev/agent/internal/remotens"
)

// ValidateEndpoint checks the push endpoint scheme. wss:// (encrypted) is always
// accepted. ws:// (plaintext) sends the bearer token in the clear and is accepted
// only when allowPlaintext is true, so plaintext is a deliberate opt-in — never a
// silent default. This is independent of TLS certificate verification, which is a
// separate concern (see Options.InsecureTLS).
func ValidateEndpoint(endpoint string, allowPlaintext bool) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("invalid --triage-endpoint %q: %w", endpoint, err)
	}
	switch u.Scheme {
	case "wss":
		return nil
	case "ws":
		if allowPlaintext || isClusterLocalHost(u.Hostname()) {
			return nil
		}
		return fmt.Errorf("--triage-endpoint uses plaintext ws:// which sends the bearer token in the clear; use wss://, address the core by its in-cluster Service name, or set --core-allow-plaintext")
	default:
		return fmt.Errorf("--triage-endpoint must be a ws:// or wss:// URL, got scheme %q", u.Scheme)
	}
}

// isClusterLocalHost reports whether a hostname can only resolve inside the
// cluster: the explicit kube DNS forms (<svc>.<ns>.svc[.cluster.local]) and
// loopback.
//
// The shorter forms kube DNS also resolves — <svc> and <svc>.<ns> — are NOT
// accepted, because the string alone cannot distinguish them from a host a
// search domain resolves elsewhere, and "example.com" is a two-label public
// domain. The unambiguous form is one word longer to write.
//
// Plaintext to one of these never leaves the pod network, so it needs no
// opt-in — requiring --core-allow-plaintext for an address that is by
// construction internal makes the flag a rite of passage rather than a
// decision, and a flag people set routinely is one they stop reading. The
// flag still exists for the case it is actually about: plaintext to an
// address that DOES leave the cluster.
//
// A public name can never take this branch: ".svc" is not a delegable
// public suffix, and a single-label host has no public meaning either.
func isClusterLocalHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return true
	}
	if host == "" || net.ParseIP(host) != nil {
		return false
	}
	return strings.HasSuffix(host, ".svc") || strings.HasSuffix(host, ".svc.cluster.local")
}

// ParseNamespaces splits a comma-separated namespace list into a clean slice,
// trimming surrounding whitespace and dropping empty entries. So "ops, dev",
// "ops,dev" and "ops, ,dev," all yield ["ops", "dev"]. An empty or all-blank
// input returns nil (meaning "all namespaces").
func ParseNamespaces(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if ns := strings.TrimSpace(part); ns != "" {
			out = append(out, ns)
		}
	}
	return out
}

// Options configures the Pusher.
type Options struct {
	Endpoint string // wss://host/events/ws; empty disables push mode unless Standalone
	// Standalone runs the pusher with no core: it resolves apps, tenants and
	// frames and shows them to the observer (the local TUI and monitoring
	// API), but never dials out. Only meaningful with an empty Endpoint.
	Standalone  bool
	ClusterName string // stamped into every frame (triage source segment)
	// Tenant is the PRIMARY tenant this cluster belongs to; when set, it
	// selects the triage service graph and is declared in the hello frame. Optional — leaving it empty runs CLUSTER MODE
	// (addendum 2026-08-27, docs/todo/virtual-apps.md "Cluster-mode push"):
	// the agent is identified only by ClusterName, the hello frame declares
	// an empty tenant, and every frame's tenant is derived purely from
	// triageagent.dev/tenant namespace annotations (namespace_scope.go) — a
	// namespace with no such mapping has its items dropped, never defaulted
	// onto an invented tenant. See tenant_routing.go for the full routing
	// rules in both modes, and the doc addendum for the core-first rollout
	// constraint cluster-mode adds on top of the existing multi-tenant one.
	Tenant string
	// JoinCAFingerprint is how the agent authenticates to the core (agent
	// join, core docs/todo/agent-join.md): the core CA fingerprint this agent
	// trusts, as the install command and GET /v2/agents on the core give
	// it. Required whenever Endpoint is set.
	JoinCAFingerprint string
	// JoinSecretName, when set, keeps the agent's key, certificate and the
	// saved core CA in that Secret in JoinSecretNamespace, so a new pod keeps
	// its identity. Otherwise they live in JoinStateDir, and a restart that
	// loses the directory needs a new approval.
	JoinSecretName      string
	JoinSecretNamespace string
	JoinStateDir        string
	// JoinTenants are the tenants the join request asks for; empty means
	// Tenant alone. A cluster-mode agent lists every tenant it serves.
	JoinTenants   []string
	Namespaces    []string      // namespaces to watch; empty = all
	AgentVersion  string        // reported in hello
	Heartbeat     time.Duration // heartbeat interval (default 20s)
	InsecureTLS   bool          // skip TLS verify (dev only)
	SnapshotChunk int           // apps per snapshot frame (default 50)
	// LogMetrics, when true, advertises the "log_metrics" hello capability. Set this
	// when a logmetrics.Collector is wired to send frames through this Pusher — it is
	// metadata only (the receiver does not gate on it), so it stays false rather than
	// implicitly true, to avoid claiming a feature that isn't actually active.
	LogMetrics bool
	// RedMetrics, when true, advertises the "red_metrics" hello capability. Set this
	// when a redsource.Collector is wired to send frames through this Pusher: same
	// metadata-only treatment as LogMetrics above (docs/adr/0009-red-metrics-beyla.md).
	RedMetrics bool
	// UseMetrics, when true, advertises the "use_metrics" hello capability. Set this
	// when a usemetrics.Collector is wired to send frames through this Pusher — same
	// metadata-only treatment as LogMetrics/RedMetrics above
	// (docs/adr/0010-use-metrics-node-exporter.md).
	UseMetrics bool
	// Topology, when true, advertises the "topology" hello capability. Set this when
	// a redsource.Collector is wired to send frames through this Pusher — same
	// metadata-only treatment as LogMetrics/RedMetrics/UseMetrics above
	// (docs/adr/0011-topology-snapshot.md).
	Topology bool
	// SensorStream, when true, advertises the "sensor_state" hello capability. Set
	// this when the sensor stream hub (sensorstream.Hub) is serving and something
	// calls SendSensorState on this Pusher — same metadata-only treatment as
	// LogMetrics/RedMetrics/UseMetrics/Topology above (docs/adr/0015-sensor-stream-
	// grpc.md).
	SensorStream bool
	// Host, when true, advertises the "host" hello capability: the agent runs
	// on a plain Linux host, not in a cluster (docs/adr/0018-linux-host-mode.md).
	// Set by NewHost; metadata only, like the capabilities above.
	Host bool
	// NoKubernetes, when true, runs the cluster Pusher with none of its
	// Kubernetes inputs: no Namespace, Service/Ingress, workload or Event
	// informers, no virtual apps, no owner reads (docs/adr/0021-otlp-only-no-
	// kubernetes-watch.md). The agent only forwards what its OTLP receiver and
	// sensors hand it, so it needs no list or watch rights on the cluster.
	// Needs Tenant: with no namespace annotation to read, routing is tenant
	// mode and every item files under it.
	NoKubernetes bool
	// VirtualApps selects label-derived Application synthesis (off|on, default
	// off — see virtualapps.go, docs/todo/virtual-apps.md). Validate with
	// ValidateVirtualAppsMode before passing an operator-supplied value here.
	VirtualApps string
	// CacheNamespaces lists the namespaces the manager's informer cache actually
	// covers — main.go's cacheOpts.DefaultNamespaces keys ({--namespace} ∪
	// --target-namespaces when --namespace is set). Empty means a cluster-wide
	// cache (main.go's --namespace="" path), which covers every namespace by
	// definition. This is a DIFFERENT question from Namespaces above: Namespaces
	// says which namespaces this agent should push at all; CacheNamespaces says
	// which of those the manager cache already knows about, at the informer
	// level, not just the push-scope level.
	//
	// A namespace pulled into scope dynamically by a matching triageagent.dev/tenant
	// Namespace annotation (namespace_scope.go) that is NOT in CacheNamespaces
	// cannot be served by the manager cache at all — controller-runtime cannot
	// add a namespace to an already namespace-scoped cache at runtime, so both
	// listing its existing workloads (virtualAppsState.backfillNamespace) and
	// receiving live Deployment/StatefulSet/DaemonSet events for it would
	// silently never happen. virtualAppsState falls back to a per-namespace
	// runtime informer factory for exactly this case (dynamic_informers.go).
	// Nil/empty is always safe: every namespace is treated as cache-covered,
	// which is the pre-existing, unaffected behavior.
	CacheNamespaces []string
	// Clientset backs the per-namespace runtime informer factories described
	// above — it needs a plain kubernetes.Interface because
	// informers.NewSharedInformerFactoryWithOptions does not accept a
	// controller-runtime cache/client. nil is safe: dynamic_informers.go
	// degrades to a logged error naming the limitation (never a crash) for any
	// namespace that would have needed a runtime factory, e.g. in tests that
	// build Options without a real cluster.
	Clientset kubernetes.Interface
	// DynamicClient backs the per-namespace runtime workload informers
	// (dynamic_informers.go): every configured workload resource is watched
	// generically, as unstructured, through this client rather than the typed
	// Clientset above — same nil-safe degrade contract as Clientset.
	DynamicClient dynamic.Interface
	// WorkloadResources lists the namespaced workload resources virtual-app
	// synthesis discovers generically (docs/todo/virtual-apps.md "Workload
	// resources"), as group/version/resource — e.g. {Group: "apps",
	// Version: "v1", Resource: "deployments"}. Empty means
	// DefaultWorkloadResources. Parse/validate operator input with
	// ParseWorkloadResourceList before setting this.
	WorkloadResources []schema.GroupVersionResource
	// Discovery resolves WorkloadResources against what the API server
	// actually serves, once at startup — a configured resource the server
	// does not serve is skipped rather than producing an error-looping
	// informer (main.go passes the same discovery client that resolves the
	// served Application API version). nil means "resolve nothing": every
	// caller that predates this feature, and any test that does not wire a
	// fake discovery client, gets zero generic workload resources rather
	// than issuing a live discovery call.
	Discovery discovery.DiscoveryInterface
	// now returns the current time; injectable in tests. Defaults to time.Now.
	now func() time.Time
}

// Pusher streams Application inventory + Kubernetes Warning events to a triage agent
// over an outbound WebSocket. It is a manager.Runnable gated behind leader election
// so only one replica streams.
//
// A single Pusher drives exactly ONE outbound connection (connworker.go),
// authenticated as the primary tenant (Options.Tenant) when one is
// configured. Multi-tenant push — any tenant named by an annotated
// namespace's triageagent.dev/tenant value (namespace_scope.go) — rides that same
// connection with a per-frame Tenant envelope stamp (tenant_routing.go),
// never a second socket. When Options.Tenant is empty the Pusher instead
// runs CLUSTER MODE: there is no primary tenant at all, only ones named by
// namespace annotations, and an unannotated namespace's items are dropped
// (noTenantDropped) rather than falling back to an implicit primary — see
// tenant_routing.go's package doc for the full contract.
//
// PER-TENANT WORKERS (tenant_worker.go): every tenant sharing the one
// connection gets its own isolated outbound queue + counters (a
// *tenantWorker), so an event storm on one tenant cannot delay or starve a
// sibling tenant's frames sitting behind it — the pre-refactor design
// funneled every tenant through one shared FIFO channel. The registry of
// active workers (a *tenantMux) persists across reconnects, unlike the
// single WebSocket connection itself: auth-rejected flag and the socket's
// dial/backoff state live on the *connWorker (per-connection); per-tenant
// queues/counters live on the tenantMux (per-Pusher, spans reconnects).
type Pusher struct {
	// remoteSeen maps a remote tenant to the time its data last arrived
	// (remote_tenants.go).
	remoteSeen sync.Map

	// eventCaps holds the daily event caps the core sent (event_caps.go).
	eventCaps eventCaps

	// scope holds the per-tenant namespace scope the core sent
	// (workspace_scope.go).
	scope workspaceScope
	// outOfScopeDropped counts payload items dropped because their
	// namespace is outside their tenant's workspace scope.
	outOfScopeDropped atomic.Uint64

	opts   Options
	cache  cache.Cache
	client client.Client
	log    logr.Logger

	// workerMu guards worker, created lazily by primaryWorker.
	workerMu sync.Mutex
	worker   *connWorker

	// joinOnce loads joinID from Options.JoinStateDir on first use.
	joinOnce sync.Once
	joinID   *joinIdentity
	joinErr  error

	// muxMu guards tenantMux, created lazily by mux() (tenant_worker.go) —
	// same lazy-singleton pattern as worker/workerMu just above. Unlike
	// worker, which represents the one WebSocket connection that comes and
	// goes across reconnects, tenantMux OWNS the per-tenant queues/counters
	// that persist for as long as a tenant stays active, independent of any
	// single connection attempt.
	muxMu     sync.Mutex
	tenantMux *tenantMux

	// skipHandlers disables informer registration (tests that drive frames directly).
	skipHandlers bool

	// host marks a Pusher built by NewHost: it has no manager, cache or
	// client, and Start runs the transport only.
	host bool

	// appSource feeds a host Pusher's virtual apps (SetAppSource); nil when
	// none is attached.
	appSource AppSource

	// vapps drives virtual-app synthesis (virtualapps.go); nil when
	// Options.VirtualApps is off/empty, so every call site is a plain nil check.
	vapps *virtualAppsState

	// svcIndex backs ServiceByName/ServiceByClusterIP/IngressBackend
	// (serviceindex.go, docs/todo/service-graph-from-obi.md) — always
	// constructed by New, independent of Options.VirtualApps (a Service/
	// Ingress lookup is a plain cluster-object read, not virtual-app
	// synthesis). nil only on a bare struct literal (tests that construct
	// *Pusher directly without New).
	svcIndex *serviceIndex

	// nsDefaults backs namespace-level triageagent.dev/* annotation inheritance
	// onto workloads (namespace_defaults.go, docs/todo/virtual-apps.md
	// namespace-inheritance addendum) — always constructed by New. nil only on
	// a bare struct literal (tests that construct *Pusher directly without
	// New), in which case virtualAppsState.workloadsSnapshot's overlay is a
	// no-op.
	nsDefaults *nsDefaultsStore

	// dynamicScope backs namespace-scope-by-annotation (namespace_scope.go,
	// docs/todo/virtual-apps.md namespace-scope-by-annotation addendum) — the
	// live, triageagent.dev/tenant-annotation-derived set of namespaces unioned with
	// opts.Namespaces by inScope. Always constructed by New, independent of
	// Options.VirtualApps: it widens both the event push path (inScope
	// below) and, when virtual-app synthesis is enabled, virtualAppsState's
	// own inScope (shared via the same pointer, wired in New). nil only on a
	// bare struct literal (tests that construct *Pusher directly without
	// New), in which case effectiveInScopeMode falls back to static-list-only
	// behavior — unchanged from before this feature.
	dynamicScope *dynamicScopeSet

	// dynInformers owns the per-namespace runtime informer factories for
	// dynamically-scoped namespaces the manager cache does not cover
	// (dynamic_informers.go). Owned by the PUSHER so a namespace's dynamic
	// scope tracking (namespace_scope.go) never depends on --virtual-apps
	// mode; it only actually starts a workload/event factory for a namespace
	// once virtual-app synthesis is on, since that is its only consumer. When
	// vapps is enabled the SAME manager (same pointer) is shared onto
	// virtualAppsState.dynInformers so the workload informers ride the same
	// per-namespace lifecycle. nil only on a bare struct literal (tests that
	// construct *Pusher directly without New) — every use is nil-guarded.
	dynInformers *dynamicInformerManager

	// noTenantDropped counts payload items dropped because their namespace
	// had no triageagent.dev/tenant mapping and this Pusher has no primary tenant to
	// fall back to (cluster-mode, Options.Tenant == "") — see
	// tenant_routing.go's resolveTenant/recordNoTenantDrop. Always zero in
	// single-tenant mode. Read/written atomically: split-path callers run
	// from informer-handler goroutines, same as connWorker.droppedFrames.
	noTenantDropped atomic.Uint64
	// malformedDropped counts payload entries dropped because the core would
	// have refused their whole frame (wellformed.go).
	malformedDropped atomic.Uint64

	// sensorState holds the latest sensor_state frame (ADR-0015 phase 1),
	// written by SendSensorState and swapped out unstamped on the wire by
	// connworker.go's writeLoop at the next heartbeat tick.
	sensorState atomic.Pointer[Frame]

	// observer, when set, sees every queued frame (observe.go); connected
	// is true while the connection to the core is up.
	observer  atomic.Pointer[observerBox]
	connected atomic.Bool

	// ownerLinks caches the owner reads behind objectExcluded
	// (exclusion.go) for ownerLinkTTL.
	ownerLinks ownerLinkCache
}

// New creates a Pusher. Returns nil if push mode is disabled (empty endpoint).
func New(opts Options, mgr manager.Manager, log logr.Logger) *Pusher {
	if opts.Endpoint == "" && !opts.Standalone {
		return nil
	}
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = 20 * time.Second
	}
	if opts.SnapshotChunk <= 0 {
		opts.SnapshotChunk = 50
	}
	if opts.now == nil {
		opts.now = time.Now
	}
	if opts.VirtualApps == "" || opts.NoKubernetes {
		opts.VirtualApps = VirtualAppsOff
	}
	p := &Pusher{
		opts:         opts,
		cache:        mgr.GetCache(),
		client:       mgr.GetClient(),
		log:          log.WithName("push"),
		nsDefaults:   newNsDefaultsStore(),
		dynamicScope: newDynamicScopeSet(),
		svcIndex:     newServiceIndex(log.WithName("push").WithName("serviceindex")),
	}
	// workloadResources is the discovery-gated list every workload discovery
	// path (manager-cache informers, out-of-cache runtime factories) shares —
	// resolved once, here, so both halves always agree on exactly which
	// resources are watched. Resolved even when opts.WorkloadResources is
	// empty (falls back to DefaultWorkloadResources); left empty when vapps
	// is off, since nothing will consult it.
	var workloadResources []workloadResource
	if opts.VirtualApps != VirtualAppsOff {
		requested := opts.WorkloadResources
		if len(requested) == 0 {
			requested = DefaultWorkloadResources
		}
		workloadResources = resolveWorkloadResources(opts.Discovery, requested, p.log.WithName("virtualapps"))

		p.vapps = newVirtualAppsState(opts.VirtualApps, opts.ClusterName, opts.Namespaces,
			p.log.WithName("virtualapps"), p.enqueue)
		p.vapps.nsDefaultsFn = p.nsDefaults.get
		// Shared, not copied: the SAME dynamic scope set both Pusher.inScope and
		// virtualAppsState.inScope consult (namespace_scope.go), so a namespace
		// entering/leaving scope via its triageagent.dev/tenant annotation is seen
		// identically by both — required for Pusher.dropDynamicScope's
		// !p.inScope(ns) check to agree with what virtualAppsState itself will
		// compute.
		p.vapps.dynamicScope = p.dynamicScope
		// cacheNamespaces backs the scoped-cache fallback dispatch
		// (namespace_scope.go's cacheCovers). Always wired (even when
		// CacheNamespaces is empty, i.e. a cluster-wide cache) — cacheCovers then
		// always reports true and the fallback path is simply never taken, so
		// this is a no-op for every existing cluster-wide deployment.
		p.vapps.cacheNamespaces = opts.CacheNamespaces
		p.vapps.clusterMode = p.clusterMode()
		p.vapps.workloadResources = workloadResources
	}
	// The runtime per-namespace informer manager (dynamic_informers.go) is
	// wired UNCONDITIONALLY so a namespace's dynamic scope tracking
	// (namespace_scope.go) never depends on --virtual-apps mode, though it
	// only starts a factory once vapps is on (nil onWorkload skips it
	// otherwise). The SAME manager instance is shared onto virtualAppsState so
	// backfill/eviction ride one per-namespace lifecycle.
	handlers := dynamicWorkloadHandlers{
		// Events have no manager-cache path once that cache is namespace-scoped
		// (see dynamicWorkloadHandlers.onEvent), so this is what keeps Warning
		// events flowing for a dynamically-scoped namespace. onEvent re-checks
		// inScope itself, exactly as the manager-cache path does.
		onEvent: func(obj any) { p.onEvent(obj) },
	}
	if p.vapps != nil {
		handlers.onWorkload = p.vapps.onWorkload
	}
	p.dynInformers = newDynamicInformerManager(opts.Clientset, opts.DynamicClient, 0,
		p.log.WithName("dynamicinformers"), handlers)
	p.dynInformers.workloadResources = workloadResources
	if p.vapps != nil {
		p.vapps.dynInformers = p.dynInformers
	}
	return p
}

// NewHost creates the Pusher of a Linux host agent (docs/adr/0018-linux-host-
// mode.md): the same connection, join, tenant workers, frame queues, hello,
// heartbeat and sensor_state as New, with none of its Kubernetes inputs: no
// manager, cache or client, no informers, no leader election. The agent is one
// process on one host, so Start runs the connection loop and nothing else.
//
// opts.Namespaces is the static scope, the host's one synthetic namespace
// (callers pass []string{hostNamespace}), and opts.Tenant must be set: a host
// has no namespace annotation to derive a tenant from, so routing is tenant
// mode and every item files under it. Returns nil if opts.Endpoint is empty.
//
// Virtual apps are off until SetAppSource attaches an app source: the snapshot
// is then only snapshot_end.
func NewHost(opts Options, log logr.Logger) *Pusher {
	if opts.Endpoint == "" && !opts.Standalone {
		return nil
	}
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = 20 * time.Second
	}
	if opts.SnapshotChunk <= 0 {
		opts.SnapshotChunk = 50
	}
	if opts.now == nil {
		opts.now = time.Now
	}
	opts.VirtualApps = VirtualAppsOff
	opts.Host = true
	return &Pusher{
		opts:         opts,
		log:          log.WithName("push"),
		host:         true,
		nsDefaults:   newNsDefaultsStore(),
		dynamicScope: newDynamicScopeSet(),
		svcIndex:     newServiceIndex(log.WithName("push").WithName("serviceindex")),
	}
}

// NeedLeaderElection ensures only the leader replica streams.
func (p *Pusher) NeedLeaderElection() bool { return true }

// Start implements manager.Runnable. It registers the Kubernetes informer
// handlers once (startKubernetes; a host Pusher and a NoKubernetes Pusher have
// none), then runs the single
// connection's connect→stream→reconnect loop until the context is cancelled
// (connworker.go).
func (p *Pusher) Start(ctx context.Context) error {
	if !p.host && !p.opts.NoKubernetes {
		stop, ok := p.startKubernetes(ctx)
		if !ok {
			<-ctx.Done()
			return nil
		}
		defer stop()
	} else if p.appSource != nil {
		go p.runAppSource(ctx)
	}

	if p.opts.Endpoint == "" {
		// Standalone: frames are observed before they are queued and the
		// queue drops them while nothing ever connected, so there is no
		// connection to run.
		<-ctx.Done()
		return nil
	}
	// The one connection's reconnect/backoff loop (connWorker.run) blocks
	// here exactly like Start's own loop did before connWorker existed.
	p.primaryWorker().run(ctx)
	return nil
}

// startKubernetes waits for the cache and registers every informer handler the
// cluster Pusher feeds from: Namespace, Service/Ingress, virtual-app workloads
// and Events. stop removes the handlers; ok is false when push must stay
// disabled for this process (the caller then waits for ctx).
func (p *Pusher) startKubernetes(ctx context.Context) (stop func(), ok bool) {
	// skipHandlers also means "no real cache wired" (every existing test that
	// sets it builds a bare *Pusher literal with cache left nil) — the same
	// gate every other cache-dependent step below already uses.
	if !p.skipHandlers && !p.cache.WaitForCacheSync(ctx) {
		p.log.Info("cache sync failed; push disabled")
		return nil, false
	}

	// Namespace informer (namespace_defaults.go) is registered once for the
	// Pusher's whole lifetime, independent of --virtual-apps mode: it feeds
	// nsDefaults regardless, and workloadsSnapshot's overlay is a no-op until
	// virtual-app synthesis is on.
	if !p.skipHandlers {
		if err := p.registerNamespaceInformer(ctx); err != nil {
			p.log.Error(err, "namespace annotation inheritance: failed to register Namespace informer; vapp overlay disabled for this process")
		}
	}

	// Service/Ingress index informers (serviceindex.go,
	// docs/todo/service-graph-from-obi.md) are registered once for the
	// Pusher's whole lifetime, independent of --virtual-apps — a topology
	// edge's server_address resolution needs them regardless of synthesis
	// mode; only the FINAL identity step (WorkloadForSelector/IdentityFor)
	// depends on vapps being on.
	if !p.skipHandlers {
		if err := p.registerServiceIndexInformers(ctx); err != nil {
			p.log.Error(err, "service/ingress index: failed to register informers; topology edge destinations will resolve as External")
		}
	}

	// Virtual-app workload informers are registered once for the Pusher's
	// whole lifetime, spanning every WS reconnect — unlike the
	// per-connection Event informer in registerHandlers, grouping state must
	// not reset just because a socket dropped.
	if p.vapps != nil && !p.skipHandlers {
		if err := p.vapps.registerInformers(ctx, p.cache); err != nil {
			p.log.Error(err, "virtual-apps: failed to register workload informers; disabling for this process")
		} else {
			p.log.Info("virtual-apps enabled", "mode", p.opts.VirtualApps)
		}
	}

	// Event informer handlers are registered ONCE for the Pusher's whole
	// lifetime (previously re-registered per connection inside runOnce) —
	// this handler stamps each produced frame with its namespace's tenant
	// (enqueue/splitFrameByTenant, tenant_routing.go) and enqueues it on the
	// single connection. A disconnected worker's enqueue just drops the frame
	// (connWorker.enqueueOK), same graceful degrade as before; a (re)connect's
	// snapshot pass always re-syncs every tenant's full state regardless of
	// what was missed while down.
	if !p.skipHandlers {
		regs, err := p.registerHandlers(ctx)
		if err != nil {
			p.log.Error(err, "failed to register Event informer handler; push disabled")
			return nil, false
		}
		return func() { p.removeHandlers(regs) }, true
	}
	return func() {}, true
}

// registerHandlers wires the Event informer callback that enqueues frames.
func (p *Pusher) registerHandlers(ctx context.Context) ([]handlerReg, error) {
	evtInf, err := p.cache.GetInformer(ctx, &corev1.Event{})
	if err != nil {
		return nil, err
	}
	evtReg, err := evtInf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { p.onEvent(obj) },
		UpdateFunc: func(old, obj any) { p.onEventUpdate(old, obj) },
	})
	if err != nil {
		return nil, err
	}
	p.log.Info("registered Event informer handler")
	return []handlerReg{{inf: evtInf, reg: evtReg}}, nil
}

type handlerReg struct {
	inf cache.Informer
	reg toolscache.ResourceEventHandlerRegistration
}

func (p *Pusher) removeHandlers(regs []handlerReg) {
	for _, r := range regs {
		_ = r.inf.RemoveEventHandler(r.reg)
	}
}

// registerNamespaceInformer wires a Namespace informer feeding nsDefaults.
// Registered once for the Pusher's whole lifetime (like the virtual-app
// workload informers). Namespace is cluster-scoped, so this is unaffected by
// --target-namespaces at the informer level.
func (p *Pusher) registerNamespaceInformer(ctx context.Context) error {
	inf, err := p.cache.GetInformer(ctx, &corev1.Namespace{})
	if err != nil {
		return fmt.Errorf("get Namespace informer: %w", err)
	}
	if _, err := inf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { p.onNamespace(obj, false) },
		UpdateFunc: func(_, obj any) { p.onNamespace(obj, false) },
		DeleteFunc: func(obj any) { p.onNamespace(obj, true) },
	}); err != nil {
		return fmt.Errorf("watch Namespace: %w", err)
	}
	p.log.Info("registered Namespace informer handler (annotation inheritance)")
	return nil
}

// onNamespace applies a Namespace's triageagent.dev/* defaults into nsDefaults
// and maintains dynamicScope from its triageagent.dev/tenant annotation
// (namespace_scope.go): virtual-app grouping is recomputed via the same
// debounce workload changes already use (or, for a dynamic-scope transition,
// immediately — see addDynamicScope/dropDynamicScope).
//
// triageagent.dev/virtual-app-identity (virtual-app-identity addendum) changes
// which name GroupNameFor resolves for a workload, so the unconditional
// vapps.scheduleRecompute() below is sufficient on its own — the next
// recompute regroups every workload under the new mode and emitDeltas
// (virtualapps.go) naturally deletes the groups that no longer exist and adds
// the ones that now do (a rename is a delete + an add). This function only
// detects the transition to log it once and to warn once on an invalid value.
func (p *Pusher) onNamespace(obj any, deleted bool) {
	ns, ok := obj.(*corev1.Namespace)
	if !ok {
		return
	}
	if deleted {
		p.nsDefaults.remove(ns.Name)
		p.dropDynamicScope(ns.Name)
	} else {
		prev, hadPrev := p.nsDefaults.get(ns.Name)
		// "" is namespaceDefaults.IdentityMode's own zero value for "not yet
		// observed" — the same value parseIdentityMode returns for an absent
		// annotation, so a namespace's first-ever observation with no
		// triageagent.dev/virtual-app-identity annotation compares equal here and logs
		// no spurious transition.
		prevIdentityMode := ""
		if hadPrev {
			prevIdentityMode = prev.IdentityMode
		}

		defaults := namespaceAnnotations(ns.Annotations)
		if defaults.TierInvalid {
			p.log.V(1).Info("annotation unparsable on namespace; treating as absent",
				"key", annotationkeys.Tier, "namespace", ns.Name, "value", ns.Annotations[annotationkeys.Tier])
		}
		if defaults.WeightInvalid {
			p.log.V(1).Info("annotation unparsable on namespace; treating as absent",
				"key", annotationkeys.Weight, "namespace", ns.Name, "value", ns.Annotations[annotationkeys.Weight])
		}
		if defaults.IdentityModeInvalid {
			p.log.V(1).Info("annotation invalid on namespace; treating as workload",
				"key", annotationkeys.VirtualAppIdentity, "namespace", ns.Name, "value", ns.Annotations[annotationkeys.VirtualAppIdentity])
		}

		p.nsDefaults.set(ns.Name, defaults)

		if defaults.IdentityMode != prevIdentityMode {
			// Added, changed, or removed: one INFO line per transition, not
			// per workload. vapps.scheduleRecompute() below is what actually
			// moves every affected workload to its new identity.
			// identityModeDisplay canonicalizes "" (the default) to
			// "workload" so this line never prints an empty from/to value.
			p.log.Info("virtual-app identity mode changed for namespace",
				"namespace", ns.Name, "from", identityModeDisplay(prevIdentityMode), "to", identityModeDisplay(defaults.IdentityMode))
		}

		// Namespace-scope-by-annotation (multi-tenant): ANY non-empty
		// annotationkeys.Tenant value pulls ns into dynamic scope now, under
		// that exact tenant, not just a value matching this agent's own
		// primary tenant. Its absence drops it. Both helpers are
		// transition-gated (no-op when nothing actually changed) and
		// nil-safe (p.dynamicScope is nil only for a bare struct literal).
		if tenant, ok := NamespaceTenant(ns); ok {
			p.addDynamicScope(ns.Name, tenant)
		} else {
			p.dropDynamicScope(ns.Name)
		}
	}

	if p.vapps != nil {
		p.vapps.scheduleRecompute()
	}
}

// onEventUpdate pushes an updated Event only when the object changed. The
// manager cache resyncs every sync-period (120s by default) and a resync
// delivers every cached Event as an update with old == new, so without this
// gate each Warning event in scope was re-pushed every two minutes for the
// hour Kubernetes keeps it: the core opened a concern for a pod nine
// minutes after the pod was gone, and event counts ran several times high.
func (p *Pusher) onEventUpdate(old, obj any) {
	if eventUnchanged(old, obj) {
		return
	}
	p.onEvent(obj)
}

// eventUnchanged reports whether an informer update carries the same Event
// revision it already delivered (a resync, not a new occurrence).
func eventUnchanged(old, cur any) bool {
	o, ok1 := old.(*corev1.Event)
	c, ok2 := cur.(*corev1.Event)
	return ok1 && ok2 && o.ResourceVersion != "" && o.ResourceVersion == c.ResourceVersion
}

func (p *Pusher) onEvent(obj any) {
	e, ok := obj.(*corev1.Event)
	if !ok || e.Type != corev1.EventTypeWarning {
		return
	}
	if !p.inScope(e.Namespace) {
		return
	}
	// A namespace outside its workspace's scope (workspace_scope.go) is
	// skipped here before any lookup; enqueueOK drops it too and is the
	// guarantee.
	if t := p.tenantForNamespace(e.Namespace); t != "" && !p.WorkspaceNamespaceInScope(t, remotens.Strip(t, e.Namespace)) {
		return
	}
	// annotationkeys.Triage "false" on the namespace, the involved object or
	// any owner up its chain: nothing about it reaches the core.
	if p.eventExcluded(e) {
		p.log.V(1).Info("drop k8s event: workload excluded from triage",
			"key", annotationkeys.Triage, "namespace", e.Namespace,
			"kind", e.InvolvedObject.Kind, "object", e.InvolvedObject.Name)
		return
	}
	// Trace (V(1)): one line per pushed Kubernetes Warning event.
	p.log.V(1).Info("push k8s event",
		"namespace", e.Namespace, "reason", e.Reason,
		"kind", e.InvolvedObject.Kind, "object", e.InvolvedObject.Name)
	p.enqueue(newK8sEvent(p.opts.ClusterName, e))
}

// SendLogMetrics enqueues one log_metrics frame per chunk of at most
// MaxLogMetricsServices services, for the logmetrics.Collector to call once per
// collection tick. Each chunk is routed (and, if it carries services from more
// than one tenant's namespaces, further split) onto its own tenant connection(s)
// by enqueue (tenant_routing.go). It is a no-op when services is empty; a
// not-yet-connected target tenant just drops its share (logged at V(1) inside
// connWorker.enqueueOK), since the underlying log-exporter counters are cumulative
// and nothing is lost long-term: the next tick's delta picks up where this one
// left off.
func (p *Pusher) SendLogMetrics(windowStart int64, windowSec int, services []ServiceLogMetrics) {
	if p == nil || len(services) == 0 {
		return
	}
	for _, chunk := range chunkServices(services) {
		p.enqueue(newLogMetrics(p.opts.ClusterName, windowStart, windowSec, chunk))
	}
}

// SendRedMetrics enqueues one red_metrics frame for redsource.Collector to call once
// per collection tick. Unlike SendLogMetrics, this never chunks: the collector already
// caps services at push.MaxRedMetricsServices (keeping the top by error delta) before
// calling this, so one frame is always enough — see docs/adr/0009-red-metrics-beyla.md.
// Routed (and split per tenant, if it spans more than one) by enqueue. It is a
// no-op when services is empty; a not-yet-connected target tenant just drops its
// share, mirroring SendLogMetrics: OBI's counters are cumulative, so nothing is
// lost long-term.
func (p *Pusher) SendRedMetrics(windowStart int64, windowSec int, services []ServiceRedMetrics) {
	if p == nil || len(services) == 0 {
		return
	}
	p.enqueue(newRedMetrics(p.opts.ClusterName, windowStart, windowSec, services))
}

// SendRedSummary enqueues one red_summary frame for redsource.Collector to call
// once per collection tick, alongside (never instead of) SendRedMetrics — this
// carries EVERY service read this window, not just the ones that crossed
// SendRedMetrics' gate (see RedSummaryPayload, docs/adr/0009-red-metrics-beyla.md).
// Same shape as SendRedMetrics: never chunks (the collector already caps services
// at push.MaxRedMetricsServices before calling this, so one frame is always
// enough), routed (and split per tenant) by enqueue, and a no-op when services is
// empty; a not-yet-connected target tenant just drops its share, mirroring
// SendRedMetrics: OBI's counters are cumulative, so nothing is lost long-term.
func (p *Pusher) SendRedSummary(windowStart int64, windowSec int, services []ServiceRedMetrics) {
	if p == nil || len(services) == 0 {
		return
	}
	p.enqueue(newRedSummary(p.opts.ClusterName, windowStart, windowSec, services))
}

// SendUseMetrics enqueues one use_metrics frame for usemetrics.Collector to call once
// per collection tick. Never chunks: the collector already caps services at
// push.MaxUseMetricsServices (keeping the top by memPsiAvg60) before calling this, so
// one frame is always enough — see docs/adr/0010-use-metrics-node-exporter.md. Routed
// (and split per tenant) by enqueue. It is a no-op when services is empty; a
// not-yet-connected target tenant just drops its share, mirroring SendRedMetrics: the
// node-exporter's counters are cumulative, so nothing is lost long-term.
func (p *Pusher) SendUseMetrics(windowStart int64, windowSec int, services []ServiceUseMetrics) {
	if p == nil || len(services) == 0 {
		return
	}
	p.enqueue(newUseMetrics(p.opts.ClusterName, windowStart, windowSec, services))
}

// SendUseSummary enqueues one use_summary frame for usemetrics.Collector to call
// once per collection tick, alongside (never instead of) SendUseMetrics — this
// carries EVERY service read this window, not just the ones that crossed
// SendUseMetrics' gate (see UseSummaryPayload, docs/adr/0010-use-metrics-node-
// exporter.md). Same shape as SendRedSummary: never chunks (the collector
// already caps services at push.MaxUseMetricsServices before calling this, so
// one frame is always enough), routed (and split per tenant) by enqueue, and a
// no-op when services is empty; a not-yet-connected target tenant just drops
// its share, mirroring SendUseMetrics: the node-exporter's counters are
// cumulative, so nothing is lost long-term.
func (p *Pusher) SendUseSummary(windowStart int64, windowSec int, services []ServiceUseSummary) {
	if p == nil || len(services) == 0 {
		return
	}
	p.enqueue(newUseSummary(p.opts.ClusterName, windowStart, windowSec, services))
}

// SendTopologySnapshot enqueues one topology_snapshot frame for redsource.Collector
// to call once per interval (~10m). Unlike SendLogMetrics/SendRedMetrics/
// SendUseMetrics, this does NOT skip an empty (zero nodes, zero edges) snapshot —
// topology_snapshot is FULL, ungated level state (docs/adr/0011-topology-snapshot.md):
// a lost frame heals on the next tick, and an empty result is itself meaningful
// ("nothing observed this window"), not "nothing to report" like a gate that never
// tripped.
//
// Returns whether the frame reached at least one live connection: false when
// every tenant it was split across (splitTopologyByTenant, tenant_routing.go) is
// disconnected, or its own send queue is full (see enqueueOK).
// redsource.Collector.flushTopology is the one caller that inspects this return
// value — unlike RED/USE, which are re-derived from Prometheus counters every
// window regardless of delivery outcome, a topology_snapshot's accumulators are
// the ONLY record of that window, so the caller must not clear them on a failed
// send (AUDIT.md finding, BUG 1).
func (p *Pusher) SendTopologySnapshot(capturedAt int64, windowSec int, nodes []TopologyNode, edges []TopologyEdge) bool {
	if p == nil {
		return false
	}
	return p.enqueueOK(newTopologySnapshot(p.opts.ClusterName, capturedAt, windowSec, nodes, edges))
}

// SendSensorState records the latest sensor_state frame (ADR-0015 phase 1)
// for the cmd/triage-agent sensor-status loop to call every 60s. The frame
// describes the sensors themselves, not any tenant's data, so it is a
// session-level frame like heartbeat: connworker.go's writeLoop writes the
// latest state unstamped on the connection at the next heartbeat tick,
// rather than enqueuing it onto any per-tenant queue. A newer call replaces
// an unsent one. A no-op when sensors is empty — nothing to report yet,
// e.g. before the hub has heard from any sensor.
func (p *Pusher) SendSensorState(at int64, sensors []SensorStatus, otlp []OTLPSourceStatus) {
	if p == nil || (len(sensors) == 0 && len(otlp) == 0) {
		return
	}
	if sensors == nil {
		sensors = []SensorStatus{} // the field is required on the wire
	}
	f := newSensorState(p.opts.ClusterName, at, sensors, p.stampOTLPTenants(otlp))
	p.sensorState.Store(f)
	p.observe("", f)
}

// stampOTLPTenants returns otlp with each source's Tenant set from its
// namespace, so core can show a tenant its own sources only. A source with
// no namespace stays unstamped: it spans the cluster. A source whose
// namespace resolves to no tenant (cluster mode, unannotated) or lies
// outside its tenant's workspace scope is left out, as it is from every data
// frame; it is not counted as a drop, since the list is resent every minute.
func (p *Pusher) stampOTLPTenants(otlp []OTLPSourceStatus) []OTLPSourceStatus {
	if len(otlp) == 0 {
		return otlp
	}
	out := make([]OTLPSourceStatus, 0, len(otlp))
	for _, o := range otlp {
		if o.Namespace != "" {
			o.Tenant = p.tenantForNamespace(o.Namespace)
			// A remote tenant's namespace is qualified inside the agent; the
			// core gets the name the sender used.
			plain := remotens.Strip(o.Tenant, o.Namespace)
			if o.Tenant == "" || !p.WorkspaceNamespaceInScope(o.Tenant, plain) {
				continue
			}
			o.Namespace = plain
		}
		out = append(out, o)
	}
	return out
}

// inScope applies the scope-union rule documented in namespace_scope.go:
// opts.Namespaces (static, operator-configured) unioned with dynamicScope
// (live, triageagent.dev/tenant-annotation-derived). An empty static list means every
// namespace when a primary tenant is configured, and the annotated set alone
// in cluster mode — where an unannotated namespace's items are dropped anyway,
// so watching it produces nothing.
func (p *Pusher) inScope(ns string) bool {
	if _, _, remote := remotens.Split(ns); remote {
		return true // a remote tenant's namespace; its sender was authenticated by certificate
	}
	if p.opts.NoKubernetes && len(p.opts.Namespaces) == 0 {
		// No annotation watcher exists to build a scope from, so the senders
		// are the scope: whatever an authenticated OTLP sender reports files
		// under the primary tenant. --target-namespaces still narrows it.
		return true
	}
	return effectiveInScopeMode(ns, p.opts.Namespaces, p.dynamicScope, p.clusterMode())
}

// clusterMode reports whether this agent has no primary tenant, the condition
// under which resolveTenant drops an unannotated namespace's items rather than
// defaulting them. It is the ONLY thing that lets an empty static list narrow.
func (p *Pusher) clusterMode() bool { return p.opts.Tenant == "" }

// NamespaceInScope reports whether ns is inside this pusher's EFFECTIVE push
// scope — the same scope-union rule inScope applies to the event push path:
// static --target-namespaces unioned with the live triageagent.dev/tenant-annotation-
// derived dynamic set, where an empty static list means every namespace.
// Exported for the metric collectors' Options.NamespaceAllowed hook
// (logmetrics/usemetrics/redsource), so their per-payload namespace filtering
// hot-follows annotation-driven scope changes instead of freezing the static
// list at process start. Nil-safe: a nil *Pusher answers false (push mode
// disabled — nothing is in scope to push).
func (p *Pusher) NamespaceInScope(ns string) bool {
	if p == nil {
		return false
	}
	return p.inScope(ns)
}

// IdentityFor resolves namespace/ownerName (a Deployment/StatefulSet/
// DaemonSet's own name) to the virtual-app identity that workload groups
// into (docs/todo/service-graph-from-obi.md) — part of redsource's
// ClusterIndex/IdentityResolver contract. ok is false when virtual-app
// tracking is off (--virtual-apps=off, p.vapps nil) or the owner is not a
// currently-known workload.
func (p *Pusher) IdentityFor(namespace, ownerName string) (string, bool) {
	if p == nil || p.vapps == nil {
		return "", false
	}
	return p.vapps.identityFor(namespace, ownerName)
}

// IdentityModeFor returns namespace's virtual-app identity mode
// (IdentityModeLabels/IdentityModeWorkload, virtualapps.go's
// triageagent.dev/virtual-app-identity) — the same value GroupNameFor resolves a
// workload's identity through for vapp synthesis, exported so a caller
// outside this package resolving a workload's identity independently (the
// USE-metrics collector, usemetrics/identity.go) agrees with it. nil-safe
// and namespace-not-yet-observed-safe: both return "", which GroupNameFor
// resolves identically to IdentityModeWorkload, the default.
func (p *Pusher) IdentityModeFor(namespace string) string {
	if p == nil || p.nsDefaults == nil {
		return ""
	}
	d, _ := p.nsDefaults.get(namespace)
	return d.IdentityMode
}

// WorkloadForSelector returns the workload in namespace whose pod-template
// labels satisfy every (key, value) pair in selector — the Service-to-
// workload join redsource's k8sResolver needs after resolving a
// server_address to a Service (docs/todo/service-graph-from-obi.md). ok is
// false when virtual-app tracking is off or no known workload matches.
func (p *Pusher) WorkloadForSelector(namespace string, selector map[string]string) (string, bool) {
	if p == nil || p.vapps == nil {
		return "", false
	}
	return p.vapps.workloadForSelector(namespace, selector)
}

// ServiceByName returns namespace/name's own pod selector — part of
// redsource's ClusterIndex contract (docs/todo/service-graph-from-obi.md).
// Backed by the Service informer registerServiceIndexInformers registers,
// independent of --virtual-apps mode (a Service lookup is a plain
// cluster-object read, not virtual-app synthesis). ok is false when the
// Service is unknown or the index was never registered (p.svcIndex nil —
// every pre-existing test that builds a bare *Pusher literal).
func (p *Pusher) ServiceByName(namespace, name string) (map[string]string, bool) {
	if p == nil || p.svcIndex == nil {
		return nil, false
	}
	return p.svcIndex.serviceByName(namespace, name)
}

// ServiceByClusterIP resolves a literal ClusterIP to the Service that owns
// it — part of redsource's ClusterIndex contract. ok is false when no
// indexed Service carries ip (including a headless Service, never indexed
// by IP) or the index was never registered.
func (p *Pusher) ServiceByClusterIP(ip string) (namespace, name string, ok bool) {
	if p == nil || p.svcIndex == nil {
		return "", "", false
	}
	return p.svcIndex.serviceByClusterIP(ip)
}

// IngressBackend resolves an Ingress rule host to the single Service backing
// every rule for that host — part of redsource's ClusterIndex contract. ok
// is false when no rule matches, several DIFFERENT Services back the same
// host (ambiguous — logged once, see serviceIndex.warnAmbiguousHostOnce), or
// the index was never registered.
func (p *Pusher) IngressBackend(host string) (namespace, name string, ok bool) {
	if p == nil || p.svcIndex == nil {
		return "", "", false
	}
	return p.svcIndex.ingressBackend(host)
}

func (p *Pusher) writeFrame(conn *websocket.Conn, f *Frame) error {
	data, err := encode(f)
	if err != nil {
		return err
	}
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return conn.WriteMessage(websocket.TextMessage, data)
}

// writePing sends a WebSocket ping control frame. Intermediaries (LBs, proxies)
// treat ping/pong as connection liveness and reset their idle timer on it, so
// this is what actually keeps a long-lived stream from being reaped.
func (p *Pusher) writePing(conn *websocket.Conn) error {
	return conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second))
}

// joinIdentity loads the agent join identity once.
func (p *Pusher) joinIdentity() (*joinIdentity, error) {
	p.joinOnce.Do(func() {
		var store joinStore
		if p.opts.JoinSecretName != "" {
			if p.opts.Clientset == nil {
				p.joinErr = errors.New("join Secret storage needs a Kubernetes clientset")
				return
			}
			store = &secretJoinStore{cs: p.opts.Clientset, namespace: p.opts.JoinSecretNamespace, name: p.opts.JoinSecretName}
		} else {
			ds, err := newDirJoinStore(p.opts.JoinStateDir)
			if err != nil {
				p.joinErr = err
				return
			}
			store = ds
		}
		p.joinID, p.joinErr = loadJoinIdentity(store, p.opts.JoinCAFingerprint)
	})
	return p.joinID, p.joinErr
}

// joinTenants are the tenants a join request asks for.
func (p *Pusher) joinTenants() []string {
	if len(p.opts.JoinTenants) > 0 {
		return p.opts.JoinTenants
	}
	if p.opts.Tenant != "" {
		return []string{p.opts.Tenant}
	}
	return nil
}
