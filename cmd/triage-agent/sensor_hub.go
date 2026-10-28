// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/credentials"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/triageagent-dev/agent/internal/mesh"
	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/otlpreceiver"
	"github.com/triageagent-dev/agent/push"
	"github.com/triageagent-dev/agent/sensorstream"
)

type sensorHubOptions struct {
	listen, tlsSecret, caConfigMap, service, serviceAccount string
	stale                                                   time.Duration
	// extraNames are host names the certificate covers beyond the Service's,
	// for a controller run outside the cluster.
	extraNames              []string
	meshListen, meshService string
	// otlpListen serves the OTLP metrics receiver on the same certificate;
	// otlpAllowed lists the ServiceAccounts allowed to send to it.
	otlpListen  string
	otlpAllowed []string
	// otlpForwarders are allowed accounts that may publish for any namespace.
	otlpForwarders []string
	// otlpTenants are the tenants a client certificate may name: the tenants
	// this agent joined the core with. A certificate naming another tenant is
	// refused, since the core would drop the frames of a tenant outside the
	// approved set.
	otlpTenants []string
	// The receiver's limits (otlpreceiver.Options); zero takes its default.
	otlpMaxResources, otlpMaxPoints, otlpMaxPerSender, otlpMaxRequest, otlpMaxSeries int
	otlpClientCA                                                                     string
	// otlpClientCASecret names the Secret that holds the agent's own client CA.
	otlpClientCASecret string
	// otlpNamespaceInScope says whether a plain namespace is inside a
	// tenant's workspace scope (push.Pusher.WorkspaceNamespaceInScope); nil
	// allows every namespace.
	otlpNamespaceInScope func(tenant, ns string) bool
	// otlpSensorRole accepts the otlp role (ADR-0022): a peer that presents
	// the controller's serving certificate. Its streams carry OTLP sources,
	// and the hub checks them against otlpTenants and otlpNamespaceInScope.
	otlpSensorRole bool
}

// startSensorHub serves the sensor stream on every replica and returns the
// hub the collectors read. Nil when o.listen is empty; any other setup
// failure exits, since the collectors that asked for it would read nothing.
// With o.meshListen set it also runs the replica gossip (ADR-0015 phase 4)
// over the same certificate, and with o.otlpListen set the OTLP receiver.
func startSensorHub(mgr manager.Manager, o sensorHubOptions) (*sensorstream.Hub, *mesh.Mesh) {
	if o.listen == "" {
		if o.meshListen != "" {
			setupLog.Error(nil, "--mesh-listen gossips the sensor stream: set --sensor-listen")
			os.Exit(1)
		}
		if o.otlpListen != "" {
			setupLog.Error(nil, "--otlp-listen feeds the sensor stream hub: set --sensor-listen")
			os.Exit(1)
		}
		return nil, nil
	}
	ns := podNamespace()
	if ns == "" || o.tlsSecret == "" || o.caConfigMap == "" || o.service == "" || o.serviceAccount == "" {
		setupLog.Error(nil, "--sensor-listen needs the pod namespace and --sensor-tls-secret, --sensor-ca-configmap, --sensor-service and --sensor-service-account")
		os.Exit(1)
	}
	client, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		setupLog.Error(err, "sensor stream: build clientset")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cert, err := sensorstream.EnsureServingCert(ctx, sensorstream.ServingCertOptions{
		Client: client, Namespace: ns, Secret: o.tlsSecret, ConfigMap: o.caConfigMap,
		DNSNames: append([]string{
			o.service,
			fmt.Sprintf("%s.%s", o.service, ns),
			fmt.Sprintf("%s.%s.svc", o.service, ns),
			fmt.Sprintf("%s.%s.svc.cluster.local", o.service, ns),
		}, o.extraNames...),
	})
	if err != nil {
		setupLog.Error(err, "sensor stream: serving certificate")
		os.Exit(1)
	}
	// The mesh is built after the hub and reads from it; the hub publishes
	// through m, which is set before the manager starts either.
	var m *mesh.Mesh
	opts := sensorstream.HubOptions{
		Addr:  o.listen,
		TLS:   credentials.NewTLS(sensorstream.ServerTLS(cert)),
		Auth:  sensorstream.TokenReviewAuth{Client: client, Namespace: ns, ServiceAccount: o.serviceAccount},
		Stale: o.stale,
		Log:   ctrl.Log,
	}
	if o.otlpSensorRole {
		opts.PinnedPeer = cert.Certificate[0]
		// The pods of the otlp role hold neither the joined tenants nor the
		// scope the core sets; this is where they are enforced.
		opts.SourceFilter = otlpreceiver.ScopeFilter(func(tenant string) bool {
			return slices.Contains(o.otlpTenants, tenant)
		}, o.otlpNamespaceInScope)
	}
	if o.meshListen != "" {
		opts.Gossip = func(rec mesh.NodeSnapshot) { m.Publish(rec) }
	}
	hub := sensorstream.NewHub(opts)
	if err := mgr.Add(hub); err != nil {
		setupLog.Error(err, "sensor stream: add runnable")
		os.Exit(1)
	}
	setupLog.Info("sensor stream enabled", "listen", o.listen, "service", o.service, "serviceAccount", o.serviceAccount)
	if o.meshListen != "" {
		m = newMesh(cert, ns, o, hub)
		if err := mgr.Add(m); err != nil {
			setupLog.Error(err, "replica gossip: add runnable")
			os.Exit(1)
		}
		setupLog.Info("replica gossip enabled", "listen", o.meshListen, "service", o.meshService)
	}
	if o.otlpListen != "" {
		startOTLPReceiver(mgr, client, cert, o, hub)
	} else if o.otlpSensorRole && o.otlpClientCASecret != "" {
		// The receiver runs in the otlp role's pods (ADR-0022), which only
		// read the CA; the controller still creates and renews it, since it
		// alone may write the Secret and `otlp-cert` reads the result.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, what, err := otlpreceiver.EnsureClientCA(ctx, client, ns, o.otlpClientCASecret)
		cancel()
		if err != nil {
			setupLog.Error(err, "otlp role: the agent's client CA")
			os.Exit(1)
		}
		setupLog.Info("otlp role: the agent's own client CA", "secret", o.otlpClientCASecret, "ca", what)
	}
	if o.otlpSensorRole {
		setupLog.Info("otlp role enabled: a peer presenting the serving certificate may stream OTLP sources", "tenants", o.otlpTenants)
	}
	return hub, m
}

// startOTLPReceiver serves OTLP metrics on the sensor stream's certificate
// and hands them to hub (docs/adr/0017-otlp-receiver.md). Every replica
// receives; the hub gossips what each one took, so the leader's collectors
// see all of it. It exits on a setup error.
func startOTLPReceiver(mgr manager.Manager, client kubernetes.Interface, cert tls.Certificate, o sensorHubOptions, hub *sensorstream.Hub) {
	if len(o.otlpAllowed) == 0 && o.otlpClientCA == "" && o.otlpClientCASecret == "" {
		setupLog.Error(nil, "--otlp-listen needs --otlp-allowed-service-accounts, --otlp-client-ca-file or --otlp-client-ca-secret: nobody could send")
		os.Exit(1)
	}
	var pool *x509.CertPool
	if o.otlpClientCA != "" || o.otlpClientCASecret != "" {
		pool = x509.NewCertPool()
		if o.otlpClientCA != "" {
			pem, err := os.ReadFile(o.otlpClientCA)
			if err != nil {
				setupLog.Error(err, "otlp receiver: read the client CA")
				os.Exit(1)
			}
			if !pool.AppendCertsFromPEM(pem) {
				setupLog.Error(nil, "otlp receiver: no certificate in the client CA file", "path", o.otlpClientCA)
				os.Exit(1)
			}
		}
		if o.otlpClientCASecret != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			caPEM, what, err := otlpreceiver.EnsureClientCA(ctx, client, podNamespace(), o.otlpClientCASecret)
			cancel()
			if err != nil {
				setupLog.Error(err, "otlp receiver: the agent's client CA")
				os.Exit(1)
			}
			pool.AppendCertsFromPEM(caPEM)
			setupLog.Info("otlp receiver: trusting the agent's own client CA", "secret", o.otlpClientCASecret, "ca", what)
			if what != otlpreceiver.CAExisting {
				setupLog.Info("otlp receiver: the agent's client CA was "+what+"; certificates issued before a renewal stay trusted until the old CA expires", "secret", o.otlpClientCASecret)
			}
		}
		if len(o.otlpAllowed) == 0 {
			setupLog.Info("otlp receiver: a client certificate is required (no token allow-list is set)")
		} else {
			setupLog.Info("otlp receiver: a client certificate is optional (a sender without one needs an allowed token); a given one must chain to the client CA")
		}
		if len(o.otlpTenants) == 0 {
			setupLog.Info("otlp receiver: no join tenants, so every client certificate that names a tenant (spiffe://<domain>/tenant/<name>) is refused")
		} else {
			setupLog.Info("otlp receiver: a client certificate with tenant URI SANs (spiffe://<domain>/tenant/<name>) publishes for those tenants, limited to the join tenants", "tenants", o.otlpTenants)
		}
		setupLog.Info("otlp receiver: a client certificate without a SPIFFE URI SAN (spiffe://<domain>/ns/<namespace>/sa/<name>) is a forwarder credential: its holder may publish for any namespace")
	}
	tc := otlpreceiver.ServerTLS(cert, pool, len(o.otlpAllowed) > 0)
	r := otlpreceiver.New(otlpreceiver.Options{
		Addr: o.otlpListen,
		TLS:  credentials.NewTLS(tc),
		Auth: &otlpreceiver.TokenReviewAuth{Client: client, Allowed: o.otlpAllowed, Forwarders: o.otlpForwarders},
		Sink: hub.Ingest,
		Log:  ctrl.Log,
		TenantAllowed: func(tenant string) bool {
			return slices.Contains(o.otlpTenants, tenant)
		},
		NamespaceInScope: o.otlpNamespaceInScope,
		Wanted:           otlpreceiver.ReadByCollectors,

		MaxResources: o.otlpMaxResources, MaxPointsPerResource: o.otlpMaxPoints,
		MaxResourcesPerSender: o.otlpMaxPerSender, MaxRequestBytes: o.otlpMaxRequest, MaxSeries: o.otlpMaxSeries,
	})
	if err := mgr.Add(r); err != nil {
		setupLog.Error(err, "otlp receiver: add runnable")
		os.Exit(1)
	}
	setupLog.Info("otlp receiver enabled", "listen", o.otlpListen, "allowed", o.otlpAllowed, "audience", otlpreceiver.TokenAudience, "mtls", pool != nil)
}

// newMesh builds the replica gossip on the sensor stream's certificate:
// every replica holds the same one from the -sensor-tls Secret and pins it,
// as the sensors do. It exits on a setup error.
func newMesh(cert tls.Certificate, ns string, o sensorHubOptions, hub *sensorstream.Hub) *mesh.Mesh {
	if o.meshService == "" {
		setupLog.Error(nil, "--mesh-listen needs --mesh-service")
		os.Exit(1)
	}
	_, portStr, err := net.SplitHostPort(o.meshListen)
	port, perr := strconv.Atoi(portStr)
	if err != nil || perr != nil {
		setupLog.Error(err, "--mesh-listen must be [host]:port", "value", o.meshListen)
		os.Exit(1)
	}
	self, err := os.Hostname()
	if err != nil {
		setupLog.Error(err, "replica gossip: this pod's name")
		os.Exit(1)
	}
	mt, err := mesh.NewTLS(cert, o.service)
	if err != nil {
		setupLog.Error(err, "replica gossip: TLS")
		os.Exit(1)
	}
	return mesh.New(mesh.Config{
		Self:       self,
		ListenAddr: o.meshListen,
		ServiceDNS: fmt.Sprintf("%s.%s.svc", o.meshService, ns),
		PeerPort:   port,
		TLS:        mt,
		Apply:      hub.Merge,
		Replay:     hub.Local,
		Log:        ctrl.Log,
	})
}

// sensorStatusInterval is how often the sensor-status loop reports every
// sensor's own health to the core (ADR-0015 phase 1).
const sensorStatusInterval = 60 * time.Second

// sensorStatusLoop reads hub.Nodes() on a tick and reports it to the core
// through pusher.SendSensorState. NeedLeaderElection matches the other
// collectors reading from hub (logmetrics/usemetrics/redsource): a
// non-leader replica's Pusher never connects, so calling SendSensorState
// there would only be dropped, never delivered.
type sensorStatusLoop struct {
	hub    *sensorstream.Hub
	pusher *push.Pusher
}

// Start implements manager.Runnable.
func (l *sensorStatusLoop) Start(ctx context.Context) error {
	ticker := time.NewTicker(sensorStatusInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			l.pusher.SendSensorState(time.Now().Unix(), sensorStatusesFromNodes(l.hub.Nodes()), otlpStatusesFromHub(l.hub.OTLPSources()))
		}
	}
}

// NeedLeaderElection ensures only the leader replica reads + sends.
func (l *sensorStatusLoop) NeedLeaderElection() bool { return true }

// startSensorStatusLoop adds the sensor-status loop when both the sensor
// stream and push are enabled. hub is nil when --sensor-listen is empty;
// pusher is nil when push mode is disabled. Either nil means there is
// nothing to read or nowhere to send it, so it is a no-op rather than an
// error: unlike the log/use/red-metrics collectors, reporting sensor health
// is not itself something an operator explicitly turns on.
func startSensorStatusLoop(mgr manager.Manager, hub *sensorstream.Hub, pusher *push.Pusher) {
	if hub == nil || pusher == nil {
		return
	}
	if err := mgr.Add(&sensorStatusLoop{hub: hub, pusher: pusher}); err != nil {
		setupLog.Error(err, "unable to add sensor-status loop runnable")
		os.Exit(1)
	}
	setupLog.Info("sensor-status loop enabled", "interval", sensorStatusInterval)
}

// sensorStatusesFromNodes maps the sensor hub's own view of every sensor
// (sensorstream.Hub.Nodes()) onto the wire's []push.SensorStatus. This lives
// here, not in push, because push must not import sensorpb
// (docs/adr/0015-sensor-stream-grpc.md). A role named in the sensor's Hello
// with no SensorState report yet (just past the handshake) gets an entry
// with only Role set.
func sensorStatusesFromNodes(nodes []sensorstream.NodeStatus) []push.SensorStatus {
	out := make([]push.SensorStatus, 0, len(nodes))
	for _, n := range nodes {
		s := push.SensorStatus{
			Node:      n.Node,
			Pod:       n.Pod,
			Version:   n.Version,
			Connected: n.Connected,
			LastSeen:  n.LastSeen.Unix(),
			Gaps:      n.Gaps,
		}
		roleState := make(map[string]push.SensorRoleStatus, len(n.Roles))
		if n.State != nil {
			s.Goroutines = n.State.GetGoroutines()
			s.HeapBytes = n.State.GetHeapBytes()
			s.SysBytes = n.State.GetSysBytes()
			s.CPUSeconds = n.State.GetCpuSeconds()
			s.Reconnects = n.State.GetReconnects()
			s.SendErrors = n.State.GetSendErrors()
			s.SendMicros = n.State.GetSendMicros()
			s.AckRTTMicros = n.State.GetAckRttMicros()
			s.TickBytes = n.State.GetTickBytes()
			s.ConnectMicros = n.State.GetConnectMicros()
			for _, r := range n.State.GetRoles() {
				roleState[r.GetRole()] = push.SensorRoleStatus{
					Role:         r.GetRole(),
					GatherMicros: r.GetGatherMicros(),
					Metrics:      r.GetMetrics(),
					DataPoints:   r.GetDataPoints(),
					GatherErrors: r.GetGatherErrors(),
					LastError:    r.GetLastError(),
				}
			}
		}
		for _, role := range n.Roles {
			if rs, ok := roleState[role]; ok {
				s.Roles = append(s.Roles, rs)
			} else {
				s.Roles = append(s.Roles, push.SensorRoleStatus{Role: role})
			}
		}
		out = append(out, s)
	}
	return out
}

// mustParseOTLPAllowed parses --otlp-allowed-service-accounts or exits.
func mustParseOTLPAllowed(v string) []string {
	out, err := otlpreceiver.ParseAllowed(v)
	if err != nil {
		setupLog.Error(err, "--otlp-allowed-service-accounts")
		os.Exit(1)
	}
	return out
}

// otlpFamilies names each sensor role by the collector family it feeds.
var otlpFamilies = map[string]string{
	metricset.RoleEBPF:         "red",
	metricset.RoleNodeExporter: "use",
	metricset.RoleLogExporter:  "log",
}

// otlpStatusesFromHub maps the hub's OTLP sources (ADR-0017) onto the
// sensor_state frame's otlp list.
func otlpStatusesFromHub(srcs []sensorstream.OTLPSource) []push.OTLPSourceStatus {
	if len(srcs) == 0 {
		return nil
	}
	out := make([]push.OTLPSourceStatus, 0, len(srcs))
	for _, s := range srcs {
		name := s.Service
		if name == "" {
			name = s.Workload // a k8s_cluster workload resource has no service.name
		}
		st := push.OTLPSourceStatus{Key: s.Key, Service: name, Namespace: s.Namespace, Instance: s.Instance, LastSeen: s.LastSeen.Unix()}
		for _, f := range s.Families {
			fam := otlpFamilies[f.Role]
			if fam == "" {
				fam = f.Role
			}
			st.Families = append(st.Families, push.OTLPFamilyStatus{
				Family: fam, Metrics: uint32(f.Metrics), DataPoints: uint32(f.Points), Pods: uint32(f.Pods),
			})
		}
		out = append(out, st)
	}
	return out
}

// splitNonEmpty splits a comma-separated flag value, dropping blanks.
func splitNonEmpty(v string) []string {
	var out []string
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// joinedTenants is the set of tenants this agent joins the core with: the
// --join-tenant values, plus --target-tenant.
func joinedTenants(join []string, target string) []string {
	out := slices.Clone(join)
	if target != "" && !slices.Contains(out, target) {
		out = append(out, target)
	}
	return out
}
