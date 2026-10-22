package redsource

import (
	"net"
	"strings"

	"github.com/triageagent-dev/agent/internal/remotens"
	"github.com/triageagent-dev/agent/push"
)

// obiUnresolvedOutgoing is the vendored OBI's own placeholder
// (attributes.rename_unresolved_hosts_outgoing, default "outgoing" —
// third_party/obi/pkg/obi/config.go) for a client span whose server address
// it could not attribute a hostname OR a resolved k8s name to at all — a
// raw ClusterIP call the embedded k8s name-resolver source (A4) failed to
// rewrite, observed in practice on the vappsim capture this design is built
// from. It carries no IP text to look up, so it always resolves to
// External — the ClusterIP branch below is a SEPARATE, genuine case where
// server_address literally IS an IP:port string.
const obiUnresolvedOutgoing = "outgoing"

// k8sResolver implements Resolver by walking a client span's server_address
// through the cluster's own Service/Ingress objects instead of parsing the
// address text for a k8s-DNS shape — the DNS-shape parse this replaces
// named an edge's destination by the k8s SERVICE name, which the core could
// never join to the SAME workload's virtual-app node
// (docs/todo/service-graph-from-obi.md).
type k8sResolver struct {
	index ClusterIndex
	allow func(string) bool
}

// NewK8sResolver builds a Resolver that names a client span's destination by
// virtual-app identity via index, honoring allow (the collector's namespace
// scope; nil means unrestricted) exactly as the address resolution it
// replaces did.
func NewK8sResolver(index ClusterIndex, allow func(string) bool) Resolver {
	return &k8sResolver{index: index, allow: allow}
}

// NewRemoteResolver builds a Resolver for an agent that watches no
// Kubernetes objects (--kubernetes=false): a remote OTLP sender's client
// spans are named by host name alone (resolveRemote), which needs no
// cluster index, and every other destination is External.
func NewRemoteResolver(allow func(string) bool) Resolver {
	return &k8sResolver{allow: allow}
}

func (r *k8sResolver) allowed(ns string) bool {
	return r.allow == nil || r.allow(ns)
}

// Resolve implements Resolver.
func (r *k8sResolver) Resolve(srcNamespace, serverAddress string) (dst servicePair, external string) {
	if serverAddress == "" {
		return servicePair{}, ""
	}
	if tenant, _, remote := remotens.Split(srcNamespace); remote {
		return r.resolveRemote(tenant, srcNamespace, serverAddress)
	}
	host := hostOnly(serverAddress)
	if host == obiUnresolvedOutgoing || r.index == nil {
		return servicePair{}, push.TruncateTopologyExternal(serverAddress)
	}

	var svcNamespace, svcName string
	if ip := net.ParseIP(host); ip != nil {
		ns, name, ok := r.index.ServiceByClusterIP(host)
		if !ok {
			return servicePair{}, push.TruncateTopologyExternal(serverAddress)
		}
		svcNamespace, svcName = ns, name
	} else if ns, name, ok := r.resolveHostname(srcNamespace, host); ok {
		svcNamespace, svcName = ns, name
	} else {
		return servicePair{}, push.TruncateTopologyExternal(serverAddress)
	}

	if !r.allowed(svcNamespace) {
		return servicePair{}, push.TruncateTopologyExternal(serverAddress)
	}
	selector, ok := r.index.ServiceByName(svcNamespace, svcName)
	if !ok {
		return servicePair{}, push.TruncateTopologyExternal(serverAddress)
	}
	owner, ok := r.index.WorkloadForSelector(svcNamespace, selector)
	if !ok {
		return servicePair{}, push.TruncateTopologyExternal(serverAddress)
	}
	identity, ok := r.index.IdentityFor(svcNamespace, owner)
	if !ok {
		return servicePair{}, push.TruncateTopologyExternal(serverAddress)
	}
	return servicePair{Namespace: svcNamespace, Workload: identity}, ""
}

// resolveRemote resolves a client span of a remote tenant's workload. The
// destination is not in this cluster's index, so the host name alone decides,
// and only the two shapes that are certainly in-cluster names count: a bare
// Service name (the source's namespace), or "<svc>.<ns>.svc[.<domain>]". The
// two-label "<svc>.<ns>" shorthand is NOT accepted here: with no index to
// confirm the Service, "example.com" would be a Service "example" of a
// namespace "com". The destination keeps the source's tenant, and its
// workload is the Service name, which is what an application is called when
// its Service and workload share a name. Anything else (an IP, "localhost", a
// public host) is external.
func (r *k8sResolver) resolveRemote(tenant, srcNamespace, serverAddress string) (servicePair, string) {
	host := hostOnly(serverAddress)
	if host == obiUnresolvedOutgoing || host == "localhost" || net.ParseIP(host) != nil {
		return servicePair{}, push.TruncateTopologyExternal(serverAddress)
	}
	parts := strings.Split(host, ".")
	var svc, ns string
	switch {
	case len(parts) == 1:
		svc, ns = host, srcNamespace
	case len(parts) >= 3 && parts[0] != "" && parts[1] != "" && parts[2] == "svc":
		svc, ns = parts[0], remotens.Qualify(tenant, parts[1])
	default:
		return servicePair{}, push.TruncateTopologyExternal(serverAddress)
	}
	if !r.allowed(ns) {
		return servicePair{}, push.TruncateTopologyExternal(serverAddress)
	}
	return servicePair{Namespace: ns, Workload: svc}, ""
}

// resolveHostname tries, in order: the bare Service name in srcNamespace,
// the "<svc>.<ns>" shorthand, "<svc>.<ns>.svc" with any suffix after ".svc"
// (the cluster domain need not be "cluster.local"), then the Ingress host
// index.
func (r *k8sResolver) resolveHostname(srcNamespace, host string) (namespace, name string, ok bool) {
	if _, ok := r.index.ServiceByName(srcNamespace, host); ok {
		return srcNamespace, host, true
	}
	if parts := strings.Split(host, "."); len(parts) >= 2 {
		svc, ns := parts[0], parts[1]
		if svc != "" && ns != "" && (len(parts) == 2 || parts[2] == "svc") {
			if _, ok := r.index.ServiceByName(ns, svc); ok {
				return ns, svc, true
			}
		}
	}
	return r.index.IngressBackend(host)
}
