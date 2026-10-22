package redsource

// Resolver resolves a client span's srcNamespace + server.address into
// either an in-scope k8s workload identity (dst set, external empty) or an
// External label (dst zero, external set). Set on Options.Resolver;
// nil (no resolver wired — every test that doesn't need address resolution,
// and any deployment before main.go wires the real one) treats every
// client-side destination as External: the caller's identity is known, the
// callee's is not. k8sResolver (k8sresolver.go) is the production
// implementation, walking the cluster's own Service/Ingress objects instead
// of parsing the address text for a k8s-DNS shape
// (docs/todo/service-graph-from-obi.md).
type Resolver interface {
	Resolve(srcNamespace, serverAddress string) (dst servicePair, external string)
}

// IdentityResolver maps a known workload's owner name (a Deployment/
// StatefulSet/DaemonSet's own name) to the virtual-app identity it groups
// into — see Profile.SourceOwnerLabel. Set on Options.Identity;
// *push.Pusher satisfies this via IdentityFor. nil (no resolver wired)
// leaves every identity as whatever Profile.DestWorkloadLabel/
// SourceWorkloadLabel already reported.
type IdentityResolver interface {
	IdentityFor(namespace, ownerName string) (identity string, ok bool)
}

// ClusterIndex is the cluster-object lookup surface k8sResolver needs to
// turn a client span's server_address into a virtual-app identity —
// implemented by *push.Pusher (push/serviceindex.go's Service/Ingress
// informers + push/virtualapps.go's identity index, both scoped the same
// way the vapp engine's own cache is). Injected so this package never
// imports client-go/controller-runtime types of its own.
type ClusterIndex interface {
	IdentityResolver
	// ServiceByName returns a Service's own pod selector.
	ServiceByName(namespace, name string) (selector map[string]string, ok bool)
	// ServiceByClusterIP resolves a literal ClusterIP to the Service that
	// owns it (skips None/headless — never indexed by IP).
	ServiceByClusterIP(ip string) (namespace, name string, ok bool)
	// IngressBackend resolves an Ingress rule host to the single Service
	// backing every rule for that host, or ok=false when no rule matches or
	// several DIFFERENT Services back the same host (ambiguous).
	IngressBackend(host string) (namespace, name string, ok bool)
	// WorkloadForSelector returns the workload in namespace whose pod-
	// template labels contain every (key, value) pair in selector.
	WorkloadForSelector(namespace string, selector map[string]string) (ownerName string, ok bool)
}
