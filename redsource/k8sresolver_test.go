package redsource

import (
	"strings"
	"testing"

	"github.com/triageagent-dev/agent/push"
)

// fakeClusterIndex is a minimal, in-memory ClusterIndex for k8sResolver's
// table-driven tests — no controller-runtime cache, no real informers.
type fakeClusterIndex struct {
	// services: "namespace/name" -> selector.
	services map[string]map[string]string
	// clusterIPs: ip -> "namespace/name".
	clusterIPs map[string]string
	// ingress: host -> "namespace/name", or "" for an ambiguous host.
	ingress map[string]string
	// workloads: "namespace/selectorKey" -> owner name.
	workloads map[string]string
	// identities: "namespace/owner" -> identity.
	identities map[string]string
}

func newFakeClusterIndex() *fakeClusterIndex {
	return &fakeClusterIndex{
		services:   map[string]map[string]string{},
		clusterIPs: map[string]string{},
		ingress:    map[string]string{},
		workloads:  map[string]string{},
		identities: map[string]string{},
	}
}

func (f *fakeClusterIndex) ServiceByName(namespace, name string) (map[string]string, bool) {
	sel, ok := f.services[namespace+"/"+name]
	return sel, ok
}

func (f *fakeClusterIndex) ServiceByClusterIP(ip string) (string, string, bool) {
	nsName, ok := f.clusterIPs[ip]
	if !ok {
		return "", "", false
	}
	ns, name, _ := strings.Cut(nsName, "/")
	return ns, name, true
}

func (f *fakeClusterIndex) IngressBackend(host string) (string, string, bool) {
	nsName, ok := f.ingress[host]
	if !ok || nsName == "" {
		return "", "", false
	}
	ns, name, _ := strings.Cut(nsName, "/")
	return ns, name, true
}

func (f *fakeClusterIndex) WorkloadForSelector(namespace string, selector map[string]string) (string, bool) {
	owner, ok := f.workloads[namespace+"/"+selectorKeyForTest(selector)]
	return owner, ok
}

func (f *fakeClusterIndex) IdentityFor(namespace, ownerName string) (string, bool) {
	identity, ok := f.identities[namespace+"/"+ownerName]
	return identity, ok
}

// selectorKeyForTest renders a selector map deterministically — the fake
// index's own bookkeeping key, unrelated to any production selector-hashing
// scheme.
func selectorKeyForTest(selector map[string]string) string {
	// Every fixture below uses single-pair selectors, so a plain join is enough.
	var b strings.Builder
	first := true
	for k, v := range selector {
		if !first {
			b.WriteByte(',')
		}
		first = false
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v)
	}
	return b.String()
}

// wireOrdersApp registers a full Service->workload->identity chain for
// "orders-app-api" in namespace "vappsim" — the shared fixture every
// resolution-form test below resolves through.
func wireOrdersApp(idx *fakeClusterIndex, clusterIP string) {
	idx.services["vappsim/orders-app-api"] = map[string]string{"app": "orders"}
	if clusterIP != "" {
		idx.clusterIPs[clusterIP] = "vappsim/orders-app-api"
	}
	idx.workloads["vappsim/"+selectorKeyForTest(map[string]string{"app": "orders"})] = "orders-app-api"
	idx.identities["vappsim/orders-app-api"] = "orders"
}

func TestK8sResolver_Resolve(t *testing.T) {
	tests := []struct {
		name          string
		serverAddress string
		srcNamespace  string
		setup         func(*fakeClusterIndex)
		allow         func(string) bool
		wantDst       servicePair
		wantExternal  string
	}{
		{
			name:          "bare Service name in the caller's own namespace",
			serverAddress: "orders-app-api",
			srcNamespace:  "vappsim",
			setup:         func(idx *fakeClusterIndex) { wireOrdersApp(idx, "") },
			wantDst:       servicePair{Namespace: "vappsim", Workload: "orders"},
		},
		{
			name:          "svc.ns shorthand",
			serverAddress: "orders-app-api.vappsim",
			srcNamespace:  "billing",
			setup:         func(idx *fakeClusterIndex) { wireOrdersApp(idx, "") },
			wantDst:       servicePair{Namespace: "vappsim", Workload: "orders"},
		},
		{
			name:          "FQDN with .svc.cluster.local and a port",
			serverAddress: "orders-app-api.vappsim.svc.cluster.local:8080",
			srcNamespace:  "billing",
			setup:         func(idx *fakeClusterIndex) { wireOrdersApp(idx, "") },
			wantDst:       servicePair{Namespace: "vappsim", Workload: "orders"},
		},
		{
			name:          "raw ClusterIP",
			serverAddress: "10.96.0.55:80",
			srcNamespace:  "billing",
			setup:         func(idx *fakeClusterIndex) { wireOrdersApp(idx, "10.96.0.55") },
			wantDst:       servicePair{Namespace: "vappsim", Workload: "orders"},
		},
		{
			name:          "ingress host",
			serverAddress: "orders.example.com",
			srcNamespace:  "billing",
			setup: func(idx *fakeClusterIndex) {
				wireOrdersApp(idx, "")
				idx.ingress["orders.example.com"] = "vappsim/orders-app-api"
			},
			wantDst: servicePair{Namespace: "vappsim", Workload: "orders"},
		},
		{
			name:          "unknown host is External",
			serverAddress: "totally-unknown-host.example.com",
			srcNamespace:  "billing",
			setup:         func(idx *fakeClusterIndex) {},
			wantExternal:  "totally-unknown-host.example.com",
		},
		{
			name:          "OBI's own unresolved-IP placeholder is always External",
			serverAddress: "outgoing",
			srcNamespace:  "billing",
			setup:         func(idx *fakeClusterIndex) { wireOrdersApp(idx, "") },
			wantExternal:  "outgoing",
		},
		{
			name:          "ambiguous ingress host is External",
			serverAddress: "shared.example.com",
			srcNamespace:  "billing",
			setup: func(idx *fakeClusterIndex) {
				idx.ingress["shared.example.com"] = "" // fakeClusterIndex's convention for "ambiguous"
			},
			wantExternal: "shared.example.com",
		},
		{
			name:          "resolved Service but no matching workload (selector unknown)",
			serverAddress: "orders-app-api.vappsim",
			srcNamespace:  "billing",
			setup: func(idx *fakeClusterIndex) {
				idx.services["vappsim/orders-app-api"] = map[string]string{"app": "orders"}
				// deliberately no idx.workloads entry
			},
			wantExternal: "orders-app-api.vappsim",
		},
		{
			name:          "resolved workload but not a tracked identity",
			serverAddress: "orders-app-api.vappsim",
			srcNamespace:  "billing",
			setup: func(idx *fakeClusterIndex) {
				idx.services["vappsim/orders-app-api"] = map[string]string{"app": "orders"}
				idx.workloads["vappsim/"+selectorKeyForTest(map[string]string{"app": "orders"})] = "orders-app-api"
				// deliberately no idx.identities entry
			},
			wantExternal: "orders-app-api.vappsim",
		},
		{
			name:          "namespace out of scope is External",
			serverAddress: "orders-app-api.vappsim",
			srcNamespace:  "billing",
			setup:         func(idx *fakeClusterIndex) { wireOrdersApp(idx, "") },
			allow:         func(ns string) bool { return ns != "vappsim" },
			wantExternal:  "orders-app-api.vappsim",
		},
		{
			name:          "empty server_address is unattributable",
			serverAddress: "",
			srcNamespace:  "billing",
			setup:         func(idx *fakeClusterIndex) {},
			wantDst:       servicePair{},
			wantExternal:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := newFakeClusterIndex()
			tt.setup(idx)
			r := NewK8sResolver(idx, tt.allow)
			dst, external := r.Resolve(tt.srcNamespace, tt.serverAddress)
			if dst != tt.wantDst || external != tt.wantExternal {
				t.Errorf("Resolve(%q, %q) = (%+v, %q), want (%+v, %q)",
					tt.srcNamespace, tt.serverAddress, dst, external, tt.wantDst, tt.wantExternal)
			}
		})
	}
}

func TestK8sResolver_TruncatesLongExternal(t *testing.T) {
	long := "a-very-long-external-hostname-" + strings.Repeat("x", push.MaxTopologyExternalLen+50) + ".example.com"
	r := NewK8sResolver(newFakeClusterIndex(), nil)
	_, external := r.Resolve("billing", long)
	if len(external) != push.MaxTopologyExternalLen {
		t.Errorf("external length = %d, want %d (truncated)", len(external), push.MaxTopologyExternalLen)
	}
}

func TestK8sResolver_NilAllowIsUnrestricted(t *testing.T) {
	idx := newFakeClusterIndex()
	wireOrdersApp(idx, "")
	r := NewK8sResolver(idx, nil)
	dst, external := r.Resolve("billing", "orders-app-api.vappsim")
	want := servicePair{Namespace: "vappsim", Workload: "orders"}
	if dst != want || external != "" {
		t.Errorf("Resolve with nil allow = (%+v, %q), want (%+v, \"\")", dst, external, want)
	}
}

// A remote tenant's workloads are not in this cluster's index, so the host
// name alone decides, and the destination keeps the source's tenant.
func TestK8sResolver_RemoteTenant(t *testing.T) {
	r := NewK8sResolver(newFakeClusterIndex(), func(string) bool { return true })
	cases := []struct {
		name, src, addr string
		wantNS, wantWl  string
		wantExternal    bool
	}{
		{"bare service name", "acme~shop", "db:5432", "acme~shop", "db", false},
		{"two labels are not a Service", "acme~shop", "cart.emea", "", "", true},
		{"public two-label host", "acme~shop", "example.com:443", "", "", true},
		{"github.com", "acme~shop", "github.com", "", "", true},
		{"localhost", "acme~shop", "localhost:9000", "", "", true},
		{"svc.ns.svc", "acme~shop", "cart.emea.svc", "acme~emea", "cart", false},
		{"svc.ns.svc.cluster.local", "acme~shop", "cart.emea.svc.cluster.local:8080", "acme~emea", "cart", false},
		{"IP is external", "acme~shop", "10.0.0.7:80", "", "", true},
		{"public host is external", "acme~shop", "api.example.com:443", "", "", true},
		{"unresolved placeholder", "acme~shop", "outgoing", "", "", true},
	}
	for _, c := range cases {
		dst, ext := r.Resolve(c.src, c.addr)
		if c.wantExternal {
			if dst != (servicePair{}) || ext == "" {
				t.Errorf("%s: got dst=%+v external=%q, want external", c.name, dst, ext)
			}
			continue
		}
		if dst.Namespace != c.wantNS || dst.Workload != c.wantWl || ext != "" {
			t.Errorf("%s: got dst=%+v external=%q, want %s/%s", c.name, dst, ext, c.wantNS, c.wantWl)
		}
	}
}

func TestK8sResolver_RemoteTenantRespectsScope(t *testing.T) {
	r := NewK8sResolver(newFakeClusterIndex(), func(ns string) bool { return ns != "acme~secret" })
	if dst, ext := r.Resolve("acme~shop", "vault.secret.svc"); dst != (servicePair{}) || ext == "" {
		t.Fatalf("an out-of-scope destination resolved: dst=%+v external=%q", dst, ext)
	}
}

// TestRemoteResolver_NoClusterIndex: an agent with no Kubernetes watch still
// names a remote sender's in-cluster destinations by host name, and calls
// everything it cannot place external instead of touching a missing index.
func TestRemoteResolver_NoClusterIndex(t *testing.T) {
	r := NewRemoteResolver(nil)
	if dst, ext := r.Resolve("acme~shop", "product-catalog:8080"); dst != (servicePair{Namespace: "acme~shop", Workload: "product-catalog"}) || ext != "" {
		t.Fatalf("bare service name: got %+v %q", dst, ext)
	}
	if dst, ext := r.Resolve("acme~shop", "cart.emea.svc.cluster.local:8080"); dst != (servicePair{Namespace: "acme~emea", Workload: "cart"}) || ext != "" {
		t.Fatalf("svc.ns.svc: got %+v %q", dst, ext)
	}
	if dst, ext := r.Resolve("shop", "cart:8080"); dst != (servicePair{}) || ext == "" {
		t.Fatalf("a local namespace with no index must be external: got %+v %q", dst, ext)
	}
	if dst, ext := r.Resolve("acme~shop", "api.example.com:443"); dst != (servicePair{}) || ext == "" {
		t.Fatalf("public host: got %+v %q", dst, ext)
	}
}
