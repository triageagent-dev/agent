// Service/Ingress cluster-object index (docs/todo/service-graph-from-obi.md):
// the lookups redsource's k8sResolver needs to turn a topology edge's
// server_address into a k8s Service, and that Service into the workload
// behind it, before the identity index (virtualapps.go) names it. A plain
// cluster-object read, independent of --virtual-apps mode — registered
// unconditionally in Pusher.Start.
package push

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	toolscache "k8s.io/client-go/tools/cache"
)

// serviceEntry is one indexed Service's identity + pod selector.
type serviceEntry struct {
	Namespace string
	Name      string
	ClusterIP string
	Selector  map[string]string
}

func serviceIndexKey(namespace, name string) string { return namespace + "/" + name }

// ingressBackend is the resolved (or ambiguous) Service backing one Ingress
// rule host.
type ingressBackend struct {
	Namespace string
	Name      string
	Ambiguous bool
}

// serviceIndex answers the two lookups a topology-edge server_address
// resolution needs off a k8s Service object — by (namespace, name), for the
// "<svc>"/"<svc>.<ns>"/"<svc>.<ns>.svc..." address shapes, and by ClusterIP,
// for a raw-IP address shape — plus the Ingress host index. Built from
// controller-runtime informers registered once for the Pusher's whole
// lifetime (registerServiceIndexInformers), on the SAME manager cache the
// vapp engine's own workload informers use, so it inherits identical
// namespace scoping for free.
type serviceIndex struct {
	mu          sync.RWMutex
	byName      map[string]serviceEntry
	byClusterIP map[string]serviceEntry

	ingressMu      sync.Mutex                   // guards ingressObjects AND ingressHost together
	ingressObjects map[string]map[string]string // "namespace/name" -> host -> backend Service name (same namespace as the Ingress)
	ingressHost    map[string]ingressBackend    // host -> resolved backend, rebuilt on every Ingress add/update/delete

	ambigMu              sync.Mutex
	warnedAmbiguousHosts map[string]bool

	log logr.Logger
}

func newServiceIndex(log logr.Logger) *serviceIndex {
	return &serviceIndex{
		byName:               make(map[string]serviceEntry),
		byClusterIP:          make(map[string]serviceEntry),
		ingressObjects:       make(map[string]map[string]string),
		ingressHost:          make(map[string]ingressBackend),
		warnedAmbiguousHosts: make(map[string]bool),
		log:                  log,
	}
}

func (s *serviceIndex) upsertService(svc *corev1.Service) {
	clusterIP := svc.Spec.ClusterIP
	entry := serviceEntry{Namespace: svc.Namespace, Name: svc.Name, ClusterIP: clusterIP, Selector: svc.Spec.Selector}
	key := serviceIndexKey(svc.Namespace, svc.Name)
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.byName[key]; ok && old.ClusterIP != "" && old.ClusterIP != clusterIP {
		delete(s.byClusterIP, old.ClusterIP)
	}
	s.byName[key] = entry
	if clusterIP != "" && clusterIP != corev1.ClusterIPNone {
		s.byClusterIP[clusterIP] = entry
	}
}

func (s *serviceIndex) removeService(namespace, name string) {
	key := serviceIndexKey(namespace, name)
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.byName[key]
	if !ok {
		return
	}
	if old.ClusterIP != "" {
		delete(s.byClusterIP, old.ClusterIP)
	}
	delete(s.byName, key)
}

// serviceByName returns namespace/name's own pod selector.
func (s *serviceIndex) serviceByName(namespace, name string) (map[string]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.byName[serviceIndexKey(namespace, name)]
	if !ok {
		return nil, false
	}
	return e.Selector, true
}

// serviceByClusterIP resolves a literal ClusterIP to the Service that owns it.
func (s *serviceIndex) serviceByClusterIP(ip string) (namespace, name string, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.byClusterIP[ip]
	if !ok {
		return "", "", false
	}
	return e.Namespace, e.Name, true
}

// upsertIngress records namespace/name's own host->backend-Service-name
// rules, then fully rebuilds the merged cross-object ingressHost index.
// Ingress objects are low-churn and low-cardinality enough that a full
// rebuild per event is simpler than incremental diffing, matching this
// repo's existing full-regroup-on-change convention (virtualapps.go's own
// recompute).
func (s *serviceIndex) upsertIngress(ing *networkingv1.Ingress) {
	hosts := make(map[string]string)
	for _, rule := range ing.Spec.Rules {
		if rule.Host == "" || rule.HTTP == nil {
			continue
		}
		for _, path := range rule.HTTP.Paths {
			if path.Backend.Service == nil || path.Backend.Service.Name == "" {
				continue
			}
			hosts[rule.Host] = path.Backend.Service.Name
		}
	}
	key := serviceIndexKey(ing.Namespace, ing.Name)
	s.ingressMu.Lock()
	defer s.ingressMu.Unlock()
	if len(hosts) == 0 {
		delete(s.ingressObjects, key)
	} else {
		s.ingressObjects[key] = hosts
	}
	s.rebuildIngressHostLocked()
}

func (s *serviceIndex) removeIngress(namespace, name string) {
	key := serviceIndexKey(namespace, name)
	s.ingressMu.Lock()
	defer s.ingressMu.Unlock()
	delete(s.ingressObjects, key)
	s.rebuildIngressHostLocked()
}

// rebuildIngressHostLocked recomputes the merged host->backend index across
// every tracked Ingress object — a host backed by the SAME (namespace, name)
// Service across every object that declares it resolves; a host backed by
// two DIFFERENT Services is ambiguous (IngressBackend returns false for it,
// logged once). Caller must hold ingressMu.
func (s *serviceIndex) rebuildIngressHostLocked() {
	type candidate struct{ namespace, name string }
	byHost := make(map[string]candidate)
	ambiguous := make(map[string]bool)
	for objKey, hosts := range s.ingressObjects {
		ns, _, ok := strings.Cut(objKey, "/")
		if !ok {
			continue
		}
		for host, svcName := range hosts {
			if c, seen := byHost[host]; seen {
				if c.namespace != ns || c.name != svcName {
					ambiguous[host] = true
				}
				continue
			}
			byHost[host] = candidate{namespace: ns, name: svcName}
		}
	}
	out := make(map[string]ingressBackend, len(byHost))
	for host, c := range byHost {
		out[host] = ingressBackend{Namespace: c.namespace, Name: c.name, Ambiguous: ambiguous[host]}
	}
	s.ingressHost = out
}

// ingressBackend resolves an Ingress rule host to its single backend
// Service. ok is false when no rule matches, or several different Services
// back the same host (ambiguous — logged once per host).
func (s *serviceIndex) ingressBackend(host string) (namespace, name string, ok bool) {
	s.ingressMu.Lock()
	b, found := s.ingressHost[host]
	s.ingressMu.Unlock()
	if !found {
		return "", "", false
	}
	if b.Ambiguous {
		s.warnAmbiguousHostOnce(host)
		return "", "", false
	}
	return b.Namespace, b.Name, true
}

func (s *serviceIndex) warnAmbiguousHostOnce(host string) {
	s.ambigMu.Lock()
	already := s.warnedAmbiguousHosts[host]
	s.warnedAmbiguousHosts[host] = true
	s.ambigMu.Unlock()
	if !already {
		s.log.Info("service index: Ingress host resolves to more than one backend Service; treating as unresolved", "host", host)
	}
}

// registerServiceIndexInformers wires Service/Ingress informer callbacks
// that feed p.svcIndex. Registered once for the Pusher's whole lifetime,
// like registerNamespaceInformer — a topology edge's destination resolution
// must survive a WebSocket reconnect, not reset with it.
func (p *Pusher) registerServiceIndexInformers(ctx context.Context) error {
	svcInf, err := p.cache.GetInformer(ctx, &corev1.Service{})
	if err != nil {
		return fmt.Errorf("get Service informer: %w", err)
	}
	if _, err := svcInf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { p.onService(obj, false) },
		UpdateFunc: func(_, obj any) { p.onService(obj, false) },
		DeleteFunc: func(obj any) { p.onService(obj, true) },
	}); err != nil {
		return fmt.Errorf("watch Service: %w", err)
	}

	ingInf, err := p.cache.GetInformer(ctx, &networkingv1.Ingress{})
	if err != nil {
		return fmt.Errorf("get Ingress informer: %w", err)
	}
	if _, err := ingInf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { p.onIngress(obj, false) },
		UpdateFunc: func(_, obj any) { p.onIngress(obj, false) },
		DeleteFunc: func(obj any) { p.onIngress(obj, true) },
	}); err != nil {
		return fmt.Errorf("watch Ingress: %w", err)
	}

	p.log.Info("registered Service + Ingress informer handlers (topology edge destination resolution)")
	return nil
}

func (p *Pusher) onService(obj any, deleted bool) {
	svc, ok := obj.(*corev1.Service)
	if !ok {
		return
	}
	if deleted {
		p.svcIndex.removeService(svc.Namespace, svc.Name)
		return
	}
	p.svcIndex.upsertService(svc)
}

func (p *Pusher) onIngress(obj any, deleted bool) {
	ing, ok := obj.(*networkingv1.Ingress)
	if !ok {
		return
	}
	if deleted {
		p.svcIndex.removeIngress(ing.Namespace, ing.Name)
		return
	}
	p.svcIndex.upsertIngress(ing)
}
