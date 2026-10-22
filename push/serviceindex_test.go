package push

import (
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func svc(namespace, name, clusterIP string, selector map[string]string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       corev1.ServiceSpec{ClusterIP: clusterIP, Selector: selector},
	}
}

func TestServiceIndex_ByNameAndClusterIP(t *testing.T) {
	idx := newServiceIndex(logr.Discard())
	idx.upsertService(svc("vappsim", "orders-app-api", "10.96.0.10", map[string]string{"app": "orders"}))

	sel, ok := idx.serviceByName("vappsim", "orders-app-api")
	if !ok || sel["app"] != "orders" {
		t.Fatalf("serviceByName = (%v, %v), want app=orders", sel, ok)
	}
	ns, name, ok := idx.serviceByClusterIP("10.96.0.10")
	if !ok || ns != "vappsim" || name != "orders-app-api" {
		t.Fatalf("serviceByClusterIP = (%q, %q, %v), want (vappsim, orders-app-api, true)", ns, name, ok)
	}
}

func TestServiceIndex_HeadlessServiceNotIndexedByIP(t *testing.T) {
	idx := newServiceIndex(logr.Discard())
	idx.upsertService(svc("vappsim", "headless", corev1.ClusterIPNone, map[string]string{"app": "headless"}))
	if _, _, ok := idx.serviceByClusterIP(""); ok {
		t.Error("a headless Service (ClusterIP: None) must never be indexed by IP")
	}
	if _, _, ok := idx.serviceByClusterIP(string(corev1.ClusterIPNone)); ok {
		t.Error("the literal \"None\" must never resolve as a ClusterIP")
	}
}

func TestServiceIndex_RemoveServiceClearsBothIndexes(t *testing.T) {
	idx := newServiceIndex(logr.Discard())
	idx.upsertService(svc("vappsim", "orders-app-api", "10.96.0.10", map[string]string{"app": "orders"}))
	idx.removeService("vappsim", "orders-app-api")

	if _, ok := idx.serviceByName("vappsim", "orders-app-api"); ok {
		t.Error("serviceByName should report ok=false after removal")
	}
	if _, _, ok := idx.serviceByClusterIP("10.96.0.10"); ok {
		t.Error("serviceByClusterIP should report ok=false after removal")
	}
}

func TestServiceIndex_UpsertOverwritesPreviousClusterIP(t *testing.T) {
	idx := newServiceIndex(logr.Discard())
	idx.upsertService(svc("vappsim", "orders-app-api", "10.96.0.10", nil))
	idx.upsertService(svc("vappsim", "orders-app-api", "10.96.0.20", nil))
	if _, _, ok := idx.serviceByClusterIP("10.96.0.10"); ok {
		t.Error("the STALE ClusterIP must not resolve after an update changes it")
	}
	if _, _, ok := idx.serviceByClusterIP("10.96.0.20"); !ok {
		t.Error("the CURRENT ClusterIP must resolve after an update")
	}
}

func ingress(namespace, name, host, backendService string) *networkingv1.Ingress {
	pathType := networkingv1.PathTypePrefix
	return &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: networkingv1.IngressSpec{
			Rules: []networkingv1.IngressRule{{
				Host: host,
				IngressRuleValue: networkingv1.IngressRuleValue{
					HTTP: &networkingv1.HTTPIngressRuleValue{
						Paths: []networkingv1.HTTPIngressPath{{
							PathType: &pathType,
							Backend: networkingv1.IngressBackend{
								Service: &networkingv1.IngressServiceBackend{Name: backendService},
							},
						}},
					},
				},
			}},
		},
	}
}

func TestServiceIndex_IngressBackend_SingleObject(t *testing.T) {
	idx := newServiceIndex(logr.Discard())
	idx.upsertIngress(ingress("vappsim", "orders-ingress", "orders.example.com", "orders-app-api"))

	ns, name, ok := idx.ingressBackend("orders.example.com")
	if !ok || ns != "vappsim" || name != "orders-app-api" {
		t.Fatalf("ingressBackend = (%q, %q, %v), want (vappsim, orders-app-api, true)", ns, name, ok)
	}
}

func TestServiceIndex_IngressBackend_AgreeingObjectsResolve(t *testing.T) {
	idx := newServiceIndex(logr.Discard())
	idx.upsertIngress(ingress("vappsim", "ingress-a", "shared.example.com", "orders-app-api"))
	idx.upsertIngress(ingress("vappsim", "ingress-b", "shared.example.com", "orders-app-api"))

	_, _, ok := idx.ingressBackend("shared.example.com")
	if !ok {
		t.Error("two Ingress objects agreeing on the same backend for one host must still resolve")
	}
}

func TestServiceIndex_IngressBackend_DisagreeingObjectsAreAmbiguous(t *testing.T) {
	idx := newServiceIndex(logr.Discard())
	idx.upsertIngress(ingress("vappsim", "ingress-a", "shared.example.com", "orders-app-api"))
	idx.upsertIngress(ingress("vappsim", "ingress-b", "shared.example.com", "billing-app-api"))

	if _, _, ok := idx.ingressBackend("shared.example.com"); ok {
		t.Error("two Ingress objects naming DIFFERENT backends for the same host must resolve as unresolved (ambiguous)")
	}
}

func TestServiceIndex_IngressBackend_RemoveResolvesAmbiguity(t *testing.T) {
	idx := newServiceIndex(logr.Discard())
	idx.upsertIngress(ingress("vappsim", "ingress-a", "shared.example.com", "orders-app-api"))
	idx.upsertIngress(ingress("vappsim", "ingress-b", "shared.example.com", "billing-app-api"))
	idx.removeIngress("vappsim", "ingress-b")

	ns, name, ok := idx.ingressBackend("shared.example.com")
	if !ok || name != "orders-app-api" || ns != "vappsim" {
		t.Fatalf("ingressBackend after removing the conflicting object = (%q, %q, %v), want (vappsim, orders-app-api, true)", ns, name, ok)
	}
}

func TestServiceIndex_IngressBackend_UnknownHost(t *testing.T) {
	idx := newServiceIndex(logr.Discard())
	if _, _, ok := idx.ingressBackend("never-declared.example.com"); ok {
		t.Error("an undeclared host must resolve as unresolved")
	}
}

func TestServiceIndex_IngressBackend_NoHTTPRuleIgnored(t *testing.T) {
	idx := newServiceIndex(logr.Discard())
	idx.upsertIngress(&networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Namespace: "vappsim", Name: "empty-ingress"},
		Spec:       networkingv1.IngressSpec{Rules: []networkingv1.IngressRule{{Host: "no-http.example.com"}}},
	})
	if _, _, ok := idx.ingressBackend("no-http.example.com"); ok {
		t.Error("a rule with no HTTP paths must not resolve")
	}
}

// --- *Pusher wiring ---

func TestPusher_OnServiceOnIngress_UpdateTheSharedIndex(t *testing.T) {
	p := &Pusher{svcIndex: newServiceIndex(logr.Discard())}
	p.onService(svc("vappsim", "orders-app-api", "10.96.0.10", map[string]string{"app": "orders"}), false)
	if sel, ok := p.ServiceByName("vappsim", "orders-app-api"); !ok || sel["app"] != "orders" {
		t.Fatalf("ServiceByName after onService = (%v, %v)", sel, ok)
	}
	p.onIngress(ingress("vappsim", "orders-ingress", "orders.example.com", "orders-app-api"), false)
	if ns, name, ok := p.IngressBackend("orders.example.com"); !ok || ns != "vappsim" || name != "orders-app-api" {
		t.Fatalf("IngressBackend after onIngress = (%q, %q, %v)", ns, name, ok)
	}

	p.onService(svc("vappsim", "orders-app-api", "10.96.0.10", nil), true)
	if _, ok := p.ServiceByName("vappsim", "orders-app-api"); ok {
		t.Error("ServiceByName should report ok=false after onService(deleted=true)")
	}
	p.onIngress(ingress("vappsim", "orders-ingress", "orders.example.com", "orders-app-api"), true)
	if _, _, ok := p.IngressBackend("orders.example.com"); ok {
		t.Error("IngressBackend should report ok=false after onIngress(deleted=true)")
	}
}

func TestPusher_OnService_IgnoresWrongType(t *testing.T) {
	p := &Pusher{svcIndex: newServiceIndex(logr.Discard())}
	// A type assertion failure must be a silent no-op, not a panic.
	p.onService("not-a-service", false)
	p.onIngress("not-an-ingress", false)
}
