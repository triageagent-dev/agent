package agentui

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	authzv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const (
	jwtShop  = "aaa.bbb.shop"
	jwtAdmin = "aaa.bbb.admin"
	jwtNone  = "aaa.bbb.none"
	jwtBad   = "aaa.bbb.bad"
)

// saFixture fakes the API server: tokens map to ServiceAccounts, an admin
// holds the grant cluster-wide, shop only in its own namespace, none nowhere.
func saFixture() (*SAAuth, *int) {
	c := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "shop", Annotations: map[string]string{"triageagent.dev/tenant": "t1"}}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "bare"}})
	users := map[string]string{
		jwtShop: "system:serviceaccount:shop:grafana", jwtAdmin: "system:serviceaccount:ops:admin", jwtNone: "system:serviceaccount:bare:x",
	}
	reviews := 0
	c.PrependReactor("create", "tokenreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		reviews++
		tr := a.(k8stesting.CreateAction).GetObject().(*authnv1.TokenReview)
		u, ok := users[tr.Spec.Token]
		tr.Status = authnv1.TokenReviewStatus{Authenticated: ok, Audiences: tr.Spec.Audiences,
			User: authnv1.UserInfo{Username: u, Groups: []string{"system:serviceaccounts", "system:authenticated"}}}
		if len(tr.Spec.Audiences) != 1 || tr.Spec.Audiences[0] != SAAudience {
			tr.Status.Authenticated = false
		}
		return true, tr, nil
	})
	c.PrependReactor("create", "subjectaccessreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		sar := a.(k8stesting.CreateAction).GetObject().(*authzv1.SubjectAccessReview)
		ra := sar.Spec.ResourceAttributes
		okRes := ra.Group == "triageagent.dev" && ra.Resource == "agent-api" && ra.Verb == "get"
		switch {
		case okRes && sar.Spec.User == users[jwtAdmin]:
			sar.Status.Allowed = true
		case okRes && sar.Spec.User == users[jwtShop] && ra.Namespace == "shop":
			sar.Status.Allowed = true
		}
		return true, sar, nil
	})
	return &SAAuth{Client: c}, &reviews
}

func TestSAAuth(t *testing.T) {
	a, reviews := saFixture()
	if n, ten, ok := a.lookup(jwtAdmin); !ok || ten != "" || n == "" {
		t.Errorf("cluster-wide grant: %q %q %v, want unrestricted", n, ten, ok)
	}
	if _, ten, ok := a.lookup(jwtShop); !ok || ten != "t1" {
		t.Errorf("namespace grant: tenant %q ok %v, want t1", ten, ok)
	}
	for _, tok := range []string{jwtNone, jwtBad, "not-a-jwt"} {
		if _, _, ok := a.lookup(tok); ok {
			t.Errorf("%s was accepted", tok)
		}
	}
	before := *reviews
	a.lookup(jwtAdmin)
	a.lookup("not-a-jwt")
	if *reviews != before {
		t.Errorf("a cached or non-JWT token cost %d more TokenReviews", *reviews-before)
	}
}

func TestSAAuthNamespaceWithoutTenantIsRefused(t *testing.T) {
	a, _ := saFixture()
	c := a.Client.(*fake.Clientset)
	// bare holds the grant but its namespace has no tenant annotation.
	c.PrependReactor("create", "subjectaccessreviews", func(act k8stesting.Action) (bool, runtime.Object, error) {
		sar := act.(k8stesting.CreateAction).GetObject().(*authzv1.SubjectAccessReview)
		sar.Status.Allowed = sar.Spec.ResourceAttributes.Namespace == "bare"
		return true, sar, nil
	})
	if _, _, ok := a.lookup(jwtNone); ok {
		t.Error("a namespace with no tenant read as unrestricted")
	}
}

func TestAPIAcceptsSAWithoutTokenFile(t *testing.T) {
	srv, _ := newTestServer(t)
	sa, _ := saFixture()
	a, err := NewAPI(srv, APIOptions{Addr: ":0", SA: sa})
	if err != nil {
		t.Fatal(err)
	}
	h := a.Handler()
	if w := call(h, "/api/v1/tenants", jwtShop); w.Code != 200 {
		t.Errorf("SA token: %d", w.Code)
	}
	if w := call(h, "/api/v1/tenants", "plain-token-not-a-jwt-at-all-1"); w.Code != 401 {
		t.Errorf("unknown token: %d, want 401", w.Code)
	}
	if _, err := NewAPI(srv, APIOptions{Addr: ":0"}); err == nil {
		t.Error("no credentials at all must refuse to start")
	}
}

func TestSAAuthShapeCacheAndExpiry(t *testing.T) {
	a, reviews := saFixture()
	for _, tok := range []string{"a.b", "a..c", "a.b.c.d", "a.b.c d", "a.b." + strings.Repeat("x", saTokenMax)} {
		a.lookup(tok)
	}
	if *reviews != 0 {
		t.Errorf("malformed tokens cost %d TokenReviews", *reviews)
	}
	// A flood of distinct refused tokens does not evict an accepted caller.
	a.lookup(jwtAdmin)
	for i := 0; i < saCacheMax+10; i++ {
		a.lookup(fmt.Sprintf("aaa.bbb.g%d", i))
	}
	before := *reviews
	if _, _, ok := a.lookup(jwtAdmin); !ok || *reviews != before {
		t.Errorf("garbage flood evicted a cached caller (ok=%v, extra reviews %d)", ok, *reviews-before)
	}
	// The token's own exp caps the cache lifetime.
	exp := time.Now().Add(2 * time.Second).Unix()
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, exp)))
	if got := tokenExpiry("h." + payload + ".s"); got.Unix() != exp {
		t.Errorf("tokenExpiry = %v, want %d", got, exp)
	}
}
