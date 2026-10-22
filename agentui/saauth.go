package agentui

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	authnv1 "k8s.io/api/authentication/v1"
	authzv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
)

// SAAudience is the audience of the ServiceAccount token an API caller
// presents (kubectl create token <sa> --audience triage-agent-api).
const SAAudience = "triage-agent-api"

// The permission RBAC grants to read the API: verb get on this resource of
// the triageagent.dev group. Granted cluster-wide it is unrestricted; granted
// in a namespace it limits the caller to that namespace's tenant.
const (
	saGroup    = "triageagent.dev"
	saResource = "agent-api"
	saVerb     = "get"
)

const (
	saOKTTL     = time.Minute
	saFailTTL   = 30 * time.Second
	saCacheMax  = 1000
	saTokenMax  = 4096 // a ServiceAccount token is far smaller; longer is garbage
	saMaxReview = 8    // concurrent TokenReview/SubjectAccessReview rounds
)

// SAAuth authenticates API callers by ServiceAccount token (cluster mode): a
// TokenReview for SAAudience, then a SubjectAccessReview for the caller on
// the API resource. A cluster-wide grant is unrestricted; a grant in the
// ServiceAccount's own namespace limits it to that namespace's
// triageagent.dev/tenant annotation. Outcomes are cached by token hash.
type SAAuth struct {
	Client kubernetes.Interface

	mu     sync.Mutex
	cache  map[[sha256.Size]byte]saOutcome // accepted callers
	denied map[[sha256.Size]byte]saOutcome // refused tokens, kept apart so a flood of garbage cannot evict the callers
	flight singleflight.Group
	sem    chan struct{}
	semMu  sync.Once
}

type saOutcome struct {
	name, tenant string
	ok           bool
	exp          time.Time
}

// looksLikeJWT keeps garbage from costing a TokenReview: three non-empty
// base64url segments, not longer than a ServiceAccount token can be.
func looksLikeJWT(tok string) bool {
	if len(tok) > saTokenMax {
		return false
	}
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return false
	}
	for _, p := range parts {
		if p == "" || strings.Trim(p, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_=") != "" {
			return false
		}
	}
	return true
}

// tokenExpiry reads the exp claim of a token the API server has already
// accepted; zero when it has none.
func tokenExpiry(tok string) time.Time {
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return time.Time{}
	}
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return time.Time{}
	}
	var c struct {
		Exp int64 `json:"exp"`
	}
	if json.Unmarshal(b, &c) != nil || c.Exp == 0 {
		return time.Time{}
	}
	return time.Unix(c.Exp, 0)
}

func (a *SAAuth) lookup(tok string) (name, tenant string, ok bool) {
	if !looksLikeJWT(tok) {
		return "", "", false
	}
	k := sha256.Sum256([]byte(tok))
	now := time.Now()
	a.mu.Lock()
	o, hit := a.cache[k]
	if !hit {
		o, hit = a.denied[k]
	}
	a.mu.Unlock()
	if hit && now.Before(o.exp) {
		return o.name, o.tenant, o.ok
	}
	// One review round per distinct token at a time, and a few at once in
	// all, so a burst cannot queue real callers behind it.
	a.semMu.Do(func() { a.sem = make(chan struct{}, saMaxReview) })
	v, _, _ := a.flight.Do(string(k[:]), func() (any, error) {
		select {
		case a.sem <- struct{}{}:
			defer func() { <-a.sem }()
		case <-time.After(2 * time.Second):
			return saOutcome{}, nil // busy: refuse without remembering
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		o, cacheable := a.review(ctx, tok)
		if !cacheable {
			return o, nil
		}
		ttl, m := saFailTTL, &a.denied
		if o.ok {
			ttl, m = saOKTTL, &a.cache
		}
		o.exp = now.Add(ttl)
		if e := tokenExpiry(tok); o.ok && !e.IsZero() && e.Before(o.exp) {
			o.exp = e
		}
		a.mu.Lock()
		if *m == nil || len(*m) >= saCacheMax {
			*m = map[[sha256.Size]byte]saOutcome{}
		}
		(*m)[k] = o
		a.mu.Unlock()
		return o, nil
	})
	o = v.(saOutcome)
	return o.name, o.tenant, o.ok
}

// review decides one token; cacheable is false when an API call failed, which
// refuses the caller without remembering it.
func (a *SAAuth) review(ctx context.Context, tok string) (saOutcome, bool) {
	tr, err := a.Client.AuthenticationV1().TokenReviews().Create(ctx, &authnv1.TokenReview{
		Spec: authnv1.TokenReviewSpec{Token: tok, Audiences: []string{SAAudience}},
	}, metav1.CreateOptions{})
	if err != nil {
		return saOutcome{}, false
	}
	user := tr.Status.User.Username
	rest, isSA := strings.CutPrefix(user, "system:serviceaccount:")
	ns, sa, twoParts := strings.Cut(rest, ":")
	if !tr.Status.Authenticated || !isSA || !twoParts || ns == "" || sa == "" || strings.Contains(sa, ":") ||
		!slices.Contains(tr.Status.Audiences, SAAudience) || !slices.Contains(tr.Status.User.Groups, "system:serviceaccounts") {
		return saOutcome{}, true
	}
	allowed := func(namespace string) (bool, bool) {
		extra := map[string]authzv1.ExtraValue{}
		for k, v := range tr.Status.User.Extra {
			extra[k] = authzv1.ExtraValue(v)
		}
		sar, err := a.Client.AuthorizationV1().SubjectAccessReviews().Create(ctx, &authzv1.SubjectAccessReview{
			Spec: authzv1.SubjectAccessReviewSpec{
				User: user, Groups: tr.Status.User.Groups, UID: tr.Status.User.UID, Extra: extra,
				ResourceAttributes: &authzv1.ResourceAttributes{Namespace: namespace, Verb: saVerb, Group: saGroup, Resource: saResource},
			},
		}, metav1.CreateOptions{})
		if err != nil {
			return false, false
		}
		return sar.Status.Allowed, true
	}
	if ok, fine := allowed(""); !fine {
		return saOutcome{}, false
	} else if ok {
		return saOutcome{name: user, ok: true}, true
	}
	if ok, fine := allowed(ns); !fine {
		return saOutcome{}, false
	} else if !ok {
		return saOutcome{}, true
	}
	n, err := a.Client.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if err != nil {
		return saOutcome{}, false
	}
	tenant := n.Annotations[annotationkeys.Tenant]
	if tenant == "" { // a namespace with no tenant would otherwise read as unrestricted
		return saOutcome{}, true
	}
	return saOutcome{name: user, tenant: tenant, ok: true}, true
}
