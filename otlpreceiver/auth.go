// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"container/list"
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// TokenAudience is the audience of the projected ServiceAccount token an
// OTLP sender presents. A token for any other audience, the sensors' one
// included, is refused.
const TokenAudience = "triage-agent-otlp"

// TokenReviewAuth accepts tokens of the listed ServiceAccounts only,
// checked with a TokenReview for TokenAudience.
//
// Outcomes are cached by the SHA-256 of the token (successes for
// tokenOKTTL, refusals for tokenFailTTL, at most tokenCacheMax entries,
// oldest evicted first), so a sender's stream of requests costs one
// TokenReview per interval, not one per request. A failed call to the API
// server is not cached.
type TokenReviewAuth struct {
	Client kubernetes.Interface
	// Allowed lists "namespace/serviceaccount" entries; "namespace/*"
	// allows every ServiceAccount of a namespace. Empty refuses everyone.
	Allowed []string
	// Forwarders lists "namespace/serviceaccount" entries (same syntax) of
	// allowed accounts that may publish for any namespace. An entry that is
	// not also allowed gains nothing.
	Forwarders []string

	cache tokenCache
}

// Cache lifetimes and size.
const (
	tokenOKTTL    = 5 * time.Minute
	tokenFailTTL  = 30 * time.Second
	tokenCacheMax = 10000
)

type tokenOutcome struct {
	key [sha256.Size]byte
	p   Principal
	err error
	exp time.Time
}

// tokenCache is a bounded cache with oldest-first eviction.
type tokenCache struct {
	mu    sync.Mutex
	now   func() time.Time // time.Now when nil
	max   int              // tokenCacheMax when 0
	byKey map[[sha256.Size]byte]*list.Element
	order *list.List // front is the oldest
}

func (c *tokenCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *tokenCache) get(k [sha256.Size]byte) (*tokenOutcome, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.byKey[k]
	if !ok {
		return nil, false
	}
	o := el.Value.(*tokenOutcome)
	if !c.clock().Before(o.exp) {
		c.order.Remove(el)
		delete(c.byKey, k)
		return nil, false
	}
	return o, true
}

func (c *tokenCache) put(k [sha256.Size]byte, p Principal, err error) {
	ttl := tokenOKTTL
	if err != nil {
		ttl = tokenFailTTL
	}
	limit := c.max
	if limit <= 0 {
		limit = tokenCacheMax
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byKey == nil {
		c.byKey, c.order = map[[sha256.Size]byte]*list.Element{}, list.New()
	}
	if el, ok := c.byKey[k]; ok {
		c.order.Remove(el)
	}
	c.byKey[k] = c.order.PushBack(&tokenOutcome{key: k, p: p, err: err, exp: c.clock().Add(ttl)})
	for c.order.Len() > limit {
		old := c.order.Remove(c.order.Front()).(*tokenOutcome)
		delete(c.byKey, old.key)
	}
}

// ParseAllowed splits a comma-separated allow-list and checks each entry's
// form.
func ParseAllowed(s string) ([]string, error) {
	var out []string
	for _, e := range strings.Split(s, ",") {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		ns, sa, ok := strings.Cut(e, "/")
		if !ok || ns == "" || sa == "" || strings.Contains(sa, "/") {
			return nil, fmt.Errorf("%q: want namespace/serviceaccount or namespace/*", e)
		}
		out = append(out, e)
	}
	return out, nil
}

// Authenticate implements Authenticator and returns the token's
// ServiceAccount.
func (a *TokenReviewAuth) Authenticate(ctx context.Context, token string) (Principal, error) {
	k := sha256.Sum256([]byte(token))
	if o, ok := a.cache.get(k); ok {
		return o.p, o.err
	}
	p, cacheable, err := a.review(ctx, token)
	if cacheable {
		a.cache.put(k, p, err)
	}
	return p, err
}

// review asks the API server; cacheable is false when the call itself
// failed.
func (a *TokenReviewAuth) review(ctx context.Context, token string) (p Principal, cacheable bool, err error) {
	tr, err := a.Client.AuthenticationV1().TokenReviews().Create(ctx, &authnv1.TokenReview{
		Spec: authnv1.TokenReviewSpec{Token: token, Audiences: []string{TokenAudience}},
	}, metav1.CreateOptions{})
	if err != nil {
		return Principal{}, false, fmt.Errorf("token review: %w", err)
	}
	if !tr.Status.Authenticated {
		return Principal{}, true, fmt.Errorf("token not authenticated: %s", tr.Status.Error)
	}
	user := tr.Status.User.Username
	ns, sa, ok := splitServiceAccount(user)
	if !ok || !matches(a.Allowed, ns, sa) {
		return Principal{}, true, fmt.Errorf("token belongs to %s, which is not allowed to send OTLP", user)
	}
	return Principal{Namespace: ns, Name: sa, Forwarder: matches(a.Forwarders, ns, sa)}, true, nil
}

// splitServiceAccount parses "system:serviceaccount:<ns>:<name>".
func splitServiceAccount(user string) (ns, sa string, ok bool) {
	rest, ok := strings.CutPrefix(user, "system:serviceaccount:")
	if !ok {
		return "", "", false
	}
	ns, sa, ok = strings.Cut(rest, ":")
	return ns, sa, ok
}

// matches reports whether ns/sa is covered by a list of
// "namespace/serviceaccount" or "namespace/*" entries.
func matches(list []string, ns, sa string) bool {
	for _, e := range list {
		ans, asa, _ := strings.Cut(e, "/")
		if ans == ns && (asa == "*" || asa == sa) {
			return true
		}
	}
	return false
}

// allows reports whether the user is an allowed ServiceAccount.
func (a *TokenReviewAuth) allows(user string) bool {
	ns, sa, ok := splitServiceAccount(user)
	return ok && matches(a.Allowed, ns, sa)
}
