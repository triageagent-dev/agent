// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	collectorpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	authnv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// reviewer returns a fake clientset whose TokenReview accepts the tokens
// "demo" (demo/collector) and "other" (other/app), refuses everything else,
// and counts the calls.
func reviewer(calls *atomic.Int64, apiErr *atomic.Bool) *fake.Clientset {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("create", "tokenreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		calls.Add(1)
		if apiErr.Load() {
			return true, nil, errors.New("api unavailable")
		}
		tr := a.(k8stesting.CreateAction).GetObject().(*authnv1.TokenReview)
		out := tr.DeepCopy()
		switch tr.Spec.Token {
		case "demo":
			out.Status = authnv1.TokenReviewStatus{Authenticated: true, User: authnv1.UserInfo{Username: "system:serviceaccount:demo:collector"}}
		case "other":
			out.Status = authnv1.TokenReviewStatus{Authenticated: true, User: authnv1.UserInfo{Username: "system:serviceaccount:other:app"}}
		default:
			out.Status = authnv1.TokenReviewStatus{Error: "unknown token"}
		}
		return true, out, nil
	})
	return cs
}

func TestTokenReviewAuth_Cache(t *testing.T) {
	var calls atomic.Int64
	var apiErr atomic.Bool
	now := time.Unix(1_800_000_000, 0)
	a := &TokenReviewAuth{Client: reviewer(&calls, &apiErr), Allowed: []string{"demo/*"}}
	a.cache.now = func() time.Time { return now }
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		p, err := a.Authenticate(ctx, "demo")
		if err != nil || p.Namespace != "demo" || p.Name != "collector" {
			t.Fatalf("demo token: %+v, %v", p, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("5 requests made %d reviews, want 1", calls.Load())
	}
	// A success is held for 5 minutes.
	now = now.Add(tokenOKTTL - time.Second)
	_, _ = a.Authenticate(ctx, "demo")
	if calls.Load() != 1 {
		t.Fatalf("success expired early: %d reviews", calls.Load())
	}
	now = now.Add(2 * time.Second)
	_, _ = a.Authenticate(ctx, "demo")
	if calls.Load() != 2 {
		t.Fatalf("success outlived its TTL: %d reviews", calls.Load())
	}

	// Refusals (unknown token, authenticated but not allowed) are held 30 s.
	calls.Store(0)
	for _, tok := range []string{"bad", "other"} {
		for i := 0; i < 3; i++ {
			if _, err := a.Authenticate(ctx, tok); err == nil {
				t.Fatalf("%s token accepted", tok)
			}
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("refusals made %d reviews, want 2", calls.Load())
	}
	now = now.Add(tokenFailTTL - time.Second)
	_, _ = a.Authenticate(ctx, "bad")
	if calls.Load() != 2 {
		t.Fatalf("refusal expired early: %d reviews", calls.Load())
	}
	now = now.Add(2 * time.Second)
	_, _ = a.Authenticate(ctx, "bad")
	if calls.Load() != 3 {
		t.Fatalf("refusal outlived its TTL: %d reviews", calls.Load())
	}

	// A failed call to the API server is not cached.
	calls.Store(0)
	apiErr.Store(true)
	for i := 0; i < 3; i++ {
		if _, err := a.Authenticate(ctx, "fresh"); err == nil {
			t.Fatal("an API error was reported as success")
		}
	}
	if calls.Load() != 3 {
		t.Fatalf("API errors made %d reviews, want 3 (uncached)", calls.Load())
	}
}

func TestTokenCache_EvictsOldestFirst(t *testing.T) {
	var calls atomic.Int64
	var apiErr atomic.Bool
	a := &TokenReviewAuth{Client: reviewer(&calls, &apiErr), Allowed: []string{"demo/*"}}
	a.cache.max = 3
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_, _ = a.Authenticate(ctx, fmt.Sprintf("t%d", i)) // refused, cached
	}
	if n := a.cache.order.Len(); n != 3 || len(a.cache.byKey) != 3 {
		t.Fatalf("cache holds %d/%d entries, want 3", n, len(a.cache.byKey))
	}
	calls.Store(0)
	_, _ = a.Authenticate(ctx, "t4") // newest: still cached
	if calls.Load() != 0 {
		t.Fatal("the newest entry was evicted")
	}
	_, _ = a.Authenticate(ctx, "t0") // oldest: evicted
	if calls.Load() != 1 {
		t.Fatal("the oldest entry was not evicted")
	}
}

// A request with neither a bearer token nor a verified certificate is
// refused before the API server is asked.
func TestExport_NoCredentialsSkipsTokenReview(t *testing.T) {
	var calls atomic.Int64
	var apiErr atomic.Bool
	r := New(Options{Auth: &TokenReviewAuth{Client: reviewer(&calls, &apiErr), Allowed: []string{"demo/*"}}, Log: logr.Discard()})
	if _, err := r.Export(context.Background(), &collectorpb.ExportMetricsServiceRequest{}); err == nil {
		t.Fatal("an export without credentials was accepted")
	}
	if calls.Load() != 0 {
		t.Fatalf("%d token reviews for a request without credentials", calls.Load())
	}
}
