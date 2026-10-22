// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package sensorstream

import (
	"context"
	"fmt"

	authnv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// TokenAudience is the audience of the projected ServiceAccount token a
// sensor presents. A token for any other audience is refused.
const TokenAudience = "triage-agent-sensors"

// nodeNameExtra is where the API server names the node a projected token was
// issued on (Kubernetes 1.30+).
const nodeNameExtra = "authentication.kubernetes.io/node-name"

// TokenReviewAuth accepts only tokens of one ServiceAccount, checked with a
// TokenReview for TokenAudience.
type TokenReviewAuth struct {
	Client kubernetes.Interface
	// Namespace and ServiceAccount name the sensors' ServiceAccount.
	Namespace      string
	ServiceAccount string
}

// Authenticate implements Authenticator.
func (a TokenReviewAuth) Authenticate(ctx context.Context, token string) (Identity, error) {
	tr, err := a.Client.AuthenticationV1().TokenReviews().Create(ctx, &authnv1.TokenReview{
		Spec: authnv1.TokenReviewSpec{Token: token, Audiences: []string{TokenAudience}},
	}, metav1.CreateOptions{})
	if err != nil {
		return Identity{}, fmt.Errorf("token review: %w", err)
	}
	if !tr.Status.Authenticated {
		return Identity{}, fmt.Errorf("token not authenticated: %s", tr.Status.Error)
	}
	want := fmt.Sprintf("system:serviceaccount:%s:%s", a.Namespace, a.ServiceAccount)
	if tr.Status.User.Username != want {
		return Identity{}, fmt.Errorf("token belongs to %s, not %s", tr.Status.User.Username, want)
	}
	id := Identity{User: tr.Status.User.Username}
	if v := tr.Status.User.Extra[nodeNameExtra]; len(v) == 1 {
		id.Node = v[0]
	}
	return id, nil
}
