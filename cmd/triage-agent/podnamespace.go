// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"strings"
)

// serviceAccountNamespaceFile is where the kubelet projects a pod's own
// namespace. Present in every pod that mounts a ServiceAccount token, which
// this agent does in both of its workloads.
const serviceAccountNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// podNamespace returns the namespace this process runs in, or "" when it
// cannot tell. Backs the three --*-metrics-service-namespace defaults, which
// always name this pod's own namespace. POD_NAMESPACE wins if set.
func podNamespace() string {
	if ns := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); ns != "" {
		return ns
	}
	raw, err := os.ReadFile(serviceAccountNamespaceFile)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}
