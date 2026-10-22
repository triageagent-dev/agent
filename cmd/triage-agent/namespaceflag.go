// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import "strings"

// namespaceList collects repeated --target-namespaces flags:
// --target-namespaces=a --target-namespaces=b, rather than one
// comma-separated value. Empty means no explicit static scope —
// push.Options.Namespaces nil does not push every namespace; the agent
// observes the triageagent.dev/tenant-annotated set instead, in both single-tenant
// and cluster mode (see push/namespace_scope.go's scope rule). A non-empty
// list unions with that annotated set rather than replacing it.
type namespaceList []string

func (n *namespaceList) String() string { return strings.Join(*n, ",") }

func (n *namespaceList) Set(v string) error {
	v = strings.TrimSpace(v)
	if v != "" {
		*n = append(*n, v)
	}
	return nil
}
