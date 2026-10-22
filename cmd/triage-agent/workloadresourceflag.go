// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import "strings"

// workloadResourceList collects repeated --workload-resources flags:
// --workload-resources=a --workload-resources=b, rather than one
// comma-separated value (same convention as namespaceList /
// --target-namespaces). Empty means push.DefaultWorkloadResources — giving
// the flag even once REPLACES that default list entirely, it does not add
// to it.
type workloadResourceList []string

func (n *workloadResourceList) String() string { return strings.Join(*n, ",") }

func (n *workloadResourceList) Set(v string) error {
	v = strings.TrimSpace(v)
	if v != "" {
		*n = append(*n, v)
	}
	return nil
}
