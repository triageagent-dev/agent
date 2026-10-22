// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import "strings"

// modeList collects repeated --mode flags: --mode=ebpf --mode=node-exporter
// rather than one comma-separated value. Empty means the default, "controller".
type modeList []string

func (m *modeList) String() string { return strings.Join(*m, ",") }

func (m *modeList) Set(v string) error {
	v = strings.TrimSpace(v)
	if v != "" {
		*m = append(*m, v)
	}
	return nil
}

// roles returns the selected roles, defaulting to the controller.
func (m modeList) roles() []string {
	if len(m) == 0 {
		return []string{"controller"}
	}
	return m
}
