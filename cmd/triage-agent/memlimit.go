// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
)

// cgroupMemoryFiles are where a container's memory limit is read from:
// cgroup v2, then v1. A host outside a container has neither, or has "max".
var cgroupMemoryFiles = []string{
	"/sys/fs/cgroup/memory.max",
	"/sys/fs/cgroup/memory/memory.limit_in_bytes",
}

// memoryLimitShare is how much of the container's limit the Go heap may
// take; the rest is for the stacks, the runtime and the gRPC buffers.
const memoryLimitShare = 0.9

// applyMemoryLimit gives the Go runtime a soft memory limit from the
// container's cgroup, so the garbage collector works harder as the heap
// nears the limit instead of letting the kernel kill the pod. Without one
// GOGC alone lets the heap grow to twice the live data whatever the pod may
// use. A GOMEMLIMIT in the environment wins; the runtime read it already.
func applyMemoryLimit(log logr.Logger) {
	if os.Getenv("GOMEMLIMIT") != "" {
		return
	}
	limit, path := cgroupMemoryLimit(cgroupMemoryFiles)
	if limit <= 0 {
		return
	}
	soft := int64(float64(limit) * memoryLimitShare)
	debug.SetMemoryLimit(soft)
	log.Info("memory limit from the cgroup", "limit", limit, "goMemLimit", soft, "from", path)
}

// cgroupMemoryLimit returns the first readable limit in bytes among paths and
// where it came from, or 0 when none states one ("max", or a value past
// what a limit can be, is none).
func cgroupMemoryLimit(paths []string) (int64, string) {
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		s := strings.TrimSpace(string(b))
		if s == "" || s == "max" {
			continue
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n <= 0 || n >= 1<<62 {
			continue
		}
		return n, p
	}
	return 0, ""
}
