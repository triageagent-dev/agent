// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCgroupMemoryLimit_ReadsTheFirstStatedLimit(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	v2max := write("memory.max", "max\n")
	v1 := write("memory.limit_in_bytes", "2147483648\n")
	v1none := write("memory.limit_in_bytes.none", "9223372036854771712\n")

	if n, from := cgroupMemoryLimit([]string{v2max, v1}); n != 2147483648 || from != v1 {
		t.Fatalf("v2 'max' then v1 limit: got %d from %q, want 2147483648 from %q", n, from, v1)
	}
	if n, _ := cgroupMemoryLimit([]string{v2max, v1none}); n != 0 {
		t.Fatalf("v1 'no limit' sentinel: got %d, want 0", n)
	}
	if n, _ := cgroupMemoryLimit([]string{filepath.Join(dir, "missing")}); n != 0 {
		t.Fatalf("no cgroup file: got %d, want 0", n)
	}
	v2 := write("memory.max.set", "1073741824")
	if n, from := cgroupMemoryLimit([]string{v2, v1}); n != 1073741824 || from != v2 {
		t.Fatalf("v2 limit first: got %d from %q, want 1073741824 from %q", n, from, v2)
	}
}
