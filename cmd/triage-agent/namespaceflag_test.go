// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"reflect"
	"testing"
)

// TestNamespaceListSet mirrors the coverage modeList's own tests would have:
// repeating the flag accumulates, a single use works, and an unset flag
// yields an empty (nil) slice — never "all namespaces" silently becoming
// "one namespace named the empty string".
func TestNamespaceListSet(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{"omitted", nil, nil},
		{"single", []string{"ops"}, []string{"ops"}},
		{"repeated", []string{"ops", "dev"}, []string{"ops", "dev"}},
		{"repeated three", []string{"ops", "dev", "prod"}, []string{"ops", "dev", "prod"}},
		{"whitespace trimmed", []string{"  ops  "}, []string{"ops"}},
		{"empty value ignored", []string{""}, nil},
		{"blank value ignored", []string{"  "}, nil},
		{"empty then real value", []string{"", "ops"}, []string{"ops"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got namespaceList
			for _, v := range tt.args {
				if err := got.Set(v); err != nil {
					t.Fatalf("Set(%q) returned error: %v", v, err)
				}
			}
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}
			if !reflect.DeepEqual([]string(got), tt.want) {
				t.Fatalf("namespaceList = %v, want %v", []string(got), tt.want)
			}
		})
	}
}

// TestNamespaceListFlagRepeated exercises the type through the real flag
// package, the way --target-namespaces is actually parsed on the command
// line: --target-namespaces=a --target-namespaces=b accumulates both.
func TestNamespaceListFlagRepeated(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var ns namespaceList
	fs.Var(&ns, "target-namespaces", "test flag")

	if err := fs.Parse([]string{"--target-namespaces=a", "--target-namespaces=b"}); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	want := []string{"a", "b"}
	if !reflect.DeepEqual([]string(ns), want) {
		t.Fatalf("namespaceList = %v, want %v", []string(ns), want)
	}
}

// TestNamespaceListFlagOmitted confirms omitting the flag entirely leaves
// the slice empty, which push.Options.Namespaces treats as the
// triageagent.dev/tenant-annotated set, not "all namespaces".
func TestNamespaceListFlagOmitted(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var ns namespaceList
	fs.Var(&ns, "target-namespaces", "test flag")

	if err := fs.Parse(nil); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(ns) != 0 {
		t.Fatalf("namespaceList = %v, want empty", []string(ns))
	}
}

// TestNamespaceListFlagSingle confirms a single use behaves like a
// one-element repeat, not like the old comma-split single-value case.
func TestNamespaceListFlagSingle(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var ns namespaceList
	fs.Var(&ns, "target-namespaces", "test flag")

	if err := fs.Parse([]string{"--target-namespaces=ops"}); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	want := []string{"ops"}
	if !reflect.DeepEqual([]string(ns), want) {
		t.Fatalf("namespaceList = %v, want %v", []string(ns), want)
	}
}

// TestNamespaceListString confirms String() round-trips into a comma-joined
// form, matching modeList's own String() shape (used only for flag.PrintDefaults
// / --help rendering, never re-parsed).
func TestNamespaceListString(t *testing.T) {
	ns := namespaceList{"a", "b"}
	if got, want := ns.String(), "a,b"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
	var empty namespaceList
	if got, want := empty.String(), ""; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}
