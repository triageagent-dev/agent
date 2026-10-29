// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// Without a flag the logger is at info: a debug line is not written. The
// flag still raises it.
func TestLogOptions_InfoUnlessTheFlagSaysOtherwise(t *testing.T) {
	opts := logOptions()
	fs := flag.NewFlagSet("default", flag.ContinueOnError)
	opts.BindFlags(fs)
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	log := zap.New(zap.UseFlagOptions(&opts))
	if log.V(1).Enabled() {
		t.Fatal("a debug line is enabled by default")
	}
	if !log.V(0).Enabled() {
		t.Fatal("an info line is not enabled by default")
	}

	opts = logOptions()
	fs = flag.NewFlagSet("debug", flag.ContinueOnError)
	opts.BindFlags(fs)
	if err := fs.Parse([]string{"--zap-log-level=debug"}); err != nil {
		t.Fatal(err)
	}
	if log := zap.New(zap.UseFlagOptions(&opts)); !log.V(1).Enabled() {
		t.Fatal("--zap-log-level=debug does not enable a debug line")
	}
}
