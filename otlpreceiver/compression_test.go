// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"testing"

	"google.golang.org/grpc/encoding"
)

// A Collector's OTLP gRPC exporter compresses with gzip unless told not to,
// and a gRPC server answers "Decompressor is not installed" to a codec nobody
// registered. This package registers gzip itself, so the intake does not
// depend on some other import having done it. (This file does not import the
// gzip package on purpose: a test that did would register it for the binary.)
func TestReceiverDecompressesGzip(t *testing.T) {
	if encoding.GetCompressor("gzip") == nil {
		t.Fatal("gzip is not registered: a Collector exporting with its default compression is refused")
	}
}
