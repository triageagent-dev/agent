// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"io"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/encoding"
	// Registers the gzip codec: a Collector's OTLP exporter compresses with it
	// by default, and the server refuses a codec nobody registered.
	_ "google.golang.org/grpc/encoding/gzip"
)

// A Collector's OTLP gRPC exporter can also compress with zstd or snappy
// (its `compression` setting). gzip comes from the gRPC package above; these
// two are registered here under the names the Collector uses. gRPC bounds the
// decompressed message by the server's receive limit, so a small frame cannot
// expand without bound.
func init() {
	encoding.RegisterCompressor(zstdCodec{})
	encoding.RegisterCompressor(snappyCodec{})
}

type zstdCodec struct{}

func (zstdCodec) Name() string { return "zstd" }

func (zstdCodec) Compress(w io.Writer) (io.WriteCloser, error) {
	return zstd.NewWriter(w, zstd.WithEncoderConcurrency(1))
}

func (zstdCodec) Decompress(r io.Reader) (io.Reader, error) {
	// One decoder goroutine-free stream per message; closed at the end of the
	// data so nothing outlives the call.
	d, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	return &closingReader{r: d, close: d.Close}, nil
}

// closingReader releases the decoder when the stream ends or fails.
type closingReader struct {
	r     io.Reader
	close func()
	done  bool
}

func (c *closingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if err != nil && !c.done {
		c.done = true
		c.close()
	}
	return n, err
}

type snappyCodec struct{}

func (snappyCodec) Name() string { return "snappy" }

func (snappyCodec) Compress(w io.Writer) (io.WriteCloser, error) {
	return snappy.NewBufferedWriter(w), nil
}

func (snappyCodec) Decompress(r io.Reader) (io.Reader, error) {
	return snappy.NewReader(r), nil
}
