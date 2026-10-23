//go:build !obi || !linux

package main

import (
	"context"
	"errors"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/triageagent-dev/agent/sensorstream"
)

// runEBPF's fallback when the binary was built without `-tags obi` (or off
// linux): the "ebpf" --mode role needs the OBI codegen build —
// hack/build-obi.sh + docs/todo/obi-embed.md. Failing fast here beats a
// container that silently exports nothing.
func runEBPF(context.Context) error {
	return errors.New(`--mode role "ebpf" requires a linux build with -tags obi (see hack/build-obi.sh and docs/todo/obi-embed.md)`)
}

// noEBPFSource is the ebpf role's metric source in a build without the OBI
// instrumenter: it has nothing to collect, and runEBPF has already ended the
// role.
type noEBPFSource struct{}

// Collect implements sensorstream.RoleSource.
func (noEBPFSource) Collect(context.Context) ([]*metricspb.ResourceMetrics, error) {
	return nil, errors.New("this build has no eBPF instrumenter")
}

func ebpfSource() sensorstream.RoleSource { return noEBPFSource{} }
