//go:build obi && linux

package main

import (
	"context"
	"fmt"

	"go.opentelemetry.io/obi/pkg/instrumenter"
	"go.opentelemetry.io/obi/pkg/obi"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/sensorstream"
)

// hostRED is host mode's RED and topology role: the embedded OBI over the
// selected units' processes (ebpf_host_red.go). It needs the -tags obi build.
func hostRED(roles map[string]sensorstream.RoleSource, o hostOptions, units func() []string) []func(context.Context) error {
	src, loop := newHostRED(o, units, runHostInstrumenter, ctrl.Log)
	roles[metricset.RoleEBPF] = src
	return []func(context.Context) error{loop}
}

func runHostInstrumenter(ctx context.Context, cfg *obi.Config) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid OBI config: %w", err)
	}
	if err := obi.CheckOSSupport(); err != nil {
		return fmt.Errorf("OS not supported for eBPF instrumentation: %w", err)
	}
	if err := obi.CheckOSCapabilities(cfg); err != nil {
		setupLog.Info("WARNING: required system capabilities not present; instrumentation may malfunction", "error", err.Error())
	}
	return instrumenter.Run(ctx, cfg)
}
