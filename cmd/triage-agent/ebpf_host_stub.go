//go:build !obi || !linux

package main

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/triageagent-dev/agent/sensorstream"
)

// hostRED's fallback when the binary was built without `-tags obi` (or off
// linux): no RED or topology source is registered, and the host still reports
// virtual apps, USE and LOG.
func hostRED(map[string]sensorstream.RoleSource, hostOptions, func() []string) []func(context.Context) error {
	ctrl.Log.WithName("host-obi").Info("RED and topology are off: built without -tags obi")
	return nil
}
