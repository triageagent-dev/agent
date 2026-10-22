// Reload scheduling for the eBPF role's instrumentation scope. Kept out of
// ebpf_obi.go's `obi && linux` build tag so it is testable without a
// codegen'd OBI checkout.
package main

import (
	"context"
	"errors"
	"time"
)

// instrumentReloadDebounce is how long the set must stay unchanged before a
// reload is acted on.
const instrumentReloadDebounce = 15 * time.Second

// settled waits for `quiet` to pass with no further change, restarting the
// timer on each one. Returns false only if the process is shutting down.
func settled(ctx context.Context, w *namespaceTenantWatch, quiet time.Duration) bool {
	timer := time.NewTimer(quiet)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-w.Changed():
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(quiet)
		case <-timer.C:
			return true
		}
	}
}

// classifyReloadStop decides whether an error returned by stopping the
// instrumenter for a supervisor-initiated reload is worth logging. A reload
// cancels the run's context itself, so context.Canceled (wrapped or bare) is
// the expected shape of a clean stop and is never logged; anything else,
// including OBI's own finisher-timeout error, which is not context.Canceled,
// is logged but must not be treated as fatal, since the reload path never
// returns the stop error to its caller.
func classifyReloadStop(err error) (logIt bool) {
	if err == nil {
		return false
	}
	return !errors.Is(err, context.Canceled)
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
