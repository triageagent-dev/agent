//go:build !obi || !linux

package main

import (
	"context"
	"errors"
)

// runEBPF's fallback when the binary was built without `-tags obi` (or off
// linux): the "ebpf" --mode role needs the OBI codegen build —
// hack/build-obi.sh + docs/todo/obi-embed.md. Failing fast here beats a
// container that silently exports nothing.
func runEBPF(context.Context) error {
	return errors.New(`--mode role "ebpf" requires a linux build with -tags obi (see hack/build-obi.sh and docs/todo/obi-embed.md)`)
}
