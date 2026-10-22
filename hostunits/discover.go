// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

// Package hostunits discovers the systemd services of a Linux host and
// delivers them as workloads to the push package, so each selected unit is one
// virtual app (docs/adr/0018-linux-host-mode.md, phase 2).
package hostunits

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"

	"github.com/triageagent-dev/agent/push"
)

// suffix is the systemd unit type this package selects.
const suffix = ".service"

// PollInterval is the fallback rescan period when no change signal arrives.
const PollInterval = 10 * time.Second

// DefaultDenylist holds the globs of services that are never selected when
// the operator gives no --host-units: system plumbing and the agent itself.
var DefaultDenylist = []string{
	"systemd-*", "dbus", "ssh*", "cron", "getty@*", "polkit", "udisks2", "snapd*", "triage-agent",
}

// Unit is one service unit as the bus reports it.
type Unit struct {
	// Name is the unit name with the .service suffix.
	Name string
	// Enabled is true when the unit file is enabled.
	Enabled     bool
	ActiveState string
	SubState    string
	// Type is the service's Type= setting (simple, oneshot, ...); empty when
	// the unit is not loaded.
	Type string
}

// Bus is the part of systemd's D-Bus API the discoverer needs. The real
// implementation is NewBus; tests use a fake.
type Bus interface {
	// Units lists every service unit known to systemd: loaded units with
	// their state, and installed unit files with their enablement.
	Units(ctx context.Context) ([]Unit, error)
	// Restarts returns the unit's NRestarts counter.
	Restarts(ctx context.Context, name string) (uint64, error)
	// Changes returns a channel that receives a signal whenever a unit's
	// properties may have changed. The channel ends or stays silent when the
	// bus cannot subscribe; the poll covers that.
	Changes(ctx context.Context) (<-chan struct{}, error)
	// Close releases the connection.
	Close()
}

// Options configures a Discoverer.
type Options struct {
	// Globs selects units by name (with or without .service). Empty selects
	// every enabled service except DefaultDenylist.
	Globs []string
	// Poll is the fallback rescan period; PollInterval when zero.
	Poll time.Duration
	// Log receives diagnostics.
	Log logr.Logger
}

// Discoverer turns systemd service units into push.AppEvents. It implements
// push.AppSource.
type Discoverer struct {
	opts    Options
	connect func(context.Context) (Bus, error)

	mu       sync.Mutex               // guards last and restarts: Scan writes, Names reads
	last     map[string]push.AppEvent // by unit name, as last emitted
	restarts map[string]uint64        // last good NRestarts per unit
}

// Names returns the app names (unit names without .service) of the units the
// last Scan selected, sorted. The host's sensor roles read it on every
// collect, so USE and LOG describe the same units the virtual apps do. Empty
// before the first Scan.
func (d *Discoverer) Names() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	names := make([]string, 0, len(d.last))
	for name := range d.last {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

var _ push.AppSource = (*Discoverer)(nil)

// New returns a Discoverer that talks to the system bus.
func New(opts Options) *Discoverer {
	return newWithConnect(opts, NewBus)
}

func newWithConnect(opts Options, connect func(context.Context) (Bus, error)) *Discoverer {
	if opts.Poll <= 0 {
		opts.Poll = PollInterval
	}
	return &Discoverer{opts: opts, connect: connect,
		last: map[string]push.AppEvent{}, restarts: map[string]uint64{}}
}

// AppName returns the virtual app name of a unit: the unit name without
// .service.
func AppName(unit string) string { return strings.TrimSuffix(unit, suffix) }

func matchAny(globs []string, unit string) bool {
	app := AppName(unit)
	for _, g := range globs {
		if ok, _ := path.Match(g, app); ok {
			return true
		}
		if ok, _ := path.Match(g, unit); ok {
			return true
		}
	}
	return false
}

// Selected reports whether the unit is selected under globs. With globs, a
// unit is selected when it matches one, enabled or not. Without, it must be
// enabled and not on the denylist. Template units (name@.service) are never
// selected; their instances are.
func Selected(u Unit, globs []string) bool {
	if !strings.HasSuffix(u.Name, suffix) || strings.HasSuffix(u.Name, "@"+suffix) {
		return false
	}
	if len(globs) > 0 {
		return matchAny(globs, u.Name)
	}
	// A oneshot runs once and exits (apparmor, grub-common and the like), and
	// an idle unit is the same thing scheduled after the rest of boot (dmesg,
	// rc-local); neither is a service to watch unless a glob names it.
	return u.Enabled && !runsOnce(u.Type) && !matchAny(DefaultDenylist, u.Name)
}

// runsOnce reports whether a service Type= describes a unit that runs to
// completion and exits instead of staying up.
func runsOnce(typ string) bool { return typ == "oneshot" || typ == "idle" }

// Ready derives readiness from the unit state: active and running.
func Ready(u Unit) bool { return u.ActiveState == "active" && u.SubState == "running" }

// Watch implements push.AppSource: it scans on start, on every change signal
// and every poll period until ctx ends.
func (d *Discoverer) Watch(ctx context.Context, emit func(push.AppEvent)) error {
	bus, err := d.connect(ctx)
	if err != nil {
		return fmt.Errorf("connect to systemd: %w", err)
	}
	defer bus.Close()

	changes, err := bus.Changes(ctx)
	if err != nil {
		d.opts.Log.Info("systemd change subscription failed; polling only", "error", err.Error())
		changes = nil
	}
	// Signals arrive in bursts; one rescan after the burst is enough.
	const settle = 500 * time.Millisecond

	t := time.NewTicker(d.opts.Poll)
	defer t.Stop()
	for {
		if err := d.Scan(ctx, bus, emit); err != nil && ctx.Err() == nil {
			d.opts.Log.Error(err, "scan systemd units")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		case _, ok := <-changes:
			if !ok {
				changes = nil
				continue
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(settle):
			}
			for drained := false; !drained; {
				select {
				case <-changes:
				default:
					drained = true
				}
			}
		}
	}
}

// Scan lists the units once and emits what changed since the last scan.
func (d *Discoverer) Scan(ctx context.Context, bus Bus, emit func(push.AppEvent)) error {
	units, err := bus.Units(ctx)
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	seen := map[string]bool{}
	sort.Slice(units, func(i, j int) bool { return units[i].Name < units[j].Name })
	for _, u := range units {
		if !Selected(u, d.opts.Globs) {
			continue
		}
		name := AppName(u.Name)
		if seen[name] {
			continue
		}
		seen[name] = true
		if n, err := bus.Restarts(ctx, u.Name); err == nil {
			d.restarts[name] = n
		}
		ev := push.AppEvent{Name: name, Ready: Ready(u), Restarts: d.restarts[name]}
		if prev, ok := d.last[name]; !ok || prev != ev {
			d.last[name] = ev
			emit(ev)
		}
	}
	for name := range d.last {
		if !seen[name] {
			delete(d.last, name)
			delete(d.restarts, name)
			emit(push.AppEvent{Name: name, Deleted: true})
		}
	}
	return nil
}
