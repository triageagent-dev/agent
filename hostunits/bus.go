// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package hostunits

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/coreos/go-systemd/v22/dbus"
)

// systemBus is the Bus backed by systemd's D-Bus API (pure Go, godbus).
type systemBus struct{ conn *dbus.Conn }

// NewBus connects to the system systemd instance.
func NewBus(ctx context.Context) (Bus, error) {
	conn, err := dbus.NewWithContext(ctx)
	if err != nil {
		return nil, err
	}
	return &systemBus{conn: conn}, nil
}

func (b *systemBus) Close() { b.conn.Close() }

func (b *systemBus) Units(ctx context.Context) ([]Unit, error) {
	loaded, err := b.conn.ListUnitsByPatternsContext(ctx, nil, []string{"*" + suffix})
	if err != nil {
		return nil, fmt.Errorf("list units: %w", err)
	}
	byName := make(map[string]*Unit, len(loaded))
	for _, s := range loaded {
		byName[s.Name] = &Unit{Name: s.Name, ActiveState: s.ActiveState, SubState: s.SubState}
	}
	files, err := b.conn.ListUnitFilesByPatternsContext(ctx, nil, []string{"*" + suffix})
	if err != nil {
		return nil, fmt.Errorf("list unit files: %w", err)
	}
	for _, f := range files {
		name := filepath.Base(f.Path)
		u, ok := byName[name]
		if !ok {
			u = &Unit{Name: name, ActiveState: "inactive", SubState: "dead"}
			byName[name] = u
		}
		u.Enabled = f.Type == "enabled" || f.Type == "enabled-runtime"
		// Type= is read per unit file; systemd loads an unloaded unit for the
		// query, so a oneshot that already ran and was unloaded is known too.
		if p, err := b.conn.GetServicePropertyContext(ctx, name, "Type"); err == nil {
			u.Type, _ = p.Value.Value().(string)
		}
	}
	out := make([]Unit, 0, len(byName))
	for _, u := range byName {
		out = append(out, *u)
	}
	return out, nil
}

func (b *systemBus) Restarts(ctx context.Context, name string) (uint64, error) {
	p, err := b.conn.GetUnitTypePropertyContext(ctx, name, "Service", "NRestarts")
	if err != nil {
		return 0, err
	}
	switch v := p.Value.Value().(type) {
	case uint32:
		return uint64(v), nil
	case uint64:
		return v, nil
	default:
		return 0, fmt.Errorf("NRestarts of %s has type %T", name, v)
	}
}

func (b *systemBus) Changes(ctx context.Context) (<-chan struct{}, error) {
	if err := b.conn.Subscribe(); err != nil {
		return nil, err
	}
	props := make(chan *dbus.PropertiesUpdate, 64)
	errs := make(chan error, 1)
	b.conn.SetPropertiesSubscriber(props, errs)
	out := make(chan struct{}, 1)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case <-errs:
				// The poll covers a broken subscription.
				return
			case u, ok := <-props:
				if !ok {
					return
				}
				if !strings.HasSuffix(u.UnitName, suffix) {
					continue
				}
				select {
				case out <- struct{}{}:
				default:
				}
			}
		}
	}()
	return out, nil
}
