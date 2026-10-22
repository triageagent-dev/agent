// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package sensorstream

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/sensorstream/sensorpb"
)

// Local runs the sensor roles inside the agent's own process (docs/adr/0018-
// linux-host-mode.md) and serves their latest metrics as a
// metricset.TimedSource, so the RED, USE and LOG collectors read them exactly
// as they read the Hub. There is one node, the host, and no stream: every Tick
// it calls each role's Collect, parses the OTLP with metricset.FromOTLP and
// keeps the newest set per role.
type Local struct {
	node    string
	version string
	roles   map[string]RoleSource
	names   []string
	tick    time.Duration
	stale   time.Duration
	now     func() time.Time

	mu        sync.RWMutex
	snapshots map[string]localSnapshot
	stats     map[string]*sensorpb.RoleState
	lastSeen  time.Time
}

type localSnapshot struct {
	set metricset.Set
	at  time.Time
}

// LocalOptions configures a Local.
type LocalOptions struct {
	// Node names the host; it becomes the target "sensor/<node>".
	Node    string
	Version string
	// Roles are the sensors this process runs, by role name.
	Roles map[string]RoleSource
	// Tick is how often every role is collected. Default 15s.
	Tick time.Duration
	// Stale is how long a snapshot stays served after its collect. Default
	// four ticks.
	Stale time.Duration
}

// NewLocal returns a Local over o.Roles. It serves nothing until Run has
// collected once.
func NewLocal(o LocalOptions) *Local {
	if o.Tick <= 0 {
		o.Tick = 15 * time.Second
	}
	if o.Stale <= 0 {
		o.Stale = 4 * o.Tick
	}
	names := make([]string, 0, len(o.Roles))
	for r := range o.Roles {
		names = append(names, r)
	}
	sort.Strings(names)
	return &Local{
		node: o.Node, version: o.Version, roles: o.Roles, names: names,
		tick: o.Tick, stale: o.Stale, now: time.Now,
		snapshots: map[string]localSnapshot{}, stats: map[string]*sensorpb.RoleState{},
	}
}

var _ metricset.TimedSource = (*Local)(nil)

// Run collects every role at once and then every Tick until ctx ends.
func (l *Local) Run(ctx context.Context) error {
	t := time.NewTicker(l.tick)
	defer t.Stop()
	for {
		l.CollectOnce(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// CollectOnce collects every role once. A role that fails keeps its previous
// snapshot, which goes stale on its own, and records the error in its state.
func (l *Local) CollectOnce(ctx context.Context) {
	for _, role := range l.names {
		start := l.now()
		rms, err := l.roles[role].Collect(ctx)
		l.record(role, rms, err, l.now().Sub(start))
	}
}

func (l *Local) record(role string, rms []*metricspb.ResourceMetrics, err error, took time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	st := l.stats[role]
	if st == nil {
		st = &sensorpb.RoleState{Role: role}
		l.stats[role] = st
	}
	st.GatherMicros = took.Microseconds()
	if err != nil {
		st.GatherErrors++
		st.LastError = err.Error()
		return
	}
	var metrics, points uint32
	for _, rm := range rms {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				metrics++
				points += dataPoints(m)
			}
		}
	}
	st.Metrics, st.DataPoints, st.LastError = metrics, points, ""
	if rs, ok := l.roles[role].(RoleStatus); ok {
		if msg := rs.StatusError(); msg != "" {
			st.LastError = msg
		}
	}
	now := l.now()
	l.snapshots[role] = localSnapshot{set: metricset.FromOTLP(rms), at: now}
	l.lastSeen = now
}

// Targets implements metricset.Source: the host's target when role has a
// snapshot no older than Stale.
func (l *Local) Targets(role string) []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if s, ok := l.snapshots[role]; ok && l.now().Sub(s.at) <= l.stale {
		return []string{TargetPrefix + l.node}
	}
	return nil
}

// Set implements metricset.Source.
func (l *Local) Set(role, target string) (metricset.Set, error) {
	set, _, err := l.SetAt(role, target)
	return set, err
}

// SetAt implements metricset.TimedSource.
func (l *Local) SetAt(role, target string) (metricset.Set, time.Time, error) {
	if target != TargetPrefix+l.node {
		return nil, time.Time{}, fmt.Errorf("no sensor %s", target)
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	s, ok := l.snapshots[role]
	if !ok {
		return nil, time.Time{}, fmt.Errorf("local sensors have no %s snapshot", role)
	}
	return s.set, s.at, nil
}

// Nodes returns the host as the one sensor node, in the shape the Hub reports,
// so the same sensor_state mapping serves both. It is always connected: the
// roles are this process.
func (l *Local) Nodes() []NodeStatus {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	state := &sensorpb.SensorState{
		Goroutines: uint32(runtime.NumGoroutine()),
		HeapBytes:  ms.HeapAlloc,
		SysBytes:   ms.Sys,
		CpuSeconds: cpuSeconds(),
	}
	for _, r := range l.names {
		if st := l.stats[r]; st != nil {
			state.Roles = append(state.Roles, &sensorpb.RoleState{
				Role: st.Role, GatherMicros: st.GatherMicros, Metrics: st.Metrics,
				DataPoints: st.DataPoints, GatherErrors: st.GatherErrors, LastError: st.LastError,
			})
		}
	}
	return []NodeStatus{{
		Node: l.node, Version: l.version, Roles: append([]string(nil), l.names...),
		Connected: true, LastSeen: l.lastSeen, State: state,
	}}
}
