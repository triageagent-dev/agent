// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package sensorstream

import (
	"context"
	"errors"
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/triageagent-dev/agent/metricset"
)

type fakeRole struct {
	rms []*metricspb.ResourceMetrics
	err error
}

func (f *fakeRole) Collect(context.Context) ([]*metricspb.ResourceMetrics, error) {
	return f.rms, f.err
}

func gaugeRM(name string, v float64, attrs map[string]string) []*metricspb.ResourceMetrics {
	var kvs []*commonpb.KeyValue
	for k, val := range attrs {
		kvs = append(kvs, &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: val}}})
	}
	return []*metricspb.ResourceMetrics{{ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{
		Name: name,
		Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{
			Value:      &metricspb.NumberDataPoint_AsDouble{AsDouble: v},
			Attributes: kvs,
		}}}},
	}}}}}}
}

func TestLocalServesCollectedSet(t *testing.T) {
	role := &fakeRole{rms: gaugeRM("m", 7, map[string]string{"service.name": "nginx"})}
	l := NewLocal(LocalOptions{Node: "web-01", Roles: map[string]RoleSource{metricset.RoleNodeExporter: role}})
	now := time.Unix(1000, 0)
	l.now = func() time.Time { return now }

	if got := l.Targets(metricset.RoleNodeExporter); len(got) != 0 {
		t.Fatalf("targets before the first collect = %v, want none", got)
	}
	l.CollectOnce(context.Background())

	targets := l.Targets(metricset.RoleNodeExporter)
	if len(targets) != 1 || targets[0] != "sensor/web-01" {
		t.Fatalf("targets = %v, want [sensor/web-01]", targets)
	}
	set, at, err := l.SetAt(metricset.RoleNodeExporter, targets[0])
	if err != nil || !at.Equal(now) {
		t.Fatalf("SetAt = (%v, %v), want received at %v", at, err, now)
	}
	m := set.Get("m")
	if m == nil || len(m.Points) != 1 || m.Points[0].Value != 7 || m.Points[0].Attr("service.name") != "nginx" {
		t.Fatalf("set = %+v, want one point of 7 for nginx", m)
	}
	if got := l.Targets(metricset.RoleEBPF); len(got) != 0 {
		t.Fatalf("a role this host does not run has targets %v", got)
	}
	if _, _, err := l.SetAt(metricset.RoleNodeExporter, "sensor/other"); err == nil {
		t.Fatal("SetAt for another node must fail")
	}
}

func TestLocalSnapshotGoesStale(t *testing.T) {
	l := NewLocal(LocalOptions{Node: "n", Tick: time.Second, Roles: map[string]RoleSource{
		metricset.RoleLogExporter: &fakeRole{rms: gaugeRM("m", 1, nil)},
	}})
	now := time.Unix(1000, 0)
	l.now = func() time.Time { return now }
	l.CollectOnce(context.Background())
	now = now.Add(5 * time.Second) // Stale defaults to four ticks
	if got := l.Targets(metricset.RoleLogExporter); len(got) != 0 {
		t.Fatalf("a stale snapshot still has targets %v", got)
	}
}

func TestLocalFailedRoleKeepsStateAndNoSnapshot(t *testing.T) {
	role := &fakeRole{err: errors.New("boom")}
	l := NewLocal(LocalOptions{Node: "n", Version: "v1", Roles: map[string]RoleSource{metricset.RoleEBPF: role}})
	l.CollectOnce(context.Background())
	if got := l.Targets(metricset.RoleEBPF); len(got) != 0 {
		t.Fatalf("a role that never collected has targets %v", got)
	}
	nodes := l.Nodes()
	if len(nodes) != 1 || nodes[0].Node != "n" || nodes[0].Version != "v1" || !nodes[0].Connected {
		t.Fatalf("nodes = %+v, want one connected node n", nodes)
	}
	roles := nodes[0].State.GetRoles()
	if len(roles) != 1 || roles[0].GetGatherErrors() != 1 || roles[0].GetLastError() != "boom" {
		t.Fatalf("role state = %+v, want one gather error \"boom\"", roles)
	}
	// A later success clears the error.
	role.err, role.rms = nil, gaugeRM("m", 2, nil)
	l.CollectOnce(context.Background())
	if r := l.Nodes()[0].State.GetRoles()[0]; r.GetLastError() != "" || r.GetDataPoints() != 1 {
		t.Fatalf("role state after recovery = %+v, want no error and 1 point", r)
	}
	if len(l.Targets(metricset.RoleEBPF)) != 1 {
		t.Fatal("recovered role must serve its snapshot")
	}
}

func TestLocalNoRolesStillReportsTheHost(t *testing.T) {
	l := NewLocal(LocalOptions{Node: "n"})
	nodes := l.Nodes()
	if len(nodes) != 1 || len(nodes[0].Roles) != 0 {
		t.Fatalf("nodes = %+v, want the host with no roles", nodes)
	}
}

func TestLocalRunStopsOnCancel(t *testing.T) {
	l := NewLocal(LocalOptions{Node: "n", Tick: time.Hour, Roles: map[string]RoleSource{
		metricset.RoleNodeExporter: &fakeRole{rms: gaugeRM("m", 1, nil)},
	}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	deadline := time.After(5 * time.Second)
	for len(l.Targets(metricset.RoleNodeExporter)) == 0 {
		select {
		case <-deadline:
			t.Fatal("Run did not collect once at start")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop on cancel")
	}
}
