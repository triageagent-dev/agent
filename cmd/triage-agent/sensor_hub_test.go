// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"
	"time"

	"github.com/triageagent-dev/agent/push"
	"github.com/triageagent-dev/agent/sensorstream"
	"github.com/triageagent-dev/agent/sensorstream/sensorpb"
)

// TestSensorStatusesFromNodes_MapsEveryField guards the ADR-0015 phase-1
// mapping from sensorstream.Hub.Nodes() onto the wire's []push.SensorStatus:
// every field on a node with a full SensorState report must survive, under
// the wire's names.
func TestSensorStatusesFromNodes_MapsEveryField(t *testing.T) {
	lastSeen := time.Unix(1720000060, 0)
	nodes := []sensorstream.NodeStatus{
		{
			Node: "node-1", Pod: "sensor-node-1", Version: "1.4.0",
			Roles: []string{"ebpf"}, Connected: true, LastSeen: lastSeen, Gaps: 2,
			State: &sensorpb.SensorState{
				Goroutines: 40, HeapBytes: 1024, SysBytes: 2048, CpuSeconds: 3.5,
				Reconnects: 1, SendErrors: 5, SendMicros: 900, AckRttMicros: 1500,
				TickBytes: 4096, ConnectMicros: 250,
				Roles: []*sensorpb.RoleState{
					{Role: "ebpf", GatherMicros: 800, Metrics: 12, DataPoints: 120, GatherErrors: 1, LastError: "read: closed"},
				},
			},
		},
	}

	got := sensorStatusesFromNodes(nodes)
	if len(got) != 1 {
		t.Fatalf("got %d sensors, want 1", len(got))
	}
	s := got[0]
	want := push.SensorStatus{
		Node: "node-1", Pod: "sensor-node-1", Version: "1.4.0",
		Connected: true, LastSeen: 1720000060, Gaps: 2,
		Goroutines: 40, HeapBytes: 1024, SysBytes: 2048, CPUSeconds: 3.5,
		Reconnects: 1, SendErrors: 5, SendMicros: 900, AckRTTMicros: 1500,
		TickBytes: 4096, ConnectMicros: 250,
		Roles: []push.SensorRoleStatus{
			{Role: "ebpf", GatherMicros: 800, Metrics: 12, DataPoints: 120, GatherErrors: 1, LastError: "read: closed"},
		},
	}
	if s.Node != want.Node || s.Pod != want.Pod || s.Version != want.Version ||
		s.Connected != want.Connected || s.LastSeen != want.LastSeen || s.Gaps != want.Gaps ||
		s.Goroutines != want.Goroutines || s.HeapBytes != want.HeapBytes || s.SysBytes != want.SysBytes ||
		s.CPUSeconds != want.CPUSeconds || s.Reconnects != want.Reconnects || s.SendErrors != want.SendErrors ||
		s.SendMicros != want.SendMicros || s.AckRTTMicros != want.AckRTTMicros || s.TickBytes != want.TickBytes ||
		s.ConnectMicros != want.ConnectMicros {
		t.Errorf("got = %+v, want %+v", s, want)
	}
	if len(s.Roles) != 1 || s.Roles[0] != want.Roles[0] {
		t.Errorf("roles = %+v, want %+v", s.Roles, want.Roles)
	}
}

// TestSensorStatusesFromNodes_HelloRoleWithNoStateGetsRoleOnly covers a
// sensor that has sent its Hello (declaring the roles it runs) but not yet a
// SensorState — and a sensor mid-run whose Hello named a role that hasn't
// reported a RoleState this tick. Either way, the role still gets an entry,
// with only Role set, rather than being left off the list.
func TestSensorStatusesFromNodes_HelloRoleWithNoStateGetsRoleOnly(t *testing.T) {
	nodes := []sensorstream.NodeStatus{
		{Node: "node-1", Pod: "sensor-node-1", Roles: []string{"ebpf", "nodeexporter"}, Connected: true, State: nil},
	}
	got := sensorStatusesFromNodes(nodes)
	if len(got) != 1 {
		t.Fatalf("got %d sensors, want 1", len(got))
	}
	want := []push.SensorRoleStatus{{Role: "ebpf"}, {Role: "nodeexporter"}}
	if len(got[0].Roles) != len(want) {
		t.Fatalf("roles = %+v, want %+v", got[0].Roles, want)
	}
	for i, r := range got[0].Roles {
		if r != want[i] {
			t.Errorf("roles[%d] = %+v, want %+v (state-less role must carry only Role)", i, r, want[i])
		}
	}

	// Mid-run: ebpf has reported a RoleState this tick, nodeexporter hasn't.
	nodes[0].State = &sensorpb.SensorState{
		Roles: []*sensorpb.RoleState{{Role: "ebpf", GatherMicros: 500, Metrics: 3}},
	}
	got = sensorStatusesFromNodes(nodes)
	wantMixed := []push.SensorRoleStatus{
		{Role: "ebpf", GatherMicros: 500, Metrics: 3},
		{Role: "nodeexporter"},
	}
	if len(got[0].Roles) != len(wantMixed) {
		t.Fatalf("roles = %+v, want %+v", got[0].Roles, wantMixed)
	}
	for i, r := range got[0].Roles {
		if r != wantMixed[i] {
			t.Errorf("roles[%d] = %+v, want %+v", i, r, wantMixed[i])
		}
	}
}

// TestSensorStatusesFromNodes_Empty guards the loop caller's contract: an
// empty hub (nothing connected yet) maps to an empty, non-nil slice, which
// SendSensorState then treats as nothing to send.
func TestSensorStatusesFromNodes_Empty(t *testing.T) {
	got := sensorStatusesFromNodes(nil)
	if len(got) != 0 {
		t.Errorf("got %d sensors, want 0", len(got))
	}
}

func TestSplitNonEmpty(t *testing.T) {
	got := splitNonEmpty(" a.example ,, b.example,")
	if len(got) != 2 || got[0] != "a.example" || got[1] != "b.example" {
		t.Fatalf("got %q", got)
	}
	if splitNonEmpty("") != nil {
		t.Fatal("empty input gave names")
	}
}
