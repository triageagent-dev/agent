// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/triageagent-dev/agent/otlpreceiver"
	"github.com/triageagent-dev/agent/sensorstream"
	"github.com/triageagent-dev/agent/sensorstream/sensorpb"
)

func nodeWith(intake ...*sensorpb.OTLPIntake) sensorstream.NodeStatus {
	return sensorstream.NodeStatus{Node: "otlp", Connected: true, State: &sensorpb.SensorState{Intake: intake}}
}

// A sender reaches several replicas of the otlp role and the controller's own
// receiver; each saw a share, so the numbers add, the longest window stands
// and the unread names merge before the top 20 are cut.
func TestOTLPIntakeFromNodes_SumsASenderOverTheReplicas(t *testing.T) {
	// 25 names in all: n00..n19 on the first replica, n10..n24 on the second,
	// so n10..n19 overlap. Points are chosen so the order is not the name order.
	var first, second []*sensorpb.NameCount
	for i := 0; i < 20; i++ {
		first = append(first, &sensorpb.NameCount{Name: fmt.Sprintf("n%02d", i), Points: int64(i + 1)})
	}
	for i := 10; i < 25; i++ {
		second = append(second, &sensorpb.NameCount{Name: fmt.Sprintf("n%02d", i), Points: int64(i + 1)})
	}
	// What a receiver did not count by name outweighs every name, and is
	// still last: it is nothing the operator can filter.
	second = append(second, &sensorpb.NameCount{Name: intakeOther, Points: 10_000})
	nodes := []sensorstream.NodeStatus{
		nodeWith(
			&sensorpb.OTLPIntake{Sender: "obs/collector", WindowSeconds: 60, Exports: 10, Points: 100, Unread: 80, Refused: 5, RefusedExports: 1, Held: 40, Sources: 3, UnreadByName: first},
			&sensorpb.OTLPIntake{Sender: "apps/other", WindowSeconds: 55, Exports: 2, Points: 4, Unread: 1, Held: 2, Sources: 1,
				UnreadByName: []*sensorpb.NameCount{{Name: "only.here", Points: 1}}},
		),
		nodeWith(&sensorpb.OTLPIntake{Sender: "obs/collector", WindowSeconds: 61, Exports: 5, Points: 50, Unread: 30, Refused: 2, RefusedExports: 4, Held: 10, Sources: 2, UnreadByName: second}),
		{Node: "kube-sensor", Connected: true}, // a sensor with no state, or none of the otlp role's
		// A replica that lost its stream: its last window is not summed.
		{Node: "otlp-gone", Connected: false, State: &sensorpb.SensorState{Intake: []*sensorpb.OTLPIntake{
			{Sender: "obs/collector", WindowSeconds: 60, Exports: 100, Points: 1000, Held: 500, Sources: 9},
		}}},
	}
	local := []otlpreceiver.IntakeStatus{{
		Sender: "zeta", Window: 60 * time.Second, Exports: 7, Points: 9, Refused: 9, RefusedExports: 2, Held: 1, Sources: 1,
		UnreadByName: []otlpreceiver.NameCount{{Name: "z.a", Points: 3}},
	}}

	got := otlpIntakeFromNodes(nodes, local)
	if len(got) != 3 || got[0].Sender != "apps/other" || got[1].Sender != "obs/collector" || got[2].Sender != "zeta" {
		t.Fatalf("senders = %+v, want apps/other, obs/collector, zeta", got)
	}

	c := got[1]
	if c.Window != 61 || c.Exports != 15 || c.Points != 150 || c.Unread != 110 || c.Refused != 7 || c.RefusedExports != 5 || c.Held != 50 || c.Sources != 5 {
		t.Errorf("obs/collector = %+v, want the sums with the longest window", c)
	}
	// The names merge by summing; the top 20 stay, most points first and ties by name.
	want := map[string]int64{}
	for _, nc := range append(append([]*sensorpb.NameCount{}, first...), second...) {
		want[nc.Name] += nc.Points
	}
	if len(c.UnreadByName) != intakeTopNames+1 {
		t.Fatalf("%d names, want the top %d and %q last", len(c.UnreadByName), intakeTopNames, intakeOther)
	}
	if last := c.UnreadByName[intakeTopNames]; last.Name != intakeOther || last.Points != 10_000 {
		t.Errorf("last = %+v, want %q with its points", last, intakeOther)
	}
	for i, nc := range c.UnreadByName[:intakeTopNames] {
		if nc.Name == intakeOther {
			t.Errorf("%q ranked among the top names at %d", intakeOther, i)
		}
		if nc.Points != want[nc.Name] {
			t.Errorf("%s = %d points, want %d", nc.Name, nc.Points, want[nc.Name])
		}
		if i > 0 {
			prev := c.UnreadByName[i-1]
			if prev.Points < nc.Points || (prev.Points == nc.Points && prev.Name > nc.Name) {
				t.Errorf("names out of order at %d: %+v before %+v", i, prev, nc)
			}
		}
	}
	// The five dropped names are the smallest of the 25.
	kept := map[string]bool{}
	for _, nc := range c.UnreadByName {
		kept[nc.Name] = true
	}
	for name, points := range want {
		if kept[name] {
			continue
		}
		for _, nc := range c.UnreadByName {
			if points > nc.Points {
				t.Errorf("%s (%d points) was cut while %s (%d) was kept", name, points, nc.Name, nc.Points)
			}
		}
	}
	if c.UnreadByName[0].Name != "n19" || c.UnreadByName[0].Points != 40 {
		t.Errorf("first name = %+v, want n19 with 40 points", c.UnreadByName[0])
	}

	if o := got[0]; o.Window != 55 || o.Exports != 2 || o.Held != 2 || o.Sources != 1 || len(o.UnreadByName) != 1 || o.UnreadByName[0].Name != "only.here" {
		t.Errorf("apps/other = %+v, want its own numbers alone", o)
	}
	if z := got[2]; z.Window != 60 || z.Exports != 7 || z.Refused != 9 || z.RefusedExports != 2 || len(z.UnreadByName) != 1 || z.UnreadByName[0].Points != 3 {
		t.Errorf("zeta = %+v, want the controller's own receiver mapped", z)
	}
}

func TestOTLPIntakeFromNodes_NoIntakeIsNil(t *testing.T) {
	nodes := []sensorstream.NodeStatus{{Node: "n"}, nodeWith()}
	if got := otlpIntakeFromNodes(nodes, nil); got != nil {
		t.Fatalf("intake = %+v, want nil", got)
	}
	if got := otlpIntakeFromNodes(nil, nil); got != nil {
		t.Fatalf("intake = %+v, want nil", got)
	}
}
