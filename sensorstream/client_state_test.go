// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package sensorstream

import (
	"testing"

	"github.com/triageagent-dev/agent/sensorstream/sensorpb"
)

// TestClientState_CarriesTheIntake: the otlp role's intake option is read at
// every SensorState; a client without it sends none.
func TestClientState_CarriesTheIntake(t *testing.T) {
	want := []*sensorpb.OTLPIntake{{Sender: "obs/collector", Exports: 3, UnreadByName: []*sensorpb.NameCount{{Name: "foo.a", Points: 7}}}}
	c := &client{
		opts:      ClientOptions{Intake: func() []*sensorpb.OTLPIntake { return want }},
		roleStats: map[string]*sensorpb.RoleState{},
	}
	got := c.state().GetIntake()
	if len(got) != 1 || got[0].GetSender() != "obs/collector" || got[0].GetExports() != 3 || got[0].GetUnreadByName()[0].GetPoints() != 7 {
		t.Fatalf("intake = %v, want %v", got, want)
	}
	plain := &client{roleStats: map[string]*sensorpb.RoleState{}}
	if got := plain.state().GetIntake(); len(got) != 0 {
		t.Fatalf("a client without the option sent intake %v", got)
	}
}
