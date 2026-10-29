// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"sort"

	"github.com/triageagent-dev/agent/otlpreceiver"
	"github.com/triageagent-dev/agent/push"
	"github.com/triageagent-dev/agent/sensorstream"
	"github.com/triageagent-dev/agent/sensorstream/sensorpb"
)

// intakeTopNames is how many unread metric names per sender the sensor_state
// frame carries: the receivers count 64 each, and the operator acts on the
// few that carry the points.
const intakeTopNames = 20

// intakeProto maps a receiver's intake onto the sensor stream's message. It
// lives here because otlpreceiver does not import sensorpb.
func intakeProto(in []otlpreceiver.IntakeStatus) []*sensorpb.OTLPIntake {
	if len(in) == 0 {
		return nil
	}
	out := make([]*sensorpb.OTLPIntake, 0, len(in))
	for _, s := range in {
		m := &sensorpb.OTLPIntake{
			Sender:         s.Sender,
			WindowSeconds:  int64(s.Window.Seconds()),
			Exports:        s.Exports,
			Points:         s.Points,
			Unread:         s.Unread,
			Refused:        s.Refused,
			RefusedExports: s.RefusedExports,
			Held:           s.Held,
			Sources:        int32(s.Sources),
		}
		for _, nc := range s.UnreadByName {
			m.UnreadByName = append(m.UnreadByName, &sensorpb.NameCount{Name: nc.Name, Points: nc.Points})
		}
		out = append(out, m)
	}
	return out
}

// otlpIntakeFromNodes sums the intake per sender over what every otlp-role
// replica reported in its SensorState and over local, the controller's own
// receiver (nil when it runs none). A sender reaches several replicas, each
// of which saw a share of its exports, so the numbers add; the window is the
// longest one reported, since the replicas close theirs at different moments.
// A replica that lost its stream is left out: its last report is of a window
// before it went, and its replacement reports the same senders. The unread
// names are merged by name and cut to the top intakeTopNames. Nil when there
// is nothing to report.
func otlpIntakeFromNodes(nodes []sensorstream.NodeStatus, local []otlpreceiver.IntakeStatus) []push.OTLPIntakeStatus {
	type sum struct {
		push.OTLPIntakeStatus
		names map[string]int64
	}
	bySender := map[string]*sum{}
	add := func(in *sensorpb.OTLPIntake) {
		s := bySender[in.GetSender()]
		if s == nil {
			s = &sum{OTLPIntakeStatus: push.OTLPIntakeStatus{Sender: in.GetSender()}, names: map[string]int64{}}
			bySender[in.GetSender()] = s
		}
		s.Window = max(s.Window, in.GetWindowSeconds())
		s.Exports += in.GetExports()
		s.Points += in.GetPoints()
		s.Unread += in.GetUnread()
		s.Refused += in.GetRefused()
		s.RefusedExports += in.GetRefusedExports()
		s.Held += in.GetHeld()
		s.Sources += in.GetSources()
		for _, nc := range in.GetUnreadByName() {
			s.names[nc.GetName()] += nc.GetPoints()
		}
	}
	for _, n := range nodes {
		if !n.Connected {
			continue
		}
		for _, in := range n.State.GetIntake() {
			add(in)
		}
	}
	for _, in := range intakeProto(local) {
		add(in)
	}
	if len(bySender) == 0 {
		return nil
	}
	out := make([]push.OTLPIntakeStatus, 0, len(bySender))
	for _, s := range bySender {
		for name, points := range s.names {
			s.UnreadByName = append(s.UnreadByName, push.NameCount{Name: name, Points: points})
		}
		sort.Slice(s.UnreadByName, func(i, j int) bool {
			a, b := s.UnreadByName[i], s.UnreadByName[j]
			if a.Points != b.Points {
				return a.Points > b.Points
			}
			return a.Name < b.Name
		})
		if len(s.UnreadByName) > intakeTopNames {
			s.UnreadByName = s.UnreadByName[:intakeTopNames]
		}
		out = append(out, s.OTLPIntakeStatus)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Sender < out[j].Sender })
	return out
}
