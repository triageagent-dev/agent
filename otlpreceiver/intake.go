// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"sort"
	"time"
)

// The intake ledger answers, per sender, what the receiver was offered and
// what it did with it: the exports and points of the last closed window, the
// points no collector reads by metric name (what the operator can filter out
// at the Collector), the points and exports refused, and what the receiver
// holds for the sender now. A window closes with the summary line, so the
// numbers are the last minute's, not a running total, and a sender that went
// quiet shows zeroes until its entry is forgotten.

const (
	// maxIntakeNames bounds the metric names counted per sender in a window:
	// a sender can invent names without end, and the ledger must not grow
	// with them. The rest is counted under intakeOther. A Collector that
	// sends everything it has sends a few hundred names, and the slots go
	// to the first names of the window, so the bound is well above that:
	// a name folded into intakeOther is one the operator cannot act on.
	maxIntakeNames = 1024
	// intakeOther is the name the unread points past maxIntakeNames are
	// counted under.
	intakeOther = "other"
)

// NameCount is the points of one metric name.
type NameCount struct {
	Name   string
	Points int64
}

// IntakeStatus is what one sender offered the receiver in its last closed
// window, and what the receiver holds for it now.
type IntakeStatus struct {
	Sender string
	Window time.Duration
	// Exports and Points are what the sender offered (a point is also a span
	// or a log record). Unread are the points dropped at intake because no
	// collector reads their metric, Refused the points refused, which are
	// inside Points. RefusedExports are the exports refused before they were
	// read (backpressure); they are outside Exports and Points.
	Exports, Points, Unread, Refused, RefusedExports int64

	// Held is the state entries held for the sender's sources now, Sources
	// the sources: neither is windowed.
	Held    int64
	Sources int

	// UnreadByName counts the unread points of the window by metric name,
	// descending by points then by name. Past maxIntakeNames names the rest is
	// one entry named intakeOther, last whatever its count: it names nothing
	// the operator can filter.
	UnreadByName []NameCount
}

// intakeCounts are the numbers of one window.
type intakeCounts struct {
	exports, points, unread, refused, refusedExports int64

	byName map[string]int64
}

// intake is one sender's window being filled and the last one closed.
type intake struct {
	cur, last  intakeCounts
	lastWindow time.Duration
}

func (c *intakeCounts) zero() bool {
	return c.exports == 0 && c.points == 0 && c.unread == 0 && c.refused == 0 && c.refusedExports == 0
}

// intakeFor returns the sender's entry, creating it while the table has room.
// For a new sender past maxSenders it returns an entry that is not stored, so
// callers never test for nil and a flood of senders cannot grow the table.
// The caller holds r.mu.
func (r *Receiver) intakeFor(sender string) *intake {
	if in := r.intake[sender]; in != nil {
		return in
	}
	in := &intake{}
	if len(r.intake) < maxSenders {
		r.intake[sender] = in
	}
	return in
}

// unreadName counts n unread points of the metric name, under intakeOther
// once maxIntakeNames other names are counted.
func (c *intakeCounts) unreadName(name string, n int) {
	c.unread += int64(n)
	if c.byName == nil {
		c.byName = map[string]int64{}
	}
	if _, ok := c.byName[name]; !ok && c.named() >= maxIntakeNames {
		name = intakeOther
	}
	c.byName[name] += int64(n)
}

// named is the names counted, not counting intakeOther.
func (c *intakeCounts) named() int {
	n := len(c.byName)
	if _, ok := c.byName[intakeOther]; ok {
		n--
	}
	return n
}

// rollIntake closes the window: what was counted becomes the last window and
// counting starts again. An entry with nothing in its closed window and no
// sources is forgotten, so senders that came and went keep no slot. The
// caller holds r.mu.
func (r *Receiver) rollIntake(now time.Time) {
	window := now.Sub(r.intakeRolled)
	for sender, in := range r.intake {
		in.last, in.cur = in.cur, intakeCounts{}
		in.lastWindow = window
		if in.last.zero() && r.bySender[sender] == 0 {
			delete(r.intake, sender)
		}
	}
	r.intakeRolled = now
}

// Intake returns, per sender, what it offered in the last closed window and
// what the receiver holds for it now, sorted by sender. A sender whose last
// window is empty but who still has sources is included.
func (r *Receiver) Intake() []IntakeStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	held := map[string]int64{}
	for _, s := range r.sources {
		held[s.sender] += int64(s.held)
	}
	out := make([]IntakeStatus, 0, len(r.intake))
	for sender, in := range r.intake {
		st := IntakeStatus{
			Sender: sender, Window: in.lastWindow,
			Exports: in.last.exports, Points: in.last.points, Unread: in.last.unread,
			Refused: in.last.refused, RefusedExports: in.last.refusedExports,
			Held: held[sender], Sources: r.bySender[sender],
		}
		for name, n := range in.last.byName {
			st.UnreadByName = append(st.UnreadByName, NameCount{Name: name, Points: n})
		}
		sort.Slice(st.UnreadByName, func(i, j int) bool {
			a, b := st.UnreadByName[i], st.UnreadByName[j]
			if (a.Name == intakeOther) != (b.Name == intakeOther) {
				return b.Name == intakeOther
			}
			if a.Points != b.Points {
				return a.Points > b.Points
			}
			return a.Name < b.Name
		})
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Sender < out[j].Sender })
	return out
}
