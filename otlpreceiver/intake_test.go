// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// gauge is a metric of n points, told apart by an attribute.
func gauge(name string, ts uint64, n int) *metricspb.Metric {
	g := &metricspb.Gauge{}
	for i := range n {
		g.DataPoints = append(g.DataPoints, &metricspb.NumberDataPoint{
			Attributes:   []*commonpb.KeyValue{strAttr("i", fmt.Sprint(i))},
			TimeUnixNano: ts,
			Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 1},
		})
	}
	return &metricspb.Metric{Name: name, Data: &metricspb.Metric_Gauge{Gauge: g}}
}

// intakeReceiver is a receiver that reads what the collectors read, on a
// clock the test moves.
func intakeReceiver() (*Receiver, *time.Time) {
	now := time.Unix(1_800_000_000, 0)
	r := New(Options{Auth: okAuth{}, Log: logr.Discard(), Wanted: ReadByCollectors, now: func() time.Time { return now }})
	return r, &now
}

func intakeOf(t *testing.T, r *Receiver, sender string) (IntakeStatus, bool) {
	t.Helper()
	for _, in := range r.Intake() {
		if in.Sender == sender {
			return in, true
		}
	}
	return IntakeStatus{}, false
}

// The ledger counts per sender what it offered in the minute: exports and
// points, the points nobody reads by metric name, the points refused, and
// what the receiver holds for the sender now. The numbers are those of the
// closed window.
func TestIntake_CountsPerSender(t *testing.T) {
	r, now := intakeReceiver()
	shop := resource("service.name", "cart", "k8s.namespace.name", "shop")
	for ts := uint64(10); ts <= 20; ts += 10 {
		export(t, r, authed(), shop,
			counter("log.errors", ts, 1),
			gauge("foo.a", ts, 3), gauge("foo.b", ts, 2), gauge("foo.c", ts, 1))
	}
	// Sender zeta names no namespace, so its points are refused.
	b := Principal{Name: "zeta", Forwarder: true}
	rejected, _ := r.accept(b, []*metricspb.ResourceMetrics{{
		Resource:     resource("service.name", "orphan"),
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{gauge("log.errors", 10, 2)}}},
	}})
	if rejected != 2 {
		t.Fatalf("sender zeta: rejected = %d, want 2", rejected)
	}

	a := Principal{Namespace: "obs", Name: "otel-collector"}.String()
	if in, ok := intakeOf(t, r, a); !ok || in.Exports != 0 || in.Points != 0 || in.Unread != 0 || in.Window != 0 {
		t.Fatalf("before the window closed: %+v (found %v), want no windowed numbers", in, ok)
	}

	*now = now.Add(time.Minute)
	r.logSummary()
	got := r.Intake()
	if len(got) != 2 || got[0].Sender != a || got[1].Sender != "zeta" {
		t.Fatalf("intake = %+v, want the two senders sorted", got)
	}
	ia, ib := got[0], got[1]
	if ia.Window != time.Minute {
		t.Errorf("window = %v, want the minute the clock advanced", ia.Window)
	}
	if ia.Exports != 2 || ia.Points != 14 || ia.Unread != 12 || ia.Refused != 0 || ia.RefusedExports != 0 {
		t.Errorf("sender a: %+v, want 2 exports, 14 points, 12 unread, nothing refused", ia)
	}
	wantNames := []NameCount{{"foo.a", 6}, {"foo.b", 4}, {"foo.c", 2}}
	if fmt.Sprint(ia.UnreadByName) != fmt.Sprint(wantNames) {
		t.Errorf("unread by name = %v, want %v", ia.UnreadByName, wantNames)
	}
	if ia.Held != 1 || ia.Sources != 1 {
		t.Errorf("sender a holds %d entries of %d sources, want 1 and 1 (one cumulative series)", ia.Held, ia.Sources)
	}
	if ib.Exports != 1 || ib.Points != 2 || ib.Refused != ib.Points || ib.Held != 0 || ib.Sources != 0 || ib.Unread != 0 {
		t.Errorf("sender zeta: %+v, want 1 export of 2 points, all refused, nothing held", ib)
	}

	// A quiet minute: the numbers are zero, what is held is not, and a
	// sender with nothing in its window and no sources is forgotten.
	*now = now.Add(time.Minute)
	r.logSummary()
	if ia, ok := intakeOf(t, r, a); !ok || ia.Exports != 0 || ia.Points != 0 || ia.Unread != 0 || len(ia.UnreadByName) != 0 || ia.Held != 1 || ia.Sources != 1 {
		t.Errorf("sender a after a quiet window: %+v (found %v), want zeroes with 1 held and 1 source", ia, ok)
	}
	if _, ok := intakeOf(t, r, "zeta"); ok {
		t.Error("sender zeta, with nothing in its window and no sources, was kept")
	}
}

// A sender can invent metric names without end; the ledger counts 64 of them
// and the rest together.
func TestIntake_BoundsNamesPerSender(t *testing.T) {
	r, now := intakeReceiver()
	var ms []*metricspb.Metric
	for i := range 70 {
		ms = append(ms, gauge(fmt.Sprintf("foo.%02d", i), 10, 1))
	}
	export(t, r, authed(), resource("service.name", "cart", "k8s.namespace.name", "shop"), ms...)
	*now = now.Add(time.Minute)
	r.logSummary()
	in, ok := intakeOf(t, r, Principal{Namespace: "obs", Name: "otel-collector"}.String())
	if !ok || in.Unread != 70 {
		t.Fatalf("intake = %+v (found %v), want 70 unread", in, ok)
	}
	if len(in.UnreadByName) != maxIntakeNames+1 {
		t.Fatalf("%d names, want %d and the rest as one", len(in.UnreadByName), maxIntakeNames)
	}
	if other := in.UnreadByName[0]; other.Name != intakeOther || other.Points != 6 {
		t.Errorf("first entry = %+v, want the 6 points past the bound as %q", other, intakeOther)
	}
	for _, nc := range in.UnreadByName[1:] {
		if nc.Points != 1 || nc.Name == intakeOther {
			t.Errorf("named entry %+v, want 1 point", nc)
		}
	}
}

// What the receiver holds for a sender follows its sources: it is counted as
// the entries are made and given back when the sweep forgets them.
func TestIntake_HeldFollowsTheSendersEntries(t *testing.T) {
	r, now := intakeReceiver()
	var ms []*metricspb.Metric
	for i := range 5 {
		ms = append(ms, counter("log.errors", 10, 1, strAttr("k", fmt.Sprint(i))))
	}
	export(t, r, authed(), resource("service.name", "cart", "k8s.namespace.name", "shop"), ms...)
	*now = now.Add(time.Minute)
	r.logSummary()
	sender := Principal{Namespace: "obs", Name: "otel-collector"}.String()
	if in, _ := intakeOf(t, r, sender); in.Held != 5 || in.Sources != 1 {
		t.Fatalf("intake = %+v, want 5 held and 1 source", in)
	}

	*now = now.Add(r.opts.Expire + time.Minute)
	r.flush()
	in, ok := intakeOf(t, r, sender)
	if !ok || in.Held != 0 || in.Sources != 0 {
		t.Fatalf("after the sweep: %+v (found %v), want nothing held", in, ok)
	}
	r.logSummary()
	if _, ok := intakeOf(t, r, sender); ok {
		t.Error("a sender with nothing in its window and no sources was kept")
	}
}

// A full table takes no new sender: its exports are still read, and counted
// nowhere.
func TestIntake_FullSenderTableTakesNoNewSender(t *testing.T) {
	r, now := intakeReceiver()
	for i := range maxSenders {
		r.intake[fmt.Sprintf("s%d", i)] = &intake{}
	}
	export(t, r, authed(), resource("service.name", "cart", "k8s.namespace.name", "shop"), counter("log.errors", 10, 1), gauge("foo.a", 10, 2))
	sender := Principal{Namespace: "obs", Name: "otel-collector"}.String()
	if _, ok := intakeOf(t, r, sender); ok {
		t.Fatal("a new sender took a slot in a full table")
	}
	if len(r.intake) != maxSenders {
		t.Fatalf("the table has %d entries, want %d", len(r.intake), maxSenders)
	}
	*now = now.Add(time.Minute)
	r.logSummary()
	if _, ok := intakeOf(t, r, sender); ok {
		t.Fatal("a sender past the table's bound was counted")
	}
	if r.Held() != 1 {
		t.Errorf("held = %d, want the export read all the same", r.Held())
	}
}
