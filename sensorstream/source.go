// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package sensorstream

import (
	"sync"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// RoleOTLP is the role of a process that receives OTLP from senders and
// streams each source to the controller (docs/adr/0022-otlp-sensor-role.md).
// Unlike the data roles it has no RoleSource: what it sends are the exports
// its receiver translated, one Snapshot per (source, data role), each
// naming its source.
const RoleOTLP = "otlp"

// SourceSnapshot is one export's metrics of one data role for one OTLP
// source.
type SourceSnapshot struct {
	Key, Role       string
	ResourceMetrics []*metricspb.ResourceMetrics
}

// SourceFeed is what the otlp role hands the stream client.
type SourceFeed interface {
	// Pending returns what was queued since the previous call, in arrival
	// order, and forgets it. full is accepted for the client's first tick
	// of a stream and changes nothing: nothing is held to replay, a source
	// exports again within its own interval.
	Pending(full bool) []SourceSnapshot
	// Ready is signalled when something is queued, so the client sends
	// without waiting for its tick.
	Ready() <-chan struct{}
}

// SourceBuffer is a SourceFeed fed by the OTLP receiver's Sink: a queue of
// what arrived and was not yet sent. It holds nothing else, and no longer
// than the stream takes to send it; while the stream is down it keeps the
// newest maxPoints points and drops the oldest exports past that, counted
// in Dropped.
type SourceBuffer struct {
	maxPoints int

	mu      sync.Mutex
	queue   []SourceSnapshot
	points  int
	dropped uint64
	ready   chan struct{}
}

// NewSourceBuffer returns a queue that holds at most maxPoints points.
func NewSourceBuffer(maxPoints int) *SourceBuffer {
	return &SourceBuffer{maxPoints: maxPoints, ready: make(chan struct{}, 1)}
}

// Put queues rms as role's export for source key. It has the shape of
// otlpreceiver.Sink.
func (b *SourceBuffer) Put(key, role string, rms []*metricspb.ResourceMetrics) {
	n := countPoints(rms)
	b.mu.Lock()
	b.queue = append(b.queue, SourceSnapshot{Key: key, Role: role, ResourceMetrics: rms})
	b.points += n
	for len(b.queue) > 1 && b.points > b.maxPoints {
		b.points -= countPoints(b.queue[0].ResourceMetrics)
		b.queue[0] = SourceSnapshot{}
		b.queue = b.queue[1:]
		b.dropped++
	}
	b.mu.Unlock()
	select {
	case b.ready <- struct{}{}:
	default:
	}
}

// Pending implements SourceFeed.
func (b *SourceBuffer) Pending(bool) []SourceSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.queue
	b.queue, b.points = nil, 0
	return out
}

// Ready implements SourceFeed.
func (b *SourceBuffer) Ready() <-chan struct{} { return b.ready }

// Len is the number of exports queued.
func (b *SourceBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue)
}

// Dropped is how many queued exports were dropped unsent because the
// queue was full while the stream was down.
func (b *SourceBuffer) Dropped() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dropped
}

func countPoints(rms []*metricspb.ResourceMetrics) int {
	n := 0
	for _, rm := range rms {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				n += int(dataPoints(m))
			}
		}
	}
	return n
}
