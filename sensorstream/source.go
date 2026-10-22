// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package sensorstream

import (
	"sort"
	"sync"
	"time"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// RoleOTLP is the role of a process that receives OTLP from senders and
// streams each source to the controller (docs/adr/0022-otlp-sensor-role.md).
// Unlike the data roles it has no RoleSource: what it sends are the OTLP
// sources its receiver holds, one Snapshot per (source, data role), each
// naming its source.
const RoleOTLP = "otlp"

// SourceSnapshot is one OTLP source's latest metrics of one data role.
type SourceSnapshot struct {
	Key, Role       string
	ResourceMetrics []*metricspb.ResourceMetrics
}

// SourceFeed is what the otlp role hands the stream client.
type SourceFeed interface {
	// Pending returns the sources that changed since the previous call, or
	// every source still held when full is set (a new stream starts with the
	// controller knowing none of them).
	Pending(full bool) []SourceSnapshot
}

// SourceBuffer is a SourceFeed fed by the OTLP receiver's Sink. It keeps the
// newest snapshot of each (source, role) until it has been silent for
// retain, so a reconnect can replay all of them.
type SourceBuffer struct {
	retain time.Duration
	now    func() time.Time

	mu      sync.Mutex
	entries map[sourceRole]*sourceEntry
}

type sourceRole struct{ key, role string }

type sourceEntry struct {
	rms   []*metricspb.ResourceMetrics
	at    time.Time
	dirty bool
}

// NewSourceBuffer returns a buffer that forgets a source silent for retain.
func NewSourceBuffer(retain time.Duration) *SourceBuffer {
	return &SourceBuffer{retain: retain, now: time.Now, entries: map[sourceRole]*sourceEntry{}}
}

// Put stores rms as role's latest snapshot of source key. It has the shape of
// otlpreceiver.Sink.
func (b *SourceBuffer) Put(key, role string, rms []*metricspb.ResourceMetrics) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries[sourceRole{key, role}] = &sourceEntry{rms: rms, at: b.now(), dirty: true}
}

// Pending implements SourceFeed. Entries come out sorted by key and role.
func (b *SourceBuffer) Pending(full bool) []SourceSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	var out []SourceSnapshot
	for k, e := range b.entries {
		if now.Sub(e.at) > b.retain {
			delete(b.entries, k)
			continue
		}
		if full || e.dirty {
			e.dirty = false
			out = append(out, SourceSnapshot{Key: k.key, Role: k.role, ResourceMetrics: e.rms})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return out[i].Role < out[j].Role
	})
	return out
}

// Len is the number of (source, role) entries held.
func (b *SourceBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.entries)
}
