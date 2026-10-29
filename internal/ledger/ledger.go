// Package ledger counts what each stage of the signal path did with what a
// sensor (or an OTLP sender) gave it, so data that is read and then not sent
// shows up as a number instead of as silence. It is a process-wide set of
// monotonic counters keyed by (role, source, outcome): role is the sensor
// role (ebpf, node-exporter, log-exporter, otlp, ...), source the sensor node
// or the OTLP sender, outcome what happened. The unit of an outcome is the
// collector's own (a service for the logs and RED roles, a pod for USE) and
// Sent means handed to the push sender, which can still drop it for a tenant
// or a full queue (the core link's own counters show that).
package ledger

import (
	"sort"
	"sync"
	"time"
)

// Outcomes. A collector adds the ones that apply to its role.
const (
	Ticks        = "ticks"        // reads of the source's snapshot
	NoSnapshot   = "no_snapshot"  // reads that found no snapshot to read
	Points       = "points"       // data points in the snapshots read
	Unattributed = "unattributed" // points that named no workload
	Filtered     = "filtered"     // entries outside the namespace scope
	Services     = "services"     // services the source reported on
	Gated        = "gated"        // services read but below every gate
	Sent         = "sent"         // services in a frame handed to the push queue
	Refused      = "refused"      // points an OTLP sender sent that were not accepted
	Unread       = "unread"       // points an OTLP sender sent that no collector reads
	Capped       = "capped"       // services cut by a frame's size cap

	RefusedExports = "refused_exports" // exports an OTLP sender sent that were refused before being read
)

// maxDrops bounds the drop records a ledger keeps; the oldest-seen go first.
const maxDrops = 64

// All is the source name of a collector's own totals across its sources.
const All = "*"

type key struct{ role, source string }

type dropKey struct{ frame, subject, reason string }

// Ledger is a set of counters and the entries held back by name. The zero
// value is ready to use.
type Ledger struct {
	mu    sync.Mutex
	m     map[key]map[string]int64
	drops map[dropKey]*Drop
}

// Drop is something kept out of a frame, by name, so a service that is read
// but never reaches the core can be found without reading the core's log.
// Frame is the frame kind, Subject what was dropped ("<namespace>/<service>",
// an edge as "<src> -> <dst>") and Reason why.
type Drop struct {
	Frame   string `json:"frame"`
	Subject string `json:"subject"`
	Reason  string `json:"reason"`
	Count   int64  `json:"count"`
	// Last is when it was last dropped, in unix seconds.
	Last int64 `json:"last"`
}

// Default is the process-wide ledger the collectors write to.
var Default = &Ledger{}

// Add adds n to the outcome's counter for the role and source.
func (l *Ledger) Add(role, source, outcome string, n int64) {
	if n == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.m == nil {
		l.m = map[key]map[string]int64{}
	}
	k := key{role, source}
	c := l.m[k]
	if c == nil {
		c = map[string]int64{}
		l.m[k] = c
	}
	c[outcome] += n
}

// Drop records that subject was left out of a frame of the given kind. A
// repeat adds to the same record; past maxDrops the least recently dropped
// record makes room.
func (l *Ledger) Drop(frame, subject, reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.drops == nil {
		l.drops = map[dropKey]*Drop{}
	}
	k := dropKey{frame, subject, reason}
	d := l.drops[k]
	if d == nil {
		if len(l.drops) >= maxDrops {
			var oldest dropKey
			var at int64 = 1<<63 - 1
			for ok, od := range l.drops {
				if od.Last < at {
					oldest, at = ok, od.Last
				}
			}
			delete(l.drops, oldest)
		}
		d = &Drop{Frame: frame, Subject: subject, Reason: reason}
		l.drops[k] = d
	}
	d.Count++
	d.Last = time.Now().Unix()
}

// Drops returns the drop records, the most recently dropped first.
func (l *Ledger) Drops() []Drop {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Drop, 0, len(l.drops))
	for _, d := range l.drops {
		out = append(out, *d)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Last != out[j].Last {
			return out[i].Last > out[j].Last
		}
		if out[i].Frame != out[j].Frame {
			return out[i].Frame < out[j].Frame
		}
		return out[i].Subject < out[j].Subject
	})
	return out
}

// Entry is one source's counters.
type Entry struct {
	Role     string           `json:"role"`
	Source   string           `json:"source"`
	Counters map[string]int64 `json:"counters"`
}

// Snapshot returns a copy of every source's counters, sorted by role then
// source.
func (l *Ledger) Snapshot() []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Entry, 0, len(l.m))
	for k, c := range l.m {
		cp := make(map[string]int64, len(c))
		for o, n := range c {
			cp[o] = n
		}
		out = append(out, Entry{Role: k.role, Source: k.source, Counters: cp})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Role != out[j].Role {
			return out[i].Role < out[j].Role
		}
		return out[i].Source < out[j].Source
	})
	return out
}

// Forget drops the counters of sources not in live for the role, so sensor
// pod churn does not grow the ledger without bound. The role's own totals
// (All) are kept.
func (l *Ledger) Forget(role string, live map[string]bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for k := range l.m {
		if k.role == role && k.source != All && !live[k.source] {
			delete(l.m, k)
		}
	}
}

// Reset clears every counter; tests use it to start clean.
func (l *Ledger) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.m = nil
	l.drops = nil
}
