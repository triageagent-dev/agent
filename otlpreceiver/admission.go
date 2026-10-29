// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"context"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/triageagent-dev/agent/internal/ledger"
)

// Backpressure. The receiver holds nothing of what it forwards, so what
// its memory is spent on is the exports being decoded and translated at
// one moment. When more arrive than it can take — too many in flight, or
// the live heap near the Go memory limit — it refuses the next export
// before reading it, with RESOURCE_EXHAUSTED and a retry delay, which a
// Collector's exporter queue honours by backing off and sending again.
// The queue in front of the receiver is the Collector's own; the pod
// sheds instead of growing until the kernel kills it. A refusal here is an
// export, not a number of points (nothing of it was read): it is counted
// apart, per sender in the intake ledger, in the summary line and in the
// ledger's refused exports, and is never added to the refused points.

const (
	// memoryHighWater is the share of the Go memory limit the live heap
	// reaches before exports are refused, and memoryLowWater the share it
	// must fall back under before they are taken again.
	memoryHighWater = 0.75
	memoryLowWater  = 0.60
	// memorySampleEvery is how often the live heap is read for the check.
	memorySampleEvery = 250 * time.Millisecond
	// Retry delays given to a refused sender.
	retryInFlight = time.Second
	retryMemory   = 5 * time.Second

	inFlightReason = "backpressure: too many exports in flight"
	memoryReason   = "backpressure: memory near the limit"
)

// liveHeapMetric is the runtime/metrics sample read for the memory check:
// the heap the last collection found live.
const liveHeapMetric = "/gc/heap/live:bytes"

// admission is the receiver's backpressure state.
type admission struct {
	max      int64
	inFlight atomic.Int64
	// liveHeap and memoryLimit are read through these so a test can set
	// them; the defaults read the runtime.
	liveHeap    func() uint64
	memoryLimit func() int64

	mu       sync.Mutex
	sampled  time.Time
	shedding bool
	refused  struct{ inFlight, memory int64 }
}

func newAdmission(max int) *admission {
	if max <= 0 {
		max = 2 * runtime.GOMAXPROCS(0)
		if max < 4 {
			max = 4
		}
	}
	a := &admission{max: int64(max)}
	a.liveHeap = func() uint64 {
		s := []metrics.Sample{{Name: liveHeapMetric}}
		metrics.Read(s)
		if s[0].Value.Kind() != metrics.KindUint64 {
			return 0
		}
		return s[0].Value.Uint64()
	}
	// SetMemoryLimit(-1) reads the limit without changing it.
	a.memoryLimit = func() int64 { return debug.SetMemoryLimit(-1) }
	return a
}

// overMemory reports whether the live heap is past the high-water mark of
// the memory limit, with hysteresis: once over, it stays so until the
// heap is back under the low-water mark. Without a memory limit there is
// no mark.
func (a *admission) overMemory(now time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if now.Sub(a.sampled) < memorySampleEvery {
		return a.shedding
	}
	a.sampled = now
	limit := a.memoryLimit()
	if limit <= 0 || limit == (1<<63-1) {
		a.shedding = false
		return false
	}
	live := float64(a.liveHeap())
	switch {
	case a.shedding && live < memoryLowWater*float64(limit):
		a.shedding = false
	case !a.shedding && live > memoryHighWater*float64(limit):
		a.shedding = true
	}
	return a.shedding
}

// tap is the gRPC tap handle: it runs when a sender opens a request,
// before the request is read, and refuses it with RESOURCE_EXHAUSTED and
// a retry delay when the receiver is at its bound.
func (r *Receiver) tap(ctx context.Context, _ *tap.Info) (context.Context, error) {
	a := r.adm
	now := r.opts.now()
	var why string
	var delay time.Duration
	switch {
	case a.inFlight.Load() >= a.max:
		why, delay = inFlightReason, retryInFlight
	case a.overMemory(now):
		why, delay = memoryReason, retryMemory
	default:
		return ctx, nil
	}
	r.refuseExport(ctx, why, delay == retryMemory)
	st, err := status.New(codes.ResourceExhausted, why).WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(delay)})
	if err != nil {
		return ctx, status.Error(codes.ResourceExhausted, why)
	}
	return ctx, st.Err()
}

// refuseExport counts one refused export against its sender, named by
// its certificate or its address (nothing else is known before the
// request is read), as a refused export in the intake ledger and the
// summary line, and logs it like any refusal. It is not a refused point,
// so it stays out of the per-sender refusal counters and the ledger's
// refused points.
func (r *Receiver) refuseExport(ctx context.Context, why string, memory bool) {
	sender := "unknown"
	if p, ok := peer.FromContext(ctx); ok {
		sender = anonymousPrincipal(ctx).String()
		if ti, ok := p.AuthInfo.(credentials.TLSInfo); ok && len(ti.State.VerifiedChains) > 0 && len(ti.State.VerifiedChains[0]) > 0 {
			if pr, err := principalFromCert(ti.State.VerifiedChains[0][0]); err == nil {
				sender = pr.String()
			}
		}
	}
	r.mu.Lock()
	in := r.intakeFor(sender)
	in.cur.refusedExports++
	r.refusedExports++
	r.logRefusal(sender, "exports", why, "", 1)
	if memory {
		r.adm.refused.memory++
	} else {
		r.adm.refused.inFlight++
	}
	r.mu.Unlock()
	ledger.Default.Add("otlp", ledger.All, ledger.RefusedExports, 1)
}

// InFlight is the number of exports being read or translated right now.
func (r *Receiver) InFlight() int { return int(r.adm.inFlight.Load()) }

// Refused returns how many exports were refused for backpressure, for too
// many in flight and for memory.
func (r *Receiver) Refused() (inFlight, memory int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.adm.refused.inFlight, r.adm.refused.memory
}

// stats.Handler: the in-flight count follows every request from its
// beginning to its end, whatever the handler did with it.

func (a *admission) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }

func (a *admission) HandleRPC(_ context.Context, s stats.RPCStats) {
	switch s.(type) {
	case *stats.Begin:
		a.inFlight.Add(1)
	case *stats.End:
		a.inFlight.Add(-1)
	}
}

func (a *admission) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }

func (a *admission) HandleConn(context.Context, stats.ConnStats) {}
