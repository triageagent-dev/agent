package push

import (
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// Event caps (core policy budgets.agentEventsDaily). The core tells the
// agent, in a limits frame, how many events per 24 hours it takes from this
// agent for each tenant. The agent holds its Kubernetes events to that with
// one token bucket per tenant, the same shape the core uses: it refills
// evenly over 24 hours and holds one hour's share, so an event storm is cut
// within minutes and the tenant keeps its allowance for the rest of the day.
// The core applies the same bucket to every event it derives, so this saves
// the bytes and makes the drop visible here; it is not the only guard.

// capBurstDivisor sets the bucket size: one hour of the daily cap.
const capBurstDivisor = 24

type eventCaps struct {
	mu      sync.Mutex
	caps    map[string]int64
	buckets map[string]*rate.Limiter
	dropped sync.Map // tenant → *atomic.Int64
}

func capLimiter(c int64, now time.Time) *rate.Limiter {
	burst := int(c / capBurstDivisor)
	if burst < 1 {
		burst = 1
	}
	l := rate.NewLimiter(rate.Limit(float64(c)/(24*time.Hour).Seconds()), burst)
	l.AllowN(now, 0)
	return l
}

// set replaces the caps. A tenant whose cap is unchanged keeps its bucket,
// so a policy change elsewhere does not refill it.
func (e *eventCaps) set(caps map[string]int64, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	next := map[string]*rate.Limiter{}
	for t, c := range caps {
		if c <= 0 {
			continue
		}
		if b, ok := e.buckets[t]; ok && e.caps[t] == c {
			next[t] = b
			continue
		}
		next[t] = capLimiter(c, now)
	}
	e.caps, e.buckets = caps, next
}

// admit reports whether one more event for tenant may be sent.
func (e *eventCaps) admit(tenant string, now time.Time) bool {
	e.mu.Lock()
	b := e.buckets[tenant]
	e.mu.Unlock()
	return b == nil || b.AllowN(now, 1)
}

// Dropped returns how many Kubernetes events were held back at each
// tenant's cap since the agent started.
func (e *eventCaps) Dropped() map[string]int64 {
	out := map[string]int64{}
	e.dropped.Range(func(k, v any) bool {
		out[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
}

// recordCapDrop counts one event held back at tenant's cap and logs the
// first, then every thousandth.
func (p *Pusher) recordCapDrop(tenant string) {
	v, _ := p.eventCaps.dropped.LoadOrStore(tenant, new(atomic.Int64))
	n := v.(*atomic.Int64).Add(1)
	if n == 1 || n%1000 == 0 {
		p.eventCaps.mu.Lock()
		c := p.eventCaps.caps[tenant]
		p.eventCaps.mu.Unlock()
		p.log.Info("event cap reached: holding back Kubernetes events for this tenant", "tenant", tenant, "capPerDay", c, "droppedTotal", n)
	}
}

// EventCapDrops reports the Kubernetes events held back at each tenant's
// daily cap since the agent started.
func (p *Pusher) EventCapDrops() map[string]int64 { return p.eventCaps.Dropped() }
