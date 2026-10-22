package push

import (
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type kindCounter struct {
	mu sync.Mutex
	n  map[string]int
}

func (k *kindCounter) Observe(_ string, f *Frame) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.n[f.Kind]++
}

func (k *kindCounter) count(kind string) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.n[kind]
}

func TestEventCaps_BucketHoldsOneHourAndRefills(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	var e eventCaps
	e.set(map[string]int64{"t1": 24_000, "t2": 0}, now)
	n := 0
	for range 3000 {
		if e.admit("t1", now) {
			n++
		}
	}
	if n != 1000 {
		t.Fatalf("one hour's share is 1000, got %d", n)
	}
	if !e.admit("t2", now) || !e.admit("other", now) {
		t.Fatal("an uncapped or unknown tenant is never held back")
	}
	e.set(map[string]int64{"t1": 24_000}, now) // same cap: the bucket stays empty
	if e.admit("t1", now) {
		t.Fatal("resending an unchanged cap must not refill the bucket")
	}
	if !e.admit("t1", now.Add(4*time.Second)) {
		t.Fatal("the bucket refills at 1 per 3.6s")
	}
}

func TestPusher_K8sEventsHeldAtCapAppDeltasNever(t *testing.T) {
	p := newTestPusher("", nil)
	obs := &kindCounter{n: map[string]int{}}
	p.SetObserver(obs)
	p.eventCaps.set(map[string]int64{"example-tenant": 48}, time.Now()) // one hour's share: 2

	for i := range 5 {
		p.enqueue(newK8sEvent("cluster-a", &corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: "e", Namespace: "ops"},
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "ops", Name: string(rune('a' + i))},
			Reason:         "BackOff", Type: corev1.EventTypeWarning,
		}))
		p.enqueue(newAppDelta(p.opts.ClusterName, OpUpdate, mkAppDeltaApp("ops", "api", i%2 == 0)))
	}
	if got := obs.count(KindK8sEvent); got != 2 {
		t.Fatalf("k8s events sent = %d, want 2", got)
	}
	if got := obs.count(KindAppDelta); got != 5 {
		t.Fatalf("app deltas carry state and are never capped: %d, want 5", got)
	}
	if d := p.EventCapDrops()["example-tenant"]; d != 3 {
		t.Fatalf("drops = %d, want 3", d)
	}
}
