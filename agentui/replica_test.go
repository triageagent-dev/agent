package agentui

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/triageagent-dev/agent/push"
)

func TestReplicaFollowerServesLeaderView(t *testing.T) {
	leaderStore, followerStore := NewStore(), NewStore()
	var wireMu sync.Mutex
	var wire []byte
	getWire := func() []byte { wireMu.Lock(); defer wireMu.Unlock(); return wire }
	leader := NewReplica(fakeLink{apps: []*push.VirtualApp{vapp("shop", "checkout", "checkout")}}, leaderStore, "a",
		func(b []byte) { wireMu.Lock(); wire = b; wireMu.Unlock() }, logr.Discard())
	follower := NewReplica(fakeLink{}, followerStore, "b", func([]byte) {}, logr.Discard())

	if got := follower.Role(); got != "follower" {
		t.Fatalf("role = %s", got)
	}

	leaderStore.Observe("t", &push.Frame{Kind: push.KindRedSummary, RedSummary: &push.RedSummaryPayload{
		WindowStart: leaderStore.now().Unix() - 60, WindowSec: 60,
		Services: []push.ServiceRedMetrics{{Namespace: "shop", Service: "checkout", RequestCount: 60}}}})
	leaderStore.Observe("t", &push.Frame{Kind: push.KindRedMetrics, RedMetrics: &push.RedMetricsPayload{WindowSec: 60,
		Services: []push.ServiceRedMetrics{{Namespace: "shop", Service: "checkout", Breach: push.BreachErrors}}}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = leader.Start(ctx); close(done) }()
	deadline := time.Now().Add(2 * time.Second)
	for getWire() == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	wire = getWire()
	if wire == nil {
		t.Fatal("leader published nothing")
	}
	if leader.Role() != "leader" {
		t.Errorf("role = %s", leader.Role())
	}
	ch, stop := followerStore.Subscribe()
	defer stop()
	follower.Apply(wire)

	if apps := follower.Apps(); len(apps) != 1 || apps[0].Name != "checkout" {
		t.Errorf("follower apps = %v", apps)
	}
	if !follower.Link().Connected {
		t.Error("follower does not show the leader's connection")
	}
	if svcs := followerStore.Services(); len(svcs) != 1 || svcs[0].RPS != 1 {
		t.Errorf("follower services = %+v", svcs)
	}
	select {
	case e := <-ch:
		if e.Family != FamilyRed {
			t.Errorf("event = %+v", e)
		}
	default:
		t.Error("follower subscriber got no event")
	}

	// Applying the same snapshot again adds no event; a leader ignores peers.
	follower.Apply(wire)
	if n := len(followerStore.Events("", nil, 0)); n != 1 {
		t.Errorf("events after re-apply = %d, want 1", n)
	}
	leader.Apply(wire)
	cancel()
	<-done

	// The follower becomes leader: ids continue from the old leader's.
	followerStore.add(Event{Family: FamilyK8s})
	if ev := followerStore.Events("", nil, 1); ev[0].ID != 2 {
		t.Errorf("next id = %d, want 2", ev[0].ID)
	}
}
