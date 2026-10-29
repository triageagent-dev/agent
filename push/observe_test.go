package push

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

type recordingObserver struct {
	mu   sync.Mutex
	seen []string // "tenant:kind"
}

func (r *recordingObserver) Observe(tenant string, f *Frame) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, tenant+":"+f.Kind)
}

func TestObserverSeesQueuedAndSessionFrames(t *testing.T) {
	p := newTestPusher("ws://unused", nil)
	p.mux().beginConnection([]string{p.opts.Tenant})
	rec := &recordingObserver{}
	p.SetObserver(rec)

	p.SendRedSummary(1, 60, []ServiceRedMetrics{{Namespace: "ns", Service: "s", RequestCount: 1}})
	p.SendSensorState(1, []SensorStatus{{Node: "n"}}, nil, nil)

	rec.mu.Lock()
	n := len(rec.seen)
	rec.mu.Unlock()
	if n != 2 {
		t.Fatalf("observer saw %v, want a red_summary and a sensor_state", rec.seen)
	}

	p.SetObserver(nil)
	p.SendSensorState(2, []SensorStatus{{Node: "n"}}, nil, nil)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.seen) != 2 {
		t.Errorf("detached observer still called: %v", rec.seen)
	}
	if p.Link().Connected {
		t.Error("Link reports connected with no connection")
	}
}

func TestStandaloneObservesWithoutAConnection(t *testing.T) {
	p := NewHost(Options{Standalone: true, ClusterName: "c", Tenant: "local", Namespaces: []string{"host"}}, logr.Discard())
	if p == nil {
		t.Fatal("a standalone pusher must exist with no endpoint")
	}
	if NewHost(Options{ClusterName: "c", Tenant: "local"}, logr.Discard()) != nil {
		t.Fatal("no endpoint and not standalone must stay disabled")
	}
	rec := &recordingObserver{}
	p.SetObserver(rec)
	p.SendSensorState(1, []SensorStatus{{Node: "n"}}, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Start(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after cancel: standalone must not dial")
	}
	if l := p.Link(); !l.Standalone || l.Connected {
		t.Errorf("link = %+v, want standalone and not connected", l)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.seen) != 1 {
		t.Errorf("observer saw %v, want the sensor_state", rec.seen)
	}
}
