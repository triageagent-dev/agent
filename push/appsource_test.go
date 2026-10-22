package push

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeAppSource struct {
	mu   sync.Mutex
	emit func(AppEvent)
	up   chan struct{}
}

func newFakeAppSource() *fakeAppSource { return &fakeAppSource{up: make(chan struct{})} }

func (f *fakeAppSource) Watch(ctx context.Context, emit func(AppEvent)) error {
	f.mu.Lock()
	f.emit = emit
	f.mu.Unlock()
	close(f.up)
	<-ctx.Done()
	return nil
}

func appByName(p *Pusher, name string) *VirtualApp {
	for _, a := range p.vapps.snapshotApps() {
		if a.Name == name {
			return a
		}
	}
	return nil
}

func TestSetAppSource_RejectsClusterPusherAndNil(t *testing.T) {
	cluster := &Pusher{}
	if err := cluster.SetAppSource(newFakeAppSource()); err == nil {
		t.Error("SetAppSource on a non-host Pusher must fail")
	}
	if err := newTestHostPusher("ws://example.internal/ws").SetAppSource(nil); err == nil {
		t.Error("SetAppSource(nil) must fail")
	}
}

func TestAppSource_SnapshotCarriesWorkloads(t *testing.T) {
	p := newTestHostPusher("ws://example.internal/ws")
	if err := p.SetAppSource(newFakeAppSource()); err != nil {
		t.Fatal(err)
	}
	p.applyAppEvent(AppEvent{Name: "nginx", Ready: true})
	p.applyAppEvent(AppEvent{Name: "redis", Ready: false})

	frames, n := p.snapshotFramesForTenant(context.Background(), "acme")
	if n != 2 {
		t.Fatalf("snapshot apps = %d, want 2", n)
	}
	if len(frames) != 2 || frames[len(frames)-1].Kind != KindSnapshotEnd {
		t.Fatalf("frames = %d, want a snapshot chunk then snapshot_end", len(frames))
	}
	ng := appByName(p, "nginx")
	if ng == nil || ng.Namespace != "host" || ng.Status.ComponentsReady != "1/1" {
		t.Fatalf("nginx app = %+v", ng)
	}
	if rd := appByName(p, "redis"); rd == nil || rd.Status.ComponentsReady != "0/1" {
		t.Fatalf("redis app = %+v", rd)
	}
}

func TestAppSource_RestartIsAChangeAndDeleteRemoves(t *testing.T) {
	p := newTestHostPusher("ws://example.internal/ws")
	if err := p.SetAppSource(newFakeAppSource()); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var ops []string
	p.vapps.enqueue = func(f *Frame) {
		mu.Lock()
		defer mu.Unlock()
		ops = append(ops, f.AppDelta.Op)
	}
	p.applyAppEvent(AppEvent{Name: "nginx", Ready: true})
	p.vapps.recomputeNow(context.Background())
	p.applyAppEvent(AppEvent{Name: "nginx", Ready: true, Restarts: 1})
	p.vapps.recomputeNow(context.Background())
	p.applyAppEvent(AppEvent{Name: "nginx", Ready: true, Restarts: 1})
	p.vapps.recomputeNow(context.Background())
	p.applyAppEvent(AppEvent{Name: "nginx", Deleted: true})
	p.vapps.recomputeNow(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(ops) != 3 {
		t.Fatalf("deltas = %v, want add, update (restart), delete", ops)
	}
	if appByName(p, "nginx") != nil {
		t.Error("deleted workload still in the snapshot")
	}
}

func TestAppSource_StartRunsSource(t *testing.T) {
	p := newTestHostPusher("ws://127.0.0.1:1/ws")
	src := newFakeAppSource()
	if err := p.SetAppSource(src); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = p.Start(ctx); close(done) }()
	select {
	case <-src.up:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not run the app source")
	}
	src.mu.Lock()
	emit := src.emit
	src.mu.Unlock()
	emit(AppEvent{Name: "api", Ready: true})
	p.vapps.recomputeNow(context.Background())
	if appByName(p, "api") == nil {
		t.Error("event emitted by the source did not reach the virtual apps")
	}
	cancel()
	<-done
}
