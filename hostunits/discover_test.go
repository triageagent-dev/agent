package hostunits

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/triageagent-dev/agent/push"
)

type fakeBus struct {
	mu       sync.Mutex
	units    []Unit
	restarts map[string]uint64
	rErr     error
	changes  chan struct{}
}

func (f *fakeBus) Units(context.Context) ([]Unit, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Unit(nil), f.units...), nil
}

func (f *fakeBus) Restarts(_ context.Context, name string) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.restarts[name], f.rErr
}

func (f *fakeBus) Changes(context.Context) (<-chan struct{}, error) { return f.changes, nil }
func (f *fakeBus) Close()                                           {}

func (f *fakeBus) set(units []Unit) {
	f.mu.Lock()
	f.units = units
	f.mu.Unlock()
}

func running(name string) Unit {
	return Unit{Name: name, Enabled: true, ActiveState: "active", SubState: "running", Type: "simple"}
}

func oneshot(name string) Unit {
	return Unit{Name: name, Enabled: true, ActiveState: "active", SubState: "exited", Type: "oneshot"}
}

func idle(name string) Unit {
	return Unit{Name: name, Enabled: true, ActiveState: "inactive", SubState: "dead", Type: "idle"}
}

func TestSelected(t *testing.T) {
	cases := []struct {
		name  string
		u     Unit
		globs []string
		want  bool
	}{
		{"default enabled", running("nginx.service"), nil, true},
		{"default skips oneshot", oneshot("apparmor.service"), nil, false},
		{"glob keeps oneshot", oneshot("apparmor.service"), []string{"apparmor"}, true},
		{"default skips idle", idle("dmesg.service"), nil, false},
		{"glob keeps idle", idle("dmesg.service"), []string{"dmesg"}, true},
		{"default disabled", Unit{Name: "nginx.service"}, nil, false},
		{"denylist systemd-*", running("systemd-resolved.service"), nil, false},
		{"denylist ssh*", running("sshd.service"), nil, false},
		{"denylist getty instance", running("getty@tty1.service"), nil, false},
		{"denylist agent", running("triage-agent.service"), nil, false},
		{"denylist dbus", running("dbus.service"), nil, false},
		{"glob without suffix", Unit{Name: "web-api.service"}, []string{"web-*"}, true},
		{"glob with suffix", Unit{Name: "web-api.service"}, []string{"web-api.service"}, true},
		{"glob miss", running("nginx.service"), []string{"web-*"}, false},
		{"glob overrides denylist", running("sshd.service"), []string{"ssh*"}, true},
		{"template never", running("foo@.service"), nil, false},
		{"not a service", running("foo.socket"), nil, false},
	}
	for _, c := range cases {
		if got := Selected(c.u, c.globs); got != c.want {
			t.Errorf("%s: Selected = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestReady(t *testing.T) {
	for _, c := range []struct {
		active, sub string
		want        bool
	}{
		{"active", "running", true},
		{"active", "exited", false},
		{"activating", "start", false},
		{"inactive", "dead", false},
		{"failed", "failed", false},
	} {
		if got := Ready(Unit{ActiveState: c.active, SubState: c.sub}); got != c.want {
			t.Errorf("Ready(%s/%s) = %v, want %v", c.active, c.sub, got, c.want)
		}
	}
}

func TestScan_StateRestartAndRemoval(t *testing.T) {
	bus := &fakeBus{units: []Unit{running("nginx.service"), running("sshd.service")}, restarts: map[string]uint64{}}
	d := newWithConnect(Options{}, nil)
	var got []push.AppEvent
	emit := func(e push.AppEvent) { got = append(got, e) }
	ctx := context.Background()

	if err := d.Scan(ctx, bus, emit); err != nil {
		t.Fatal(err)
	}
	if want := []push.AppEvent{{Name: "nginx", Ready: true}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("first scan = %+v, want %+v", got, want)
	}
	if names := d.Names(); !reflect.DeepEqual(names, []string{"nginx"}) {
		t.Fatalf("Names after first scan = %v, want [nginx]", names)
	}

	got = nil
	_ = d.Scan(ctx, bus, emit)
	if len(got) != 0 {
		t.Fatalf("unchanged scan emitted %+v", got)
	}

	bus.restarts["nginx.service"] = 1
	_ = d.Scan(ctx, bus, emit)
	if want := []push.AppEvent{{Name: "nginx", Ready: true, Restarts: 1}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("restart scan = %+v, want %+v", got, want)
	}

	got = nil
	bus.rErr = errors.New("property gone")
	bus.restarts["nginx.service"] = 0
	_ = d.Scan(ctx, bus, emit)
	if len(got) != 0 {
		t.Fatalf("a failed NRestarts read must keep the last value, emitted %+v", got)
	}
	bus.rErr = nil
	bus.restarts["nginx.service"] = 1

	bus.set([]Unit{{Name: "nginx.service", Enabled: true, ActiveState: "inactive", SubState: "dead"}})
	_ = d.Scan(ctx, bus, emit)
	if want := []push.AppEvent{{Name: "nginx", Restarts: 1}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("stop scan = %+v, want %+v", got, want)
	}

	got = nil
	bus.set(nil)
	_ = d.Scan(ctx, bus, emit)
	if want := []push.AppEvent{{Name: "nginx", Deleted: true}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("removal scan = %+v, want %+v", got, want)
	}
	if names := d.Names(); len(names) != 0 {
		t.Fatalf("Names after removal = %v, want none", names)
	}
}

func TestWatch_RescansOnChangeSignal(t *testing.T) {
	bus := &fakeBus{units: []Unit{running("nginx.service")}, restarts: map[string]uint64{}, changes: make(chan struct{}, 1)}
	d := newWithConnect(Options{Poll: time.Hour}, func(context.Context) (Bus, error) { return bus, nil })
	events := make(chan push.AppEvent, 8)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = d.Watch(ctx, func(e push.AppEvent) { events <- e }); close(done) }()

	wait := func() push.AppEvent {
		select {
		case e := <-events:
			return e
		case <-time.After(5 * time.Second):
			t.Fatal("no event")
			return push.AppEvent{}
		}
	}
	if e := wait(); e.Name != "nginx" || !e.Ready {
		t.Fatalf("first event = %+v", e)
	}
	bus.set([]Unit{{Name: "nginx.service", Enabled: true, ActiveState: "failed", SubState: "failed"}})
	bus.changes <- struct{}{}
	if e := wait(); e.Name != "nginx" || e.Ready {
		t.Fatalf("event after change = %+v, want not ready", e)
	}
	cancel()
	<-done
}

func TestWatch_ConnectError(t *testing.T) {
	d := newWithConnect(Options{}, func(context.Context) (Bus, error) { return nil, errors.New("no bus") })
	if err := d.Watch(context.Background(), func(push.AppEvent) {}); err == nil {
		t.Fatal("want a connect error")
	}
}
