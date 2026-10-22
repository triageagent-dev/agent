// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/gorilla/websocket"

	"github.com/triageagent-dev/agent/logmetrics"
	"github.com/triageagent-dev/agent/push"
	"github.com/triageagent-dev/agent/redsource"
	"github.com/triageagent-dev/agent/sensorstream"
	"github.com/triageagent-dev/agent/usemetrics"
)

func TestHostModeSelected(t *testing.T) {
	tests := []struct {
		name    string
		modes   modeList
		want    bool
		wantErr string
	}{
		{"default controller", nil, false, ""},
		{"sensor roles", modeList{"ebpf", "node-exporter"}, false, ""},
		{"host alone", modeList{"host"}, true, ""},
		{"host twice is one mode list entry each", modeList{"host", "host"}, false, "cannot be combined"},
		{"host with a sensor role", modeList{"host", "ebpf"}, false, "cannot be combined"},
		{"sensor role then host", modeList{"ebpf", "host"}, false, "cannot be combined"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := hostModeSelected(tc.modes)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("hostModeSelected = (%v, %v), want (%v, nil)", got, err, tc.want)
			}
		})
	}
}

func TestHostClusterName(t *testing.T) {
	hn := func() (string, error) { return "web-01", nil }
	if got, err := hostClusterName("", hn); err != nil || got != "web-01" {
		t.Fatalf("default = (%q, %v), want the host name", got, err)
	}
	fqdn := func() (string, error) { return "web-01.c.example.internal", nil }
	if got, err := hostClusterName("", fqdn); err != nil || got != "web-01" {
		t.Fatalf("fqdn default = (%q, %v), want the short host name", got, err)
	}
	if got, err := hostClusterName(" edge-a ", hn); err != nil || got != "edge-a" {
		t.Fatalf("override = (%q, %v), want edge-a", got, err)
	}
	if _, err := hostClusterName("", func() (string, error) { return "", errors.New("no") }); err == nil {
		t.Fatal("a failing host name lookup must be an error")
	}
	if _, err := hostClusterName("", func() (string, error) { return "  ", nil }); err == nil {
		t.Fatal("an empty host name must be an error")
	}
}

func validHostOptions() hostOptions {
	return hostOptions{
		endpoint: "wss://core.example.internal/events/ws", tenant: "acme",
		joinCAFingerprint: "SHA256:0000000000000000000000000000000000000000000000000000000000000000",
		hostname:          func() (string, error) { return "web-01", nil },
	}
}

func TestHostOptionsValidate(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		o := validHostOptions()
		if err := o.validate(); err != nil {
			t.Fatal(err)
		}
		if o.cluster != "web-01" || o.namespace != "host" || o.tick <= 0 || o.statusEvery != sensorStatusInterval {
			t.Fatalf("defaults = cluster %q namespace %q tick %v status %v", o.cluster, o.namespace, o.tick, o.statusEvery)
		}
	})
	tests := []struct {
		name    string
		mutate  func(*hostOptions)
		wantErr string
	}{
		{"no endpoint", func(o *hostOptions) { o.endpoint = "" }, "--triage-endpoint"},
		{"plaintext needs opt-in", func(o *hostOptions) { o.endpoint = "ws://core.example.internal/events/ws" }, "plaintext"},
		{"no join fingerprint", func(o *hostOptions) { o.joinCAFingerprint = "" }, "--join-ca-fingerprint"},
		{"no tenant", func(o *hostOptions) { o.tenant = "" }, "--target-tenant"},
		{"join secret", func(o *hostOptions) { o.joinSecret = "s" }, "--join-secret"},
		{"namespace with comma", func(o *hostOptions) { o.namespace = "a,b" }, "--host-namespace"},
		{"bad unit glob", func(o *hostOptions) { o.units = []string{"nginx[.service"} }, "--host-units"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := validHostOptions()
			tc.mutate(&o)
			err := o.validate()
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestHostSensorStatusLoopSendsAtStartThenEveryPeriod(t *testing.T) {
	local := sensorstream.NewLocal(sensorstream.LocalOptions{Node: "web-01"})
	var mu sync.Mutex
	var sent [][]push.SensorStatus
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		hostSensorStatusLoop(ctx, local.Nodes, func(_ int64, s []push.SensorStatus, _ []push.OTLPSourceStatus) {
			mu.Lock()
			sent = append(sent, s)
			mu.Unlock()
		}, 20*time.Millisecond)
		close(done)
	}()
	deadline := time.After(3 * time.Second)
	for {
		mu.Lock()
		n := len(sent)
		mu.Unlock()
		if n >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d sensor_state sends, want at least 3", n)
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if len(sent[0]) != 1 || sent[0][0].Node != "web-01" || !sent[0][0].Connected {
		t.Fatalf("first send = %+v, want the host as one connected sensor", sent[0])
	}
}

func TestBuildHostWiresCollectorsOnLocalSource(t *testing.T) {
	o := validHostOptions()
	o.logMetrics, o.useMetrics, o.redMetrics = true, true, true
	o.logOptions = func() logmetrics.Options { return logmetrics.Options{} }
	o.useOptions = func() usemetrics.Options { return usemetrics.Options{} }
	o.redOptions = func() redsource.Options {
		return redsource.Options{Profile: redsource.ProfileOBI, EnableRED: true}
	}
	if err := o.validate(); err != nil {
		t.Fatal(err)
	}
	rt, err := buildHost(o, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	// The journald follower, local.Run, pusher.Start, the status loop and the
	// three collectors.
	if got, want := len(rt.runnables), 7; got != want {
		t.Fatalf("runnables = %d, want %d", got, want)
	}

	o.logMetrics, o.useMetrics, o.redMetrics = false, false, false
	rt, err = buildHost(o, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(rt.runnables), 3; got != want {
		t.Fatalf("runnables with no collector flag = %d, want %d", got, want)
	}
}

// TestHostRuntimeJoinsAsAHost runs the host runtime against a fake core and
// checks what the core sees from a process with no Kubernetes: the hello with
// the host name, tenant, synthetic namespace and host capability, and the
// in-process sensors' sensor_state.
func TestHostRuntimeJoinsAsAHost(t *testing.T) {
	var mu sync.Mutex
	var frames []*push.Frame
	core := newJoinCore(t)
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		hello, ok := core.accept(t, r, conn)
		if !ok {
			return
		}
		mu.Lock()
		frames = append(frames, hello)
		mu.Unlock()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			f := &push.Frame{}
			if err := json.Unmarshal(data, f); err != nil {
				continue
			}
			mu.Lock()
			frames = append(frames, f)
			mu.Unlock()
		}
	}))
	defer srv.Close()

	o := validHostOptions()
	o.endpoint = "ws" + strings.TrimPrefix(srv.URL, "http")
	o.allowPlaintext = true
	o.joinCAFingerprint = core.fingerprint
	o.joinStateDir = core.joinedState(t, "web-01", "acme")
	o.heartbeat = 100 * time.Millisecond
	o.statusEvery = time.Hour
	if err := o.validate(); err != nil {
		t.Fatal(err)
	}
	rt, err := buildHost(o, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, run := range rt.runnables {
		wg.Add(1)
		go func() { defer wg.Done(); _ = run(ctx) }()
	}
	defer func() { cancel(); wg.Wait() }()

	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		kinds := make([]string, len(frames))
		for i, f := range frames {
			kinds[i] = f.Kind
		}
		mu.Unlock()
		if slices.Contains(kinds, push.KindSensorState) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("no sensor_state frame; frames = %v", kinds)
		case <-time.After(10 * time.Millisecond):
		}
	}
	mu.Lock()
	defer mu.Unlock()
	hello := frames[0]
	if hello.Kind == push.KindHello && (hello.Hello.ClusterName != "web-01" || hello.Hello.Tenant != "acme") {
		t.Errorf("hello cluster/tenant = %q/%q, want web-01/acme", hello.Hello.ClusterName, hello.Hello.Tenant)
	}
	if hello.Kind != push.KindHello || !slices.Equal(hello.Hello.Namespaces, []string{"host"}) ||
		!slices.Contains(hello.Hello.Capabilities, "host") {
		t.Errorf("hello = %+v, want namespaces [host] and the host capability", hello.Hello)
	}
}

func TestHostJoinStateDir(t *testing.T) {
	cases := []struct {
		name     string
		value    string
		explicit bool
		want     string
	}{
		{"flag left at the cluster default", "/data/join", false, "/var/lib/triage-agent/join"},
		{"flag given", "/srv/join", true, "/srv/join"},
		{"flag given empty", "", true, "/var/lib/triage-agent/join"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := hostJoinStateDir(c.value, c.explicit); got != c.want {
				t.Fatalf("hostJoinStateDir(%q, %v) = %q, want %q", c.value, c.explicit, got, c.want)
			}
		})
	}
}
