// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/obi/pkg/obi"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/redsource"
)

func kvs(pairs ...string) []*commonpb.KeyValue {
	var out []*commonpb.KeyValue
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, &commonpb.KeyValue{Key: pairs[i], Value: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: pairs[i+1]}}})
	}
	return out
}

func serverRM(res, dp []*commonpb.KeyValue) *metricspb.ResourceMetrics {
	return &metricspb.ResourceMetrics{
		Resource: &resourcepb.Resource{Attributes: res},
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{
			Name: "http.server.request.duration",
			Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
				AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
				DataPoints:             []*metricspb.HistogramDataPoint{{Attributes: dp, Count: 5}},
			}},
		}}}},
	}
}

type staticRole []*metricspb.ResourceMetrics

func (s staticRole) Collect(context.Context) ([]*metricspb.ResourceMetrics, error) { return s, nil }

func TestHostREDSourceStampsFromPinnedEntry(t *testing.T) {
	// No process lookup: the entry's namespace/name are enough.
	rm := serverRM(kvs("service.name", "nginx", "service.namespace", "web", "service.instance.id", "web-01:42"),
		kvs("service.name", "nginx", "service.namespace", "web", "http.response.status_code", "200"))
	src := newHostREDSource(staticRole{rm}, "web", func(int) (string, bool) { return "", false })
	out, err := src.Collect(context.Background())
	if err != nil || len(out) != 1 {
		t.Fatalf("Collect = %v, %v", out, err)
	}
	pt := metricset.FromOTLP(out)["http.server.request.duration"].Points[0]
	if pt.Attr("k8s.namespace.name") != "web" || pt.Attr("service.name") != "nginx" {
		t.Errorf("attrs = %v", pt.Attrs)
	}
	// The wrapped message is shared with the store: it must be unchanged.
	if resourceAttr(rm, "k8s.namespace.name") != "" {
		t.Error("the source edited the store's message in place")
	}
}

func TestHostREDSourceMapsPIDThroughCgroup(t *testing.T) {
	// The process names itself, so service.name/namespace are not the unit's.
	rm := serverRM(kvs("service.name", "my-app", "service.instance.id", "web-01:42"),
		kvs("service.name", "my-app", "http.response.status_code", "500"))
	calls := 0
	src := newHostREDSource(staticRole{rm}, "web", func(pid int) (string, bool) {
		calls++
		return "api", pid == 42
	})
	for i := 0; i < 2; i++ {
		out, _ := src.Collect(context.Background())
		pt := metricset.FromOTLP(out)["http.server.request.duration"].Points[0]
		if pt.Attr("k8s.namespace.name") != "web" || pt.Attr("service.name") != "api" {
			t.Fatalf("attrs = %v", pt.Attrs)
		}
	}
	if calls != 1 {
		t.Errorf("cgroup lookups = %d, want 1 (cached)", calls)
	}
}

func TestHostREDSourceLeavesUnattributedAlone(t *testing.T) {
	flow := &metricspb.ResourceMetrics{Resource: &resourcepb.Resource{Attributes: kvs("host.name", "web-01")}}
	other := serverRM(kvs("service.name", "x", "service.namespace", "elsewhere", "service.instance.id", "web-01:7"), nil)
	src := newHostREDSource(staticRole{flow, other}, "web", func(int) (string, bool) { return "", false })
	out, _ := src.Collect(context.Background())
	if out[0] != flow || out[1] != other {
		t.Error("unattributed resources must pass through untouched")
	}
}

// The stamped series reach the RED collector's keys: a destination identity.
func TestHostREDSourceSeriesCarryDestination(t *testing.T) {
	rm := serverRM(kvs("service.name", "nginx", "service.namespace", "web"), kvs("http.response.status_code", "200"))
	src := newHostREDSource(staticRole{rm}, "web", func(int) (string, bool) { return "", false })
	out, _ := src.Collect(context.Background())
	pt := metricset.FromOTLP(out)["http.server.request.duration"].Points[0]
	p := redsource.ProfileOBI
	if pt.Attr(p.DestNamespaceLabel) != "web" || pt.Attr(p.DestWorkloadLabel) != "nginx" {
		t.Errorf("destination = %q/%q", pt.Attr(p.DestNamespaceLabel), pt.Attr(p.DestWorkloadLabel))
	}
}

func TestUnitFromCgroup(t *testing.T) {
	for in, want := range map[string]string{
		"0::/system.slice/nginx.service\n":               "nginx",
		"0::/system.slice/nginx.service/control\n":       "nginx",
		"1:name=systemd:/system.slice/redis.service\n":   "redis",
		"0::/user.slice/user-1000.slice/session-2.scope": "",
		"0::/\n": "",
	} {
		got, ok := unitFromCgroup(in)
		if got != want || ok != (want != "") {
			t.Errorf("unitFromCgroup(%q) = %q,%v want %q", in, got, ok, want)
		}
	}
}

func writeProcs(t *testing.T, root, unit, sub, procs string) {
	t.Helper()
	dir := filepath.Join(root, "system.slice", unit+".service", sub)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cgroup.procs"), []byte(procs), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestReadSelectionAndConfig(t *testing.T) {
	root := t.TempDir()
	writeProcs(t, root, "nginx", "", "100\n101\n")
	writeProcs(t, root, "nginx", "sub", "102\n")
	writeProcs(t, root, "redis", "", "200\n")
	sel := readSelection(root, []string{"redis", "nginx", "idle", "nginx"})
	if len(sel) != 3 || sel[0].Name != "idle" || len(sel[0].PIDs) != 0 {
		t.Fatalf("selection = %+v", sel)
	}
	if got := sel[1].PIDs; len(got) != 3 || got[0] != 100 || got[2] != 102 {
		t.Errorf("nginx pids = %v", got)
	}

	for _, flows := range []bool{false, true} {
		yaml := hostOBIYAML("web", sel, flows)
		cfg, err := obi.LoadConfig(strings.NewReader(yaml))
		if err != nil {
			t.Fatalf("LoadConfig(flows=%v): %v\n%s", flows, err, yaml)
		}
		if len(cfg.Discovery.Instrument) != 2 {
			t.Fatalf("instrument entries = %d, want 2 (the idle unit has none)\n%s", len(cfg.Discovery.Instrument), yaml)
		}
		e := cfg.Discovery.Instrument[0]
		//nolint:staticcheck // SA1019: reading back the pinned name
		if len(e.PIDs) != 3 || e.Name != "nginx" || e.Namespace != "web" {
			t.Errorf("entry = pids %v name %q ns %q", e.PIDs, e.Name, e.Namespace)
		}
		if cfg.Attributes.Kubernetes.Enable != "false" {
			t.Error("kubernetes must be disabled")
		}
		if len(cfg.Discovery.ExcludedLinuxSystemPaths) != 0 {
			t.Errorf("excluded system paths = %v, want none", cfg.Discovery.ExcludedLinuxSystemPaths)
		}
		if err := newOBIMetricsStore().exportTo(cfg); err != nil {
			t.Fatal(err)
		}
		if got := cfg.JoinMetricsConfig().Features.NetworkBytes(); got != flows {
			t.Errorf("network flow feature = %v, want %v", got, flows)
		}
		if err := cfg.ValidateStatic(); err != nil {
			t.Errorf("ValidateStatic(flows=%v): %v", flows, err)
		}
	}
	if !hostSelectionEmpty(sel[:1]) || hostSelectionEmpty(sel) {
		t.Error("hostSelectionEmpty")
	}
}

func TestPIDTracker(t *testing.T) {
	sel := func(pids ...uint32) hostSelection { return hostSelection{{Name: "a", PIDs: pids}} }
	var tr pidTracker
	if !tr.observe(sel(1)) {
		t.Fatal("first observation must start the instrumenter")
	}
	for i := 0; i < 2; i++ {
		if tr.observe(sel(1)) {
			t.Error("unchanged set must not restart")
		}
	}
	if tr.observe(sel(1, 2)) {
		t.Error("a PID seen once (maybe a short-lived child) must not restart")
	}
	if tr.observe(sel(1)) {
		t.Error("a vanished PID must not restart")
	}
	if tr.observe(sel(1, 3)) {
		t.Error("new PID, first sighting")
	}
	if !tr.observe(sel(1, 3)) {
		t.Error("a PID seen twice must restart")
	}
	if tr.observe(sel(1, 3)) {
		t.Error("applied now: no restart")
	}
	if tr.observe(nil) {
		t.Error("a unit leaving the selection must not restart")
	}
}

func TestHostOBIRunnerRestartsOnNewPIDs(t *testing.T) {
	root := t.TempDir()
	writeProcs(t, root, "nginx", "", "100\n")
	var mu sync.Mutex
	var starts [][]uint32
	run := func(ctx context.Context, cfg *obi.Config) error {
		mu.Lock()
		var pids []uint32
		for _, e := range cfg.Discovery.Instrument {
			pids = append(pids, e.PIDs...)
		}
		starts = append(starts, pids)
		mu.Unlock()
		<-ctx.Done()
		return nil
	}
	r := &hostOBIRunner{
		namespace: "web", cgroupRoot: root, units: func() []string { return []string{"nginx"} },
		store: newOBIMetricsStore(), run: run, poll: 10 * time.Millisecond, log: logr.Discard(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = r.Run(ctx); close(done) }()

	waitStarts := func(n int) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			got := len(starts)
			mu.Unlock()
			if got >= n {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("instrumenter starts < %d", n)
	}
	waitStarts(1)
	writeProcs(t, root, "nginx", "", "100\n300\n")
	waitStarts(2)
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if len(starts[0]) != 1 || len(starts[1]) != 2 {
		t.Errorf("starts = %v", starts)
	}
}

func TestHostOBIRunnerSurvivesInstrumenterFailure(t *testing.T) {
	root := t.TempDir()
	writeProcs(t, root, "nginx", "", "100\n")
	calls := make(chan struct{}, 8)
	r := &hostOBIRunner{
		namespace: "web", cgroupRoot: root, units: func() []string { return []string{"nginx"} },
		store: newOBIMetricsStore(), poll: time.Hour, retry: 10 * time.Millisecond, log: logr.Discard(),
		run: func(context.Context, *obi.Config) error {
			calls <- struct{}{}
			return os.ErrPermission
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	<-calls
	<-calls
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run returned %v; a failing instrumenter must not end the agent", err)
	}
}
