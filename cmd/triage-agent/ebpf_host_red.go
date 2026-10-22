// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

// The host-mode "ebpf" role (ADR-0018 phase 5): the OBI runner that follows the
// selected units, and the source wrapper that gives its series the identity the
// RED collector keys on. Untagged so both are testable on any dev host.
package main

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/obi/pkg/obi"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"github.com/triageagent-dev/agent/hostunits"
	"github.com/triageagent-dev/agent/sensorstream"
)

const (
	attrServiceName      = "service.name"
	attrServiceNamespace = "service.namespace"
	attrServiceInstance  = "service.instance.id"
	attrK8sNamespace     = "k8s.namespace.name"
)

// maxPIDCache bounds the PID-to-unit memory of hostREDSource.
const maxPIDCache = 4096

// hostREDSource wraps the OBI metrics store. RED keys on k8s.namespace.name and
// service.name, and OBI without Kubernetes sets neither the first nor, for a
// process that names itself, the second. Each resource is attributed to a
// unit, then stamped k8s.namespace.name=<host namespace> and service.name=
// <unit>, on the resource and on every data point that carries service.name,
// because a data point's attribute wins over its resource's.
//
// A resource is attributed by, in order: the PID in service.instance.id
// (<hostname>:<pid>) mapped through /proc/<pid>/cgroup, which holds even when
// the process sets OTEL_SERVICE_NAME; or service.namespace equal to the host
// namespace, as pinned on the instrument entry, whose service.name is the unit.
// A resource that matches neither (the network flow resource) passes untouched.
type hostREDSource struct {
	next      sensorstream.RoleSource
	namespace string
	unitOf    func(pid int) (string, bool)

	mu    sync.Mutex
	cache map[int]string
}

func newHostREDSource(next sensorstream.RoleSource, namespace string, unitOf func(pid int) (string, bool)) *hostREDSource {
	return &hostREDSource{next: next, namespace: namespace, unitOf: unitOf, cache: map[int]string{}}
}

// Collect implements sensorstream.RoleSource. The store's messages are shared
// with its next reader, so changed resources are cloned, never edited in place.
func (s *hostREDSource) Collect(ctx context.Context) ([]*metricspb.ResourceMetrics, error) {
	in, err := s.next.Collect(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*metricspb.ResourceMetrics, 0, len(in))
	for _, rm := range in {
		if unit, ok := s.unit(rm); ok {
			rm = s.stamp(rm, unit)
		}
		out = append(out, rm)
	}
	return out, nil
}

// StatusError passes the wrapped role's failure through.
func (s *hostREDSource) StatusError() string {
	if rs, ok := s.next.(sensorstream.RoleStatus); ok {
		return rs.StatusError()
	}
	return ""
}

func resourceAttr(rm *metricspb.ResourceMetrics, key string) string {
	for _, kv := range rm.GetResource().GetAttributes() {
		if kv.GetKey() == key {
			return kv.GetValue().GetStringValue()
		}
	}
	return ""
}

func (s *hostREDSource) unit(rm *metricspb.ResourceMetrics) (string, bool) {
	if id := resourceAttr(rm, attrServiceInstance); id != "" {
		if pid, err := strconv.Atoi(id[strings.LastIndex(id, ":")+1:]); err == nil && pid > 0 {
			if u, ok := s.unitForPID(pid); ok {
				return u, true
			}
		}
	}
	if resourceAttr(rm, attrServiceNamespace) == s.namespace {
		if n := resourceAttr(rm, attrServiceName); n != "" {
			return n, true
		}
	}
	return "", false
}

// unitForPID remembers a resolved PID, so the last series of a process that
// has since exited keep their unit until the store's TTL drops them.
func (s *hostREDSource) unitForPID(pid int) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if u, ok := s.cache[pid]; ok {
		return u, true
	}
	u, ok := s.unitOf(pid)
	if !ok {
		return "", false
	}
	if len(s.cache) >= maxPIDCache {
		s.cache = map[int]string{}
	}
	s.cache[pid] = u
	return u, true
}

func (s *hostREDSource) stamp(rm *metricspb.ResourceMetrics, unit string) *metricspb.ResourceMetrics {
	c := proto.Clone(rm).(*metricspb.ResourceMetrics)
	if c.Resource == nil {
		c.Resource = &resourcepb.Resource{}
	}
	c.Resource.Attributes = setStr(c.Resource.Attributes, attrK8sNamespace, s.namespace)
	c.Resource.Attributes = setStr(c.Resource.Attributes, attrServiceName, unit)
	for _, sm := range c.GetScopeMetrics() {
		for _, m := range sm.GetMetrics() {
			for _, dpAttrs := range dataPointAttrs(m) {
				if hasAttr(*dpAttrs, attrServiceName) {
					*dpAttrs = setStr(*dpAttrs, attrServiceName, unit)
				}
				if hasAttr(*dpAttrs, attrK8sNamespace) {
					*dpAttrs = setStr(*dpAttrs, attrK8sNamespace, s.namespace)
				}
			}
		}
	}
	return c
}

// dataPointAttrs returns a pointer to each data point's attribute slice.
func dataPointAttrs(m *metricspb.Metric) []*[]*commonpb.KeyValue {
	var out []*[]*commonpb.KeyValue
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Sum:
		for _, dp := range d.Sum.GetDataPoints() {
			out = append(out, &dp.Attributes)
		}
	case *metricspb.Metric_Gauge:
		for _, dp := range d.Gauge.GetDataPoints() {
			out = append(out, &dp.Attributes)
		}
	case *metricspb.Metric_Histogram:
		for _, dp := range d.Histogram.GetDataPoints() {
			out = append(out, &dp.Attributes)
		}
	}
	return out
}

func hasAttr(kvs []*commonpb.KeyValue, key string) bool {
	for _, kv := range kvs {
		if kv.GetKey() == key {
			return true
		}
	}
	return false
}

func setStr(kvs []*commonpb.KeyValue, key, val string) []*commonpb.KeyValue {
	v := &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: val}}
	for _, kv := range kvs {
		if kv.GetKey() == key {
			kv.Value = v
			return kvs
		}
	}
	return append(kvs, &commonpb.KeyValue{Key: key, Value: v})
}

// hostOBIRunner runs the embedded instrumenter over the selected units and
// restarts it when a unit gets processes OBI does not know. OBI exposes a
// runtime PID selector, but passing one turns the network and stats agents on
// whatever the config says, so the plain static config with a restart is used
// instead (docs/adr/0018-linux-host-mode.md, phase 5).
type hostOBIRunner struct {
	namespace    string
	cgroupRoot   string
	networkFlows bool
	units        func() []string
	store        *obiMetricsStore
	// run starts the instrumenter and blocks until it stops; ebpf_host_obi.go
	// supplies the real one, tests a fake.
	run   func(ctx context.Context, cfg *obi.Config) error
	poll  time.Duration
	retry time.Duration
	log   logr.Logger
}

// Run follows the selection until ctx ends. A failing instrumenter is logged
// and retried, never returned: RED must not take the host's USE and LOG down.
func (r *hostOBIRunner) Run(ctx context.Context) error {
	if r.poll <= 0 {
		r.poll = hostunits.PollInterval
	}
	if r.retry <= 0 {
		r.retry = 30 * time.Second
	}
	var tr pidTracker
	sel := readSelection(r.cgroupRoot, r.units())
	tr.observe(sel)
	tick := time.NewTicker(r.poll)
	defer tick.Stop()
	for ctx.Err() == nil {
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		if hostSelectionEmpty(sel) && !r.networkFlows {
			// No process to follow: OBI would return at once. Drop the old
			// series and wait for a unit to start.
			r.store.reset()
			go func() { <-runCtx.Done(); done <- nil }()
		} else if cfg, err := r.config(sel); err != nil {
			r.log.Error(err, "host OBI config rejected")
			go func() { <-runCtx.Done(); done <- nil }()
		} else {
			r.log.Info("host OBI starting", "units", len(sel), "processes", countPIDs(sel))
			go func() { done <- r.run(runCtx, cfg) }()
		}

		restart := false
		failed := false
		for !restart && !failed {
			select {
			case <-ctx.Done():
				cancel()
				<-done
				return nil
			case err := <-done:
				if err != nil && ctx.Err() == nil {
					r.log.Error(err, "host OBI stopped; retrying", "after", r.retry.String())
				}
				failed = true
			case <-tick.C:
				next := readSelection(r.cgroupRoot, r.units())
				if tr.observe(next) {
					r.log.Info("host OBI selection changed, restarting", "units", len(next))
					sel = next
					restart = true
				}
			}
		}
		cancel()
		if !failed {
			<-done
		} else {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(r.retry):
			}
			sel = readSelection(r.cgroupRoot, r.units())
			tr.observe(sel)
		}
	}
	return nil
}

func (r *hostOBIRunner) config(sel hostSelection) (*obi.Config, error) {
	cfg, err := obi.LoadConfig(strings.NewReader(hostOBIYAML(r.namespace, sel, r.networkFlows)))
	if err != nil {
		return nil, err
	}
	if err := applyOBILogLevel(cfg); err != nil {
		return nil, err
	}
	cfg.InternalMetrics.Reporter = obiAttach
	if err := r.store.exportTo(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func countPIDs(sel hostSelection) int {
	n := 0
	for _, u := range sel {
		n += len(u.PIDs)
	}
	return n
}

// newHostRED returns the host's "ebpf" role source and its background loop.
func newHostRED(o hostOptions, units func() []string, run func(context.Context, *obi.Config) error, log logr.Logger) (sensorstream.RoleSource, func(context.Context) error) {
	store := newOBIMetricsStore()
	r := &hostOBIRunner{
		namespace: o.namespace, cgroupRoot: o.cgroupRoot, networkFlows: os.Getenv("OBI_NETWORK_FLOWS") == "true",
		units: units, store: store, run: run, log: log.WithName("host-obi"),
	}
	src := newHostREDSource(store, o.namespace, func(pid int) (string, bool) { return procUnit("/proc", pid) })
	return src, r.Run
}
