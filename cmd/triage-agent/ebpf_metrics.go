//go:build obi

// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

// The "ebpf" role's metrics half, kept out of ebpf_obi.go's `obi && linux`
// build tag so it is testable on the darwin dev host.
//
// The embedded OBI instrumenter exports OTLP in process through its
// MetricsConsumer (otelcfg.MetricsConfig), with cumulative temporality. OBI
// runs one meter provider per instrumented service plus one for network
// flows, so each export carries one resource. obiMetricsStore keeps the
// latest export per resource, and the sensor stream reads all of them each
// tick (docs/adr/0015-sensor-stream-grpc.md). Nothing listens.
package main

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/obi/pkg/obi"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

// obiResourceTTL drops a resource that stopped exporting, such as a service
// that went away, so its last cumulative values do not linger.
const obiResourceTTL = 3 * time.Minute

type obiResource struct {
	rm *metricspb.ResourceMetrics
	at time.Time
}

// obiMetricsStore is the latest OTLP export per resource.
type obiMetricsStore struct {
	mu        sync.Mutex
	resources map[string]obiResource
	now       func() time.Time
}

func newOBIMetricsStore() *obiMetricsStore {
	return &obiMetricsStore{resources: map[string]obiResource{}, now: time.Now}
}

// obiMetrics is the process's store: runEBPF hands its consumer to OBI and
// the sensor stream collects from it.
var obiMetrics = newOBIMetricsStore()

// exportTo points OBI's metrics at the store and turns OBI's own Prometheus
// listener off. Call before cfg.Validate: OBI requires an exporter.
func (s *obiMetricsStore) exportTo(cfg *obi.Config) error {
	c, err := s.consumer()
	if err != nil {
		return err
	}
	cfg.OTELMetrics.MetricsConsumer = c
	cfg.Prometheus.Port = 0
	return nil
}

// consumer returns the OBI MetricsConsumer that feeds the store.
func (s *obiMetricsStore) consumer() (consumer.Metrics, error) {
	return consumer.NewMetrics(s.consume)
}

func (s *obiMetricsStore) consume(_ context.Context, md pmetric.Metrics) error {
	raw, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	if err != nil {
		return err
	}
	var req colmetricspb.ExportMetricsServiceRequest
	if err := proto.Unmarshal(raw, &req); err != nil {
		return err
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rm := range req.GetResourceMetrics() {
		s.resources[resourceKey(rm.GetResource().GetAttributes())] = obiResource{rm: rm, at: now}
	}
	return nil
}

// reset drops everything, for when the instrumenter stops.
func (s *obiMetricsStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resources = map[string]obiResource{}
}

// Collect implements sensorstream.RoleSource.
func (s *obiMetricsStore) Collect(context.Context) ([]*metricspb.ResourceMetrics, error) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.resources))
	for k, r := range s.resources {
		if now.Sub(r.at) > obiResourceTTL {
			delete(s.resources, k)
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*metricspb.ResourceMetrics, 0, len(keys))
	for _, k := range keys {
		out = append(out, s.resources[k].rm)
	}
	return out, nil
}

func resourceKey(kvs []*commonpb.KeyValue) string {
	pairs := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		pairs = append(pairs, kv.GetKey()+"="+kv.GetValue().String())
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}
