//go:build obi

// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

func obiExport(service string, value int64) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", service)
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("requests")
	sum := m.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.DataPoints().AppendEmpty().SetIntValue(value)
	return md
}

func TestOBIMetricsStore_KeepsLatestPerResourceAndExpires(t *testing.T) {
	now := time.Unix(1000, 0)
	s := newOBIMetricsStore()
	s.now = func() time.Time { return now }
	ctx := context.Background()
	c, err := s.consumer()
	if err != nil {
		t.Fatal(err)
	}

	for _, md := range []pmetric.Metrics{obiExport("api", 1), obiExport("web", 5), obiExport("api", 2)} {
		if err := c.ConsumeMetrics(ctx, md); err != nil {
			t.Fatal(err)
		}
	}
	rms, _ := s.Collect(ctx)
	if len(rms) != 2 {
		t.Fatalf("resources = %d, want one per service", len(rms))
	}
	for _, rm := range rms {
		name := rm.GetResource().GetAttributes()[0].GetValue().GetStringValue()
		v := rm.GetScopeMetrics()[0].GetMetrics()[0].GetSum().GetDataPoints()[0].GetAsInt()
		if name == "api" && v != 2 {
			t.Errorf("api = %d, want the latest export", v)
		}
	}

	now = now.Add(obiResourceTTL + time.Second)
	if err := c.ConsumeMetrics(ctx, obiExport("web", 6)); err != nil {
		t.Fatal(err)
	}
	rms, _ = s.Collect(ctx)
	if len(rms) != 1 {
		t.Fatalf("resources = %d, want the silent service dropped", len(rms))
	}

	s.reset()
	if rms, _ := s.Collect(ctx); len(rms) != 0 {
		t.Error("reset must drop everything")
	}
}
