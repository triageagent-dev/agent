// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package nodeexporter

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/usemetrics"
)

const hostFixture = "testdata/host"

func hostExporter(units ...string) *HostExporter {
	return NewHost(HostOptions{
		NodeName: "web-01", Namespace: "host",
		Units:      func() []string { return units },
		CgroupRoot: hostFixture + "/cgroup", ProcRoot: hostFixture + "/proc", SysRoot: hostFixture + "/sys",
	})
}

// byService maps service -> metric name -> value.
func byService(t *testing.T, e *HostExporter) map[string]map[string]float64 {
	t.Helper()
	rms, err := e.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]float64{}
	for name, m := range metricset.FromOTLP(rms) {
		for _, p := range m.Points {
			if p.Attr(usemetrics.LabelNamespace) != "host" || p.Attr(usemetrics.LabelPod) != p.Attr(usemetrics.LabelService) {
				t.Errorf("%s: bad labels %v", name, p)
			}
			svc := p.Attr(usemetrics.LabelService)
			if out[svc] == nil {
				out[svc] = map[string]float64{}
			}
			out[svc][name] = p.Value
		}
	}
	return out
}

func TestHostExporter_LimitedUnit(t *testing.T) {
	got := byService(t, hostExporter("app.service"))["app"]
	want := map[string]float64{
		usemetrics.MetricCPUPsiAvg60:         61.5,
		usemetrics.MetricMemPsiAvg60:         3.25,
		usemetrics.MetricIOPsiAvg60:          0.5,
		usemetrics.MetricMemPct:              40,
		usemetrics.MetricMemWorkingSetPct:    30,
		usemetrics.MetricMemBytes:            300000000,
		usemetrics.MetricMemLimitBytes:       1e9,
		usemetrics.MetricOOMKillTotal:        1,
		usemetrics.MetricCPUUsageUsecTotal:   5e6,
		usemetrics.MetricCPUNrPeriodsTotal:   100,
		usemetrics.MetricCPUNrThrottledTotal: 7,
		usemetrics.MetricCPULimitCores:       0.5,
		usemetrics.MetricMemNodeTotalBytes:   8000000 * 1024,
		usemetrics.MetricCPUNodeCores:        4,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("metrics = %v\nwant     %v", got, want)
	}
}

func TestHostExporter_UnlimitedUnitReportsNodeRelativeOnly(t *testing.T) {
	got := byService(t, hostExporter("free"))["free"]
	for _, absent := range []string{
		usemetrics.MetricMemPct, usemetrics.MetricMemWorkingSetPct, usemetrics.MetricMemLimitBytes,
		usemetrics.MetricCPULimitCores, usemetrics.MetricMemPsiAvg60, usemetrics.MetricIOPsiAvg60,
		usemetrics.MetricMemRequestBytes, usemetrics.MetricCPURequestCores,
	} {
		if _, ok := got[absent]; ok {
			t.Errorf("%s emitted for an unlimited unit", absent)
		}
	}
	for name, want := range map[string]float64{
		usemetrics.MetricMemBytes: 150000000, usemetrics.MetricCPUUsageUsecTotal: 900,
		usemetrics.MetricCPUPsiAvg60: 1, usemetrics.MetricMemNodeTotalBytes: 8000000 * 1024,
		usemetrics.MetricCPUNodeCores: 4,
	} {
		if got[name] != want {
			t.Errorf("%s = %v, want %v", name, got[name], want)
		}
	}
}

func TestHostExporter_MissingUnitAndDuplicates(t *testing.T) {
	got := byService(t, hostExporter("gone.service", "app", "app.service", ""))
	if len(got) != 1 || got["app"] == nil {
		t.Errorf("services = %v, want only app", got)
	}
	if rms, err := hostExporter().Collect(context.Background()); err != nil || len(metricset.FromOTLP(rms)) != 0 {
		t.Errorf("no units: rms=%v err=%v", rms, err)
	}
	if rms, err := NewHost(HostOptions{CgroupRoot: t.TempDir()}).Collect(context.Background()); err != nil || len(metricset.FromOTLP(rms)) != 0 {
		t.Errorf("nil Units: rms=%v err=%v", rms, err)
	}
}

func TestSystemSliceUnits(t *testing.T) {
	root := hostFixture + "/cgroup"
	tests := []struct {
		name  string
		globs []string
		want  []string
	}{
		{"default denylist", nil, []string{"app", "free"}},
		{"glob without suffix", []string{"a*"}, []string{"app"}},
		{"glob with suffix", []string{"free.service"}, []string{"free"}},
		{"explicit glob beats denylist", []string{"ssh*"}, []string{"ssh", "sshd"}},
		{"no match", []string{"nginx"}, nil},
		{"slice units are not services", []string{"not-a-service*"}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SystemSliceUnits(root, tc.globs)()
			sort.Strings(got)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
	if got := SystemSliceUnits(t.TempDir(), nil)(); got != nil {
		t.Errorf("no system.slice: got %v", got)
	}
}
