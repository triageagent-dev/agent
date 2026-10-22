// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package nodeexporter

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/usemetrics"
)

// Host mode USE (docs/adr/0018-linux-host-mode.md, phase 3): the same series
// the cluster exporter emits, read from the cgroup of each selected systemd
// unit under <cgroup-root>/system.slice/<unit>.service. A unit is one pod and
// one service of the synthetic namespace. Limits come from the unit's own
// cpu.max and memory.max only: a unit without them reports usage against the
// node alone, exactly as an unlimited pod does in a cluster.

// HostOptions configures a HostExporter.
type HostOptions struct {
	// NodeName is the k8s.node.name resource attribute: the host's cluster name.
	NodeName string
	// Namespace is the synthetic namespace every unit is reported under.
	Namespace string
	// Units returns the unit names currently selected, with or without the
	// ".service" suffix. It is called on every Collect. The exporter does not
	// discover units itself.
	Units func() []string
	// CgroupRoot, ProcRoot and SysRoot default like Options.
	CgroupRoot, ProcRoot, SysRoot string
}

// HostExporter is the host-mode counterpart of Exporter. It implements
// sensorstream.RoleSource.
type HostExporter struct {
	opts         HostOptions
	memTotal     float64
	memTotalOK   bool
	cpuCores     float64
	unitsInSlice string
}

// NewHost builds a HostExporter. Node capacity is resolved once, as in New.
func NewHost(o HostOptions) *HostExporter {
	if o.CgroupRoot == "" {
		o.CgroupRoot = "/sys/fs/cgroup"
	}
	if o.ProcRoot == "" {
		o.ProcRoot = "/proc"
	}
	if o.SysRoot == "" {
		o.SysRoot = "/sys"
	}
	mem, ok := resolveNodeMemTotalBytes(o.ProcRoot)
	cores, _ := resolveNodeCPUCores(o.SysRoot, o.ProcRoot)
	return &HostExporter{
		opts: o, memTotal: float64(mem), memTotalOK: ok, cpuCores: float64(cores),
		unitsInSlice: filepath.Join(o.CgroupRoot, "system.slice"),
	}
}

// Collect reads every selected unit's cgroup. A unit whose directory is
// missing (stopped, or not yet started) is skipped, not an error.
func (e *HostExporter) Collect(_ context.Context) ([]*metricspb.ResourceMetrics, error) {
	var units []string
	if e.opts.Units != nil {
		units = e.opts.Units()
	}
	names := make([]string, 0, len(units))
	seen := map[string]bool{}
	for _, u := range units {
		if n := strings.TrimSuffix(u, ".service"); n != "" && !seen[n] {
			seen[n] = true
			names = append(names, n)
		}
	}
	sort.Strings(names)

	b := metricset.NewBuilder("k8s.node.name", e.opts.NodeName)
	for _, name := range names {
		dir := filepath.Join(e.unitsInSlice, name+".service")
		if st, err := os.Stat(dir); err != nil || !st.IsDir() {
			continue
		}
		e.collectUnit(b, dir, metricset.KV(usemetrics.LabelNamespace, e.opts.Namespace,
			usemetrics.LabelService, name, usemetrics.LabelPod, name))
	}
	return b.ResourceMetrics(), nil
}

func (e *HostExporter) collectUnit(b *metricset.Builder, dir string, lbl []*commonpb.KeyValue) {
	for _, p := range []struct{ file, metric string }{
		{"cpu.pressure", usemetrics.MetricCPUPsiAvg60},
		{"memory.pressure", usemetrics.MetricMemPsiAvg60},
		{"io.pressure", usemetrics.MetricIOPsiAvg60},
	} {
		if v, ok := readPSIAvg60(filepath.Join(dir, p.file)); ok {
			b.Gauge(p.metric, lbl, v)
		}
	}
	// No pod spec on a host: the cgroup is the only source of a limit.
	if v, ok := readMemPct(dir, nil); ok {
		b.Gauge(usemetrics.MetricMemPct, lbl, v)
	}
	if v, ok := readMemWorkingSetPct(dir, nil); ok {
		b.Gauge(usemetrics.MetricMemWorkingSetPct, lbl, v)
	}
	if v, ok := readMemWorkingSetBytes(dir); ok {
		b.Gauge(usemetrics.MetricMemBytes, lbl, v)
	}
	if v, ok := memLimitBytes(dir, nil); ok {
		b.Gauge(usemetrics.MetricMemLimitBytes, lbl, float64(v))
	}
	if v, ok := readOOMKill(dir); ok {
		b.Counter(usemetrics.MetricOOMKillTotal, lbl, v)
	}
	if stat, ok := readCPUStat(dir); ok {
		b.Counter(usemetrics.MetricCPUUsageUsecTotal, lbl, stat.usageUsec)
		b.Counter(usemetrics.MetricCPUNrPeriodsTotal, lbl, stat.nrPeriods)
		b.Counter(usemetrics.MetricCPUNrThrottledTotal, lbl, stat.nrThrottled)
	}
	if v, ok := readCPULimitCores(dir, nil); ok {
		b.Gauge(usemetrics.MetricCPULimitCores, lbl, v)
	}
	if e.memTotalOK {
		b.Gauge(usemetrics.MetricMemNodeTotalBytes, lbl, e.memTotal)
	}
	b.Gauge(usemetrics.MetricCPUNodeCores, lbl, e.cpuCores)
}

// defaultHostDenylist names the services never selected when --host-units is
// empty: the host's own plumbing and the agent itself.
var defaultHostDenylist = []string{
	"systemd-*", "dbus", "ssh*", "cron", "getty@*", "polkit", "udisks2", "snapd*", "triage-agent",
}

// SystemSliceUnits returns a unit-list function that lists the service
// cgroups present under <cgroupRoot>/system.slice. With no globs it selects
// every service except the default denylist; with globs it selects exactly the
// units a glob matches (against the name with or without ".service"), and the
// denylist does not apply, so an explicit pattern is honoured. The host
// runtime uses the systemd discoverer instead; this is the fallback for a host
// without D-Bus and for tests.
func SystemSliceUnits(cgroupRoot string, globs []string) func() []string {
	slice := filepath.Join(cgroupRoot, "system.slice")
	return func() []string {
		entries, err := os.ReadDir(slice)
		if err != nil {
			return nil
		}
		var out []string
		for _, ent := range entries {
			full := ent.Name()
			name, isSvc := strings.CutSuffix(full, ".service")
			if !ent.IsDir() || !isSvc || name == "" {
				continue
			}
			if len(globs) == 0 {
				if !matchAny(defaultHostDenylist, name) {
					out = append(out, name)
				}
			} else if matchAny(globs, name) || matchAny(globs, full) {
				out = append(out, name)
			}
		}
		return out
	}
}

func matchAny(globs []string, s string) bool {
	for _, g := range globs {
		if ok, err := path.Match(g, s); err == nil && ok {
			return true
		}
	}
	return false
}
