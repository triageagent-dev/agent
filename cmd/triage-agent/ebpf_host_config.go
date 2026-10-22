// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

// Host mode's OBI config and process-to-unit helpers (ADR-0018 phase 5).
// OS-agnostic and untagged, like ebpf_config.go, so the tests run on any dev
// host; the instrumenter itself is started from ebpf_host_obi.go.
package main

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// hostUnitPIDs is one selected unit and the processes in its cgroup.
type hostUnitPIDs struct {
	Name string   // the unit's app name, without .service
	PIDs []uint32 // sorted, deduplicated
}

// hostSelection is the set of units OBI instruments, sorted by name.
type hostSelection []hostUnitPIDs

// maxCgroupDepth bounds the walk below a unit's cgroup (a service with
// Delegate=yes can have sub-cgroups; deeper trees are not a service's own).
const maxCgroupDepth = 4

// readUnitPIDs returns every PID in the unit's cgroup and its sub-cgroups,
// from <cgroupRoot>/system.slice/<unit>.service. A stopped unit has no
// directory and no PIDs.
func readUnitPIDs(cgroupRoot, unit string) []uint32 {
	root := filepath.Join(cgroupRoot, "system.slice", unit+".service")
	seen := map[uint32]bool{}
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			if rel, rerr := filepath.Rel(root, p); rerr == nil && rel != "." &&
				strings.Count(rel, string(filepath.Separator)) >= maxCgroupDepth {
				return fs.SkipDir
			}
			return nil
		}
		if d.Name() != "cgroup.procs" {
			return nil
		}
		f, oerr := os.Open(p)
		if oerr != nil {
			return nil
		}
		defer func() { _ = f.Close() }()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if pid, perr := strconv.ParseUint(strings.TrimSpace(sc.Text()), 10, 32); perr == nil && pid > 0 {
				seen[uint32(pid)] = true
			}
		}
		return nil
	})
	out := make([]uint32, 0, len(seen))
	for pid := range seen {
		out = append(out, pid)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// readSelection reads the cgroup PIDs of each named unit.
func readSelection(cgroupRoot string, units []string) hostSelection {
	names := append([]string(nil), units...)
	sort.Strings(names)
	out := make(hostSelection, 0, len(names))
	var prev string
	for _, n := range names {
		if n == "" || n == prev {
			continue
		}
		prev = n
		out = append(out, hostUnitPIDs{Name: n, PIDs: readUnitPIDs(cgroupRoot, n)})
	}
	return out
}

// hostOBIYAML builds the OBI config host mode loads. kubernetes.enable is
// false, so OBI sets no k8s.* attribute and drops the k8s name resolver on its
// own; every unit with processes becomes one discovery.instrument entry that
// selects its PIDs and pins name=<unit> and namespace=<namespace>
// (service.name / service.namespace). A unit with no PIDs yet has no entry.
//
// excluded_linux_system_paths is emptied: it only keeps OBI from detecting the
// language of binaries under /usr/sbin, /usr/libexec and similar, which is
// where distro packages put the daemons that units run. Selection is by PID,
// so nothing else is pulled in. The default exclusions (OBI itself and
// otelcol) stay.
//
// networkFlows adds the L4 flow metric exactly as the cluster config does
// (the deprecated network.enable toggle, which ORs onto the RED features).
// Without Kubernetes metadata a flow carries addresses only, no owner names.
func hostOBIYAML(namespace string, sel hostSelection, networkFlows bool) string {
	var sb strings.Builder
	sb.WriteString("discovery:\n  excluded_linux_system_paths: []\n")
	n := 0
	for _, u := range sel {
		if len(u.PIDs) == 0 {
			continue
		}
		if n == 0 {
			sb.WriteString("  instrument:\n")
		}
		n++
		pids := make([]string, len(u.PIDs))
		for i, p := range u.PIDs {
			pids[i] = strconv.FormatUint(uint64(p), 10)
		}
		fmt.Fprintf(&sb, "    - target_pids: [%s]\n      name: %q\n      namespace: %q\n",
			strings.Join(pids, ", "), u.Name, namespace)
	}
	sb.WriteString("attributes:\n  kubernetes:\n    enable: false\n")
	if networkFlows {
		sb.WriteString("network:\n  enable: true\n  source: socket_filter\n  guess_ports: ordinal\n")
	}
	sb.WriteString("routes:\n  unmatched: heuristic\n")
	sb.WriteString("otel_metrics_export:\n  interval: 15s\n")
	return sb.String()
}

// hostSelectionEmpty reports whether no unit has a process to instrument.
func hostSelectionEmpty(sel hostSelection) bool {
	for _, u := range sel {
		if len(u.PIDs) > 0 {
			return false
		}
	}
	return true
}

// pidTracker decides when the running OBI config is out of date. OBI reads
// target_pids once at start, so a PID it does not know needs a restart. To
// keep short-lived children (a shell a unit spawns, a cron-like helper) from
// restarting the instrumenter, a PID counts only after two consecutive polls
// saw it. A PID that vanished needs no restart: OBI drops a process when it
// exits. A unit that left the selection is left in place for the same reason.
type pidTracker struct {
	started bool
	applied map[string]map[uint32]bool
	last    map[string]map[uint32]bool
}

func pidSets(sel hostSelection) map[string]map[uint32]bool {
	out := make(map[string]map[uint32]bool, len(sel))
	for _, u := range sel {
		set := make(map[uint32]bool, len(u.PIDs))
		for _, p := range u.PIDs {
			set[p] = true
		}
		out[u.Name] = set
	}
	return out
}

// observe takes the current selection and reports whether the instrumenter
// must restart with it. The first observation always does.
func (t *pidTracker) observe(cur hostSelection) bool {
	now := pidSets(cur)
	defer func() { t.last = now }()
	if !t.started {
		t.started = true
		t.applied = now
		return true
	}
	for unit, pids := range now {
		for pid := range pids {
			if !t.applied[unit][pid] && t.last[unit][pid] {
				t.applied = now
				return true
			}
		}
	}
	return false
}

// unitFromCgroup returns the systemd service a /proc/<pid>/cgroup file names:
// the first path segment ending in .service, without the suffix.
func unitFromCgroup(content string) (string, bool) {
	for _, line := range strings.Split(content, "\n") {
		// "0::/system.slice/nginx.service" (v2) or "1:name=systemd:/..." (v1).
		i := strings.LastIndex(line, ":")
		if i < 0 {
			continue
		}
		for _, seg := range strings.Split(line[i+1:], "/") {
			if name, ok := strings.CutSuffix(seg, ".service"); ok && name != "" {
				return name, true
			}
		}
	}
	return "", false
}

// procUnit maps a PID to its unit through <procRoot>/<pid>/cgroup.
func procUnit(procRoot string, pid int) (string, bool) {
	raw, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "cgroup"))
	if err != nil {
		return "", false
	}
	return unitFromCgroup(string(raw))
}
