package nodeexporter

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// resolveNodeMemTotalBytes reads MemTotal from /proc/meminfo (kB, per the
// documented /proc/meminfo interface) and returns it in bytes. ok is false when
// the file is unreadable or the key is missing/unparsable — a node whose /proc
// doesn't look like this (essentially never on a real cgroup v2 host) omits the
// node-capacity gauge entirely rather than reporting a fabricated 0, so a
// service's "% of node" figure can never render a nonsensical spike against a
// zero denominator.
//
// Deliberately NOT read from the k8s Node object: that would require a
// cluster-API round trip and tie this exporter's node-capacity reading to
// Kubernetes at all, defeating the entire point of this pipeline — cgroup v2
// and /proc work identically on bare metal, plain containers, and anything else
// with a Linux kernel, which is the portability this metric exists for.
func resolveNodeMemTotalBytes(procRoot string) (bytes uint64, ok bool) {
	f, err := os.Open(filepath.Join(procRoot, "meminfo"))
	if err != nil {
		return 0, false
	}
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		// "MemTotal:       16374420 kB" — fields[0] is the key, fields[1] the
		// value, fields[2] the unit. /proc/meminfo has reported this line in kB
		// since long before cgroup v2 existed; no other unit has ever shipped.
		if len(fields) < 2 {
			return 0, false
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, false
		}
		return kb * 1024, true
	}
	return 0, false
}

// resolveNodeCPUCores resolves the node's online CPU core count, in priority
// order:
//
//  1. /sys/devices/system/cpu/online — the kernel's own list of online CPUs, as
//     a range list ("0-7", or "0,2-3" when a CPU has been hot-unplugged). This is
//     ground truth for what's actually schedulable on the node.
//  2. Counting "processor" lines in /proc/cpuinfo, when sysfs is absent or
//     unparsable (some container/VM environments restrict /sys but still expose
//     /proc/cpuinfo).
//  3. runtime.NumCPU(), which never fails, so this function always resolves —
//     but note it reports the CALLING PROCESS's visible CPU count (possibly
//     narrowed by a cgroup cpuset), not necessarily the true node-wide count,
//     which is exactly why it is the last resort rather than the first choice.
//
// source names which level actually won, so the caller can log it: a node
// whose /sys is restricted enough to fall all the way through to
// runtime.NumCPU() is producing a different, and possibly wrong, denominator
// than the first two levels would — worth seeing in the startup log rather
// than discovering later from a surprising node percentage.
func resolveNodeCPUCores(sysRoot, procRoot string) (cores int, source string) {
	if n, ok := readCPUOnlineRange(filepath.Join(sysRoot, "devices", "system", "cpu", "online")); ok {
		return n, "sysfs-online"
	}
	if n, ok := countCPUInfoProcessors(filepath.Join(procRoot, "cpuinfo")); ok {
		return n, "cpuinfo"
	}
	return runtime.NumCPU(), "runtime.NumCPU"
}

// readCPUOnlineRange reads and parses a CPU range-list file (only
// /sys/devices/system/cpu/online in practice, but takes a path for testing).
func readCPUOnlineRange(path string) (int, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	return parseCPUOnlineRange(string(data))
}

// parseCPUOnlineRange parses the kernel's CPU range-list format (used by
// /sys/devices/system/cpu/online and its sibling files under
// /sys/devices/system/cpu/): a comma-separated list of either a single CPU
// index ("3") or an inclusive range ("0-7"). Returns the total COUNT of CPU
// indices named, not the highest index — "0,2-3" is 3 cores (0, 2, 3), not 4,
// which is what makes this format able to represent a hot-unplugged CPU in the
// middle of the range. ok is false for an empty, whitespace-only, or
// unparsable list.
func parseCPUOnlineRange(s string) (count int, ok bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	total := 0
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lo, hi, isRange := strings.Cut(part, "-")
		if !isRange {
			if _, err := strconv.Atoi(lo); err != nil {
				return 0, false
			}
			total++
			continue
		}
		loN, errLo := strconv.Atoi(lo)
		hiN, errHi := strconv.Atoi(hi)
		if errLo != nil || errHi != nil || hiN < loN {
			return 0, false
		}
		total += hiN - loN + 1
	}
	if total == 0 {
		return 0, false
	}
	return total, true
}

// countCPUInfoProcessors counts "processor" key lines in /proc/cpuinfo — one
// per logical CPU — the fallback used when /sys/devices/system/cpu/online is
// unavailable or unparsable.
func countCPUInfoProcessors(path string) (int, bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer func() { _ = f.Close() }()

	count := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		key, _, found := strings.Cut(sc.Text(), ":")
		if !found {
			continue
		}
		if strings.TrimSpace(key) == "processor" {
			count++
		}
	}
	if count == 0 {
		return 0, false
	}
	return count, true
}
