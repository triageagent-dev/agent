package nodeexporter

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// kubepodsSlice locates the systemd kubepods slice hierarchy under a cgroup v2
// root: dir is the directory that directly contains "<prefix>.slice", and
// prefix is the slice-name prefix used at every level below it.
//
// Default layout (kubelet --cgroup-root unset, i.e. "/"): dir is root itself
// and prefix is "kubepods" — root/kubepods.slice.
//
// A kubelet started with --cgroup-root=/<name> (kind's node default is
// "/kubelet") nests the kubepods slice one level deeper, under a slice named
// after that root, and systemd prefixes every descendant slice name with the
// full parent path joined by "-": dir is root/<name>.slice and prefix is
// "<name>-kubepods" — root/<name>.slice/<name>-kubepods.slice/<name>-kubepods-<qos>.slice/...
type kubepodsSlice struct {
	dir    string
	prefix string
}

// discoverKubepodsSlice resolves the kubepodsSlice for root. It is a bounded
// directory-listing probe, done once and cached by the caller, never a
// per-collect tree walk: it checks the default location first, then lists root
// (one level, not recursive) for a "*.slice" directory whose own contents
// include "<name>-kubepods.slice".
//
// Falls back to the default dir/prefix when neither is found (unreadable
// root, or a layout this function doesn't recognize), which reproduces the
// exact candidates this package generated before kubepodsSlice existed.
func discoverKubepodsSlice(root string) kubepodsSlice {
	def := kubepodsSlice{dir: root, prefix: "kubepods"}
	if fi, err := os.Stat(filepath.Join(root, "kubepods.slice")); err == nil && fi.IsDir() {
		return def
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		return def
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasSuffix(e.Name(), ".slice") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".slice")
		prefix := name + "-kubepods"
		dir := filepath.Join(root, e.Name())
		if fi, err := os.Stat(filepath.Join(dir, prefix+".slice")); err == nil && fi.IsDir() {
			return kubepodsSlice{dir: dir, prefix: prefix}
		}
	}
	return def
}

// podCgroupDirCandidates returns, in priority order, the cgroup v2 directories a
// pod's cgroup MAY live at under root, covering both cgroup driver layouts
// Kubernetes uses (docs/adr/0010-use-metrics-node-exporter.md):
//
//   - systemd: sd.dir/sd.prefix.slice/sd.prefix-<qos>.slice/sd.prefix-<qos>-pod<uid_>.slice
//     (Guaranteed pods skip the <qos> level entirely: sd.dir/sd.prefix.slice/sd.prefix-pod<uid_>.slice)
//   - cgroupfs: root/kubepods/<qos>/pod<uid> (Guaranteed: root/kubepods/pod<uid>)
//
// sd is resolved once by discoverKubepodsSlice and passed in by the caller, so
// this function does no filesystem I/O of its own — it is pure string building.
//
// uid_ is the pod UID with "-" replaced by "_" (systemd slice-name convention);
// cgroupfs keeps the UID's dashes as-is. The caller (podCgroupDir) tries each
// candidate with os.Stat and uses the first that exists — this repo has no
// dependency on the kubelet's own --cgroup-driver flag, so probing beats trusting a
// guess.
func podCgroupDirCandidates(root string, sd kubepodsSlice, uid types.UID, qos corev1.PodQOSClass) []string {
	uidDash := string(uid)
	uidUnderscore := strings.ReplaceAll(uidDash, "-", "_")

	var qosSlice string // e.g. "burstable"; empty for Guaranteed
	switch qos {
	case corev1.PodQOSBurstable:
		qosSlice = "burstable"
	case corev1.PodQOSBestEffort:
		qosSlice = "besteffort"
	}

	var out []string
	if qosSlice == "" {
		out = append(out, filepath.Join(sd.dir, sd.prefix+".slice", fmt.Sprintf("%s-pod%s.slice", sd.prefix, uidUnderscore)))
		out = append(out, filepath.Join(root, "kubepods", fmt.Sprintf("pod%s", uidDash)))
	} else {
		out = append(out, filepath.Join(sd.dir, sd.prefix+".slice",
			fmt.Sprintf("%s-%s.slice", sd.prefix, qosSlice),
			fmt.Sprintf("%s-%s-pod%s.slice", sd.prefix, qosSlice, uidUnderscore)))
		out = append(out, filepath.Join(root, "kubepods", qosSlice, fmt.Sprintf("pod%s", uidDash)))
	}
	return out
}

// podCgroupDir returns the first candidate directory that actually exists, or ""
// if none do (the pod may have just terminated, or the node uses a cgroup layout
// this function doesn't recognize — either way, the caller skips the pod for this
// collect rather than erroring the whole call).
func podCgroupDir(root string, sd kubepodsSlice, uid types.UID, qos corev1.PodQOSClass) string {
	for _, dir := range podCgroupDirCandidates(root, sd, uid, qos) {
		if fi, err := os.Stat(dir); err == nil && fi.IsDir() {
			return dir
		}
	}
	return ""
}

// readPSIAvg60 parses a PSI file (cpu.pressure/memory.pressure/io.pressure) and
// returns the "some" line's avg60 field. PSI's text format is fixed-width lines:
//
//	some avg10=0.00 avg60=0.00 avg300=0.00 total=0
//	full avg10=0.00 avg60=0.00 avg300=0.00 total=0
//
// ok is false when the file is absent (CONFIG_PSI compiled out on this node) or
// unparsable — the caller omits the corresponding gauge entirely rather than
// reporting a misleading 0, per docs/adr/0010-use-metrics-node-exporter.md's
// graceful-degrade requirement.
func readPSIAvg60(path string) (val float64, ok bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "some ") {
			continue
		}
		for _, field := range strings.Fields(line)[1:] {
			k, v, found := strings.Cut(field, "=")
			if !found || k != "avg60" {
				continue
			}
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return 0, false
			}
			return f, true
		}
	}
	return 0, false
}

// memLimitBytes resolves the memory limit to measure a pod's utilization
// against, in bytes. ok is false only when the pod is genuinely unbounded.
//
// It prefers the pod cgroup's own memory.max, and falls back to the SUM of the
// pod spec's regular-container limits when that reads the literal "max".
//
// The fallback is not belt-and-braces, it is the common case for any pod
// carrying an unlimited sidecar or init container: kubelet sets a pod-level
// memory.max only when EVERY container in the pod — init containers included in
// the sizing calculation — declares a memory limit. One container without one
// leaves the whole pod cgroup unbounded, so the cgroup alone reports nothing
// even though the workload that matters is tightly limited.
//
// Observed on qdrant (2026-08-29): all three pods run a 40Gi-limited qdrant
// container beside an init container `sysctl-tune` that declares no limit. The
// pod cgroup read "max", utilization silently vanished for the entire service,
// and the absent-vs-zero rule downstream correctly rendered no utilization line
// at all — a coverage hole indistinguishable, from the outside, from a service
// nobody had bothered to limit.
//
// Init containers are deliberately excluded from the sum: by the time a pod is
// Running they have terminated and hold no memory, so counting them would
// inflate the denominator and under-report every pod that has one. Regular
// containers must ALL declare a limit for the sum to mean anything — if any one
// of them can grow without bound then so can the pod, and there is no
// denominator to honestly divide by.
func memLimitBytes(dir string, containerLimits []int64) (limit uint64, ok bool) {
	maxRaw, err := readCgroupFirstField(filepath.Join(dir, "memory.max"))
	if err == nil && maxRaw != "max" {
		if maxV, perr := strconv.ParseUint(maxRaw, 10, 64); perr == nil && maxV > 0 {
			return maxV, true
		}
	}
	if len(containerLimits) == 0 {
		return 0, false
	}
	var sum int64
	for _, l := range containerLimits {
		if l <= 0 {
			return 0, false // an unbounded container means an unbounded pod
		}
		sum += l
	}
	if sum <= 0 {
		return 0, false
	}
	return uint64(sum), true
}

// readMemPct reads memory.current from dir and returns current/limit*100, with
// the limit resolved by memLimitBytes (pod cgroup first, pod-spec container
// limits as the fallback). ok is false when memory.current is missing or the
// pod is genuinely unbounded — matching ServiceUseMetrics.MemPct's "0 when no
// limit" wire semantics via plain omission.
//
// NOTE this is memory.current, which charges reclaimable page cache and
// therefore reads HIGHER than the working-set figure `kubectl top` and k9s
// show: tenant-a/neo4j-0 measured 99.97% here against 96.5% there at the same
// instant. See readMemWorkingSetPct for the OOM-relevant number.
func readMemPct(dir string, containerLimits []int64) (pct float64, ok bool) {
	cur, err := readCgroupUint(filepath.Join(dir, "memory.current"))
	if err != nil {
		return 0, false
	}
	maxV, ok := memLimitBytes(dir, containerLimits)
	if !ok {
		return 0, false
	}
	return float64(cur) / float64(maxV) * 100, true
}

// readMemWorkingSetPct returns (memory.current - inactive_file) / limit * 100 —
// the utilization number that actually predicts an OOM kill, and the one
// `kubectl top` and k9s display.
//
// memory.current charges reclaimable page cache, which the kernel drops under
// pressure rather than OOM-killing over. A cache-heavy service can therefore
// pin memory.current at ~100% indefinitely and never be at risk, which makes
// that figure useless as a threshold input: a gate wired to it fires forever
// and means nothing. Subtracting inactive_file leaves anonymous memory plus
// active page cache, which is what the kernel's own OOM accounting effectively
// considers.
//
// ok is false when memory.current or memory.stat is unreadable, or the pod is
// unbounded. A cgroup whose inactive_file exceeds memory.current (racing reads
// of two files) clamps to 0 rather than reporting a negative percentage.
func readMemWorkingSetPct(dir string, containerLimits []int64) (pct float64, ok bool) {
	cur, err := readCgroupUint(filepath.Join(dir, "memory.current"))
	if err != nil {
		return 0, false
	}
	inactiveFile, ok := readCgroupKeyedField(filepath.Join(dir, "memory.stat"), "inactive_file")
	if !ok {
		return 0, false
	}
	maxV, ok := memLimitBytes(dir, containerLimits)
	if !ok {
		return 0, false
	}
	ws := float64(cur) - inactiveFile
	if ws < 0 {
		ws = 0
	}
	return ws / float64(maxV) * 100, true
}

// readMemWorkingSetBytes returns memory.current minus inactive_file in bytes —
// the same quantity readMemWorkingSetPct divides, exported absolutely so a
// consumer can render "1976Mi of 2Gi" rather than only "96%". Clamps to 0 on
// the racing-reads case, same as the percentage form.
func readMemWorkingSetBytes(dir string) (bytes float64, ok bool) {
	cur, err := readCgroupUint(filepath.Join(dir, "memory.current"))
	if err != nil {
		return 0, false
	}
	inactiveFile, ok := readCgroupKeyedField(filepath.Join(dir, "memory.stat"), "inactive_file")
	if !ok {
		return 0, false
	}
	ws := float64(cur) - inactiveFile
	if ws < 0 {
		ws = 0
	}
	return ws, true
}

// readOOMKill reads memory.events' cumulative OOM-kill count as a number of
// KILLS (OOM events), not of processes killed. ok is false when the file is
// missing or neither field is found (defensive — the fields are part of the
// documented cgroup v2 interface and should always be present when the file
// exists).
//
// oom_kill counts every PROCESS the OOM killer killed, so one container kill
// that takes out a group of processes moves it by more than one: observed on a
// 6.12 kernel, one OOMKilled container moved the pod's oom_kill by 2 while
// oom_group_kill moved by 1. oom_group_kill counts group kills, which is what
// kubelet's memory.oom.group=1 containers produce, so it is preferred when the
// field exists and is non-zero. A zero or absent oom_group_kill (kernels before
// it was added, or cgroups not using group kill, where one kill is one process
// anyway) falls back to oom_kill, which keeps older kernels working unchanged.
func readOOMKill(dir string) (count float64, ok bool) {
	path := filepath.Join(dir, "memory.events")
	if g, found := readCgroupKeyedField(path, "oom_group_kill"); found && g > 0 {
		return g, true
	}
	return readCgroupKeyedField(path, "oom_kill")
}

// cpuStat holds cpu.stat's cumulative counters.
type cpuStat struct {
	nrPeriods, nrThrottled float64
	// usageUsec is cumulative CPU time consumed by the cgroup in microseconds.
	// It sits on the FIRST line of the same cpu.stat this function has always
	// opened, and went unread until 2026-08-29 — which is the entire reason CPU
	// utilization did not exist anywhere in either repo, despite USE's U being
	// half of what the method is named for. Diffed across reads by the
	// collector and divided by the window and the cgroup's own quota to produce
	// a percentage; see readCPULimitCores.
	usageUsec float64
}

// readCPUStat reads cpu.stat's cumulative usage_usec/nr_periods/nr_throttled
// fields. ok is false when the file is missing.
func readCPUStat(dir string) (stat cpuStat, ok bool) {
	f, err := os.Open(filepath.Join(dir, "cpu.stat"))
	if err != nil {
		return cpuStat{}, false
	}
	defer func() { _ = f.Close() }()

	found := false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, hasField := strings.Cut(sc.Text(), " ")
		if !hasField {
			continue
		}
		n, err := strconv.ParseFloat(v, 64)
		if err != nil {
			continue
		}
		switch k {
		case "usage_usec":
			stat.usageUsec = n
			found = true
		case "nr_periods":
			stat.nrPeriods = n
			found = true
		case "nr_throttled":
			stat.nrThrottled = n
			found = true
		}
	}
	return stat, found
}

// readCPULimitCores resolves the CPU limit to measure utilization against, in
// cores. ok is false only when the cgroup is genuinely unbounded.
//
// cpu.max holds "<quota> <period>" in microseconds, or "max <period>" when no
// CPU limit is set — so 50000/100000 is half a core. The pod-spec fallback
// mirrors memLimitBytes exactly and for the same reason: kubelet sets a
// pod-level cpu.max only when every container declares a CPU limit, so one
// unlimited sidecar or init container would otherwise erase CPU utilization for
// the whole pod.
func readCPULimitCores(dir string, containerLimitsMilli []int64) (cores float64, ok bool) {
	raw, err := os.ReadFile(filepath.Join(dir, "cpu.max"))
	if err == nil {
		fields := strings.Fields(string(raw))
		if len(fields) == 2 && fields[0] != "max" {
			quota, qerr := strconv.ParseFloat(fields[0], 64)
			period, perr := strconv.ParseFloat(fields[1], 64)
			if qerr == nil && perr == nil && period > 0 && quota > 0 {
				return quota / period, true
			}
		}
	}
	if len(containerLimitsMilli) == 0 {
		return 0, false
	}
	var sumMilli int64
	for _, m := range containerLimitsMilli {
		if m <= 0 {
			return 0, false // an unbounded container means an unbounded pod
		}
		sumMilli += m
	}
	if sumMilli <= 0 {
		return 0, false
	}
	return float64(sumMilli) / 1000.0, true
}

// readCgroupUint reads a cgroup file holding a single bare integer (e.g.
// memory.current).
func readCgroupUint(path string) (uint64, error) {
	raw, err := readCgroupFirstField(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(raw, 10, 64)
}

// readCgroupFirstField reads a cgroup file and returns its first whitespace-
// delimited field (handles both a bare-value file like memory.current and a
// single-token-per-line file like memory.max, which is either a number or the
// literal string "max").
func readCgroupFirstField(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	fields := strings.Fields(string(data))
	if len(fields) == 0 {
		return "", fmt.Errorf("%s: empty", path)
	}
	return fields[0], nil
}

// readCgroupKeyedField reads a cgroup "key value" per-line file (e.g.
// memory.events) and returns the numeric value for the named key.
func readCgroupKeyedField(path, key string) (float64, bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, hasField := strings.Cut(sc.Text(), " ")
		if !hasField || k != key {
			continue
		}
		n, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	}
	return 0, false
}
