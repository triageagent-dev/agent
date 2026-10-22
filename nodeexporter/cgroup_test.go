package nodeexporter

import (
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// defaultSlice is the systemd kubepods slice location for a kubelet with no
// custom --cgroup-root: directly at root, prefixed "kubepods".
func defaultSlice(root string) kubepodsSlice {
	return kubepodsSlice{dir: root, prefix: "kubepods"}
}

// TestPodCgroupDirCandidates verifies the exact candidate directories built for
// each QoS class, in priority order (systemd slice first, then cgroupfs) — both
// cgroup driver layouts docs/adr/0010-use-metrics-node-exporter.md commits to
// supporting, using the default (no custom --cgroup-root) systemd slice.
func TestPodCgroupDirCandidates(t *testing.T) {
	uid := types.UID("11112222-3333-4444-5555-666677778888")
	sd := defaultSlice("/root")

	guaranteedWant := []string{
		"/root/kubepods.slice/kubepods-pod11112222_3333_4444_5555_666677778888.slice",
		"/root/kubepods/pod11112222-3333-4444-5555-666677778888",
	}
	if got := podCgroupDirCandidates("/root", sd, uid, corev1.PodQOSGuaranteed); !equalStrings(got, guaranteedWant) {
		t.Errorf("Guaranteed candidates = %v, want %v", got, guaranteedWant)
	}

	burstableWant := []string{
		"/root/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod11112222_3333_4444_5555_666677778888.slice",
		"/root/kubepods/burstable/pod11112222-3333-4444-5555-666677778888",
	}
	if got := podCgroupDirCandidates("/root", sd, uid, corev1.PodQOSBurstable); !equalStrings(got, burstableWant) {
		t.Errorf("Burstable candidates = %v, want %v", got, burstableWant)
	}

	besteffortWant := []string{
		"/root/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod11112222_3333_4444_5555_666677778888.slice",
		"/root/kubepods/besteffort/pod11112222-3333-4444-5555-666677778888",
	}
	if got := podCgroupDirCandidates("/root", sd, uid, corev1.PodQOSBestEffort); !equalStrings(got, besteffortWant) {
		t.Errorf("BestEffort candidates = %v, want %v", got, besteffortWant)
	}
}

// TestPodCgroupDirCandidates_CustomSystemdRoot verifies candidate generation
// for a discovered non-default systemd slice location (the shape
// discoverKubepodsSlice returns for a kubelet started with --cgroup-root set),
// independent of the discovery probe itself.
func TestPodCgroupDirCandidates_CustomSystemdRoot(t *testing.T) {
	uid := types.UID("df0e0542-ad82-445f-be9e-e581823af864")
	sd := kubepodsSlice{dir: "/root/kubelet.slice", prefix: "kubelet-kubepods"}

	guaranteedWant := "/root/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-poddf0e0542_ad82_445f_be9e_e581823af864.slice"
	if got := podCgroupDirCandidates("/root", sd, uid, corev1.PodQOSGuaranteed); got[0] != guaranteedWant {
		t.Errorf("Guaranteed systemd candidate = %q, want %q", got[0], guaranteedWant)
	}

	burstableWant := "/root/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-burstable.slice/kubelet-kubepods-burstable-poddf0e0542_ad82_445f_be9e_e581823af864.slice"
	if got := podCgroupDirCandidates("/root", sd, uid, corev1.PodQOSBurstable); got[0] != burstableWant {
		t.Errorf("Burstable systemd candidate = %q, want %q", got[0], burstableWant)
	}

	besteffortWant := "/root/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-besteffort.slice/kubelet-kubepods-besteffort-poddf0e0542_ad82_445f_be9e_e581823af864.slice"
	if got := podCgroupDirCandidates("/root", sd, uid, corev1.PodQOSBestEffort); got[0] != besteffortWant {
		t.Errorf("BestEffort systemd candidate = %q, want %q", got[0], besteffortWant)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestPodCgroupDir_FindsCgroupfsLayout verifies podCgroupDir picks whichever
// candidate directory actually exists — here, only the cgroupfs-layout candidate.
func TestPodCgroupDir_FindsCgroupfsLayout(t *testing.T) {
	root := t.TempDir()
	uid := types.UID("aaaa-bbbb-cccc-dddd")
	dir := filepath.Join(root, "kubepods", "burstable", "podaaaa-bbbb-cccc-dddd")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	got := podCgroupDir(root, discoverKubepodsSlice(root), uid, corev1.PodQOSBurstable)
	if got != dir {
		t.Errorf("podCgroupDir = %q, want %q", got, dir)
	}
}

// TestPodCgroupDir_FindsSystemdLayout verifies the systemd-slice candidate is
// tried first and picked when it exists, for a kubelet with no custom
// --cgroup-root (the slice sits directly at root).
func TestPodCgroupDir_FindsSystemdLayout(t *testing.T) {
	root := t.TempDir()
	uid := types.UID("aaaa-bbbb-cccc-dddd")
	dir := filepath.Join(root, "kubepods.slice", "kubepods-pod" /* guaranteed, no qos slice */ +"aaaa_bbbb_cccc_dddd.slice")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	got := podCgroupDir(root, discoverKubepodsSlice(root), uid, corev1.PodQOSGuaranteed)
	if got != dir {
		t.Errorf("podCgroupDir = %q, want %q", got, dir)
	}
}

// TestPodCgroupDir_NoneExistsReturnsEmpty verifies a pod whose cgroup directory
// exists under neither layout is skipped gracefully (empty string, no error/panic).
func TestPodCgroupDir_NoneExistsReturnsEmpty(t *testing.T) {
	root := t.TempDir()
	got := podCgroupDir(root, discoverKubepodsSlice(root), types.UID("gone"), corev1.PodQOSBestEffort)
	if got != "" {
		t.Errorf("podCgroupDir = %q, want \"\" (no candidate exists)", got)
	}
}

// TestDiscoverKubepodsSlice_Default verifies a plain root with a top-level
// kubepods.slice (no custom --cgroup-root) resolves to the default dir/prefix.
func TestDiscoverKubepodsSlice_Default(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "kubepods.slice"), 0o755); err != nil {
		t.Fatal(err)
	}
	got := discoverKubepodsSlice(root)
	want := kubepodsSlice{dir: root, prefix: "kubepods"}
	if got != want {
		t.Errorf("discoverKubepodsSlice = %+v, want %+v", got, want)
	}
}

// TestDiscoverKubepodsSlice_MissingFallsBackToDefault verifies a root with
// neither a top-level kubepods.slice nor a nested "<name>-kubepods.slice"
// falls back to the default dir/prefix rather than erroring.
func TestDiscoverKubepodsSlice_MissingFallsBackToDefault(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "system.slice"), 0o755); err != nil {
		t.Fatal(err)
	}
	got := discoverKubepodsSlice(root)
	want := kubepodsSlice{dir: root, prefix: "kubepods"}
	if got != want {
		t.Errorf("discoverKubepodsSlice = %+v, want %+v", got, want)
	}
}

// TestDiscoverKubepodsSlice_CustomCgroupRoot verifies the kind layout — a
// kubelet started with --cgroup-root=/kubelet nests the kubepods slice one
// level down under kubelet.slice, prefixed "kubelet-kubepods".
func TestDiscoverKubepodsSlice_CustomCgroupRoot(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "kubelet.slice", "kubelet-kubepods.slice"), 0o755); err != nil {
		t.Fatal(err)
	}
	got := discoverKubepodsSlice(root)
	want := kubepodsSlice{dir: filepath.Join(root, "kubelet.slice"), prefix: "kubelet-kubepods"}
	if got != want {
		t.Errorf("discoverKubepodsSlice = %+v, want %+v", got, want)
	}
}

// TestPodCgroupDir_FindsKindLayout_Guaranteed verifies resolution of the real
// layout observed inside a kind v1.37 node (systemd driver, --cgroup-root
// /kubelet): a Guaranteed pod's slice sits directly under
// kubelet.slice/kubelet-kubepods.slice, with no QoS level.
func TestPodCgroupDir_FindsKindLayout_Guaranteed(t *testing.T) {
	root := t.TempDir()
	uid := types.UID("df0e0542-ad82-445f-be9e-e581823af864")
	dir := filepath.Join(root, "kubelet.slice", "kubelet-kubepods.slice",
		"kubelet-kubepods-poddf0e0542_ad82_445f_be9e_e581823af864.slice")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	got := podCgroupDir(root, discoverKubepodsSlice(root), uid, corev1.PodQOSGuaranteed)
	if got != dir {
		t.Errorf("podCgroupDir = %q, want %q", got, dir)
	}
}

// TestPodCgroupDir_FindsKindLayout_Burstable is the kind-layout counterpart of
// TestPodCgroupDir_FindsKindLayout_Guaranteed for a Burstable pod.
func TestPodCgroupDir_FindsKindLayout_Burstable(t *testing.T) {
	root := t.TempDir()
	uid := types.UID("df0e0542-ad82-445f-be9e-e581823af864")
	dir := filepath.Join(root, "kubelet.slice", "kubelet-kubepods.slice", "kubelet-kubepods-burstable.slice",
		"kubelet-kubepods-burstable-poddf0e0542_ad82_445f_be9e_e581823af864.slice")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	got := podCgroupDir(root, discoverKubepodsSlice(root), uid, corev1.PodQOSBurstable)
	if got != dir {
		t.Errorf("podCgroupDir = %q, want %q", got, dir)
	}
}

// TestPodCgroupDir_FindsKindLayout_BestEffort is the kind-layout counterpart of
// TestPodCgroupDir_FindsKindLayout_Guaranteed for a BestEffort pod.
func TestPodCgroupDir_FindsKindLayout_BestEffort(t *testing.T) {
	root := t.TempDir()
	uid := types.UID("df0e0542-ad82-445f-be9e-e581823af864")
	dir := filepath.Join(root, "kubelet.slice", "kubelet-kubepods.slice", "kubelet-kubepods-besteffort.slice",
		"kubelet-kubepods-besteffort-poddf0e0542_ad82_445f_be9e_e581823af864.slice")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	got := podCgroupDir(root, discoverKubepodsSlice(root), uid, corev1.PodQOSBestEffort)
	if got != dir {
		t.Errorf("podCgroupDir = %q, want %q", got, dir)
	}
}

func TestReadPSIAvg60(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "memory.pressure")
	content := "some avg10=1.23 avg60=34.56 avg300=0.10 total=123456\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	v, ok := readPSIAvg60(path)
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if v != 34.56 {
		t.Errorf("avg60 = %v, want 34.56", v)
	}
}

func TestReadPSIAvg60_MissingFileGracefulDegrade(t *testing.T) {
	if _, ok := readPSIAvg60(filepath.Join(t.TempDir(), "does-not-exist")); ok {
		t.Error("ok = true for a missing PSI file, want false (CONFIG_PSI-off graceful degrade)")
	}
}

func TestReadMemPct(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "memory.current"), []byte("536870912\n"), 0o644); err != nil { // 512Mi
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("1073741824\n"), 0o644); err != nil { // 1Gi
		t.Fatal(err)
	}
	pct, ok := readMemPct(dir, nil)
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if pct != 50 {
		t.Errorf("pct = %v, want 50", pct)
	}
}

func TestReadMemPct_UnlimitedReturnsNotOK(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "memory.current"), []byte("1000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("max\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := readMemPct(dir, nil); ok {
		t.Error("ok = true for memory.max=\"max\" (unlimited), want false")
	}
}

// TestReadMemPct_FallsBackToSpecLimitsWhenPodCgroupUnbounded is the qdrant case
// (2026-08-29): a pod whose real workload declares a 40Gi limit but which also
// carries a limitless init container, so kubelet never set a pod-level
// memory.max and the cgroup reads "max".
//
// Before the fallback, utilization silently vanished for the entire service —
// and the absent-vs-zero rule downstream rendered no utilization line at all,
// which from the outside is indistinguishable from a service nobody limited.
func TestReadMemPct_FallsBackToSpecLimitsWhenPodCgroupUnbounded(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "memory.current"), []byte("10737418240\n"), 0o644); err != nil { // 10Gi
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("max\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pct, ok := readMemPct(dir, []int64{42949672960}) // one 40Gi container
	if !ok {
		t.Fatal("ok = false; the pod spec declares a limit even though the cgroup does not")
	}
	if pct != 25 {
		t.Errorf("pct = %v, want 25 (10Gi of 40Gi)", pct)
	}
}

// TestReadMemPct_UnboundedContainerStillReturnsNotOK proves the fallback does
// not invent a denominator: if any regular container can grow without bound
// then so can the pod, and there is nothing honest to divide by.
func TestReadMemPct_UnboundedContainerStillReturnsNotOK(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "memory.current"), []byte("1000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("max\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := readMemPct(dir, []int64{1073741824, 0}); ok {
		t.Error("ok = true with one unbounded container, want false")
	}
}

// TestReadMemWorkingSetPct proves the OOM-relevant figure subtracts reclaimable
// page cache, which is what separates it from readMemPct. The numbers are the
// shape of the tenant-a/neo4j-0 finding: a cgroup sitting near its limit on
// memory.current where much of the charge is cache the kernel would simply drop.
func TestReadMemWorkingSetPct(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "memory.current"), []byte("1073741824\n"), 0o644); err != nil { // 1Gi
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("1073741824\n"), 0o644); err != nil { // 1Gi
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.stat"), []byte("anon 536870912\ninactive_file 268435456\nslab 100\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	full, ok := readMemPct(dir, nil)
	if !ok || full != 100 {
		t.Fatalf("readMemPct = %v/%v, want 100/true — memory.current is at the limit", full, ok)
	}
	ws, ok := readMemWorkingSetPct(dir, nil)
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if ws != 75 {
		t.Errorf("working set = %v, want 75 (1Gi current - 256Mi inactive_file, over a 1Gi limit)", ws)
	}
}

// TestReadMemWorkingSetPct_ClampsNegative guards the racing-reads case:
// memory.current and memory.stat are two separate files read a moment apart, so
// inactive_file can transiently exceed current. Report 0, never a negative
// percentage.
func TestReadMemWorkingSetPct_ClampsNegative(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "memory.current"), []byte("1000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("10000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.stat"), []byte("inactive_file 99999\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ws, ok := readMemWorkingSetPct(dir, nil)
	if !ok || ws != 0 {
		t.Errorf("working set = %v/%v, want 0/true", ws, ok)
	}
}

// TestReadMemWorkingSetPct_MissingMemStatReturnsNotOK proves an unreadable
// memory.stat degrades to "no working-set reading" rather than silently
// reporting the page-cache-inflated figure under the working-set name.
func TestReadMemWorkingSetPct_MissingMemStatReturnsNotOK(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "memory.current"), []byte("1000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("10000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := readMemWorkingSetPct(dir, nil); ok {
		t.Error("ok = true without memory.stat, want false")
	}
}

func TestReadOOMKill(t *testing.T) {
	dir := t.TempDir()
	content := "low 0\nhigh 0\nmax 0\noom 2\noom_kill 2\noom_group_kill 0\n"
	if err := os.WriteFile(filepath.Join(dir, "memory.events"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	v, ok := readOOMKill(dir)
	if !ok || v != 2 {
		t.Errorf("readOOMKill = (%v, %v), want (2, true)", v, ok)
	}
}

func TestReadCPUStat(t *testing.T) {
	dir := t.TempDir()
	content := "usage_usec 123456\nuser_usec 100000\nsystem_usec 23456\nnr_periods 500\nnr_throttled 25\nthrottled_usec 9999\n"
	if err := os.WriteFile(filepath.Join(dir, "cpu.stat"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	stat, ok := readCPUStat(dir)
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if stat.nrPeriods != 500 || stat.nrThrottled != 25 {
		t.Errorf("stat = %+v, want {nrPeriods:500 nrThrottled:25}", stat)
	}
}

func TestReadCPUStat_MissingFile(t *testing.T) {
	if _, ok := readCPUStat(t.TempDir()); ok {
		t.Error("ok = true for a missing cpu.stat, want false")
	}
}
