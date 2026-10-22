package nodeexporter

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestParseCPUOnlineRange(t *testing.T) {
	tests := []struct {
		name   string
		in     string
		want   int
		wantOK bool
	}{
		{name: "single contiguous range", in: "0-7", want: 8, wantOK: true},
		{name: "single contiguous range with newline", in: "0-7\n", want: 8, wantOK: true},
		{name: "single CPU", in: "3", want: 1, wantOK: true},
		{name: "gapped list", in: "0,2-3", want: 3, wantOK: true},
		{name: "multiple ranges", in: "0-1,4-7", want: 6, wantOK: true},
		{name: "trailing comma tolerated", in: "0-7,", want: 8, wantOK: true},
		{name: "whitespace around entries tolerated", in: " 0-3 , 5 ", want: 5, wantOK: true},
		{name: "empty string", in: "", wantOK: false},
		{name: "whitespace only", in: "   ", wantOK: false},
		{name: "unparsable range", in: "a-b", wantOK: false},
		{name: "unparsable single", in: "x", wantOK: false},
		{name: "inverted range", in: "7-0", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseCPUOnlineRange(tc.in)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if ok && got != tc.want {
				t.Errorf("count = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestReadCPUOnlineRange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "online")
	if err := os.WriteFile(path, []byte("0-7\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	n, ok := readCPUOnlineRange(path)
	if !ok || n != 8 {
		t.Errorf("readCPUOnlineRange = (%d, %v), want (8, true)", n, ok)
	}
}

func TestReadCPUOnlineRange_MissingFile(t *testing.T) {
	if _, ok := readCPUOnlineRange(filepath.Join(t.TempDir(), "does-not-exist")); ok {
		t.Error("ok = true for a missing online file, want false")
	}
}

func TestCountCPUInfoProcessors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cpuinfo")
	content := "processor\t: 0\nvendor_id\t: GenuineIntel\n\nprocessor\t: 1\nvendor_id\t: GenuineIntel\n\nprocessor\t: 2\nvendor_id\t: GenuineIntel\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	n, ok := countCPUInfoProcessors(path)
	if !ok || n != 3 {
		t.Errorf("countCPUInfoProcessors = (%d, %v), want (3, true)", n, ok)
	}
}

func TestCountCPUInfoProcessors_MissingFile(t *testing.T) {
	if _, ok := countCPUInfoProcessors(filepath.Join(t.TempDir(), "does-not-exist")); ok {
		t.Error("ok = true for a missing cpuinfo file, want false")
	}
}

func TestCountCPUInfoProcessors_NoProcessorLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cpuinfo")
	if err := os.WriteFile(path, []byte("vendor_id\t: GenuineIntel\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := countCPUInfoProcessors(path); ok {
		t.Error("ok = true with no processor lines, want false")
	}
}

// TestResolveNodeCPUCores_PrefersSysfsOnline verifies the sysfs online range is
// tried first and wins when it parses.
func TestResolveNodeCPUCores_PrefersSysfsOnline(t *testing.T) {
	root := t.TempDir()
	sysRoot := filepath.Join(root, "sys")
	procRoot := filepath.Join(root, "proc")
	sysCPUDir := filepath.Join(sysRoot, "devices", "system", "cpu")
	if err := os.MkdirAll(sysCPUDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sysCPUDir, "online"), []byte("0-3\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Even with a DIFFERENT count available via cpuinfo, sysfs must win.
	if err := os.MkdirAll(procRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(procRoot, "cpuinfo"), []byte("processor\t: 0\nprocessor\t: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cores, source := resolveNodeCPUCores(sysRoot, procRoot)
	if cores != 4 || source != "sysfs-online" {
		t.Errorf("resolveNodeCPUCores = (%d, %q), want (4, \"sysfs-online\")", cores, source)
	}
}

// TestResolveNodeCPUCores_FallsBackToCPUInfo verifies the /proc/cpuinfo
// processor-count fallback fires when sysfs is absent.
func TestResolveNodeCPUCores_FallsBackToCPUInfo(t *testing.T) {
	root := t.TempDir()
	sysRoot := filepath.Join(root, "sys") // deliberately empty — no online file
	procRoot := filepath.Join(root, "proc")
	if err := os.MkdirAll(procRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(procRoot, "cpuinfo"), []byte("processor\t: 0\nprocessor\t: 1\nprocessor\t: 2\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cores, source := resolveNodeCPUCores(sysRoot, procRoot)
	if cores != 3 || source != "cpuinfo" {
		t.Errorf("resolveNodeCPUCores = (%d, %q), want (3, \"cpuinfo\")", cores, source)
	}
}

// TestResolveNodeCPUCores_FallsBackToRuntimeNumCPU verifies the final fallback
// — which never fails — fires when both /sys and /proc/cpuinfo are unusable.
func TestResolveNodeCPUCores_FallsBackToRuntimeNumCPU(t *testing.T) {
	root := t.TempDir()
	sysRoot := filepath.Join(root, "sys")   // empty
	procRoot := filepath.Join(root, "proc") // empty, no cpuinfo either

	cores, source := resolveNodeCPUCores(sysRoot, procRoot)
	if source != "runtime.NumCPU" {
		t.Errorf("source = %q, want \"runtime.NumCPU\"", source)
	}
	if cores != runtime.NumCPU() {
		t.Errorf("cores = %d, want %d (runtime.NumCPU())", cores, runtime.NumCPU())
	}
}

func TestResolveNodeMemTotalBytes(t *testing.T) {
	procRoot := t.TempDir()
	content := "MemTotal:       16374420 kB\nMemFree:         1234 kB\n"
	if err := os.WriteFile(filepath.Join(procRoot, "meminfo"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	bytes, ok := resolveNodeMemTotalBytes(procRoot)
	if !ok {
		t.Fatal("ok = false, want true")
	}
	want := uint64(16374420) * 1024
	if bytes != want {
		t.Errorf("bytes = %d, want %d", bytes, want)
	}
}

func TestResolveNodeMemTotalBytes_MissingFile(t *testing.T) {
	if _, ok := resolveNodeMemTotalBytes(t.TempDir()); ok {
		t.Error("ok = true for a missing meminfo file, want false")
	}
}

func TestResolveNodeMemTotalBytes_NoMemTotalKey(t *testing.T) {
	procRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(procRoot, "meminfo"), []byte("MemFree: 1234 kB\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := resolveNodeMemTotalBytes(procRoot); ok {
		t.Error("ok = true with no MemTotal key, want false")
	}
}

func TestResolveNodeMemTotalBytes_UnparsableValue(t *testing.T) {
	procRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(procRoot, "meminfo"), []byte("MemTotal: notanumber kB\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := resolveNodeMemTotalBytes(procRoot); ok {
		t.Error("ok = true with an unparsable MemTotal value, want false")
	}
}
