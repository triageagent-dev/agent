// Wire-format regression test (docs/todo/application-cr-removal.md, step 0):
// the agent sends synthesized virtual apps to the core in snapshot and
// app_delta frames, and the core decodes them with an independent struct
// that shares the same JSON tags — the two repos never share a Go type, so
// the bytes on the wire are the only contract between them. This test builds
// a representative set of virtual apps through the real synthesis path
// (synthesizeApplication) and the real frame constructors (newSnapshot,
// newAppDelta), then compares the marshaled bytes against golden files under
// testdata/. A byte difference here means the Application shape changed on
// the wire; fix the type that changed, never the golden file. Run with
// UPDATE_GOLDEN=1 to (re)generate the golden files from the current code.
package push

import (
	"os"
	"testing"
)

// goldenSnapshotApps builds the representative set the golden files pin:
// one Ready app with two components and all four annotations (domain,
// description, tier, weight), one not-Ready app with no annotations, and
// one whose version comes from a uniform image tag.
func goldenSnapshotApps() []*VirtualApp {
	ready := synthesizeApplication(groupKey{Namespace: "ops", Name: "checkout"}, []workloadInfo{
		{Kind: workloadDeployment, Name: "api", Ready: true,
			Domain: "commerce", Description: "Checkout service", Tier: 2, HasTier: true, Weight: 3.5, HasWeight: true},
		{Kind: workloadStatefulSet, Name: "db", Ready: true,
			Domain: "commerce", Description: "Checkout service", Tier: 4, HasTier: true, Weight: 1.5, HasWeight: true},
	})
	notReady := synthesizeApplication(groupKey{Namespace: "ops", Name: "worker"}, []workloadInfo{
		{Kind: workloadDeployment, Name: "worker", Ready: false},
	})
	versioned := synthesizeApplication(groupKey{Namespace: "ops", Name: "web"}, []workloadInfo{
		{Kind: workloadDeployment, Name: "web", Ready: true, ImageTag: "1.4.2"},
	})
	return []*VirtualApp{ready, notReady, versioned}
}

// compareGolden marshals v (via the package's own frame encoder) and
// compares it against the golden file at path, byte for byte. UPDATE_GOLDEN=1
// (re)writes the golden file instead of comparing against it.
func compareGolden(t *testing.T, path string, f *Frame) {
	t.Helper()
	got, err := encode(f)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("write golden %s: %v", path, err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (run with UPDATE_GOLDEN=1 to create it)", path, err)
	}
	if string(got) != string(want) {
		t.Errorf("wire bytes for %s changed:\n got:  %s\n want: %s", path, got, want)
	}
}

// TestVirtualAppFrameGolden pins the snapshot and app_delta frame bytes a
// virtual app travels in. See the package doc comment above for what changing
// this means and what it does not.
func TestVirtualAppFrameGolden(t *testing.T) {
	apps := goldenSnapshotApps()

	compareGolden(t, "testdata/virtualapp_snapshot.golden.json", newSnapshot("cluster-a", apps))
	compareGolden(t, "testdata/virtualapp_app_delta.golden.json", newAppDelta("cluster-a", OpUpdate, apps[0]))
}
