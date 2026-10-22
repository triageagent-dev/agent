package nodeexporter

import (
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// statusPod builds a pod whose single container "app" reports restartCount and
// a last termination (nil reason = no lastState at all).
func statusPod(uid string, restarts int32, reason string, finished time.Time) *corev1.Pod {
	cs := corev1.ContainerStatus{Name: "app", RestartCount: restarts}
	if reason != "" {
		cs.LastTerminationState.Terminated = &corev1.ContainerStateTerminated{
			Reason: reason, ExitCode: 137, FinishedAt: metav1.NewTime(finished),
		}
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p-" + uid, Namespace: "ops", UID: types.UID(uid)},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{cs}},
	}
}

func TestOOMStatusTracker(t *testing.T) {
	t0 := time.Date(2026, 9, 30, 15, 0, 0, 0, time.UTC)
	var tr oomStatusTracker

	// First observation establishes the baseline: a pod that already has three
	// recorded OOM kills in its history counts nothing.
	tr.observe([]*corev1.Pod{statusPod("u1", 3, "OOMKilled", t0)})
	if got := tr.kills("u1"); got != 0 {
		t.Fatalf("first observation: kills = %v, want 0 (history is not new kills)", got)
	}

	// Unchanged status: still nothing.
	tr.observe([]*corev1.Pod{statusPod("u1", 3, "OOMKilled", t0)})
	if got := tr.kills("u1"); got != 0 {
		t.Fatalf("unchanged: kills = %v, want 0", got)
	}

	// restartCount 3 -> 4 with an OOMKilled last state: one kill.
	tr.observe([]*corev1.Pod{statusPod("u1", 4, "OOMKilled", t0.Add(time.Minute))})
	if got := tr.kills("u1"); got != 1 {
		t.Fatalf("restartCount 3->4 OOMKilled: kills = %v, want 1", got)
	}

	// Same observation again: the same kill is not counted twice.
	tr.observe([]*corev1.Pod{statusPod("u1", 4, "OOMKilled", t0.Add(time.Minute))})
	if got := tr.kills("u1"); got != 1 {
		t.Fatalf("repeat observation: kills = %v, want still 1", got)
	}

	// A non-OOM termination (restart moved, reason Error) counts nothing.
	tr.observe([]*corev1.Pod{statusPod("u1", 5, "Error", t0.Add(2*time.Minute))})
	if got := tr.kills("u1"); got != 1 {
		t.Fatalf("non-OOM termination: kills = %v, want 1 (unchanged)", got)
	}

	// Two kills between observations: restartCount 5 -> 7, only the last
	// termination is visible. Counts exactly 1 (an undercount by design).
	tr.observe([]*corev1.Pod{statusPod("u1", 7, "OOMKilled", t0.Add(3*time.Minute))})
	if got := tr.kills("u1"); got != 2 {
		t.Fatalf("restartCount 5->7: kills = %v, want 2 (one more, for the one visible termination)", got)
	}

	// finishedAt moving alone (restartCount unchanged) is also a new termination.
	tr.observe([]*corev1.Pod{statusPod("u1", 7, "OOMKilled", t0.Add(4*time.Minute))})
	if got := tr.kills("u1"); got != 3 {
		t.Fatalf("finishedAt changed: kills = %v, want 3", got)
	}

	// Deleted pod is forgotten; a pod with the same UID coming back is a new
	// first observation (baseline, 0).
	tr.observe(nil)
	if _, ok := tr.pods["u1"]; ok {
		t.Fatal("deleted pod still tracked")
	}
	tr.observe([]*corev1.Pod{statusPod("u1", 7, "OOMKilled", t0.Add(4*time.Minute))})
	if got := tr.kills("u1"); got != 0 {
		t.Fatalf("pod back after deletion: kills = %v, want 0 (fresh baseline)", got)
	}
}

// TestOOMStatusTracker_FromZeroRestarts covers the common live shape: a pod
// first seen healthy (restartCount 0, no lastState), then OOM killed once.
func TestOOMStatusTracker_FromZeroRestarts(t *testing.T) {
	t0 := time.Date(2026, 9, 30, 15, 0, 0, 0, time.UTC)
	var tr oomStatusTracker
	tr.observe([]*corev1.Pod{statusPod("u2", 0, "", time.Time{})})
	tr.observe([]*corev1.Pod{statusPod("u2", 1, "OOMKilled", t0)})
	if got := tr.kills("u2"); got != 1 {
		t.Fatalf("restartCount 0->1 OOMKilled: kills = %v, want 1", got)
	}
}

// TestReadOOMKill_PrefersGroupKill pins the kills-not-processes decision: one
// container kill moved oom_kill by 2 and oom_group_kill by 1 on kernel 6.12.
func TestReadOOMKill_PrefersGroupKill(t *testing.T) {
	cases := []struct {
		name, events string
		want         float64
	}{
		{"group kill present", "low 0\nhigh 0\nmax 0\noom 3\noom_kill 6\noom_group_kill 3\n", 3},
		{"group kill zero falls back", "low 0\nhigh 0\nmax 0\noom 2\noom_kill 2\noom_group_kill 0\n", 2},
		{"older kernel, no group field", "low 0\nhigh 0\nmax 0\noom 2\noom_kill 2\n", 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeFile(t, filepath.Join(dir, "memory.events"), tc.events)
			if v, ok := readOOMKill(dir); !ok || v != tc.want {
				t.Errorf("readOOMKill = (%v, %v), want (%v, true)", v, ok, tc.want)
			}
		})
	}
}
