package nodeexporter

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// oomKilledReason is the reason the kubelet records on a container's terminated
// state when the kernel OOM killer took it out.
const oomKilledReason = "OOMKilled"

// containerOOMSeen is what oomStatusTracker remembers about one container
// between observations: enough to tell whether a NEW termination has been
// recorded since (see observe).
type containerOOMSeen struct {
	restartCount int32
	finishedAt   time.Time
}

// podOOMState is one pod's container memory plus its running count.
type podOOMState struct {
	containers map[string]containerOOMSeen
	kills      float64
}

// oomStatusTracker derives a monotonic per-pod counter of OOMKilled container
// terminations from pod STATUS — the second, independent source beside the pod
// cgroup's memory.events (readOOMKill), which it does not read at all.
//
// Pod status only exposes the LAST termination of each container
// (lastState.terminated), so the kill history is not there to be counted; it
// has to be accumulated across observations. For each (pod UID, container name)
// the tracker keeps the last seen restartCount and lastState.terminated
// .finishedAt, and counts one kill whenever the container's last termination is
// OOMKilled and either of those two moved since the previous observation.
//
// What is counted, exactly: terminations the tracker SAW as a container's last
// termination, one each. If several restarts fall between two observations
// (restartCount 1 -> 3) only the last termination is visible, so that interval
// counts 1 even though up to 2 kills may have happened — an undercount by
// construction, never an overcount; the cgroup counter covers that case. The
// first observation of a pod only establishes the baseline and counts nothing,
// so a pod's history (or a restarted exporter's ignorance of it) is never
// reported as new kills. A container that is down right now with
// state.terminated=OOMKilled but has not restarted yet is counted once its
// restart publishes it as lastState, not before, so it is not counted twice.
//
// Not safe for concurrent use: Exporter.Collect is the only caller and the
// sensor stream runs it from a single goroutine per tick.
type oomStatusTracker struct {
	pods map[types.UID]*podOOMState
}

// observe folds one listing of the node's pods into the tracker and forgets
// every pod not in it. All pods are observed, whatever their phase, so a pod
// that flaps out of Running (or whose cgroup dir is briefly missing, which skips
// it in Collect) keeps its baseline and a kill during the gap is still counted
// on the next observation.
func (t *oomStatusTracker) observe(pods []*corev1.Pod) {
	if t.pods == nil {
		t.pods = make(map[types.UID]*podOOMState)
	}
	live := make(map[types.UID]bool, len(pods))
	for _, pod := range pods {
		live[pod.UID] = true
		st := t.pods[pod.UID]
		first := st == nil
		if first {
			st = &podOOMState{containers: make(map[string]containerOOMSeen)}
			t.pods[pod.UID] = st
		}
		// Init containers are trivial to include (same status shape, names are
		// unique across both lists) and an OOM-killed init container is a real
		// kill, so they are counted too.
		for _, list := range [][]corev1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
			for i := range list {
				cs := &list[i]
				seen, known := st.containers[cs.Name]
				now := containerOOMSeen{restartCount: cs.RestartCount}
				term := cs.LastTerminationState.Terminated
				if term != nil {
					now.finishedAt = term.FinishedAt.Time
				}
				st.containers[cs.Name] = now
				// First observation of the pod is baseline only. A container
				// that shows up later in an already-known pod (its status list
				// was empty before) starts from a zero baseline, so a kill that
				// landed before we first saw it is not lost.
				if first || term == nil || term.Reason != oomKilledReason {
					continue
				}
				if !known || cs.RestartCount > seen.restartCount || !now.finishedAt.Equal(seen.finishedAt) {
					st.kills++
				}
			}
		}
	}
	for uid := range t.pods {
		if !live[uid] {
			delete(t.pods, uid)
		}
	}
}

// kills returns the pod's cumulative OOMKilled termination count (0 for a pod
// never observed).
func (t *oomStatusTracker) kills(uid types.UID) float64 {
	if st := t.pods[uid]; st != nil {
		return st.kills
	}
	return 0
}
