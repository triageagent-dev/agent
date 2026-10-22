package nodeexporter

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/push"
	"github.com/triageagent-dev/agent/sensorstream/sensorpb"
	"github.com/triageagent-dev/agent/usemetrics"
)

// This file is the end-to-end regression guard for the USE-metrics OOM-kill
// path: a fake cgroup tree on disk -> the real Exporter.Collect -> the same
// encode/decode the sensor stream applies (a StreamRequest snapshot marshalled
// to the wire and back, then metricset.FromOTLP, exactly what
// sensorstream.Hub.receive stores and Hub.Set returns) -> the real
// usemetrics.Collector, driven window by window -> the captured frames.
// Nothing in the middle is mocked; only the gRPC transport itself is skipped.

// e2eHub mirrors sensorstream.Hub's per-node snapshot bookkeeping (latest
// snapshot per role replaces the previous one; Targets lists nodes with a
// snapshot; Set returns the stored set) and gates the collector so a test
// decides exactly when a window runs: Targets blocks until the test releases
// the window, and signals idle when the previous window has fully completed.
type e2eHub struct {
	mu    sync.Mutex
	nodes map[string]metricset.Set
	// hidden nodes are withheld from Targets, standing in for a sensor whose
	// stream is reconnecting for one collector tick.
	hidden map[string]bool

	gate chan struct{}
	idle chan struct{}
}

func newE2EHub() *e2eHub {
	return &e2eHub{nodes: map[string]metricset.Set{}, hidden: map[string]bool{}, gate: make(chan struct{}), idle: make(chan struct{})}
}

// receive applies what Hub.receive does to a snapshot message: the OTLP goes
// through the protobuf wire format and metricset.FromOTLP.
func (h *e2eHub) receive(t *testing.T, node string, e *Exporter) {
	t.Helper()
	rms, err := e.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wire, err := proto.Marshal(&sensorpb.StreamRequest{Body: &sensorpb.StreamRequest_Snapshot{Snapshot: &sensorpb.Snapshot{
		Role: metricset.RoleNodeExporter, ResourceMetrics: rms,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	var msg sensorpb.StreamRequest
	if err := proto.Unmarshal(wire, &msg); err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nodes[node] = metricset.FromOTLP(msg.GetSnapshot().GetResourceMetrics())
}

func (h *e2eHub) Targets(string) []string {
	h.idle <- struct{}{}
	<-h.gate
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for n := range h.nodes {
		if !h.hidden[n] {
			out = append(out, "sensor/"+n)
		}
	}
	sortStrings(out)
	return out
}

func sortStrings(s []string) {
	for i := range s {
		for j := i + 1; j < len(s); j++ {
			if s[j] < s[i] {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}

func (h *e2eHub) Set(_, target string) (metricset.Set, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.nodes[strings.TrimPrefix(target, "sensor/")]
	if !ok {
		return nil, fmt.Errorf("no sensor for %s", target)
	}
	return s, nil
}

type e2eSender struct {
	mu      sync.Mutex
	metrics [][]push.ServiceUseMetrics
	summary [][]push.ServiceUseSummary
}

func (s *e2eSender) SendUseMetrics(_ int64, _ int, svcs []push.ServiceUseMetrics) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics = append(s.metrics, append([]push.ServiceUseMetrics(nil), svcs...))
}

func (s *e2eSender) SendUseSummary(_ int64, _ int, svcs []push.ServiceUseSummary) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.summary = append(s.summary, append([]push.ServiceUseSummary(nil), svcs...))
}

// e2eNode is one node: a cgroup root, an Exporter over a fake informer, and a
// Running Burstable pod whose files the test rewrites.
type e2eNode struct {
	t       *testing.T
	name    string
	e       *Exporter
	podDir  string
	pod     *corev1.Pod
	clients *k8sfake.Clientset
}

func newE2ENode(t *testing.T, name, podName, service string) *e2eNode {
	t.Helper()
	root := t.TempDir()
	uid := types.UID("11111111-2222-3333-4444-" + fmt.Sprintf("%012d", len(podName)))
	// systemd layout, Burstable, exactly as on the affected node.
	dir := filepath.Join(root, "kubepods.slice", "kubepods-burstable.slice",
		"kubepods-burstable-pod"+strings.ReplaceAll(string(uid), "-", "_")+".slice")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: "ops", UID: uid, Labels: map[string]string{"app.kubernetes.io/name": service}},
		Spec: corev1.PodSpec{NodeName: name, Containers: []corev1.Container{{Name: "ad", Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1000")}}}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, QOSClass: corev1.PodQOSBurstable},
	}
	cs := k8sfake.NewSimpleClientset(pod)
	factory := informers.NewSharedInformerFactory(cs, 0)
	pi := factory.Core().V1().Pods()
	_ = pi.Informer()
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	factory.Start(stop)
	factory.WaitForCacheSync(stop)
	n := &e2eNode{t: t, name: name, podDir: dir, pod: pod, clients: cs, e: &Exporter{
		opts: Options{CgroupRoot: root}, factory: factory, lister: pi.Lister(), log: logr.Discard(),
		kubepodsSlice: discoverKubepodsSlice(root),
	}}
	n.write(0, 850)
	return n
}

// write rewrites the pod cgroup files: pod-level memory.events oom_kill and
// the working-set bytes (limit is 1000 from the pod spec).
func (n *e2eNode) write(oomKill int, memCurrent int) {
	n.t.Helper()
	writeFile(n.t, filepath.Join(n.podDir, "memory.events"),
		fmt.Sprintf("low 0\nhigh 0\nmax %d\noom %d\noom_kill %d\noom_group_kill %d\n", oomKill, oomKill/2, oomKill, oomKill/2))
	writeFile(n.t, filepath.Join(n.podDir, "memory.current"), fmt.Sprintf("%d\n", memCurrent))
	writeFile(n.t, filepath.Join(n.podDir, "memory.max"), "max\n")
	writeFile(n.t, filepath.Join(n.podDir, "memory.stat"), "inactive_file 0\n")
	writeFile(n.t, filepath.Join(n.podDir, "cpu.stat"), "usage_usec 1\nnr_periods 1\nnr_throttled 0\n")
}

type e2eRig struct {
	t      *testing.T
	hub    *e2eHub
	sender *e2eSender
	nodes  []*e2eNode
}

func newE2ERig(t *testing.T, nodes ...*e2eNode) *e2eRig {
	t.Helper()
	hub := newE2EHub()
	sender := &e2eSender{}
	c := usemetrics.New(usemetrics.Options{
		Source: hub, Interval: time.Millisecond,
		MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40, MemPctThreshold: 85,
	}, &usemetrics.OwnerIdentityResolver{Client: ctrlfake.NewClientBuilder().Build()}, sender, logr.Discard())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = c.Start(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		// Release a possibly blocked Targets call so Start can observe ctx.
		go func() {
			for {
				select {
				case hub.gate <- struct{}{}:
				case <-hub.idle:
				case <-done:
					return
				}
			}
		}()
		<-done
	})
	<-hub.idle // the collector is parked at the start of its first window
	r := &e2eRig{t: t, hub: hub, sender: sender, nodes: nodes}
	return r
}

// tick is one sensor tick on every node (Collect -> wire -> hub).
func (r *e2eRig) tick() {
	for _, n := range r.nodes {
		r.hub.receive(r.t, n.name, n.e)
	}
}

// window runs exactly one collector window and returns the use_summary and
// use_metrics frames it produced (nil when it sent none).
func (r *e2eRig) window() (summary []push.ServiceUseSummary, metrics []push.ServiceUseMetrics) {
	r.t.Helper()
	r.sender.mu.Lock()
	ns, nm := len(r.sender.summary), len(r.sender.metrics)
	r.sender.mu.Unlock()
	r.hub.gate <- struct{}{}
	<-r.hub.idle // the window finished; the collector is parked at the next one
	r.sender.mu.Lock()
	defer r.sender.mu.Unlock()
	if len(r.sender.summary) > ns {
		summary = r.sender.summary[len(r.sender.summary)-1]
	}
	if len(r.sender.metrics) > nm {
		metrics = r.sender.metrics[len(r.sender.metrics)-1]
	}
	return summary, metrics
}

func oomOf(summary []push.ServiceUseSummary, svc string) (int64, bool) {
	for _, s := range summary {
		if s.Service == svc {
			return s.OOMKills, true
		}
	}
	return 0, false
}

// TestE2E_OOMKillReachesFrame is the baseline: several windows at
// oom_kill 0, then 2 (with oom_group_kill 1, as one container kill looks on a
// 6.12 kernel), then flat. The window holding the increase carries 1 kill.
func TestE2E_OOMKillReachesFrame(t *testing.T) {
	n := newE2ENode(t, "node-a", "ad-5f5b5b8849-wncfh", "ad")
	r := newE2ERig(t, n)
	for i := 0; i < 3; i++ {
		r.tick()
		sum, _ := r.window()
		if got, _ := oomOf(sum, "ad"); got != 0 {
			t.Fatalf("window %d: OOMKills = %d, want 0", i, got)
		}
	}
	n.write(2, 990)
	r.tick()
	sum, met := r.window()
	if got, ok := oomOf(sum, "ad"); !ok || got != 1 {
		t.Fatalf("kill window: summary OOMKills = %d (present=%v), want 1", got, ok)
	}
	if len(met) != 1 || met[0].OOMKills != 1 {
		t.Fatalf("kill window: use_metrics = %+v, want one service with OOMKills=1", met)
	}
	n.write(2, 100)
	r.tick()
	sum, _ = r.window()
	if got, _ := oomOf(sum, "ad"); got != 0 {
		t.Fatalf("window after the kill: OOMKills = %d, want 0 (flat)", got)
	}
}

// warm runs n quiet windows so the collector holds a baseline of 0.
func (r *e2eRig) warm(n int) {
	for i := 0; i < n; i++ {
		r.tick()
		r.window()
	}
}

func TestE2E_Variants(t *testing.T) {
	wantOneKill := func(t *testing.T, sum []push.ServiceUseSummary, met []push.ServiceUseMetrics) {
		t.Helper()
		if got, ok := oomOf(sum, "ad"); !ok || got != 1 {
			t.Errorf("summary OOMKills = %d (present=%v), want 1", got, ok)
		}
		var adKills int64 = -1
		for _, m := range met {
			if m.Service == "ad" {
				adKills = m.OOMKills
			}
		}
		if adKills != 1 {
			t.Errorf("use_metrics = %+v, want service ad with OOMKills=1", met)
		}
	}

	t.Run("a_four_sensor_ticks_per_window", func(t *testing.T) {
		n := newE2ENode(t, "node-a", "ad-x", "ad")
		r := newE2ERig(t, n)
		r.warm(2)
		r.tick()
		n.write(2, 990)
		r.tick()
		r.tick()
		r.tick()
		sum, met := r.window()
		wantOneKill(t, sum, met)
	})

	t.Run("b_read_between_ticks", func(t *testing.T) {
		n := newE2ENode(t, "node-a", "ad-x", "ad")
		r := newE2ERig(t, n)
		r.warm(2)
		n.write(2, 990) // the file changed, but no sensor tick has run yet
		sum, _ := r.window()
		if got, _ := oomOf(sum, "ad"); got != 0 {
			t.Fatalf("window before the tick: %d, want 0", got)
		}
		r.tick()
		sum, met := r.window() // the increase lands one window later, not lost
		wantOneKill(t, sum, met)
	})

	t.Run("c_target_absent_for_one_collector_tick", func(t *testing.T) {
		a := newE2ENode(t, "node-a", "ad-x", "ad")
		b := newE2ENode(t, "node-b", "other-x", "other")
		r := newE2ERig(t, a, b)
		r.warm(2)
		r.hub.mu.Lock()
		r.hub.hidden["node-a"] = true
		r.hub.mu.Unlock()
		a.write(2, 990)
		r.tick()
		r.window() // node-a missing from the live list this tick
		r.hub.mu.Lock()
		r.hub.hidden["node-a"] = false
		r.hub.mu.Unlock()
		r.tick()
		sum, met := r.window()
		wantOneKill(t, sum, met)
	})

	t.Run("d_memory_drops_in_same_window", func(t *testing.T) {
		n := newE2ENode(t, "node-a", "ad-x", "ad")
		r := newE2ERig(t, n)
		r.warm(2)
		n.write(2, 50)
		r.tick()
		sum, _ := r.window()
		if got, _ := oomOf(sum, "ad"); got != 1 {
			t.Errorf("summary OOMKills = %d, want 1", got)
		}
	})

	t.Run("e_service_label_changes_between_reads", func(t *testing.T) {
		n := newE2ENode(t, "node-a", "ad-x", "ad")
		r := newE2ERig(t, n)
		r.warm(2)
		n.write(2, 990)
		// The pod's identity label flips; wait for the informer to reflect it.
		p := n.pod.DeepCopy()
		p.Labels["app.kubernetes.io/name"] = "ad-renamed"
		if _, err := n.clients.CoreV1().Pods("ops").Update(context.Background(), p, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		waitUntil(t, func() bool {
			got, _ := n.e.lister.Pods("ops").Get("ad-x")
			return got != nil && got.Labels["app.kubernetes.io/name"] == "ad-renamed"
		})
		r.tick()
		sum, _ := r.window()
		if got, ok := oomOf(sum, "ad-renamed"); !ok || got != 1 {
			t.Errorf("renamed service OOMKills = %d (present=%v), want 1", got, ok)
		}
	})

	t.Run("f_two_targets_same_pod", func(t *testing.T) {
		a := newE2ENode(t, "node-a", "ad-x", "ad")
		b := newE2ENode(t, "node-b", "ad-x", "ad")
		r := newE2ERig(t, a, b)
		r.warm(2)
		a.write(2, 990)
		b.write(2, 990)
		r.tick()
		sum, _ := r.window()
		// Each target is diffed against its own baseline and summed per service.
		if got, _ := oomOf(sum, "ad"); got != 2 {
			t.Errorf("summary OOMKills = %d, want 2 (1 per target)", got)
		}
	})

	t.Run("g1_cgroup_dir_missing_for_one_read", func(t *testing.T) {
		n := newE2ENode(t, "node-a", "ad-x", "ad")
		r := newE2ERig(t, n)
		r.warm(2)
		moved := n.podDir + ".gone"
		if err := os.Rename(n.podDir, moved); err != nil {
			t.Fatal(err)
		}
		r.tick()
		r.window() // pod absent from the snapshot the collector reads
		if err := os.Rename(moved, n.podDir); err != nil {
			t.Fatal(err)
		}
		n.write(2, 990)
		r.tick()
		sum, met := r.window()
		wantOneKill(t, sum, met)
	})

	t.Run("g2_pod_not_running_for_one_read", func(t *testing.T) {
		n := newE2ENode(t, "node-a", "ad-x", "ad")
		r := newE2ERig(t, n)
		r.warm(2)
		setPhase := func(ph corev1.PodPhase) {
			p := n.pod.DeepCopy()
			p.Status.Phase = ph
			if _, err := n.clients.CoreV1().Pods("ops").UpdateStatus(context.Background(), p, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			waitUntil(t, func() bool {
				got, _ := n.e.lister.Pods("ops").Get("ad-x")
				return got != nil && got.Status.Phase == ph
			})
		}
		setPhase(corev1.PodPending)
		r.tick()
		r.window()
		setPhase(corev1.PodRunning)
		n.write(2, 990)
		r.tick()
		sum, met := r.window()
		wantOneKill(t, sum, met)
	})
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 500; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not reached")
}

// TestE2E_StatusSourceCatchesWhatCgroupMisses: the cgroup counter stays flat
// (as if its read were broken or the file unreadable), but the kubelet records
// an OOMKilled restart in pod status. The independent status source carries
// the kill to the frame.
func TestE2E_StatusSourceCatchesWhatCgroupMisses(t *testing.T) {
	n := newE2ENode(t, "node-a", "ad-x", "ad")
	setStatus := func(restarts int32, reason string) {
		p := n.pod.DeepCopy()
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "ad", RestartCount: restarts}}
		if reason != "" {
			p.Status.ContainerStatuses[0].LastTerminationState.Terminated = &corev1.ContainerStateTerminated{
				Reason: reason, ExitCode: 137, FinishedAt: metav1.NewTime(time.Now()),
			}
		}
		if _, err := n.clients.CoreV1().Pods("ops").UpdateStatus(context.Background(), p, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		waitUntil(t, func() bool {
			got, _ := n.e.lister.Pods("ops").Get("ad-x")
			return got != nil && len(got.Status.ContainerStatuses) == 1 && got.Status.ContainerStatuses[0].RestartCount == restarts
		})
	}
	setStatus(0, "")
	r := newE2ERig(t, n)
	r.warm(2)
	setStatus(1, "OOMKilled") // cgroup files untouched: oom_kill stays 0
	r.tick()
	sum, met := r.window()
	if got, ok := oomOf(sum, "ad"); !ok || got != 1 {
		t.Fatalf("summary OOMKills = %d (present=%v), want 1 from the status source", got, ok)
	}
	if len(met) != 1 || met[0].OOMKills != 1 {
		t.Fatalf("use_metrics = %+v, want OOMKills=1", met)
	}
}
