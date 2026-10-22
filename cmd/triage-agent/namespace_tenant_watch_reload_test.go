package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
)

func ns(name, tenant string) *corev1.Namespace {
	n := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if tenant != "" {
		n.Annotations = map[string]string{annotationkeys.Tenant: tenant}
	}
	return n
}

func fired(w *namespaceTenantWatch) bool {
	select {
	case <-w.Changed():
		return true
	default:
		return false
	}
}

// TestNamespaceTenantWatchSignalsOnlyMembershipEdges: the eBPF role reloads on
// this signal, and a reload detaches and re-attaches every uprobe on the node.
// Namespace objects are updated constantly for unrelated reasons (labels,
// finalizers, informer resync), so signalling on anything but an actual
// membership change would re-instrument the node for nothing.
func TestNamespaceTenantWatchSignalsOnlyMembershipEdges(t *testing.T) {
	w := &namespaceTenantWatch{set: map[string]bool{}, changed: make(chan struct{}, 1)}

	w.apply(ns("gitlab", "gitlab"), false)
	if !fired(w) {
		t.Error("annotating a namespace must signal")
	}

	// Same annotation again — a resync, not a change.
	w.apply(ns("gitlab", "gitlab"), false)
	if fired(w) {
		t.Error("re-applying an unchanged namespace must not signal")
	}

	// An unrelated namespace that was never annotated.
	w.apply(ns("kube-system", ""), false)
	if fired(w) {
		t.Error("an unannotated namespace must not signal")
	}

	// Removing the annotation is an edge in the other direction.
	w.apply(ns("gitlab", ""), false)
	if !fired(w) {
		t.Error("removing the annotation must signal")
	}

	w.apply(ns("shop", "shop"), false)
	if !fired(w) {
		t.Error("a second namespace must signal")
	}
	w.apply(ns("shop", "shop"), true)
	if !fired(w) {
		t.Error("deleting an annotated namespace must signal")
	}
}

// TestNamespaceTenantWatchNamesUnsyncedIsNotEmpty: "not listed yet" and "none
// are annotated" mean opposite things — the first must not be read as the
// second, or a reload racing startup would narrow the scope to nothing.
func TestNamespaceTenantWatchNamesUnsyncedIsNotEmpty(t *testing.T) {
	w := &namespaceTenantWatch{set: map[string]bool{}, changed: make(chan struct{}, 1)}
	if _, ok := w.names(); ok {
		t.Fatal("names() must report not-ok before the first list completes")
	}
	w.apply(ns("b", "t"), false)
	w.apply(ns("a", "t"), false)
	w.mu.Lock()
	w.seen = true
	w.mu.Unlock()

	got, ok := w.names()
	if !ok {
		t.Fatal("names() must report ok once synced")
	}
	// Sorted, so a caller comparing two snapshots sees a change only when
	// membership actually changed rather than when map order shifted.
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("names() = %v, want [a b] sorted", got)
	}
}

// TestClassifyReloadStop: a reload cancels the run itself, so an error that
// is exactly context.Canceled (or wraps it) is the expected shape of a clean
// stop and must not be logged or treated as fatal. Any other error,
// including OBI's own finisher-timeout error, which is a plain error, not
// context.Canceled, must be logged, but the caller must still not return it
// on the reload path.
func TestClassifyReloadStop(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"bare context.Canceled", context.Canceled, false},
		{"wrapped context.Canceled", fmt.Errorf("run: %w", context.Canceled), false},
		{"obi finisher timeout", errors.New("setupAppO11y: timed out waiting for finisher process finder to finish"), true},
		{"other error", errors.New("some other failure"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyReloadStop(tc.err); got != tc.want {
				t.Errorf("classifyReloadStop(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestNewNamespaceTenantWatchNamesReadyAfterConstructor guards defect 1: the
// factory's own WaitForCacheSync only reports the informer's store is
// filled, not that the Add handlers have been delivered for it. If
// newNamespaceTenantWatch flipped seen on the store alone, names() could
// still return an empty set with ok=true immediately after construction.
// Run with -count=20 to catch that race; it is not always visible on a
// single run.
func TestNewNamespaceTenantWatchNamesReadyAfterConstructor(t *testing.T) {
	cs := fake.NewSimpleClientset(
		ns("tenant-a-ns1", "tenant-a"),
		ns("tenant-a-ns2", "tenant-a"),
		ns("tenant-b-ns1", "tenant-b"),
		ns("tenant-b-ns2", "tenant-b"),
		ns("kube-system", ""),
		ns("default", ""),
	)

	w, err := newNamespaceTenantWatch(context.Background(), cs)
	if err != nil {
		t.Fatalf("newNamespaceTenantWatch: %v", err)
	}

	got, ok := w.names()
	if !ok {
		t.Fatal("names() must report ok immediately after the constructor returns")
	}
	want := []string{"tenant-a-ns1", "tenant-a-ns2", "tenant-b-ns1", "tenant-b-ns2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("names() = %v, want %v", got, want)
	}
}

func TestSameStrings(t *testing.T) {
	for _, tc := range []struct {
		a, b []string
		want bool
	}{
		{nil, nil, true},
		{[]string{"a"}, []string{"a"}, true},
		{[]string{"a"}, []string{"b"}, false},
		{[]string{"a"}, []string{"a", "b"}, false},
		{nil, []string{"a"}, false},
	} {
		if got := sameStrings(tc.a, tc.b); got != tc.want {
			t.Errorf("sameStrings(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

// TestSettledWaitsForQuiet: a GitOps pass annotates namespaces one object at a
// time, and each one arrives as its own edge. settled must collapse a burst
// into ONE reload — the alternative is re-attaching every uprobe on the node
// once per namespace in the batch.
func TestSettledWaitsForQuiet(t *testing.T) {
	w := &namespaceTenantWatch{set: map[string]bool{}, changed: make(chan struct{}, 1)}
	quiet := 60 * time.Millisecond

	done := make(chan bool, 1)
	start := time.Now()
	go func() { done <- settled(context.Background(), w, quiet) }()

	// Three edges spaced inside the quiet window: each must restart the timer,
	// so settled returns only after the last one plus the full window.
	for range 3 {
		time.Sleep(quiet / 2)
		w.notifyChanged()
	}

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("settled returned false without a cancelled context")
		}
		if elapsed := time.Since(start); elapsed < quiet*2 {
			t.Errorf("settled returned after %v; a burst must not short-circuit the %v quiet window", elapsed, quiet)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("settled never returned")
	}
}

// TestSettledStopsOnContextCancel: shutdown must not be held up by the quiet
// window, and the caller distinguishes the two outcomes to decide whether to
// reload or exit.
func TestSettledStopsOnContextCancel(t *testing.T) {
	w := &namespaceTenantWatch{set: map[string]bool{}, changed: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- settled(ctx, w, time.Hour) }()
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Error("a cancelled context must report not-settled, not a reload")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("settled ignored context cancellation")
	}
}

// TestNamespaceTenantWatchEmptyMeansNone: once the first list completes, an
// empty set is an answer — nothing is onboarded — not an absence of one. The
// eBPF role instruments nothing on it.
func TestNamespaceTenantWatchEmptyMeansNone(t *testing.T) {
	w := &namespaceTenantWatch{set: map[string]bool{}, changed: make(chan struct{}, 1)}
	if _, ok := w.names(); ok {
		t.Error("names() must report not-ok before the first list completes")
	}
	w.mu.Lock()
	w.seen = true
	w.mu.Unlock()

	got, ok := w.names()
	if !ok || len(got) != 0 {
		t.Errorf("names() = %v, %v; want empty, true", got, ok)
	}
	if w.allowed("anything") {
		t.Error("with nothing annotated, no namespace may be read")
	}

	w.apply(ns("gitlab", "gitlab"), false)
	if !w.allowed("gitlab") || w.allowed("other") {
		t.Error("only annotated namespaces may be read")
	}
	w.apply(ns("gitlab", ""), false)
	if got, ok := w.names(); !ok || len(got) != 0 {
		t.Errorf("after de-annotating: names() = %v, %v; want empty, true", got, ok)
	}
}

// TestNamespaceTenantWatchTriageFalseLeavesScope: a tenant-annotated
// namespace that also carries triageagent.dev/triage: "false" is neither
// instrumented by eBPF nor tailed by the log exporter, and toggling the
// annotation live is a membership edge both roles reload on.
func TestNamespaceTenantWatchTriageFalseLeavesScope(t *testing.T) {
	w := &namespaceTenantWatch{set: map[string]bool{}, changed: make(chan struct{}, 1), seen: true}
	w.apply(ns("shop", "shop"), false)
	_ = fired(w)

	off := ns("shop", "shop")
	off.Annotations[annotationkeys.Triage] = "false"
	w.apply(off, false)
	if w.allowed("shop") {
		t.Error("a triage-excluded namespace must not be read")
	}
	if !fired(w) {
		t.Error("excluding a namespace must signal a reload")
	}
	if names, _ := w.names(); len(names) != 0 {
		t.Errorf("instrumented namespaces = %v, want none", names)
	}

	on := ns("shop", "shop")
	on.Annotations[annotationkeys.Triage] = "true"
	w.apply(on, false)
	if !w.allowed("shop") || !fired(w) {
		t.Error("removing the exclusion must bring the namespace back and signal")
	}
}
