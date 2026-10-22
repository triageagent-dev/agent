package push

import "testing"

// Not configured is not "everything". An agent given a primary tenant and no
// --target-namespaces must observe only the triageagent.dev/tenant-annotated set, not
// the whole cluster — cluster-a shipped train, perf, sandbox, argo and sqlproxy
// under one tenant because this returned true for every namespace.
func TestEmptyListNeverWidensScope(t *testing.T) {
	annotated := newDynamicScopeSet()
	annotated.add("dev", "tenant-a")

	for _, clusterMode := range []bool{false, true} {
		if !effectiveInScopeMode("dev", nil, annotated, clusterMode) {
			t.Errorf("clusterMode=%v: annotated namespace must be in scope", clusterMode)
		}
		if effectiveInScopeMode("sandbox", nil, annotated, clusterMode) {
			t.Errorf("clusterMode=%v: unannotated namespace must NOT be in scope", clusterMode)
		}
	}

	// Nothing annotated at all means nothing observed, not everything.
	empty := newDynamicScopeSet()
	if effectiveInScopeMode("anything", nil, empty, false) {
		t.Error("no list and no annotations must observe nothing")
	}

	// An explicit list still wins, and still unions with the annotated set.
	if !effectiveInScopeMode("prod", []string{"prod"}, empty, false) {
		t.Error("explicit list entry must be in scope")
	}
	if effectiveInScopeMode("other", []string{"prod"}, empty, false) {
		t.Error("namespace outside the list and unannotated must not be in scope")
	}

	// An unwired watcher is not a deployment: keep the permissive answer.
	if !effectiveInScopeMode("anything", nil, nil, false) {
		t.Error("nil dynamic set must stay permissive for bare-struct callers")
	}
}
