package logexporter

import (
	"os"
	"path/filepath"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
)

// parseLogFilename parses a /var/log/containers entry's basename:
//
//	<pod>_<namespace>_<container>-<containerid>.log
//
// e.g. "api-7d9f8b6c4-abcde_ops_api-3f2a1b9c8d7e6f5a4b3c2d1e0f9a8b7c.log" →
// pod="api-7d9f8b6c4-abcde", namespace="ops", container="api". The container ID
// suffix is dropped by splitting on the LAST "-" (container IDs are hex and
// never contain "_", but a container NAME can contain "-", so splitting on the
// last "-" rather than the first is what keeps a hyphenated container name
// intact). ok is false for anything that doesn't fit this shape (should not
// occur for real kubelet-managed symlinks, but a defensive parse never trusts a
// directory listing to be well-formed).
func parseLogFilename(name string) (pod, namespace, container string, ok bool) {
	name = strings.TrimSuffix(name, ".log")
	if name == "" {
		return "", "", "", false
	}
	idx := strings.LastIndex(name, "-")
	if idx <= 0 || idx == len(name)-1 {
		return "", "", "", false
	}
	head := name[:idx]
	parts := strings.SplitN(head, "_", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

// namespaceSet turns a comma-separated, exact-name namespace list into a lookup
// set. Returns nil for an empty/blank-only list, which means "no filter — every
// namespace's files are tailed" (namespaceAllowed treats a nil set as allow-all).
func namespaceSet(list []string) map[string]bool {
	var set map[string]bool
	for _, ns := range list {
		if ns = strings.TrimSpace(ns); ns != "" {
			if set == nil {
				set = make(map[string]bool, len(list))
			}
			set[ns] = true
		}
	}
	return set
}

func namespaceAllowed(set map[string]bool, ns string) bool {
	return set == nil || set[ns]
}

// serviceIdentity resolves a pod's service identity for log attribution, so a
// log signal lands on the same graph node as the pod's USE and RED signals
// (the workload the virtual app is named after, push.GroupNameFor in its
// default workload mode):
//
//  1. the pod's triageagent.dev/virtual-app-group annotation (copied from the
//     pod template), the explicit grouping an operator set;
//  2. the name of the workload that owns the pod: the Deployment behind a
//     ReplicaSet (its pod-template-hash suffix trimmed), else the DaemonSet,
//     StatefulSet, Job or other controller's own name;
//  3. a pod with no owner: the app.kubernetes.io/name label, else the plain
//     "app" label;
//  4. "" (unattributable: skipped, never counted under any service).
//
// The labels alone named a DaemonSet pod "ebpf" where its app is
// "triage-agent-ebpf", and the core dropped every log signal of such a service
// as an unknown one. A namespace in label identity mode
// (triageagent.dev/virtual-app-identity: labels) names apps by label; the node
// cannot see that namespace annotation, so there the owner name stays.
func serviceIdentity(pod *corev1.Pod) string {
	if v := strings.TrimSpace(pod.Annotations[annotationkeys.VirtualAppGroup]); v != "" {
		return v
	}
	for _, o := range pod.OwnerReferences {
		if o.Controller == nil || !*o.Controller {
			continue
		}
		name := o.Name
		if o.Kind == "ReplicaSet" {
			if h := pod.Labels["pod-template-hash"]; h != "" {
				name = strings.TrimSuffix(name, "-"+h)
			}
		}
		if name != "" {
			return name
		}
	}
	if v := pod.Labels["app.kubernetes.io/name"]; v != "" {
		return v
	}
	return pod.Labels["app"]
}

// resolveService looks up a pod's service identity via the node-scoped Pod
// lister. Returns ("", false) when the pod isn't in the lister's cache yet (a
// timing race between a new pod's log file appearing and its object reaching
// this informer's watch) — the caller retries this on the next directory
// rescan rather than giving up permanently.
//
// A pod carrying annotationkeys.Triage "false" (copied from its pod
// template) resolves to "" — unattributable, so its lines are never counted.
// Owner- and namespace-level exclusion is applied by the controller
// (push/exclusion.go), which sees owners this node-scoped exporter does not.
func resolveService(lister corelisters.PodLister, namespace, pod string) (string, bool) {
	p, err := lister.Pods(namespace).Get(pod)
	if err != nil {
		return "", false
	}
	if p.Annotations[annotationkeys.Triage] == "false" {
		return "", true
	}
	return serviceIdentity(p), true
}

// discoveredFile is one currently-present entry under the log directory that
// passed the namespace filter and parsed as a valid container log filename.
type discoveredFile struct {
	path      string // full path, e.g. /var/log/containers/<name>.log
	pod       string
	namespace string
	container string
}

// scanLogDir lists dir non-recursively and returns every *.log entry that
// parses as a container log filename and passes nsAllow. Sorted by path for
// deterministic diffing against the previously tracked set. A missing dir
// (nothing mounted yet, or a test fixture that hasn't created it) returns an
// empty, non-error result — the next rescan tries again.
func scanLogDir(dir string, nsAllow map[string]bool, nsAllowFn func(string) bool) []discoveredFile {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []discoveredFile
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		pod, ns, container, ok := parseLogFilename(name)
		if !ok {
			continue
		}
		if nsAllowFn != nil {
			if !nsAllowFn(ns) {
				continue
			}
		} else if !namespaceAllowed(nsAllow, ns) {
			continue
		}
		out = append(out, discoveredFile{
			path:      filepath.Join(dir, name),
			pod:       pod,
			namespace: ns,
			container: container,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out
}
