// Workload resources (docs/todo/virtual-apps.md "Workload resources"):
// the configurable, discovery-gated list of namespaced GVRs virtual-app
// synthesis discovers generically, replacing one typed informer per
// hardcoded kind with one unstructured informer per configured resource.
package push

import (
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

// DefaultWorkloadResources is the list --workload-resources uses when the
// flag is not given at all. Giving the flag even once REPLACES this list
// entirely — it is never unioned with it.
var DefaultWorkloadResources = []schema.GroupVersionResource{
	{Group: "apps", Version: "v1", Resource: "deployments"},
	{Group: "apps", Version: "v1", Resource: "statefulsets"},
	{Group: "apps", Version: "v1", Resource: "daemonsets"},
	{Group: "argoproj.io", Version: "v1alpha1", Resource: "rollouts"},
}

// workloadResource pairs a configured GVR with the Kind API discovery
// resolved it to (resolveWorkloadResources) — the Kind every generic
// workload informer/lister callback carries alongside the object, since a
// List response's items are not guaranteed to carry TypeMeta.
type workloadResource struct {
	GVR  schema.GroupVersionResource
	Kind string
}

// ParseWorkloadResource parses one --workload-resources value of the form
// group/version/resource (e.g. "apps/v1/deployments" or
// "argoproj.io/v1alpha1/rollouts"). The core API group's "/v1/..." form is
// not accepted — every workload resource this feature targets is a
// namespaced kind under a named group.
func ParseWorkloadResource(raw string) (schema.GroupVersionResource, error) {
	parts := strings.Split(raw, "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return schema.GroupVersionResource{}, fmt.Errorf("--workload-resources value %q must have the form group/version/resource (e.g. apps/v1/deployments)", raw)
	}
	return schema.GroupVersionResource{Group: parts[0], Version: parts[1], Resource: parts[2]}, nil
}

// ParseWorkloadResourceList parses every --workload-resources value in
// order. An empty raw list returns DefaultWorkloadResources — the flag
// omitted entirely means the default list, and giving it even once replaces
// that default rather than adding to it (the caller collects raw values with
// that replacement semantic; this function only ever sees the final list).
func ParseWorkloadResourceList(raw []string) ([]schema.GroupVersionResource, error) {
	if len(raw) == 0 {
		return append([]schema.GroupVersionResource(nil), DefaultWorkloadResources...), nil
	}
	out := make([]schema.GroupVersionResource, 0, len(raw))
	for _, r := range raw {
		gvr, err := ParseWorkloadResource(r)
		if err != nil {
			return nil, err
		}
		out = append(out, gvr)
	}
	return out, nil
}

// resolveWorkloadResources checks requested against what the API server
// actually serves, one ServerResourcesForGroupVersion call per distinct
// GroupVersion regardless of how many requested resources share it. A
// resource that is not served — its GroupVersion does not exist at all, or
// exists but does not list that resource — is dropped with one INFO line
// here, at startup, never per namespace: this is what keeps a cluster
// without (say) Argo Rollouts installed unaffected by requesting them. The
// discovered Kind travels with the surviving GVR so every informer/lister
// callback downstream carries it without re-deriving it per object. dc nil
// (no discovery client wired) resolves nothing — every caller that predates
// this feature, and any test that does not wire a fake discovery client,
// gets zero generic workload resources rather than issuing a live call.
func resolveWorkloadResources(dc discovery.DiscoveryInterface, requested []schema.GroupVersionResource, log logr.Logger) []workloadResource {
	if dc == nil || len(requested) == 0 {
		return nil
	}

	type groupVersion struct{ Group, Version string }
	byGV := make(map[groupVersion][]schema.GroupVersionResource)
	var order []groupVersion
	for _, gvr := range requested {
		gv := groupVersion{gvr.Group, gvr.Version}
		if _, ok := byGV[gv]; !ok {
			order = append(order, gv)
		}
		byGV[gv] = append(byGV[gv], gvr)
	}

	out := make([]workloadResource, 0, len(requested))
	for _, gv := range order {
		gvString := schema.GroupVersion{Group: gv.Group, Version: gv.Version}.String()
		list, err := dc.ServerResourcesForGroupVersion(gvString)
		if err != nil {
			for _, gvr := range byGV[gv] {
				log.Info("workload resource not served; skipping", "resource", gvr.String())
			}
			continue
		}
		kindByName := make(map[string]string, len(list.APIResources))
		for _, r := range list.APIResources {
			kindByName[r.Name] = r.Kind
		}
		for _, gvr := range byGV[gv] {
			kind, ok := kindByName[gvr.Resource]
			if !ok {
				log.Info("workload resource not served; skipping", "resource", gvr.String())
				continue
			}
			out = append(out, workloadResource{GVR: gvr, Kind: kind})
		}
	}
	return out
}
