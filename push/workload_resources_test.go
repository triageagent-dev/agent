package push

import (
	"reflect"
	"testing"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	discoveryfake "k8s.io/client-go/discovery/fake"
	clienttesting "k8s.io/client-go/testing"
)

// fakeDiscovery builds a discovery.DiscoveryInterface backed by a fixed set
// of served GroupVersion resource lists, for resolveWorkloadResources'
// discovery gate.
func fakeDiscovery(resources ...*metav1.APIResourceList) *discoveryfake.FakeDiscovery {
	return &discoveryfake.FakeDiscovery{Fake: &clienttesting.Fake{Resources: resources}}
}

// --- resolveWorkloadResources (discovery gate) ---

func TestResolveWorkloadResources_ServedResourcesKeepTheirDiscoveredKind(t *testing.T) {
	dc := fakeDiscovery(&metav1.APIResourceList{
		GroupVersion: "apps/v1",
		APIResources: []metav1.APIResource{
			{Name: "deployments", Kind: "Deployment", Namespaced: true},
			{Name: "statefulsets", Kind: "StatefulSet", Namespaced: true},
		},
	})
	requested := []schema.GroupVersionResource{
		{Group: "apps", Version: "v1", Resource: "deployments"},
		{Group: "apps", Version: "v1", Resource: "statefulsets"},
	}
	got := resolveWorkloadResources(dc, requested, logr.Discard())
	if len(got) != 2 {
		t.Fatalf("resolveWorkloadResources = %+v, want 2 resolved resources", got)
	}
	want := map[schema.GroupVersionResource]string{
		requested[0]: "Deployment",
		requested[1]: "StatefulSet",
	}
	for _, wr := range got {
		if want[wr.GVR] != wr.Kind {
			t.Errorf("resource %v resolved Kind = %q, want %q", wr.GVR, wr.Kind, want[wr.GVR])
		}
	}
}

// TestResolveWorkloadResources_UnservedGroupVersionSkipped is the "cluster
// without Argo installed" case: a whole GroupVersion the server does not
// serve at all must be skipped without failing the served resources
// alongside it.
func TestResolveWorkloadResources_UnservedGroupVersionSkipped(t *testing.T) {
	dc := fakeDiscovery(&metav1.APIResourceList{
		GroupVersion: "apps/v1",
		APIResources: []metav1.APIResource{{Name: "deployments", Kind: "Deployment", Namespaced: true}},
	})
	requested := []schema.GroupVersionResource{
		{Group: "apps", Version: "v1", Resource: "deployments"},
		{Group: "argoproj.io", Version: "v1alpha1", Resource: "rollouts"},
	}
	got := resolveWorkloadResources(dc, requested, logr.Discard())
	if len(got) != 1 || got[0].Kind != "Deployment" {
		t.Fatalf("resolveWorkloadResources = %+v, want only the served Deployment resource — an unserved GroupVersion must not fail the whole list", got)
	}
}

// TestResolveWorkloadResources_UnservedResourceInAServedGroupVersionSkipped
// covers a GroupVersion that IS served but does not list one of the
// requested resources.
func TestResolveWorkloadResources_UnservedResourceInAServedGroupVersionSkipped(t *testing.T) {
	dc := fakeDiscovery(&metav1.APIResourceList{
		GroupVersion: "apps/v1",
		APIResources: []metav1.APIResource{{Name: "deployments", Kind: "Deployment", Namespaced: true}},
	})
	requested := []schema.GroupVersionResource{
		{Group: "apps", Version: "v1", Resource: "deployments"},
		{Group: "apps", Version: "v1", Resource: "widgets"},
	}
	got := resolveWorkloadResources(dc, requested, logr.Discard())
	if len(got) != 1 || got[0].Kind != "Deployment" {
		t.Fatalf("resolveWorkloadResources = %+v, want only deployments — widgets is not in this GroupVersion's resource list", got)
	}
}

// TestResolveWorkloadResources_OneDiscoveryCallPerDistinctGroupVersion
// proves several requested resources sharing one GroupVersion (the common
// apps/v1 case) cost exactly one ServerResourcesForGroupVersion call.
func TestResolveWorkloadResources_OneDiscoveryCallPerDistinctGroupVersion(t *testing.T) {
	dc := fakeDiscovery(&metav1.APIResourceList{
		GroupVersion: "apps/v1",
		APIResources: []metav1.APIResource{
			{Name: "deployments", Kind: "Deployment", Namespaced: true},
			{Name: "statefulsets", Kind: "StatefulSet", Namespaced: true},
			{Name: "daemonsets", Kind: "DaemonSet", Namespaced: true},
		},
	})
	requested := []schema.GroupVersionResource{
		{Group: "apps", Version: "v1", Resource: "deployments"},
		{Group: "apps", Version: "v1", Resource: "statefulsets"},
		{Group: "apps", Version: "v1", Resource: "daemonsets"},
	}
	resolveWorkloadResources(dc, requested, logr.Discard())

	calls := 0
	for _, a := range dc.Actions() {
		if a.GetVerb() == "get" {
			calls++
		}
	}
	if calls != 1 {
		t.Errorf("ServerResourcesForGroupVersion invoked %d times, want exactly 1 (three requested resources share one GroupVersion)", calls)
	}
}

func TestResolveWorkloadResources_NilDiscoveryResolvesNothing(t *testing.T) {
	got := resolveWorkloadResources(nil, DefaultWorkloadResources, logr.Discard())
	if got != nil {
		t.Errorf("resolveWorkloadResources(nil, ...) = %+v, want nil (no discovery client wired)", got)
	}
}

func TestResolveWorkloadResources_EmptyRequestedResolvesNothing(t *testing.T) {
	got := resolveWorkloadResources(fakeDiscovery(), nil, logr.Discard())
	if got != nil {
		t.Errorf("resolveWorkloadResources(dc, nil, ...) = %+v, want nil", got)
	}
}

// --- ParseWorkloadResource / ParseWorkloadResourceList (flag parsing) ---

func TestParseWorkloadResource(t *testing.T) {
	tests := []struct {
		raw     string
		want    schema.GroupVersionResource
		wantErr bool
	}{
		{raw: "apps/v1/deployments", want: schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}},
		{raw: "argoproj.io/v1alpha1/rollouts", want: schema.GroupVersionResource{Group: "argoproj.io", Version: "v1alpha1", Resource: "rollouts"}},
		{raw: "deployments", wantErr: true},
		{raw: "v1/deployments", wantErr: true},
		{raw: "apps/v1/", wantErr: true},
		{raw: "/v1/deployments", wantErr: true},
		{raw: "apps//deployments", wantErr: true},
		{raw: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := ParseWorkloadResource(tt.raw)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseWorkloadResource(%q) err = %v, wantErr %v", tt.raw, err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Errorf("ParseWorkloadResource(%q) = %+v, want %+v", tt.raw, got, tt.want)
			}
		})
	}
}

func TestParseWorkloadResourceList_EmptyIsDefault(t *testing.T) {
	got, err := ParseWorkloadResourceList(nil)
	if err != nil {
		t.Fatalf("ParseWorkloadResourceList(nil) error: %v", err)
	}
	if !reflect.DeepEqual(got, DefaultWorkloadResources) {
		t.Errorf("ParseWorkloadResourceList(nil) = %+v, want DefaultWorkloadResources %+v", got, DefaultWorkloadResources)
	}
}

func TestParseWorkloadResourceList_GivenListReplacesDefault(t *testing.T) {
	got, err := ParseWorkloadResourceList([]string{"argoproj.io/v1alpha1/rollouts"})
	if err != nil {
		t.Fatalf("ParseWorkloadResourceList error: %v", err)
	}
	want := []schema.GroupVersionResource{{Group: "argoproj.io", Version: "v1alpha1", Resource: "rollouts"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ParseWorkloadResourceList([rollouts]) = %+v, want exactly %+v (no defaults mixed in)", got, want)
	}
}

func TestParseWorkloadResourceList_MalformedValueRejected(t *testing.T) {
	_, err := ParseWorkloadResourceList([]string{"apps/v1/deployments", "bogus"})
	if err == nil {
		t.Fatal("ParseWorkloadResourceList with a malformed value must return an error")
	}
}
