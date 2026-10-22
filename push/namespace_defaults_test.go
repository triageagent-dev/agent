package push

import (
	"reflect"
	"testing"
)

// --- namespaceAnnotations: parsing ---

func TestNamespaceAnnotations(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        namespaceDefaults
	}{
		{
			name:        "no annotations at all",
			annotations: nil,
			want:        namespaceDefaults{},
		},
		{
			name: "domain, description, and a valid tier",
			annotations: map[string]string{
				annotationDomain: "commerce", annotationDescription: "Checkout namespace", annotationTier: "2",
			},
			want: namespaceDefaults{Domain: "commerce", Description: "Checkout namespace", Tier: 2, HasTier: true},
		},
		{
			name:        "virtual-app: false excludes the whole namespace",
			annotations: map[string]string{annotationVirtualApp: "false"},
			want:        namespaceDefaults{Excluded: true},
		},
		{
			name:        "virtual-app: true does not exclude",
			annotations: map[string]string{annotationVirtualApp: "true"},
			want:        namespaceDefaults{Excluded: false},
		},
		{
			name:        "virtual-app-group has no namespace-level meaning and is discarded",
			annotations: map[string]string{annotationVirtualAppGroup: "billing"},
			want:        namespaceDefaults{},
		},
		{
			name:        "unparsable tier is reported invalid, not a declared tier",
			annotations: map[string]string{annotationTier: "critical"},
			want:        namespaceDefaults{TierInvalid: true},
		},
		{
			name:        "a valid weight",
			annotations: map[string]string{annotationWeight: "3.5"},
			want:        namespaceDefaults{Weight: 3.5, HasWeight: true},
		},
		{
			name:        "unparsable weight is reported invalid, not a declared weight",
			annotations: map[string]string{annotationWeight: "20"},
			want:        namespaceDefaults{WeightInvalid: true},
		},
		{
			name:        "virtual-app-identity: workload is the explicit default",
			annotations: map[string]string{annotationVirtualAppIdentity: "workload"},
			want:        namespaceDefaults{IdentityMode: IdentityModeWorkload},
		},
		{
			name:        "virtual-app-identity: labels opts into the label chain",
			annotations: map[string]string{annotationVirtualAppIdentity: "labels"},
			want:        namespaceDefaults{IdentityMode: IdentityModeLabels},
		},
		{
			name:        "virtual-app-identity absent defaults to workload (zero value)",
			annotations: nil,
			want:        namespaceDefaults{},
		},
		{
			name:        "virtual-app-identity: Workload (wrong case) falls back to workload, reported invalid",
			annotations: map[string]string{annotationVirtualAppIdentity: "Workload"},
			want:        namespaceDefaults{IdentityModeInvalid: true},
		},
		{
			name:        "virtual-app-identity: garbage falls back to workload, reported invalid",
			annotations: map[string]string{annotationVirtualAppIdentity: "garbage"},
			want:        namespaceDefaults{IdentityModeInvalid: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := namespaceAnnotations(tt.annotations)
			if got != tt.want {
				t.Errorf("namespaceAnnotations(%v) = %+v, want %+v", tt.annotations, got, tt.want)
			}
		})
	}
}

// --- applyNamespaceDefaultsToWorkload: precedence + exclusion OR ---

func TestApplyNamespaceDefaultsToWorkload(t *testing.T) {
	tests := []struct {
		name string
		w    workloadInfo
		ns   namespaceDefaults
		want workloadInfo
	}{
		{
			name: "workload inherits everything when it declares nothing",
			w:    workloadInfo{Namespace: "ops", Name: "api"},
			ns:   namespaceDefaults{Domain: "commerce", Description: "Checkout", Tier: 3, HasTier: true},
			want: workloadInfo{Namespace: "ops", Name: "api", Domain: "commerce", Description: "Checkout", Tier: 3, HasTier: true},
		},
		{
			name: "workload's own domain wins over the namespace default",
			w:    workloadInfo{Namespace: "ops", Name: "api", Domain: "payments"},
			ns:   namespaceDefaults{Domain: "commerce"},
			want: workloadInfo{Namespace: "ops", Name: "api", Domain: "payments"},
		},
		{
			name: "workload's own description wins over the namespace default",
			w:    workloadInfo{Namespace: "ops", Name: "api", Description: "own description"},
			ns:   namespaceDefaults{Description: "ns description"},
			want: workloadInfo{Namespace: "ops", Name: "api", Description: "own description"},
		},
		{
			name: "workload's own tier wins over the namespace default",
			w:    workloadInfo{Namespace: "ops", Name: "api", Tier: 1, HasTier: true},
			ns:   namespaceDefaults{Tier: 5, HasTier: true},
			want: workloadInfo{Namespace: "ops", Name: "api", Tier: 1, HasTier: true},
		},
		{
			name: "declared tier 0 is not overridden by a namespace default",
			w:    workloadInfo{Namespace: "ops", Name: "api", Tier: 0, HasTier: true},
			ns:   namespaceDefaults{Tier: 5, HasTier: true},
			want: workloadInfo{Namespace: "ops", Name: "api", Tier: 0, HasTier: true},
		},
		{
			name: "workload inherits the namespace weight when it declares none",
			w:    workloadInfo{Namespace: "ops", Name: "api"},
			ns:   namespaceDefaults{Weight: 4, HasWeight: true},
			want: workloadInfo{Namespace: "ops", Name: "api", Weight: 4, HasWeight: true},
		},
		{
			name: "workload's own weight wins over the namespace default",
			w:    workloadInfo{Namespace: "ops", Name: "api", Weight: 2, HasWeight: true},
			ns:   namespaceDefaults{Weight: 8, HasWeight: true},
			want: workloadInfo{Namespace: "ops", Name: "api", Weight: 2, HasWeight: true},
		},
		{
			name: "namespace exclusion ORs in even when the workload itself is not excluded",
			w:    workloadInfo{Namespace: "ops", Name: "api", Excluded: false},
			ns:   namespaceDefaults{Excluded: true},
			want: workloadInfo{Namespace: "ops", Name: "api", Excluded: true},
		},
		{
			name: "a workload's own exclusion survives a non-excluding namespace",
			w:    workloadInfo{Namespace: "ops", Name: "api", Excluded: true},
			ns:   namespaceDefaults{Excluded: false},
			want: workloadInfo{Namespace: "ops", Name: "api", Excluded: true},
		},
		{
			name: "no namespace defaults at all leaves the workload untouched",
			w:    workloadInfo{Namespace: "ops", Name: "api", Domain: "payments", Tier: 2, HasTier: true},
			ns:   namespaceDefaults{},
			want: workloadInfo{Namespace: "ops", Name: "api", Domain: "payments", Tier: 2, HasTier: true},
		},
		{
			name: "workload identity mode always mirrors the namespace's — there is no workload-level source",
			w:    workloadInfo{Namespace: "ops", Name: "api"},
			ns:   namespaceDefaults{IdentityMode: IdentityModeWorkload},
			want: workloadInfo{Namespace: "ops", Name: "api", IdentityMode: IdentityModeWorkload},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := applyNamespaceDefaultsToWorkload(tt.w, tt.ns)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("applyNamespaceDefaultsToWorkload() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// --- nsDefaultsStore ---

func TestNsDefaultsStore(t *testing.T) {
	s := newNsDefaultsStore()
	if _, ok := s.get("ops"); ok {
		t.Fatal("empty store must report ok=false for any namespace")
	}
	s.set("ops", namespaceDefaults{Domain: "commerce"})
	got, ok := s.get("ops")
	if !ok || got.Domain != "commerce" {
		t.Fatalf("get(ops) = (%+v, %v), want commerce/true", got, ok)
	}
	s.remove("ops")
	if _, ok := s.get("ops"); ok {
		t.Error("removed namespace must report ok=false")
	}
}
