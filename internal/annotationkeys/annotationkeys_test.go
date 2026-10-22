package annotationkeys

import "testing"

// TestKeyNames pins each constant's value, so a typo in the table shows up
// here instead of silently changing what a reader or writer resolves.
func TestKeyNames(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"Tenant", Tenant, "triageagent.dev/tenant"},
		{"Tier", Tier, "triageagent.dev/tier"},
		{"Weight", Weight, "triageagent.dev/weight"},
		{"Domain", Domain, "triageagent.dev/domain"},
		{"Description", Description, "triageagent.dev/description"},
		{"VirtualApp", VirtualApp, "triageagent.dev/virtual-app"},
		{"VirtualAppGroup", VirtualAppGroup, "triageagent.dev/virtual-app-group"},
		{"VirtualAppIdentity", VirtualAppIdentity, "triageagent.dev/virtual-app-identity"},
		{"Instrument", Instrument, "triageagent.dev/instrument"},
		{"Triage", Triage, "triageagent.dev/triage"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.want)
		}
	}
}
