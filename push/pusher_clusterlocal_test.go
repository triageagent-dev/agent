package push

import "testing"

// Plaintext to a name that only resolves inside the cluster never leaves the
// pod network, so it must not require --core-allow-plaintext. A name that
// leaves the cluster still must.
func TestValidateEndpointClusterLocalPlaintext(t *testing.T) {
	for _, tc := range []struct {
		endpoint string
		wantErr  bool
	}{
		{"ws://triage-events.triage.svc.cluster.local:8086/events/ws", false},
		{"ws://triage-events.triage.svc:8086/events/ws", false},
		{"ws://triage-events.triage:8086/events/ws", true},
		{"ws://triage-events:8086/events/ws", true},
		{"ws://example.com/events/ws", true},
		{"ws://localhost:8086/events/ws", false},
		{"ws://triage.example.com/events/ws", true},
		{"ws://10.0.0.5:8086/events/ws", true},
		{"wss://triage.example.com/events/ws", false},
	} {
		t.Run(tc.endpoint, func(t *testing.T) {
			err := ValidateEndpoint(tc.endpoint, false)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateEndpoint(%q, false) = %v, wantErr %v", tc.endpoint, err, tc.wantErr)
			}
			// The explicit flag still allows anything.
			if err := ValidateEndpoint(tc.endpoint, true); err != nil {
				t.Fatalf("ValidateEndpoint(%q, true) = %v, want nil", tc.endpoint, err)
			}
		})
	}
}
