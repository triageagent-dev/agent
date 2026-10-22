package redsource

import (
	"testing"

	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

// TestProfileOBI_StatusLabelsAreSemconvKeys: the status labels are the exact
// semantic-convention keys OBI exports (third_party/obi/pkg/export/attributes/
// names/attrs.go). A misspelt key never matches, every request reads as a
// success, and the collector reports zero errors for every service.
func TestProfileOBI_StatusLabelsAreSemconvKeys(t *testing.T) {
	want := map[string]string{
		"http.server.request.duration": string(semconv.HTTPResponseStatusCodeKey),
		"rpc.server.call.duration":     string(semconv.RPCResponseStatusCodeKey),
	}
	for _, m := range ProfileOBI.RequestMetrics {
		if w, ok := want[m.MetricName]; ok && m.StatusLabel != w {
			t.Errorf("%s StatusLabel = %q, want %q", m.MetricName, m.StatusLabel, w)
		}
	}
}
