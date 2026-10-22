package sensorstream

import (
	"context"
	"testing"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/triageagent-dev/agent/sensorstream/sensorpb"
)

type statusSource struct{ msg string }

func (s statusSource) Collect(context.Context) ([]*metricspb.ResourceMetrics, error) { return nil, nil }
func (s statusSource) StatusError() string                                           { return s.msg }

// TestCollect_RoleStatusBecomesLastError: a source that reports a status
// error after a clean gather shows it as the role's LastError without
// counting a gather error.
func TestCollect_RoleStatusBecomesLastError(t *testing.T) {
	c := &client{
		opts:      ClientOptions{Roles: map[string]RoleSource{"ebpf": statusSource{msg: "uprobes failed"}}},
		roleStats: map[string]*sensorpb.RoleState{},
	}
	if _, err := c.collect(context.Background(), "ebpf"); err != nil {
		t.Fatal(err)
	}
	st := c.roleStats["ebpf"]
	if st.GetLastError() != "uprobes failed" || st.GetGatherErrors() != 0 {
		t.Errorf("LastError = %q, GatherErrors = %d", st.GetLastError(), st.GetGatherErrors())
	}
}
