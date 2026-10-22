// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"context"
	"testing"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/triageagent-dev/agent/metricset"
)

func rec(n logspb.SeverityNumber, text string) *logspb.LogRecord {
	return &logspb.LogRecord{SeverityNumber: n, SeverityText: text, Body: nil}
}

func exportLogs(t *testing.T, r *Receiver, ctx context.Context, res *resourcepb.Resource, recs ...*logspb.LogRecord) *collectorlogs.ExportLogsServiceResponse {
	t.Helper()
	resp, err := logService{r: r}.Export(ctx, &collectorlogs.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: res, ScopeLogs: []*logspb.ScopeLogs{{LogRecords: recs}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func logRole(t *testing.T, r *Receiver, got *[]sunk) metricset.Set {
	t.Helper()
	r.flush()
	for _, s := range *got {
		if s.set.Get("log.lines") != nil {
			return s.set
		}
	}
	t.Fatalf("no log counters flushed: %+v", *got)
	return nil
}

func TestLogs_RefusesWithoutToken(t *testing.T) {
	r, _, _ := newTest(t)
	if _, err := (logService{r: r}).Export(context.Background(), &collectorlogs.ExportLogsServiceRequest{}); err == nil {
		t.Fatal("an export of log records without a token was accepted")
	}
}

func TestLogs_CountsByThreshold(t *testing.T) {
	r, got, _ := newTest(t)
	res := resource("service.name", "checkout", "k8s.namespace.name", "shop")
	exportLogs(t, r, authed(), res,
		rec(logspb.SeverityNumber_SEVERITY_NUMBER_INFO, "INFO"),
		rec(logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG, "DEBUG"),
		rec(logspb.SeverityNumber_SEVERITY_NUMBER_WARN, "WARN"),
		rec(logspb.SeverityNumber_SEVERITY_NUMBER_WARN4, "WARN"),
		rec(logspb.SeverityNumber_SEVERITY_NUMBER_ERROR, "ERROR"),
		rec(logspb.SeverityNumber_SEVERITY_NUMBER_FATAL, "FATAL"),
		rec(logspb.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED, ""),
	)
	set := logRole(t, r, got)
	for name, want := range map[string]float64{"log.lines": 7, "log.warns": 2, "log.errors": 2} {
		m := set.Get(name)
		if m == nil || len(m.Points) != 1 || m.Points[0].Value != want {
			t.Errorf("%s = %+v, want %v", name, m, want)
		}
	}
	if p := set.Get("log.errors").Points[0]; p.Attr("service.name") != "checkout" || p.Attr("k8s.namespace.name") != "shop" {
		t.Fatalf("resource attributes not on the counter: %v", p.Attrs)
	}
}

// A logging library that sets only the severity text still counts.
func TestLogs_SeverityTextWithoutANumber(t *testing.T) {
	r, got, _ := newTest(t)
	exportLogs(t, r, authed(), resource("service.name", "x", "k8s.namespace.name", "shop"),
		rec(0, "error"), rec(0, "Warning"), rec(0, "fatal"), rec(0, "info"), rec(0, "whatever"),
	)
	set := logRole(t, r, got)
	if e, w, l := set.Get("log.errors").Points[0].Value, set.Get("log.warns").Points[0].Value, set.Get("log.lines").Points[0].Value; e != 2 || w != 1 || l != 5 {
		t.Fatalf("errors=%v warns=%v lines=%v, want 2, 1, 5", e, w, l)
	}
}

func TestLogs_CountsAreCumulative(t *testing.T) {
	r, got, _ := newTest(t)
	res := resource("service.name", "x", "k8s.namespace.name", "shop")
	exportLogs(t, r, authed(), res, rec(logspb.SeverityNumber_SEVERITY_NUMBER_ERROR, "ERROR"))
	if v := logRole(t, r, got).Get("log.errors").Points[0].Value; v != 1 {
		t.Fatalf("first = %v", v)
	}
	*got = nil
	exportLogs(t, r, authed(), res, rec(logspb.SeverityNumber_SEVERITY_NUMBER_ERROR, "ERROR"), rec(logspb.SeverityNumber_SEVERITY_NUMBER_INFO, "INFO"))
	set := logRole(t, r, got)
	if e, l := set.Get("log.errors").Points[0].Value, set.Get("log.lines").Points[0].Value; e != 2 || l != 3 {
		t.Fatalf("errors=%v lines=%v, want 2 and 3", e, l)
	}
}

func TestLogs_ReplicaAttributeKeepsSharesApart(t *testing.T) {
	r, got, _ := newTest(t)
	r.opts.Replica = "pod-a"
	exportLogs(t, r, authed(), resource("service.name", "x", "k8s.namespace.name", "shop"), rec(logspb.SeverityNumber_SEVERITY_NUMBER_ERROR, "ERROR"))
	if p := logRole(t, r, got).Get("log.errors").Points[0]; p.Attr(replicaAttr) != "pod-a" {
		t.Fatalf("replica = %q, attrs %v", p.Attr(replicaAttr), p.Attrs)
	}
}

func TestLogs_SenderMayNotPublishForAnotherNamespace(t *testing.T) {
	r, _, _ := newTest(t)
	resp := exportLogs(t, r, asDemo(), resource("service.name", "x", "k8s.namespace.name", "other"), rec(logspb.SeverityNumber_SEVERITY_NUMBER_ERROR, "ERROR"))
	if resp.GetPartialSuccess().GetRejectedLogRecords() != 1 {
		t.Fatalf("a record claiming another namespace was accepted: %+v", resp.GetPartialSuccess())
	}
}

func TestLogs_RoutedToTheLogCollector(t *testing.T) {
	if RoleOf("log.errors") != metricset.RoleLogExporter || RoleOf("log.warns") != metricset.RoleLogExporter || RoleOf("log.lines") != metricset.RoleLogExporter {
		t.Fatal("the counters are not routed to the log-exporter role")
	}
}
