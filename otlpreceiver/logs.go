// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"context"
	"strings"
	"time"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/triageagent-dev/agent/internal/ledger"
	"github.com/triageagent-dev/agent/metricset"
)

// A Collector needs the count connector to turn log records into the
// log.errors, log.warns and log.lines counters the log collector reads. This
// receiver also serves the OTLP logs service and counts the records itself
// (docs/adr/0022-otlp-sensor-role.md), per resource, with the same names and
// the same thresholds as the documented connector configuration: ERROR and
// above, WARN up to ERROR, and every record. Counts only; the error sample
// needs the exemplar the log exporter adds.

// logCounts is a resource's cumulative record counts: four numbers, the
// only state a log source costs.
type logCounts struct {
	start                uint64
	errors, warns, lines uint64
	seen                 time.Time
}

// logService serves the OTLP logs service on the receiver.
type logService struct {
	collectorlogs.UnimplementedLogsServiceServer
	r *Receiver
}

// Export implements collectorlogs.LogsServiceServer.
func (s logService) Export(ctx context.Context, req *collectorlogs.ExportLogsServiceRequest) (*collectorlogs.ExportLogsServiceResponse, error) {
	p, err := s.r.authenticate(ctx)
	if err != nil {
		s.r.log.Info("otlp export refused", "reason", err.Error())
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	rejected, reasons := s.r.acceptLogs(p, req.GetResourceLogs())
	resp := &collectorlogs.ExportLogsServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &collectorlogs.ExportLogsPartialSuccess{
			RejectedLogRecords: rejected,
			ErrorMessage:       strings.Join(reasons, "; "),
		}
	}
	return resp, nil
}

func countRecords(rl *logspb.ResourceLogs) int {
	n := 0
	for _, sl := range rl.GetScopeLogs() {
		n += len(sl.GetLogRecords())
	}
	return n
}

// severityClass is how a record counts: 2 for an error, 1 for a warning, 0
// for the rest. A record with no severity number is judged by its text, as a
// logging library that sets only the text would send it.
func severityClass(rec *logspb.LogRecord) int {
	n := rec.GetSeverityNumber()
	if n == logspb.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED {
		switch strings.ToLower(strings.TrimSpace(rec.GetSeverityText())) {
		case "error", "err", "fatal", "critical", "crit", "panic", "emergency", "alert":
			return 2
		case "warn", "warning":
			return 1
		}
		return 0
	}
	switch {
	case n >= logspb.SeverityNumber_SEVERITY_NUMBER_ERROR:
		return 2
	case n >= logspb.SeverityNumber_SEVERITY_NUMBER_WARN:
		return 1
	}
	return 0
}

// acceptLogs adds the records of rls, sent by p, to the counters of the
// resources that sent them, forwards the counters that changed and returns
// how many records it refused and why.
func (r *Receiver) acceptLogs(p Principal, rls []*logspb.ResourceLogs) (int64, []string) {
	sender := p.String()
	now := r.opts.now()
	replica := r.replicaName()
	f := r.newRefusal(sender, "log records")
	reject := f.reject
	var outs []forward

	r.mu.Lock()
	r.exports++
	var offered int64
	for _, rl := range rls {
		nr := int64(countRecords(rl))
		r.points += nr
		offered += nr
		n := func() int { return countRecords(rl) }
		f.at(rl.GetResource())
		resource, ok := r.admit(p, &metricspb.ResourceMetrics{Resource: rl.GetResource()}, n, reject)
		if !ok {
			continue
		}
		if attr(resource.GetAttributes(), nsAttr) == "" {
			reject(n(), noNamespaceReason)
			continue
		}
		resource.Attributes = append(resource.Attributes, kv(replicaAttr, replica))
		key := resourceKey(resource)
		if !r.touchSource(key, sender, now, n, reject) {
			continue
		}
		c := r.logs[key]
		if c == nil {
			if r.nheld >= r.opts.MaxSeries {
				reject(n(), "series limit reached")
				continue
			}
			c = &logCounts{start: uint64(now.UnixNano())}
			r.logs[key] = c
			r.nheld++
		}
		c.seen = now
		before := *c
		for _, sl := range rl.GetScopeLogs() {
			for _, rec := range sl.GetLogRecords() {
				c.lines++
				switch severityClass(rec) {
				case 2:
					c.errors++
				case 1:
					c.warns++
				}
			}
		}
		if c.lines == before.lines {
			continue
		}
		outs = append(outs, forward{key: key, role: metricset.RoleLogExporter, rms: []*metricspb.ResourceMetrics{{
			Resource:     resource,
			ScopeMetrics: []*metricspb.ScopeMetrics{{Scope: &commonpb.InstrumentationScope{Name: "otlp"}, Metrics: logCounters(c, now)}},
		}}})
	}
	r.rejected += f.rejected
	ledger.Default.Add("otlp", ledger.All, ledger.Ticks, 1)
	ledger.Default.Add("otlp", ledger.All, ledger.Points, offered)
	ledger.Default.Add("otlp", ledger.All, ledger.Refused, f.rejected)
	r.sweep(now)
	r.mu.Unlock()

	r.send(outs)
	return f.rejected, f.sortedReasons()
}

// logCounters are the count connector's three cumulative counters for c.
func logCounters(c *logCounts, now time.Time) []*metricspb.Metric {
	counter := func(name string, v uint64) *metricspb.Metric {
		return &metricspb.Metric{Name: name, Unit: "{record}", Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			IsMonotonic:            true,
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			DataPoints: []*metricspb.NumberDataPoint{{
				StartTimeUnixNano: c.start, TimeUnixNano: uint64(now.UnixNano()),
				Value: &metricspb.NumberDataPoint_AsInt{AsInt: int64(v)},
			}},
		}}}
	}
	return []*metricspb.Metric{counter("log.errors", c.errors), counter("log.warns", c.warns), counter("log.lines", c.lines)}
}
