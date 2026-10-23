//go:build obi

// Untagged, like ebpf_config.go itself — runs natively on the darwin dev
// host (see that file's doc comment for why obi.LoadConfig/obi.Config are
// safe to exercise without a codegen'd OBI checkout).
package main

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/obi/pkg/export/attributes"
	"go.opentelemetry.io/obi/pkg/netolly/flowdef"
	"go.opentelemetry.io/obi/pkg/obi"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
	"github.com/triageagent-dev/agent/redsource"
)

// TestObiEssentialsYAML_PodAnnotationExcludes: the env-built config keeps
// the same pod-annotation opt-outs the chart's ConfigMap always renders —
// triageagent.dev/instrument: "false" and triageagent.dev/triage: "false",
// as separate exclude_instrument entries (one map's keys must all match) —
// alongside any owner-name globs.
func TestObiEssentialsYAML_PodAnnotationExcludes(t *testing.T) {
	cfg, err := obi.LoadConfig(strings.NewReader(obiEssentialsYAML([]string{"*"}, []string{"*synthetic*"}, false)))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	found := map[string]bool{}
	owners := 0
	for _, ex := range cfg.Discovery.ExcludeInstrument {
		if len(ex.PodAnnotations) == 1 {
			for k, v := range ex.PodAnnotations {
				if v != nil && v.MatchString("false") && !v.MatchString("true") {
					found[k] = true
				}
			}
		}
		if _, ok := ex.Metadata["k8s_owner_name"]; ok {
			owners++
		}
	}
	for _, key := range []string{annotationkeys.Instrument, annotationkeys.Triage} {
		if !found[key] {
			t.Errorf("exclude_instrument has no k8s_pod_annotations entry for %s: \"false\"", key)
		}
	}
	if owners != 1 {
		t.Errorf("k8s_owner_name excludes = %d, want 1", owners)
	}
}

// TestObiEssentialsYAML_NetworkFlowsOff is the pre-existing behavior: no
// network feature, RED metrics (FeatureApplicationRED, DefaultConfig's own
// default) untouched.
func TestObiEssentialsYAML_NetworkFlowsOff(t *testing.T) {
	yaml := obiEssentialsYAML([]string{"*"}, nil, false)
	if strings.Contains(yaml, "network:") {
		t.Fatalf("networkFlows=false must not render a network: block\n%s", yaml)
	}
	cfg, err := obi.LoadConfig(strings.NewReader(yaml))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if err := newOBIMetricsStore().exportTo(cfg); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	features := cfg.JoinMetricsConfig().Features
	if !features.AppRED() {
		t.Error("FeatureApplicationRED must stay enabled (DefaultConfig's own default)")
	}
	if features.NetworkBytes() {
		t.Error("FeatureNetwork must NOT be enabled when networkFlows=false")
	}
	if cfg.NetworkFlows.GuessPorts != flowdef.PortGuessDisable {
		t.Errorf("network.guess_ports = %q, want default %q when networkFlows=false", cfg.NetworkFlows.GuessPorts, flowdef.PortGuessDisable)
	}
}

// TestObiEssentialsYAML_NetworkFlowsOn asserts the exact claim
// docs/todo/service-graph-from-obi.md makes: enabling network flows ORs
// FeatureNetwork onto the existing FeatureApplicationRED default (RED
// metrics keep working) rather than replacing it, network.source is
// socket_filter, and the k8s owner + direction attributes are selected for
// the flow metric, and redsource.ProfileOBI.FlowMetric is the vendored OBI's
// own OTel name for it (go.opentelemetry.io/obi/pkg/export/attributes/metric.go).
func TestObiEssentialsYAML_NetworkFlowsOn(t *testing.T) {
	yaml := obiEssentialsYAML([]string{"*"}, nil, true)
	cfg, err := obi.LoadConfig(strings.NewReader(yaml))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if err := newOBIMetricsStore().exportTo(cfg); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	features := cfg.JoinMetricsConfig().Features
	if !features.NetworkBytes() {
		t.Error("FeatureNetwork must be enabled when networkFlows=true")
	}
	if !features.AppRED() {
		t.Error("FeatureApplicationRED must SURVIVE enabling network flows (OR, not replace) — RED metrics must keep working")
	}

	if cfg.NetworkFlows.Source != "socket_filter" {
		t.Errorf("network.source = %q, want socket_filter", cfg.NetworkFlows.Source)
	}
	if cfg.NetworkFlows.GuessPorts != flowdef.PortGuessOrdinal {
		t.Errorf("network.guess_ports = %q, want %q — unknown-initiator flows (connections predating the DS pod) must resolve to a direction", cfg.NetworkFlows.GuessPorts, flowdef.PortGuessOrdinal)
	}

	if attributes.NetworkFlow.OTEL != redsource.ProfileOBI.FlowMetric {
		t.Fatalf("attributes.NetworkFlow.OTEL = %q, redsource.ProfileOBI.FlowMetric = %q: they must match", attributes.NetworkFlow.OTEL, redsource.ProfileOBI.FlowMetric)
	}

	sel, ok := cfg.Attributes.Select[attributes.NetworkFlow.Section]
	if !ok {
		t.Fatalf("attributes.select has no entry for %q — the flow metric's k8s owner/direction attributes are not selected", attributes.NetworkFlow.Section)
	}
	for _, attr := range []string{
		"k8s.src.owner.name", "k8s.src.namespace", "k8s.src.owner.type",
		"k8s.dst.owner.name", "k8s.dst.namespace", "k8s.dst.owner.type",
		"direction",
	} {
		found := false
		for _, inc := range sel.Include {
			if inc == attr {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("attributes.select[%q].include is missing %q", attributes.NetworkFlow.Section, attr)
		}
	}
}

// TestProfileOBI_NamesMatchVendoredOBI pins every metric name the RED and
// topology profile reads to the vendored OBI's own OTel metric names, so an
// OBI bump that renames one fails here rather than going silent.
func TestProfileOBI_NamesMatchVendoredOBI(t *testing.T) {
	p := redsource.ProfileOBI
	wantReq := map[string]string{
		"http.server.request.duration": attributes.HTTPServerDuration.OTEL,
		"rpc.server.call.duration":     attributes.RPCServerDuration.OTEL,
	}
	if len(p.RequestMetrics) != len(wantReq) {
		t.Fatalf("request metrics = %+v", p.RequestMetrics)
	}
	for _, rm := range p.RequestMetrics {
		if wantReq[rm.MetricName] != rm.MetricName || rm.DurationBucketMetric != rm.MetricName {
			t.Errorf("request/duration = %q/%q, not a vendored OBI server metric name", rm.MetricName, rm.DurationBucketMetric)
		}
		if rm.IsError == nil || rm.FormatSample == nil {
			t.Errorf("request metric %q has no IsError/FormatSample", rm.MetricName)
		}
	}
	want := map[string]string{
		"http": attributes.HTTPClientDuration.OTEL,
		"grpc": attributes.RPCClientDuration.OTEL,
		"db":   attributes.DBClientDuration.OTEL,
	}
	if len(p.ClientMetrics) != len(want) {
		t.Fatalf("client metrics = %+v", p.ClientMetrics)
	}
	for _, cm := range p.ClientMetrics {
		if want[cm.Protocol] != cm.MetricName {
			t.Errorf("%s client metric = %q, OBI = %q", cm.Protocol, cm.MetricName, want[cm.Protocol])
		}
	}
}

// TestApplyOBILogLevel: the config's log_level reaches OBI's logger (the
// default slog logger); OBI's own default, INFO, keeps debug off.
func TestApplyOBILogLevel(t *testing.T) {
	t.Cleanup(func() { slog.SetLogLoggerLevel(slog.LevelInfo) })
	for _, tc := range []struct {
		yaml  string
		debug bool
	}{
		{"log_level: debug\n", true},
		{"", false},
	} {
		cfg, err := obi.LoadConfig(strings.NewReader(tc.yaml))
		if err != nil {
			t.Fatalf("LoadConfig(%q): %v", tc.yaml, err)
		}
		if err := applyOBILogLevel(cfg); err != nil {
			t.Fatal(err)
		}
		if got := slog.Default().Enabled(context.Background(), slog.LevelDebug); got != tc.debug {
			t.Errorf("log_level %q: debug enabled = %v, want %v", tc.yaml, got, tc.debug)
		}
	}
}
