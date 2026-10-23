//go:build obi && linux

// The "ebpf" role of --mode's multi-role form (docs/todo/obi-embed.md): the
// embedded OTel eBPF instrumenter, in-process. Only builds under `-tags obi`
// with the go.mod replace pointing at a codegen'd OBI checkout
// (hack/build-obi.sh); the published module ships no bpf2go output.
// ebpf_stub.go is the untagged/non-linux fallback.
//
// Config comes from OBI_PROBE_NAMESPACES when no config file
// is mounted; the YAML essentials builder is obiEssentialsYAML, ebpf_config.go.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"go.opentelemetry.io/obi/pkg/appolly/services"
	"go.opentelemetry.io/obi/pkg/instrumenter"
	"go.opentelemetry.io/obi/pkg/obi"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
	"github.com/triageagent-dev/agent/push"
	"github.com/triageagent-dev/agent/sensorstream"
)

// ebpfSource is the ebpf role's metric source: what the embedded instrumenter
// exports.
func ebpfSource() sensorstream.RoleSource { return obiMetrics }

func runEBPF(ctx context.Context) error {
	// Config-FILE first (OBI_CONFIG_PATH, chart-mounted ConfigMap, parsed by
	// OBI's own obi.LoadConfig): discovery/exclude/export changes are a helm
	// upgrade, never a rebuild. The env-built essentials below are only the
	// chartless/dev fallback.
	var configSrc *strings.Reader
	if path := os.Getenv("OBI_CONFIG_PATH"); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("reading OBI config file %s: %w", path, err)
		}
		setupLog.Info("embedded eBPF instrumenter using config file", "path", path)
		configSrc = strings.NewReader(string(raw))
	} else {
		namespaces := push.ParseNamespaces(os.Getenv("OBI_PROBE_NAMESPACES"))
		if len(namespaces) == 0 {
			namespaces = []string{"*"}
		}
		excludes := push.ParseNamespaces(os.Getenv("OBI_PROBE_EXCLUDE"))
		// OBI_NETWORK_FLOWS gates the L4 network-flow metric family
		// (docs/todo/service-graph-from-obi.md) — chart value
		// ebpf.networkFlows.enabled sets this env var, and (because
		// socket_filter captures from the host network namespace) also
		// hostNetwork: true on this DaemonSet.
		networkFlows := os.Getenv("OBI_NETWORK_FLOWS") == "true"
		setupLog.Info("embedded eBPF instrumenter using env-built config (no OBI_CONFIG_PATH)",
			"namespaces", namespaces, "networkFlows", networkFlows)
		configSrc = strings.NewReader(obiEssentialsYAML(namespaces, excludes, networkFlows))
	}

	cfg, err := obi.LoadConfig(configSrc)
	if err != nil {
		return fmt.Errorf("loading OBI config: %w", err)
	}
	if err := applyOBILogLevel(cfg); err != nil {
		return err
	}
	// Failed uprobe attaches reach sensor_state through OBI's internal
	// metrics (ebpf_attach.go), not only its debug log.
	cfg.InternalMetrics.Reporter = obiAttach

	// Metrics go to the sensor stream as OTLP through OBI's in-process
	// consumer (ebpf_metrics.go); OBI opens no Prometheus listener.
	if err := obiMetrics.exportTo(cfg); err != nil {
		return fmt.Errorf("OBI metrics consumer: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid OBI config: %w", err)
	}
	if err := obi.CheckOSSupport(); err != nil {
		return fmt.Errorf("OS not supported for eBPF instrumentation: %w", err)
	}
	if err := obi.CheckOSCapabilities(cfg); err != nil {
		// Mirrors OBI's own cmd/obi/main.go: warn and continue, EnforceSysCaps
		// is off in the essentials YAML.
		setupLog.Info("WARNING: required system capabilities not present; instrumentation may malfunction", "error", err.Error())
	}

	return superviseInstrumenter(ctx, cfg)
}

// superviseInstrumenter runs the OBI instrumenter and restarts it in place
// when the set of annotationkeys.Tenant-annotated namespaces changes. OBI
// reads its discovery criteria once at startup and cannot select on a
// namespace's annotation, so the set is re-derived here and the instrumenter
// is restarted; the metrics store keeps serving the sensor stream across
// the restart.
//
// A reload detaches and re-attaches every uprobe on the node, which is why
// changes are debounced.
func superviseInstrumenter(ctx context.Context, cfg *obi.Config) error {
	// A pinned scope wins and never watches. The bare wildcard is the chart's
	// "no opinion" default; any other scope was set deliberately.
	if !isWildcardInstrumentScope(cfg.Discovery.Instrument) {
		setupLog.Info("ebpf: instrumentation scope is pinned in the config file; not following annotations", "key", annotationkeys.Tenant)
		return instrumenter.Run(ctx, cfg)
	}

	watch, err := sharedNamespaceTenantWatch(ctx)
	if err != nil {
		// Fail closed, like logExporterNamespaceGate: if we cannot tell which
		// namespaces are onboarded, instrument none. The fix is an RBAC grant.
		setupLog.Error(err, "ebpf: cannot determine annotated namespaces — instrumenting NOTHING until this is fixed; grant get/list/watch on namespaces", "key", annotationkeys.Tenant)
		<-ctx.Done()
		return nil
	}

	var current []string
	for {
		if next, ok := watch.names(); ok {
			applyInstrumentNamespaces(cfg, next)
			current = next
		}
		// Empty scope: OBI's Enabled(FeatureAppO11y) is false with an empty
		// discovery.instrument, so Run would return immediately and look like
		// the instrumenter exiting. Hold idle until something is annotated.
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		if len(cfg.Discovery.Instrument) == 0 {
			// No instrumenter is going to run this cycle: drop the previous
			// cycle's now-frozen series rather than keep sending them.
			obiMetrics.reset()
			go func() { <-runCtx.Done(); done <- nil }()
		} else {
			go func() { done <- instrumenter.Run(runCtx, cfg) }()
		}

		reload := false
		for !reload {
			select {
			case <-ctx.Done():
				cancel()
				<-done
				return nil
			case err := <-done:
				cancel()
				return err
			case <-watch.Changed():
				// Debounce: a GitOps pass touches namespaces one at a time and
				// each re-attach is expensive.
				if !settled(ctx, watch, instrumentReloadDebounce) {
					cancel()
					<-done
					return nil
				}
				latest, ok := watch.names()
				if !ok || sameStrings(current, latest) {
					continue
				}
				setupLog.Info("ebpf: annotated namespace set changed, reloading instrumentation",
					"from", current, "to", latest)
				reload = true
			}
		}

		cancel()
		if err := <-done; classifyReloadStop(err) {
			// A reload-triggered stop that did not come back clean, such as
			// OBI's own finisher-timeout error, is not fatal: the supervisor
			// cancelled this run on purpose, and returning here would exit
			// the process and crash-loop the pod for a condition the next
			// run recovers from.
			setupLog.Error(err, "ebpf: instrumenter did not stop cleanly on reload, starting the next run")
		}
	}
}

// isWildcardInstrumentScope reports whether the scope is exactly the chart's
// default: one entry matching every namespace, with nothing else narrowing it.
// Anything more specific is an intent this code must not overwrite.
func isWildcardInstrumentScope(crit services.GlobDefinitionCriteria) bool {
	if len(crit) != 1 {
		return false
	}
	a := crit[0]
	// Name is deprecated as a way to SET a service name; this reads it to detect
	// a name the operator set, which this function exists to avoid overwriting.
	//nolint:staticcheck // SA1019: read-only intent check, never a way to set it
	if a.Name != "" || a.Namespace != "" || len(a.PIDs) > 0 ||
		a.OpenPorts.Len() > 0 || a.Languages.IsSet() ||
		a.Path.IsSet() || a.CmdArgs.IsSet() ||
		len(a.PodLabels) > 0 || len(a.PodAnnotations) > 0 ||
		len(a.Metadata) != 1 {
		return false
	}
	ns, ok := a.Metadata["k8s_namespace"]
	if !ok || ns == nil {
		return false
	}
	// MarshalYAML returns the source pattern; GlobAttr keeps it unexported.
	// Comparing the pattern is exact, where probing MatchString is not.
	pattern, err := ns.MarshalYAML()
	if err != nil {
		return false
	}
	str, _ := pattern.(string)
	return str == "*"
}

// applyInstrumentNamespaces replaces OBI's discovery.instrument with one entry
// per annotated namespace. An empty set instruments nothing.
func applyInstrumentNamespaces(cfg *obi.Config, namespaces []string) {
	if len(namespaces) == 0 {
		// Nothing annotated: instrument nothing. Uprobes held for a namespace
		// nobody onboarded produce metrics with no tenant to file them under.
		setupLog.Info("ebpf: no namespace carries the annotation; instrumenting nothing", "key", annotationkeys.Tenant)
		cfg.Discovery.Instrument = services.GlobDefinitionCriteria{}
		return
	}
	crit := make(services.GlobDefinitionCriteria, 0, len(namespaces))
	for _, ns := range namespaces {
		crit = append(crit, services.GlobAttributes{
			Metadata: services.MetadataGlobMap{
				// Matched literally: a namespace NAME, never a pattern. Glob
				// metacharacters cannot appear in an RFC 1123 label, so there
				// is nothing here to escape.
				"k8s_namespace": globAttrPtr(services.NewGlob(ns)),
			},
		})
	}
	cfg.Discovery.Instrument = crit
	setupLog.Info("ebpf: instrumenting the annotated namespaces", "key", annotationkeys.Tenant, "namespaces", namespaces)
}

func globAttrPtr(g services.GlobAttr) *services.GlobAttr { return &g }

// wireOBIMetricsEmbedding, setOBIMetricsHandler and serveEmbeddedOBIMetrics
// moved to ebpf_metrics.go — untagged, like ebpf_config.go and
// ebpf_reload.go, so the metrics-serving wiring is testable on the darwin
// dev host without a codegen'd OBI checkout.

// obiEssentialsYAML moved to ebpf_config.go — untagged, like ebpf_reload.go,
// so its `obi.LoadConfig` round-trip is testable on the darwin dev host
// without a codegen'd OBI checkout (obi.LoadConfig/obi.Config have no eBPF
// dependency and build on every OS the vendored module supports; only
// pkg/instrumenter, used elsewhere in this file, is linux-only).
