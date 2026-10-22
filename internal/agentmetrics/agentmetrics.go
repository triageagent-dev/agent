// Package agentmetrics holds the agent's own Prometheus metrics. They are
// registered with the controller-runtime registry, so the controller serves
// them on --metrics-addr next to the controller-runtime and Go runtime
// metrics.
package agentmetrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Sensor label values for BaselineMutedKeys: the collector that owns the
// baseline.
const (
	SensorRED = "red"
	SensorUSE = "use"
	SensorLog = "log"
)

// BaselineMutedKeys is how many baseline keys each sensor currently mutes: a
// key (a service, or one operation of it) whose breaches recur is chronic, so
// it reports only every baseline.StillBreachingEvery windows instead of every
// window (docs/todo/baseline-gates.md). arm is the baseline arm, as in the
// wire's Breach field.
var BaselineMutedKeys = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "triage_agent_baseline_muted_keys",
	Help: "Baseline keys (a service, or one operation of it) currently muted as chronically breaching, per sensor and arm.",
}, []string{"sensor", "arm"})

func init() {
	metrics.Registry.MustRegister(BaselineMutedKeys)
}

// MutedKeys reads BaselineMutedKeys back from the registry for one sensor and
// arm; 0 when it was never set. The collectors' tests use it.
func MutedKeys(sensor, arm string) float64 {
	mfs, err := metrics.Registry.Gather()
	if err != nil {
		return 0
	}
	for _, mf := range mfs {
		if mf.GetName() != "triage_agent_baseline_muted_keys" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["sensor"] == sensor && labels["arm"] == arm {
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
}
