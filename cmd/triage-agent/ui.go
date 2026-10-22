package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/triageagent-dev/agent/agentui"
	"github.com/triageagent-dev/agent/internal/mesh"
	"github.com/triageagent-dev/agent/push"
	"github.com/triageagent-dev/agent/sensorstream"
)

// newUI builds the local TUI over pusher and attaches its store as the
// pusher's observer (docs/adr/0019-local-tui.md). It returns nothing when
// addr is empty: the TUI is opt-in. With gossip (m non-nil) every replica
// serves the leader's view: the leader publishes it, the others apply it.
// api configures the integration listener (ADR-0020); its Addr empty leaves it off.
func newUI(addr, adminKeyFile, adminKey string, maxEvents int, api agentui.APIOptions, mode, cluster string, pusher *push.Pusher, m *mesh.Mesh, sensors func() []agentui.SensorInfo, log logr.Logger) ([]manager.Runnable, error) {
	if (addr == "" && api.Addr == "") || pusher == nil {
		return nil, nil
	}
	if maxEvents < agentui.MinMaxEvents || maxEvents > agentui.MaxMaxEvents {
		return nil, fmt.Errorf("--ui-max-events %d: must be %d to %d", maxEvents, agentui.MinMaxEvents, agentui.MaxMaxEvents)
	}
	store := agentui.NewStoreSized(maxEvents)
	var link agentui.Link = pusher
	var peers func() []string
	out := []manager.Runnable{}
	if m != nil {
		self, _ := os.Hostname()
		rep := agentui.NewReplica(pusher, store, self, m.PublishUI, log.WithName("ui"))
		m.SetApplyUI(rep.Apply)
		link = rep
		peers = m.Peers
		out = append(out, rep)
	}
	srv, err := agentui.New(agentui.Options{
		Addr: addr, Store: store, Link: link, Log: log.WithName("ui"),
		Mode: mode, Version: appVersion(), Cluster: cluster, Config: uiConfig, Sensors: sensors, Peers: peers, AdminKeyFile: adminKeyFile, AdminKey: adminKey,
	})
	if err != nil {
		return nil, err
	}
	pusher.SetObserver(store)
	if addr != "" {
		out = append(out, srv)
	}
	if api.Addr != "" {
		as, err := agentui.NewAPI(srv, api)
		if err != nil {
			return nil, err
		}
		out = append(out, as)
	}
	return out, nil
}

// hostUIAdminKey returns the host agent's admin key: --ui-admin-key, else the
// TRIAGE_AGENT_UI_ADMIN_KEY environment variable (the packaged unit's
// EnvironmentFile carries it).
func hostUIAdminKey(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	return os.Getenv("TRIAGE_AGENT_UI_ADMIN_KEY")
}

// sensorInfos maps the hub's nodes to what the TUI shows: the roles and the
// flags each sensor sent in its hello. Every replica's hub holds every node
// (the gossip merges them), so any replica can answer.
func sensorInfos(hub *sensorstream.Hub) func() []agentui.SensorInfo {
	if hub == nil {
		return nil
	}
	return func() []agentui.SensorInfo {
		var out []agentui.SensorInfo
		for _, n := range hub.Nodes() {
			si := agentui.SensorInfo{Node: n.Node, Pod: n.Pod, Version: n.Version, Roles: n.Roles, Connected: n.Connected}
			for _, st := range n.Settings {
				si.Settings = append(si.Settings, agentui.ConfigEntry{Name: st.GetName(), Value: st.GetValue(), Default: st.GetDefault(), Changed: st.GetChanged()})
			}
			out = append(out, si)
		}
		return out
	}
}

// defaultStandaloneCluster names a controller that runs without a core: the
// CLUSTER_NAME environment variable, else "standalone".
func defaultStandaloneCluster() string {
	if v := strings.TrimSpace(os.Getenv("CLUSTER_NAME")); v != "" {
		return v
	}
	return "standalone"
}
