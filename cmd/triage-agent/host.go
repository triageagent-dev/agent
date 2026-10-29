// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sync/errgroup"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/triageagent-dev/agent/agentui"
	"github.com/triageagent-dev/agent/hostunits"

	"github.com/triageagent-dev/agent/logexporter"
	"github.com/triageagent-dev/agent/logmetrics"
	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/nodeexporter"
	"github.com/triageagent-dev/agent/push"
	"github.com/triageagent-dev/agent/redsource"
	"github.com/triageagent-dev/agent/sensorstream"
	"github.com/triageagent-dev/agent/usemetrics"
)

// Host mode (docs/adr/0018-linux-host-mode.md): the agent runs as one process
// on a plain Linux host, with no Kubernetes API, no controller-runtime manager,
// no sensor stream listener and no mesh. A host is a cluster of one with one
// synthetic namespace. The leader-only loops run unconditionally: one agent
// per host, no HA.

// modeHost is the --mode value that selects host mode.
const modeHost = "host"

// defaultHostNamespace is the synthetic namespace of a host.
const defaultHostNamespace = "host"

// defaultHostJoinStateDir is where a host keeps its join identity unless
// --join-state-dir says otherwise; the systemd unit grants write access to it.
const defaultHostJoinStateDir = "/var/lib/triage-agent/join"

// hostJoinStateDir returns the join state directory a host uses: the flag
// value when --join-state-dir was given, else defaultHostJoinStateDir. The
// flag's own default (/data/join) is the cluster pod's volume and means
// nothing on a host.
func hostJoinStateDir(flagValue string, explicit bool) string {
	if explicit && flagValue != "" {
		return flagValue
	}
	return defaultHostJoinStateDir
}

// hostModeSelected reports whether --mode names host mode. Host mode is its
// own entry point, so it cannot be combined with any other --mode value.
func hostModeSelected(m modeList) (bool, error) {
	if !slices.Contains(m, modeHost) {
		return false, nil
	}
	if len(m) > 1 {
		return false, fmt.Errorf("--mode=%s cannot be combined with other --mode values (got %s)", modeHost, strings.Join(m, ","))
	}
	return true, nil
}

// hostOptions carries the flag values host mode reads.
type hostOptions struct {
	endpoint               string
	standalone             bool // no core: --ui-listen or --api-listen set and --triage-endpoint empty
	allowPlaintext         bool
	insecure               bool
	cluster                string // defaults to the OS host name
	tenant                 string
	joinCAFingerprint      string
	joinStateDir           string
	joinSecret             string
	joinTenants            []string
	heartbeat              time.Duration
	namespace              string   // the synthetic namespace
	units                  []string // --host-units globs
	cgroupRoot             string   // used from phase 3
	logFiles               []string // --host-log-files <unit>=<glob> specs
	tick                   time.Duration
	logMetrics, useMetrics bool
	redMetrics, topology   bool
	logOptions             func() logmetrics.Options
	useOptions             func() usemetrics.Options
	redOptions             func() redsource.Options
	hostname               func() (string, error) // os.Hostname; injectable in tests
	statusEvery            time.Duration          // sensor_state period; sensorStatusInterval by default
	// unitsBus opens the systemd bus the units source reads; nil is the
	// system bus. A test passes a bus of its own, so it never opens the
	// machine's.
	unitsBus       func(context.Context) (hostunits.Bus, error)
	uiAdminKeyFile string
	uiMaxEvents    int
	api            agentui.APIOptions // the monitoring API (ADR-0020); Addr empty is off
	uiAdminKey     string             // --ui-admin-key or TRIAGE_AGENT_UI_ADMIN_KEY; host mode only
	uiListen       string             // --ui-listen: the local TUI's loopback address; empty is off
}

// hostClusterName returns the cluster name a host reports under: the flag
// value when set, else the short OS host name (the part before the first
// dot: a cloud VM's host name is often a fully qualified one that carries
// the project or zone).
func hostClusterName(flagValue string, hostname func() (string, error)) (string, error) {
	if v := strings.TrimSpace(flagValue); v != "" {
		return v, nil
	}
	if hostname == nil {
		hostname = os.Hostname
	}
	h, err := hostname()
	if err != nil {
		return "", fmt.Errorf("read the host name (set --cluster-name): %w", err)
	}
	h, _, _ = strings.Cut(strings.TrimSpace(h), ".")
	if h == "" {
		return "", errors.New("the host name is empty (set --cluster-name)")
	}
	return h, nil
}

// validate checks the flags host mode needs and fills the defaults.
func (o *hostOptions) validate() error {
	if o.endpoint == "" && !o.standalone {
		return errors.New("--triage-endpoint is required in host mode (or set --ui-listen or --api-listen to run standalone)")
	}
	if o.endpoint != "" {
		if err := push.ValidateEndpoint(o.endpoint, o.allowPlaintext); err != nil {
			return err
		}
	}
	if err := checkPushCredentials(o.endpoint, o.joinCAFingerprint); err != nil {
		return err
	}
	if o.tenant == "" && o.standalone {
		o.tenant = "local"
	}
	if o.tenant == "" {
		return errors.New("--target-tenant is required in host mode: a host has no namespace annotation to derive a tenant from")
	}
	if o.joinSecret != "" {
		return errors.New("--join-secret needs Kubernetes; a host keeps its join identity in --join-state-dir")
	}
	if o.namespace == "" {
		o.namespace = defaultHostNamespace
	}
	if strings.ContainsAny(o.namespace, " \t,/") {
		return fmt.Errorf("--host-namespace %q must be a single name without spaces, commas or slashes", o.namespace)
	}
	for _, g := range o.units {
		if _, err := path.Match(g, ""); err != nil {
			return fmt.Errorf("--host-units glob %q: %w", g, err)
		}
	}
	if _, err := logexporter.ParseHostLogFiles(o.logFiles); err != nil {
		return err
	}
	cluster, err := hostClusterName(o.cluster, o.hostname)
	if err != nil {
		return err
	}
	o.cluster = cluster
	if o.tick <= 0 {
		o.tick = 15 * time.Second
	}
	if o.statusEvery <= 0 {
		o.statusEvery = sensorStatusInterval
	}
	return nil
}

// hostRuntime is everything host mode runs, built but not started.
type hostRuntime struct {
	pusher *push.Pusher
	local  *sensorstream.Local
	// runnables are the long-running loops: the sensor roles, the push
	// transport, the sensor_state loop and the enabled collectors. Each is
	// started on its own goroutine and ends with the context.
	runnables []func(context.Context) error
}

// hostRoles returns the sensor roles the host runs in-process, by role name.
// This is the seam phases 3 to 5 plug into: USE from unit cgroups registers
// metricset.RoleNodeExporter, LOG from journald registers
// metricset.RoleLogExporter and RED from OBI registers metricset.RoleEBPF.
// units returns the unit names the systemd discoverer (phase 2) currently
// selects; every role reads it on each collect, so the three sensors and the
// virtual apps always describe the same set of units.
func hostRoles(o hostOptions, units func() []string) (map[string]sensorstream.RoleSource, []func(context.Context) error) {
	roles := map[string]sensorstream.RoleSource{}
	var loops []func(context.Context) error
	// USE from unit cgroups (phase 3).
	roles[metricset.RoleNodeExporter] = nodeexporter.NewHost(nodeexporter.HostOptions{
		NodeName: o.cluster, Namespace: o.namespace, CgroupRoot: o.cgroupRoot,
		Units: units,
	})
	// RED and topology from OBI (phase 5); a build without -tags obi registers nothing.
	if o.redMetrics || o.topology {
		loops = append(loops, hostRED(roles, o, units)...)
	}
	// LOG from journald (phase 4), only when the LOG collector is on, so
	// journalctl never runs for nothing.
	if !o.logMetrics {
		return roles, loops
	}
	if files, err := logexporter.ParseHostLogFiles(o.logFiles); err == nil {
		if hl, err := logexporter.NewHostLog(logexporter.HostLogOptions{
			Node: o.cluster, Namespace: o.namespace, LogFiles: files,
			Units: units,
		}, ctrl.Log); err == nil {
			roles[metricset.RoleLogExporter] = hl
			loops = append(loops, hl.Run)
		}
	}
	return roles, loops
}

// buildHost wires the host runtime from validated options.
func buildHost(o hostOptions, log logr.Logger) (*hostRuntime, error) {
	pusher := push.NewHost(push.Options{
		Endpoint:          o.endpoint,
		Standalone:        o.standalone,
		ClusterName:       o.cluster,
		Tenant:            o.tenant,
		JoinCAFingerprint: o.joinCAFingerprint,
		JoinStateDir:      o.joinStateDir,
		JoinTenants:       o.joinTenants,
		Namespaces:        []string{o.namespace},
		AgentVersion:      appVersion(),
		Heartbeat:         o.heartbeat,
		InsecureTLS:       o.insecure,
		LogMetrics:        o.logMetrics,
		RedMetrics:        o.redMetrics,
		UseMetrics:        o.useMetrics,
		Topology:          o.topology,
		SensorStream:      true,
	}, log)
	if pusher == nil {
		return nil, errors.New("push is disabled: --triage-endpoint is empty and not standalone")
	}
	// Phase 2: systemd units are the host's virtual apps. Pusher.Start runs
	// the source, and the sensor roles read the same selection from it.
	units := hostunits.New(hostunits.Options{Globs: o.units, Log: log.WithName("hostunits"), Connect: o.unitsBus})
	if err := pusher.SetAppSource(units); err != nil {
		return nil, err
	}

	roles, roleLoops := hostRoles(o, units.Names)
	local := sensorstream.NewLocal(sensorstream.LocalOptions{
		Node: o.cluster, Version: appVersion(), Roles: roles, Tick: o.tick,
	})
	rt := &hostRuntime{pusher: pusher, local: local}
	ui, err := newUI(o.uiListen, o.uiAdminKeyFile, o.uiAdminKey, o.uiMaxEvents, o.api, "host", o.cluster, pusher, nil, nil, log)
	if err != nil {
		return nil, err
	}
	for _, r := range ui {
		rt.runnables = append(rt.runnables, r.Start)
	}
	rt.runnables = append(rt.runnables, roleLoops...)
	rt.runnables = append(rt.runnables, local.Run, pusher.Start)
	rt.runnables = append(rt.runnables, func(ctx context.Context) error {
		hostSensorStatusLoop(ctx, local.Nodes, pusher.SendSensorState, o.statusEvery)
		return nil
	})

	namespaces := []string{o.namespace}
	if o.logMetrics {
		lo := o.logOptions()
		lo.Source, lo.Namespaces, lo.NamespaceAllowed = local, namespaces, pusher.NamespaceInScope
		if c := logmetrics.New(lo, pusher, log); c != nil {
			rt.runnables = append(rt.runnables, c.Start)
		}
	}
	if o.useMetrics {
		uo := o.useOptions()
		uo.Source, uo.Namespaces, uo.NamespaceAllowed = local, namespaces, pusher.NamespaceInScope
		// One unit is one pod is one service: every pod of the synthetic
		// namespace is named by itself. No pod is excluded (no annotations).
		if c := usemetrics.New(uo, usemetrics.StaticIdentityResolver{Namespace: o.namespace}, pusher, log); c != nil {
			rt.runnables = append(rt.runnables, c.Start)
		}
	}
	if o.redMetrics || o.topology {
		ro := o.redOptions()
		ro.Source, ro.Namespaces, ro.NamespaceAllowed = local, namespaces, pusher.NamespaceInScope
		// No cluster index: a client span's destination resolves by address
		// only, so a call to another host stays external (plan phase 5).
		if c := redsource.New(ro, pusher, log); c != nil {
			rt.runnables = append(rt.runnables, c.Start)
		}
	}
	return rt, nil
}

// hostSensorStatusLoop reports the in-process sensors' health to the core:
// once at start, then every period until ctx ends. A cluster controller waits
// a period first because its hub has no sensor yet; the host's own are there.
func hostSensorStatusLoop(ctx context.Context, nodes func() []sensorstream.NodeStatus,
	send func(at int64, sensors []push.SensorStatus, otlp []push.OTLPSourceStatus, intake []push.OTLPIntakeStatus), every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		send(time.Now().Unix(), sensorStatusesFromNodes(nodes()), nil, nil)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// runHost runs host mode until a signal and returns the process exit code.
func runHost(o hostOptions) int {
	if err := o.validate(); err != nil {
		setupLog.Error(err, "invalid host mode configuration")
		return 1
	}
	if o.joinCAFingerprint != "" {
		setupLog.Info("push authenticates by agent join certificate", "caFingerprint", o.joinCAFingerprint, "stateDir", o.joinStateDir)
	}
	rt, err := buildHost(o, ctrl.Log)
	if err != nil {
		setupLog.Error(err, "unable to start host mode")
		return 1
	}
	setupLog.Info("host mode enabled", "endpoint", o.endpoint, "cluster", o.cluster, "tenant", o.tenant,
		"namespaces", []string{o.namespace}, "units", o.units, "cgroupRoot", o.cgroupRoot)

	g, gctx := errgroup.WithContext(ctrl.SetupSignalHandler())
	for _, run := range rt.runnables {
		g.Go(func() error { return run(gctx) })
	}
	if err := g.Wait(); err != nil {
		setupLog.Error(err, "host mode exited")
		return 1
	}
	return 0
}
