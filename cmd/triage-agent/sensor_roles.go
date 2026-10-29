// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/triageagent-dev/agent/logexporter"
	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/nodeexporter"
	"github.com/triageagent-dev/agent/push"
	"github.com/triageagent-dev/agent/sensorstream"
	"github.com/triageagent-dev/agent/sensorstream/sensorpb"
)

// sensorRoleOptions carries the flag values the sensor roles read.
type sensorRoleOptions struct {
	cgroupRoot, procRoot, sysRoot string

	logIncludeNamespaces        namespaceList
	logErrorRegex, logWarnRegex string
	logSampleMaxLen             int
	logMaxSamples               int
	logSampleJSONFields         string

	otlp otlpSensorFlags

	// clientCert and clientKey are what the otlp role presents to the
	// controller instead of a token.
	clientCert, clientKey string

	controller, caFile, tokenFile string
	tick                          time.Duration
}

// runSensorRoles runs each role and the sensor stream client until a signal,
// and returns the process exit code.
func runSensorRoles(roles []string, o sensorRoleOptions) int {
	ctx := ctrl.SetupSignalHandler()
	g, gctx := errgroup.WithContext(ctx)
	sources := map[string]sensorstream.RoleSource{}
	node := os.Getenv("NODE_NAME")
	// The otlp role (ADR-0022) streams what its receiver holds; it has no
	// RoleSource and runs alone, since the controller accepts its account for
	// that role only.
	var feed sensorstream.SourceFeed
	var intake func() []*sensorpb.OTLPIntake
	if slices.Contains(roles, sensorstream.RoleOTLP) {
		if len(roles) != 1 {
			setupLog.Error(nil, "--mode=otlp runs alone: the otlp account may not run other roles")
			return 1
		}
		buf := sensorstream.NewSourceBuffer(otlpQueuePoints)
		recv, err := newOTLPSensorReceiver(o.otlp, buf.Put)
		if err != nil {
			setupLog.Error(err, "unable to start the otlp role")
			return 1
		}
		feed = buf
		intake = func() []*sensorpb.OTLPIntake { return intakeProto(recv.Intake()) }
		// Pods of one Deployment can share a node: the pod is the hub's key.
		if pod := os.Getenv("POD_NAME"); pod != "" {
			node = pod
		}
		setupLog.Info("starting role", "role", sensorstream.RoleOTLP, "listen", o.otlp.listen)
		g.Go(func() error { return recv.Start(gctx) })
		roles = nil
	}

	for _, role := range roles {
		switch role {
		case metricset.RoleNodeExporter:
			exp, err := nodeexporter.New(nodeexporter.Options{
				NodeName:   node,
				CgroupRoot: o.cgroupRoot,
				ProcRoot:   o.procRoot,
				SysRoot:    o.sysRoot,
			}, ctrl.GetConfigOrDie(), ctrl.Log)
			if err != nil {
				setupLog.Error(err, "unable to start node-exporter role")
				return 1
			}
			sources[role] = exp
			setupLog.Info("starting role", "role", role, "cgroupRoot", o.cgroupRoot, "procRoot", o.procRoot, "sysRoot", o.sysRoot)
			g.Go(func() error { return exp.Run(gctx) })
		case metricset.RoleLogExporter:
			exp, err := logexporter.New(logexporter.Options{
				NodeName:             node,
				IncludeNamespaces:    []string(o.logIncludeNamespaces),
				NamespaceAllowed:     logExporterNamespaceGate(o.logIncludeNamespaces),
				ErrorRegex:           o.logErrorRegex,
				WarnRegex:            o.logWarnRegex,
				SampleMaxLen:         o.logSampleMaxLen,
				MaxSamplesPerService: o.logMaxSamples,
				// push.ParseNamespaces is a generic "comma list -> trimmed,
				// blank-dropped []string" splitter, reused here for a JSON key
				// list.
				SampleJSONFields: push.ParseNamespaces(o.logSampleJSONFields),
			}, ctrl.GetConfigOrDie(), ctrl.Log)
			if err != nil {
				setupLog.Error(err, "unable to start log-exporter role")
				return 1
			}
			sources[role] = exp
			setupLog.Info("starting role", "role", role)
			g.Go(func() error { return exp.Run(gctx) })
		case metricset.RoleEBPF:
			sources[role] = ebpfSource()
			setupLog.Info("starting role", "role", "ebpf (embedded OTel eBPF instrumenter)")
			g.Go(func() error { return runEBPF(gctx) })
		default:
			setupLog.Error(nil, "unknown role in --mode list (valid: ebpf, node-exporter, log-exporter)", "role", role)
			return 1
		}
	}

	tokenFile := o.tokenFile
	if o.clientCert != "" {
		tokenFile = "" // the role authenticates with its certificate
	}
	if o.controller == "" {
		setupLog.Info("--sensor-controller is empty: sensor metrics are collected but not sent")
	} else {
		g.Go(func() error {
			return sensorstream.RunClient(gctx, sensorstream.ClientOptions{
				Addr:           o.controller,
				CAFile:         o.caFile,
				TokenFile:      tokenFile,
				ClientCertFile: o.clientCert,
				ClientKeyFile:  o.clientKey,
				Node:           node,
				Pod:            os.Getenv("POD_NAME"),
				Version:        appVersion(),
				Roles:          sources,
				Feed:           feed,
				Intake:         intake,
				Settings:       sensorSettings(),
				Tick:           o.tick,
				Log:            ctrl.Log,
			})
		})
	}

	if err := g.Wait(); err != nil {
		setupLog.Error(err, "sensor role exited")
		return 1
	}
	return 0
}
