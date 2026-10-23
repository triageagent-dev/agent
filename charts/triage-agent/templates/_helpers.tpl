{{- define "triage-agent.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "triage-agent.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "triage-agent.labels" -}}
app.kubernetes.io/name: {{ include "triage-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
{{- end -}}

{{- define "triage-agent.selectorLabels" -}}
app.kubernetes.io/name: {{ include "triage-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "triage-agent.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "triage-agent.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Image ref. image.tag wins verbatim when set. Otherwise the default is the
ko-published tag, which is "v" + Chart.AppVersion (ko tags with a leading v;
see Makefile VER = v${app_major}.${app_minor}.${app_patch}). appVersion itself
stays the plain semver per Helm convention.
*/}}
{{- define "triage-agent.image" -}}
{{- $tag := .Values.image.tag | default (printf "v%s" .Chart.AppVersion) -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
Image of the eBPF DaemonSet, the only workload that runs the eBPF role: the
image built with OBI. image.ebpf.repository names it; empty means
"<image.repository>-obi". image.ebpf.tag defaults to image.tag, then to "v" +
appVersion. The controller and the OTLP role run image.repository, built
without OBI.
*/}}
{{- define "triage-agent.ebpfImage" -}}
{{- $repo := .Values.image.ebpf.repository | default (printf "%s-obi" .Values.image.repository) -}}
{{- $tag := .Values.image.ebpf.tag | default .Values.image.tag | default (printf "v%s" .Chart.AppVersion) -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end -}}

{{/* "true" when the sensor stream renders (docs/adr/0015-sensor-stream-grpc.md):
     the core connection is on and the DaemonSet runs at least one sensor role. Nil-safe for
     `helm upgrade --reuse-values`, which renders with the previous release's
     values only. */}}
{{- define "triage-agent.sensorStream" -}}
{{- $le := .Values.logExporter | default dict -}}
{{- $e := .Values.ebpf | default dict -}}
{{- if and (include "triage-agent.core" . | fromYaml).enabled (or $e.enabled (include "triage-agent.nodeExporter" .) $le.enabled (include "triage-agent.otlp" .)) -}}true{{- end -}}
{{- end -}}

{{/* "true" when the DaemonSet runs the node-exporter role. It is a sensor
     on every node, so it is opt-in like ebpf.enabled: useMetrics.nodeExporter
     must be true, and USE must be on. */}}
{{- define "triage-agent.nodeExporter" -}}
{{- $use := .Values.useMetrics | default dict -}}
{{- if and $use.enabled $use.nodeExporter -}}true{{- end -}}
{{- end -}}

{{/* Which collectors the controller runs, as YAML booleans red, use, log and
     topology. A collector runs when it is enabled (the default), the core
     connection is on, and one of its sources is: OTLP for all four, or the
     ebpf role (RED, topology), the node-exporter role (USE) or the
     log-exporter role (LOG). */}}
{{- define "triage-agent.collectors" -}}
{{- $core := (include "triage-agent.core" . | fromYaml).enabled -}}
{{- $otlp := include "triage-agent.otlp" . -}}
{{- $e := .Values.ebpf | default dict -}}
{{- $le := .Values.logExporter | default dict -}}
{{- $red := .Values.redMetrics | default dict -}}
{{- $use := .Values.useMetrics | default dict -}}
{{- $log := .Values.logMetrics | default dict -}}
{{- $top := .Values.topology | default dict -}}
red: {{ and $core $red.enabled (or $e.enabled $otlp) | toString | eq "true" }}
use: {{ and $core $use.enabled (or (include "triage-agent.nodeExporter" .) $otlp) | toString | eq "true" }}
log: {{ and $core $log.enabled (or $le.enabled $otlp) | toString | eq "true" }}
topology: {{ and $core $top.enabled (or $e.enabled $otlp) | toString | eq "true" }}
{{- end -}}

{{/* "true" unless kubernetes.watch is false. A nil .Values.kubernetes (a
     --reuse-values upgrade of an older release) keeps watching. */}}
{{- define "triage-agent.kubernetesWatch" -}}
{{- $k := .Values.kubernetes | default dict -}}
{{- if or (not (hasKey $k "watch")) $k.watch -}}true{{- end -}}
{{- end -}}

{{/* "true" when something authenticates to the controller with a
     ServiceAccount token, which the controller checks with a TokenReview: the
     eBPF DaemonSet roles, or OTLP senders named in otlp.allowedServiceAccounts.
     OTLP senders that use a client certificate need no TokenReview. */}}
{{- define "triage-agent.tokenReview" -}}
{{- $le := .Values.logExporter | default dict -}}
{{- $e := .Values.ebpf | default dict -}}
{{- $o := .Values.otlp | default dict -}}
{{- if and (include "triage-agent.sensorStream" .) (or $e.enabled (include "triage-agent.nodeExporter" .) $le.enabled $o.allowedServiceAccounts) -}}true{{- end -}}
{{- end -}}

{{/* "true" when the OTLP receiver runs (docs/adr/0017-otlp-receiver.md). It
     feeds the sensor stream hub, so it turns the sensor stream on even with
     no DaemonSet. */}}
{{- define "triage-agent.otlp" -}}
{{- $o := .Values.otlp | default dict -}}
{{- if and (include "triage-agent.core" . | fromYaml).enabled $o.enabled -}}true{{- end -}}
{{- end -}}

{{/* "true" when the OTLP receiver runs in its own Deployment, the otlp sensor
     role, instead of in the controller (docs/adr/0022-otlp-sensor-role.md). */}}
{{- define "triage-agent.otlpSensor" -}}
{{- $o := .Values.otlp | default dict -}}
{{- $s := $o.sensor | default dict -}}
{{- if and (include "triage-agent.otlp" .) $s.enabled -}}true{{- end -}}
{{- end -}}

{{/* The otlp sensor Deployment's selector: apart from the controller's, for
     the reason the eBPF DaemonSet's is. */}}
{{- define "triage-agent.otlpSensorSelectorLabels" -}}
app.kubernetes.io/name: otlp
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* The OTLP receiver's gRPC port. */}}
{{- define "triage-agent.otlpPort" -}}
{{- $o := .Values.otlp | default dict -}}
{{- $o.port | default 4317 -}}
{{- end -}}

{{/* The sensor stream's gRPC port. */}}
{{- define "triage-agent.sensorStreamPort" -}}
{{- $ss := .Values.sensorStream | default dict -}}
{{- $ss.port | default 9470 -}}
{{- end -}}

{{/* The monitoring API's port, from api.listen (":9480" or "0.0.0.0:9480"). */}}
{{- define "triage-agent.apiPort" -}}
{{- $api := .Values.api | default dict -}}
{{- trimPrefix ":" (regexFind ":[0-9]+$" ($api.listen | default "")) -}}
{{- end -}}

{{/* The replica gossip port (ADR-0015 phase 4). */}}
{{- define "triage-agent.meshPort" -}}
{{- $ss := .Values.sensorStream | default dict -}}
{{- $ss.meshPort | default 9471 -}}
{{- end -}}

{{/* "true" when the controller runs leader election: when asked, and always
     with more than one replica, since only the leader may push to the core. */}}
{{- define "triage-agent.leaderElection" -}}
{{- $le := .Values.leaderElection | default dict -}}
{{- if or $le.enabled (gt (int .Values.replicaCount) 1) -}}true{{- end -}}
{{- end -}}


{{/*
Selector labels for the eBPF DaemonSet (docs/adr/0009-red-metrics-beyla.md).

Deliberately a DIFFERENT app.kubernetes.io/name than the controller's (be91af3).
Sharing the controller's selectorLabels made the DaemonSet's pods match the
controller Deployment's own selector ({instance, name}) — overlapping
selectors, which Kubernetes warns against. The Deployment did not adopt them
(its ReplicaSet selector also carries pod-template-hash), but everything that
selects the controller by its Deployment selector, kubectl -l and triagectl's
pod panel, listed the DaemonSet's pods as controller pods too.
*/}}
{{- define "triage-agent.ebpfSelectorLabels" -}}
app.kubernetes.io/name: ebpf
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
The agent's --triage-endpoint URL.

The ROUTE is the chart's to declare, not the caller's: it is a property of the
triage core this chart is built against, so installers (triagectl agent install, a
customer HelmRelease) supply only the parts that vary per deployment — the host
— and inherit the rest. A caller that composed the whole URL had to restate the
path, and a restated path drifts silently: /v1/cluster-agent/ws shipped in the
FDE tooling and the runbook for months against a core that has only ever served
/events/ws, and every install had to be hand-corrected.

core.endpoint remains an explicit full-URL override for a deployment behind a
proxy that rewrites the path. Set it or core.host, not both.
*/}}
{{- define "triage-agent.triageEndpoint" -}}
{{- $core := include "triage-agent.core" . | fromYaml -}}
{{- if $core.endpoint -}}
{{- $core.endpoint -}}
{{- else -}}
{{- $host := $core.host -}}
{{- printf "%s://%s%s" ($core.scheme | default "wss") $host ($core.path | default "/events/ws") -}}
{{- end -}}
{{- end -}}

{{/*
The core connection settings (core.*), as YAML for fromYaml. push.* is the
old name of the same block: an install that still sets push.enabled=true is
read from it (push.clusterName also without it, for a standalone agent), value by value, wherever core.* leaves a value empty
(push.join.enrollKey becomes core.agentKey, the other push.join.* values move up
one level). enabled is derived: the agent talks to a core when core.host or
core.endpoint is set.
*/}}
{{- define "triage-agent.core" -}}
{{- $c := deepCopy (.Values.core | default dict) -}}
{{- $p := .Values.push | default dict -}}
{{- if and $p.clusterName (not $c.clusterName) -}}
{{- $_ := set $c "clusterName" $p.clusterName -}}
{{- end -}}
{{- if $p.enabled -}}
{{- $j := $p.join | default dict -}}
{{- $legacy := dict "host" $p.host "scheme" $p.scheme "path" $p.path "endpoint" $p.endpoint "clusterName" $p.clusterName "heartbeatSeconds" $p.heartbeatSeconds "insecureSkipVerify" $p.insecureSkipVerify "allowPlaintext" $p.allowPlaintext "caFingerprint" $j.caFingerprint "agentKey" $j.enrollKey "tenants" $j.tenants "storage" $j.storage "existingClaim" $j.existingClaim -}}
{{- range $k, $v := $legacy -}}
{{- if and $v (not (get $c $k)) -}}
{{- $_ := set $c $k $v -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $_ := set $c "enabled" (ternary true false (or (not (empty $c.host)) (not (empty $c.endpoint)))) -}}
{{- $_ := set $c "storage" ($c.storage | default "secret") -}}
{{- toYaml $c -}}
{{- end -}}
