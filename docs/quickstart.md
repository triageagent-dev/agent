# Quick Start

## Deploy to cluster (Helm)

The chart is `charts/triage-agent` in this repo.

```bash
git clone https://github.com/triageagent-dev/agent.git
helm install triage-agent agent/charts/triage-agent -n triage --create-namespace
```

Verify:
```bash
kubectl get pods -n triage
```

## Core connection (remote agent)

If this cluster's API isn't reachable from wherever you monitor it (behind a
firewall/NAT), the controller can dial **out** to a triage agent over a
WebSocket and stream virtual-app status + Kubernetes Warning events instead
([ADR-0005](adr/0005-outbound-push-mode.md)). It's opt-in — off unless
`core.host` is set.

An agent authenticates to the core only by a join certificate, pinned to the
core CA by `core.caFingerprint` (from `triagectl agent list` on the core). It
is required when `core.host` is set; the binary refuses to start without it
whenever a core endpoint is set, Linux host mode included. There are two ways
to get the certificate:

- A one-time agent key (core ADR-090). The core mints the key and returns the
  whole install command, with `core.clusterName`, `core.tenants` and
  `core.agentKey=tak_...`. The agent spends the key on its first connect, gets
  its certificate at once and removes the key. Set no `target.*` value: the
  agent runs in cluster mode and each namespace maps to a tenant by its
  `triageagent.dev/tenant` annotation.
- An admin approval. After install the agent logs `join pending` with its key
  fingerprint; approve it with
  `triagectl agent approve <cluster> --fingerprint <agent fingerprint>`.

Upgrading an install that used `push.token` or `push.existingSecret`: set
`core.caFingerprint`, upgrade, then approve the join as above.

```bash
helm upgrade --install triage-agent agent/charts/triage-agent -n triage \
  --set core.host=<core-host> \
  --set core.clusterName=<cluster> \
  --set target.tenant=<tenant> \
  --set 'target.namespaces={<ns1>,<ns2>}' \
  --set core.caFingerprint=<SHA256:... from `triagectl agent list`>
```

`core.clusterName` is required once `core.host` is set. `target.tenant` is
optional: set it to run as a single primary tenant (the behavior above), or
leave it empty to run **cluster-mode** — the agent is identified only by
`core.clusterName`, and every frame's tenant is derived purely from
`triageagent.dev/tenant` namespace annotations instead (see `docs/todo/virtual-apps.md`
"Cluster-mode push"). A key install runs in cluster-mode.
`target.namespaces` scopes which namespaces get pushed. Leaving it empty
does not push every namespace: it falls back to the `triageagent.dev/tenant`-annotated
set, in both cluster-mode and single-tenant deployments. Set
`target.namespaces` to name namespaces up front instead of, or alongside,
annotating them; an explicit list unions with the annotated set. With
neither set, the agent observes and pushes nothing. Full option list in the
[chart README](../charts/triage-agent/README.md#core-connection)
and [values.yaml](../charts/triage-agent/values.yaml).

# Dev Quick Start

## Clone

```bash
git clone https://github.com/triageagent-dev/agent.git
cd agent
```

## Run tests (no cluster required)

```bash
make test        # unit tests
```

## Build binary

```bash
make bin/triage-agent
./bin/triage-agent --help
```

## Build and push image

```bash
export KO_DOCKER_REPO=ghcr.io/<your-org>/triage-agent
make ko-push
```

## Deploy to cluster

```bash
make deploy      # helm upgrade --install
make undeploy
```
