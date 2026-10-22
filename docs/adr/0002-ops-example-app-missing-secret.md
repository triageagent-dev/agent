# ADR-0002: Ops secret-consumer Rollout Degraded — Missing Secret

- **Date:** 2026-04-20
- **Status:** Superseded. Applications come from virtual apps only; the agent has no Application custom resource.
- **Cluster:** ops / namespace: ops

## Context

`example-secret-consumer` Application showed `3/4 componentsReady` with
the Argo Rollout component in `InProgress` state.

The Rollout was `Degraded` with `ProgressDeadlineExceeded` since 2026-04-16.
The new ReplicaSet `67cbfc6969` had one pod stuck in `ContainerCreating` for
90+ minutes.

## Root Cause

Pod events showed:

```
MountVolume.SetUp failed for volume "api-key-example-service":
secret "service-api-key-example-service" not found
```

The Rollout spec was updated to mount a new secret
`service-api-key-example-service` but the secret was never created in
the `ops` namespace. A `VaultStaticSecret` existed in `dev` namespace but:

1. The Vault Secrets Operator was in `ImagePullBackOff` (broken), so no secrets
   were being synced from Vault.
2. The Vault path `example/ops/service-api-keys/example-service` did
   not exist — only the `dev` path was populated.

## Resolution

The secret data was sourced from the existing `example-service` Opaque secret in
`ops` namespace (synced 123 days ago when the operator was healthy), which
contained the same keys (`api-key.txt`,
`example-service-notification-api-key.txt`).

Created the secret manually:

```bash
kubectl create secret generic service-api-key-example-service -n ops \
  --from-literal=api-key.txt=<value from example-service> \
  --from-literal=example-service-notification-api-key.txt=<value from example-service>
```

Also created a `VaultStaticSecret` in `ops` pointing to the `dev` Vault path as
a placeholder until the ops Vault path is populated:

```yaml
apiVersion: secrets.hashicorp.com/v1beta1
kind: VaultStaticSecret
metadata:
  name: service-api-key-example-service
  namespace: ops
spec:
  mount: secret
  namespace: dev
  path: example/dev/service-api-keys/example-service
  destination:
    create: true
    name: service-api-key-example-service
  type: kv-v2
  refreshAfter: 300s
```

## Follow-up Actions

- [ ] Fix Vault Secrets Operator `ImagePullBackOff` in `vault` namespace
- [ ] Create Vault secret at `example/ops/service-api-keys/example-service`
      and update the `VaultStaticSecret` to point to the ops path
- [ ] Investigate why the Rollout spec was updated without the secret being
      provisioned first
