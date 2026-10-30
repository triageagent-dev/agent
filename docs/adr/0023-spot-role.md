# ADR-0023: Spot Role (the agent runs the workspace's spot on the customer's side)

- **Status:** Proposed 2026-10-30 (Den: "add spot capabilities to agent. so
  agent became spot with this mode enabled. so customer will have
  comprehensive agent (we call it spot) on their side that send telemetry to
  tria + has capabilities to call llm and talk to customer via chat"; "go").
  Nothing is built.
- **Date:** 2026-10-30
- **Deciders:** Denis (Principal SRE)
- **Related:** ADR-0015 (sensor stream), ADR-0022 (OTLP sensor role, the
  pattern of a role in its own Deployment). Core: ADR-098 (the spot runs its
  own agent loop), ADR-099 (the spot runs the tenant's model calls), ADR-105
  (spots join by certificate), ADR-106 (the core is the spot's scheduler),
  ADR-107 (spots on Agent Substrate), and the core ADR this one asks for (the
  spot of an agent cluster; see "What the core has to decide").

## Context

Two things run for a workspace today, in two places:

- **The agent**, on the customer's cluster. It joins the core by certificate,
  holds one outbound WebSocket to the core and sends telemetry over it:
  virtual apps, Kubernetes events, RED/USE/LOG metrics, topology, sensor
  state. It reads the customer's cluster through a read-only ClusterRole. It
  runs no model and talks to nobody.
- **The spot**, on our side, as a Deployment or an Agent Substrate actor. It
  is the conversational and investigative half of Tria for one workspace: an
  agent loop over a model (Anthropic or Gemini), the core's MCP tools, the
  messenger rooms where it talks with the customer, and the tenant's model
  calls the core hands it as jobs (ADR-099). It joins the core by certificate
  too (ADR-105 copied the agent's join) and is woken by the core (ADR-106).

The spot sees the customer's cluster only through what the core stored from
the agent's frames. When a person asks it in a room why a pod restarts, the
answer stops at the signals the agent chose to send. The thing that could look
at the pod, the agent, has no model; the thing that has a model cannot look.

The customer installs two things, approves two identities, and the model
credential that pays for the workspace's calls sits in our cluster although
ADR-099 says it should be the customer's choice of project, account, model and
spend.

The spot's runtime is already a Go library: `supervisor`, `llm`, `tools`,
`mcptools`, `prompt`, `ledger`, `schedule`, `telemetry`, `triggers`,
`identity`, `corecli`, `edges`, `config`, about 10,000 lines behind a 470-line
main. Its `triggers` package has two drivers: the WebSocket to the core (the
`deployment` runtime) and the HTTP link of a substrate actor (ADR-107). Its
only dependencies the agent lacks are the Anthropic SDK, `genai`, `mcp-go` and
the OpenTelemetry SDK, all pure Go.

## Decision

The spot becomes a role of the agent, `--mode=spot`, in the same image. A
customer who enables it has one install, one key, one component on their side
that sends telemetry, calls the model with their credential, looks at their
cluster with the agent's access and talks with them in the rooms. We call that
component the spot.

1. **A role in its own Deployment, not in the controller.** Like the OTLP
   sensor role (ADR-0022), `--mode=spot` runs as `<release>-spot`, one
   replica, chart `spot.enabled` (default `false`). The controller process
   keeps leader election, the replica mesh and the push; the spot keeps the
   model credential, the provider egress and the memory of a long turn. A
   restart of one never touches the other. The role refuses to run with any
   other mode in the same process.

2. **The spot stream to the core, not the agent hub.** The role joins and
   talks over the core's spot stream (`/v2/messenger/spot/ws`, ADR-105 join,
   ADR-106 frames), the WebSocket the `deployment` runtime already uses. It is
   a second outbound connection from the cluster, nothing new to open, and
   the core's wire does not change: challenge, join, hello, `wake`, `rooms`,
   `name`, `budget`, `control` in; `wake`, `turn`, `stats`, `schedule` reports
   out. The agent hub stays a telemetry stream. (Teaching the hub the spot
   frames would duplicate the spot stream for no gain.)

3. **One key, one approval per identity.** The role presents the cluster's
   agent key (the one in `<release>-join`) for its first join and asks for the
   spot identity `spot:<workspace>/<name>`; `name` defaults to the cluster
   name. The core shows it pending as any spot and a person approves it in the
   TUI. The core has to accept an agent key for a spot join bound to that
   key's workspace; that is item 1 of "What the core has to decide". Until it
   does, the chart also takes a spot enroll key (`spot.enrollKeySecret`) so the
   role works against a core that has not changed.

4. **The runtime moves into this repo.** The spot's runtime packages come in
   under `spot/` with their tests and keep their shape; `cmd/triage-agent` is
   the only main and wires `--mode=spot` to them. The role's flags are the
   spot's, prefixed `spot-` where they would collide with the agent's (`name`,
   `tenant`, `listen`, `log-level`, `runtime`). The spot repo's `tria-spot`
   main becomes a build of the same packages until step 4 of the rollout
   retires it. Our-side actors run this image with `--mode=spot
   --spot-runtime=substrate` (the HTTP link of ADR-107), so one image runs
   every spot.

5. **Local tools, read-only.** The role serves one more tool server to the
   loop, `cluster`, from the agent's own Kubernetes client: pods and their
   logs, events, workloads, services and endpoints, the fields `describe`
   shows, bounded to the workspace scope the core sends in the `limits` frame
   and to `tool-result-max-bytes` as every tool result is. Its ServiceAccount
   gets the controller's read ClusterRole and nothing else. No tool in this
   role writes to the cluster; an acting tool is a separate ADR with its own
   opt-in, if it ever comes.

6. **The customer's credential, never in the environment.** The model
   credential is a file from a Secret (`spot.llm.keySecret`, Anthropic or
   Gemini key) or workload identity (Vertex, Anthropic federation), as the
   spot's `--llm-auth` already offers. The chart renders no NetworkPolicy
   (the agent chart's rule); the role's egress is the core and the provider.

7. **The spot's own telemetry stays optional.** OTLP spans of the loop to ego
   (`spot.otel.*`) are off by default on a customer cluster and on when we
   run the image as our own actor.

8. **Resources of its own.** `spot.resources` with `GOMEMLIMIT` from the
   memory limit as the other roles do; a defaults block sized from the
   measured spots (about 100 MiB resident at a 100K-token context).

### What stays as it is

- ADR-099's rule: a workspace with a spot runs its model calls there; a
  workspace without one is covered by the core. A customer spot is a spot.
- The core's messenger, the MCP tool surface and the instruction cell. The
  instruction gains the `cluster` tools (core side).
- The controller, the sensor roles and the host mode. `agent-obi` is
  unchanged.

### What the core has to decide

A short core ADR, handed to the core session with this one:

1. **An agent key may join a spot** of the workspace the key is bound to; the
   spot's hello carries the cluster name so the TUI shows "spot of cluster
   X", and the core gives the spot the cluster's namespace scope.
2. **One spot per workspace stays the model.** A second spot join for a
   workspace that already has an approved spot stays pending with the reason
   "the workspace already has a spot: <name>" shown in the TUI; a person
   revokes one to switch. (A customer with several clusters enables the role
   on one of them. Spots per cluster, or a speaker election, is a later ADR
   when someone needs it.)
3. **Instruction and tools.** The instruction cell names the `cluster` tools
   and when to prefer them over stored signals.

## Consequences

- One binary carries the Anthropic SDK, `genai`, `mcp-go` and the OTel SDK
  (pure Go, no cgo). The image grows; nothing starts unless the role runs. A
  separate image for the role is not worth a second supply chain; revisit if
  the size ever matters to a customer.
- The runtime has two homes until the spot repo is retired; the move is a
  copy with history (`git subtree` or a filter), and the spot repo accepts
  only fixes it then ports.
- The customer's cluster now holds a model credential and runs model calls on
  the customer's bill, which is the point (ADR-099). Nothing in the
  substrate sandbox exists there: the binary and the read-only ClusterRole are
  the boundary. The security review of the role covers prompt injection from
  logs and events into a loop that holds no write verb.
- The `cluster` tools answer from the live cluster, so a room question gets
  the pod's logs and events rather than the agent's summary of them.

## Rollout

1. **The role.** Packages in, `--mode=spot`, chart `spot.*`, join with a spot
   enroll key (works against today's core); a customer spot on the test
   cluster talks in a room.
2. **One key.** Core item 1 and 2 land; the chart's enroll key becomes
   optional; the first customer spot joins with the cluster's agent key.
3. **Local tools.** The `cluster` tool server, its RBAC, the instruction;
   a room question answered from live logs.
4. **One image everywhere.** Our-side actors on the agent image
   (`--spot-runtime=substrate`); the spot repo archived, its chart deprecated
   with a pointer to `spot.enabled`.

Each phase is a release of this chart; a customer on an earlier phase loses
nothing, the role is off until they turn it on.

## Non-goals

- A spot that acts on the cluster (writes). Separate ADR.
- Several spots per workspace or a speaker election among them.
- Moving the core's machine path (ingest, gating, issue lifecycle) anywhere.
