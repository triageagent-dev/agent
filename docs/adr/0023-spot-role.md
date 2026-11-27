# ADR-0023: Spot Role (the cluster's MCP tools for Tria: tunnel and delegate)

- **Status:** Proposed. Nothing is built. This text replaces the draft of
  2026-10-30, which gave the role built-in cluster tools, its own chat with
  people and the tenant's model calls.
- **Date:** 2026-11-27
- **Deciders:** Denis (Principal SRE)
- **Related:** ADR-0015 (sensor stream), ADR-0022 (OTLP sensor role, the
  pattern of a role in its own Deployment). Core: ADR-098 (the spot's own
  agent loop), ADR-099 (a spot runs the core's model calls), ADR-105 (spots
  join by certificate), ADR-109 (Tria fronts the workspace), ADR-114 (agent
  cards), ADR-121 (durable tasks), and the core ADR this one asks for.

## Context

The agent runs on the customer's cluster and sends telemetry to the core. When
Tria investigates, she sees what that telemetry holds and what the core's own
external MCP servers reach. Data that only the customer's tools can serve, such
as a log search or a cluster API behind the cluster's network, is out of her
reach.

Customers have MCP servers for such data inside their network, and some have a
model of their own that they want to read it with. They need one component
that connects outbound to the core, uses those servers within limits the
cluster owner sets, and can be managed from a conversation with Tria.

A spot today is an agent loop for one workspace that joins the core by
certificate and reaches other systems through connectors: streamable-HTTP MCP
servers with a required list of allowed tools. Two things it does not do are
needed here: it sends a connector no credential of its own, and it talks to
Anthropic or Gemini only, at their own endpoints.

## Decision

The agent gets a spot role, `--mode=spot`. It connects the customer's MCP
servers to Tria in one of two modes.

```
tunnel:    Tria (core's model) -> agent -> customer's MCP servers
delegate:  Tria -> agent (customer's model, customer's MCP servers) -> answer
```

### 1. Who does what

| Tria (core) | Spot role (agent) |
|---|---|
| Talks with people | Takes requests from Tria only |
| RCA, decisions, memory, issues | None of these |
| Chooses what to ask for | Reads through MCP, and in `delegate` analyses and answers |
| Switches installed servers, tools and models | Applies what narrows its cluster config |

The role starts no work of its own: no issue wakes, timers, promises or chat
with people. It gets no model calls of the core (ADR-099 does not apply to it).
With no request it calls no model and no tool, apart from health checks of the
MCP servers (`initialize`, `ping`, `tools/list`).

Tools come from the customer's MCP servers only. The role has no built-in
cluster tools and no shell. Cluster access is an MCP server like any other,
and what it can read is set by that server's own credential. A Kubernetes MCP
server shipped with the chart can be proposed later.

### 2. Deployment

- The role runs as `<release>-spot`: its own Deployment, one replica, chart
  `spot.enabled` (default `false`), in the `agent` image. It refuses to run
  with another mode in the same process. The telemetry roles do not change.
- It has its own ServiceAccount with **no ClusterRole**. Its one Kubernetes
  grant is `get` and `update` on the Secret that holds its identity, by name.
- The pods that send telemetry get no MCP credential and no model credential.
- As for the other roles, the chart renders no NetworkPolicy.

### 3. Identity

The role joins over the core's spot join (ADR-105), under its own identity
named after the cluster by default. An enrollment key is good for one join and
the controller has spent the install key, so the first join of the role either
uses a spot enrollment key from a Secret (`spot.enrollKeySecret`) or waits as
pending until a person approves it. One key for both roles is an open
question. The role renews its certificate and keeps it in
`<release>-spot-identity`.

### 4. The cluster owner sets the limits

Everything the role can reach is declared in chart values. The core cannot add
to it: it never sends a URL, a credential or a new tool name.

```yaml
spot:
  enabled: false
  mode: ""                  # tunnel | delegate; required when enabled
  mcpServers:
    - name: example-logs
      url: https://logs.example.svc/mcp
      tools: [search_logs]  # required, exact names
      hint: Application logs.
      assumeReadOnly: false
      auth:
        bearerSecret: {name: "", key: token}
      caSecret: {name: "", key: ca.crt}
  limits: {}                # result bytes, call timeout, parallel calls, calls per minute
  models: []                # delegate only
  dailyTokens: 0            # delegate only: the ceiling
```

- MCP transport is streamable HTTP.
- Credentials are files mounted from Secrets, read again when they change. They
  are never environment variables and never leave the cluster, except toward
  the server they are for.
- A credential goes over `https`, or over `http` only to a cluster-local
  Service name. The role follows no redirect.
- The first release sends a bearer token. OAuth client credentials and
  workload identity come later.
- The render fails when the role is enabled without a mode, when a server has
  no tool list, when `tunnel` is given a model, and when `delegate` is given
  none.

### 5. No writes

MCP gives no reliable way to know whether a tool writes. The role therefore
serves a listed tool only when its server annotates it `readOnlyHint: true`,
or when the cluster owner sets `assumeReadOnly: true` for that server. A tool
held back is reported with the reason.

Both are claims, one by the server and one by the owner. **The boundary against
writes is the credential the MCP server itself holds**, which the install guide
asks to be read-only. Write tools, and the approval they need, are a separate
ADR.

### 6. `tunnel`: Tria uses the tools, no model in the cluster

The core talks MCP to the role over the role's own outbound connection,
separate from the telemetry stream and from the spot's control stream. The
customer opens nothing inbound.

It is not a network tunnel and not a message forwarder. Toward the core the
role **is** an MCP server for each installed server name, and toward that
server it is an MCP client. Only what the role implements passes.

| Control | Rule |
|---|---|
| Target | The core names an installed server, never a URL |
| Features | Tools only: `initialize` and its notice, `ping`, `tools/list`, `tools/call`, the cancel notice |
| Tools | The list holds served tools only, and every call is checked again |
| Headers | The role adds the server's credential and passes no header in either direction |
| Server requests | Sampling, elicitation and roots are refused, so a server cannot use the core's model or prompt a person |
| Resources and prompts | Not offered |
| Limits | Result size, run time, parallel calls and calls per minute; an oversize result fails or is cut with a mark, never silently |
| Disconnect | Calls in flight fail; the role replays none |
| Audit | One log line per call: who asked (as the core states it), server, tool, outcome, time, size. No arguments and no results |

Raw tool results and tool descriptions reach the core and Tria's model. The
prompt injection risk is that of any MCP client with any server. The core
treats them as it treats its other external MCP servers, with one rule of its
own: a tunnelled server exists only in its workspace.

### 7. `delegate`: the customer's model reads, Tria gets the answer

- Tria sends a request the way she asks any spot. The role runs one agent turn
  for it and returns one answer. No MCP is offered to the core.
- The model gets the served tools of the customer's servers **and nothing
  else**: no core tool and no memory of the core. The role's own code uses the
  core to receive the request and send the answer.
- Tool results are handed to the model fenced as data.
- Each turn is bounded by steps, tool calls, tokens and a deadline, and all
  turns by the daily ceiling. A request that cannot be run is answered with
  the reason.
- Between requests the role keeps only what its limits need.
- Raw results stay between the MCP servers, the role and the customer's model
  provider. The answer goes to the core and may quote them.
- Model endpoints and credentials come from the cluster config only. Phase 2
  has Anthropic and Gemini, by key file or workload identity. Phase 3 adds an
  OpenAI-compatible provider, a custom endpoint and a private CA. A provider
  with its own sign-in may need more than that.
- Spans of the loop are off by default. When they are on, prompts, arguments
  and results are left out unless the cluster owner turns them on.

### 8. The mode is explicit

`spot.mode` has no default and a release runs one mode. A `delegate` whose
model is missing or failing **fails the request**. It never serves a tunnel
instead, because that would send raw results to the core without anyone
choosing it.

### 9. Report and settings

After joining, and on every change, the role reports: mode, version and
cluster; for each server its name, hint and health; for each tool its name,
what the server annotates and whether it is served, with the reason when not;
for each installed model its name, provider and model id; its limits; and the
settings revision it applies. It reports no URL, credential or Secret name.

Through Tria, a workspace admin may switch an installed server or tool off and
on again, choose among installed models, set the effort, lower the daily
budget, and pause the role. Settings can only narrow the cluster config:

- The core sends the whole wanted state with a revision.
- The role answers applied or rejected, with the reason. A setting that names
  something no longer installed is ignored and listed in the answer. A model
  that is not installed is rejected. A budget above the ceiling changes
  nothing.
- After each connect the role serves nothing until the core has sent the
  current settings, so a restart cannot switch a disabled tool back on.

### 10. Code

The join and identity code of the spot comes into this repository for phase 1,
with an MCP library as a new dependency. Phase 2 copies the model clients and
the MCP client and adds a small loop for one request at a time. The Anthropic
and Gemini SDKs come with it. The spot's supervisor, schedule and notes do not
come: this role has no work of its own. The spot repository stays as it is and
keeps building the full spot. If the copied code drifts, it moves to a shared
module.

## What the core has to decide

1. **Two spot kinds, `tunnel` and `delegate`**, from the join. Neither gets
   model calls of the core, wakes, promises or booked work, and neither is
   picked when Tria asks "a spot" without naming it. Neither takes the place
   of the workspace's full spot.
2. **Several per workspace**, one for each cluster, chosen by name and by what
   they report.
3. **The tunnel as an MCP server of Tria**, in the machinery that serves her
   other external MCP servers: bound to one workspace, tools loaded when
   needed, off in shared rooms by default. A call reaches the replica that
   holds the connection.
4. **Report and settings**, with tools for Tria; settings need workspace admin.
5. **Delegate requests** over the path Tria uses to ask a spot, without the
   core tools in the model's turn.
6. **Guardrail on tool results** as a policy setting that only flags at first.
   Blocking is decided after false alarms on real logs are measured.

## Consequences

- A customer can let Tria use data that never had to be sent as telemetry, and
  chooses whether raw results leave the cluster (`tunnel`) or only answers
  (`delegate`).
- A cluster with the role holds MCP credentials, and in `delegate` a model
  credential, in pods that have no cluster-wide rights.
- A compromised core can call served tools with any arguments (`tunnel`) or
  send any request (`delegate`), inside the limits. It cannot add a server, a
  tool, a model or a credential.
- Join code, model clients and the MCP client have two homes until a shared
  module exists.

## Rollout

| Phase | Agent | Needs from the core |
|---|---|---|
| 1 | `tunnel`: join, report, MCP with bearer token and private CA, cluster limits | Spot kind, report, tunnel as Tria's MCP server |
| 2 | `delegate` with Anthropic and Gemini | Delegate kind, requests without core tools |
| 3 | OpenAI-compatible provider, custom model endpoint, private CA | Nothing |
| 4 | Settings applied | Settings tools for Tria |

Each phase is a release of this chart. The role is off until it is turned on.

## Non-goals

- Tools that write.
- Adding a server, tool, model or credential through Tria or the core.
- RCA, memory, schedules or chat with people in this role.
- Built-in cluster tools or a shell.
- A raw network or HTTP tunnel.
- MCP clients other than Tria using the tunnel.
- MCP resources and prompts.

## Open questions

- One enrollment for the controller and the role.
- Whether these identities count against a workspace's spot licences.
- Both modes in one release, chosen per server.
