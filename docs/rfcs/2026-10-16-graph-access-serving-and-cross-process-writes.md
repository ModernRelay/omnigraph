---
rfc: "2026-10-16-graph-access-serving-and-cross-process-writes"
title: "Graph access, serving and cross-process writes"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-16
updated: 2026-10-18
discussion: https://github.com/ModernRelay/omnigraph/issues/980
supersedes: []
superseded_by: []
blocked_on:
  - "Named targets and CLI coherency (RFC 0059, PR #675) must be accepted with the singular `target` group, no compatibility interval and no transport flags: every command here addresses graphs only through `--target`."
---

# RFC: Graph access, serving and cross-process writes

> A term set in ***bold italics*** is defined at that spot; it is plain text afterwards.

## Summary

This RFC designs how people, agents and programs reach a graph, from first
principles rather than from the commands that exist today. One idea organizes
it: **you name what you act on, and the path, credential, actor and process
follow from that name.** Seven rules follow from the idea:

1. **Name the target, never the path.** Every command addresses a graph through
   an RFC 0059 target. No command takes a transport flag.
2. **Correctness lives in the engine, across processes and machines.** Branch
   creation and deletion, dropping a proposal and cleanup decide through the
   `__manifest` version compare-and-swap, as row writes, merges and schema apply
   already do. Any number of processes may write one graph, and no command
   routes a write through a particular process.
3. **One set of operations, three surfaces.** A CLI command, an HTTP route and
   an MCP tool share a name, a JSON request and result, and one error envelope.
   A command's `--json` output is the route's response body.
4. **One serving concept.** `omnigraph serve` makes a target reachable over
   HTTP: for you on loopback by default, or for others with `--listen`, which
   only a cluster supports. The `omnigraph-server` binary folds into it.
5. **Identity is fixed at the boundary.** `--as` declares the actor on direct
   access and is checked against the credential on served access. It is never
   ignored.
6. **One outcome model.** Servers and the CLI share one registry of error codes
   and one outcome object, and the exit code follows from the outcome.
7. **Introspection is a command.** `omnigraph target list` says what you can
   reach and what is running; `omnigraph access` says who you are there and
   what you may do.

| Rule | Delivered by |
|---|---|
| 1 | RFC 0059's targets with the changes in [Addressing](#addressing), and the `target` group's `add` and `remove` defined here |
| 2 | [Cross-process writes](#cross-process-writes) in the engine |
| 3 | [One set of operations](#one-set-of-operations) and its inventory test |
| 4 | `omnigraph serve`, its ***local server*** for you and its deployment for others; `omnigraph studio`; `omnigraph mcp` |
| 5 | [Identity](#identity) and the `omnigraph-expect-actor` request header |
| 6 | [Outcomes and refusals](#outcomes-and-refusals) |
| 7 | `omnigraph target list`, `omnigraph access` and `GET /graphs/{graph_id}/access` |

`omnigraph serve` is for programs that speak only HTTP or MCP, such as
OmniGraph Studio and agents. `omnigraph-server` serves clusters to others today
and `omnigraph serve` replaces it; a local server serves a graph or a cluster to
you. `omnigraph credential get` hands a server target's static token to a
calling process, so self-hosted servers with static tokens work in every client.

The engine phase adds `__manifest` rows under the next storage stamp; nothing
else changes the storage format. The HTTP API gains an access route, an actor
precondition header and an `outcome` object on error responses. Changes
with a wider blast radius are in
[Appendix A](#appendix-a-changes-for-separate-decisions), each with a
recommended RFC or issue.

## Motivation

Every fact below was checked against OmniGraph main (`794cebab`), the managed
control plane and OmniGraph Studio between 2026-10-16 and 2026-10-18.

**A caller picks a mechanism instead of naming a graph.** The CLI's help lists
about eight ways to address one: `--server`, `--profile`, `--store`, a
positional URI, `--cluster`, `--managed`, `--direct` and implicit folder
context. Each changes the path, the credential and sometimes the actor. RFC 0059
replaces them with named targets but keeps them through a compatibility
interval.

**Writers must stay in one process, and nothing enforces it.** The engine
supports schema apply, branch creation and deletion, and cleanup from one writer
process only ([support boundaries](../dev/invariants.md#current-support-boundaries)).
Its process-local gates, chiefly `WriteQueueManager::for_root`, a process-wide
registry, are what stop five cross-process races, and a second process neither
sees them nor is told they exist. A branch deleted while another process
publishes to it acknowledges a write no live branch shows. Two creates of one
name leave two live incarnations and an unusable name. A branch created during
schema apply keeps the old contract. Cleanup can remove the tree of a branch
another process is creating. A proposal can be both dropped and published.
Ordinary writes, merges and index maintenance are already safe across processes
through the `__manifest` version CAS.

**Nothing serves a local graph to a client that speaks only HTTP.** Studio
never opens storage (its constitution, principle I), and `omnigraph-server`
serves clusters only. The library's eleven single-graph constructors are shaped
for tests: they open read-write; the `open` family acquires cluster admission
that an embedder can never release, because it lives in a private field; they
mount deployment routes that then answer 409 or 500; they ignore the
session-setting environment; and they leave the serving lifecycle (the shutdown
watchdog, the owned-operation drain, exit 2 on an uncertain write) to the
embedder, because only the private `serve_config` implements it.

**Serving has two shapes and one word.** The CLI validates, plans and applies a
cluster; a second binary, `omnigraph-server --cluster`, serves it. The cluster
guide's "serving owner" and "stop serving" mean that binary. Any new local
serving command beside it would make those phrases ambiguous unless serving is
one concept.

**A caller cannot state the actor it expects a server to use.** `--as` is
refused on a served write, although its help still says it has "No effect on
remote writes", and the configured `operator.actor` is dropped there without a
word. A script that means to act as one actor and reaches a server holding
another one's token cannot find out until it reads a receipt.

**Failures have several shapes and clashing exit codes.** HTTP errors are
`ErrorOutput` with an `ErrorCode`, and several kinds are told apart only by
which detail field is present. Data-write commands add a `command_outcome`
object, other commands print `ErrorOutput` alone, cluster commands print
`Diagnostic` records, and managed commands print `{type, title, detail}`. Exit 2
means a usage error, a managed refusal, or a server that stopped with an
uncertain write; exit 4 means a failed precondition on data commands and
`recovery_required` on managed ones.

**Clients learn their limits from refusals.** No route says which actor a
credential resolves to or which actions it may take, so a client shows every
action and learns from a 403. That is the common case for self-hosted static
tokens without a graph policy.

**Clients keep their own registries, and they drift.** Studio parses the CLI's
`config.yaml` and `credentials` file itself, finds a token by server name where
the CLI sends one only for a URL under a declared server (since #1030), and
keeps a second list of servers it adds, with tokens in its own keychain. RFC
0059 replaces the configuration format. A GUI launched from Finder sees
launchd's environment, not the shell's, so environment tokens are invisible to
it.

**Agents have no MCP path to a local graph.** [RFC 0003](0003-mcp-server-surface.md)
makes the server's `/mcp` the only MCP implementation, retires the stdio
package, and defers a stdio bridge "only if local desktop clients need it".

**A loopback port is exposed.** Every local process can reach it, and web pages
can through DNS rebinding. The REST routes have no `Host` check and no CORS
layer; `/mcp` checks host names but not the port.

**Static tokens must stay first-class.** They are how people host their own
graphs: no identity provider and no admission-snapshot publisher.

This needs an RFC: it changes engine control protocols and the storage stamp,
adds CLI commands, a persisted record format, HTTP routes and headers, a public
Rust API and an authorization boundary, retires a binary, amends two support
boundaries, and asks RFC 0059 for three changes.

## User and operational behavior

### Commands

```text
# Where things are, and who I am there
omnigraph target list|show|add|remove [NAME]
omnigraph login|logout --target NAME
omnigraph credential get --target NAME          # a static token, to a pipe only
omnigraph access --target NAME [--branch B]     # my actor and allowed actions

# The graph: each command is one HTTP route and one MCP tool
omnigraph query | mutate | load | export
omnigraph blob get|stat|put|clear
omnigraph branch list|create|delete|merge
omnigraph commit list|show|publish|drop
omnigraph diff
omnigraph schema show|plan|apply

# Physical upkeep, where the storage is
omnigraph optimize | cleanup | index rebuild

# Declarations
omnigraph cluster validate|plan|apply|status

# Reaching a graph from other programs
omnigraph serve  --target NAME [--write]          # for you
omnigraph serve  <cluster root> --listen ADDR     # for others
omnigraph studio --target NAME
omnigraph mcp    --target NAME [--write]
```

The tree is abridged: `snapshot`, `changes`, `graphs`, `init`, `upgrade`,
`repair`, `lint`, `queries`, `policy`, `embed`, `alias`, `skill` and `version`
keep their places. The data, history, schema, upkeep and cluster commands exist
today; this RFC changes how they are addressed, how `--as` behaves on them and
how they report failure. `target`, `credential`, `access`, `serve`, `studio`
and `mcp` are new; `index rebuild` is today's `rebuild-full-text-indexes`; `use`
and `profile`, which select and inspect RFC 0059's predecessors, retire with
them. Groups are singular nouns followed by a verb, as `branch list` already is.

### Addressing

Every command addresses its graph the same way: `--target NAME`, else
`defaults.target`, else an error; `--graph ID` for a target that holds several
graphs, else the target's `default_graph`; and `--branch NAME` where the
operation takes one. A direct graph target already names its graph and rejects
`--graph`. A target says where the graph lives and how to reach it: through
storage, a server or the managed service, as RFC 0059's kinds define.

No command takes a transport flag. `--server`, `--profile`, `--store`, a
positional URI, `--cluster` as an address, `--managed`, `--direct` and folder
context are retired without a compatibility interval, and a legacy-format
operator home refuses every command (`legacy_operator_config`).

The one literal address is a cluster root, taken by the commands a deployment's
own tooling runs on its root without an operator home: `serve --listen` and the
root-scoped cluster administration commands (`cluster status`, `observe`,
`force-unlock`, `upgrade-ledger` and `apply --writers-stopped`). A deployment is
configured by its environment, not by a person's operator home, and a
deployment manager that verifies a process's command can read the root from
its arguments. Each takes the root as its first positional argument and refuses
`--target` beside it.

This asks RFC 0059 for three changes: the singular `target` group with `list`,
`show`, `add` and `remove`; no compatibility interval; and the retirement of
every transport flag at once. [Appendix A5](#a5-rfc-0059-alignment) tracks them.

### Targets

Every server a client uses is a target in the operator configuration, and the
CLI is that file's only program writer. Studio, the agents it runs and terminal
agents then see the same servers, and each stored token exists once.

`omnigraph target list --json` lists the targets and aliases RFC 0059's resolver
reads, and the live local servers of direct targets, without secrets and
without a network request. `omnigraph target show NAME --json` prints one entry
in the same shape.

```json
{"protocol":1,"default_target":"staging",
 "targets":[
  {"name":"local","kind":"direct","scope":"graph","root":"file:///Users/me/company.omni","binding":"sha256:…",
   "local_servers":[{"id":"01JA9…","url":"http://127.0.0.1:53211","access":"read_only","actor":"default"}]},
  {"name":"brain","kind":"direct","scope":"root","root":"file:///Users/me/brain","binding":"sha256:…",
   "local_servers":[{"id":"01JAB…","url":"http://127.0.0.1:53340","access":"read_write","actor":"act-me"}]},
  {"name":"staging","kind":"server","endpoint":"https://staging.example","default_graph":"knowledge","auth":"token","binding":"sha256:…"},
  {"name":"public","kind":"server","endpoint":"https://public.example","auth":"anonymous","binding":"sha256:…"},
  {"name":"cloud","kind":"managed","scope":"cluster","endpoint":"https://…","default_graph":"knowledge","auth":"managed","binding":"sha256:…"}],
 "aliases":[{"name":"people","target":"staging","graph":"knowledge","query":"list_people"}]}
```

- A direct target always carries `local_servers`, empty when none is live: each
  live local server whose [destination](#one-local-server-per-destination) is
  the target's. Liveness comes from each record's lock, and a record from
  another machine is never listed. A
  local server is not a server target; it belongs to the direct target it
  serves.
- The listing never carries a token. A client that needs one runs
  `omnigraph serve --target NAME --reuse --json`, which returns the live local
  server's line.

`omnigraph target add NAME --json` reads one target entry, the same fields the
operator configuration holds, on standard input; `omnigraph target remove NAME`
removes one. Both:

- validate through RFC 0059's resolver: a complete binding, an endpoint scope
  for the credential owner (`target_invalid`), and no duplicate name
  (`target_exists`); `remove` refuses a name it does not hold
  (`target_unknown`);
- change only that entry, keep the rest of the file's text, write through a
  temporary file and rename, and refuse (`config_not_editable`) when they cannot
  do that safely;
- never change `defaults.target` and never store a secret. A token is stored
  with `omnigraph login --target NAME`, which reads it on standard input;
- `remove` refuses the default target (`target_is_default`) and leaves its
  credential, which `omnigraph logout --target NAME` removes.

RFC 0059 says "Add no target administration group" and "Register targets by
editing operator config". This RFC asks for `add` and `remove` as the one
exception: a GUI must add servers without becoming a second writer of a strictly
validated file. It adds no selection command and no current-target state, which
is what that rule protects against; `git config`, `kubectl config set-context`
and `gh config set` play the same part for their tools.

### Serving

`omnigraph serve` makes a target reachable over HTTP and `/mcp`. What it serves,
and to whom:

| | For you (the default) | For others (`--listen ADDR`) |
|---|---|---|
| **Standalone graph** | A local server: loopback, a generated token, your actor, read-only unless `--write` | Refused (`serve_requires_cluster`). Sharing a graph means declaring its policy and identity sources, which is what a cluster is; a one-graph cluster serves it |
| **Cluster** | A local server over the cluster's applied state: loopback, a generated token resolving to your actor, the cluster's own policy, stored queries, embedding-provider bindings and external-Blob rules, read-only unless `--write` | A deployment, as `omnigraph-server --cluster` is today |

`serve` has one meaning, making a target reachable, so the cluster guide's
"serving owner" is any `serve` of a cluster that holds its admission, local or
deployed.
A cluster graph is never served without its cluster's contract, so no graph has
two served views. `omnigraph-server` is retired; see
[Serving for others](#serving-for-others).

A ***local server*** is the for-you form: one `omnigraph` process serving one
target on loopback to clients of the same OS account on the same machine. Its
***server record*** names its URL, token, access mode and actor, so a
same-account client attaches by reading it.

| Target kind (RFC 0059) | `serve` | `studio` | `mcp` | `credential get` |
|---|---|---|---|---|
| Direct graph, standalone | A local server for the graph | Opens Studio, which attaches to or starts a local server | Relays to the live local server, or hosts one | Refused: a local server's token is in its record |
| Direct root of a cluster, or a direct graph inside one | A local server for the cluster | As above | As above | Refused |
| Direct catalogue (`og_catalog`) | Refused | Refused | Refused | Refused |
| Server with a token owner | Refused: the graph already has a server | Opens Studio on it | Relays to the server's `/mcp` with the owner's token | Returns the owner's token |
| Server with anonymous access | Refused | Opens Studio on it | Relays without a token | Refused (`credential_anonymous`) |
| Server on a Unix socket | Refused | Refused (`studio_transport_unsupported`): Studio's client speaks TCP | Relays over the socket | Returns the owner's token |
| Managed cluster | Refused | Refused until [A2](#a2-oauth-for-standard-clients-and-managed-access) | Refused until [A3](#a3-mcp-relay-for-managed-targets) | Refused: managed credentials stay the CLI's |
| Managed account | Refused | Refused | Refused | Refused |

A command refuses a kind its column marks refused (`target_kind_unsupported`).
A direct graph target inside a cluster root serves its cluster, and the ready
line names that graph as `graph_id`.

#### Access and actor

A local server is read-only unless `--write` is given. Its actor comes from
`--as`, else `operator.actor`; a read-only local server of a standalone graph
falls back to the actor `default`, the actor a single unnamed server token
already resolves to. `--write` without an actor refuses before opening anything
(`actor_required`).

For a standalone graph the local server generates its policy:

| | Read-only | Read-write |
|---|---|---|
| Granted | `read`, `export` on every branch and graph-wide; `graph_list` | Also `change`, `branch_create`, `branch_delete`, `branch_merge`, `commit_publish`, `commit_drop` |
| Never granted | `schema_apply`, `admin`, `config_manage`, `invoke_query`, and any action not named above | The same |
| Write routes | 403 `forbidden` before the body is read | Admitted |
| `/mcp` | Read tools, including ad-hoc `read` | Read and write tools |

- **Every action is classified.** The generated policy decides each
  `PolicyAction` by an exhaustive match, so an action added later is never
  granted until an RFC names its local-server class.
- **Previews are routed as writes.** A merge `dry_run` is documented as a read
  of both branches but sent to `/branches/merge`, which ingress classifies as a
  write before reading the body, so a read-only local server refuses it.
- **Schema and upkeep stay commands.** No local server mounts deployment,
  schema-apply or maintenance routes; `schema apply`, `optimize` and `cleanup`
  run where the storage is, beside it once the engine phase ships
  ([Cross-process writes](#cross-process-writes)).
- **Actor views.** On a standalone graph that declares
  [actor views](https://github.com/ModernRelay/omnigraph/pull/951), the
  generated policy is a version-2 bundle that places the local server's actor in
  the views' bypass, as anyone holding storage credentials already sees
  everything, and the startup line says so. `--write` refuses there
  (`actor_views_read_only`) until that RFC's write phase.

For a cluster the cluster's applied policy decides, for the local server's
actor, exactly as it would on a deployment; a read-only local server adds a
generated `forbid` for every write action to that policy set, so read-only
holds in the engine as well as at ingress and `/mcp`. A read-only local server
of a cluster is a read-only consumer, as direct CLI reads are: it validates
membership, the ledger CAS and the accepted schema through `graph_read.rs` and
takes no admission. It captures the serving projection from one ledger CAS and
keeps it for its lifetime; an apply made after it started reaches it at its
next start.
`--write` makes it the cluster's serving owner. It acquires the cluster's
admission as a deployment does, so it refuses while another owner holds it
(`state_lock_held`, naming the lock), and while it runs a direct `cluster
apply` against the root refuses the same way. A clean shutdown releases
admission on local and S3 roots, as a deployment's does. Admission has no
lease: after a crash, as after any owner's, the lock stays until `omnigraph
cluster force-unlock` names it once the process is gone.
[#1067](https://github.com/ModernRelay/omnigraph/issues/1067) proposes a leased takeover, so a successor restarts without an
operator; on local roots, which have no update-if-unchanged, that needs lock
generations written if absent. Azure roots keep their
existing rule that even a clean exit retains admission.

#### Lifetime and output

A local server runs in the foreground until interrupted, or until its parent
closes it, and shuts down through the server's bounded shutdown. It handles a
storage write of unknown outcome as a deployment does. Today that stops the
process, which keeps its admission and exits 2; after
[#1067](https://github.com/ModernRelay/omnigraph/issues/1067), the write's own
request reports `outcome_unknown` and the server keeps serving. Read-write local
servers wait for that change, so one storage error cannot take a graph away from
Studio. Nothing replays the write. Logs and the startup line (the resolved
target, access mode, actor and URL) go to stderr. With `--json`, stdout carries
exactly one line, when it is ready:

```json
{"protocol":1,"id":"01JA9…","reused":false,"url":"http://127.0.0.1:53211","scope":"graph","graph_id":"default","access":"read_write","actor":"act-me","binding":"sha256:9e1c…","token":"<bearer>","record":"/Users/me/Library/Application Support/OmniGraph/servers/01JA9….json"}
```

`scope` is `graph` or `cluster`; a cluster's line names the target's graph as
`graph_id` when the target names one and omits it otherwise. If the line cannot
be delivered, the local server shuts down and removes its record. `binding` is a
digest of the resolved destination, not a secret. `--port <n>` replaces port 0
for SSH forwarding; the bind address is always `127.0.0.1`.

**No command writes a token to a terminal.** The ready line carries `token`
only when stdout is not a terminal; to a terminal it omits the field, and the
record still holds the token. The human banner never shows it,
`credential get` refuses a terminal (`credential_terminal`), and `target`
listings never carry a token. A caller that needs a token reads it through a
pipe, as Studio and `omnigraph mcp` do.

#### One local server per destination

A local server is identified by its ***destination***: the canonical root
together with every non-secret setting of the target's binding that selects the
backing store, such as an S3-compatible endpoint or an Azure account. Two
targets with the same `s3://bucket/graph` on different endpoints are different
destinations; two targets whose bindings name the same root and the same store
settings are one destination, whatever their names. When the CLI cannot
establish a target's destination from its binding, it refuses to start or
reuse a local server for it (`local_server_destination_unknown`).

`serve` elects one local server per destination. It takes the destination's
election lock, a file named by the destination's digest in the records
directory, with `try_lock`, and holds it from its look for a live record until
its own record is published. A second `serve` for the destination waits, within
a bounded deadline, for the first to publish, then finds its record; if the
first exits without publishing, the waiter takes the lock and starts itself,
and if the deadline passes it refuses (`local_server_starting`). So Studio and
an `omnigraph mcp` bridge starting together end with one local server. The
election lock orders starts on one machine for one account; it is not writer
fencing, and it never touches direct commands.

With a live local server for the destination:

- Without `--reuse`, `serve` refuses with `local_server_exists`, naming it.
- With `--reuse`, it prints the live local server's line with `"reused": true`
  and exits 0, unless one of these refuses:
  - `--write` was asked of a read-only one (`local_server_access_mismatch`),
    which names the process that owns it from its record;
  - the actor this start would resolve, by the rule in [Access and
    actor](#access-and-actor), differs from the live one
    (`local_server_actor_mismatch`). A standalone graph's local server compares
    actors when `--write` is asked; a cluster's compares them for every reuse,
    because the cluster's policy decides what that actor may read;
  - its CLI speaks another HTTP contract or cannot serve the graph's storage
    format (`local_server_cli_incompatible`).

  Studio and `omnigraph mcp` always pass `--reuse`, so neither inherits another
  invocation's actor or access silently.

Until the engine phase ships, election is also what keeps every client of one
destination in one writer process: Studio, its agents and `omnigraph mcp` all
reuse one local server. Direct CLI writers beside it remain the documented
interim boundary. After the engine phase, two writer processes of one
destination are as correct as one, and election is a convenience that gives
every client one URL, one token and one actor
([Cross-process writes](#cross-process-writes)).

#### Serving for others

`omnigraph serve <cluster root> --listen ADDR` is a deployment. `--listen`
replaces `--bind` and has no default, so serving for others is always a
deliberate choice of address. Everything else carries over from
`omnigraph-server` unchanged:

- the flags `--bootstrap-handoff`, `--data-token-trust`,
  `--oidc-identity-trust`, `--unauthenticated`, `--require-all-graphs`,
  `--shutdown-grace-seconds`, `--mcp-allow-adhoc`, `--mcp-allow-writes` and
  `--mcp-allowed-host`, with their environment equivalents;
- the token sources `OMNIGRAPH_SERVER_BEARER_TOKEN`,
  `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE`, `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON`
  and `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET`;
- the workload limits, the session-setting environment, `RUST_LOG` and the
  restricted log targets;
- admission, snapshot capture before binding, clean release, and the
  `OMNIGRAPH_LISTEN_ADDR=` line and logs on stdout;
- exit 0 on clean shutdown and 2 when it stops with an unknown outcome or an
  exhausted shutdown grace, whose triggers [#1067](https://github.com/ModernRelay/omnigraph/issues/1067) narrows.

It takes the cluster root as its one positional argument and refuses a root that
is not a cluster (`serve_requires_cluster`). `/healthz`, `/readyz`,
`--bootstrap-handoff`, provider and data-token trust, and `/mcp`'s host rules,
including OIDC identity trust admitting its resource's host, behave as they do
today. Without `--listen`, `serve` never binds anything but `127.0.0.1`. The
container entrypoint runs `omnigraph serve`, wrapped by
`omnigraph-azure-admission` for `az://` roots as it wraps the server today; the
release archives, images, `scripts/install.sh` and the Homebrew formula drop the
`omnigraph-server` binary.

### Cross-process writes

Any number of processes, on one machine or many, may run any operation on one
standalone graph. Each operation either publishes once or refuses with a typed
outcome; an operation that lost to another reports it, and the caller retries
from a fresh view. No command routes a write through a particular process and
no client needs to know which processes are running.

**Until phase 5 ships,** today's boundary applies unchanged: schema apply,
branch creation and deletion, and cleanup are supported from one writer
process, and a read-write local server counts as one. The user page says so,
and so does the skill: beside a read-write local server, use `omnigraph mcp`,
which goes through it, or stop it first.

Cluster admission is unchanged by this rule. It serializes a cluster's
deployments and its serving owner; it was never the engine's correctness
mechanism, and the engine changes below make a cluster's graphs correct
whoever writes them.

### One set of operations

Every graph operation has one name and one JSON contract on three surfaces: a
CLI command, an HTTP route and an MCP tool. A command's `--json` output is the
route's response body, and a tool's structured content is the same body.

One table in `omnigraph-api-types` lists each operation with its route, tool
and command, or a declared exception with its reason. An inventory test
enumerates the server's router, the MCP tool list and the CLI's command tree,
and fails when any of them has an operation the table does not bind, or binds
it differently. Adding an operation therefore adds it to all three surfaces or
states why not.

Declared exceptions, by reason:

| Reason | Operations |
|---|---|
| Runs where the storage is | `init`, `upgrade`, `repair`, `optimize`, `cleanup`, `index rebuild`, `schema plan` and `schema apply` on a direct target, and `cluster validate`, `observe`, `force-unlock` and `upgrade-ledger` |
| Local tooling with no graph | `lint`, `embed`, `policy test` and `explain`, `queries validate`, `skill`, `version` |
| Byte streams, which MCP tools do not carry | Blob payloads and export streams; `blob stat` is a tool |
| Deployment control, which is not an agent tool | `/cluster/*` over MCP |
| Process probes, not graph operations | `/healthz` and `/readyz` in the CLI |

Gaps on main that this closes:

| Gap | Change |
|---|---|
| MCP has no commit diff, commit publish or drop, change-feed baseline, branch diff or merge-base tool, and no `blob stat` | Add the tools; write tools stay behind `--mcp-allow-writes` |
| MCP refuses stored mutations | Admit them as write tools behind `--mcp-allow-writes`, or declare the reason in the table |
| The CLI cannot list a server's stored queries | `queries list --target NAME` reads `GET …/queries` for a server target |
| `load --json` prints a CLI-only `LoadOutput`, which adds `uri`, drops `actor_id` and omits a null `base_branch` | Print the route's body |

The parity matrix already runs each CLI invocation embedded and remote and
compares their JSON, and the MCP suite compares read tools with their routes.
The matrix gains the third comparison: the CLI's `--json` output against the
HTTP body of the same request.

### Identity

An operation's actor is fixed where trust is established:

- **Direct access.** The process holds storage credentials and declares its
  actor: `--as`, else `operator.actor`
  ([CLI addressing](0011-cli-addressing-and-config.md)). The engine enforces the
  graph's policy for that actor.
- **Served access.** The server resolves the actor from the bearer credential,
  and `--as` becomes an assertion. The CLI sends `omnigraph-expect-actor:
  <actor>`, and the server compares it with the resolved actor after
  authentication and the contract-header check, before graph lookup, body
  decoding or effects. A mismatch is `412 actor_mismatch`: a condition the
  caller set did not hold, and nothing ran. A duplicate or malformed header is
  `400 bad_request`. Without `--as` the CLI sends no header: `operator.actor`
  names the direct actor and is not asserted against servers, whose tokens may
  resolve to other names. The refusal of `--as` on served writes, and its stale
  help text, go.
- **Local servers.** `serve --as` declares the local server's actor, as direct
  access does; requests to it may assert it like any served request.

An assertion can only narrow: a matching header changes nothing, and no client
supplies an actor the server trusts. `--as` is never ignored.

### Outcomes and refusals

Every failure, from a server or from the CLI, has one shape: today's
`ErrorOutput` (`error`, `code` and its typed details) plus an `outcome` object.

```json
{"error":"a local server already serves file:///Users/me/company.omni","code":"local_server_exists",
 "outcome":{"execution":"not_started","effects":"none","action":"refresh"}}
```

- **One registry of codes.** `ErrorCode` in `omnigraph-api-types` holds every
  code a server or the CLI emits, and each code documents whether servers send
  it and under which status. Every failure carries a `code`, including the kinds
  told apart today only by which detail field is present, so a client reads one
  field everywhere.
- **Managed failures keep their owner's body.** The `{type, title, detail}`
  problems managed commands print are the managed control plane's own. The CLI
  relays one verbatim under `problem`, with `code: managed_problem`, so
  OmniGraph's registry never lists another service's problem types. Its
  `outcome` is the one the control plane states; where it states none, the CLI
  reports `not_started` only if no request was sent and `unknown` otherwise,
  because it never infers whether a remote refusal had effects.
- **One outcome object.** `execution` says whether the operation ran
  (`not_started`, `unknown`), `effects` whether it changed anything (`none`,
  `unknown`), and `action` what to do next (`retry`, `refresh`, `recover`,
  `reconcile`). A server includes it on every error response, computed where
  the knowledge is. The CLI combines it with transport evidence, as
  `command_outcome` does today for data writes, so a lost response becomes
  `unknown` and `reconcile` and is never replayed. Every command reports it, not
  only data writes.
- **A write has three results.** It is committed, not committed, or of unknown
  outcome, the same on every surface, as
  [#1067](https://github.com/ModernRelay/omnigraph/issues/1067) proposes. A server reports unknown only when it could not settle
  the attempt itself, and then as `outcome_unknown`, never as a plain
  `internal`. `outcome.check` carries what a later read needs to settle it: the
  branch, its head before the write and the attempt's transaction id.

  ```json
  {"error":"the write may have published","code":"outcome_unknown",
   "outcome":{"execution":"unknown","effects":"unknown","action":"reconcile",
              "check":{"branch":"main","head_before":"01JAC…","attempt":"0c410ce6-…"}}}
  ```

- **Exit codes follow from the outcome**, the same for every command:

| Exit | Meaning | Today |
|---|---|---|
| 0 | Success | The same |
| 1 | Not started: a refusal, a cancelled queued deployment, or an error before any effect | Also managed `failed`; managed refusals are 2 and cancellation 6 |
| 2 | Effects unknown or incomplete: reconcile or recover before retrying. Also a deployment or local server that stopped with an uncertain write | Also usage errors and managed refusals; managed `recovery_required` is 4 |
| 3 | Completed in part, and the result says which part | Managed partial convergence and partial organization results |
| 4 | A condition the caller set did not hold: a Blob precondition, a conditional publish, an `--as` assertion | Also managed `recovery_required` |
| 5 | A bounded wait ended; the work continues | Also managed `stalled` |
| 64 | Usage error (`EX_USAGE`) | 2, Clap's default |
| 75 | Busy; retry after `Retry-After` (`EX_TEMPFAIL`) | The same |

Each exit has one meaning on every plane. Cluster commands keep reporting
`Diagnostic` findings inside their results; a failed command still ends with
the one envelope.

The new codes:

| Code | Commands | When |
|---|---|---|
| `legacy_operator_config` | All | The operator home has the legacy format |
| `target_kind_unsupported` | `serve`, `studio`, `mcp`, `credential get` | The command does not serve the target's kind |
| `serve_requires_cluster` | `serve --listen` | The root is not a cluster |
| `actor_required` | `serve --write`, `mcp --write` | Neither `--as` nor `operator.actor` names an actor |
| `actor_views_read_only` | `serve --write`, `mcp --write` | The standalone graph declares actor views |
| `actor_mismatch` | Every served command with `--as` | The asserted actor differs from the credential's |
| `local_server_exists` | `serve` | A live local server holds the destination and `--reuse` was not given |
| `local_server_starting` | `serve`, `mcp` | Another start for the destination held the election past the deadline |
| `local_server_actor_mismatch` | `serve --reuse`, `mcp` | The live local server's actor differs from the one this start would resolve |
| `local_server_destination_unknown` | `serve`, `mcp` | The target's binding does not establish its destination |
| `local_server_access_mismatch` | `serve --reuse --write`, `mcp --write` | The live local server is read-only |
| `local_server_cli_incompatible` | `serve --reuse`, `mcp` | The live local server's CLI speaks another HTTP contract or cannot serve the graph's storage format |
| `local_server_records_unsafe` | `serve`, `mcp`, `target list`, `target show` | The record directory has another owner, group or other permission bits, or is a symlink |
| `studio_unavailable` | `studio` | Studio is not installed, or nothing on this platform handles its links |
| `studio_transport_unsupported` | `studio` | The server target is on a Unix socket |
| `credential_terminal` | `credential get` | Stdout is a terminal |
| `credential_anonymous` | `credential get` | The server target uses anonymous access |
| `credential_missing` | `credential get` | No token is stored for the target's owner |
| `target_invalid` | `target add` | The entry fails RFC 0059's resolver |
| `target_exists` | `target add` | The name is taken |
| `target_unknown` | `target show`, `target remove` | No target has the name |
| `target_is_default` | `target remove` | The target is `defaults.target` |
| `config_not_editable` | `target add`, `target remove` | The entry cannot be changed while keeping the rest of the file's text |
| `branch_exists` | `branch create` | Another process registered the name first |
| `branch_deleted` | Every write to a branch | The branch was deleted while the write prepared |
| `commit_dropped` | `commit publish` | The proposal was dropped first |
| `commit_published` | `commit drop` | The proposal was published first |
| `outcome_unknown` | Every write | The server could not settle whether the attempt published |
| `storage_unavailable` | Every write | Storage is failing broadly and writes are refused for now, with `Retry-After` ([#1067](https://github.com/ModernRelay/omnigraph/issues/1067)) |

A read-only local server's write routes and its loopback guard answer with the
existing `forbidden`.

### Access

`omnigraph access --target NAME [--graph G] [--branch B] [--target-branch T]`
says who you are at a target and what you may do:

```json
{"protocol":1,"target":"staging","graph_id":"knowledge","actor":"act-me","source":"server",
 "graph_wide":["read","export"],"branch":"main","actions":["read","export","change"]}
```

- **Server targets and local servers** answer through
  `GET /graphs/{graph_id}/access`, which evaluates the installed policy for the
  caller's resolved actor and each action, in the three shapes policy decisions
  take: graph-wide, per branch (`?branch=`), and from a source to a target
  branch (`&target_branch=`, for `branch_create` and `branch_merge`). It
  evaluates actions by the names given, never by whether a branch exists, so it
  cannot probe what a 404 hides, and it reveals nothing about other actors or
  the policy's rules. A static token without a graph policy gets `["read"]` and
  a signed data credential without one `[]`, matching `authorize`.
- **Direct targets** answer locally (`"source":"direct"`): the CLI evaluates the
  graph's installed policy for the declared actor; without a policy every
  action is allowed, as it is for direct writes.
- It reports Cedar operations, not actor views, whose per-securable roles can
  still refuse a permitted `change`; a `view` field is reserved, and the actor
  views RFC should list the route among its surfaces.

Clients show the actions `access` allows instead of learning them from a 403.
Studio's spec 012 lists this as E15.

### Opening Studio

`omnigraph studio --target NAME [--graph G]` validates the target, opens
`omnigraph-studio://open?target=NAME&binding=<digest>&home=<digest>[&graph=G]`,
prints the link and exits, as `codex app` opens the Codex app. It owns nothing
at runtime, so its one contract with Studio is the link format, which Studio
owns.

- On macOS it opens Studio by bundle identifier (`open -b`). On Linux and
  Windows it opens the link only when the platform names a handler for the
  scheme (`xdg-mime query default x-scheme-handler/omnigraph-studio`, or the
  scheme's registry association), because `xdg-open`'s generic path hands an
  unhandled scheme to a web browser. Without Studio or a handler it prints the
  link and how to install Studio, and refuses (`studio_unavailable`), until
  [installing Studio](#installing-studio) ships. It never opens a browser.
- With `--json` it prints
  `{"protocol":1,"target":"local","binding":"sha256:…","home":"sha256:…","link":"omnigraph-studio://open?target=local&binding=sha256:…&home=sha256:…"}`.
- **Studio resolves the target itself**, through the CLI it chose, in the
  operator home its captured environment names, and checks the link against
  that resolution. `home` is a digest of the canonical operator home the
  launcher used: a link from another home, such as a one-off
  `OMNIGRAPH_HOME=/tmp/work omnigraph studio --target prod`, is refused with
  both homes named, rather than resolving `prod` in Studio's own home. A
  matching home with a different `binding` digest means the target changed
  since the link was made, and is refused the same way. Studio never opens a
  destination the launcher did not validate. A link never names a path or an
  executable.
- For a direct target Studio runs `omnigraph serve --target NAME --reuse --json`
  and attaches to the local server in the line: a live one started by a
  terminal, an agent or Studio, or a new one that is Studio's child. For a
  server target it runs `omnigraph credential get` and connects directly.
- A link opens read-only and never writes, as Studio's own link rules require.
  Read-write is a deliberate action inside Studio, or a local server the person
  started with `omnigraph serve --write`, which Studio attaches to.
- **Enabling writes.** When the read-only local server is Studio's own child,
  Studio stops it through the bounded shutdown, which has no writes to drain,
  and runs `serve --write --reuse`; it then points its windows and its agent
  gateway at the new URL and token. When another process owns it, Studio does
  not upgrade it: `serve --write --reuse` refuses with
  `local_server_access_mismatch`, naming the owner from the record (its process
  ID, start time and CLI), and Studio says that writing needs that process
  stopped first, or the graph stays read-only.
- **Environment.** Studio passes the environment of an interactive login shell
  (`$SHELL -i -l`), captured once, to the `serve` and `credential get` it runs,
  and to nothing else; agents never receive it. A login shell that is not
  interactive skips `.zshrc`, where people export keys. A local server needs
  that environment for its storage provider's credentials and for text search:
  the embedding provider comes from `OMNIGRAPH_EMBED_PROVIDER`,
  `OMNIGRAPH_EMBED_BASE_URL`, `OMNIGRAPH_EMBED_MODEL` and its timeout and retry
  settings, with `OPENAI_API_KEY`, `OPENROUTER_API_KEY` or `GEMINI_API_KEY`. A
  model that differs from the one a schema's `@embed` records refuses `nearest`
  with text loudly. A cluster's local server uses the cluster's applied
  embedding-provider bindings instead.
- A person who needs a terminal-only environment, such as a one-off
  `AWS_PROFILE`, runs `omnigraph serve` in that terminal; Studio attaches to it.

### Installing Studio

A later phase, gated on Studio shipping releases a launcher can verify: builds
signed with Studio's Developer ID and notarized, a fixed Apple team identifier,
a stable "latest" download URL for each architecture, and an updater inside
the app. Until then `omnigraph studio` only prints how to install Studio. Once
the gate opens it follows the Codex desktop launcher
(`codex-rs/cli/src/desktop_app/`):

- **macOS.**
  - Find Studio in `/Applications` and `~/Applications` by its bundle
    identifier, `com.modernrelay.omnigraph-studio`, not by its name.
  - Before every open, run `codesign --verify --deep --strict` against the
    requirement `identifier "com.modernrelay.omnigraph-studio" and anchor apple
    generic and certificate leaf[subject.OU] = "<team>"`, so an app that only
    carries Studio's name or bundle identifier is refused.
  - When Studio is missing, download the latest disk image for the machine's
    architecture into a temporary directory, mount it, and verify the mounted
    app against the same requirement, refusing to install it if verification
    fails. Copy it to `/Applications`, falling back to `~/Applications`, detach
    the image and open Studio. The Apple signature is the integrity proof, so
    no separate checksum is needed. A `--download-url` override, for testing,
    is verified the same way.
- **Windows.** Detect Studio by its package identity and open its link through
  the protocol handler. When it is missing, open its installer or store page
  and ask the person to rerun the command.
- **Linux.** Print how to install Studio. Linux has no common installer
  convention, and Codex implements none.

Apple's signature stays the gate because the operating system checks it
offline. Release provenance attestations, which OmniGraph's own archives carry,
would need a Sigstore verifier inside the CLI; Studio's releases may add them,
but the installer does not require them. The CLI pins no Studio version: it
installs whatever the latest URL serves, and Studio updates itself. The two
versions stay independent, and their only contract remains the link format.

### Platform coverage

The CLI's commands work alike on macOS, Linux and Windows. Studio's side does
not yet:

| | macOS | Linux | Windows |
|---|---|---|---|
| `serve`, `mcp`, `target`, `credential get`, `access` | Supported | Supported | Supported |
| Studio opens a link from `omnigraph studio` | Supported | Once Studio registers its scheme and GPUI's Linux platform dispatches opened links | Once Studio registers its scheme and forwards a second launch's link to the running instance |
| Studio's agent panel | Supported | Supported | Not yet available, as Studio's README says |
| Installing Studio | A verified, signed disk image | Prints how to install | Opens Studio's installer once one ships |

Until Studio's rows land, `omnigraph studio` on Linux and Windows prints the
link and refuses with `studio_unavailable`, and agents there use the CLI and
`omnigraph mcp`. Each gap is a row in
[the Studio changes](#modernrelayomnigraph-studio).

### MCP for agents

`omnigraph mcp --target NAME` speaks MCP over stdio for agents that start their
tools as child processes, such as Claude Code or Codex in a terminal:

```json
{"command": "omnigraph", "args": ["mcp", "--target", "local"]}
```

- **Direct target.** Runs the equivalent of `serve --reuse` in its own process:
  it relays JSON-RPC to the live local server's `/mcp` with the record's token,
  or hosts a local server, read-only unless `--write`, and publishes its record
  so Studio can attach. It exits when its input closes, shutting down a local
  server it hosted. `--write` against a live read-only local server refuses
  (`local_server_access_mismatch`).
- **Server target.** Relays to the server's `/mcp` with the owner's stored
  token, which never reaches the agent's configuration.
- **A 401 or a refused connection while relaying** makes the bridge read the
  record again (a restarted local server) or resolve the owner's token again,
  once; a second failure ends the bridge with a typed error.
- **Bound to one graph.** The bridge resolves one graph as every command does:
  `--graph`, else the target's `default_graph`; a standalone graph's local
  server has only `default`. It fills the `graph` argument of each tool call
  that omits it, refuses with a JSON-RPC error a call or resource URI naming
  another graph, and lists only its graph. An agent that needs several graphs
  is configured with one bridge per graph.

Write tools need two consents. The endpoint is the ceiling: a read-only local
server, or a deployment without `--mcp-allow-writes`, lists no write tools. The
agent's own configuration is the other: without `--write`, the bridge lists and
admits only tools the server annotates `readOnlyHint`, which it already sets
for every read tool. A person opening a graph read-write in Studio has not
thereby given every connected agent write access. Tool logic, refusals and the
write contract stay the server's; the bridge adds no tool.

**Agents inside Studio keep Studio's gateway.** The gateway limits a panel
agent's writes to the agent's own branch and can ask the person before each
write; `omnigraph mcp` does neither. Studio points the gateway at the local
server's URL and token, and on a read-only local server it tells the agent that
its branch writes are refused. Every agent still writes as the local server's
actor until
[delegated, branch-scoped credentials](https://github.com/ModernRelay/omnigraph/issues/978)
and actor views' agent identities exist; then a branch limit becomes a scope
the server enforces rather than one Studio filters.

### Which path an agent uses

The bundled skill, which is what agents read, states one path per place an
agent runs:

| Where the agent runs | Its path |
|---|---|
| A terminal, using the CLI | Commands with `--target`, beside any local server |
| A terminal, using MCP | `omnigraph mcp --target NAME`, which attaches to a live local server by itself |
| Studio's agent panel | Studio's gateway |

An agent finds live local servers and its own access through
`omnigraph target list` and `omnigraph access`, never by reading the records
directory or by trying a write. Until the engine phase ships, an agent beside a
read-write local server creates and deletes branches through `omnigraph mcp`,
which goes through it ([Cross-process writes](#cross-process-writes)).

### Static tokens for clients

`omnigraph credential get --target NAME --json` returns a server target's
stored static token, as `git credential fill` and `gh auth token` do for theirs:

```json
{"protocol":1,"target":"staging","owner":"staging","endpoint":"https://staging.example","scheme":"bearer","token":"<static token>","source":"credentials_file"}
```

- It resolves the target's credential owner as the CLI does since #1030 and
  RFC 0059: `OMNIGRAPH_TOKEN_<OWNER>`, then the owner's section of the `0600`
  credentials file, checked against the owner's endpoint scope before either is
  read. `source` is `environment` or `credentials_file`. The CLI reads no
  unkeyed token.
- It refuses when stdout is a terminal (`credential_terminal`), so a token never
  lands in scrollback.
- It refuses a target that is not a server (`target_kind_unsupported`), an
  anonymous server (`credential_anonymous`) and a target without a stored
  token (`credential_missing`).
- A client sends the token only to URLs under `endpoint`, the rule the CLI
  applies to its own requests.

Studio runs `credential get` again after a 401, so a token rotated with
`omnigraph login --target NAME` reaches it without a restart. Static tokens go
through this broker because a server cannot tell which client holds one; an
OIDC server can scope a token per client, so clients obtain those themselves
([A2](#a2-oauth-for-standard-clients-and-managed-access)).

### Self-hosted servers with static tokens

A person hosting their own graphs runs `omnigraph serve <cluster root> --listen
ADDR` with static tokens and adds the deployment as a target. Four settings
decide how complete a client's experience is:

| What the person wants | What the deployment needs |
|---|---|
| Edits attributed to people | Named actors through `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON`, `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` or `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET`. A single `OMNIGRAPH_SERVER_BEARER_TOKEN` is the actor `default`. |
| Edits at all | An applied graph policy granting the actor its write actions. Without one a static token only reads, which `omnigraph access` now shows before the first refused write. |
| Listing the deployment's graphs | `graph_list` in the cluster policy. Without it a client opens the target's `default_graph` or the graph the person names; `/graphs/discovery` serves verified identities only. |
| Agents over MCP | `--mcp-allowed-host <public host>`, because `/mcp` admits only loopback `Host` values by default (OIDC identity trust admits its resource's host by itself), and `--mcp-allow-writes` for write tools. |

## Design

### Engine: control operations on the version CAS

Checked against main and Lance 11.0.0's source on 2026-10-18.

**What already holds across processes.** Every `__manifest` publication is an
`Operation::Overwrite` committed with zero retries. Lance writes the next
version only if it is absent: a hard link that fails on an existing target on
local disks, `If-None-Match: *` on S3 and Azure. A concurrent commit therefore
loses with `CommitConflict` instead of being rebased. The publisher maps that to
`RowLevelCasContention`, reloads, rechecks its precondition (`ExactGraphHead`,
`ExactGraphVersion` or `ExactCandidate`) and its expected table versions, and
retries within its budget of five. Ordinary writes, loads, merges, optimize and
index maintenance rest on this, and their in-process gates only save wasted
staging. The module says so: "cross-process writers on one graph remain
one-winner-CAS at publish". A loser's detached staging is unreachable and
collectible, as invariant 5 requires. Schema apply against a writer or another
apply is already decided the same way. The one linear `__manifest` commit,
optimize's removal of stale auto-cleanup configuration, can only make a
concurrent schema apply refuse.

**What does not.** The rest of a branch's state lives in Lance refs, and Lance
gives refs weaker guarantees: `Branches::create` is put-if-absent, but
`replace_metadata` is an unconditional read-modify-write, `delete` is
unconditional, no Lance commit reads a ref, and object stores offer no
conditional delete. Local storage has no update-if-unchanged primitive at all
(`PutMode::Update` returns `NotImplemented`), so the one compare-and-swap that
works on every backend is the one manifests use: a numbered object written if
absent. Five races depend on the process-local gates
(`WriteQueueManager::for_root`, a process-wide registry, and the schema gate,
whose exclusive side branch creation takes):

| Race | What happens today across processes |
|---|---|
| Branch delete beside a publisher | The publisher passes its precondition, the delete retires, archives and unlinks the ref, then the publisher's commit lands in the retired tree. It acknowledges a write no live branch shows, and the branch's settled history lacks it. |
| Two creates of one logical name | Each mints its own `{logical}.{ULID}` ref, so neither put-if-absent conflicts. Two live incarnations result, and every resolution of the name fails until someone intervenes. |
| A create during schema apply | Apply checks that main is the only branch, a create in another process adds one without moving main, and apply's `ExactGraphVersion` still succeeds. The new branch keeps the old contract, and merging it fails. System-column upgrade has the same window. |
| Cleanup during a two-phase create | Lance writes the new tree, then its ref. Cleanup can classify the tree as ref-less, and its forced Lance delete can remove the tree, and the ref if it has landed. |
| Publish beside drop of one proposal | Drop releases the proposal's tags and deletes its tag object; publish's `ExactCandidate` never reads the tag. Both acknowledge, and a merge proposal's released source tag can let a third process's cleanup sweep a pin the publish then names. |

Each fix moves the deciding step onto a `__manifest` chain's version CAS, and
leaves every other step a derived, idempotent follow-up:

1. **Retirement is a publication.** Branch delete first publishes a
   head-preserving tombstone version on the branch's own `__manifest` chain,
   with zero retries. A concurrent publisher either loses that version and
   reloads into the tombstone, or wins it and the delete reloads into the new
   head and retries. History settlement reads the chain up to the tombstone, so
   it is complete. The retirement marker, the `BranchContents` archive and the
   unlink follow as today, and a crash between them leaves a tombstoned branch
   that the next delete, cleanup or `repair` finishes. A publisher reading a
   tombstone refuses with `branch_deleted`.
2. **The branch namespace is registered on main.** Creating a branch publishes a
   head-preserving registration of its logical name and native identity on
   main's `__manifest`, after the native ref exists; deleting it publishes the
   matching deregistration after the tombstone. Two creates of one name race on
   main's version: the loser reloads, finds the name registered, reclaims its
   own native ref and tree, which nobody else can have registered, and refuses
   with `branch_exists`. Because each create mints its own ULID, a ref conflict
   on that exact name can only be its own retried request, which Lance's
   `put_ref_if_absent` reports as `RefConflict` without checking; the create
   reads the ref back and treats it as landed. Unretired, registered refs define
   the logical branches. Because a create now moves main's version, schema
   apply's `ExactGraphVersion` loses to a create made after its main-only check,
   and so does system-column upgrade.
3. **Cleanup reclaims only what no registration can still claim.** A tree that
   no registration names and no tombstone retired, such as a create's tree
   before it registers, is reclaimed only after the collector's existing
   seven-day orphan age, whatever the retention policy, and through a delete of
   that tree alone, with the ref rechecked first. Tombstoned trees follow
   today's rules for retired trees. Cleanup never calls
   Lance's forced branch delete, which deletes a ref it finds. This is the one
   place age decides, as it already does for orphan objects: an unregistered
   tree names no publication authority the collector could consult.
4. **Drop is a publication.** Dropping a proposal publishes a head-preserving
   drop record on its target branch's chain before it releases any tag.
   Publish's `ExactCandidate` reload sees the record and refuses
   (`commit_dropped`); a drop that reloads into a published candidate refuses
   (`commit_published`). Tags are released only after the record is durable,
   so no published pin can lose its protection. Publish's precondition is a
   graph-head expectation, so a head-preserving record costs other writers on
   the branch a reload, not a refusal.
5. **The gates stay as contention optimizations.** No correctness argument rests
   on `for_root`, the schema gate or the branch gates any longer, and their
   comments say so. The exclusive schema permit around branch creation becomes
   shared.

**Late requests are harmless by construction.** Every storage mutation in this
design is one of three kinds, the classification
[#1067](https://github.com/ModernRelay/omnigraph/issues/1067) proposes for all of them:

- a fresh, uniquely named object, such as a data, deletion, index or
  transaction file or a detached manifest, which nothing names until a
  publication does;
- a conditional publication, a `__manifest` version written only if absent;
- a follow-up of a publication whose late or repeated landing changes nothing
  logical: retirement's marker rewrite, the archive, the unlink, a losing
  create's reclaim and a drop's tag release.

The marker rewrite is unconditional in Lance 11: `Branches::replace_metadata`
reads the ref, edits it and puts it back with no version check. A late one can
even recreate a ref the delete already unlinked. That ref is retired and
unregistered, so it names no logical branch, and cleanup reclaims it. A guard
against unconditional writes of mutable keys, as #1067 proposes, admits these
only as follow-ups of a publication.

**Format.** Tombstones, registrations and drop records are new `__manifest`
rows, so a graph that uses them carries the next storage stamp, which older
binaries refuse to open, as they refuse every newer stamp. A branch publishes
the stamp with its first new row; `omnigraph upgrade`'s stamp-only route
registers every live branch on main in one publication, from the census it
already takes. [Schema apply with live
branches](https://github.com/ModernRelay/omnigraph/pull/963) also proposes a
stamp; whichever lands second takes the next one, and the two are independent.
Downgrade is export and load.

**Cost.** Branch creation and deletion each gain one zero-retry `__manifest`
publication on main, and deletion one on the branch; drop gains one. Each is
also one more version main's or the branch's writers may reload past, within
their existing retry budget; their preconditions are graph-head expectations,
so they do not refuse. Only schema apply and system-column upgrade, which fix
main's exact version, refuse instead, which is the point. Row writes, loads and
merges are otherwise unchanged. `integration/branch_control_cost.rs` measures
the added requests per backend.

**Recovery.** Every new durable step is a publication or a follow-up of one. A
crash after a tombstone and before the unlink leaves a branch that refuses
writes and that cleanup and `repair` complete. A crash after a create's native
ref and before its registration leaves an unregistered tree that cleanup
reclaims after the orphan age. A crash after a drop record and before the tag
release leaves tags that expire with their captured authority, as a proposal's
tags already do.

### `omnigraph-server` becomes a library

The `omnigraph-server` crate keeps every route, layer and lifecycle and loses
its binary. `omnigraph serve` calls it in both forms:

- **For others**, through the entry point the binary's `main` uses today, which
  becomes public with the options it parses: the cluster root, the listen
  address and the settings in [Serving for others](#serving-for-others). Its
  output and exit behavior are the binary's.
- **For you**, through the local embedding API below.

The CLI already links the crate for its types and error mapping. The crate's
one feature, `aws` (Secrets Manager token sources), becomes a CLI feature that
release builds enable, as they already build the two packages together with
`--features omnigraph-server/aws`; Azure support is compiled in through the
workspace's storage features either way. The crate is still published, as a
library.

### Local embedding API

Exact signatures are settled in the implementation PR; the contract is:

```rust
pub enum LocalAccess { ReadOnly, ReadWrite }

pub enum LocalScope {
    /// A standalone graph, served under the route id `default`.
    Graph(LocalBinding),
    /// A cluster root, served from its applied state.
    Cluster(LocalBinding),
}

pub struct LocalOptions {
    pub access: LocalAccess,
    /// Required for ReadWrite; `default` when absent for a read-only graph.
    pub actor: Option<String>,
    pub process_defaults: ProcessDefaults,
}

/// The local server's bearer token: read once, with no `Debug` or `Display`.
pub struct LocalCredential { /* … */ }

pub enum LocalOpenError {
    ActorRequired,
    ActorViewsReadOnly,
    Admission(ClusterDiagnostic),
    Open(OmniError),
    Policy(PolicyError),
}

/// `Uncertain` when the process stopped with an unknown outcome; callers exit 2.
pub enum ServeOutcome { Clean, Uncertain }

impl AppState {
    pub async fn local(scope: LocalScope, options: LocalOptions)
        -> Result<(AppState, LocalCredential), LocalOpenError>;
}

pub async fn serve_local(
    state: AppState,
    listener: tokio::net::TcpListener,
    extend: impl FnOnce(Router) -> Router,
) -> ServeOutcome;

/// The restricted server subscriber `init_tracing` installs, on a chosen writer.
pub fn install_log_subscriber<W>(writer: W)
where
    W: for<'a> tracing_subscriber::fmt::MakeWriter<'a> + Send + Sync + 'static;
```

`LocalBinding` is the direct binding RFC 0059's resolver captured: the
canonical root and its storage provider settings, so catalog, admission and
Lance access use one destination and no ambient variable redirects them. It is
never a bare URI string.

For a standalone graph, `AppState::local`:

1. Classifies the root with `cluster_root_for_graph_uri`, the rule direct CLI
   commands use; a root inside a cluster becomes `LocalScope::Cluster`.
2. Opens read-only with `Omnigraph::open_read_only`, or read-write with
   `Omnigraph::open`, refusing `ReadWrite` without an actor or on a graph with
   actor views.
3. Generates a 32-byte token and keeps only its SHA-256 hash.
4. Builds the policy as a structured `PolicyConfig` with
   `PolicyCompiler::compile` followed by the kind-alignment validation the
   policy loaders run, for the route's graph id (`default`), deciding every
   `PolicyAction` by an exhaustive match, and installs it with `with_policy`.
   Branch-scoped grants use an `any` scope, which covers graph-wide reads. A
   one-handle registry carries the server policy granting `graph_list`, which a
   single-mode state cannot. A graph with actor views gets the version-2 bundle
   described above.
5. Mounts no `/cluster/*` route, enables MCP ad-hoc reads (`allow_adhoc`), and
   lists MCP write tools only for `ReadWrite`. `McpSettings::default()` keeps
   ad-hoc reads off, so without this a standalone graph, which has no stored
   queries, would give MCP clients no way to read its data; deployments keep
   their default.

For a cluster it builds the serving projection the deployment builds, from the
same applied state, with the generated token as the only credential, resolving
to the local server's actor, and enables MCP ad-hoc reads as for a graph.
Read-only opens every graph through `graph_read.rs` without admission and adds
the generated `forbid` for write actions; read-write acquires admission with
`ClusterAdmissionPurpose::Serve`, as the deployment does. Neither mounts
deployment routes.

Read-only holds in three independent places: `ingress::admit` refuses every
`Write`-class request before its body; `/mcp`, which ingress admits as one
read-class route, lists no write tools; and the engine's policy refuses every
write, including one without an actor, which `enforce` denies whenever a policy
is installed. A local state is never in open or default-deny mode.

`serve_local` refuses a listener whose local address is not loopback and runs
the lifecycle the deployment runs: the signal and uncertain-owner listener, the
watchdog armed with the shutdown grace, graceful HTTP shutdown and the
logical-owner drain. That lifecycle is one private function both entry points
call. `serve_local` prints nothing to stdout and returns an uncertain outcome
instead of exiting the process; the CLI maps it to exit 2. The caller binds the
listener, so it knows the port and writes the record before announcing
readiness. `extend` receives the router `build_app` built from this state.
`install_log_subscriber` makes the private `server_log_subscriber` installable
on a chosen writer, so the CLI logs a local server's events to stderr with the
filters the deployment uses.

The existing convenience constructors stay until
[A4](#a4-retire-the-test-shaped-constructors).

### HTTP API

| Change | Contract |
|---|---|
| `GET /graphs/{graph_id}/access` | [Access](#access); requires authentication like every graph route, and no graph action |
| `omnigraph-expect-actor` request header | [Identity](#identity); `412 actor_mismatch` before graph lookup |
| `outcome` on every error body | [Outcomes and refusals](#outcomes-and-refusals); the existing fields keep their meaning |
| `ErrorCode` | Grows to the one registry; OpenAPI lists every code with the statuses servers send it under |

The HTTP contract version moves once for all four, with `openapi.json`
regenerated through the server's OpenAPI test. Servers keep accepting exactly
one version. A client that talks to deployments of several versions, as a
manager of long-lived deployments does, reads each one's version from
`/healthz`, which needs no header and names it, and speaks that version to it.
A client that refuses unknown fields in error bodies must accept `outcome`
before the new version reaches it.

### CLI

- **Command structure.** Each new command is a `Command` variant in the
  exhaustive `command_plane` and `command_label` matches, with its own module
  under `commands/` and an arm in `app.rs`, which captures the invocation once.
- **Contract versions.** `omnigraph version --json` gains a `protocols` object
  naming the version of each machine-readable contract here (`serve`, `studio`,
  `target`, `credential`, `access`), beside its `http_contract`, `channel`,
  `commit` and `internal_schema`. A client requires the protocol versions it
  speaks before running a command.
- **One failure path.** `app.rs`'s `finish` prints one `ErrorOutput` with
  `outcome` for every command family, replacing the data-write `Failure`, the
  `RemoteRefusalOutput` and the managed `Output` failure bodies.
- **`--as`** sends `omnigraph-expect-actor` on every served request of the
  command.
- **Loopback guard**, a layer the CLI adds in `extend`, outside every server
  layer, so a deployment never gains it:
  - `Host` must be exactly `127.0.0.1:PORT`, `localhost:PORT` or `[::1]:PORT`,
    case-insensitively; anything else, or none, is 403. This closes DNS
    rebinding, which `Origin` alone cannot: a rebound page's `GET` is
    same-origin and carries no `Origin`.
  - Any request carrying `Origin` is 403; native clients send none and a local
    server serves no page.
  - No response carries a CORS header; every response carries
    `Cross-Origin-Resource-Policy: same-origin`. Nothing relies on
    `Sec-Fetch-*`, which browsers do not send to plain-HTTP origins.
  - Malformed `Host` or `Origin` bytes are refused, never a panic.
- **The MCP bridge** relays JSON-RPC unchanged except for filtering
  `tools/list` and refusing `tools/call` for tools without `readOnlyHint` when
  it was started without `--write`.
- **The opener** passes only the link, which carries no secret, so its
  appearance in the opener's arguments discloses nothing.

### Server records

Records live in one directory per OS account, resolved from the account rather
than from `HOME`, `TMPDIR`, `XDG_*` or `OMNIGRAPH_HOME`, so a GUI process, a
terminal and an agent with its own CLI home find the same local servers. The
home directory comes from the account database, as the CLI's managed-session
locks already resolve it (`managed/auth/coordination.rs`):

| Platform | Directory |
|---|---|
| macOS | `<home>/Library/Application Support/OmniGraph/servers/` |
| Linux | `<home>/.local/state/omnigraph/servers/` |
| Windows | `OmniGraph\servers\` in the account's local application data folder (`FOLDERID_LocalAppData`) |

No system cleaner ages these directories out, while `/tmp` is aged out: macOS's
daily `com.apple.tmp_cleaner` deletes `/tmp` files whose access, modification
and change times are all over three days old, so a local server left running
over a weekend would lose its record and the next `serve` would start a second
one. Linux uses the state directory rather than `$XDG_RUNTIME_DIR`, which a
process outside a login session, such as a cron job or a container, lacks;
falling back from one to the other would split one account's processes between
two directories.

On macOS and Linux the directory must be owned by the account with no group or
other permission bits, and is refused otherwise (`local_server_records_unsafe`);
files are opened without following symlinks, with the owner and mode checks
`coordination.rs` applies. On Windows the directory inherits its folder's
access list, which admits only the account, `SYSTEM` and administrators, and
reparse points are refused as `coordination.rs` refuses them.

```text
<directory>/<id>.json    the record, 0600
<directory>/<id>.lock    held by the local server for its whole life
```

```json
{
  "format": 1,
  "id": "01JA9…",
  "machine": "5F0C…",
  "root": "file:///Users/me/company.omni",
  "destination": "sha256:4b7e…",
  "scope": "graph",
  "binding": "sha256:9e1c…",
  "graph_id": "default",
  "url": "http://127.0.0.1:53211",
  "token": "<32 random bytes, base64url>",
  "access": "read_write",
  "actor": "act-me",
  "pid": 41873,
  "started_at": "2026-10-16T09:12:44Z",
  "cli": {"path": "/opt/homebrew/bin/omnigraph", "version": "0.15.0", "channel": "stable", "commit": "…", "http_contract": "0.14", "internal_schema": {"writes": 16, "serves": [14, 16]}}
}
```

The `.lock` file is a liveness signal, not writer exclusion: it decides only
whether a record's owner is alive. The owner takes it with
`std::fs::File::try_lock`, opens the target, binds, writes the record
(temporary file and rename) and then announces readiness.

A reader checks `machine` first. It is the platform's host identifier
(`gethostuuid` on macOS, `/etc/machine-id` on Linux, the `MachineGuid` registry
value on Windows), and a reader ignores a record from another machine without
touching its lock or deleting it, so a home directory shared over a network
cannot send a client to another machine's port or remove a live record. For a
record from this machine, a reader that can take the lock has found a dead
local server: it ignores the record and may delete both files. The operating
system releases the lock however its process exits, so liveness never consults
the process ID, which proves neither liveness nor quiescence
([server runtime RFC](2026-09-29-server-runtime-and-online-deployment.md)). A
record with an unknown `format` is ignored. Matching uses `destination`, never
`root`, which is for display. `cli` is informational; a client chooses its CLI
itself.

The election lock is `<directory>/<destination digest>.start`, opened like the
record files. It is held only while a local server starts, so a reader never
waits on it.

## Invariants

- **Trust at the boundary (10).** No client supplies an actor. Direct access
  declares one, because the process holds storage access and is entitled to
  it, as a direct `omnigraph mutate` is; served access resolves one from the
  credential, and `omnigraph-expect-actor` only narrows. A local server keeps
  only its token's hash; the record is a user-only delivery file like the CLI's
  `credentials`. `credential get` hands a person's own static token to a
  process running as that person. The engine enforces the policy for every
  write.
- **One publication door (2), one view (3), publish once (4).** Local servers
  and deployments write through the existing routes and engine. Cross-process
  writes keep every operation's single publication and make the control
  operations' authority checks part of it.
- **Crash convergence (5).** Every new durable step is either a zero-retry
  publication on a `__manifest` chain or a derived, idempotent follow-up of
  one: retirement's marker, archive and unlink; a losing create's reclaim; a
  drop's tag release. A crash between them leaves state the next attempt,
  cleanup or `repair` completes, and nothing becomes visible before its
  publication.
- **Bounded and observable (11).** One local server per destination, elected;
  fixed shapes for the record and the JSON lines; server limits and the
  owned-operation lifecycle unchanged; one registry of codes and one outcome
  object, with exit codes derived from it; no silent retry.
- **One source of truth (12).** The CLI is the only resolver of targets, the
  only program writer of their configuration and the only store of their static
  tokens. A record is a hint whose liveness the operating system establishes;
  no write path trusts its contents.
- **Deny-list.** No writer lock, fencing or exclusion is added. The record's
  lock signals liveness, and the election lock only orders local server starts
  on one machine for one account; neither is presented as writer fencing, and
  cross-process safety comes from the substrate's CAS. No queue, no silent
  retry, no shadow credential store, no cloud-only path.
- **Support boundaries.** Two change:
  - "The server is cluster-only" becomes: serving for others requires a
    cluster; `omnigraph serve` serves one standalone graph or one cluster to
    same-account loopback clients as a local server.
  - "Schema apply ... Branch creation/deletion and cleanup retain their existing
    single-writer-process control boundary" is lifted by the last rollout phase.
    Until then it applies unchanged, and a read-write local server is one writer
    process.

## Compatibility and reversibility

Storage changes only in the engine phase, below. The changes that break
existing use, deliberately and without adapters:

- **Addressing.** Every transport flag, `use`, `profile` and folder context go
  with RFC 0059's legacy forms; a legacy-format operator home refuses.
- **The server binary.** `omnigraph-server --cluster ROOT --bind ADDR` becomes
  `omnigraph serve ROOT --listen ADDR` with the same settings. A deployment
  keeps the image and command it was created with; a deployment manager that
  fixes each deployment's command generates the new form for deployments it
  creates afterwards and accepts both while older ones remain.
- **Exit codes.** Managed refusals move from 2 to 1, managed
  `recovery_required` from 4 to 2, managed cancellation from 6 to 1, and usage
  errors from 2 to 64.
- **The HTTP contract.** One version bump carries the access route, the actor
  header, `outcome` and the code registry; clients and servers upgrade
  together, as the contract header already requires.
- **Failure bodies.** The managed `{type, title, detail}` body and the CLI's
  `command_outcome` field become `ErrorOutput` with `outcome`.

New contracts: `serve`, `studio`, `mcp`, `target`, `credential get` and
`access`, their outputs (`protocol: 1`), the `protocols` field of
`version --json`, the record (`format: 1`) and its directories, the Studio link
format, and the local embedding API. Studio stops reading the CLI's files and
keeping its own server list.

Reverting the client commands removes them with no data migration. Reverting
the binary fold restores the binary from the same library. The cross-process
phase is the one irreversible change: it adds `__manifest` rows under the next
storage stamp, which older binaries refuse, and a graph goes back only by
export and load ([Engine](#engine-control-operations-on-the-version-cas)).

## Alternatives

| Alternative | Why not |
|---|---|
| Keep `omnigraph-server` beside a new local `serve` | Two binaries sharing one verb, and "serving owner" in the cluster docs would stop having one meaning. One command whose audience is explicit is what Vault does with `vault server -dev` and `vault server -config`. |
| Refuse to serve cluster graphs locally | An earlier revision. It avoids two views of one graph, but leaves a local cluster unopenable in Studio without a deployment. Serving the cluster's applied state for you gives the same single contract and works locally. |
| Serve a cluster graph from direct storage, without its cluster's contract | An earlier revision. The same graph would show different policy, stored queries and bindings depending on how it was opened. |
| A per-user daemon that every CLI command goes through, as `dockerd` or Codex's app-server | Correctness must already hold across machines, since servers and laptops share object-storage roots, so the engine needs cross-process safety regardless. A daemon then adds lifecycle and version skew between the CLI and the daemon, and [unified access paths](0009-unified-access-paths.md) rejects a localhost funnel for the embedded path. |
| A cooperative machine-local writer lock | Protects only cooperating processes of one account on one machine, and changes every direct write. The substrate's CAS protects every writer everywhere. |
| A `--via-server` flag that routes a direct write through the live local server | An earlier revision's stopgap. It is a transport flag, which rule 1 forbids, and becomes dead once the engine is safe across processes. The interim boundary is documented instead. |
| Refuse a direct write beside a live read-write local server | A record is a hint no write path trusts; refusing on it would make a discovered file a precondition of every write and break scripts, and it still protects nothing that does not look. |
| A CLI-only refusal field beside the HTTP API's `code` | An earlier revision. Two code namespaces for one failure; one registry costs nothing more. |
| Call the local server a session | "Session" already names the engine's session settings, the managed control plane's login sessions and the CLI's internal session plane, whose name its messages print. |
| Records under `/tmp` | macOS deletes `/tmp` files untouched for three days (`com.apple.tmp_cleaner`), and many Linux distributions age `/tmp` out through `systemd-tmpfiles`. |
| A single-use connect code in the opened link, redeemed over loopback | The first draft. Jupyter shipped a single-use token in argv and removed it because another logged-in user could read it from the process list; on Linux and Windows a scheme handler receives its URL in argv. The record replaces the redemption route, attach keys and handoff files it needed. |
| The launcher hosts the local server in its terminal and opens a link to it | It keeps the terminal's environment, but the launcher then owns runtime state and a second link form. Studio owning its local servers, as the Codex app owns its app-server, gives one owner and one contract; a terminal-hosted `serve` remains available. |
| A browser UI embedded in the CLI ([PR #850](https://github.com/ModernRelay/omnigraph/pull/850)) | Studio is a native client in its own repository, and embedding a JavaScript build makes a frontend toolchain a prerequisite of every CLI build. Its boot mode takes cluster writer admission and runs unauthenticated. Its point that an embedder must reuse the server lifecycle is adopted as `serve_local`. |
| Serve local graphs unauthenticated on loopback | Any local process or account can write, writes record no actor, and `Host` checks do not stop another local user. |
| Studio bundles its own engine | A newer engine stamps a branch's storage format forward on first publication, and an older binary refuses a newer stamp. Local servers run the person's own CLI. |
| Studio parses the CLI's files, or writes them itself | Two resolvers have already drifted, RFC 0059 changes the format and validates it strictly, and a second writer can collide with a person's edit. |
| Studio keeps its own server list and token copies | Agents Studio runs would see other servers than Studio, and rotated tokens would drift. |
| Agents inside Studio use `omnigraph mcp` | The gateway limits their writes to their own branch and can ask before each write; the bridge does neither. |
| A CLI proxy for every remote target | A second request path that must stay transparent for streams, headers and outcomes. |
| Unix sockets instead of loopback TCP | No browser can reach a socket and file permissions would authenticate, but the server's lifecycle and Studio's client are TCP-only and Windows needs named pipes. A sandbox that blocks loopback does not block stdio, so `omnigraph mcp` already reaches sandboxed agents. A later protocol can add a socket. |
| Learn access from a 403, or report it in a response header on every request | A refused write is the worst moment to learn a limit, and a header on every response costs a policy evaluation per action per request. A route asked once is cheap and cacheable for the credential's lifetime. |
| Never install Studio from the CLI | Leaves the first launch to a manual step. Installing is safe only with a signature to verify, which is why it waits for Studio's signed releases. |

### Prior art

Surveyed 2026-10-15 to 2026-10-18 from source, documentation and advisories:

| Tool | Serving and transport | Authentication | Browser defence | Second launch |
|---|---|---|---|---|
| Drizzle Studio | `drizzle-kit studio` serves an API on `127.0.0.1:4983`; the UI is a hosted page calling it | None | None: wildcard CORS and `Access-Control-Allow-Private-Network: true` | Not detected |
| Prisma Studio | Serves its own UI locally and opens a browser | None | Fixed in 7.10 (August 2026, PR 29890) after "a malicious website visited while Studio was open could read or modify the connected database": binds `127.0.0.1`, no CORS, foreign `Origin` refused | Another port |
| Supabase local | Studio and pg-meta in Docker | "Studio, pgMeta (/pg/*), and analytics have no authentication"; binds `0.0.0.0` | None | Finds the running stack by container label |
| Jupyter Server | Serves its UI on `localhost` | Per-launch token; opens the browser on a `0600` file that redirects with the token, which replaced a single-use token in argv (notebook 5.7.3) | `Host` check on loopback; no CORS; foreign `Origin` refused | Runtime files (`0600`, `0700` directory) list running servers |
| Codex | The desktop app runs its own app-server as a child; `codex app` opens it with `codex://threads/new?path=…` after verifying its code signature, and installs it from a signed disk image when missing; the daemon uses a `0600` Unix socket in a per-user `0700` directory | Socket permissions; non-loopback `ws://` requires a token given by file or digest | Any `Origin` refused | Lock plus connect probe; per-thread writer locks are empty files held with `try_lock` |
| Vault | One binary serves both a development form and a deployment: `vault server -dev` and `vault server -config` | The development form generates a root token; a deployment declares its authentication | Development forms bind loopback | Not applicable |
| kubectl, gcloud, gh, git | `kubectl proxy` holds kubeconfig credentials; exec credential plugins, `gcloud auth print-access-token`, `gh auth token` and `git credential fill` hand credentials to other programs; `kubectl config set-context` writes kubeconfig | The tool's own store | `kubectl proxy --accept-hosts` defaults to loopback | Not applicable |
| Ollama, Vite, webpack-dev-server | Local HTTP servers | None by default | `Host` allowlists after DNS-rebinding advisories (CVE-2024-28224, CVE-2025-24010); webpack stopped relying on `Sec-Fetch-*` (CVE-2026-6402) and hardened header parsing (CVE-2026-14631) | Not applicable |

No surveyed database tool authenticates its local UI or defaults to read-only.
This design takes Jupyter's private runtime record and browser defence,
Prisma's fix, Codex's thin launcher, app-owned child process, per-user
directory and lock-based liveness, Vault's one serving command with explicit
audiences, and kubectl's split between shared configuration, written by its own
tool, and credentials obtained through it.

## Evidence and tests

Extend the existing owners in [the testing map](../dev/testing.md):

- **API types, in-source:** every `ErrorCode` documents whether servers send it
  and under which statuses; the operation table binds every route, tool and
  command or declares an exception.
- **Server, in-source `ingress` tests:** every route `build_app` registers is
  classified, enumerated from the router; a local read-only state refuses each
  `Write`-class route before its body, merge `dry_run` included.
- **Server, `integration/auth_policy.rs`:** the local token resolves to the
  local server's actor; every `PolicyAction` has a local-server class, so adding
  one fails the test until it is classified; a read-only local server's engine
  refuses a write even where a route would admit it, and its write routes answer
  `forbidden`; `ReadWrite` refuses without an actor and on an actor-views graph
  before any open; no local state runs open or default-deny; the access route's
  graph-wide, per-branch and source-to-target answers, its refusal to probe a
  hidden branch, and its answers for a static token and a signed credential
  without a policy; `omnigraph-expect-actor` checked after authentication and
  the contract header and before graph lookup, a mismatch answering 412 with no
  effect, a duplicate 400; `outcome` on every error body.
- **Server, `integration/mcp.rs`:** a read-only local server lists no write
  tools and refuses one as unknown; the new tools equal their routes.
- **Server, `boot_settings.rs`:** `serve_local` refuses a non-loopback
  listener, prints nothing to stdout, drains admitted writes on shutdown, and
  returns an uncertain outcome on uncertain completion; serving for others
  keeps every behavior the binary's tests pin, now through `omnigraph serve`.
- **Server, `integration/openapi.rs`:** `openapi.json` regenerated for the
  route, the header, `outcome` and the code registry.
- **Cluster:** a read-only local server of a cluster leaves
  `__cluster/lock.json` untouched; a read-write one holds and releases it as a
  deployment does.
- **CLI, a new `cli_local_servers` integration module** (a new public
  boundary):
  - start, the one stdout line, logs on stderr, and the record; shutdown when
    the line cannot be delivered; the line's `token` present through a pipe and
    absent under a pseudo-terminal;
  - the guard's `Host`, `Origin`, CORS and malformed-header cases; read-only
    refusals; a cluster's local server applying the cluster's policy and stored
    queries;
  - every code in [Outcomes and refusals](#outcomes-and-refusals) that these
    commands make, with `code`, `outcome` and its exit;
  - records: a dead owner's record ignored with no process-ID check; a record
    from another machine ignored and kept; the directory resolved from the
    account database with `HOME`, `TMPDIR` and `XDG_*` pointing elsewhere;
    ownership and mode refusals;
  - election: simultaneous `serve --write --reuse` and `mcp --write` starts for
    one destination ending with one local server; a start whose holder exits
    before publishing taking over; the deadline refusal;
  - destination: two targets with the same root URI on different S3-compatible
    endpoints, and on different Azure accounts, never sharing a local server;
    two names for one store sharing one; an unestablished destination refused;
  - reuse: an actor mismatch refused for `--write` on a graph and for any
    access on a cluster; a read-only server owned by another process refused
    for `--write` with its owner named;
  - `mcp` relaying, hosting, re-resolving after a 401 or a refused connection,
    filtering write tools without `--write`, filling its graph into calls that
    omit it and refusing a call or resource naming another graph, and, from a
    clean environment, reading a standalone graph's data through ad-hoc `read`;
  - `studio` opening its link with the binding digest through a substituted
    opener, never opening it on Linux or Windows without a scheme handler, and
    refusing an unsupported transport.
- **CLI, the installer,** with a substituted verifier and download source: an
  app failing the signature requirement is refused before it opens and before
  it installs; a missing app is downloaded, verified and installed to the first
  writable Applications directory; an override URL is verified the same way;
  Linux prints instructions.
- **CLI, `integration/cli_schema_config.rs`:** `target list` and `show` for
  every kind, with aliases, no secret and no network request, and
  `local_servers` for live, dead and other-machine records; `target add` and
  `remove` keeping the rest of the file's text, never changing the default, and
  refusing a duplicate, an incomplete binding, the default target and a
  legacy-format home; `credential get` over a pipe from
  `OMNIGRAPH_TOKEN_<OWNER>` and the credentials file, with the `KeyedServer`
  support helper, and its three refusals; `access` against a server, a local
  server and a direct target; `version --json` reporting `protocols`.
- **CLI, one failure path:** every command family's failure prints `code` and
  `outcome`, and the exit table holds on every plane, including usage errors at
  64 and managed refusals at 1; `--as` sends `omnigraph-expect-actor` on every
  served request.
- **CLI, `integration/parity_matrix.rs`:** each row's `--json` output equals the
  HTTP body of the same request.
- **CLI, the journeys:** `system_remote` and `cli_cluster_e2e` start
  `omnigraph serve --listen` instead of the server binary, and their support
  module builds one binary.
- **CLI, `skill.rs` drift tests:** command index rows and condensed reference
  entries for the new commands.
- **Engine, `tests/failpoints.rs`:** the child-process actions gain create
  against create, delete against a publisher, cleanup against an in-flight
  create, publish against drop, schema apply and system-column upgrade against
  create, and cleanup against cleanup, each proving which side won and that the
  loser left nothing visible.
- **Engine, `tests/detached_commit_matrix.rs`:** the other-process actor runs by
  default for the control operations, not only under `OMNIGRAPH_MATRIX=full`.
- **DST:** actors that model separate processes get separate write-queue
  registries, so a schedule can reach the interleavings the process-local
  gates hid; today all actors share `for_root`.
- **Engine, `integration/branch_control_cost.rs`:** the added publications per
  create, delete and drop, per backend.
- **Storage upgrade:** the stamp-only route registers every live branch,
  older binaries refuse the new stamp, and the cross-version fences cover it.

Run CLI integration tests with `RUST_MIN_STACK=16777216`. Until the binary
fold lands they also need `omnigraph-server` built first.

## Changes by repository

This RFC is decided and implemented in ModernRelay/omnigraph. Other
repositories' changes are listed as dependencies, made and reviewed there.

### ModernRelay/omnigraph

| Area | Change |
|---|---|
| Engine | Retirement as a tombstone publication on the branch's chain; branch registration and deregistration on main's `__manifest`; drop as a publication on the target branch's chain; cleanup's unregistered-tree rule and a ref-absent-only tree delete in place of Lance's forced delete; the new stamp and its upgrade census; the gates' comments rewritten as contention optimizations; `branch_deleted`, `branch_exists`, `commit_dropped` and `commit_published`. |
| `omnigraph-api-types` | The code registry, `outcome`, the access output and the operation table. |
| `omnigraph-server` | A library only: the deployment entry point made public; `AppState::local` for graphs and clusters, `LocalScope`, `LocalBinding`, `LocalOptions`, `LocalCredential`, `LocalOpenError`; `serve_local` with the lifecycle shared with deployments, returning `ServeOutcome` and printing nothing to stdout; `install_log_subscriber`; the access route; `omnigraph-expect-actor`; `outcome` on error bodies; read-only refusal in `ingress::admit` with `forbidden`; the exhaustive local-server class of every `PolicyAction`; the new MCP tools. |
| `omnigraph-cli` | `serve` (with `--write`, `--reuse`, `--port`, `--json`, and `--listen` for deployments), `studio`, `mcp`, `target`, `credential get` and `access`, each with its `commands/` module and `app.rs` arm; `protocols` in `version --json`; one failure path and the exit table; `--as` as an assertion; the loopback guard; server records in the per-platform directories, reusing the `coordination.rs` home resolution and checks; the MCP bridge with its `readOnlyHint` filter; the platform opener with its scheme-handler check; `queries list` for server targets and `load --json` printing the route's body; later, the Studio installer with code-signature verification. |
| Packaging | The Dockerfile, `docker/entrypoint.sh`, the release, edge, image and package workflows, `scripts/install.sh` and the Homebrew formula ship and smoke-test `omnigraph serve`, and no longer build or install `omnigraph-server`. |
| Docs | The support boundaries in [invariants](../dev/invariants.md); a user page for `serve`, with local servers, serving for others and the interim one-writer-process note; the server operations guide and deployment guide rewritten for `omnigraph serve`; the CLI reference; the server authentication page's self-hosting settings; the MCP guide's `omnigraph mcp`; the README's quick start and clients table, which still lists the retired stdio package; the public embedding APIs in [control plane](../dev/control-plane.md) and the observability guide's note for embedders; [writes](../dev/writes.md) and [recovery](../dev/recovery.md) for the engine phase; new owners in [testing](../dev/testing.md); a disposition note on RFC 0003 for the stdio bridge it deferred; the bundled skill's command index and condensed reference, its rule for which path an agent uses, and its lines saying the cluster server is "the only way to serve" and that there is "no single-graph server mode" (`SKILL.md`, `references/cluster.md`, `references/server-policy.md`); one `changelog.d` note per phase. |

### ModernRelay/omnigraph-studio

| Area | Change |
|---|---|
| Choosing the CLI | Choose the `omnigraph` binary by `version --json`: an exact `http_contract` match, the `protocols` it needs, and an `internal_schema.serves` range covering the graph's storage format; warn before a read-write open when `writes` is newer than the graph's format. Never run an executable a link names. |
| Local servers | Run `omnigraph serve --target NAME --reuse --json` through a pipe, with the interactive login-shell environment captured once; attach to the local server in its line, for a graph or a cluster; stop a child local server on close; treat a vanished local server or exit 2 as an unknown outcome and never resubmit. Offer "Open read-write" as a deliberate action. Show which graphs are open from `target list`'s `local_servers`. |
| Links and saved state | Handle `open?target=<name>&binding=<digest>&home=<digest>[&graph=<id>][&branch=<name>]`, read-only, and accept `target=` on the `graph`, `node` and `query` routes; refuse a link from another operator home or with a binding Studio's own resolution does not match, naming both; drop the unshipped `connect` route. Key saved windows, drafts and agent directories by target or canonical root, not by a local server's random URL. Register the scheme on Linux (`MimeType=x-scheme-handler/omnigraph-studio;`, `Exec=omnigraph-studio %u`) and dispatch `open_urls` on GPUI Linux, which stores the callback without calling it; register it on Windows and forward a second launch's link to the running instance. |
| Connections | Remove the operator-config and credentials parsers (`graph_store/src/cli_config.rs`), the name-based token lookup and Studio's own server list: list targets and aliases with `omnigraph target list --json`, add a server with `omnigraph target add` and `omnigraph login --target NAME` on standard input, and move existing keychain servers the same way once. |
| Static tokens | Obtain them with `omnigraph credential get --json`, keep them in memory, send them only to URLs under `endpoint`, and run it again after a 401. |
| Access | Show only the actions `omnigraph access` or the access route allows, for the target, graph and branch on screen; hide write affordances on a read-only local server. |
| Failures | Read `code` and `outcome` from every error body; never resubmit on `unknown`, and settle it from `outcome.check`. Show writes as degraded, not the graph as gone, on `storage_unavailable` or a degraded readiness report. |
| Agents | Keep the agent gateway for panel agents and point it at the local server's URL and token; say when a read-only local server refuses their branch writes. Create each conversation's CLI home with `omnigraph target add` under that home's `OMNIGRAPH_HOME`, so the CLI writes its format, and refuse `--target` in the agent wrapper as it refuses `--server` and `--as`. Never pass the captured environment to agents. Bring the agent panel to Windows. |
| Releases | Configure the Apple secrets so every release is Developer ID signed and notarized; the workflow already does both when they are set, but releases so far are pre-releases signed ad hoc. A fixed, documented team identifier the CLI can pin; full releases with version-free asset names, so a latest URL resolves for each architecture; an updater inside the app; a package identity when Windows ships as an installer rather than a zip. Installing Studio waits for these ([omnigraph-studio#3](https://github.com/ModernRelay/omnigraph-studio/issues/3)). |
| Specs | Rewrite spec 012's E1–E5 and E15 as this RFC, drop spec 013's note that the launcher passes its CLI path, and update specs 002 (links), 003 (connections), 011 and 013 (agents). |

### The managed control plane

Each change follows the OmniGraph phase that introduces it, when the control
plane moves its pinned OmniGraph version past that phase:

| Phase | What the control plane changes |
|---|---|
| 1, the HTTP contract | Its OmniGraph clients choose the contract per deployment while deployments of several versions coexist, and accept `outcome` in error bodies. Its problem bodies state their `outcome`, so the CLI need not report `unknown` for every managed refusal. Its data hosts route the access route and pass `omnigraph-expect-actor` through. |
| 2, addressing | Its documentation, procedures and tests move from the retired flags, `use` and folder context to targets and the literal-root administration commands, and its managed CLI behavior takes the exit table. |
| 4, the binary fold | Deployments it creates run `omnigraph serve <root> --listen ADDR`, its checks of a serving process accept both forms while older deployments remain, and its images ship the one binary. |
| 5, the engine | Nothing beyond the version move: the new rows live inside each graph's own `__manifest`, so nothing outside a graph's root is written. |

OAuth for standard clients and managed access stay in
[A2](#a2-oauth-for-standard-clients-and-managed-access), tracked in that
repository.

## Rollout

The interfaces land first and are final when they land: they assume rule 2, so
no command, route or output changes when the engine phase ships, which only
adds typed conflict codes to the registry. Until then the one-writer-process
boundary applies as it does today, documented and unenforced, and a read-write
local server counts as one writer process.

Each phase is one or more pull requests titled in Conventional Commits, each
with its single-bullet `changelog.d` note, the public Rust APIs included.

1. **Outcomes, identity and access over HTTP.** The code registry, `outcome` on
   every failure with the three write results and `outcome_unknown`, the exit
   codes, `omnigraph-expect-actor` and the `--as` assertion, and
   `GET /graphs/{graph_id}/access`, under one HTTP contract bump. This is the
   result surface [#1067](https://github.com/ModernRelay/omnigraph/issues/1067) asks for in its sixth step. `implementation: in-progress`.
2. **Targets.** `target list|show|add|remove` with RFC 0059, `credential get`,
   `access`, and `protocols` in `version --json`.
3. **Read-only local servers.** `AppState::local` for graphs and clusters,
   `serve_local`, records, the guard, `serve`, `studio`, `local_servers` in the
   listing, `mcp` with read tools, user docs and the skill.
4. **Read-write local servers and serving for others.** `--write`, `mcp
   --write`, and `serve --listen` replacing the `omnigraph-server` binary in
   images, archives, tests and docs. The user page states the interim
   one-writer-process boundary. Read-write local servers wait for
   [#1067](https://github.com/ModernRelay/omnigraph/issues/1067)'s first change: a storage error ends its own operation, not the
   process, and its trigger is logged.
5. **Cross-process writes.** The engine changes in
   [Cross-process writes](#cross-process-writes); the support boundary is
   lifted and the interim note removed.
6. **Installing Studio**, once Studio's signed and notarized releases, team
   identifier, latest URL and updater exist
   ([omnigraph-studio#3](https://github.com/ModernRelay/omnigraph-studio/issues/3)).
   It can land any time after phase 3. `implementation: complete` follows the
   last of phases 5 and 6.

Phases 1–4 are usable without 5 and 6. Studio's steps follow each phase in its
repository.

## Unresolved questions

1. **RFC 0059's agreement** to the singular `target` group with `add` and
   `remove`, no compatibility interval, and retiring every transport flag at
   once. Without it, the rules hold for the commands this RFC adds, and Studio
   must keep its own server list.
2. **Catching a branch up through a local server.** Whether a merge that
   carries a newer schema needs `branch_merge` or `schema_apply`, which
   [schema apply with live branches](https://github.com/ModernRelay/omnigraph/pull/963)
   leaves open.
3. **Ordering with schema apply with live branches.** Both take a storage
   stamp. Landing them in one stamp saves a format step; landing them apart
   keeps each reviewable. This RFC assumes apart.
4. **Headless managed automation.** Qualification and other automation reach
   the managed service today through environment overrides, without an
   operator home. RFC 0059 decides whether managed targets keep an
   environment-only form; the literal-root exception here covers only storage
   roots.

## Appendix A: changes for separate decisions

These changes came out of the same analysis but have a wider blast radius than
this RFC: they change every client of every server, another author's RFC, or
another repository's security posture. Each needs its own decision. None is
required for the scope above to ship. Cross-process writer safety (#1018) and
credential introspection (#1019), which earlier revisions kept here, are now in
scope as rules 2 and 7.

| | Change | Why it is separate | Recommendation |
|---|---|---|---|
| A1 | `WWW-Authenticate` on every 401 | Changes every protected route's 401 for every client | Issue [#1020](https://github.com/ModernRelay/omnigraph/issues/1020), as RFC 0003's named follow-up |
| A2 | OAuth for standard clients and managed access | Security-sensitive and cross-repository, gated on the managed control plane's own qualification | Control-plane and Studio spec changes; tracking issue [#1025](https://github.com/ModernRelay/omnigraph/issues/1025) |
| A3 | MCP relay for managed targets | Lends the CLI's native managed authority to local agents | Decided after A2, in [#1025](https://github.com/ModernRelay/omnigraph/issues/1025) |
| A4 | Retire the test-shaped constructors | Public API removal with test churn across crates | Issue [#1021](https://github.com/ModernRelay/omnigraph/issues/1021), after `AppState::local` lands |
| A5 | RFC 0059 alignment | Another author's open RFC | [Comments on PR #675](https://github.com/ModernRelay/omnigraph/pull/675#issuecomment-6223864972) |
| A6 | Defects found | Bugs, independent of this design | One issue each, linked in A6 |

### A1. `WWW-Authenticate` on every 401

The server accepts OIDC resource tokens on every protected route but sends the
OAuth challenge only from `/mcp`. Every 401 would carry
`WWW-Authenticate: Bearer resource_metadata="<url>"` with OIDC configured (plus
`error="invalid_token"` when a presented bearer was refused), and a bare
`WWW-Authenticate: Bearer` otherwise, as RFC 9110 §15.5.2 asks. The challenge
would move into the authentication middleware every route shares, so REST and
`/mcp` cannot drift. A client should follow the challenge's metadata URL, or
the path RFC 9728 derives from the resource's URL, and never assume the metadata
sits at the host's root: a deployment may publish it only at
`/.well-known/oauth-protected-resource/<resource path>`.

**Recommendation:** issue [#1020](https://github.com/ModernRelay/omnigraph/issues/1020), as the "server-wide follow-up"
RFC 0003 names, with a short amendment to RFC 0003.

### A2. OAuth for standard clients and managed access

A client other than the CLI should reach an OIDC-enabled server, self-hosted or
managed, as a standard OAuth public client rather than borrow the CLI's
credentials. A managed data token is a bearer credential valid for up to 24
hours that removing admission does not retract (the managed-data guide and
RFC 0053 describe both), and credential families stay separate. The client
flow:

1. Discover the protected-resource metadata as A1 describes; take `resource` and
   `authorization_servers`; refuse an issuer the metadata does not name. The
   resource may be a URL such as the graph host's `/mcp`; the client requests
   exactly that resource and uses the token on REST and `/mcp` alike.
2. Open the system browser with a `client_id` that is a Client ID Metadata
   Document URL, or a client pre-registered with that authorization server, a
   loopback `redirect_uri` (RFC 8252 §7.3), PKCE with S256, a `state`, and
   `resource=<resource>` (RFC 8707). Studio does not use dynamic registration.
3. Exchange the code with its verifier and the resource; keep the refresh token
   in the client's keychain under issuer, resource and account, and the access
   token, at most 300 seconds long, in memory.
4. Refresh with the resource every time; without it an authorization server
   may issue a token for a default audience.
5. On a 401, refresh, then sign in again; a 403 never triggers a sign-in.

The managed control plane would need to enable and qualify a standard OAuth
resource profile, register Studio as a public client with loopback redirect
URIs, expose the protected-resource metadata and `/mcp` on public data hosts,
and start managed servers with OIDC identity trust, which also admits the
resource's host for MCP. A client must not depend on `/openapi.json` being
reachable through every deployment's proxy. Studio would implement the client.

**Recommendation:** control-plane and Studio spec changes, tracked in
[#1025](https://github.com/ModernRelay/omnigraph/issues/1025). No OmniGraph RFC is needed unless the server contract
changes beyond A1.

### A3. MCP relay for managed targets

`omnigraph mcp` could relay to a managed cluster's `/mcp` with the CLI's own
data credential once managed hosts route `/mcp`. The token would never leave the
CLI, but every local agent using the bridge would act with the CLI's managed
authority.

**Recommendation:** decide after A2, as the last item of
[#1025](https://github.com/ModernRelay/omnigraph/issues/1025): whether managed agents should use the bridge or be OAuth
clients themselves.

### A4. Retire the test-shaped constructors

Once `AppState::local` exists, the eleven convenience constructors (`new`,
`new_with_bearer_token`, `new_with_bearer_tokens`,
`new_with_bearer_tokens_and_policy`, `new_with_workload`, `new_single`, `open`,
`open_with_bearer_token`, `open_with_bearer_tokens`,
`open_with_bearer_tokens_and_policy`, `open_single_with_queries`) can be
deleted. Only tests and deferred benches call them, including
`omnigraph-gqt-served`; `open_single_with_queries` also compiles its policy
against the graph URI while the route id is `default`.

**Recommendation:** issue [#1021](https://github.com/ModernRelay/omnigraph/issues/1021), done after `AppState::local` lands.

### A5. RFC 0059 alignment

This RFC asks RFC 0059, at `323068fa`, for three changes, the first in two
parts; none has a reply yet:

| Ask | State |
|---|---|
| The singular `target` group with `list` and `show` | Not added; defined here |
| `target add` and `remove` | Contradicted by its "Add no target administration group"; asked as the one exception, with the reasoning in [Targets](#targets) |
| No compatibility interval | Rejected so far: it keeps legacy adapters for an interval |
| Retiring every transport flag at once, `--server`, `--profile`, `--store`, positional URIs, `--cluster` as an address, `--managed`, `--direct`, `use`, `profile` and folder context | Planned, after its interval |

It already adopted a server target's authentication binding: a token owner
bound to an endpoint scope, or anonymous access.

**Recommendation:** a third comment on
[PR #675](https://github.com/ModernRelay/omnigraph/pull/675) stating the three
changes and pointing at this RFC.

### A6. Defects found

| Issue | Defect | State |
|---|---|---|
| [#1022](https://github.com/ModernRelay/omnigraph/issues/1022) | The CLI sent `OMNIGRAPH_BEARER_TOKEN` to any remote URL, including an unregistered literal one. | Fixed by #1030: the CLI reads no unkeyed token |
| [#1023](https://github.com/ModernRelay/omnigraph/issues/1023) | `commit publish` and `commit drop` passed `--as` to the engine, but the plane guard rejected `--as` for every `commit` command. | Fixed by #1029 |
| [#1024](https://github.com/ModernRelay/omnigraph/issues/1024) | The credentials file's directory is created with default permissions, not `0700`. | Open |
| [omnigraph-studio#2](https://github.com/ModernRelay/omnigraph-studio/issues/2) | The agent gateway checks neither `Host` nor `Origin`. | Open |
| Not filed | `docker/entrypoint.sh` still passes a positional URI, `--config` or `--target` to the server for `OMNIGRAPH_TARGET_URI`, `OMNIGRAPH_CONFIG` and `OMNIGRAPH_TARGET`, none of which the server accepts, so those paths fail at parse time. | Open; the binary fold rewrites the entrypoint |
| Not filed | `--as` help says it has "No effect on remote writes", but served writes refuse it. | Open; [Identity](#identity) replaces both |

## Decision log

- 2026-10-15: First draft, "Studio launcher and local graph sessions", from
  Studio spec 012 (E1–E5) and issue #980, reconciled with PR #850.
- 2026-10-16: Rewritten after checking every assumption against OmniGraph
  main, the managed control plane, Studio and prior art, and consolidated with
  the client-access design. The connect code, handoff files, redemption route
  and attach key became session records; the session became a general local
  serving primitive; Studio became a standard OAuth client instead of borrowing
  managed data tokens; static tokens stayed first-class through the CLI's store.
- 2026-10-16: Narrowed to what ships alone. The cooperative writer lock was
  removed; only the record's liveness lock remains, and cross-process writer
  safety moved to A1. The launcher became thin, as `codex app` is: it opens
  `open?target=` and exits, Studio owns its sessions through `serve --reuse`,
  and links open read-only. The access route, the server-wide challenge, OAuth
  and managed access, the managed MCP relay, the constructor removal and the
  RFC 0059 additions moved to Appendix A with a recommended RFC or issue each.
- 2026-10-16: Linked the Appendix A issues and the comment on PR #675, and
  described the managed control plane by what it must change rather than by
  its internal specs.
- 2026-10-16: Added installing Studio as phase 5, following the Codex desktop
  launcher and gated on Studio's signed, notarized releases.
- 2026-10-17: Reconciled with main and open pull requests. #1030 and #1029
  fixed two A8 defects, and static tokens follow #1030's owner-keyed rule. The
  commands follow #1047's structure and #1031's version fields. The target
  listing and writer moved here from A7 as a deliberate exception to RFC 0059,
  which has not answered; panel agents keep Studio's gateway. Added the binding
  digest on links, stderr-only logs, exhaustive action classes, actor-views
  graphs (#951), the merge `dry_run` refusal, the schema note for #963, `--as`
  under `--session`, and the CLI choice Studio makes.
- 2026-10-17: Checked against best practice and for one vocabulary across
  clients. A session became a ***local server*** and `--session` became
  `--via-server`, because "session" already had three meanings. Records moved
  out of `/tmp`, which macOS cleans after three days, into per-user directories
  resolved from the account, and gained the machine they belong to. No command
  writes a token to a terminal. `targets --json` lists live local servers. Each
  place an agent runs has one write path, and the notice to a direct writer
  gives the exact `--via-server` command. Added platform coverage, one refusal
  table with a CLI-local `refusal` field, `forbidden` for read-only and guard
  refusals so the HTTP API gains no code, and the Studio link check for a scheme
  handler. The file keeps its name so existing links resolve.
- 2026-10-18: Redesigned from first principles around one idea, that a person
  names what they act on and the path, credential, actor and process follow,
  and seven rules. Retitled "Graph access, serving and cross-process writes"
  and renamed the file to match. Serving became one concept with explicit
  audiences: a local server serves a standalone graph or a cluster's applied
  state to you, `--listen` serves a cluster to others, and the
  `omnigraph-server` binary folds into `omnigraph serve`. Cross-process writer
  safety (#1018) and the access route (#1019) moved from the appendix into
  scope, with the engine design resting on the `__manifest` version CAS
  because Lance's ref updates and deletes are unconditional. `--via-server`
  and its notice went, since rule 2 makes them unnecessary; the interfaces land
  first, and the engine phase last, with today's one-writer-process boundary
  documented until then. Added the operation table, the actor precondition
  header, one code registry with `outcome` on every failure, and one exit code
  per meaning. `targets` became the singular `target` group and `credential`
  became `credential get`.
- 2026-10-18: Checked against the managed control plane. Deployments keep the
  command and HTTP contract they were created with, so the binary fold and the
  contract bump each need per-deployment handling there; managed failures keep
  the control plane's own problem body; deployment tooling keeps a literal root
  for the root-scoped cluster administration commands.
- 2026-10-18: Aligned with [#1067](https://github.com/ModernRelay/omnigraph/issues/1067), the server stopping on one storage
  write of unknown outcome. Its result surface became part of rule 6
  (`outcome_unknown` with `outcome.check`, `storage_unavailable`); the serving
  lifecycle is inherited from it rather than promised, and read-write local
  servers wait for its first change; the engine design states the late-request
  classification it proposes, and why branch-ref rewrites fall in its last
  class.
- 2026-10-18: Addressed aaltshuler's review of `557d0f24`. `serve` elects one
  local server per destination with a start-only election lock, since two
  racing read-write starts would be two writer processes until the engine
  phase. A local server is identified by its destination, the root with the
  binding's non-secret store settings, not by its root URI. Reuse refuses an
  actor mismatch. Studio stops its own read-only child to enable writes and
  never upgrades another process's. Links carry the operator home's digest, so
  a link from another home is refused. The MCP bridge is bound to its graph,
  and every local server enables ad-hoc MCP reads.
