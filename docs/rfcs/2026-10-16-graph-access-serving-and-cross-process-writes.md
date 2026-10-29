---
rfc: "2026-10-16-graph-access-serving-and-cross-process-writes"
title: "Graph access and cross-process writes"
track: maintainer
status: accepted
implementation: in-progress
authors:
  - ragnorc
created: 2026-10-16
updated: 2026-10-29
discussion: https://github.com/ModernRelay/omnigraph/pull/1017
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Graph access and cross-process writes

> A term set in ***bold italics*** is defined at that spot; it is plain text afterwards.

## Summary

This RFC decides consistent graph access for people, agents and programs:
named targets, explicit actor assertions, one operation and failure contract,
and engine correctness across writer processes. It uses the accepted
[RFC 0059](https://github.com/ModernRelay/omnigraph/blob/main/docs/rfcs/0059-config-cli-coherency.md)
target model without extending its grammar or configuration.

**There is one serving model: the ordinary cluster-backed server, including on
localhost.** Its applied cluster revision owns graph inventory, policy, stored
queries and provider bindings. This RFC adds no standalone-graph HTTP mode,
local-server boot path, generated policy or token mode, process registry,
automatic hosting, or replacement for the `omnigraph-server` binary. Direct CLI
and embedded access remain independent of HTTP serving.

The design has six rules:

1. **Name the target, never the transport.** CLI operations use RFC 0059's
   `--target` resolution and command applicability rules.
2. **Correctness lives in the engine, across processes and machines.** Branch
   creation and deletion, dropping a proposal and cleanup decide through the
   `__manifest` version compare-and-swap. This is an engine phase with its own
   evidence and storage-format gates, not a reason to add another server.
3. **One set of operations, three surfaces.** CLI, HTTP and MCP share operation
   contracts where applicable, with explicit exceptions.
4. **Identity is fixed at the boundary.** `--as` declares the actor on direct
   access and asserts the credential's actor on served access.
5. **One failure model.** Servers and the CLI share error codes and a
   `disposition` object; exit codes follow the disposition.
6. **Introspection is a command.** RFC 0059's `target list` describes configured
   destinations; `access` reports the caller's actor and allowed actions.

A stdio MCP bridge may connect to an existing server's `/mcp`, and a Studio
opener may select an existing server target. Neither starts a server. Static
tokens remain available through RFC 0059's `credential get` client contract.
The engine phase adds control facts under the next storage stamp; the client
and HTTP phases change no graph storage format.

**2026-10-20 disposition:** the second serving model proposed in earlier
revisions is discontinued, including the work in
[#1075](https://github.com/ModernRelay/omnigraph/pull/1075),
[#1077](https://github.com/ModernRelay/omnigraph/pull/1077) and
[#1083](https://github.com/ModernRelay/omnigraph/pull/1083), now closed
unmerged. The original local-serving issue
[#980](https://github.com/ModernRelay/omnigraph/issues/980) is closed as not
planned. The independent access, identity, disposition, operation-parity,
client-connection and engine work was accepted on 2026-10-20. Historical
serving decisions in the decision log are superseded by this disposition; they
are not implementation requirements.

## Motivation

The original problem survey used OmniGraph main (`794cebab`), the managed
control plane and OmniGraph Studio between 2026-10-16 and 2026-10-18. Later
accepted decisions and landed work are named where they change that baseline.

**A caller picks a mechanism instead of naming a graph.** The CLI's help lists
about eight ways to address one: `--server`, `--profile`, `--store`, a
positional URI, `--cluster`, `--managed`, `--direct` and implicit folder
context. Each changes the path, the credential and sometimes the actor. RFC 0059
replaces them with named targets in one breaking migration; its design was
accepted in [#675](https://github.com/ModernRelay/omnigraph/pull/675), and
[#1090](https://github.com/ModernRelay/omnigraph/pull/1090) implemented it on
main.

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

**Clients need a consistent connection contract.** Studio and MCP clients use
HTTP rather than opening storage. The existing cluster-backed server already
provides that boundary, whether it listens on localhost or a remote host.
Client discovery and credential support should connect to it without creating
a second boot, policy, admission or deployment model.

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

**Clients learn their limits from refusals.** Until
[#1073](https://github.com/ModernRelay/omnigraph/pull/1073) added
`GET /graphs/{graph_id}/access`, no route said which actor a credential
resolves to or which actions it may take, so a client showed every action and
learned from a 403. That is the common case for self-hosted static tokens
without a graph policy, and the CLI still has no command that asks.

**Clients keep their own registries, and they drift.** Studio parses the CLI's
`config.yaml` and `credentials` file itself, finds a token by server name where
the CLI sends one only for a URL under a declared server (since #1030), and
keeps a second list of servers it adds, with tokens in its own keychain. RFC
0059 replaces the configuration format. A GUI launched from Finder sees
launchd's environment, not the shell's, so environment tokens are invisible to
it.

**Desktop agents need a stdio client path to an existing server.**
[RFC 0003](0003-mcp-server-surface.md) makes the server's `/mcp` the only MCP
implementation and defers a stdio bridge "only if local desktop clients need
it". A bridge can satisfy that need while leaving tools and serving authority
at the existing server.

**Static tokens must stay first-class.** They are how people host their own
graphs: no identity provider and no admission-snapshot publisher.

This needs an RFC: it changes engine control protocols and the storage stamp,
adds client commands and HTTP contracts, and proposes lifting the engine's
single-writer-process control boundary. It does not change the cluster-only
serving boundary or the server binary.

## User and operational behavior

### Commands

```text
# Where things are, and who I am there
omnigraph target list|show|add|remove|import [NAME]   # RFC 0059
omnigraph login|logout --target NAME
omnigraph credential get --target NAME          # RFC 0059: a static token, to a pipe only
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

# Connecting other programs to an existing server
omnigraph studio --target NAME
omnigraph mcp    --target NAME [--write]
```

The tree is abridged: `snapshot`, `changes`, `graphs`, `init`, `upgrade`,
`repair`, `lint`, `queries`, `policy`, `embed`, `alias`, `skill` and `version`
keep their places. The data, history, schema, upkeep and cluster commands exist
today; this RFC changes how they are addressed, how `--as` behaves on them and
how they report failure. `access`, `studio` and `mcp` are new here,
`target` and `credential` are RFC 0059's; `index rebuild` is today's `rebuild-full-text-indexes`; `use`
and `profile`, which select and inspect RFC 0059's predecessors, retire with
them. Groups are singular nouns followed by a verb, as `branch list` already is.

### Addressing

RFC 0059 owns addressing, and this RFC adds nothing to it. Every command
selects its destination the same way: `--target NAME|ADDRESS`, else
`defaults.target`, else an error. A name selects a configured target. An inline
address, a path, a storage URI or a server endpoint, is a complete binding
written on the command line, and a root-only command reads an inline storage
address as a root. `--graph ID` selects a graph within a root or a server, else
the target's `default_graph`, and `--branch NAME` selects a branch where the
operation takes one. A direct graph target already names its graph and rejects
`--graph`.

Legacy destination selectors and folder routing retire according to RFC 0059;
its explicit operands such as `init URI` remain. The operator-home migration
and refusal rules also belong to that RFC.

Root-scoped CLI administration uses the same target resolver. The existing
`omnigraph-server --cluster ROOT --bind ADDR` boot contract is unchanged by
this RFC; targeting client commands does not add a second server launcher.

### Targets

RFC 0059 owns `target list`, `show`, `add`, `remove` and `import`, together with
`credential get`. The CLI remains the operator configuration's only program
writer. Studio and terminal agents use the same configured server destinations
and credential resolution rather than keeping a second list or token store.

`target list --json` and `target show NAME --json` describe configured targets
without secrets or network requests. This RFC adds no `local_servers` field,
process discovery, liveness check or server record to those outputs. A direct
target describes direct storage access; it does not imply an HTTP endpoint.
A client that needs HTTP selects a server target configured for an existing
ordinary cluster-backed server.

### One server model

The existing `omnigraph-server` boots from applied cluster state. A cluster may
contain one graph and listen on localhost, with the same authentication, policy,
routes, admission, deployment and shutdown behavior as any other cluster.
This RFC preserves that model and its packaging. It introduces no `serve`
command, alternate embedding API, generated standalone policy, automatic token,
or implicit adoption of an existing graph into a cluster.

Standalone graphs remain available through direct CLI or embedded access.
Studio and the MCP bridge refuse direct targets before any graph open or server
startup. Making a standalone graph available through HTTP requires the ordinary
explicit cluster deployment workflow; this RFC creates no shortcut around it.

| Target kind (RFC 0059) | `studio` | `mcp` | `credential get` |
|---|---|---|---|
| Direct graph, root or catalogue | Refused: HTTP requires a configured server target | Refused: the bridge connects to an existing server | Refused |
| Server with a token owner | Opens Studio on that server | Relays to its `/mcp` with the owner's token | Returns the owner's token |
| Server with anonymous access | Opens Studio on that server | Relays without a token | Refused (`credential_anonymous`) |
| Server on a Unix socket | Refused (`studio_transport_unsupported`): Studio's client speaks TCP | Relays over the socket | Returns the owner's token if authenticated |
| Managed cluster | Deferred to [A2](#a2-oauth-for-standard-clients-and-managed-access) | Deferred to [A3](#a3-mcp-relay-for-managed-targets) | Refused: managed credentials stay the CLI's |
| Managed account | Refused | Refused | Refused |

Unsupported kinds refuse with `target_kind_unsupported`; the dedicated
transport and credential refusals apply where the table names them. No refusal
silently changes the target kind or starts a process that serves storage.

### Cross-process writes

Any number of processes, on one machine or many, may run any operation on one
standalone graph. Each operation either publishes once or refuses with a typed
outcome; an operation that lost to another reports it, and the caller retries
from a fresh view. No command routes a write through a particular process and
no client needs to know which processes are running.

**Until the engine phase ships,** today's boundary applies unchanged: schema
apply, branch creation and deletion, and cleanup are supported from one writer
process. Clients of a server continue to use its routes; direct storage access
does not bypass the existing cluster admission and support boundaries.

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
| Byte streams, which MCP tools do not carry | Blob payloads, export streams and the change-feed baseline, an NDJSON snapshot whose cursor is valid only after the snapshot is installed; `blob stat` is a tool |
| Deployment control, which is not an agent tool | `/cluster/*` over MCP |
| Process probes, not graph operations | `/healthz` and `/readyz` in the CLI |

Gaps on main that this closes:

| Gap | Change |
|---|---|
| MCP has no commit diff, commit publish or drop, branch diff or merge-base tool, and no `blob stat` or `blob clear` | Add the tools; write tools stay behind `--mcp-allow-writes` |
| MCP refuses stored mutations | Admit them as write tools behind `--mcp-allow-writes`: `mutate` with a stored mutation's `name` and no `query`, through the stored-query routes and their preconditions |
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
  and `--as` becomes an assertion. The CLI sends `Omnigraph-If-Actor: <actor>`,
  and the server compares it with the resolved actor after authentication and
  the contract-header check (on `/mcp`, which has no contract check, after
  authentication and before admission), before graph lookup, body decoding or
  effects. A mismatch is `412 actor_mismatch`: as with HTTP's `If-*` headers,
  whose naming it follows, a condition the caller set did not hold, and nothing
  ran. A duplicate, padded, empty or malformed header is `400 bad_request`. The
  header arrives with the contract bump in [HTTP API](#http-api), so a server
  that would ignore it refuses the request's contract first; no separate
  capability header advertises it. Without `--as` the CLI sends no header:
  `operator.actor` names the direct actor and is not asserted against servers,
  whose tokens may resolve to other names. The refusal of `--as` on served
  targets, which the help text now states, goes.

An assertion can only narrow: a matching header changes nothing, and no client
supplies an actor the server trusts. `--as` is never ignored.

### Dispositions and refusals

Every failure, from a server or from the CLI, has one shape: today's
`ErrorOutput` (`error`, `code` and its typed details) plus a `disposition`
object.

```json
{"error":"the credential's actor does not match the asserted actor","code":"actor_mismatch",
 "disposition":{"effects":"none","action":"fix"}}
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
  `disposition` is the one the control plane states; where it states none, the
  CLI reports `effects: none` only if no request was sent and `unknown`
  otherwise, because it never infers whether a remote refusal had effects.
- **One disposition object.** `effects` says what the operation changed:
  `none`; `partial`, when an earlier step of the same operation published and
  the failing step wrote nothing; or `unknown`. `action` says what to do next:
  `fix` the request, `refresh` from the current state, `retry` the same
  request, `recover` what the error names, or `reconcile` an unknown effect
  before writing again. Each code names the action its refusal asks for, so a
  client never reads it from the status. A server includes the disposition on
  every error response, computed where the knowledge is. The CLI combines it
  with transport evidence, as `command_outcome` does today for data writes, so
  a lost response becomes `unknown` and `reconcile` and is never replayed.
  Every command reports it, not only data writes. A separate field for whether
  the operation ran would only repeat `effects`.
- **A write has three results, which
  [storage outcomes per operation](https://github.com/ModernRelay/omnigraph/pull/1093)
  defines.** It committed; it did not commit, because it was refused before any
  effect, lost its conflict or failed before its manifest request could land;
  or its outcome is unknown, because its manifest request got no definite
  answer. This RFC owns only how they are reported. A write the server could
  not settle answers `outcome_unknown`, never a plain `internal`, and later
  writes to its branch proceed as [§5 of storage outcomes per operation](https://github.com/ModernRelay/omnigraph/pull/1093) decides: a linear publication's
  version key fences it, so the branch's next publication either builds on the
  late commit or takes its version first, and a detached proposal or
  unattached snapshot settles as that section says, not by the next linear
  version. Every other error keeps its code, and its disposition follows from
  what the server knows:

  | Error | `effects` | `action` |
  |---|---|---|
  | Before the request submitted a write | `none` | the code's: `retry` when busy or unavailable, `fix` when the request itself is wrong, `recover` when something must be rebuilt, else `refresh` |
  | A refusal the write met before it started (authentication, contract, actor assertion, policy, admission, a precondition), or a typed refusal of its one publication | `none` | the code's |
  | Any of those after an earlier publication of the same operation, such as the fork a load makes before its rows, which the engine marks on its error | `partial` | the code's |
  | Any other error after a write | `unknown` | `reconcile` |
  | An attempt the server could not settle (`outcome_unknown`) | `unknown` | `reconcile`, `recover` when recovery is required |

  A later phase adds `disposition.check`, what a later read needs to settle an
  unknown write: its branch and its publication id (a graph commit id, or a
  control fact's attempt id), with the branch's head before the write. The
  outcome is final once the attempted `__manifest` version is readable, by the
  [read-back](#engine-control-facts-on-the-version-cas) rule:
  the write landed if it holds the publication id, and lost if it holds
  another, since a create-if-absent cannot land once its version is held. The
  check cannot see whether the store acknowledged the attempt; the in-doubt
  result of an acknowledged attempt stays on the publisher's read-back, which
  holds the request record. While that version is absent the outcome stays
  unknown, until some commit holds it. It
  arrives with the attempt identity of [cross-process
  writes](#cross-process-writes). Under an actor view that hides the commit
  listing, the check answers for the caller's own publication id and returns
  only the caller's own receipt. Until then a caller settles a row publication
  from the branch it sent and that branch's head, and a control fact from the
  registration, tombstone or drop record it names, since a control fact does
  not move the head. Retrying one is safe: a create refuses with
  `branch_exists`, and a delete or a drop converges. The body below is that
  later phase's:

  ```json
  {"error":"the write may have published","code":"outcome_unknown",
   "disposition":{"effects":"unknown","action":"reconcile",
                  "check":{"branch":"main","head_before":"01JAC…","attempt":"01JAD…"}}}
  ```

- **Exit codes follow from the disposition**, the same for every command:

| Exit | Meaning | Today |
|---|---|---|
| 0 | Success | The same |
| 1 | No effects: a refusal, a cancelled queued deployment, or an error before any effect | Also managed `failed`; managed refusals are 2 and cancellation 6 |
| 2 | Effects unknown, or partial because an earlier step published: read the target, and reconcile or recover before retrying. Also a server that stopped with an uncertain write | Also usage errors and managed refusals; managed `recovery_required` is 4 |
| 3 | Completed in part, and the result says which part | Managed partial convergence and partial organization results |
| 4 | A condition the caller set did not hold: a Blob precondition, a conditional publish, an `--as` assertion | Also managed `recovery_required` |
| 5 | A bounded wait ended; the work continues | Also managed `stalled` |
| 64 | Usage error (`EX_USAGE`) | 2, Clap's default |
| 75 | Busy; retry after `Retry-After` (`EX_TEMPFAIL`) | The same |

Each exit has one meaning on every plane. Cluster commands keep reporting
`Diagnostic` findings inside their results; a failed command still ends with
the one envelope.

The new codes, with the target and credential codes RFC 0059 defines, since the
registry holds every code:

| Code | Commands | When |
|---|---|---|
| `legacy_operator_config` | All | The operator home has the legacy format |
| `target_kind_unsupported` | `studio`, `mcp`, `credential get` | The command does not support the target's kind |
| `actor_mismatch` | Every served command with `--as` | The asserted actor differs from the credential's |
| `studio_unavailable` | `studio` | Studio is not installed, or nothing on this platform handles its links |
| `studio_transport_unsupported` | `studio` | The server target is on a Unix socket |
| `credential_terminal` | `credential get` | Stdout is a terminal |
| `credential_anonymous` | `credential get` | The server target uses anonymous access |
| `credential_missing` | `credential get` | No token is stored for the target's owner |
| `target_invalid` | Target-resolving commands | The entry fails RFC 0059's resolver |
| `target_exists` | `target add` | The name is taken |
| `target_unknown` | `target show`, `target remove` | No target has the name |
| `target_is_default` | `target remove` | The target is `defaults.target` |
| `config_not_editable` | `target add`, `target remove` | The entry cannot be changed while keeping the rest of the file's text |
| `branch_exists` | `branch create` | Another process registered the name first |
| `branch_deleted` | Every write to a branch | The branch was deleted while the write prepared |
| `commit_dropped` | `commit publish` | The proposal was dropped first |
| `commit_published` | `commit drop` | The proposal was published first |
| `outcome_unknown` | Every write | The server could not settle whether the attempt published |

### Access

[#1073](https://github.com/ModernRelay/omnigraph/pull/1073) added the route
at contract `0.13`. Its `AccessOutput` is `actor`, `graph_id`, `graph_wide`,
`branch`, `target_branch` and `actions`. This RFC adds the command, whose
output adds `protocol`, `target` and `source`, and the answer for direct
targets. `omnigraph access --target NAME [--graph G] [--branch B]
[--target-branch T]` says who you are at a target and what you may do:

```json
{"protocol":1,"target":"staging","graph_id":"knowledge","actor":"act-me","source":"server",
 "graph_wide":["read","export"],"branch":"main","actions":["read","export","change"]}
```

- **Server targets** answer through
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
  still refuse a permitted `change`. [Actor views and subgraph
  sharing](https://github.com/ModernRelay/omnigraph/pull/951) lists the route
  and the command among its surfaces and fills the reserved `view` field with
  facts about the caller's own view only.

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
  scheme. On Linux the CLI launches that handler itself: it finds the desktop
  entry `xdg-mime query default x-scheme-handler/omnigraph-studio` names
  under `$XDG_DATA_HOME` and `$XDG_DATA_DIRS`, and runs the entry's `Exec`,
  without a shell, with the link in place of `%u`. It never runs `xdg-open`,
  whose generic path, with no display set, skips the registered handler and
  hands the link to a web browser. On Windows it requires the scheme's
  registry association. Without Studio or a handler it prints the
  link and how to install Studio, and refuses (`studio_unavailable`), until
  [installing Studio](#installing-studio) ships. It never opens a browser.
- With `--json` it prints
  `{"protocol":1,"target":"staging","binding":"sha256:…","home":"sha256:…","link":"omnigraph-studio://open?target=staging&binding=sha256:…&home=sha256:…"}`.
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
- Studio accepts an existing server target and obtains its static credential
  through `omnigraph credential get` when authentication is configured. It
  never opens graph storage or starts a server, and a direct target refuses.
- A link opens without executing a write, as Studio's own link rules require.
  Write affordances follow the server's access response and the person's
  explicit action; opening a link grants no authority or server mode.
- **Environment.** Studio passes the environment of an interactive login shell
  (`$SHELL -i -l`), captured once, to the CLI resolver and credential commands
  it runs, and to no agent. This makes owner-keyed environment tokens available
  to the client without copying credentials into Studio's own store. Storage
  and embedding-provider credentials belong to the existing server's process
  and applied cluster configuration; Studio does not launch that process.

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

The client commands have the same contract on macOS, Linux and Windows.
Studio's platform integration does not yet:

| | macOS | Linux | Windows |
|---|---|---|---|
| `target`, `credential get`, `access`, `mcp` on a server target | Supported | Supported | Supported |
| Studio opens a link from `omnigraph studio` | Supported | Once Studio registers its scheme and GPUI's Linux platform dispatches opened links | Once Studio registers its scheme and forwards a second launch's link to the running instance |
| Studio's agent panel | Supported | Supported | Not yet available, as Studio's README says |
| Installing Studio | A verified, signed disk image | Prints how to install | Opens Studio's installer once one ships |

Until Studio's rows land, `omnigraph studio` on Linux and Windows prints the
link and refuses with `studio_unavailable`, and agents there use the CLI and
`omnigraph mcp`. Each gap is a row in
[the Studio changes](#modernrelayomnigraph-studio).

### MCP for agents

`omnigraph mcp --target NAME [--graph G] [--write]` speaks MCP over stdio for
agents that start their tools as child processes, such as Claude Code or Codex
in a terminal. It connects to an existing server target:

```json
{"command": "omnigraph", "args": ["mcp", "--target", "staging"]}
```

- **Server target.** Relays to the server's `/mcp` with the owner's stored
  token, which never reaches the agent's configuration, or without a token
  when the target explicitly selects anonymous access. Unix-socket server
  transport follows the existing client transport contract.
- **Direct target.** Refuses with `target_kind_unsupported`. The bridge never
  opens storage, hosts HTTP, starts a server or discovers one from a process
  registry. Managed targets remain deferred to A3.
- **A 401** permits one credential re-resolution for subsequent requests. It
  does not replay a tool call whose effects may have occurred. A refused
  connection or continued authentication failure is a typed failure for the
  selected endpoint, never a reason to auto-host a replacement.
- **Bound to one graph.** The bridge resolves one graph as every command does:
  `--graph`, else the target's `default_graph`. It fills the `graph` argument
  of each tool call that omits it, refuses with a JSON-RPC error a call or
  resource URI naming another graph, and lists only its graph. An agent that
  needs several graphs is configured with one bridge per graph.

Write tools need two consents. The existing server is the ceiling: without
`--mcp-allow-writes` it lists no write tools, and its applied policy still
controls every call. The agent's configuration is the other: without `--write`,
the bridge lists and admits only tools the server annotates `readOnlyHint`.
Opening a graph in Studio does not give a connected agent write access. Tool
logic, refusals and the write contract stay at the server; the bridge adds no
tool and no serving mode.

**Agents inside Studio keep Studio's gateway.** The gateway limits a panel
agent's writes to the agent's own branch and can ask the person before each
write; `omnigraph mcp` does neither. Studio points the gateway at the existing
server endpoint and credential. A gateway filter is not server-enforced
delegation; delegated, branch-scoped credentials remain the separate work in
[#978](https://github.com/ModernRelay/omnigraph/issues/978).

### Which path an agent uses

The bundled skill states one path per place an agent runs:

| Where the agent runs | Its path |
|---|---|
| A terminal, using the CLI | Commands with `--target`, under the target kind's existing support boundaries |
| A terminal, using MCP | `omnigraph mcp --target NAME`, connected to an existing server target |
| Studio's agent panel | Studio's gateway connected to the existing server |

An agent finds configured destinations and its own access through
`omnigraph target list` and `omnigraph access`. These commands do not discover
or start serving processes. The engine's existing writer-process boundary
remains until the cross-process phase passes its qualification.

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

A person hosting their own graphs runs the existing
`omnigraph-server --cluster ROOT --bind ADDR` with static tokens and adds that
server as a target. Four settings decide how complete a client's experience is:

| What the person wants | What the deployment needs |
|---|---|
| Edits attributed to people | Named actors through `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON`, `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` or `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET`. A single `OMNIGRAPH_SERVER_BEARER_TOKEN` is the actor `default`. |
| Edits at all | An applied graph policy granting the actor its write actions. Without one a static token only reads, which `omnigraph access` now shows before the first refused write. |
| Listing the deployment's graphs | `graph_list` in the cluster policy. Without it a client opens the target's `default_graph` or the graph the person names; `/graphs/discovery` serves verified identities only. |
| Agents over MCP | `--mcp-allowed-host <public host>`, because `/mcp` admits only loopback `Host` values by default (OIDC identity trust admits its resource's host by itself), and `--mcp-allow-writes` for write tools. |

## Design

### Engine: control facts on the version CAS

Checked against main and Lance 13.0.0's source on 2026-10-19.

**What already holds across processes.** Every `__manifest` publication is an
`Operation::Overwrite` committed with zero retries. Lance writes the next
version only if it is absent: a hard link that fails on an existing target on
local disks, `If-None-Match: *` on S3 and Azure. A concurrent commit therefore
loses with `CommitConflict` instead of being rebased. The publisher maps that to
`RowLevelCasContention` and reloads. On every attempt,
`check_publish_precondition` rechecks its precondition (`ExactGraphHead`,
`ExactGraphVersion` or `ExactCandidate`) and its expected table versions, and
the loop retries up to `PUBLISHER_RETRY_BUDGET`, five times, six attempts in
all. A precondition that no longer holds ends the attempt with
`ReadSetChanged`; nothing re-parents silently. Ordinary writes, loads, merges,
optimize and index maintenance rest on this, and their in-process gates only
save wasted staging. The module says so: "cross-process writers on one graph
remain one-winner-CAS at publish". A loser's detached staging is unreachable and
collectible, as invariant 5 requires. Schema apply against a writer or another
apply is already decided the same way. Optimize's config commit that strips
stale auto-cleanup configuration (`Dataset::update_config`) also moves
`__manifest`'s version, so it can only make a concurrent schema apply refuse.

Two facts of the publisher shape the fix. A publication that changes rows must
record a graph commit (`publish_with_precondition` refuses one without a
lineage intent), so the head commit is the state of the `table` rows. And the
preconditions read the branch binding, its native branch identifier and the
head commit id, nothing else: a version that keeps the head passes them.

**What does not.** The rest of a branch's state lives in Lance refs, and Lance
gives refs weaker guarantees. In Lance 13, `Branches::create` is put-if-absent,
but `replace_metadata` reads the ref and puts it back with no version check,
`delete` removes it unconditionally, no Lance commit reads a ref, and object
stores offer no conditional delete. Local storage has no update-if-unchanged
primitive at all (`PutMode::Update` returns `NotImplemented`), so the one
compare-and-swap that works on every backend is the one manifests use: a
numbered object written if absent. Five races depend on the process-local gates
(`WriteQueueManager::for_root`, a process-wide registry, and the schema gate,
whose exclusive side branch creation takes):

| Race | What happens today across processes |
|---|---|
| Branch delete beside a publisher | The publisher passes its precondition, the delete retires, archives and unlinks the ref, then the publisher's commit lands in the retired tree. It acknowledges a write no live branch shows, and the branch's settled history lacks it. |
| Two creates of one logical name | Each mints its own `{logical}.{ULID}` ref, so neither put-if-absent conflicts. Two live incarnations result, and every resolution of the name fails until someone intervenes. |
| A create during schema apply | Apply checks that main is the only branch, a create in another process adds one without moving main, and apply's `ExactGraphVersion` still succeeds. The new branch keeps the old contract, and merging it fails. System-column upgrade has the same window. |
| Cleanup during a two-phase create | Lance writes the new tree, then its ref. Cleanup can classify the tree as ref-less, and its forced Lance delete can remove the tree, and the ref if it has landed. `reclaim_ref_absent_tree` says the single-writer gate is what stops a create between its last ref read and the delete. |
| Publish beside drop of one proposal | Drop releases the proposal's tags and deletes its tag object; publish's `ExactCandidate` never reads the tag. Both acknowledge, and a merge proposal's released source tag can let a third process's cleanup sweep a pin the publish then names. |

Each fix moves the deciding step onto a `__manifest` chain's version CAS through
one new kind of row.

**Control facts.** A ***control fact*** is a `__manifest` row that changes no
`table` row and no schema contract, and so records no graph commit: a branch's
tombstone on its own chain, a branch's registration on main, and a proposal's
drop record on its target branch's chain. The row-change rule becomes: a
publication that changes `table` rows or the schema contract records a graph
commit, and one that writes only control facts records none. The publisher
loads a chain's control facts with its other rows on every attempt, and
`check_publish_precondition` fails closed, whatever the precondition, when a
fact names the attempt's branch, name or candidate:

| Fact | Chain | An attempt refuses when | Refusal |
|---|---|---|---|
| Tombstone | The branch's own | Its chain tombstones its native branch | `branch_deleted` |
| Registration | main | It registers a logical name main already registers | `branch_exists` |
| Drop record | The target branch's | It publishes a candidate a drop record names | `commit_dropped` |

A fact that does not name the attempt leaves it unaffected: a registration costs
main's writers a reload within their retry budget, since their head did not
move, and a drop record costs the branch's other writers the same. A check made
only before the first attempt would leave open the window the facts close, so
the check runs where the precondition already runs, on every attempt.

**Each control fact carries its attempt id.** A control-fact operation mints one
***attempt id***, a ULID, before its first publish attempt, and every fact it
writes records it. The attempt id is a control fact's identity, as the graph
commit id is a row publication's:

- **Read-back.** After a lost acknowledgement, the publisher settles the
  attempt as [#1092](https://github.com/ModernRelay/omnigraph/pull/1092) splits
  a row publication's. A request its request record shows could not have
  landed lost: one never sent, or one the store refused before it could apply.
  Otherwise the publisher reads the attempted version, and a readable version
  is final. It landed if the version holds a fact with the attempt id. It lost
  if the version holds another fact and the store never acknowledged this
  attempt; after an acknowledgement it stays unknown, as the publisher reports
  an acknowledged attempt in doubt. An absent version stays unknown until some
  commit holds it, and a failed read until a read succeeds. The row read-back
  compares heads, which cannot tell two control facts apart, since neither
  moves the head.
- **The owner's retry.** A fact with the operation's own attempt id is its own
  landed attempt, never a refusal: the operation goes on to its follow-ups,
  the ref, the deregistration, the archive and unlink, or the tag release.
- **Unknown outcomes.** Every control fact is a linear publication on its
  chain, so one whose request stays ambiguous is fenced by its version key, as
  a linear row publication is in [§5 of storage outcomes per operation](https://github.com/ModernRelay/omnigraph/pull/1093): the chain's next publication either
  loads it or takes its version first. A read of the registration, tombstone
  or drop record is final once the attempted version is readable, by the
  read-back rule above, without waiting for a later version: a create's
  follow-up writes the ref, not another manifest version, so the chain may stay
  at the registration. The attempt id stays on the fact, in the log and in
  `disposition.check`.

The refusals in the table apply to facts with another attempt id, with two
convergent exceptions: a delete that finds its branch tombstoned by another
attempt completes the follow-ups and succeeds, and so does a drop that finds
its candidate's drop record.

1. **Retirement is a publication.** Branch delete first publishes a tombstone on
   the branch's own chain. A concurrent publisher either loses that version,
   reloads and refuses with `branch_deleted`, or wins it, and the delete
   reloads into the new head and tombstones that. History settlement reads the
   chain up to the tombstone, so it is complete. Delete then publishes the
   deregistration on main, and the retirement marker, the `BranchContents`
   archive and the unlink follow as today. A crash between them leaves a
   tombstoned branch that refuses writes, which the next delete, cleanup or
   `repair` finishes.
2. **A branch exists when main registers it.** Create mints the native name
   `{logical}.{ULID}` and first publishes its registration on main: the logical
   name, that native name and the fork point it branches from. Only then does it
   create the native ref and tree. The registration is the branch's visibility
   point: list, resolve, schema apply, system-column upgrade and cleanup read
   the logical branches from main's registrations, in the attempt that
   publishes, and a ref no registration names is no branch. Two creates of one
   name race on main's version, and the loser refuses with `branch_exists`
   before it has written anything. A crash after the registration and before
   the ref leaves a registered branch with no ref, which refuses reads and
   writes as not ready. A create of that name refuses with `branch_exists`;
   its delete removes it, and `repair` completes it from the recorded fork
   point, which cleanup retains while the registration names it. Listings read
   registrations too: `branch list`, with the details
   [#1012](https://github.com/ModernRelay/omnigraph/pull/1012) adds, lists such
   a branch as not ready. Its registration fills `name`, `from` and
   `from_commit_id`; `head`, `created_at` and `updated_at` stay absent until its
   ref exists, so #1012 makes them optional.

   Until [schema apply with live branches](https://github.com/ModernRelay/omnigraph/pull/963)
   lands, a registration also refuses, on any attempt, when main's schema
   contract is no longer the one its fork point carries (`schema_changed`, the
   branch-start refusal [#1016](https://github.com/ModernRelay/omnigraph/pull/1016)
   defines), and schema apply's `ExactGraphVersion` refuses when a registration
   moved main's version after its live-branch check. System-column upgrade pins
   main's head, not its version (`ExactGraphHead`), so its attempts read main's
   registrations in `check_publish_precondition`, every attempt, and refuse when
   they name any branch but main. Whichever publishes second refuses, so no
   branch outlives an apply or an upgrade with the old contract. Once PR 963
   lands, its rule wins: a branch keeps its contract until a merge, a
   registration no longer reads main's contract, and a registration is never
   the reason an apply or an upgrade refuses. PR 963 then decides what they
   pin in place of main's exact version and branch set.
3. **Cleanup reclaims only what no registration names.** A create registers
   before its ref and tree exist, so a ref or tree that no registration names
   is no create in progress. The collector lists refs and trees before it reads
   main's registrations, so any listed ref or tree whose create is in flight is
   already registered in what it reads. Such a ref or tree, whether a tombstone
   retired it, a late marker rewrite recreated its ref, or a create crashed
   before it, is a candidate with no age threshold. The registration set decides
   which refs and trees are candidates; it does not replace the collector's
   keep set. `protected_manifest_branches` still keeps any tree a live branch
   reads and any retired tree that supplies a live tag, a retention root or
   imported merge-base lineage (`retained_merge_bases`), and a not-ready
   registration's fork point stays while the registration names it. The
   collector deletes the remaining candidates through a ref delete and a delete
   of that tree alone, never Lance's forced branch delete. The deciding
   authority is the registration set, never an age, as invariant 5 requires. Lance 13's stock
   `cleanup_old_versions` now also reaches unreferenced data newer than the
   retained manifests
   ([lance#8944](https://github.com/lance-format/lance/pull/8944)), so the engine
   keeps never calling it on a graph table.
4. **Drop is a publication.** Dropping a proposal publishes a drop record on
   its target branch's chain before it releases any tag. An attempt to publish
   that candidate refuses with `commit_dropped`, and a drop that reloads into a
   published candidate refuses with `commit_published`. Tags are released only
   after the record is durable, so no published pin can lose its protection.
5. **The gates stay as contention optimizations.** No correctness argument rests
   on `for_root`, the schema gate or the branch gates any longer, and their
   comments say so. The exclusive schema permit around branch creation becomes
   shared.

**Late requests are harmless by construction.** Every storage mutation in this
design falls in one of the key classes
[storage outcomes per operation](https://github.com/ModernRelay/omnigraph/pull/1093)
defines:

- a fresh key, such as a data, deletion, index or transaction file or a
  detached manifest, which nothing names until a publication does;
- a conditional key, a `__manifest` version written only if absent;
- a mutable key written only as a follow-up of a publication, whose late or
  repeated landing changes nothing logical: retirement's marker rewrite, the
  archive, the unlink and a drop's tag release.

The marker rewrite is unconditional in Lance 13: a late one can even recreate a
ref the delete already unlinked. That ref is deregistered, so it names no
logical branch, and cleanup's registration rule reclaims it with its tree. A guard against unconditional
writes of mutable keys admits these only as follow-ups of a publication.

**Format.** Control facts are new `__manifest` rows under the next storage
stamp. Only `omnigraph upgrade`'s stamp-only route raises a graph to it, and
that route's one publication on main registers every live branch from the
census it already takes, with writers stopped as it already requires; an
ordinary publication never raises the stamp, so no registration can hide a
live branch. Once main carries the stamp, older binaries refuse the graph and
each other branch takes the stamp with its next publication. Until a graph is
upgraded, the new binary keeps today's branch protocol and its
one-writer-process boundary. [Schema apply with live
branches](https://github.com/ModernRelay/omnigraph/pull/963) also proposes a
stamp; whichever lands second takes the next one, and the two are independent.
Downgrade is export and load.

**Cost.** Branch creation gains one zero-retry publication on main, deletion one
on the branch and one on main, and drop one on the target branch. Each is one
more version its chain's writers may reload past within their retry budget; a
fact that does not name them never makes them refuse. Only schema apply,
which pins main's exact version, and system-column upgrade, whose attempts read
main's registrations, refuse instead, which is the point until PR 963. Row writes, loads and merges are otherwise unchanged.
`integration/branch_control_cost.rs` measures the added requests per backend.

**Recovery.** Every new durable step is a publication or a follow-up of one. A
crash after a tombstone and before the unlink leaves a branch that refuses
writes, which delete, cleanup and `repair` complete. A crash after a
registration and before its ref leaves a branch that refuses as not ready until
its delete removes it or `repair` completes it. A crash loses the attempt id
with its process. Delete and drop need no id, because a later one completes
the follow-ups. A create does not converge: a new create of the name refuses
with `branch_exists`, and `repair` completes the branch or its delete removes
it. A crash after a drop
record and before the tag release leaves tags that expire with their captured
authority, as a proposal's tags already do.

### Server boundary

The `omnigraph-server` binary and crate keep their existing cluster-backed boot
and serving lifecycle. This RFC's server changes are actor assertions,
dispositions, the code registry and operation parity. Authentication sources,
applied policy, graph inventory, admission, online deployment, resource bounds
and shutdown keep their existing owners. Localhost does not select another
boot path or suppress deployment routes.

No local embedding API, alternate policy generator, local token source or
per-account process registry is introduced. Constructor cleanup, if pursued,
is independent work under [A4](#a4-retire-the-test-shaped-constructors), using
the ordinary cluster-backed model rather than a replacement serving API.

### HTTP API

| Change | Contract |
|---|---|
| `GET /graphs/{graph_id}/access` | Shipped in [#1073](https://github.com/ModernRelay/omnigraph/pull/1073) at contract `0.13`; [Access](#access) adds the command around it |
| `Omnigraph-If-Actor` request header | [Identity](#identity); `412 actor_mismatch` before graph lookup |
| `disposition` on every error body | [Dispositions and refusals](#dispositions-and-refusals); the existing fields keep their meaning |
| `ErrorCode` | Grows to the one registry; OpenAPI lists every code with the statuses servers send it under |

The HTTP contract version moves once, from `0.13` to `0.14`, with
`openapi.json` regenerated through the server's OpenAPI test. The bump lands
with the actor header ([#1074](https://github.com/ModernRelay/omnigraph/pull/1074)),
whose silent loss on an older server is the one that matters; `disposition`
and the registry ride it ([#1076](https://github.com/ModernRelay/omnigraph/pull/1076)),
and the access route already shipped at `0.13`. Servers keep accepting exactly
one version. A client that talks to deployments of several versions, as a
manager of long-lived deployments does, reads each one's version from
`/healthz`, which needs no header and names it, and speaks that version to it.
A client that refuses unknown fields in error bodies must accept
`disposition` before the new version reaches it.

### CLI

- **Command structure.** Each new command is a `Command` variant in the
  exhaustive `command_plane` and `command_label` matches, with its own module
  under `commands/` and an arm in `app.rs`, which captures the invocation once.
- **Contract versions.** `omnigraph version --json` gains a `protocols` object
  naming the version of each machine-readable contract here (`studio`,
  `target`, `credential`, `access`), beside its `http_contract`, `channel`,
  `commit` and `internal_schema`. A client requires the protocol versions it
  speaks before running a command.
- **One failure path.** `app.rs`'s `finish` prints one `ErrorOutput` with
  `disposition` for every command family, replacing the data-write `Failure`, the
  `RemoteRefusalOutput` and the managed `Output` failure bodies.
- **`--as`** sends `Omnigraph-If-Actor` on every served request of the
  command.

- **The MCP bridge** relays JSON-RPC unchanged except where
  [MCP for agents](#mcp-for-agents) binds it. For its one graph it fills
  `graph` in each tool call that omits it, refuses a call or resource URI
  naming another graph, and narrows the `graphs` tool and `resources/list` to
  that graph; it appends that binding to `initialize`'s instructions. Without
  `--write` it lists only tools annotated `readOnlyHint: true` and refuses
  `tools/call` for any other. It applies `notifications/cancelled` itself
  rather than forwarding it: the cancelled request gets no answer unless the
  answer was written before the cancel arrived.
- **The opener** passes only the link, which carries no secret, so its
  appearance in the opener's arguments discloses nothing.

## Invariants

- **Trust at the boundary (10).** Direct access declares an actor because the
  process holds storage access; served access resolves one from the credential,
  and `Omnigraph-If-Actor` only narrows. `credential get` hands a person's own
  static token to a process running as that person. Clients introduce no
  generated policy or identity source. The engine enforces policy for writes.
- **One publication door (2), one view (3), publish once (4).** HTTP and MCP
  use the ordinary server routes and engine. Cross-process writes keep each
  operation's publication contract and move control-operation authority checks
  into the manifest CAS.
- **Crash convergence (5).** Every new engine durable step is either a
  zero-retry publication on a `__manifest` chain or a derived, idempotent
  follow-up: retirement's marker, archive and unlink; a registered create's
  ref; a drop's tag release. A crash leaves state the next attempt, cleanup or
  `repair` completes. Cleanup decides from registration authority, not age.
- **Bounded and observable (11).** Server limits and operation lifecycle keep
  their owners. One code registry and disposition object describe failures;
  clients do not silently replay an uncertain operation.
- **One source of truth (12).** RFC 0059 owns target and credential resolution.
  The applied cluster revision remains the server's configuration authority;
  there is no local-server registry or second credential store.
- **Deny-list.** No daemon, cooperative writer lock, process-local fencing,
  work queue, shadow authority or cloud-only correctness path is added.
  Cross-process safety must come from the substrate CAS, not client routing.
- **Support boundaries.** Cluster-only serving remains unchanged, including
  on localhost. The engine phase proposes lifting the existing
  single-writer-process boundary for schema apply, branch control and cleanup;
  that boundary remains until its own evidence gates pass. Cluster admission
  is not removed or bypassed.

## Compatibility and reversibility

Storage changes only in the engine phase. The deliberate compatibility changes
are:

- **Addressing.** RFC 0059 owns the target migration and removal of legacy CLI
  selectors. This RFC adds no further destination selector or target state.
- **Exit codes.** Managed refusals move from 2 to 1, managed
  `recovery_required` from 4 to 2, managed cancellation from 6 to 1, and usage
  errors from 2 to 64.
- **The HTTP contract.** The actor header, `disposition` and code registry
  use the contract bump described in [HTTP API](#http-api); the access route
  already shipped at `0.13`. Clients and servers honor the existing
  contract-version refusal rules.
- **Failure bodies.** Managed problems and the CLI's `command_outcome` become
  `ErrorOutput` with `disposition`, with the managed owner's body under
  `problem`.

New client contracts are `studio`, `mcp` and `access`, their machine-readable
outputs where applicable, protocol discovery, and the Studio link format.
RFC 0059 owns `target` and `credential get`. Studio stops reading the CLI's
files and maintaining its own server list. Direct targets refuse in clients
that require HTTP; there is no automatic migration or implicit graph adoption.

The `omnigraph-server` binary, its packaging and its boot contract remain.
No server record format, local-server protocol or alternate serving API ships
under this RFC. Reverting a client command needs no graph migration. The
cross-process phase is irreversible for an upgraded graph: new `__manifest`
rows use the next storage stamp, older binaries refuse it, and downgrade is
export and load ([Engine](#engine-control-facts-on-the-version-cas)).

## Alternatives

| Alternative | Why not |
|---|---|
| A separate local-serving model for standalone graphs or read-only cluster snapshots | Discontinued on 2026-10-20. Generated identities and policies, separate route availability, boot and shutdown paths, and process discovery create another contract to maintain. A local ordinary cluster already uses the accepted serving model. |
| Fold the server binary into a new `serve` command | Not needed for target resolution or client access. This RFC preserves the existing binary and boot interface. |
| Automatically turn a direct target into a server or adopt a graph into a cluster | A direct destination does not authorize a new deployment, policy or process lifetime. Clients requiring HTTP select an existing server target. |
| A daemon through which every CLI command writes | Cross-machine correctness still belongs to the engine; a daemon adds lifecycle and version skew. [Unified access paths](0009-unified-access-paths.md) rejects a localhost funnel for embedded access. |
| A cooperative machine-local writer lock | Protects only cooperating writers on one machine. The manifest CAS must protect every supported writer. |
| A CLI-only refusal field beside the HTTP API's `code` | Two code namespaces for one failure; one registry keeps consumers consistent. |
| Studio bundles an engine, or starts a local server | Adds storage ownership and lifecycle to a client whose connection contract can use the existing server. |
| Studio parses or writes CLI configuration itself | Two resolvers have already drifted, and a second writer can collide with operator edits. |
| Studio keeps its own server list and token copies | Agents and Studio see different destinations and rotated tokens drift. |
| Agents inside Studio use the generic MCP bridge | Studio's gateway also limits branches and can request approval; the bridge does neither. |
| A CLI proxy for every remote target | Adds a second request path for streams, headers and uncertain outcomes. The stdio bridge exists only where a client requires that transport. |
| Learn access from a 403, or report all access in every response | A failed write is too late to learn a limit, while every-response policy evaluation is unnecessary. An explicit access request answers the client's question. |
| Never install Studio from the CLI | Installation is independently useful but waits for verifiable signed releases; it has no server-starting behavior. |

The retained client pattern is shared configuration and credential retrieval
through the owning CLI, as in kubectl's configuration/exec-credential split,
`git credential fill` and `gh auth token`. The former local-serving prior-art
comparison does not authorize a second OmniGraph server model.

## Evidence and tests

Extend the existing owners in [the testing map](../dev/testing.md):

- **API types, in-source:** every `ErrorCode` states its server statuses; the
  operation table binds every route, tool and command or declares an exception.
- **Server, `integration/auth_policy.rs`:** access responses for graph-wide,
  per-branch and source-to-target policy, hidden-branch refusal, static and
  signed credentials without policy; actor assertion ordering after
  authentication and contract checks, before lookup or effects; mismatch 412
  and malformed or duplicate header 400; disposition on failures.
- **Server, `integration/mcp.rs`:** new tools equal their existing routes;
  write tools remain behind the server's MCP setting and applied policy.
- **Server, `integration/openapi.rs`:** regenerate `openapi.json` for the
  actor header, disposition and code registry.
- **Server, `boot_settings.rs`:** preserve cluster-only boot, applied-state
  authority and the existing lifecycle while changing HTTP error contracts.
- **CLI, existing command/config owners:** target and credential behavior
  follows RFC 0059; `access` answers for direct graph, direct root and server targets, a direct
  graph in a cluster layout reading the ledger at its root, refuses direct
  catalogue and managed targets, and tells an absent store (`not_found`) from
  a damaged one (`internal`), as
  `cli_data::access_reports_what_the_caller_may_do_at_a_target` checks in
  [#1104](https://github.com/ModernRelay/omnigraph/pull/1104);
  `version --json` reports the protocols implemented. Target listing remains
  configuration-only and contains no live-process registry.
- **CLI, MCP bridge owner:** relay to an existing server, credential refresh
  without replaying uncertain tool calls, read-tool filtering, graph binding,
  cancellation and transport failures. Direct targets refuse before storage
  access or process startup; losing a connection never starts a server.
- **CLI, Studio opener owner:** the substituted opener receives only a
  validated link; direct targets and unsupported transports refuse; a missing
  scheme handler never opens a browser; the opener does not start a server.
- **CLI, installer owner:** a substituted verifier and download source prove
  signature checks before open/install, destination fallback and override-URL
  verification; Linux prints instructions. This remains a separate phase.
- **CLI, failure and parity owners:** every command family reports `code`,
  disposition and the declared exit; `--as` reaches every served request;
  `integration/parity_matrix.rs` compares CLI JSON with the HTTP response.
- **CLI, journeys:** extend `system_remote` and `cli_cluster_e2e` through the
  existing ordinary server and support fixtures. No alternate server fixture
  or binary replacement is required.
- **CLI, `skill.rs` drift tests:** command index and condensed reference stay
  aligned; the cluster-only serving statements remain intact.

- **Engine, `tests/failpoints.rs`:** the child-process actions gain create
  against create, delete against a publisher, cleanup against an in-flight
  create, publish against drop, schema apply and system-column upgrade against
  create, and cleanup against cleanup, each proving which side won and that the
  loser left nothing visible. Each control fact is checked on a retry, not only
  on a first attempt: a publisher whose first attempt loses the CAS to a
  tombstone, or a candidate publish that loses it to a drop record, must
  refuse on the attempt after.
- **Catalog, publisher tests:**
  - A publication of control facts alone records no graph commit and leaves
    every `table` row and the head unchanged.
  - Each precondition kind refuses on an attempt a fact names and passes one it
    does not.
  - With two control facts on one chain, a lost acknowledgement reads back as
    landed only for the fact carrying the attempt's own id. A never-sent or
    refused request reads back as lost. Another fact at a readable attempted
    version reads back as lost without an acknowledgement and as unknown after
    one. An absent or unreadable attempted version stays unknown, and a
    readable one settles without a later publication.
  - A delete and a drop that find another attempt's fact complete the
    follow-ups and succeed.
  - An operation retried with its attempt id goes on to its follow-ups.
  - A system-column upgrade attempt refuses when main's registrations name
    another branch.
- **Engine, `tests/detached_commit_matrix.rs`:** the other-process actor runs by
  default for the control operations, not only under `OMNIGRAPH_MATRIX=full`.
- **DST:** actors that model separate processes get separate write-queue
  registries, so a schedule can reach the interleavings the process-local
  gates hid; today all actors share `for_root`.
- **Engine, `integration/branch_control_cost.rs`:** the added publications per
  create, delete and drop, per backend.
- **Storage upgrade:** the stamp-only route registers every live branch in its
  one publication, no ordinary publication raises the stamp, older binaries
  refuse the new stamp, and the cross-version fences cover it.

Run CLI integration tests with `RUST_MIN_STACK=16777216` and the existing
`omnigraph-server` binary built for the served journeys. The documentation-only
disposition removes design scope; it does not claim these runtime evidence
gates have passed.

## Changes by repository

This RFC is decided and implemented in ModernRelay/omnigraph. Other
repositories' changes are listed as dependencies, made and reviewed there.

### ModernRelay/omnigraph

| Area | Change |
|---|---|
| Engine and catalog | The control-fact design above, including per-attempt checks, branch registration and retirement, proposal drops, cleanup authority, upgrade and cross-process qualification. |
| `omnigraph-api-types` | Code registry, disposition and operation table; reuse the shipped access output. |
| `omnigraph-server` | Actor assertions, disposition and operation parity on the ordinary cluster-backed server. Keep the binary, boot sources, policy, admission, deployment routes and lifecycle. |
| `omnigraph-cli` | Access reporting, actor assertions, failure/exit consistency, server-only MCP bridge and Studio opener, operation parity and protocol discovery. Reuse RFC 0059 target and credential commands. |
| Packaging | Keep the server binary and existing deployment entrypoint. Any existing entrypoint defect is an independent fix, not a reason to replace the binary. |
| Docs and skill | Document retained commands and HTTP contracts. Preserve cluster-only serving; update the engine support boundary only when its phase qualifies. Amend RFC 0003 for the stdio client bridge it deferred. Add release notes with each user-visible implementation phase. |

### ModernRelay/omnigraph-studio

| Area | Change |
|---|---|
| Choosing the CLI | Check the target, credential and Studio-link protocols needed for client commands. The server owns storage compatibility; Studio never selects a local engine or executable named by a link. |
| Server connections | Use an existing configured server endpoint. Do not open storage, start a server, generate its policy or token, or keep a local-server registry. |
| Links and saved state | Validate the target binding and operator-home digest; links do not execute writes. Keep saved state keyed by the validated target binding and graph. Register and dispatch the link scheme on supported platforms. |
| Configuration | Replace duplicate operator-config and credential parsing with the CLI's target and credential contracts. Migrate existing keychain server entries through the owning CLI once. |
| Static tokens | Obtain with `credential get --json`, keep in memory, send only within the returned endpoint scope, and refresh on 401 without replaying uncertain writes. |
| Access and failures | Show actions allowed by the access response; interpret `code` and disposition, and never replay an unknown write. |
| Agents | Keep the gateway connected to the existing server and preserve its own branch/approval checks. Give agent CLI homes explicit targets through RFC 0059; never pass the captured credential environment to agents. |
| Releases | Signed/notarized releases, a pinned team identifier and stable download locations gate the independent installer phase ([omnigraph-studio#3](https://github.com/ModernRelay/omnigraph-studio/issues/3)). |
| Specs | Remove auto-hosting, local-server reuse and generated-policy assumptions from client specs. Preserve existing-server connection, access, link and agent requirements. |

### The managed control plane

The control plane adopts the HTTP contract, actor assertion and disposition
when it moves its pinned OmniGraph version. While deployments of several
versions coexist, clients select the contract per deployment and data hosts
preserve the actor header. Control-plane problems should state their own
disposition so the CLI need not classify every sent request as unknown.

Its CLI procedures follow RFC 0059's target migration. The server executable,
launch arguments and deployment lifecycle do not change under this RFC. The
engine phase changes graph-internal state under its format gate, not the
control plane's boot contract. OAuth and managed MCP remain the independent
work in [A2](#a2-oauth-for-standard-clients-and-managed-access) and
[A3](#a3-mcp-relay-for-managed-targets).

## Rollout

Each phase is independently reviewable, and no phase requires the
discontinued local-serving work. Implementation PRs link this RFC and carry
their tests, user documentation and release note.

1. **HTTP identity, access and failures.** Actor assertions, disposition, the
   code registry and exit consistency use the stated HTTP contract boundary.
   The access route already shipped in
   [#1073](https://github.com/ModernRelay/omnigraph/pull/1073); independent
   remaining work stays in progress.
2. **Targets and access reporting.** Build on accepted RFC 0059 and its merged
   implementation, [#1090](https://github.com/ModernRelay/omnigraph/pull/1090).
   Add the access command without a process registry or serving command.
3. **Existing-server clients and operation parity.** An existing-server stdio MCP
   bridge, Studio opener and credential client integration connect to the
   ordinary cluster-backed server. The dependent bridge
   [#1084](https://github.com/ModernRelay/omnigraph/pull/1084) is closed unmerged.
   Its existing-server MCP adapter logic may be selectively reused in a
   separate PR, without its auto-hosting, direct-target serving or
   discontinued dependencies. Add the operation inventory and parity evidence
   with that work. This phase does not retire the
   server binary.
4. **Cross-process writes.** Qualify the engine control-fact, recovery, cost
   and upgrade design. Only then lift its single-writer-process boundary.
   Until then the current boundary and cluster admission remain unchanged.
5. **Installing Studio.** Independent of engine qualification, after the
   existing-server opener and Studio's signed release prerequisites exist
   ([omnigraph-studio#3](https://github.com/ModernRelay/omnigraph-studio/issues/3)).

Implementation status advances for retained work only; no discontinued
serving phase counts toward completion.

## Unresolved questions

1. **Schema carried by a merge.** Whether a merge that carries a newer schema
   needs `branch_merge` or `schema_apply`, which
   [schema apply with live branches](https://github.com/ModernRelay/omnigraph/pull/963)
   leaves open. The answer applies to ordinary engine and server operations,
   not a special local-serving mode.
2. **Ordering storage-format work.** The cross-process phase and schema apply
   with live branches both need a stamp. This RFC assumes separate, ordered
   upgrades; the implementation must assign the actual next stamp against its
   merge base.

RFC 0059 owns target grammar, migration and headless managed configuration;
its accepted decision is a dependency, not an unresolved request here.

## Appendix A: changes for separate decisions

These changes came out of the same analysis but have a wider blast radius than
this RFC: they change every client of every server, another author's RFC, or
another repository's security posture. Each needs its own decision. None is
required for the scope above to ship. Cross-process writer safety (#1018) and
credential introspection (#1019), which earlier revisions kept here, are now in
scope as cross-process correctness and access reporting.

| | Change | Why it is separate | Recommendation |
|---|---|---|---|
| A1 | `WWW-Authenticate` on every 401 | Changes every protected route's 401 for every client | Implemented for issue [#1020](https://github.com/ModernRelay/omnigraph/issues/1020) with an amendment to RFC 0003 |
| A2 | OAuth for standard clients and managed access | Security-sensitive and cross-repository, gated on the managed control plane's own qualification | Control-plane and Studio spec changes; tracking issue [#1025](https://github.com/ModernRelay/omnigraph/issues/1025) |
| A3 | MCP relay for managed targets | Lends the CLI's native managed authority to local agents | Decided after A2, in [#1025](https://github.com/ModernRelay/omnigraph/issues/1025) |
| A4 | Retire the test-shaped constructors | Public API removal with test churn across crates | Issue [#1021](https://github.com/ModernRelay/omnigraph/issues/1021), with ordinary cluster-backed replacements |
| A5 | RFC 0059 alignment | Accepted target contract with independent implementation | Design accepted in [#675](https://github.com/ModernRelay/omnigraph/pull/675); implementation [#1090](https://github.com/ModernRelay/omnigraph/pull/1090) |
| A6 | Defects found | Bugs, independent of this design | One issue each, linked in A6 |

### A1. `WWW-Authenticate` on every 401

Every 401 carries `WWW-Authenticate`, as RFC 9110 §15.5.2 asks: `Bearer`, plus
`error="invalid_token"` when a presented bearer was refused, plus
`resource_metadata="<url>"` with OIDC configured. Authentication, which every
route shares, decides it, so REST and `/mcp` cannot drift; the
[MCP server surface](0003-mcp-server-surface.md#challenge) RFC owns the
contract. A client should follow the challenge's metadata URL, or the path
RFC 9728 derives from the resource's URL, and never assume the metadata sits at
the host's root: a deployment may publish it only at
`/.well-known/oauth-protected-resource/<resource path>`.

**Disposition:** implemented for issue
[#1020](https://github.com/ModernRelay/omnigraph/issues/1020), with an amendment
to RFC 0003.

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

**Interim exception for Studio.** Until the managed control plane enables that
resource profile and registers Studio, and Studio implements this client,
Studio may reach a managed cluster with the CLI's data token for it. The
launcher stays refused (the target table's "Deferred to A2" and Opening
Studio): Studio uses the token only after a person selects a managed target
inside Studio. It takes the token from `omnigraph credentials export --target
NAME`, the one credential command that accepts a managed target, which writes
only to a Unix FIFO and never starts an interactive login. Export returns the
CLI's cached data token while it validates and asks the issuer for a new one
only when the cache is missing, expired or for another principal. That request
asks for 3600 seconds, which the issuer may shorten, and the CLI refuses an
expiry later than that plus 30 seconds; a token stored by `cluster token --ttl`
can last up to 86400 seconds. After a 401, a new export returns the same token
while the cache validates, and a target whose endpoint pin no longer matches
fails the export. Studio keeps the token in memory, and only while `target
inspect` still names that managed target at that endpoint; that is Studio's
obligation, since the pipe's reader could store it, and for an `omnigraph`
profile the CLI's own copy stays in the OS keychain. The exception accepts
what the first paragraph rules out: Studio and the CLI hold the same bearer as
one Cedar principal, so the server cannot give Studio less authority; the
token stays valid until it expires or its signing key retires, and removing
the person's issuer membership does not cut that short, though a policy change
denies both on their next request.

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

The convenience constructors (`new`, `new_with_bearer_token`,
`new_with_bearer_tokens`, `new_with_bearer_tokens_and_policy`,
`new_with_workload`, `new_single`, `open`, `open_with_bearer_token`,
`open_with_bearer_tokens`, `open_with_bearer_tokens_and_policy`,
`open_single_with_queries`) are test-shaped public APIs. Their removal is
independent API cleanup; it must migrate callers to ordinary cluster-backed
fixtures or appropriately test-only helpers and preserve the existing lifecycle.
It does not wait for or introduce an alternate local-serving API.

**Recommendation:** scope [#1021](https://github.com/ModernRelay/omnigraph/issues/1021)
around current caller evidence and the existing cluster-backed model. The
former dependency on `AppState::local` is withdrawn.

### A5. RFC 0059 alignment

[RFC 0059](https://github.com/ModernRelay/omnigraph/blob/main/docs/rfcs/0059-config-cli-coherency.md)
was accepted in [#675](https://github.com/ModernRelay/omnigraph/pull/675).
It owns the singular target group, inline addresses, one breaking migration,
credential binding and `credential get`, and
[#1090](https://github.com/ModernRelay/omnigraph/pull/1090) implemented it on
main. This RFC consumes that contract without adding live-server fields or
another selection state. Target-dependent client commands build on it;
independent HTTP and engine changes do not depend on a second serving model.

### A6. Defects found

| Issue | Defect | State |
|---|---|---|
| [#1022](https://github.com/ModernRelay/omnigraph/issues/1022) | The CLI sent `OMNIGRAPH_BEARER_TOKEN` to any remote URL, including an unregistered literal one. | Fixed by #1030: the CLI reads no unkeyed token |
| [#1023](https://github.com/ModernRelay/omnigraph/issues/1023) | `commit publish` and `commit drop` passed `--as` to the engine, but the plane guard rejected `--as` for every `commit` command. | Fixed by #1029 |
| [#1024](https://github.com/ModernRelay/omnigraph/issues/1024) | The credentials file's directory is created with default permissions, not `0700`. | Open |
| [omnigraph-studio#2](https://github.com/ModernRelay/omnigraph-studio/issues/2) | The agent gateway checks neither `Host` nor `Origin`. | Open |
| Not filed | The surveyed `docker/entrypoint.sh` passed unsupported positional URI, `--config` or `--target` forms to the server. | Revalidate and fix independently through the existing server boot contract; no binary fold |
| Not filed | `--as` help said it had "No effect on remote writes", but served writes refused it. | The help now states the refusal; [Identity](#identity) replaces the refusal |

## Decision log

Entries before 2026-10-20 describe superseded iterations where they concern
local serving. The current body and the final disposition own the retained
proposal.

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
- 2026-10-19: Aligned with the phase 1 and phase 3 implementations and their
  reviews. The actor header rides the contract bump to `0.14` rather than a
  capability response header, and `/mcp` compares it after authentication
  alone. Rule 6 states the outcome table the server implements: a typed
  refusal keeps its code, an earlier publication of the same operation is the
  engine's mark on its error, and `outcome.check` waits for an attempt id. The
  local embedding API names `LocalBinding::local`, `ReadWriteUnavailable`,
  `start_local` and `LocalServing::release`, refuses a graph inside a cluster
  instead of promoting it, and binds local roots only.
- 2026-10-19: Named the actor header `Omnigraph-If-Actor`, after HTTP's
  conditional `If-*` headers, since a mismatch is a failed condition (412). The
  error object became `disposition`: `execution` went, because it only repeated
  `effects`; `effects` gained `partial` for a refused step after an earlier
  publication of the same operation, which `unknown` overstated; and `action`
  gained `fix`, because a malformed request or a wrong credential fails the
  same way on resend, which `refresh` did not say. Each code names its own
  action. Records accept only a 32-hexadecimal machine id, Windows records wait
  for their own pull request, and the MCP bridge's relay rules match the
  implementation.
- 2026-10-19: Addressed the review of `c4641d79`. Control facts replace the
  head-preserving rows: a tombstone, a registration or a drop record changes no
  `table` row and records no graph commit, and `check_publish_precondition`
  reads the facts that name an attempt on every attempt, so a writer refuses
  whatever its precondition. A create registers before it writes its ref, which
  makes the registration the branch's visibility point and gives cleanup an
  authority to read in place of the seven-day age. Until PR 963 lands, a
  registration refuses when main's contract moved past its fork; after it, PR
  963's rule wins. Only the upgrade census raises the new stamp. RFC 0059's
  accepted revision in PR #1090 owns addressing, inline addresses included,
  and the `target` and `credential` commands; deployments pass their root
  through `--target`. The access route is #1073's, and this RFC adds the
  command. A direct graph inside a cluster refuses and names the root. Storage
  outcomes and pending publications are PR 1093's; this RFC keeps the
  registry, `disposition` and the exit table, drops `storage_unavailable`, and
  labels `disposition.check` as a later phase. The file keeps its new name,
  chosen before it merges, so the registry records one name.
- 2026-10-20: Retitled "Graph access and cross-process writes", keeping the
  canonical filename. Discontinued the second serving model and the work in
  [#1075](https://github.com/ModernRelay/omnigraph/pull/1075),
  [#1077](https://github.com/ModernRelay/omnigraph/pull/1077) and
  [#1083](https://github.com/ModernRelay/omnigraph/pull/1083). Removed normative
  local embedding APIs, generated standalone policy/tokens, read-only cluster
  snapshot serving, record/election/reuse protocols, `serve` and the binary
  retirement. Supersedes the earlier "for you/for others" rule and every
  rollout or compatibility dependency on that rule. The ordinary applied
  cluster server remains the only serving model, on localhost as elsewhere;
  no auto-hosting or implicit graph adoption replaces it. Retained independent
  target consumption, access, actor assertions, dispositions/exits, operation
  parity, existing-server MCP/Studio clients and cross-process engine work.
  RFC 0059's design is accepted by #675; #1090 remains its implementation,
  separate from this draft. The three serving PRs are closed unmerged and
  #980 is closed as not planned. #1084 is also closed unmerged; useful
  existing-server MCP adapter logic may be selectively reused in a future
  separate PR. No cherry-pick or implementation is part of this
  change, and the future client proposal remains draft. Constructor
  cleanup (#1021) remains independent with
  ordinary cluster-backed fixtures. RFC 0059's former reference to this
  separate local-serving proposal never accepted it and creates no remaining
  serving dependency.
- 2026-10-20: #1090 merged RFC 0059's implementation, so `blocked_on` is empty
  and the target-dependent commands build on main.
- 2026-10-20: Accepted by maintainer decision (Ragnor): the actor assertion
  (`Omnigraph-If-Actor`), the error-code registry and `disposition`, the exit
  table, the `access` command, operation parity, the existing-server MCP
  bridge and Studio opener, and the engine control-fact design with its
  qualification gates. Appendix A's items stay separate decisions. The
  phase 1 and 2 implementations are #1074, #1076, #1078 and #1081.
- 2026-10-20: Addressed the review of `eeb6e123`. Every control fact records
  the attempt id its operation mints once: the read-back after a lost
  acknowledgement compares it, a pending control fact is keyed by it, and an
  operation's retry treats its own fact as landed, so deletes and drops converge
  instead of refusing themselves. System-column upgrade pins main's head, not
  its version, so its attempts read main's registrations. Cleanup's
  registration rule covers refs as well as trees, including a ref a late
  marker rewrite recreates, under the collector's dependency closure. A create
  never completes a crashed create's registration. The pending-branch
  `service_unavailable` covers one branch, `branch list` lists a registered
  branch with no ref as not ready, and the `--as` help quote is current.
- 2026-10-20: Took the optional corrections of the review of `ef441758`:
  read-back has three results (landed, lost, pending); a caller settles a
  control fact from the fact itself, since the head does not move; cleanup's
  candidates still pass the collector's keep set (`protected_manifest_branches`,
  `retained_merge_bases`, a not-ready fork point); a crash loses the attempt id,
  so only delete and drop converge after one; `branch list` fills a not-ready
  branch from its registration.
- 2026-10-20: Followed storage outcomes per operation's version-key fence: an
  unknown publication, row or control fact, is recorded nowhere and blocks
  nothing; the branch's next publication builds on it or takes its version,
  and a read of the fact is final only after the chain moves past it. The
  pending-branch `service_unavailable` row is gone. Read-back follows #1092's
  split, a not-ready branch's absent fields are named, and the access evidence
  uses RFC 0059's target kinds.
- 2026-10-20: Aligned with actor views and subgraph sharing (#951): it fills
  the access output's reserved `view` field with the caller's own view facts,
  its refusals join the one registry with `effects: none` and `action: fix`,
  and under a view that hides commit listings `disposition.check` answers only
  for the caller's own publication and receipt.
- 2026-10-21: A read of an attempted version is final once that version is
  readable, as storage outcomes per operation's §5 has it, not once the chain
  moves past it, which a create whose follow-up writes only the ref would
  never see. A refused request is a loss beside a never-sent one, and an
  acknowledged attempt whose version holds another fact stays unknown. The
  version-key fence covers linear publications; §5 keeps the detached proposal
  and snapshot.
- 2026-10-21: `disposition.check`, a later read that cannot see the store's
  acknowledgement, reports a loss once the attempted version holds another
  commit; the acknowledged in-doubt result stays on the publisher's read-back.
- 2026-10-21: A1 implemented for issue
  [#1020](https://github.com/ModernRelay/omnigraph/issues/1020). Superseded
  sentences: "Every 401 would carry … and a bare `WWW-Authenticate: Bearer`
  otherwise" (a refused credential carries `error="invalid_token"` with or
  without OIDC), "The challenge would move into the authentication middleware
  every route shares", and A1's recommendation, now its disposition.
- 2026-10-22: MCP parity implemented as phase 3 of
  [RFC 0003](0003-mcp-server-surface.md). The change-feed
  baseline joins the byte-stream exceptions: its route answers an NDJSON entity
  snapshot, an export stream, whose cursor is valid only once the snapshot is
  installed, so no bounded tool result can carry it. Stored mutations are
  admitted through `mutate`, since a caller allowed `change` can already run
  any mutation there. Superseded sentences: the gap row "MCP has no commit
  diff, commit publish or drop, change-feed baseline, branch diff or
  merge-base tool, and no `blob stat`", the exception "Blob payloads and
  export streams; `blob stat` is a tool", and the stored-mutation change "or
  declare the reason in the table".
- 2026-10-22: The Linux opener launches the `Exec` of the desktop entry `xdg-mime` names instead of running `xdg-open`, which without a display hands the link to a browser.
- 2026-10-25: A2 gains one exception. Until the managed control plane enables
  the resource profile and registers Studio, and Studio implements the client,
  Studio may reach a managed cluster that a person selects inside Studio with
  the CLI's data token from `credentials export`. It qualifies the sentence "A
  client other than the CLI should reach an OIDC-enabled server, self-hosted
  or managed, as a standard OAuth public client rather than borrow the CLI's
  credentials" for that case only: the rule stays in force for self-hosted
  OIDC servers and every other client, and the launcher's refusal of a managed
  target (the target table and Opening Studio) is unchanged.

- 2026-10-29: Pending amendment. [Remote optimize through the serving
  owner](2026-10-29-remote-optimize-through-the-serving-owner.md), a draft, runs a served
  cluster root's `optimize` in the process that holds the cluster lock,
  through graph-scoped routes with a durable operation record. Once it is
  accepted, `optimize` leaves the "Runs where the storage is" row of the
  exception table for served cluster roots and stays there for direct
  targets. The other commands in that row are unchanged. The table stands
  unchanged until acceptance.
