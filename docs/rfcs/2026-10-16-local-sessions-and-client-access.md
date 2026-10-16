---
rfc: "2026-10-16-local-sessions-and-client-access"
title: "Local graph sessions and client access"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-16
updated: 2026-10-16
discussion: https://github.com/ModernRelay/omnigraph/issues/980
supersedes: []
superseded_by: []
blocked_on:
  - "Named targets and CLI coherency (RFC 0059, PR #675) must be accepted: every command here addresses graphs only through `--target`."
---

# RFC: Local graph sessions and client access

> A term set in ***bold italics*** is defined at that spot; it is plain text afterwards.

## Summary

This RFC lets a person, and the agents working with them, open a graph in
OmniGraph Studio and in MCP clients with the identity and authorization the CLI
gives. Its scope is the part that can ship on its own; changes with a wider
blast radius are in [Appendix A](#appendix-a-changes-for-separate-decisions),
each with a recommended RFC or issue.

1. **Local graph sessions.** A ***session*** is one `omnigraph` process that
   serves one directly accessed graph on loopback, through the HTTP API and
   `/mcp`, to clients of the same OS account on the same machine.
   `omnigraph serve --target NAME` starts one. It is read-only unless
   `--write` is given, never unauthenticated, never holds cluster admission,
   and installs a generated Cedar policy on the engine. Its ***session
   record*** names its URL, token, access mode and actor, so a same-account
   client attaches by reading it. A graph has at most one session, found
   through its record.
2. **A thin Studio launcher.** `omnigraph studio --target NAME` opens
   `omnigraph-studio://open?target=NAME` and exits, as `codex app` opens the
   Codex app. Studio owns its sessions: it runs
   `omnigraph serve --target NAME --reuse --json`, which attaches to the
   graph's live session or starts one as Studio's child. Links carry no secret,
   no path and no request for write access.
3. **MCP for agents.** `omnigraph mcp --target NAME` is a stdio MCP endpoint for
   local graphs and static-token servers, so no agent configuration holds a
   token. Write tools need the endpoint's permission and the agent's own
   `--write`.
4. **Static tokens for clients.** `omnigraph credential --target NAME --json`
   hands a server target's stored static token to a calling process. The CLI
   stays the one resolver and store of targets and static tokens, so a
   self-hosted server with static tokens works in Studio without Studio reading
   the CLI's files.

The `omnigraph-server` binary stays cluster-only, and the storage format and
the HTTP API do not change.

## Motivation

Every fact below was checked against OmniGraph main, the managed control plane
and OmniGraph Studio on 2026-10-16.

**Nothing serves a local graph to a client that speaks only HTTP.** Studio
never opens storage (its constitution, principle I), and the server binary
serves clusters only. The library's eleven single-graph constructors are
shaped for tests: they open read-write; the `open` family acquires cluster
admission that an embedder can never release, because it lives in a private
field; they mount deployment routes that then answer 409 or 500; they ignore
the session-setting environment; and they leave the serving lifecycle (the
shutdown watchdog, the owned-operation drain, exit 2 on an uncertain write)
to the embedder, because only the private `serve_config` implements it.

**Agents have no MCP path to a local graph.** [RFC 0003](0003-mcp-server-surface.md)
makes the server's `/mcp` the only MCP implementation, retires the stdio
package, and defers a stdio bridge "only if local desktop clients need it".

**Several processes reach one graph.** Studio, its agents and terminal agents
will touch one local graph at once. The engine supports schema apply, branch
creation and deletion, and cleanup from one writer process only
([support boundaries](../dev/invariants.md#current-support-boundaries)). A
session gives every client that attaches to it one writer process. The gap that
remains for processes that do not attach exists today and is
[Appendix A1](#a1-cross-process-writer-safety).

**A loopback port is exposed.** Every local process can reach it, and web pages
can through DNS rebinding. The REST routes have no `Host` check and no CORS
layer; `/mcp` checks host names but not the port.

**Clients resolve targets and credentials on their own, and disagree.** Studio
parses the CLI's `config.yaml` and `credentials` file itself, finding a token
by server name where the CLI matches the longest URL prefix, and RFC 0059
replaces that format. A GUI launched from Finder sees launchd's environment,
not the shell's, so environment tokens are invisible to it.

**Static tokens must stay first-class.** They are how people host their own
graphs: no identity provider and no admission-snapshot publisher.

This needs an RFC: it adds CLI commands, a persisted format, a public Rust API,
an authorization boundary, and an amendment to the cluster-only support
boundary.

## User and operational behavior

### Commands

```bash
omnigraph serve --target local                     # read-only session
omnigraph serve --target local --write             # read-write, as operator.actor
omnigraph serve --target brain --graph people      # cluster graph: read-only
omnigraph studio --target local                    # open a graph in Studio
omnigraph mutate --target local --session -e '…'   # write through the live session
omnigraph mcp --target local                       # stdio MCP, read tools
omnigraph mcp --target prod --graph people --write # stdio MCP with write tools
omnigraph credential --target prod --json | …      # a target's static token, to a pipe
```

| Target kind (RFC 0059) | `serve` | `studio` | `mcp` | `credential` |
|---|---|---|---|---|
| Direct graph, outside a cluster | Read-only, or read-write with `--write` | Opens Studio, which attaches to or starts a session | Relays to the live session, or hosts one | Refused: sessions carry their own token |
| Direct root with `--graph` (a cluster graph) | Read-only only | As above, read-only | As above, read-only | Refused |
| Server with a static token | Refused: the graph already has a server | Opens Studio on it | Relays to the server's `/mcp` with the stored token | Returns the stored token |
| Managed cluster | Refused | Refused until [A4](#a4-oauth-for-standard-clients-and-managed-access) | Refused until [A5](#a5-mcp-relay-for-managed-targets) | Refused: managed credentials stay the CLI's |
| Managed account | Refused | Refused | Refused | Refused |

A session on a cluster graph is the direct-storage view, the same view direct
CLI reads have. It applies none of the cluster's Cedar policy, stored queries,
embedding-provider bindings or external-Blob policy, and its startup line says
so, naming the cluster's server target as the served view when one exists.

### Session access and actor

A session is read-only unless `--write` is given. `--write` needs an actor,
from `--as` or else `operator.actor`, and refuses before opening anything
without one (`actor_required`); it refuses for a cluster graph
(`cluster_graph_read_only`). A read-only session uses `operator.actor` when it
is set and otherwise the actor `default`, the actor a single unnamed server
token already resolves to. Reads record no actor, so a read-only session asks
for no configuration.

| | Read-only | Read-write |
|---|---|---|
| Granted | `read`, `export` on any branch; `graph_list` | Also `change`, `branch_create`, `branch_delete`, `branch_merge`, `commit_publish`, `commit_drop` |
| Never granted | `schema_apply`, `admin`, `config_manage`, `invoke_query` | The same |
| Write routes | 403 `read_only_session` before the body is read | Admitted |
| `/mcp` | Read tools only | Read and write tools |

No session mounts deployment, schema-apply or maintenance routes. Schema
changes and maintenance stay direct CLI commands.

### Session lifetime and output

A session runs in the foreground until interrupted, or until its parent closes
it, and shuts down through the server's bounded shutdown. An uncertain write
completion makes it exit 2, as `omnigraph-server` does; nothing replays the
write. Startup prints the resolved target, access mode, actor and URL to
stderr. With `--json`, stdout carries one line when the session is ready:

```json
{"protocol":1,"session":"01JA9…","reused":false,"url":"http://127.0.0.1:53211","graph_id":"default","access":"read_write","actor":"act-me","token":"<bearer>","record":"/tmp/omnigraph-501/sessions/01JA9….json"}
```

The token appears in this line and in the record, never in the human banner.
`--port <n>` replaces port 0 for SSH forwarding; the bind address is always
`127.0.0.1`.

### One session per graph

Before starting, `serve` looks for a live session with the same canonical root:

- Without `--reuse`, it refuses with `session_exists`, naming the session.
- With `--reuse`, it prints the live session's line with `"reused": true` and
  exits 0, unless `--write` was asked of a read-only session, which refuses
  with `session_access_mismatch`. Studio and `omnigraph mcp` always pass
  `--reuse`.

Two launches racing can still start two sessions; both stay correct for rows
through the manifest CAS, as two CLI processes are today.

When a direct write command targets a root whose live session is read-write, it
prints one notice to stderr naming the session and `--session`, then runs as it
does today. The notice changes no outcome. `--session` on a data write command
sends the operation over HTTP to that session with its token. The command's
resolved actor must equal the session's actor (`session_actor_mismatch`), so
`--session` cannot change attribution. Outcomes are the served ones: a refusal
before effects is "not started"; a lost response is unknown and never
replayed. Nothing routes a command through a session unless `--session` asks.

### Opening Studio

`omnigraph studio --target NAME [--graph G]` validates the target, opens
`omnigraph-studio://open?target=NAME[&graph=G]`, prints the link and exits. It
owns nothing at runtime, so its one contract with Studio is the link format,
which Studio owns.

- On macOS it opens Studio by bundle identifier (`open -b`); elsewhere through
  the platform opener. When Studio is not installed it prints how to install it
  and exits nonzero. It never downloads anything or opens a browser.
- With `--json` it prints the link and the target.
- Studio resolves the target. For a direct target it runs
  `omnigraph serve --target NAME --reuse --json` with the login-shell
  environment it captured, and attaches to the session in the line: a live one
  started by a terminal, an agent or Studio, or a new one that is Studio's
  child. For a static-token server target it runs `omnigraph credential` and
  connects directly.
- A link opens read-only. Read-write is a deliberate action inside Studio,
  which runs `serve --write --reuse`, or a session the person started with
  `omnigraph serve --write`, which Studio attaches to. A link never writes,
  as Studio's own link rules require.
- A person who needs a terminal-only environment, such as a one-off
  `AWS_PROFILE`, runs `omnigraph serve` in that terminal; Studio attaches to it.

### MCP for agents

`omnigraph mcp --target NAME` speaks MCP over stdio for agents that start their
tools as child processes:

```json
{"command": "omnigraph", "args": ["mcp", "--target", "local"]}
```

- **Direct target.** Runs the equivalent of `serve --reuse` in its own process:
  it relays JSON-RPC to the live session's `/mcp` with the record's token, or
  hosts a session, read-only unless `--write`, and publishes its record so
  Studio can attach. It exits when its input closes, shutting down a session it
  hosted.
- **Server target with a static token.** Relays to the server's `/mcp` with the
  stored token, which never reaches the agent's configuration.

Write tools need two consents. The endpoint is the ceiling: a read-only
session, or a server without `--mcp-allow-writes`, lists no write tools. The
agent's own configuration is the other: without `--write`, the bridge lists and
admits only tools the server annotates `readOnlyHint`, which it already sets
for every read tool. A person opening a graph read-write in Studio has not
thereby given every connected agent write access. Tool logic, refusals and the
write contract stay the server's; the bridge adds no tool.

### Static tokens for clients

`omnigraph credential --target NAME --json` returns a server target's stored
static token:

```json
{"protocol":1,"target":"staging","endpoint":"https://staging.example","scheme":"bearer","token":"<static token>","source":"credentials_file"}
```

- It resolves the target's own binding: the target-keyed environment variable,
  then the `0600` credentials file. The unkeyed `OMNIGRAPH_BEARER_TOKEN` is not
  a target's credential and is never returned.
- It refuses when stdout is a terminal (`credential_terminal`), so a token never
  lands in scrollback; a caller reads it through a pipe.
- It refuses a non-server target (`credential_kind`) and a target without a
  stored token (`credential_missing`).

Every server a client uses is a target, and no client keeps a second registry
or a second copy of a stored token. Studio runs `credential` again after a 401,
so a token rotated with `omnigraph login --target NAME` reaches it without a
restart.

### Self-hosted servers with static tokens

A person hosting their own graphs runs `omnigraph-server --cluster …` with
static tokens and adds the server as a target. Four server settings decide how
complete a client's experience is:

| What the person wants | What the server needs |
|---|---|
| Edits attributed to people | Named actors through `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON`, `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` or `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET`. A single `OMNIGRAPH_SERVER_BEARER_TOKEN` is the actor `default`. |
| Edits at all | An applied graph policy granting the actor its write actions. Without one a static token only reads, and today a client learns that from its first refused write ([A2](#a2-credential-introspection)). |
| Listing the server's graphs | `graph_list` in the cluster policy. Without it a client opens the target's `default_graph` or the graph the person names; `/graphs/discovery` serves verified identities only. |
| Agents over MCP | `--mcp-allowed-host <public host>`, because `/mcp` admits only loopback `Host` values by default, and `--mcp-allow-writes` for write tools. |

## Design

### Local embedding API in `omnigraph-server`

Exact signatures are settled in the implementation PR; the contract is:

```rust
pub enum LocalAccess { ReadOnly, ReadWrite }

pub struct LocalOptions {
    pub access: LocalAccess,
    /// Required for ReadWrite; `default` when absent for ReadOnly.
    pub actor: Option<String>,
    pub process_defaults: ProcessDefaults,
}

impl AppState {
    /// Returns the state and the session's bearer token, once.
    pub async fn local(uri: &str, options: LocalOptions)
        -> Result<(AppState, SessionCredential), LocalOpenError>;
}

pub async fn serve_local(
    state: AppState,
    listener: tokio::net::TcpListener,
    extend: impl FnOnce(Router) -> Router,
) -> Result<()>;
```

`AppState::local`:

1. Classifies the URI with `cluster_root_for_graph_uri`, the rule direct CLI
   commands use, and refuses `ReadWrite` for a cluster graph or without an
   actor. It never calls `acquire_graph_admission`.
2. Opens read-only with `Omnigraph::open_read_only`, validated against a
   captured `GraphReadAuthority` for a cluster graph, or read-write with
   `Omnigraph::open`.
3. Generates a 32-byte token and keeps only its SHA-256 hash.
4. Builds the policy as a structured `PolicyConfig` with `PolicyCompiler::compile`
   followed by the kind-alignment validation the policy loaders run, for the
   route's graph id (`default`), and installs it with `with_policy`. A
   one-handle registry carries the server policy granting `graph_list`, which a
   single-mode state cannot.
5. Mounts no `/cluster/*` route. Lists MCP write tools only for `ReadWrite`.

Read-only holds in three independent places: `ingress::admit` refuses every
`Write`-class request before its body; `/mcp`, which ingress admits as one
read-class route, lists no write tools; and the engine's policy refuses every
write, including one without an actor, which `enforce` denies whenever a policy
is installed. A local state is never in open or default-deny mode.

`serve_local` refuses a listener whose local address is not loopback and runs
the lifecycle `serve_config` runs: the signal and uncertain-owner listener, the
watchdog armed with the shutdown grace, graceful HTTP shutdown, the
logical-owner drain and exit 2 on uncertain completion. That lifecycle moves
into one private function both entry points call. The caller binds the
listener, so it knows the port and writes the record before announcing
readiness. `extend` receives the router `build_app` built from this state.

The existing convenience constructors stay until
[A6](#a6-retire-the-test-shaped-constructors).

### CLI

- **Commands.** `serve`, `studio`, `mcp`, `credential`, and `--session` on data
  write commands, each declared in `planes.rs`, which today has no capability
  spanning store, cluster and server targets.
- **Loopback guard**, a layer the CLI adds in `extend`, outside every server
  layer, so a deployed server never gains it:
  - `Host` must be exactly `127.0.0.1:PORT`, `localhost:PORT` or `[::1]:PORT`,
    case-insensitively; anything else, or none, is 403. This closes DNS
    rebinding, which `Origin` alone cannot: a rebound page's `GET` is
    same-origin and carries no `Origin`.
  - Any request carrying `Origin` is 403; native clients send none and a
    session serves no page.
  - No response carries a CORS header; every response carries
    `Cross-Origin-Resource-Policy: same-origin`. Nothing relies on `Sec-Fetch-*`,
    which browsers do not send to plain-HTTP origins.
  - Malformed `Host` or `Origin` bytes are refused, never a panic.
- **The MCP bridge** relays JSON-RPC unchanged except for filtering
  `tools/list` and refusing `tools/call` for tools without `readOnlyHint` when
  it was started without `--write`.
- **The opener** passes only the link, which carries no secret, so its
  appearance in the opener's arguments discloses nothing.

### Session records

Records live in a fixed per-user directory resolved from the OS account, not
from `HOME`, `TMPDIR` or `OMNIGRAPH_HOME`, so a GUI process and a terminal find
the same sessions: `/tmp/omnigraph-<uid>/sessions/` on Unix, the account's
local application data on Windows. The directory must be owned by the account
with no group or other permission bits and is refused otherwise. Files are
opened without following symlinks, with the owner and mode checks
`managed/auth/coordination.rs` already applies.

```text
/tmp/omnigraph-<uid>/sessions/<session>.json    the record, 0600
/tmp/omnigraph-<uid>/sessions/<session>.lock    held by the session for its whole life
```

```json
{
  "format": 1,
  "session": "01JA9…",
  "root": "file:///Users/me/company.omni",
  "graph_id": "default",
  "url": "http://127.0.0.1:53211",
  "token": "<32 random bytes, base64url>",
  "access": "read_write",
  "actor": "act-me",
  "pid": 41873,
  "started_at": "2026-10-16T09:12:44Z",
  "cli": {"path": "/opt/homebrew/bin/omnigraph", "version": "0.15.0", "http_contract": "0.13"}
}
```

The `.lock` file is a liveness signal, not writer exclusion: it decides only
whether a record's owner is alive. The owner takes it with
`std::fs::File::try_lock`, opens the graph, binds, writes the record (temporary
file and rename) and then announces readiness. A reader that can take the lock
has found a dead session: it ignores the record and may delete both files. The
operating system releases the lock however its process exits, so liveness never
consults the process ID, which proves neither liveness nor quiescence
([server runtime RFC](2026-09-29-server-runtime-and-online-deployment.md)). A
record with an unknown `format` is ignored. `root` is canonical, so two targets
naming one graph find the same session.

## Invariants

- **Trust at the boundary (10).** No client supplies an actor. A session's
  actor is fixed at start by the direct-path rule, `--as` or else
  `operator.actor` ([CLI addressing](0011-cli-addressing-and-config.md)): the
  process holds storage access and is entitled to it, as a direct
  `omnigraph mutate` is. Every request presents a token the server resolves.
  The session keeps only its token's hash; the record is a user-only delivery
  file like the CLI's `credentials`. `credential` hands a person's own static
  token to a process running as that person. The engine enforces the generated
  policy for every write.
- **One publication door (2), one view (3), publish once (4).** Sessions write
  through the existing routes and engine; publication is unchanged.
- **Bounded and observable (11).** One session per root; fixed shapes for the
  record and the JSON lines; server limits and the owned-operation lifecycle
  unchanged; typed refusal codes.
- **One source of truth (12).** The CLI is the only resolver of targets and the
  only store of their static tokens. A record is a hint whose liveness the
  operating system establishes; no write path trusts its contents.
- **Deny-list.** No writer lock, fencing or exclusion is added; the record's
  lock signals liveness only. No queue, no silent retry, no shadow credential
  store, no cloud-only path.
- **Support boundary.** "The server is cluster-only" becomes: the
  `omnigraph-server` binary is cluster-only; the CLI serves one directly
  accessed graph to same-account loopback clients as a session. The
  single-writer-process boundary is unchanged ([A1](#a1-cross-process-writer-safety)).

## Compatibility and reversibility

Storage, the server binary and the HTTP API are unchanged. New contracts: the
four commands and `--session`, the session line and `credential` output
(`protocol: 1`), the record (`format: 1`) and its directory, the Studio link
format, and the local embedding API. Existing direct commands behave as today;
the only addition is the notice when a live read-write session holds their
root. Studio stops reading the CLI's files. There is no legacy addressing,
format reader or adapter.

Reverting removes the commands and the embedding API; no graph data migrates.

## Alternatives

| Alternative | Why not |
|---|---|
| A single-use connect code in the opened link, redeemed over loopback | This RFC's first draft. Jupyter shipped a single-use token in argv and removed it because another logged-in user could read it from the process list; on Linux and Windows a scheme handler receives its URL in argv. It needed a redemption route, attach keys and handoff files; the record replaces all of them. |
| The launcher hosts the session in its terminal and opens a session link | The previous revision. It keeps the terminal's environment, but the launcher then owns runtime state and a second link form. Studio owning its sessions, as the Codex app owns its app-server, gives one owner and one contract; a terminal-hosted `serve` remains available and Studio attaches to it. |
| A cooperative writer lock on every direct write | The previous revision. It changes every direct write for a gap that exists without Studio, and protects only cooperating processes of one account on one machine. See [A1](#a1-cross-process-writer-safety). |
| A browser UI embedded in the CLI ([PR #850](https://github.com/ModernRelay/omnigraph/pull/850)) | Studio is a native client in its own repository, and embedding a JavaScript build makes a frontend toolchain a prerequisite of every CLI build. Its boot mode takes cluster writer admission and runs unauthenticated. Its point that an embedder must reuse the server lifecycle is adopted as `serve_local`. |
| Serve local graphs unauthenticated on loopback | Any local process or account can write, writes record no actor, and `Host` checks do not stop another local user. |
| Studio bundles its own engine | A newer engine stamps a branch's storage format forward on first publication, and an older binary refuses a newer stamp. Sessions run the person's own CLI. |
| Route every direct command through a live session automatically | RFC 0059 forbids discovery from selecting a destination, and [unified access paths](0009-unified-access-paths.md) rejects a localhost funnel for the embedded path. `--session` is explicit. |
| Studio parses the CLI's files | Two resolvers have already drifted, and RFC 0059 changes the format. |
| Studio keeps its own server list and token copies | Agents Studio runs would see other servers than Studio, and rotated tokens would drift. |
| A CLI proxy for every remote target | A second request path that must stay transparent for streams, headers and outcomes. |
| Unix sockets instead of loopback TCP | No browser can reach a socket and file permissions would authenticate, but the server's lifecycle and Studio's client are TCP-only and Windows needs named pipes. A later protocol can add it. |
| Liveness from a process ID or an HTTP probe | A bare process ID is reused; a held operating-system lock answers liveness directly. |

### Prior art

Surveyed 2026-10-15 and 2026-10-16 from source, documentation and advisories:

| Tool | Serving and transport | Authentication | Browser defence | Second launch |
|---|---|---|---|---|
| Drizzle Studio | `drizzle-kit studio` serves an API on `127.0.0.1:4983`; the UI is a hosted page calling it | None | None: wildcard CORS and `Access-Control-Allow-Private-Network: true` | Not detected |
| Prisma Studio | Serves its own UI locally and opens a browser | None | Fixed in 7.10 (August 2026, PR 29890) after "a malicious website visited while Studio was open could read or modify the connected database": binds `127.0.0.1`, no CORS, foreign `Origin` refused | Another port |
| Supabase local | Studio and pg-meta in Docker | "Studio, pgMeta (/pg/*), and analytics have no authentication"; binds `0.0.0.0` | None | Finds the running stack by container label |
| Jupyter Server | Serves its UI on `localhost` | Per-launch token; opens the browser on a `0600` file that redirects with the token, which replaced a single-use token in argv (notebook 5.7.3) | `Host` check on loopback; no CORS; foreign `Origin` refused | Runtime files (`0600`, `0700` directory) list running servers |
| Codex | The desktop app runs its own app-server as a child; `codex app` only opens it with `codex://threads/new?path=…`; the daemon uses a `0600` Unix socket in a per-user `0700` directory | Socket permissions; non-loopback `ws://` requires a token given by file or digest | Any `Origin` refused | Lock plus connect probe; per-thread writer locks are empty files held with `try_lock` |
| kubectl | `kubectl proxy` holds kubeconfig credentials; exec credential plugins return short-lived tokens to the client | kubeconfig bindings | `--accept-hosts` defaults to loopback | Not applicable |
| Ollama, Vite, webpack-dev-server | Local HTTP servers | None by default | `Host` allowlists after DNS-rebinding advisories (CVE-2024-28224, CVE-2025-24010); webpack stopped relying on `Sec-Fetch-*` (CVE-2026-6402) and hardened header parsing (CVE-2026-14631) | Not applicable |

No surveyed database tool authenticates its local UI or defaults to read-only.
This design takes Jupyter's private runtime record and browser defence,
Prisma's fix, Codex's thin launcher, app-owned child process, per-user
directory and lock-based liveness, and kubectl's split between shared
configuration and credentials obtained through the owning tool.

## Evidence and tests

Extend the existing owners in [the testing map](../dev/testing.md):

- **Server, in-source `ingress` tests:** every route `build_app` registers is
  classified, enumerated from the router; a local read-only state refuses each
  `Write`-class route before its body.
- **Server, `integration/auth_policy.rs`:** the session token resolves to the
  session actor; a read-only session's engine refuses a write even where a route
  would admit it; `ReadWrite` refuses without an actor and for a cluster graph
  before any open; no local state runs open or default-deny; the generated
  policy's graph id matches the route's.
- **Server, `integration/mcp.rs`:** a read-only session lists no write tools
  and refuses one as unknown.
- **Server, `boot_settings.rs`:** `serve_local` refuses a non-loopback
  listener; shutdown drains admitted writes; uncertain completion exits 2.
- **Cluster:** a session on a cluster graph leaves `__cluster/lock.json`
  untouched.
- **CLI, a new `cli_sessions` integration module** (a new public boundary):
  session start, the JSON line and the record; the guard's `Host`, `Origin`,
  CORS and malformed-header cases; read-only refusals; `actor_required`,
  `cluster_graph_read_only`, `session_exists`, `--reuse` and
  `session_access_mismatch`; the notice to direct writers, with their outcome
  unchanged; `--session` with and without an actor match and its
  unknown-outcome reporting; a dead owner's record ignored with no process-ID
  check; directory ownership and mode refusals; `mcp` relaying, hosting and
  filtering write tools without `--write`; `studio` opening its link through a
  substituted opener and refusing when Studio is absent.
- **CLI, `integration/cli_schema_config.rs`:** `credential` over a pipe from the
  target-keyed variable and the credentials file, never `OMNIGRAPH_BEARER_TOKEN`;
  `credential_terminal` under a pseudo-terminal; `credential_kind` and
  `credential_missing`.

Run CLI integration tests after building `omnigraph-server`, with
`RUST_MIN_STACK=16777216`. No cost instrument is needed; sessions add no storage
I/O to existing paths.

## Changes by repository

This RFC is decided and implemented in ModernRelay/omnigraph. Other
repositories' changes are listed as dependencies, made and reviewed there.
Appendix A lists the changes kept out of this scope.

### ModernRelay/omnigraph

| Area | Change |
|---|---|
| `omnigraph-server` | `AppState::local`, `LocalOptions`, `SessionCredential`; `serve_local` with the lifecycle extracted from `serve_config`; local states mount no `/cluster/*` routes and cap MCP write tools by access; read-only refusal in `ingress::admit`. |
| `omnigraph-cli` | `serve` (with `--write`, `--reuse`, `--port`, `--json`), `studio`, `mcp`, `credential` and `--session`, classified in `planes.rs`; the loopback guard; session records, reusing the `coordination.rs` checks; the notice to direct writers; the MCP bridge with its `readOnlyHint` filter; the platform opener. |
| Docs | Amend the support boundary in [invariants](../dev/invariants.md); a user page for local sessions; the CLI reference; the server authentication page's self-hosting settings; the MCP guide's `omnigraph mcp`; the public embedding APIs in [control plane](../dev/control-plane.md); new owners in [testing](../dev/testing.md); a disposition note on RFC 0003 for the stdio bridge it deferred; `changelog.d` notes; new rows in the bundled skill's command index, which a drift test requires. |

### ModernRelay/omnigraph-studio

| Area | Change |
|---|---|
| Sessions | Run `omnigraph serve --target NAME --reuse --json` with the login-shell environment captured once; attach to the session in its line; stop a child session on close; treat a vanished session or exit 2 as an unknown outcome and never resubmit. Offer "Open read-write" as a deliberate action. |
| Links | Handle `open?target=<name>[&graph=<id>]`, read-only; drop the unshipped `connect` route; register the scheme on Linux (`MimeType=x-scheme-handler/omnigraph-studio;`, `Exec=omnigraph-studio %u`) and dispatch `open_urls` on GPUI Linux, which stores the callback without calling it. |
| Static tokens | Obtain them with `omnigraph credential --json`, keep them in memory, and run it again after a 401. |
| Connections | Remove the operator-config and credentials parsers (`graph_store/src/cli_config.rs`) and the name-based token lookup. The target picker waits for the listing in [A7](#a7-rfc-0059-additions); the launcher path does not need it. |
| Access | Show a session's access from its line and hide write affordances in a read-only session. |
| Agents | Give agents `omnigraph mcp --target`, or point the agent gateway at the session. |
| Specs | Rewrite spec 012's E1–E5 as this RFC and update specs 002 (links), 003 (connections), 011 and 013 (agents). |

### The managed control plane

No change in this scope. Managed access needs control-plane changes,
summarized in [A4](#a4-oauth-for-standard-clients-and-managed-access) and
tracked in that repository.

## Rollout

1. **Server foundations:** `AppState::local`, `serve_local`, read-only
   enforcement. `implementation: in-progress`.
2. **Read-only sessions:** `serve`, records, the guard, `--reuse`, the
   `studio` launcher, user docs and a changelog note.
3. **Read-write sessions:** `--write`, `--session` and the notice to direct
   writers.
4. **`credential` and `omnigraph mcp`.** `implementation: complete`.

Studio's steps follow each phase in its repository.

## Unresolved questions

None blocks acceptance of this scope beyond RFC 0059. Appendix A lists the
decisions deliberately left to their own RFCs and issues.

## Appendix A: changes for separate decisions

These changes came out of the same analysis but have a wider blast radius than
this RFC: they change every writer, every client of every server, a public
authorization surface, or another repository's security posture. Each needs its
own decision. None is required for the scope above to ship.

| | Change | Why it is separate | Recommendation |
|---|---|---|---|
| A1 | Cross-process writer safety | Changes the engine's control protocols or every direct write; the gap exists without Studio | New RFC (engine); issue [#1018](https://github.com/ModernRelay/omnigraph/issues/1018) |
| A2 | Credential introspection | A new public authorization surface on every server, including managed | New RFC; issue [#1019](https://github.com/ModernRelay/omnigraph/issues/1019) |
| A3 | `WWW-Authenticate` on every 401 | Changes every protected route's 401 for every client | Issue [#1020](https://github.com/ModernRelay/omnigraph/issues/1020), as RFC 0003's named follow-up |
| A4 | OAuth for standard clients and managed access | Security-sensitive and cross-repository, gated on the managed control plane's own qualification | Control-plane and Studio spec changes; tracking issue [#1025](https://github.com/ModernRelay/omnigraph/issues/1025) |
| A5 | MCP relay for managed targets | Lends the CLI's native managed authority to local agents | Decided after A4, in [#1025](https://github.com/ModernRelay/omnigraph/issues/1025) |
| A6 | Retire the test-shaped constructors | Public API removal with test churn across crates | Issue [#1021](https://github.com/ModernRelay/omnigraph/issues/1021), after `AppState::local` lands |
| A7 | RFC 0059 additions | Another author's open RFC | [Comment on PR #675](https://github.com/ModernRelay/omnigraph/pull/675#issuecomment-6223864972) |
| A8 | Defects found | Bugs, independent of this design | One issue each, linked in A8 |

### A1. Cross-process writer safety

The engine supports schema apply, branch creation and deletion, and cleanup from
one writer process only. Its write queues and schema gate are process-local,
and its comment gives the reason: "a separately-opened handle can apply a
schema or delete refs, which must serialize with a live writer". Two terminals,
two agents, or an agent beside a session can break that boundary today with no
warning, and row writes across processes can return `ReadSetChanged` after
effects.

Two designs were considered. The first makes each of those operations safe
across processes in the engine, conditioning its effect on the manifest version
or native-ref CAS and revalidating under publication, as row writes already do.
That protects every writer, on every machine. The second is a cooperative
machine-local writer lock, shared for row writes and exclusive for the
operations above, with each operation's class declared once by the engine. This
RFC's previous revision carried the lock; it protects only cooperating
processes of one account on one machine, and it changes every direct write.

**Recommendation:** an engine RFC preferring the first design. Issue
[#1018](https://github.com/ModernRelay/omnigraph/issues/1018) records the gap with the comment quoted above.

### A2. Credential introspection

`GET /graphs/{graph_id}/access?branch=<branch>` would tell an authenticated
caller which graph actions its own credential allows on a branch, for example
`{"actor":"act-me","branch":"main","actions":["read","export","change"]}`. It
would evaluate the installed policy for the caller's actor and each action,
revealing nothing about other actors or the policy's rules. Static tokens
without a policy would get `["read"]`, and signed data credentials without one
`[]`, matching `authorize`. Local sessions would answer it from their generated
policy, so a client reads access the same way everywhere. Without it, a client
shows every action and learns a limit from a 403, which is the common case for
self-hosted static tokens without a policy. The server and the managed control
plane each list this as a gap; it is Studio spec 012's E15.

**Recommendation:** a new RFC, since it is a lasting public authorization
contract; issue [#1019](https://github.com/ModernRelay/omnigraph/issues/1019).

### A3. `WWW-Authenticate` on every 401

The server accepts OIDC resource tokens on every protected route but sends the
OAuth challenge only from `/mcp`. Every 401 would carry
`WWW-Authenticate: Bearer resource_metadata="<url>"` with OIDC configured (plus
`error="invalid_token"` when a presented bearer was refused), and a bare
`WWW-Authenticate: Bearer` otherwise, as RFC 9110 §15.5.2 asks. The challenge
would move into the authentication middleware every route shares, so REST and
`/mcp` cannot drift. A client can meanwhile fetch
`/.well-known/oauth-protected-resource` directly.

**Recommendation:** issue [#1020](https://github.com/ModernRelay/omnigraph/issues/1020), as the "server-wide follow-up"
RFC 0003 names, with a short amendment to RFC 0003.

### A4. OAuth for standard clients and managed access

A client other than the CLI should reach an OIDC-enabled server, self-hosted or
managed, as a standard OAuth public client rather than borrow the CLI's
credentials. A managed data token is a bearer credential valid for up to 24
hours that removing admission does not retract (the managed-data guide and
RFC 0053 describe both), and credential families stay separate. The client
flow:

1. Fetch the protected-resource metadata; take `resource` and
   `authorization_servers`; refuse an issuer the metadata does not name.
2. Open the system browser with a `client_id` that is a Client ID Metadata
   Document URL (pre-registration as the fallback, never dynamic
   registration), a loopback `redirect_uri` (RFC 8252 §7.3), PKCE with S256, a
   `state`, and `resource=<resource>` (RFC 8707).
3. Exchange the code with its verifier and the resource; keep the refresh token
   in the client's keychain under issuer, resource and account, and the access
   token, at most 300 seconds long, in memory.
4. Refresh with the resource every time; without it an authorization server
   may issue a token for a default audience.
5. On a 401, refresh, then sign in again; a 403 never triggers a sign-in.

The managed control plane would need to enable and qualify a standard OAuth
resource profile, register Studio as a public client with loopback redirect
URIs, expose the protected-resource metadata and `/mcp` on public data hosts,
along with `/openapi.json`, which Studio reads to detect optional routes, and
start managed servers with OIDC identity trust and their public host allowed
for MCP. Studio would implement the client.

**Recommendation:** control-plane and Studio spec changes, tracked in
[#1025](https://github.com/ModernRelay/omnigraph/issues/1025). No OmniGraph RFC is needed unless the server contract
changes beyond A3.

### A5. MCP relay for managed targets

`omnigraph mcp` could relay to a managed cluster's `/mcp` with the CLI's own
data credential. The token would never leave the CLI, but every local agent
using the bridge would act with the CLI's managed authority, and managed public
hosts do not route `/mcp` today.

**Recommendation:** decide after A4, as the last item of
[#1025](https://github.com/ModernRelay/omnigraph/issues/1025): whether managed agents should use the bridge or be OAuth
clients themselves.

### A6. Retire the test-shaped constructors

Once `AppState::local` exists, the eleven convenience constructors (`new`,
`new_with_bearer_token`, `new_with_bearer_tokens`,
`new_with_bearer_tokens_and_policy`, `new_with_workload`, `new_single`, `open`,
`open_with_bearer_token`, `open_with_bearer_tokens`,
`open_with_bearer_tokens_and_policy`, `open_single_with_queries`) can be
deleted. Only tests and deferred benches call them, including
`omnigraph-gqt-served`; `open_single_with_queries` also compiles its policy
against the graph URI while the route id is `default`.

**Recommendation:** issue [#1021](https://github.com/ModernRelay/omnigraph/issues/1021), done after this RFC's first phase.

### A7. RFC 0059 additions

- Rows for `serve`, `studio`, `mcp`, `credential` and `--session` in its
  applicability table, with `--session` defined as an explicit access path,
  never a destination selector.
- A read-only `omnigraph targets --json` listing names, kinds, canonical roots,
  endpoints, default graphs and authentication kinds, without secrets. Studio's
  target picker and agents both need it.
- A minimal structured target writer (`omnigraph targets add` and `remove`), so
  the CLI stays the only writer of operator configuration when a GUI adds a
  server; RFC 0059 rejects target-administration groups today.
- A server target's authentication binding that the listing can report as
  `static`.
- No transition period or legacy adapters.

**Recommendation:** a [comment on PR #675](https://github.com/ModernRelay/omnigraph/pull/675#issuecomment-6223864972).

### A8. Defects found

| Issue | Defect |
|---|---|
| [#1022](https://github.com/ModernRelay/omnigraph/issues/1022) | The CLI sends `OMNIGRAPH_BEARER_TOKEN` to any remote URL, including an unregistered literal one. |
| [#1023](https://github.com/ModernRelay/omnigraph/issues/1023) | `commit publish` and `commit drop` pass `--as` to the engine, but the plane guard rejects `--as` for every `commit` command. |
| [#1024](https://github.com/ModernRelay/omnigraph/issues/1024) | The credentials file's directory is created with default permissions, not `0700`. |
| [omnigraph-studio#2](https://github.com/ModernRelay/omnigraph-studio/issues/2) | The agent gateway checks neither `Host` nor `Origin`. |

## Decision log

- 2026-10-15: First draft, "Studio launcher and local graph sessions", from
  Studio spec 012 (E1–E5) and issue #980, reconciled with PR #850.
- 2026-10-16: Rewritten after checking every assumption against OmniGraph
  main, the managed control plane, Studio and prior art, and consolidated with the
  client-access design. The connect code, handoff files, redemption route and
  attach key became session records; the session became a general local
  serving primitive; Studio became a standard OAuth client instead of borrowing
  managed data tokens; static tokens stayed first-class through the CLI's store.
- 2026-10-16: Narrowed to what ships alone. The cooperative writer lock was
  removed; only the record's liveness lock remains, and cross-process writer
  safety moved to A1. The launcher became thin, as `codex app` is: it opens
  `open?target=` and exits, Studio owns its sessions through
  `serve --reuse`, and links open read-only. The access route, the server-wide
  challenge, OAuth and managed access, the managed MCP relay, the constructor
  removal and the RFC 0059 additions moved to Appendix A with a recommended RFC
  or issue each.
- 2026-10-16: Linked the Appendix A issues and the comment on PR #675, and
  described the managed control plane by what it must change rather than by
  its internal specs.
