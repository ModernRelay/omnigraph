---
rfc: "2026-10-16-local-sessions-and-client-access"
title: "Local graph servers and client access"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-16
updated: 2026-10-17
discussion: https://github.com/ModernRelay/omnigraph/issues/980
supersedes: []
superseded_by: []
blocked_on:
  - "Named targets and CLI coherency (RFC 0059, PR #675) must be accepted: every command here addresses graphs only through `--target`, and this RFC defines the one target writer RFC 0059 does not yet allow."
---

# RFC: Local graph servers and client access

> A term set in ***bold italics*** is defined at that spot; it is plain text afterwards.

## Summary

This RFC lets a person, and the agents working with them, open a graph in
OmniGraph Studio and in MCP clients with the identity and authorization the CLI
gives. Its scope is the part that can ship on its own; changes with a wider
blast radius are in [Appendix A](#appendix-a-changes-for-separate-decisions),
each with a recommended RFC or issue.

1. **Local graph servers.** A ***local server*** is one `omnigraph` process
   that serves one directly accessed graph on loopback, through the HTTP API
   and `/mcp`, to clients of the same OS account on the same machine:
   `omnigraph-server` serves clusters to anyone holding a token, and
   `omnigraph serve` serves one graph to you. `omnigraph serve --target NAME`
   starts one. It is read-only unless `--write` is given, never
   unauthenticated, never holds cluster admission, and installs a generated
   Cedar policy on the engine. Its ***server record*** names its URL, token,
   access mode and actor, so a same-account client attaches by reading it. A
   graph has at most one local server, found through its record, and
   `omnigraph targets --json` lists the live ones.
2. **A thin Studio launcher.** `omnigraph studio --target NAME` opens
   `omnigraph-studio://open?target=NAME` and exits, as `codex app` opens the
   Codex app. Studio owns its local servers: it runs
   `omnigraph serve --target NAME --reuse --json`, which attaches to the
   graph's live local server or starts one as Studio's child. Links carry no
   secret, no path and no request for write access.
3. **MCP for agents.** `omnigraph mcp --target NAME` is a stdio MCP endpoint for
   local graphs and static-token servers, so no agent configuration holds a
   token. Write tools need the endpoint's permission and the agent's own
   `--write`. Each place an agent runs has one write path
   ([Which path an agent uses](#which-path-an-agent-uses)).
4. **One registry of targets and static tokens.** The CLI stays the only
   resolver and writer of the operator's targets and static tokens.
   `omnigraph targets` lists, adds and removes targets, and
   `omnigraph credential --target NAME --json` hands a server target's stored
   static token to a calling process. Studio reads neither file itself, so a
   self-hosted server with static tokens works in Studio, its agents and the CLI
   alike.

The `omnigraph-server` binary stays cluster-only, and the storage format and
the HTTP API do not change.

## Motivation

Every fact below was checked against OmniGraph main (`02026b5b`), the managed
control plane and OmniGraph Studio on 2026-10-16 and 2026-10-17.

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
creation and deletion, and cleanup from one writer process only ([support
boundaries](../dev/invariants.md#current-support-boundaries)). A local server
gives every client that attaches to it one writer process. The gap that remains
for processes that do not attach exists today and is [Appendix
A1](#a1-cross-process-writer-safety).

**A loopback port is exposed.** Every local process can reach it, and web pages
can through DNS rebinding. The REST routes have no `Host` check and no CORS
layer; `/mcp` checks host names but not the port.

**Clients keep their own registries, and they drift.** Studio parses the CLI's
`config.yaml` and `credentials` file itself, finds a token by server name where
the CLI sends one only for a URL under a declared server (since #1030), and
keeps a second list of servers it adds, with tokens in its own keychain. RFC
0059 replaces the configuration format. A GUI launched from Finder sees
launchd's environment, not the shell's, so environment tokens are invisible to
it.

**Static tokens must stay first-class.** They are how people host their own
graphs: no identity provider and no admission-snapshot publisher.

This needs an RFC: it adds CLI commands, a persisted format, a public Rust API,
an authorization boundary, an amendment to the cluster-only support boundary,
and an exception to RFC 0059's rule against target-administration commands.

## User and operational behavior

### Commands

```bash
omnigraph serve --target local                        # read-only local server
omnigraph serve --target local --write                # read-write, as operator.actor
omnigraph serve --target brain --graph people         # cluster graph: read-only
omnigraph studio --target local                       # open a graph in Studio
omnigraph mutate --target local --via-server -e '…'   # write through the live local server
omnigraph mcp --target local                          # stdio MCP, read tools
omnigraph mcp --target prod --graph people --write    # stdio MCP with write tools
omnigraph targets --json                              # targets, aliases, live local servers; no secrets
omnigraph targets add staging --json < entry.json     # the CLI writes the config
omnigraph targets remove staging                      # and removes an entry
omnigraph credential --target prod --json | …         # a target's static token, to a pipe
```

Every command here selects its destination as RFC 0059 does, `--target`, else
`defaults.target`, else an error, takes no legacy selector during RFC 0059's
compatibility interval, and refuses in a legacy-format operator home
(`legacy_operator_config`). A graph-scoped kind needs `--graph` or the target's
`default_graph`; a direct graph target already names its graph and rejects
`--graph`. A command refuses a target kind its column below marks refused
(`target_kind_unsupported`); [Refusals](#refusals) lists every code.

| Target kind (RFC 0059) | `serve` | `studio` | `mcp` | `credential` |
|---|---|---|---|---|
| Direct graph, outside a cluster | Read-only, or read-write with `--write` | Opens Studio, which attaches to or starts a local server | Relays to the live local server, or hosts one | Refused: a local server's token is in its record |
| Direct root with `--graph`, or a direct graph under a cluster root (a cluster graph) | Read-only only | As above, read-only | As above, read-only | Refused |
| Direct catalogue (`og_catalog`) | Refused | Refused | Refused | Refused |
| Server with a token owner | Refused: the graph already has a server | Opens Studio on it | Relays to the server's `/mcp` with the owner's token | Returns the owner's token |
| Server with anonymous access | Refused | Opens Studio on it | Relays without a token | Refused (`credential_anonymous`) |
| Server on a Unix socket | Refused | Refused (`studio_transport_unsupported`): Studio's client speaks TCP | Relays over the socket | Returns the owner's token |
| Managed cluster | Refused | Refused until [A4](#a4-oauth-for-standard-clients-and-managed-access) | Refused until [A5](#a5-mcp-relay-for-managed-targets) | Refused: managed credentials stay the CLI's |
| Managed account | Refused | Refused | Refused | Refused |

A local server on a cluster graph is the direct-storage view, the same view
direct CLI reads have. It applies none of the cluster's Cedar policy, stored
queries, embedding-provider bindings or external-Blob policy, and its startup
line says so, naming the cluster's server target as the served view when one
exists.

### Access and actor

A local server is read-only unless `--write` is given. `--write` needs an actor,
from `--as` or else `operator.actor`, and refuses before opening anything
without one (`actor_required`); it refuses for a cluster graph
(`cluster_graph_read_only`). A read-only local server uses `operator.actor` when
it is set and otherwise the actor `default`, the actor a single unnamed server
token already resolves to.

| | Read-only | Read-write |
|---|---|---|
| Granted | `read`, `export` on every branch and graph-wide; `graph_list` | Also `change`, `branch_create`, `branch_delete`, `branch_merge`, `commit_publish`, `commit_drop` |
| Never granted | `schema_apply`, `admin`, `config_manage`, `invoke_query`, and any action not named above | The same |
| Write routes | 403 `forbidden`, the code a policy refusal carries, before the body is read | Admitted |
| `/mcp` | Read tools only | Read and write tools |

- **Every action is classified.** The generated policy decides each
  `PolicyAction` by an exhaustive match, so an action added later is never
  granted until an RFC names its local-server class. Read-only therefore holds
  even if maintenance or schema apply later arrive through `/mutate`.
- **One actor per local server.** Studio, its agents and terminal agents that
  attach all write as the local server's actor. Commits, batched commits and
  branch creators cannot tell them apart.
- **Previews are routed as writes.** A merge `dry_run` is documented as a read
  of both branches but sent to `/branches/merge`, which ingress classifies as a
  write before reading the body, so a read-only local server refuses it. A
  future preview needs a read-class route to work there.
- **Schema.** No local server mounts deployment, schema-apply or maintenance
  routes; schema apply and maintenance stay direct CLI commands. A read-write
  local server's merge can still carry a newer schema into a branch once
  [schema apply with live branches](https://github.com/ModernRelay/omnigraph/pull/963)
  lands, depending on that RFC's answer to whether `branch_merge` suffices.
- **Actor views.** On a graph that declares
  [actor views](https://github.com/ModernRelay/omnigraph/pull/951), the
  generated policy is a version-2 bundle that places the local server's actor
  in the views' bypass, as anyone holding storage credentials already sees
  everything, and the startup line says views are not applied. `--write`
  refuses there (`actor_views_read_only`) until that RFC's write phase, and the
  `default` fallback applies only to graphs without actor views.

### Lifetime and output

A local server runs in the foreground until interrupted, or until its parent
closes it, and shuts down through the server's bounded shutdown. An uncertain
write completion makes it exit 2, as `omnigraph-server` does; nothing replays
the write. Logs and the startup line (the resolved target, access mode, actor
and URL) go to stderr. A local server never exports traces, even when the
environment names an OTLP endpoint. With `--json`, stdout carries exactly one
line, when the local server is ready:

```json
{"protocol":1,"id":"01JA9…","reused":false,"url":"http://127.0.0.1:53211","graph_id":"default","access":"read_write","actor":"act-me","binding":"sha256:9e1c…","token":"<bearer>","record":"/Users/me/Library/Application Support/OmniGraph/servers/01JA9….json"}
```

If that line cannot be delivered, the local server shuts down and removes its
record. `binding` is a digest of the resolved destination, not a secret.
`--port <n>` replaces port 0 for SSH forwarding; the bind address is always
`127.0.0.1`.

**No command here writes a token to a terminal.** The ready line carries
`token` only when stdout is not a terminal; to a terminal it omits the field,
and the record still holds the token. The human banner never shows it,
`credential` refuses a terminal (`credential_terminal`), and `targets` never
carries a token. A caller that needs a token reads it through a pipe, as Studio
and `omnigraph mcp` do.

### One local server per graph

Before starting, `serve` looks for a live local server with the same canonical
root:

- Without `--reuse`, it refuses with `local_server_exists`, naming it.
- With `--reuse`, it prints the live local server's line with `"reused": true`
  and exits 0, unless `--write` was asked of a read-only one
  (`local_server_access_mismatch`) or its CLI speaks another HTTP contract or
  cannot serve the graph's storage format (`local_server_cli_incompatible`).
  Studio and `omnigraph mcp` always pass `--reuse`.

Two launches racing can still start two local servers; both stay correct for
rows through the manifest CAS, as two CLI processes are today.

When a direct write command targets a root with a live read-write local
server, it prints one notice to stderr naming the local server and giving the
same command with `--via-server` added, so a person or an agent switches in one
step. With `--json` the notice is one JSON object on stderr,
`{"notice":"local_server_live","id":"01JA9…","command":["omnigraph","mutate","--target","local","--via-server","-e","…"]}`,
whose `command` is the argument vector, not a shell string. `cleanup` and
`optimize` print a notice when any local server holds their root. A notice
changes no outcome; a cleanup can remove versions beneath a local server's
in-flight reads, which then fail loudly.

`--via-server` on a data write command (load, mutate, branch create, delete and
merge, commit publish and drop, Blob put and clear) sends the operation over
HTTP to the root's live read-write local server with its token. It refuses
before any request when no local server is live for the root
(`local_server_absent`) or the live one is read-only (`local_server_read_only`).
A served write's actor is the server's, so `--as` is only an assertion that
must equal the local server's actor (`local_server_actor_mismatch`);
`--via-server` cannot change attribution. Outcomes are the served ones: a
refusal before effects is "not started"; a lost response is unknown and never
replayed. Nothing routes a command through a local server unless
`--via-server` asks.

### Opening Studio

`omnigraph studio --target NAME [--graph G]` validates the target, opens
`omnigraph-studio://open?target=NAME&binding=<digest>[&graph=G]`, prints the
link and exits. It owns nothing at runtime, so its one contract with Studio is
the link format, which Studio owns.

- On macOS it opens Studio by bundle identifier (`open -b`). On Linux and
  Windows it opens the link only when the platform names a handler for the
  scheme (`xdg-mime query default x-scheme-handler/omnigraph-studio`, or the
  scheme's registry association), because `xdg-open`'s generic path hands an
  unhandled scheme to a web browser. Without Studio or a handler it prints the
  link and how to install Studio, and refuses (`studio_unavailable`), until
  [installing Studio](#installing-studio) ships. It never opens a browser.
- With `--json` it prints
  `{"protocol":1,"target":"local","binding":"sha256:…","link":"omnigraph-studio://open?target=local&binding=sha256:…"}`.
- **Studio resolves the target itself**, through the CLI it chose, and checks
  that its resolution has the link's `binding`. A different digest means the
  link was made under another operator home, and Studio refuses it rather than
  open another graph under the same name. A link never names an executable.
- For a direct target Studio runs `omnigraph serve --target NAME --reuse --json`
  and attaches to the local server in the line: a live one started by a
  terminal, an agent or Studio, or a new one that is Studio's child. For a
  server target it runs `omnigraph credential` and connects directly.
- A link opens read-only. Read-write is a deliberate action inside Studio, which
  runs `serve --write --reuse`, or a local server the person started with
  `omnigraph serve --write`, which Studio attaches to. A link never writes, as
  Studio's own link rules require.
- **Environment.** Studio passes the environment of an interactive login shell
  (`$SHELL -i -l`), captured once, to the `serve` and `credential` it runs, and
  to nothing else; agents never receive it. A login shell that is not
  interactive skips `.zshrc`, where people export keys. A local server needs
  that environment for its storage provider's credentials and for text search:
  the embedding provider comes from `OMNIGRAPH_EMBED_PROVIDER`,
  `OMNIGRAPH_EMBED_BASE_URL`, `OMNIGRAPH_EMBED_MODEL` and its timeout and retry
  settings, with `OPENAI_API_KEY`, `OPENROUTER_API_KEY` or `GEMINI_API_KEY`. A
  model that differs from the one a schema's `@embed` records refuses `nearest`
  with text loudly.
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
| `serve`, `mcp`, `targets`, `credential`, `--via-server` | Supported | Supported | Supported |
| Studio opens a link from `omnigraph studio` | Supported | Once Studio registers its scheme and GPUI's Linux platform dispatches opened links | Once Studio registers its scheme and forwards a second launch's link to the running instance |
| Studio's agent panel | Supported | Supported | Not yet available, as Studio's README says |
| Installing Studio (phase 5) | A verified, signed disk image | Prints how to install | Opens Studio's installer once one ships |

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
- **A 401 while relaying** makes the bridge read the record again (a restarted
  local server) or resolve the owner's token again, once; a second 401 ends the
  bridge with a typed error.

Write tools need two consents. The endpoint is the ceiling: a read-only local
server, or a server without `--mcp-allow-writes`, lists no write tools. The
agent's own configuration is the other: without `--write`, the bridge lists and
admits only tools the server annotates `readOnlyHint`, which it already sets for
every read tool. A person opening a graph read-write in Studio has not thereby
given every connected agent write access. Tool logic, refusals and the write
contract stay the server's; the bridge adds no tool.

**Agents inside Studio keep Studio's gateway.** The gateway limits a panel
agent's writes to the agent's own branch and can ask the person before each
write; `omnigraph mcp` does neither. Studio points the gateway at the local
server's URL and token instead of at a deployed server, and on a read-only
local server it tells the agent that its branch writes are refused. Every agent
still writes as the local server's actor until
[delegated, branch-scoped credentials](https://github.com/ModernRelay/omnigraph/issues/978)
and actor views' agent identities exist.

### Which path an agent uses

Each place an agent runs has one write path, and the bundled skill, which is
what agents read, states the rule:

| Where the agent runs | Its path | Beside a live read-write local server |
|---|---|---|
| A terminal, using the CLI | Direct commands with `--target` | Adds `--via-server`; the notice on a direct write gives the exact command |
| A terminal, using MCP | `omnigraph mcp --target NAME` | Attaches to it by itself |
| Studio's agent panel | Studio's gateway | Studio points the gateway at it |

An agent finds live local servers through `omnigraph targets --json`, never by
reading the records directory. A direct write beside a live local server gets a
notice, not a refusal: a record is a hint no write path trusts, and refusing on
it would make a discovered file a precondition of every direct write without
protecting processes that never look ([A1](#a1-cross-process-writer-safety)).

### Targets for clients

Every server a client uses is a target in the operator configuration, and the
CLI is that file's only program writer. Studio, the agents it runs and terminal
agents then see the same servers, and each stored token exists once.

`omnigraph targets --json` lists the targets and aliases RFC 0059's resolver
reads, and the live local servers of direct targets, without secrets and
without a network request:

```json
{"protocol":1,"default_target":"staging",
 "targets":[
  {"name":"local","kind":"direct","scope":"graph","root":"file:///Users/me/company.omni","binding":"sha256:…",
   "local_servers":[{"id":"01JA9…","url":"http://127.0.0.1:53211","access":"read_only","actor":"default"}]},
  {"name":"brain","kind":"direct","scope":"root","root":"s3://bucket/brain","binding":"sha256:…",
   "local_servers":[{"id":"01JAB…","graph":"people","url":"http://127.0.0.1:53340","access":"read_only","actor":"default"}]},
  {"name":"staging","kind":"server","endpoint":"https://staging.example","default_graph":"knowledge","auth":"token","binding":"sha256:…"},
  {"name":"public","kind":"server","endpoint":"https://public.example","auth":"anonymous","binding":"sha256:…"},
  {"name":"cloud","kind":"managed","scope":"cluster","endpoint":"https://…","default_graph":"knowledge","auth":"managed","binding":"sha256:…"}],
 "aliases":[{"name":"people","target":"staging","graph":"knowledge","query":"list_people"}]}
```

- A direct target always carries `local_servers`, empty when none is live: each
  live local server whose canonical root is the target's root or, for a
  root-scoped target, a graph under it, which `graph` names. Liveness comes
  from each record's lock, as `serve` decides it, and a record from another
  machine is never listed. Server and managed targets carry no
  `local_servers`.
- The listing never carries a token. A client that needs one runs
  `serve --reuse --json`, which returns the live local server's line.

`omnigraph targets add NAME --json` reads one target entry, the same fields
the operator configuration holds, on standard input; `omnigraph targets remove
NAME` removes one. Both:

- validate through RFC 0059's resolver: a complete binding, an endpoint scope
  for the credential owner (`target_invalid`), and no duplicate name
  (`target_exists`); `remove` refuses a name it does not hold
  (`target_unknown`);
- change only that entry, keep the rest of the file's text, write through a
  temporary file and rename, and refuse (`config_not_editable`) when they cannot
  do that safely;
- never change `defaults.target`, never store a secret, and refuse in a
  legacy-format home. A token is stored with `omnigraph login --target NAME`,
  which reads it on standard input;
- `remove` refuses the default target (`target_is_default`) and leaves its
  credential, which `omnigraph logout --target NAME` removes.

RFC 0059 says "Add no target administration group" and "Register targets by
editing operator config". This RFC makes `targets` the one exception: a GUI
must add servers without becoming a second writer of a strictly validated
file, and a listing gives Studio and agents one reading of it. It adds no
selection command and no current-target state, which is what that rule
protects against; `git config`, `kubectl config set-context` and `gh config
set` play the same part for their tools.

### Static tokens for clients

`omnigraph credential --target NAME --json` returns a server target's stored
static token:

```json
{"protocol":1,"target":"staging","owner":"staging","endpoint":"https://staging.example","scheme":"bearer","token":"<static token>","source":"credentials_file"}
```

- It resolves the target's credential owner as the CLI does since #1030 and
  RFC 0059: `OMNIGRAPH_TOKEN_<OWNER>`, then the owner's section of the `0600`
  credentials file, checked against the owner's endpoint scope before either is
  read. `source` is `environment` or `credentials_file`. The CLI reads no
  unkeyed token.
- It refuses when stdout is a terminal (`credential_terminal`), so a token never
  lands in scrollback; a caller reads it through a pipe, the rule
  [Lifetime and output](#lifetime-and-output) states for every command here.
- It refuses a target that is not a server (`target_kind_unsupported`), an
  anonymous server (`credential_anonymous`) and a target without a stored
  token (`credential_missing`).
- A client sends the token only to URLs under `endpoint`, the rule the CLI
  applies to its own requests.

Studio runs `credential` again after a 401, so a token rotated with
`omnigraph login --target NAME` reaches it without a restart.

### Self-hosted servers with static tokens

A person hosting their own graphs runs `omnigraph-server --cluster …` with
static tokens and adds the server as a target. Four server settings decide how
complete a client's experience is:

| What the person wants | What the server needs |
|---|---|
| Edits attributed to people | Named actors through `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON`, `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` or `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET`. A single `OMNIGRAPH_SERVER_BEARER_TOKEN` is the actor `default`. |
| Edits at all | An applied graph policy granting the actor its write actions. Without one a static token only reads, and today a client learns that from its first refused write ([A2](#a2-credential-introspection)). |
| Listing the server's graphs | `graph_list` in the cluster policy. Without it a client opens the target's `default_graph` or the graph the person names; `/graphs/discovery` serves verified identities only. |
| Agents over MCP | `--mcp-allowed-host <public host>`, because `/mcp` admits only loopback `Host` values by default (OIDC identity trust admits its resource's host by itself), and `--mcp-allow-writes` for write tools. |

### Refusals

Every refusal here has a stable snake_case code and happens before any effect.
In JSON the code is a CLI-local `refusal` field beside the CLI's existing
failure fields, as `missing_credential` is, because the HTTP API's `code`
enumerates server codes only. A data write command with `--via-server` reports
its refusal in the command-outcome envelope it already prints, with
`execution: not_started`, `effects: none` and `action: refresh`. Every refusal
exits 1. A local server that stops with an uncertain write exits 2, and a
`--via-server` write whose response was lost reports `execution: unknown`,
`effects: unknown` and `action: reconcile`, and is never replayed.

| Code | Commands | When |
|---|---|---|
| `legacy_operator_config` | All here | The operator home has the legacy format |
| `target_kind_unsupported` | `serve`, `studio`, `mcp`, `credential` | The command table marks the target's kind refused |
| `actor_required` | `serve --write`, `mcp --write` | Neither `--as` nor `operator.actor` names an actor |
| `cluster_graph_read_only` | `serve --write`, `mcp --write` | The graph is inside a cluster |
| `actor_views_read_only` | `serve --write`, `mcp --write` | The graph declares actor views |
| `local_server_exists` | `serve` | A live local server holds the graph and `--reuse` was not given |
| `local_server_access_mismatch` | `serve --reuse --write`, `mcp --write` | The live local server is read-only |
| `local_server_cli_incompatible` | `serve --reuse`, `mcp` | The live local server's CLI speaks another HTTP contract or cannot serve the graph's storage format |
| `local_server_records_unsafe` | `serve`, `mcp`, `targets`, `--via-server` | The record directory has another owner, group or other permission bits, or a symlink |
| `local_server_absent` | `--via-server` | No local server is live for the root |
| `local_server_read_only` | `--via-server` | The live local server is read-only |
| `local_server_actor_mismatch` | `--via-server` with `--as` | `--as` differs from the local server's actor |
| `studio_unavailable` | `studio` | Studio is not installed, or nothing on this platform handles its links |
| `studio_transport_unsupported` | `studio` | The server target is on a Unix socket |
| `credential_terminal` | `credential` | Stdout is a terminal |
| `credential_anonymous` | `credential` | The server target uses anonymous access |
| `credential_missing` | `credential` | No token is stored for the target's owner |
| `target_invalid` | `targets add` | The entry fails RFC 0059's resolver |
| `target_exists` | `targets add` | The name is taken |
| `target_unknown` | `targets remove` | No target has the name |
| `target_is_default` | `targets remove` | The target is `defaults.target` |
| `config_not_editable` | `targets add`, `targets remove` | The entry cannot be changed while keeping the rest of the file's text |

A read-only local server's write routes and the loopback guard answer with the
existing HTTP `forbidden` code, so the HTTP API gains no code.

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

/// The local server's bearer token: read once, with no `Debug` or `Display`.
pub struct LocalCredential { /* … */ }

pub enum LocalOpenError {
    ActorRequired,
    ClusterGraphReadOnly,
    ActorViewsReadOnly,
    Open(OmniError),
    Policy(PolicyError),
}

/// `Uncertain` when a write's completion is unknown; callers exit 2.
pub enum ServeOutcome { Clean, Uncertain }

impl AppState {
    pub async fn local(binding: LocalBinding, options: LocalOptions)
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

`AppState::local`:

1. Classifies the root with `cluster_root_for_graph_uri`, the rule direct CLI
   commands use, and refuses `ReadWrite` for a cluster graph, for a graph with
   actor views, or without an actor. It never calls `acquire_graph_admission`.
2. Opens read-only with `Omnigraph::open_read_only`, validated against a
   captured `GraphReadAuthority` for a cluster graph, or read-write with
   `Omnigraph::open`.
3. Generates a 32-byte token and keeps only its SHA-256 hash.
4. Builds the policy as a structured `PolicyConfig` with `PolicyCompiler::compile`
   followed by the kind-alignment validation the policy loaders run, for the
   route's graph id (`default`), deciding every `PolicyAction` by an exhaustive
   match, and installs it with `with_policy`. Branch-scoped grants use an `any`
   scope, which covers graph-wide reads. A one-handle registry carries the
   server policy granting `graph_list`, which a single-mode state cannot. A
   graph with actor views gets the version-2 bundle described above.
5. Mounts no `/cluster/*` route. Lists MCP write tools only for `ReadWrite`.

Read-only holds in three independent places: `ingress::admit` refuses every
`Write`-class request before its body; `/mcp`, which ingress admits as one
read-class route, lists no write tools; and the engine's policy refuses every
write, including one without an actor, which `enforce` denies whenever a policy
is installed. A local state is never in open or default-deny mode.

`serve_local` refuses a listener whose local address is not loopback and runs
the lifecycle `serve_config` runs: the signal and uncertain-owner listener, the
watchdog armed with the shutdown grace, graceful HTTP shutdown and the
logical-owner drain. That lifecycle moves into one private function both entry
points call. It prints nothing to stdout: `serve_config` keeps its
`OMNIGRAPH_LISTEN_ADDR=` line. It returns an uncertain outcome instead of
exiting the process, and the server binary and the CLI map that outcome to exit
2. The caller binds the listener, so it knows the port and writes the record
before announcing readiness. `extend` receives the router `build_app` built from
this state. `install_log_subscriber` makes the private `server_log_subscriber`
installable on a chosen writer, so the CLI logs a local server's events to
stderr with the filters `omnigraph-server` uses.

The existing convenience constructors stay until
[A6](#a6-retire-the-test-shaped-constructors).

### CLI

- **Command structure.** Each command is a `Command` variant in the exhaustive
  `command_plane` and `command_label` matches, with its own module under
  `commands/` and an arm in `app.rs`, which captures the invocation once.
  `--via-server` is an argument of the data write commands that
  `command_outcome::applies` lists, minus `schema apply`, and reuses that
  module's outcome evidence for unknown outcomes. A local server's uncertain
  outcome becomes `Completion::exit(2)`. Refusals carry their code in the
  `refusal` field of the CLI's failure output.
- **Contract versions.** `omnigraph version --json` gains a `protocols` object
  naming the version of each machine-readable contract here (`serve`,
  `studio`, `targets`, `credential`), beside its `http_contract`, `channel`, `commit` and
  `internal_schema`. A client requires the protocol versions it speaks before
  running a command.
- **Loopback guard**, a layer the CLI adds in `extend`, outside every server
  layer, so a deployed server never gains it:
  - `Host` must be exactly `127.0.0.1:PORT`, `localhost:PORT` or `[::1]:PORT`,
    case-insensitively; anything else, or none, is 403. This closes DNS
    rebinding, which `Origin` alone cannot: a rebound page's `GET` is
    same-origin and carries no `Origin`.
  - Any request carrying `Origin` is 403; native clients send none and a
    local server serves no page.
  - No response carries a CORS header; every response carries
    `Cross-Origin-Resource-Policy: same-origin`. Nothing relies on `Sec-Fetch-*`,
    which browsers do not send to plain-HTTP origins.
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
  "binding": "sha256:9e1c…",
  "graph_id": "default",
  "url": "http://127.0.0.1:53211",
  "token": "<32 random bytes, base64url>",
  "access": "read_write",
  "actor": "act-me",
  "pid": 41873,
  "started_at": "2026-10-16T09:12:44Z",
  "cli": {"path": "/opt/homebrew/bin/omnigraph", "version": "0.15.0", "channel": "stable", "commit": "…", "http_contract": "0.13", "internal_schema": {"writes": 16, "serves": [14, 16]}}
}
```

The `.lock` file is a liveness signal, not writer exclusion: it decides only
whether a record's owner is alive. The owner takes it with
`std::fs::File::try_lock`, opens the graph, binds, writes the record (temporary
file and rename) and then announces readiness.

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
record with an unknown `format` is ignored. `root` is canonical, so two targets
naming one graph find the same local server. `cli` is informational; a client
chooses its CLI itself.

## Invariants

- **Trust at the boundary (10).** No client supplies an actor. A local server's
  actor is fixed at start by the direct-path rule, `--as` or else
  `operator.actor` ([CLI addressing](0011-cli-addressing-and-config.md)): the
  process holds storage access and is entitled to it, as a direct `omnigraph
  mutate` is. Every request presents a token the server resolves. The local
  server keeps only its token's hash; the record is a user-only delivery file
  like the CLI's `credentials`. `credential` hands a person's own static token
  to a process running as that person. The engine enforces the generated policy
  for every write.
- **One publication door (2), one view (3), publish once (4).** Local servers
  write through the existing routes and engine; publication is unchanged.
- **Bounded and observable (11).** One local server per root; fixed shapes for
  the record and the JSON lines; server limits and the owned-operation
  lifecycle unchanged; one table of typed refusal codes.
- **One source of truth (12).** The CLI is the only resolver of targets, the
  only program writer of their configuration and the only store of their static
  tokens. A record is a hint whose liveness the operating system establishes;
  no write path trusts its contents.
- **Deny-list.** No writer lock, fencing or exclusion is added; the record's
  lock signals liveness only. No queue, no silent retry, no shadow credential
  store, no cloud-only path.
- **Support boundary.** "The server is cluster-only" becomes: the
  `omnigraph-server` binary is cluster-only; the CLI serves one directly
  accessed graph to same-account loopback clients as a local server. The
  single-writer-process boundary is unchanged
  ([A1](#a1-cross-process-writer-safety)).

## Compatibility and reversibility

Storage, the server binary and the HTTP API are unchanged. New contracts: the
five commands and `--via-server`, the ready line, `studio`, `targets` and
`credential` output (`protocol: 1`), the `protocols` field of `version --json`,
the CLI's `refusal` field and its codes, the record (`format: 1`) and its
directories, the Studio link format, and the local embedding API. Existing
direct commands behave as today; the only addition is the notice when a live
local server holds their root. Studio stops reading the CLI's
files and keeping its own server list.

RFC 0059 (at `323068fa`) keeps a compatibility interval with legacy adapters.
The commands here take only `--target`, refuse in a legacy-format home, and
define the one target writer RFC 0059's text does not yet allow; RFC 0059
should name `targets` when it is accepted.

Reverting removes the commands and the embedding API; no graph data migrates.

## Alternatives

| Alternative | Why not |
|---|---|
| A single-use connect code in the opened link, redeemed over loopback | This RFC's first draft. Jupyter shipped a single-use token in argv and removed it because another logged-in user could read it from the process list; on Linux and Windows a scheme handler receives its URL in argv. It needed a redemption route, attach keys and handoff files; the record replaces all of them. |
| The launcher hosts the local server in its terminal and opens a link to it | An earlier revision. It keeps the terminal's environment, but the launcher then owns runtime state and a second link form. Studio owning its local servers, as the Codex app owns its app-server, gives one owner and one contract; a terminal-hosted `serve` remains available and Studio attaches to it. |
| A cooperative writer lock on every direct write | An earlier revision. It changes every direct write for a gap that exists without Studio, and protects only cooperating processes of one account on one machine. See [A1](#a1-cross-process-writer-safety). |
| A browser UI embedded in the CLI ([PR #850](https://github.com/ModernRelay/omnigraph/pull/850)) | Studio is a native client in its own repository, and embedding a JavaScript build makes a frontend toolchain a prerequisite of every CLI build. Its boot mode takes cluster writer admission and runs unauthenticated. Its point that an embedder must reuse the server lifecycle is adopted as `serve_local`. |
| Serve local graphs unauthenticated on loopback | Any local process or account can write, writes record no actor, and `Host` checks do not stop another local user. |
| Never install Studio from the CLI | Leaves the first launch to a manual step. Installing is safe only with a signature to verify, which is why it waits for Studio's signed releases rather than being dropped. |
| Studio bundles its own engine | A newer engine stamps a branch's storage format forward on first publication, and an older binary refuses a newer stamp. Local servers run the person's own CLI. |
| Route every direct command through a live local server automatically | RFC 0059 forbids discovery from selecting a destination, and [unified access paths](0009-unified-access-paths.md) rejects a localhost funnel for the embedded path. `--via-server` is explicit. |
| Refuse a direct write beside a live read-write local server | A record is a hint no write path trusts. Refusing on it would make a discovered file a precondition of every direct write and break scripts, without protecting processes that never look ([A1](#a1-cross-process-writer-safety)). The notice gives the one-step fix instead. |
| Call it a session | "Session" already names the engine's session settings, the managed control plane's login sessions and the CLI's internal session plane, whose name its messages print. "Local server" says what runs, and `omnigraph serve` reads as the one-graph form of `omnigraph-server`. |
| Records under `/tmp` | macOS deletes `/tmp` files untouched for three days (`com.apple.tmp_cleaner`), and many Linux distributions age `/tmp` out through `systemd-tmpfiles`; a long-running local server would lose its record. |
| Studio parses the CLI's files, or writes them itself | Two resolvers have already drifted, RFC 0059 changes the format and validates it strictly, and a second writer can collide with a person's edit. |
| Studio keeps its own server list and token copies | Agents Studio runs would see other servers than Studio, and rotated tokens would drift. |
| Wait for RFC 0059 to add a target writer | RFC 0059 rejects target-administration commands, and Studio cannot add a server without one. This RFC defines the minimal writer and leaves selection to RFC 0059. |
| Agents inside Studio use `omnigraph mcp` | The gateway limits their writes to their own branch and can ask before each write; the bridge does neither. |
| A CLI proxy for every remote target | A second request path that must stay transparent for streams, headers and outcomes. |
| Unix sockets instead of loopback TCP | No browser can reach a socket and file permissions would authenticate. The CLI already speaks `unix://` to servers (#988), but the server's lifecycle and Studio's client are TCP-only and Windows needs named pipes. A sandbox that blocks loopback does not block stdio, so `omnigraph mcp` already reaches sandboxed agents. A later protocol can add a socket. |
| Liveness from a process ID or an HTTP probe | A bare process ID is reused; a held operating-system lock answers liveness directly. |

### Prior art

Surveyed 2026-10-15 and 2026-10-16 from source, documentation and advisories:

| Tool | Serving and transport | Authentication | Browser defence | Second launch |
|---|---|---|---|---|
| Drizzle Studio | `drizzle-kit studio` serves an API on `127.0.0.1:4983`; the UI is a hosted page calling it | None | None: wildcard CORS and `Access-Control-Allow-Private-Network: true` | Not detected |
| Prisma Studio | Serves its own UI locally and opens a browser | None | Fixed in 7.10 (August 2026, PR 29890) after "a malicious website visited while Studio was open could read or modify the connected database": binds `127.0.0.1`, no CORS, foreign `Origin` refused | Another port |
| Supabase local | Studio and pg-meta in Docker | "Studio, pgMeta (/pg/*), and analytics have no authentication"; binds `0.0.0.0` | None | Finds the running stack by container label |
| Jupyter Server | Serves its UI on `localhost` | Per-launch token; opens the browser on a `0600` file that redirects with the token, which replaced a single-use token in argv (notebook 5.7.3) | `Host` check on loopback; no CORS; foreign `Origin` refused | Runtime files (`0600`, `0700` directory) list running servers |
| Codex | The desktop app runs its own app-server as a child; `codex app` opens it with `codex://threads/new?path=…` after verifying its code signature, and installs it from a signed disk image when missing; the daemon uses a `0600` Unix socket in a per-user `0700` directory | Socket permissions; non-loopback `ws://` requires a token given by file or digest | Any `Origin` refused | Lock plus connect probe; per-thread writer locks are empty files held with `try_lock` |
| kubectl | `kubectl proxy` holds kubeconfig credentials; exec credential plugins return short-lived tokens to the client; `kubectl config set-context` writes kubeconfig | kubeconfig bindings | `--accept-hosts` defaults to loopback | Not applicable |
| Ollama, Vite, webpack-dev-server | Local HTTP servers | None by default | `Host` allowlists after DNS-rebinding advisories (CVE-2024-28224, CVE-2025-24010); webpack stopped relying on `Sec-Fetch-*` (CVE-2026-6402) and hardened header parsing (CVE-2026-14631) | Not applicable |

No surveyed database tool authenticates its local UI or defaults to read-only.
This design takes Jupyter's private runtime record and browser defence,
Prisma's fix, Codex's thin launcher, app-owned child process, per-user
directory and lock-based liveness, and kubectl's split between shared
configuration, written by its own tool, and credentials obtained through it.

## Evidence and tests

Extend the existing owners in [the testing map](../dev/testing.md):

- **Server, in-source `ingress` tests:** every route `build_app` registers is
  classified, enumerated from the router; a local read-only state refuses each
  `Write`-class route before its body, merge `dry_run` included.
- **Server, `integration/auth_policy.rs`:** the local server's token resolves
  to its actor; every `PolicyAction` has a local-server class, so adding one
  fails the test until it is classified; a read-only local server's engine
  refuses a write even where a route would admit it, and its write routes
  answer `forbidden`; graph-wide and per-branch reads are
  granted; `ReadWrite` refuses without an actor, for a cluster graph and on an
  actor-views graph before any open; no local state runs open or default-deny;
  the generated policy's graph id matches the route's.
- **Server, `integration/mcp.rs`:** a read-only local server lists no write
  tools and refuses one as unknown.
- **Server, `boot_settings.rs`:** `serve_local` refuses a non-loopback
  listener, prints nothing to stdout, drains admitted writes on shutdown, and
  returns an uncertain outcome on uncertain completion.
- **Cluster:** a local server on a cluster graph leaves `__cluster/lock.json`
  untouched.
- **CLI, a new `cli_local_servers` integration module** (a new public
  boundary):
  - start, the one stdout line, logs on stderr, and the record; shutdown when
    the line cannot be delivered; the line's `token` present through a pipe and
    absent under a pseudo-terminal;
  - the guard's `Host`, `Origin`, CORS and malformed-header cases; read-only
    refusals;
  - every code in [Refusals](#refusals) that these commands make, with its
    `refusal` field, its outcome envelope under `--via-server` and exit 1;
    `--reuse` returning the live line;
  - the notices to direct writers, cleanup and optimize, with their outcomes
    unchanged, the direct writer's notice giving the `--via-server` command in
    both its human and JSON forms; `--via-server` with and without a matching
    `--as`, and its unknown-outcome reporting;
  - records: a dead owner's record ignored with no process-ID check; a record
    from another machine ignored and kept; the directory resolved from the
    account database with `HOME`, `TMPDIR` and `XDG_*` pointing elsewhere;
    ownership and mode refusals;
  - `mcp` relaying, hosting, re-resolving after a 401 and filtering write tools
    without `--write`;
  - `studio` opening its link with the binding digest through a substituted
    opener, never opening it on Linux or Windows without a scheme handler, and
    refusing an unsupported transport.
- **CLI, the installer (phase 5),** with a substituted verifier and download
  source: an app failing the signature requirement is refused before it opens
  and before it installs; a missing app is downloaded, verified and installed
  to the first writable Applications directory; an override URL is verified
  the same way; Linux prints instructions.
- **CLI, `integration/cli_schema_config.rs`:** `targets --json` for every kind,
  with aliases, no secret and no network request, and `local_servers` for live,
  dead and other-machine records and for a root-scoped target's graphs; `targets
  add` and `remove` keeping the rest of the file's text, never changing the
  default, refusing a duplicate, an incomplete binding, the default target and a
  legacy-format home; `credential` over a pipe from `OMNIGRAPH_TOKEN_<OWNER>`
  and the credentials file, with the `KeyedServer` support helper;
  `credential_terminal` under a pseudo-terminal; `target_kind_unsupported`,
  `credential_anonymous` and `credential_missing`; `version --json` reporting
  `protocols`.
- **CLI, `skill.rs` drift tests:** command index rows and condensed reference
  entries for the five commands and `--via-server`.

Run CLI integration tests after building `omnigraph-server`, with
`RUST_MIN_STACK=16777216`. No cost instrument is needed; local servers add no
storage I/O to existing paths.

## Changes by repository

This RFC is decided and implemented in ModernRelay/omnigraph. Other
repositories' changes are listed as dependencies, made and reviewed there.
Appendix A lists the changes kept out of this scope.

### ModernRelay/omnigraph

| Area | Change |
|---|---|
| `omnigraph-server` | `AppState::local`, `LocalBinding`, `LocalOptions`, `LocalCredential`, `LocalOpenError`; `serve_local` with the lifecycle extracted from `serve_config`, returning `ServeOutcome` and printing nothing to stdout; `install_log_subscriber`; local states mount no `/cluster/*` routes and cap MCP write tools by access; read-only refusal in `ingress::admit` with `forbidden`; the exhaustive local-server class of every `PolicyAction`. |
| `omnigraph-cli` | `serve` (with `--write`, `--reuse`, `--port`, `--json`), `studio`, `mcp`, `targets` (with `local_servers`), `credential` and `--via-server`, each with its `commands/` module and `app.rs` arm; `protocols` in `version --json`; the `refusal` field and its codes; the loopback guard; server records in the per-platform directories, reusing the `coordination.rs` home resolution and checks; the notices, the direct writer's naming its `--via-server` command; the MCP bridge with its `readOnlyHint` filter; the platform opener with its scheme-handler check; later, the Studio installer with code-signature verification. |
| Docs | Amend the support boundary in [invariants](../dev/invariants.md); a user page for local servers, with the one-line distinction from `omnigraph-server`; the CLI reference; the server authentication page's self-hosting settings; the MCP guide's `omnigraph mcp`; the README's clients table, which still lists the retired stdio package; the public embedding APIs in [control plane](../dev/control-plane.md) and the observability guide's note for embedders; new owners in [testing](../dev/testing.md); a disposition note on RFC 0003 for the stdio bridge it deferred; the bundled skill's command index rows and condensed reference entries, the rule for which path an agent uses, and its lines saying the cluster server is "the only way to serve" and that there is "no single-graph server mode" (`SKILL.md`, `references/cluster.md`, `references/server-policy.md`); one `changelog.d` note per phase. |

### ModernRelay/omnigraph-studio

| Area | Change |
|---|---|
| Choosing the CLI | Choose the `omnigraph` binary by `version --json`: an exact `http_contract` match, the `protocols` it needs, and an `internal_schema.serves` range covering the graph's storage format; warn before a read-write open when `writes` is newer than the graph's format. Never run an executable a link names. |
| Local servers | Run `omnigraph serve --target NAME --reuse --json` with the interactive login-shell environment captured once, through a pipe; attach to the local server in its line; stop a child local server on close; treat a vanished local server or exit 2 as an unknown outcome and never resubmit. Offer "Open read-write" as a deliberate action. Show which graphs are open from `targets --json`'s `local_servers`. |
| Links and saved state | Handle `open?target=<name>&binding=<digest>[&graph=<id>][&branch=<name>]`, read-only, and accept `target=` on the `graph`, `node` and `query` routes; refuse a binding Studio's own resolution does not match; drop the unshipped `connect` route. Key saved windows, drafts and agent directories by target or canonical root, not by a local server's random URL. Register the scheme on Linux (`MimeType=x-scheme-handler/omnigraph-studio;`, `Exec=omnigraph-studio %u`) and dispatch `open_urls` on GPUI Linux, which stores the callback without calling it; register it on Windows and forward a second launch's link to the running instance. |
| Connections | Remove the operator-config and credentials parsers (`graph_store/src/cli_config.rs`), the name-based token lookup and Studio's own server list: list targets and aliases with `omnigraph targets --json`, add a server with `omnigraph targets add` and `omnigraph login --target NAME` on standard input, and move existing keychain servers the same way once. |
| Static tokens | Obtain them with `omnigraph credential --json`, keep them in memory, send them only to URLs under `endpoint`, and run it again after a 401. |
| Access | Show a local server's access from its line and hide write affordances when it is read-only. |
| Agents | Keep the agent gateway for panel agents and point it at the local server's URL and token; say when a read-only local server refuses their branch writes. Create each conversation's CLI home with `omnigraph targets add` under that home's `OMNIGRAPH_HOME`, so the CLI writes its format, and refuse `--target` and `--via-server` in the agent wrapper as it refuses `--server` and `--as`. Never pass the captured environment to agents. Bring the agent panel to Windows. |
| Releases | Configure the Apple secrets so every release is Developer ID signed and notarized; the workflow already does both when they are set, but releases so far are pre-releases signed ad hoc. A fixed, documented team identifier the CLI can pin; full releases with version-free asset names, so a latest URL resolves for each architecture; an updater inside the app; a package identity when Windows ships as an installer rather than a zip. Phase 5 waits for these ([omnigraph-studio#3](https://github.com/ModernRelay/omnigraph-studio/issues/3)). |
| Specs | Rewrite spec 012's E1–E5 as this RFC, align its E15 with [#1019](https://github.com/ModernRelay/omnigraph/issues/1019), drop spec 013's note that the launcher passes its CLI path, and update specs 002 (links), 003 (connections), 011 and 013 (agents). |

### The managed control plane

No change in this scope. Managed access needs control-plane changes,
summarized in [A4](#a4-oauth-for-standard-clients-and-managed-access) and
tracked in that repository.

## Rollout

Each phase is one or more pull requests titled in Conventional Commits, each
with its single-bullet `changelog.d` note, the public Rust API in phase 1
included.

1. **Server foundations:** `AppState::local`, `serve_local`, read-only
   enforcement, the log subscriber. `implementation: in-progress`.
2. **Read-only local servers:** `serve`, records, the guard, `--reuse`, the
   `studio` launcher, `targets --json` with `local_servers`, the `refusal`
   field, `protocols` in `version --json`, user docs and the skill.
3. **Read-write local servers:** `--write`, `--via-server` and the notices.
4. **`targets add` and `remove`, `credential` and `omnigraph mcp`.**
5. **Installing Studio**, once Studio's signed and notarized releases, team
   identifier, latest URL and updater exist
   ([omnigraph-studio#3](https://github.com/ModernRelay/omnigraph-studio/issues/3)).
   `implementation: complete`.

Phases 1–4 are usable without phase 5. Studio's steps follow each phase in its
repository.

## Unresolved questions

1. **RFC 0059's agreement** to the `targets` writer and to rows for the new
   commands. Without it, Studio must keep its own server list, and this RFC
   would say so.
2. **Catching a branch up through a local server.** Whether a merge that carries
   a newer schema needs `branch_merge` or `schema_apply`, which [schema apply
   with live branches](https://github.com/ModernRelay/omnigraph/pull/963) leaves
   open.

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
| A7 | RFC 0059 alignment | Another author's open RFC | [Comment on PR #675](https://github.com/ModernRelay/omnigraph/pull/675#issuecomment-6223864972) |
| A8 | Defects found | Bugs, independent of this design | One issue each, linked in A8 |

### A1. Cross-process writer safety

The engine supports schema apply, branch creation and deletion, and cleanup from
one writer process only. Its write queues and schema gate are process-local, and
its comment gives the reason: "a separately-opened handle can apply a schema or
delete refs, which must serialize with a live writer". Two terminals, two
agents, or an agent beside a local server can break that boundary today with no
warning, and row writes across processes can return `ReadSetChanged` after
effects.

Two designs were considered. The first makes each of those operations safe
across processes in the engine, conditioning its effect on the manifest version
or native-ref CAS and revalidating under publication, as row writes already do.
That protects every writer, on every machine. The second is a cooperative
machine-local writer lock, shared for row writes and exclusive for the
operations above, with each operation's class declared once by the engine. An
earlier revision of this RFC carried the lock; it protects only cooperating
processes of one account on one machine, and it changes every direct write.
[Schema apply with live branches](https://github.com/ModernRelay/omnigraph/pull/963)
already takes the first design's shape for schema apply, resting on write
revalidation and its merge rule rather than the process-local gate, and
[group commit](https://github.com/ModernRelay/omnigraph/pull/785) keeps the CAS
as the only guard between processes.

**Recommendation:** an engine RFC preferring the first design. Issue
[#1018](https://github.com/ModernRelay/omnigraph/issues/1018) records the gap with the comment quoted above.

### A2. Credential introspection

`GET /graphs/{graph_id}/access` would tell an authenticated caller which graph
actions its own credential allows, for example
`{"actor":"act-me","graph_wide":["read","export"],"branch":"main","actions":["read","export","change"]}`.
It would answer the three shapes policy decisions take: graph-wide, per branch
(`?branch=`), and from a source to a target branch (`&target_branch=`, for
`branch_create` and `branch_merge`). It would evaluate the installed policy for
the caller's actor and each action, by the names given and never by whether
they exist, so it cannot probe what a 404 hides, and reveal nothing about other
actors or the policy's rules. Static tokens without a policy would get
`["read"]`, and signed data credentials without one `[]`, matching `authorize`.
Local servers would answer it from their generated policy, so a client reads
access the same way everywhere. It reports Cedar operations, not actor views,
whose per-securable roles can still refuse a permitted `change`; a `view` field
stays reserved, and the actor views RFC should list the route among its
surfaces. Without it, a client shows every action and learns a limit from a
403, which is the common case for self-hosted static tokens without a policy.
The server and the managed control plane each list this as a gap; it is Studio
spec 012's E15.

**Recommendation:** a new RFC, kept separate from actor views because static
tokens need it now; issue [#1019](https://github.com/ModernRelay/omnigraph/issues/1019).

### A3. `WWW-Authenticate` on every 401

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

### A4. OAuth for standard clients and managed access

A client other than the CLI should reach an OIDC-enabled server, self-hosted or
managed, as a standard OAuth public client rather than borrow the CLI's
credentials. A managed data token is a bearer credential valid for up to 24
hours that removing admission does not retract (the managed-data guide and
RFC 0053 describe both), and credential families stay separate. The client
flow:

1. Discover the protected-resource metadata as A3 describes; take `resource` and
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
changes beyond A3.

### A5. MCP relay for managed targets

`omnigraph mcp` could relay to a managed cluster's `/mcp` with the CLI's own
data credential once managed hosts route `/mcp`. The token would never leave the
CLI, but every local agent using the bridge would act with the CLI's managed
authority.

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

### A7. RFC 0059 alignment

The comment on PR #675 asked for five additions; as of `323068fa` none has a
reply:

| Ask | State |
|---|---|
| Rows for `serve`, `studio`, `mcp`, `credential` and `--via-server` in its applicability table | Not added; this RFC's command table holds them until it does |
| A read-only `omnigraph targets --json` | Not added; defined here |
| A structured target writer | Contradicted by its "Add no target administration group"; defined here as the one exception, with the reasoning in [Targets for clients](#targets-for-clients) |
| A server target's authentication binding | Adopted in substance: a server target names its token owner, bound to an endpoint scope, or selects anonymous access; the listing reports `token` or `anonymous` |
| No transition period | Rejected: it keeps legacy adapters for an interval; the commands here take only `--target` and refuse in a legacy-format home |

**Recommendation:** a [comment on PR #675](https://github.com/ModernRelay/omnigraph/pull/675#issuecomment-6223864972), followed by a second one
naming the exception and its reasons.

### A8. Defects found

| Issue | Defect | State |
|---|---|---|
| [#1022](https://github.com/ModernRelay/omnigraph/issues/1022) | The CLI sent `OMNIGRAPH_BEARER_TOKEN` to any remote URL, including an unregistered literal one. | Fixed by #1030: the CLI reads no unkeyed token |
| [#1023](https://github.com/ModernRelay/omnigraph/issues/1023) | `commit publish` and `commit drop` passed `--as` to the engine, but the plane guard rejected `--as` for every `commit` command. | Fixed by #1029 |
| [#1024](https://github.com/ModernRelay/omnigraph/issues/1024) | The credentials file's directory is created with default permissions, not `0700`. | Open |
| [omnigraph-studio#2](https://github.com/ModernRelay/omnigraph-studio/issues/2) | The agent gateway checks neither `Host` nor `Origin`. | Open |

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
