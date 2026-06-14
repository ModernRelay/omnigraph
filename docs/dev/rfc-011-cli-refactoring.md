# RFC-011: CLI refactoring — one addressing & config model

**Status:** Proposed
**Date:** 2026-06-14
**Audience:** CLI/server maintainers
**Builds on:** [rfc-007-operator-config.md](rfc-007-operator-config.md)
(per-operator config, keyed credentials, named servers),
[rfc-008-deprecate-omnigraph-yaml.md](rfc-008-deprecate-omnigraph-yaml.md)
(the legacy file this RFC finishes removing),
[rfc-009-unify-access-paths.md](rfc-009-unify-access-paths.md)
(`GraphClient` — embedded ≡ remote at the execution layer),
[rfc-010-cli-planes-restructure.md](rfc-010-cli-planes-restructure.md)
(declared planes + the wrong-plane guard this RFC subsumes).
**Sequencing:** lands as / after RFC-008 stage 5 (the `omnigraph.yaml` removal).

## Summary

Refactor the CLI around one coherent model once `omnigraph.yaml` is gone. The
shape:

- **One ontology** (store, server, cluster; cluster config vs operator config;
  catalog; context; capability) where each term names exactly one concept.
- **Addressing = scope + `--graph`, with the access path *derived*.** A command
  resolves a *scope* (operator defaults, an optional named *context*, or one
  explicit primitive address — `--store` / `--server` / `--cluster`), selects a
  graph inside it with `--graph`, and the **served-vs-direct access path falls out
  of the scope's bindings × the verb's capability** — it is never a per-command
  toggle and never inferred from a URI scheme.
- **Served is the front door; direct storage is privileged.** The everyday scope
  is a *server* (a bearer token, no bucket credentials). Reading or writing a
  remote store/cluster directly is an explicit, credentialed, admin/break-glass
  act — never the default, never baked into everyday operator config.
- **The CLI is stateless per command.** No `current_context` pointer, no
  `USE`-style mode; every command is fully determined by its flags + static
  config. You *select* a graph, you do not *switch into* one.
- **Definitions are named; payloads are passed.** Queries (`.gq`) and schema
  (`.pg`) live in the catalog and are invoked by name; params and bulk data are
  the only per-call inputs.

This removes `--target`, `--cluster-graph`, `--uri` scheme-dispatch, and the
plane guard's "a `--target` that resolves to a remote URL" special case — and it
collapses the four-plane vocabulary, for users, into a single capability rule.

## Motivation: the legacy file pollutes the taxonomy

Today the CLI exposes four overlapping addressing forms but the system has only
three real entities; the mismatch is the whole problem, and `omnigraph.yaml` is
the carrier:

1. **`--target` straddles kinds.** It resolves through the legacy
   `omnigraph.yaml` `graphs:` map (`config.rs::resolve_target_uri`), and that
   `.uri` can be a **storage location** (`file`/`s3`) *or* a **remote server**
   (`http`). One flag, two access paths with different capability and trust
   models. The wrong-plane guard's storage-plane remote rejection
   (`helpers.rs:467`) exists *only* to compensate for this overload.
2. **Scheme-inferred transport.** `<URI>`/`--uri` has the same disease a level
   down: `is_remote_uri` (`helpers.rs:15`) silently picks embedded vs remote from
   the scheme. Transport is guessed from a string, not declared.
3. **No single environment concept.** Defaults are smeared across the deprecated
   `omnigraph.yaml` (`cli.graph`, `server.graph`) with no clean way to name or
   switch environments.

Removing `omnigraph.yaml` is the moment to fix all three at once.

## Ontology

Every term is one concept. The rest of this RFC uses them precisely.

### Entities — the things that exist

- **Graph** — a typed property graph (node/edge types over Lance); the thing you
  query and mutate. *Example: the `knowledge` graph.*
- **Store** — the storage location of a **single** graph: its Lance datasets at a
  `file://`/`s3://` URI. Addressed directly with `--store`. *Example:
  `s3://acme/clusters/brain/graphs/knowledge.omni`.*
- **Cluster** — a storage root holding **many** graphs plus the catalog and
  control-plane state (state ledger, approvals, recovery). Managed as-code by the
  team. *Example: the `brain` cluster at `s3://acme/clusters/brain`.*
- **Server** — an `omnigraph-server` process serving graphs over HTTP with bearer
  auth and Cedar policy; boots from a bare graph or a cluster. *Example: `prod` at
  `https://graph.example.com`, serving the `brain` cluster.*

### Config & catalog — the descriptions

- **Cluster config** — `cluster.yaml` in the cluster root, declaring the **desired
  state** (graphs, schemas, stored queries, policies, storage), applied with
  `cluster apply`. Team-owned; the source of truth for *what the system is*.
- **Catalog** — the **applied** registry the cluster owns in storage: the graphs,
  stored queries, and policies `cluster apply` materialized. What a server serves
  and what `query <name>` resolves against. *(Cluster config is the spec; the
  catalog is the applied result.)*
- **Operator config** — `~/.omnigraph/config.yaml`, your **personal** file:
  identity (actor), default graph, named servers/clusters, output prefs, optional
  contexts. Declares *who I am*, never what the system is.
- **Context** — an optional named bundle of **defaults inside the operator
  config** (one of {cluster, server, store} + a default graph). Config data,
  **not state**: selecting one fills in omitted flags for a command; it does not
  put you "in" a mode. Chosen per command (`--context <name>`) or per shell
  (`OMNIGRAPH_CONTEXT`).
- **Credential** — a bearer token keyed to a **server name**, resolved via
  `OMNIGRAPH_TOKEN_<NAME>` or `~/.omnigraph/credentials` (`0600`); sent only to
  the server it is keyed to. (Per RFC-007 — the operator config holds endpoints,
  never tokens.)

### What you run — definitions vs payloads

- **Schema** — the `.pg` type definitions for a graph; authored as a file, applied
  via `schema apply` (or `cluster apply`).
- **Stored query** — a named query in the catalog, the team's reusable contract;
  invoked by name. *Example: `find_people`.*
- **Query file (`.gq`)** — an authoring artifact holding `query <name>`
  declarations; becomes a stored query when `cluster apply` adopts it. For
  authoring/ad-hoc, not everyday invocation.
- **Payload** — the per-call inputs that vary each run: params (`--params`,
  positional args) and bulk data (`--data`). Never part of config.

### How a command resolves

- **Scope** — the resolved environment a command addresses: operator defaults, a
  named context, or one explicit primitive address.
- **Access path** — **served** (through a server) or **direct** (open storage
  in-process). Derived from scope × capability; see "Access path" below.
- **Capability** — what a verb requires: `any`, `served`, `direct`, `control`,
  or `local`.
- **Target shape** — whether the verb is **graph-scoped** (selects one graph
  inside the scope), **scope-scoped** (operates on the whole server/cluster
  scope), or **local** (does not resolve scope or graph).
- **Actor** — the identity a write is attributed to: server-resolved from the
  bearer token (served), or `--as` ?? `operator.actor` (direct).

### The relationships that prevent confusion

- **Exactly two config surfaces:** **cluster config** (team) and **operator
  config** (personal). Nothing else is "a config."
- A **context is not a third config** — it lives *inside* the operator config, and
  it is **defaults, not state**.
- A **catalog is not config** — it is the *applied state* the cluster owns.
- A **store is one graph; a cluster is many graphs** + catalog + control state.
- A **graph is the logical thing**; store/server/cluster are ways to reach it.
- "State" elsewhere is not the context: *graph state* is committed data in Lance;
  *cluster state* is the applied control-plane ledger. Neither is operator config.

## Design

### First principles

> Addressing should be 1:1 with the system's real entities; the access path
> (served vs direct) should be **derived**, never inferred from a string or
> toggled per command; the CLI should be **terse by config and stateless per
> command**; and **definitions are named while payloads are passed**.

Every command answers four orthogonal questions — kept orthogonal here:

| Axis | Question | Today | Target |
|---|---|---|---|
| Scope | which environment? | `omnigraph.yaml` defaults / `--target` | operator defaults · `--context` · one primitive |
| Target shape | whole scope or one graph? | implicit in command family | declared per verb |
| Graph | which graph in it? | tangled into the address | `--graph` only for graph-scoped server/cluster verbs |
| Access path | served or direct? | inferred from scheme / target | **derived** from scope × capability |
| Actor | who am I? | `--as` > `cli.actor` (yaml) > `operator.actor` | `--as`/`operator.actor` (direct) · token (served) |

### A scope binds one entity — and served is the default

A scope (a context, the flat defaults, or one primitive flag) binds **exactly one
of** {server, cluster, store}. Server and cluster scopes may contain many graphs
and can carry a `default_graph`; a store scope is already one graph and does not
accept `--graph`. They differ by privilege, and **the everyday default is a
server**:

- **server** → served (the everyday scope). A bearer token, **no storage
  credentials**. Data verbs run through it, policy-enforced; maintenance verbs are
  unavailable from this scope — there is no server route for them, so you must
  name storage explicitly. This is what a normal operator's config binds.
- **cluster** → direct storage to a managed cluster, for **control,
  maintenance, and graph-backed validation only** (`cluster *`,
  `optimize`/`repair`/`cleanup`/`schema plan`, graph-backed `lint`, and
  `queries validate`). Data verbs are **not** run directly against a cluster —
  they go served, or `--store` for ad-hoc. **Privileged:** requires bucket
  credentials, so it appears only in a maintainer's config or as an explicit
  `--cluster` flag — never in an everyday operator's defaults.
- **store** → one graph's storage, direct. A **local file** store is ordinary
  local dev; a **remote `s3://`** store is break-glass. No catalog (named queries
  do not resolve — the ad-hoc lane).

A scope names **one** thing, so there is no independent `server`+`cluster` pair
that could disagree (the audit's coherence hazard is gone by construction — the
default is just a server). And the storage root lives only where it must:

### Direct storage access is privileged (the storage-root rule)

> The storage root (`s3://…`) is **server-and-admin knowledge, never
> everyday-operator knowledge.** Everyday operator config binds a server (a bearer
> token, no bucket credentials). Direct remote access — opening a cluster root or
> an `s3://` store — is always **explicit and privileged**: you name
> `--cluster`/`--store`, and only someone with bucket credentials can. The CLI
> never opens a remote store from a default scope.

This is the least-privilege posture — revoke a bearer token, don't rotate bucket
keys; only the **server process** and an occasional **maintenance admin** ever
hold storage credentials. It makes "use the server, not raw storage"
**structural**, not advisory: direct access requires credentials a normal operator
does not have *and* a flag they must type. The only storage root in an everyday
setup is the one the **server** boots from; operators never see it. (Local *file*
stores for dev are unaffected — a local file is not the production bucket.)

### Access path is derived, not chosen

The two access paths are genuinely different — not two transports for one thing:

- **Served** (through a server): the server resolves your actor from a token and
  enforces Cedar policy at the HTTP boundary. In cluster mode the **catalog and
  config** (graph set, stored queries, policy bundles) are pinned to the applied
  serving revision and move only on restart; **graph data** is read through the
  server's engine handle against the requested branch/snapshot (it is not frozen
  at boot, though a long-running server will not observe *out-of-band direct
  writes* to storage until its handle refreshes). No storage credentials needed.
- **Direct** (open the Lance storage in-process): a **privileged** path — it needs
  your own storage credentials, so only an admin/maintainer (or a local-dev file
  store) takes it. Actor self-declared (`--as` ?? `operator.actor`), reads **live
  storage HEAD**. There is **no server-side identity/auth gate** — but engine-level
  Cedar policy *is* still enforced when the graph selection provides a policy
  (enforcement is engine-wide; embedded `_as` writers call the same `enforce`).
  "Direct" means "no HTTP boundary," not "unpoliced."

Because they differ in authority, freshness, and availability, a graph reached via
a server and that graph's raw storage are **different things you name
differently** — not one identity you flip. Making the access path a per-command
toggle (`--via`) is the `--target` mistake in new clothes; it is rejected.

> **The access path follows from the scope and the verb.** A **server** scope →
> served (data/catalog). A **cluster** scope → direct control, maintenance, and
> validation. A **store** scope → direct ad-hoc data (no catalog). The verb's
> capability picks which applies and rejects the mismatches.

State the bound plainly: the everyday data path
(`query`/`mutate`/`load`/`branch`/`export`/`commit`) against a served graph
**never needs direct storage access**, and direct access is legitimate only in
bounded places: **bootstrap** (`init`), **storage-native maintenance**
(`optimize`/`repair`/`cleanup`/`schema plan`), **graph-backed validation**
(`lint`), **catalog validation** (`queries validate`), the **control plane**
(`cluster *`), **local dev** with no server, and **break-glass** (recovery, or
checking whether a long-running server's handle lags live HEAD). Everything else
is served. This is what makes "discourage direct storage" enforceable rather
than aspirational.

This list is expected to **shrink**: Decision 11 moves
`optimize`/`cleanup` (and healthy-path `repair`) to server-managed jobs, which
would leave direct access to just standalone/local dev, the control plane, and
break-glass — and remove the last routine reason an admin needs bucket
credentials.

### Capability semantics

The CLI validates through verb capability, not plane jargon:

| Capability | Meaning | Examples |
|---|---|---|
| `any` | graph-scoped data; served via a server scope; direct only against a **store** scope (local dev / break-glass); **errors on a cluster scope** | `query`, `mutate`, `load`, `export`, branch reads, `schema show/apply` |
| `served` | requires an HTTP server; may be graph-scoped or scope-scoped | `graphs list`, `queries list` |
| `direct` | graph-scoped storage-native or graph-backed validation; no server form exists | `init`, `optimize`, `repair`, `cleanup`, `schema plan`, graph-backed `lint` |
| `control` | cluster-scoped catalog/control-plane work; addresses the cluster, not a single raw store | `cluster *`, `queries validate` |
| `local` | does not address a graph or scope | `config`, `context`, `lint --query ... --schema ...` |

`any` does **not** mean "the user picks": the resolver picks from the scope.
Internally the exhaustive `command_plane` match (`planes.rs`) stays as the drift
guard; user-facing errors speak in terms of what the command needs.

### Definitions vs payloads

Queries and schema are **definitions** — contracts that live in the catalog and
are invoked **by name**; params and data are **payloads** passed per call. So the
everyday form is `omnigraph query <name> [params]`, not
`omnigraph query --file find.gq`. A `.gq` path on a routine query is a smell: the
query is not in the catalog yet. Lifecycle: **author a `.gq` → `cluster apply`
adopts it → invoke by name thereafter.**

Named queries resolve through a **server** (which serves the cluster's catalog).
`queries list` is therefore a served catalog read. `queries validate` is a
control/catalog check against the cluster-owned query definitions. A bare
`--store` has **no catalog**, so it is the ad-hoc lane (`-e` / `--file`), and
`--cluster` does not invoke stored queries. So named-query invocation is a
**served** convenience; direct access (`--store`) is always ad-hoc.

| Kind | Examples | How it enters a command |
|---|---|---|
| Definition | stored query, schema | named in the catalog; authored as a file, adopted by `cluster apply` |
| Payload | params, bulk data | passed per call (`--params`, positional args, `--data`) |
| Authoring / ad-hoc | a `.gq` you're writing | `-e '…'`, `--file new.gq`, `lint --query new.gq --schema schema.pg`, `schema apply --schema` |

### Resolution rule

1. If the verb is `local`, reject graph/scope flags and run without resolving a
   scope.
2. If a primitive address is supplied (`--store`/`--server`/`--cluster`), use it
   and ignore operator-config scope defaults. *(A **named** primitive — `--server
   prod`, `--cluster brain` — still resolves through the operator-config registry;
   a **literal** — `--server https://…`, `--store s3://…` — bypasses it. Per
   Decision 2: a value containing `://` is a literal, otherwise a config-name
   lookup.)*
3. Else if `--context <name>` (or `OMNIGRAPH_CONTEXT`) selects a context, use it.
4. Else use the operator config's flat defaults. Error only if neither resolves.
   *(No sticky "current" pointer — each command resolves scope fresh.)*
5. Resolve the graph only for **graph-scoped** verbs. Server/cluster scopes:
   exactly one graph in scope → use it; else `default_graph`; else require
   `--graph <id>`. Store scopes are already one graph, so `--graph` is rejected.
   **Scope-scoped** verbs (`graphs list`, `queries list`, `queries validate`,
   and `cluster *`) do not select a graph unless their own resource argument says
   otherwise.
6. Derive the access path from capability × scope:
   - `direct` verb → the scope's cluster/store; if the scope is a server, error
     (name storage explicitly — it is privileged).
   - `served` verb → the scope's server; if the scope is a cluster/store, error.
   - `control` verb → the scope's cluster; if the scope is a server/store, error
     (name a cluster explicitly — it is privileged).
   - `any` verb → **served** if the scope is a server; **direct** against a
     **store** scope (ad-hoc); on a **cluster** scope, error — cluster is
     maintenance-only, so use a server for data or `--store` for ad-hoc.
7. Reject mismatches with an error naming the missing axis.

Good errors:

```text
scope "prod" has 4 graphs; pass --graph <id> or set default_graph
optimize needs direct storage access; scope "prod" is a server — name storage with --cluster s3://… or --store (requires storage credentials)
graphs list enumerates a server scope; do not pass --graph
--store opens raw storage directly, bypassing any server (no HTTP auth gate, live HEAD); for recovery/inspection
```

### Config shape (operator config)

`~/.omnigraph/config.yaml` — your personal file; the cluster config
(`cluster.yaml` + catalog) is the separate, team-owned surface. The default-graph
key is `default_graph` everywhere (the per-command flag is `--graph`).

**Everyday operator — binds a server, holds no storage root:**

```yaml
defaults:
  server: prod
  default_graph: knowledge
  output: table
servers:
  prod:    { url: https://graph.example.com }    # token keyed by name (RFC-007); no creds here
  staging: { url: https://staging.example.com }
contexts:                                          # optional, only for multiple environments
  staging: { server: staging, default_graph: knowledge }
```

A normal operator never has a storage root or bucket credentials. Their default
scope is served; `optimize`/`repair`/`cleanup` error with a pointer to name
storage explicitly.

**Maintainer — opts into a cluster root (and has bucket credentials):**

```yaml
contexts:
  brain-admin: { cluster: brain, default_graph: knowledge }   # direct; admin/control/maintenance
clusters:
  brain: { root: s3://acme/clusters/brain }                   # the s3:// root lives ONLY here
```

The `clusters:` block — the only place a storage root appears in operator config —
is **admin-only and opt-in**, absent from a normal operator's file. Equivalently,
skip config and name it per command:
`omnigraph optimize --cluster s3://acme/clusters/brain --graph knowledge`. The
cluster stays the source of truth for the managed catalog; tokens live in the
keyed credential store, never in this file.

### Command shape

Assume the everyday flat defaults: server `prod`, default graph `knowledge`.

| Intent | Command | Path |
|---|---|---|
| Run a catalog query | `omnigraph query find_people` | served |
| …with params | `omnigraph query find_people --params '{"title":"Eng"}'` | served |
| Another graph in scope | `omnigraph query find_people --graph archive` | served |
| Write | `omnigraph load --data batch.jsonl --mode append` | served |
| A different environment | `omnigraph --context staging query find_people` | served |
| One-off server, no config | `omnigraph query find_people --server https://graph.example.com --graph knowledge` | served |
| Maintain (admin, explicit storage) | `omnigraph optimize --cluster s3://acme/clusters/brain --graph knowledge` | direct (privileged) |
| Maintain (admin, via admin context) | `omnigraph --context brain-admin optimize --graph knowledge` | direct (privileged) |
| List catalog queries | `omnigraph queries list` | served |
| Validate cluster query catalog | `omnigraph queries validate --cluster s3://acme/clusters/brain` | control (privileged) |
| Offline query lint | `omnigraph lint --query new.gq --schema schema.pg` | local |
| Graph-backed query lint | `omnigraph lint --query new.gq --cluster s3://acme/clusters/brain --graph knowledge` | direct (privileged) |
| Local dev, no server | `omnigraph query -e 'match { … } return { … }' --store graph.omni` | direct (local file) |
| Break-glass: raw storage of a served graph | `omnigraph query --file find.gq --store s3://acme/clusters/brain/graphs/knowledge.omni` | direct (privileged, rare) |

Note what the everyday rows are: **all served.** `optimize` does *not* appear in
the default-scope rows — from a server scope it errors and points you to name
storage (see the resolution rule), so maintenance is always a deliberate,
credentialed act. There is no "force served/direct" row — you never toggle the
path on a configured graph; the only way to reach raw storage is to *name it*
(`--cluster`/`--store`), which makes the privileged bypass unmistakable. Everyday
rows invoke a query **by name**; a `.gq` file appears only where there is no
catalog (bare store, break-glass) via `-e`/`--file`.

## Before / after

**Before** = best available today (legacy `omnigraph.yaml` `--target`, `.gq`
files, `--cluster-graph`, scheme inference). **After** = this model.

| Intent | Before | After |
|---|---|---|
| Run a query | `omnigraph query --target knowledge --query find.gq --name find_people` | `omnigraph query find_people` |
| Another graph | `omnigraph query --target archive --query find.gq --name find_people` | `omnigraph query find_people --graph archive` |
| Load | `omnigraph load --data b.jsonl --mode append --target knowledge` | `omnigraph load --data b.jsonl --mode append` |
| Maintain (admin) | `omnigraph optimize --cluster brain --cluster-graph knowledge` | `omnigraph optimize --cluster s3://acme/clusters/brain --graph knowledge` |
| Another environment | edit `omnigraph.yaml`, or re-address with full URIs | `--context staging …` or `OMNIGRAPH_CONTEXT=staging` |
| One-off remote | `omnigraph query --uri https://… --query find.gq` *(scheme→remote)* | `omnigraph query find_people --server https://… --graph knowledge` |
| Raw storage of a served graph | `omnigraph query s3://…/knowledge.omni --query find.gq` *(looks like a normal query)* | `omnigraph query --file find.gq --store s3://…/knowledge.omni` *(explicit bypass)* |

**Removed:** `--target`; `--cluster-graph` (`--graph` is the graph selector only
for graph-scoped server/cluster verbs); `--uri` http-scheme dispatch; `--via`
(never ships); everyday `--query <file>` (definitions are named);
`omnigraph.yaml` and its `cli.graph`/`server.graph` defaults.

## Server-side corollary

The same ontology applies to `omnigraph-server` boot: with `omnigraph.yaml` gone,
a server boots from a single bare graph URI **or** a cluster (`--cluster <dir|s3>`,
RFC-005), never a `graphs:` map. The store/server/cluster ontology is then
consistent across CLI and server.

## Migration & compatibility

Addressing flags and config keys are observable contract (Hyrum); every removal is
staged and release-noted.

- **`config migrate`** (shipped) maps each legacy `graphs:` entry **by what it
  actually is**: `http(s)` URIs → a `server:` (the recommended everyday shape);
  `file` URIs → a local `store:`; an `s3://` **graph** URI → an **admin** `store:`
  (it is a single graph, not a cluster); an `s3://` **cluster root** (one that
  carries cluster state) → an **admin** `cluster:`. Everyday `s3://` graph usage
  migrates with a **warning** — prefer serving it via a server rather than
  re-establishing direct remote access. It reports dropped keys.
- **Operators move to a server-default scope.** Where a legacy setup pointed
  `cli.graph` at an `s3://` graph for everyday use, migration flags it: the
  recommended shape is a `server:` scope (bearer token, no bucket creds), with the
  `s3://` root kept only in a maintainer's config — not every operator's.
- **`--target`** warns for one release, then errors; **`OMNIGRAPH_NO_LEGACY_CONFIG=1`**
  (already the strict switch) becomes the default — loading `omnigraph.yaml` is a
  hard error.
- **`--cluster-graph` → `--graph`**: `--cluster-graph` is accepted with a warning
  for one release, then removed.
- **`--graph` meaning change**: today `--graph` is "graph id on a multi-graph
  server" (paired with `--server`); it generalizes to "select the graph for
  graph-scoped verbs in server/cluster scopes." Existing `--server --graph`
  usage keeps working (it is a strict superset); release-note the broadened
  meaning and the fact that store/scope-scoped verbs reject it.
- **`--uri http://…`** warns, then errors with a pointer to `--server`.
- **`--as` on served paths**: today global `--as` is accepted (a no-op on remote
  writes — the server resolves the actor from the token); rejecting it on the
  served path is staged — warn for one release, then error.
- **`--alias`** → the `alias` namespace (`omnigraph alias <name>`, Decision 4);
  the old `--alias` flag warns for one release, then is removed.

## Non-goals

- **No change to the direct/served capability split.** Maintenance stays
  storage-direct by design (no server routes for `optimize`/`repair`/`cleanup`);
  this RFC only makes the split explicit.
- **No new transport.** Addressing surface, not protocol.
- **No positional sigil grammar** (`@server/graph`, `%cluster/graph`). Considered
  and rejected: explicit flags are more discoverable; contexts already give
  brevity. Revisit only on demonstrated expert-terseness demand.

## Decisions

The questions this RFC opened are resolved as follows. Two are explicitly
deferred (see below); they do not block the model.

1. **Local-dev path → embedded `--store` scope.** Local dev runs the engine
   in-process against a `--store <file>` (or a store-scoped context); `omnigraph
   serve` stays available but is not required. Consistent with embedded ≡ remote
   (RFC-009).
2. **Primitives are one flag, typed by content.** `--server` and `--cluster`
   accept either a config name or a literal URI: a value containing `://` is a
   literal (bypasses the registry); otherwise it is a config-name lookup (error if
   unknown). `--store` is always a URI. (Replaces the earlier "literal-vs-named"
   question — no `--server-url`/`--cluster-root` split.)
3. **Stored invocation: `query <name>` (read) / `mutate <name>` (write), one
   catalog namespace.** A name maps to one definition; the verb asserts its kind
   and the CLI errors on mismatch (`'apply_labels' is a mutation — use
   omnigraph mutate apply_labels`). No `invoke` verb.
4. **Aliases live under an `alias` namespace** — `omnigraph alias <name> [args]`,
   never bare top-level. An alias can therefore neither shadow nor be shadowed by a
   built-in (current or future) verb.
6. **Context merge: scope wholesale, prefs layered.** The entity binding +
   `default_graph` come *wholesale* from the active scope (a context, or flat
   defaults if none) — never per-key merged across the entity dimension (that would
   yield "server *and* cluster"). Only non-scope preferences (`output`, table
   layout) take flat defaults as a base. Precedence: explicit flag > context > flat
   defaults.
7. **No default graph → error + list candidates.** A graph-scoped verb with no
   `--graph`, no `default_graph`, and >1 graph in scope errors and lists candidates
   (served: `GET /graphs`; cluster-direct: catalog enumeration). If enumeration is
   policy-gated/unavailable, it says so and asks for `--graph`. Never auto-pick.
9. **Diagnostics & safety.** Writes echo the resolved scope + access path to stderr
   (suppress with `--quiet`). Destructive verbs (`cleanup`, overwrite `load`,
   `branch delete`) require confirmation when the scope is not local; `--yes` skips
   it; **no TTY without `--yes` errors** (never silently proceed). `--json`/CI never
   prompt — destructive without `--yes` errors.
10. **Cluster graphs evolve only via `cluster apply`.** `schema apply` (an `any`
   verb) targets standalone graphs; against a cluster-managed graph it errors and
   points at `cluster apply` (which records ledger/recovery/approvals — RFC-004).
   Mirrors `init`'s refusal of a cluster-managed path.
11. **Maintenance moves server-side (committed direction).** `optimize`/`cleanup`
   (and healthy-path `repair`) become server/cluster-managed async jobs —
   policy-gated, audited, single-coordinator — with `direct` retained only as
   break-glass (`repair` when the server is down). Runs out-of-band (a worker +
   async job routes, the `POST …` / `GET …/{id}` shape of the bulk-data-plane RFC,
   `docs/rfcs/0001-bulk-data-plane.md`), never inline in serving; `schema plan` is
   excluded (≈ `cluster plan` in cluster mode). The **mechanism** (job routes,
   worker, scheduling) is a follow-up RFC; until it lands the capability table above
   stands, and maintenance is `direct`. When it lands, the maintenance verbs'
   capability becomes "served-job + direct break-glass."

## Deferred

Non-blocking; settle when convenient.

- **D5 — combined admin scope.** A scope binds one entity; admins read via a
  server scope and maintain via `--cluster`. A `deployments: { … }` object
  (server + cluster validated coherent, referenced by a context) is revisited only
  if admin ergonomics demand it — and Decision 11 largely removes the need.
- **D8 — the `context` command surface.** `context list` / `context show`
  (read-only inspection) are additive diagnostics, shippable anytime; they don't
  touch the grammar or resolution. The *no sticky `context use`* constraint holds
  regardless — it is a design principle, not a command.

## Safety

Dropping the sticky `current_context` pointer removes the main footgun — a
destructive command silently inheriting a "current" environment from an earlier
session. Because each command resolves scope fresh, what is on the command line is
what runs. Two guards remain (a flat default or `OMNIGRAPH_CONTEXT` can still point
at prod): echo the resolved scope + access path on writes, and require
confirmation (or `--yes`) for destructive verbs when the resolved scope is not
local (Decision 9). The most dangerous direct writes (`cleanup`, overwrite
`load`) are *structurally* rare now — unavailable from the everyday server scope,
and gated behind bucket credentials plus an explicit `--cluster`/`--store` — so a
normal operator's setup mostly cannot issue them by accident at all.

## Invariants & deny-list check

- **§10 query semantics first-class / §11 transport at the boundary:** preserved —
  addressing resolves CLI-side to a `GraphClient`; no transport concepts leak into
  engine crates.
- **§12 no client-set actor:** strengthened — the served path's actor stays
  token-resolved and `--as` is rejected there; direct self-declares.
- **Least privilege (security posture):** everyday operators hold a revocable
  bearer token, not bucket credentials; only the server process and maintenance
  admins hold storage creds. Direct remote access is structural opt-in, not a
  default — narrowing the blast radius of a leaked operator config.
- **§6 strong consistency:** both paths are snapshot-isolated per query; this RFC
  changes addressing, not isolation.
- **Deny-list (no state that drifts):** contexts and aliases are static config
  sugar that resolve to canonical scopes; they declare nothing the cluster or
  server doesn't already own. No sticky session state is introduced.
- No Hard Invariant is weakened; the change is CLI surface + config removal.

## Relationship to prior work

The completion of the config/CLI lineage: RFC-007 added the operator config and
keyed credentials; RFC-008 demoted `omnigraph.yaml`; RFC-009 unified execution
behind `GraphClient`; RFC-010 declared the planes. This RFC removes the last
legacy addressing surface so the plane model becomes a clean function of the three
real entities, and folds the planes into a single capability rule. It is adjacent
to the public-track bulk-data-plane RFC (`docs/rfcs/0001-bulk-data-plane.md`,
PR #219, not yet merged), which canonicalizes `load`/`export` verbs; this RFC
canonicalizes how every verb *addresses* a graph.

## Appendix: target CLI taxonomy (end state)

The full command set under this model, organized by **capability** (the new
classifying axis) instead of plane — the end-state counterpart to the
current-taxonomy appendix below. Every command, with its end-state addressing.

```
omnigraph
│
├─ any — data verbs · served by default (server scope, or --server <url|name>);
│        --graph selects the graph in scope; --store forces ad-hoc direct (no catalog)
│  ├─ query   (alias: read*)    invoke a stored query by NAME; -e/--file for ad-hoc
│  ├─ mutate  (alias: change*)  invoke a stored mutation by name; -e/--file for ad-hoc
│  ├─ load                      bulk write — --data, --mode required; --from forks a missing branch
│  ├─ export                    dump graph data (NDJSON / Arrow)
│  ├─ snapshot                  current per-table versions
│  ├─ branch { create | list | delete | merge }    merge takes --into <target>
│  ├─ commit { list | show }    inspect the commit graph
│  └─ schema { show (alias: get) | apply }          apply in cluster mode → Open Q10
│
├─ served — needs a server (errors on a store/cluster scope)
│  ├─ graphs list               enumerate the graphs a server serves
│  └─ queries list              list stored queries in the served catalog
│
├─ direct — storage-native, PRIVILEGED · --cluster <root> | --store <uri> + bucket creds; never a server
│  ├─ init                      bootstrap a graph (--store <uri>); refuses a cluster-managed path
│  ├─ optimize                  compaction; --graph selects
│  ├─ repair                    publish uncovered drift; --confirm / --force
│  ├─ cleanup                   version GC; --keep / --older-than / --confirm
│  ├─ schema plan               migration preview (reads storage directly)
│  └─ lint --query <path>       graph-backed query lint (with --graph on cluster scope)
│
├─ control — cluster/catalog control, PRIVILEGED · --cluster <dir|s3>
│  ├─ cluster { validate | plan | apply | approve | status | refresh | import | force-unlock }
│                               apply/approve take --as <actor>; force-unlock takes <LOCK_ID>
│  └─ queries validate          validate cluster-owned stored queries against graph schemas
│
└─ local — no graph
   ├─ policy { validate | test | explain }   offline Cedar tooling
   ├─ context { list | show }                read-only; NO mutating `use` (no sticky state)
   ├─ config { migrate }                     finish the omnigraph.yaml split (RFC-008)
   ├─ login / logout                         per-server bearer credentials
   ├─ embed                                  offline embedding pipeline
   ├─ lint --query <path> --schema <path>    file-only query lint
   └─ version  (-v)
```

`*` `read`/`change` remain as deprecated aliases (warn on use); `ingest` is
**removed** (it was a deprecated `load` alias). `get` aliases `schema show`.

### Addressing forms (end state)

Three scope forms — one per real entity — plus the graph selector. No `--target`,
no `--cluster-graph`, no `--uri` scheme-dispatch, no `--via`.

| Form | Resolves to | Access | Privilege |
|---|---|---|---|
| **server scope** — operator default, a `--context`, or `--server <url\|name>` | a served endpoint + keyed token | served | everyday (bearer token) |
| **cluster scope** — an admin context, or `--cluster <root>` | a managed cluster's storage + catalog | direct | privileged (bucket creds) |
| **store scope** — `--store <uri>` | one graph's storage (no catalog) | direct | local-dev (file) / break-glass (s3) |
| **`--graph <id>`** | selects the graph for graph-scoped verbs in server/cluster scopes; invalid for store scopes and scope-scoped verbs | — | — |

Resolution: explicit primitive (`--server`/`--cluster`/`--store`) → `--context` /
`OMNIGRAPH_CONTEXT` → operator flat defaults. Access path is then derived from the
scope kind × the verb's capability (see the Resolution rule); it is never inferred
from a URI scheme and never toggled.

### What moved vs today

| Command(s) | Today (plane) | End state (capability) |
|---|---|---|
| `query`/`mutate`/`load`/`export`/`snapshot`/`branch`/`commit`/`schema show`/`schema apply` | Data | **`any`** (served-default; `--store` ad-hoc) |
| `graphs list` | Data (remote-only) | **`served`** |
| `queries list` | Session | **`served`** (catalog read) |
| `init`/`optimize`/`repair`/`cleanup`/`schema plan`/graph-backed `lint` | Storage | **`direct`** (privileged) |
| `queries validate` | Storage | **`control`** (catalog validation) |
| `cluster *` | Control | **control** (unchanged) |
| `policy *`/`embed`/`login`/`logout`/`config`/`version`/offline `lint --query --schema` | Session | **`local`** |
| `ingest`; `--target`; `--cluster-graph`; `--uri http` dispatch | present | **removed** |
| — | — | **added:** `context { list | show }` (read-only) |

Cross-capability families: `schema` (`plan` is `direct`, `show`/`apply` are
`any`), `queries` (`list` is `served`, `validate` is `control`), and `lint`
(offline with `--schema` is `local`, graph-backed is `direct`) split per
subcommand/mode, exactly where their authority and data dependencies differ.

## Appendix: current CLI taxonomy (today)

The **as-is** command surface this RFC transforms, kept so the RFC is
self-contained. The source of truth is the exhaustive `command_plane` match in
`crates/omnigraph-cli/src/planes.rs`.
Where it disagrees with the design above (four planes, `--target`,
`--cluster-graph`, scheme-inferred transport), the design is the *target* and this
is *today*.

### The four planes (today)

| Plane | What it touches | Addressing accepted |
|---|---|---|
| **Data** | a graph — embedded **or** via a server | `<URI>` · `--target` · `--server` (+`--graph`) |
| **Storage** | direct storage, no server | `<URI>` · `--target` (local/S3 only) · some also `--cluster`+`--cluster-graph` |
| **Control** | a cluster *directory* | `--config <dir>` |
| **Session** | no graph | — |

`--server`/`--graph` are gated strictly to the data plane; `guard_addressing`
(`planes.rs:128`) rejects them elsewhere (RFC-010 Slice 1).

### Command tree by plane (today)

```
omnigraph
├─ DATA ────────── run against a graph; embedded or --server
│  ├─ query (alias: read) · mutate (alias: change) · load · ingest (hidden, deprecated)
│  ├─ branch { create | list | delete | merge } · snapshot · export · commit { list | show }
│  ├─ graphs { list }                         (remote-only)
│  └─ schema { show (alias: get) | apply }    ← show/apply are DATA
├─ STORAGE ─────── direct file://|s3:// access; --server rejected
│  ├─ init · optimize · repair · cleanup       (optimize/repair/cleanup also: --cluster --cluster-graph)
│  ├─ lint (check shim) · schema plan          ← plan is STORAGE
│  └─ queries validate
├─ CONTROL ─────── cluster directory via --config <dir>
│  └─ cluster { validate | plan | apply | approve | status | refresh | import | force-unlock }
└─ SESSION ─────── no graph
   ├─ policy { validate | test | explain } · embed · login / logout
   ├─ config { migrate } · queries list         ← list is SESSION
   └─ version (-v)
```

`read`/`change` are visible clap aliases (deprecated names, warn); `check` is an
argv-shim → `lint`; `get` aliases `schema show`; `ingest` is hidden but runs.

### Cross-plane families (today)

- **`schema`**: `schema plan` is Storage; `schema show`/`apply` are Data.
- **`queries`**: `queries validate` is Storage; `queries list` is Session.

### Addressing forms (today)

| Form | Looks up in | Resolves to | Source |
|---|---|---|---|
| `<URI>` / `--uri` | nothing (explicit) | the literal URI | — |
| `--target <name>` | `omnigraph.yaml` `graphs:` | that graph's `uri` (local / S3 / **http**) | `config.rs::resolve_target_uri` |
| `--server <name>` (+`--graph`) | `~/.omnigraph/config.yaml` `servers:` | a remote server URL | `helpers.rs::resolve_server_flag` |
| `--cluster <dir\|s3> --cluster-graph <id>` | served cluster state | the graph's storage URI | `helpers.rs` (RFC-010 Slice 3) |

Precedence (`resolve_target_uri`): explicit `<URI>`/`--uri` → `--target` →
`cli.graph` default → error. `is_remote_uri` (`helpers.rs:15`) then selects
`GraphClient::Remote` vs `Embedded` (`client.rs:86`).

### Enforcement points (today)

- **`guard_addressing`** (`planes.rs:128`): `--server`/`--graph` on a non-data verb
  fails with a declared message.
- **Storage-plane remote rejection** (`helpers.rs:467`): a storage verb whose
  `--target` resolves to `http(s)://` is rejected.
- **`init` into a cluster layout** is refused (use `cluster apply`).

## Audit comments

Reviewed against the current CLI taxonomy, `planes.rs`, `cli.rs`, `helpers.rs`,
`client.rs`, RFC-007/RFC-010, and the user-facing CLI/server docs.

### Validated

- The target taxonomy now has a stable classifier: `any`, `served`, `direct`,
  `control`, and `local` are all declared capabilities.
- Cluster scope is coherent: it is privileged direct storage for control,
  maintenance, and validation, not a direct data path. `any` data verbs served by
  default and reject cluster scope.
- Graph selection is no longer universal. Graph-scoped verbs select a graph;
  scope-scoped verbs such as `graphs list`, `queries list`, `queries validate`,
  and `cluster *` address the whole server/cluster scope.
- The current-state appendix still matches the implemented CLI: four planes,
  `--target`, `--cluster-graph`, scheme-inferred transport, `schema plan` as
  Storage, and `schema show/apply` as Data.

Decisions and deferrals are tracked in [Decisions](#decisions) above — not
duplicated here.
