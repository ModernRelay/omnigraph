# RFC: Config & CLI Architecture — Layered Config, Client Targeting, Typed Locators

**Status:** Proposed (revised 2026-06-02)
**Supersedes:** the original additive-only draft (2026-05-30). This revision **embraces breaking changes** to remove ambiguity and conflation rather than carrying every legacy shape forward. It is gated behind a config `version:` field and ships compat aliases for the highest-traffic legacy keys, but it does not pretend the end-state is purely additive. Incorporates an implementation-readiness review: endpoint-bound credentials, layer identity trust, route-unification specifics, restored `query.roots`, and right-sized auth scope.
**Target release:** v0.8.x (phased — see Rollout)

## Summary

OmniGraph today reads one config file, `omnigraph.yaml`, from both the CLI (operating the embedded engine) and `omnigraph-server` (hosting graphs). The CLI **can** already reach a *single-graph* server — point a graph entry's URI at the endpoint and set `bearer_token_env` — but it **cannot address a specific graph on a multi-graph server**, has no named-server credential model, and does not work without a project file in the current directory. Those are the real gaps.

This RFC defines the config and CLI architecture that closes them, derived from first principles — *working backward from what OmniGraph uniquely enables* rather than copying kubeconfig. The result:

1. A **typed locator** replacing the conflated `uri: String`. A graph entry is **embedded (`storage:`) XOR remote (`server:` + `graph_id:`)**; the *key* names the locus so neither a URI scheme nor a comment is load-bearing.
2. **Three-tier server addressing.** A `servers:` entry is self-sufficient — graph identity is server-owned, so you address a known `server/graph_id` directly with no per-graph entry (listing what exists is `graph_list`-gated, §9). Per-graph `graphs:` entries become *optional aliases* (for a short name, a branch pin, or multi-homing). Below that, env vars (`OMNIGRAPH_SERVER` + token) give a fileless floor.
3. **Global-first layered config.** The user-global `~/.omnigraph/config.yaml` is the primary, self-sufficient default; `./omnigraph.yaml` is an optional repo-scoped override + deployment manifest. One schema, both layers optional. The CLI works from any directory with no project file (the `kubectl`/`aws`/`gh` posture).
4. **A method-tagged auth model.** `auth:` is a tagged union over `bearer | oauth | mtls | none`; bearer/mtls reference a *secret source* (`env | file | command | keychain`). v1 ships `bearer`/`none`; `oauth`/`mtls` are reserved (the enum shape is fixed, so adding them is non-breaking — V6). Auth is **per-server**, not per-graph, and trusted-origin (§7): a lower-trust layer cannot supply credentials. Secrets are never inlined and never live in any `*.yaml` or in the project tree.
5. **A clean file layout split on the two real boundaries — secrecy and scope, never role.** Global `~/.omnigraph/config.yaml`; project `./omnigraph.yaml` (one artifact, both roles by section); credentials in the OS keychain → `~/.omnigraph/credentials` (INI, `0600`). No `credentials.yaml`.

The design optimizes jointly for **DX** (one command surface across embedded and remote; clone-and-go) and **AX** (agent experience: one flat resolved context, secrets out of the repo and endpoint-bound, branch-pinned reproducible reads, a GitOps'd capability surface).

## Reconciliation with the code

Verified **against the code**, not ticket status. Findings, with the corrections they force on the design:

- **Config lives in `crates/omnigraph-server/src/config.rs`**, and `omnigraph-cli` depends on the whole `omnigraph-server` crate to use it (`crates/omnigraph-cli/Cargo.toml:19`; the CLI imports `OmnigraphConfig`, `PolicyEngine`, `QueryRegistry`, `load_config` from `omnigraph_server`). The new layered-config stack should land in a **new shared `omnigraph-config` crate**, so the CLI stops pulling Axum/utoipa transitively just to parse YAML (see Implementation).
- **The config noun is `graphs:` (key) / `cli.graph` (default), but the shipped command-line flag is `--target`** (`main.rs:91,148,…`; field `target`, no `--graph` alias) — the code is itself split between "graph" config terminology and a "target" flag. This RFC unifies on **graph**: `--graph` becomes the canonical flag with `--target` kept as a deprecated alias (Migration).
- **`TargetConfig` models a graph as a single `uri: String`** with code branching on `is_remote_uri(uri)` (an `http(s)://` prefix check, `main.rs:686`). That string cannot express `{server, graph_id}`; today the only way to address a graph on a multi-graph server is to hand-write the prefix into the URI (`uri: https://host/graphs/prod`) and rely on the flat path append. §2 fixes this with the typed locator.
- **The CLI already speaks HTTP for many verbs** — `query`, `mutate`, `ingest`, `branch`, `commit`, `schema`, `snapshot`, `export`, `graphs` all have remote paths. But every URL is **flat** (`remote_url(&uri, "/branches")`, `…/commits`, `…/snapshot`, etc.) with **no `/graphs/{graph_id}/` prefix anywhere**, so the entire remote surface targets **single-graph-mode servers only** and 404s against a multi-graph server's nested routes. `query`/`mutate` additionally hit the **deprecated** `/read` (`main.rs:1991`) and `/change` (`main.rs:2068`), not the primary `/query`/`/mutate`. The HTTP client is therefore **extended**, not built from scratch.
- **Operations that bail on remote**: `load`, `lint`, `schema plan`, `optimize`, `cleanup` via `resolve_local_graph` → *"… is only supported against local graph URIs in this milestone"* (`main.rs:984`).
- **The CLI does not walk parent directories** — it reads `./omnigraph.yaml` in the cwd only (pinned by a `config.rs` test). Global-first is a deliberate posture flip.
- **What exists in the CLI** (verified): `init, query (read), mutate (change), load, ingest, branch, schema, lint, queries, snapshot, export, commit, policy, optimize, cleanup, graphs`. Note `queries` already shipped (the stored-query registry, PR #128). **Not built:** `login, use, config view, serve, quickstart`.
- **`scaffold_config_if_missing` exists** at `main.rs:1547` (invoked by `init`).
- **The default client bearer env is `OMNIGRAPH_BEARER_TOKEN`** (`main.rs:45`); the server uses `OMNIGRAPH_SERVER_BEARER_TOKEN[_JSON|_FILE|_AWS_SECRET]`. The implicit credential chain in §6 **reuses `OMNIGRAPH_BEARER_TOKEN`** rather than minting a new `OMNIGRAPH_TOKEN`.
- **The server already exposes the target surface**: `POST /query`, `POST /mutate`, `GET /queries`, `POST /queries/{name}`, `GET /graphs` (405 in single mode, list in multi), and the nested `/graphs/{graph_id}/…` cluster routes. `POST /graphs` and `DELETE /graphs/{id}` are intentionally **not** exposed. The one server-side change this RFC needs is **route unification** (§9).
- **`project.name` has no consumer** in the code; it is dropped. `server.graph` is purely the single-graph-mode selector (`lib.rs`); it is dropped in favor of structural mode (§9). `cli.actor` is the engine-layer policy actor default (`--as` > `cli.actor` > none, `main.rs:854`); it moves under `defaults:`.

## Motivation

Three problems, in priority order:

- **No multi-graph client targeting.** OmniGraph runs N graphs per server across M servers, but the CLI's remote path is flat-only and single-graph-only. There is no first-class way to say "graph `production` on server `prod-eu`," and the same graph is **multi-homed** — `s3://b/prod` may be `prod` on server A, `production` on server B, and opened directly by the CLI.
- **No global, no-project operation.** A solo developer or an agent should be able to define everything in `~` and run from any directory. Today the CLI is project-anchored.
- **Sub-optimal credentials for a multi-server world.** `bearer_token_env` is per-graph and forces the operator to invent and coordinate an env-var name per server. The peer group keys the secret **by the server's name** and supports interactive login, dynamic tokens, and OAuth. OmniGraph should match that.

## Non-Goals

- **A control plane / runtime config-mutation API.** Operators edit files and (for servers) restart.
- **Hot reload.** Restart-only for server-side config.
- **Embedding secrets in any config file.** Credentials are by-reference; secrets live in the OS keychain or a `0600` profile file, never a committable `*.yaml`, never in the project tree.
- **Renaming the project manifest by role.** Role lives in sections, not filenames (§5).
- **Dropping embedded mode.** Embedded-first is load-bearing for the file-layout decision.
- **Cross-graph / cross-server tool listing in MCP.** Clients loop over per-graph catalogs.
- **Managing cloud-storage credentials.** Embedded graphs authenticate to object storage via the standard cloud chain (`AWS_*`, instance roles); OmniGraph does not own those (§6).
- **Cloud-mode multi-tenancy.** A future multi-tenant Cloud tier (tenant resolved from the OAuth `org_id` claim, per-tenant Cedar bundles, dynamic graph lifecycle, `DELETE /graphs`) is out of scope and lands in the cloud RFC (MR-956 RFC 0003/0004). This RFC only **reserves the shapes** so that work is additive — `serve.auth.oauth` multi-issuer + `tenant_claim` (§6), `serve.policy` as a tagged source (§6), `(server, org)` credential keying (§7), and the `GraphKey { tenant_id, graph_id }` registry seam already shipped in MR-668 (§9). Tenant is **server-resolved from the token** (the MR-731 invariant, `identity.rs:180`) and never appears in the locator, URL path, or request body.

## Background

OmniGraph runs on Lance 6.x: typed nodes/edges in per-type Lance datasets, atomic multi-table commits via a `__manifest` table, branchable and time-travelable. The CLI operates the **embedded engine** directly against a storage URI. `omnigraph-server` (Axum) is a separate HTTP front-end over the same engine, with bearer auth + per-graph Cedar.

OmniGraph **already has a credentials-by-reference mechanism** this RFC builds on: `bearer_token_env` names the env var holding a graph's bearer token; `auth.env_file` points at a git-ignored dotenv that the CLI auto-loads (`load_env_file_into_process`, `main.rs:755`, real-env-wins); `resolve_remote_bearer_token` (`main.rs:870`) resolves a token via env then dotenv.

The six **irreducible enablers** that drive the design (E1–E6):

| # | Enabler | Consequence |
|---|---|---|
| E1 | A graph is a **self-contained storage URI**; the substrate is the source of truth — no server required to read/write. | A graph is addressable **directly (embedded)**, not only via a server. |
| E2 | A server hosts **many graphs**; **many servers** exist. | The remote address space is **`{server} × {graph_id}`**. |
| E3 | The same graph is **multi-homed** under different per-locus names; a server can **enumerate its own graphs** (`GET /graphs`, `graph_list`-gated). | **Name ≠ identity.** Addressing a graph by a *known* `server/graph_id` needs only read/invoke permission on that graph; *discovering* what exists is `graph_list`-gated. Clients need not pre-declare each graph. |
| E4 | **Branch / commit / snapshot** are first-class addressable sub-state. | An address is *graph @ branch/snapshot*, not just graph. |
| E5 | Enforcement is **two-layered**: engine-layer Cedar (`_as` writers, embedded) + HTTP-boundary bearer+Cedar (server only). | *How* you reach a graph determines *which* enforcement applies. |
| E6 | **Stored queries / MCP tools are a per-graph registry in the deployment config.** | The **agent tool surface is version-controlled in the repo.** |

There are also **two distinct credential domains**, conflated nowhere in this design:

- **Bearer / session credentials** (client → remote server). OmniGraph owns these: keychain / `credentials` / env / OAuth (§6).
- **Cloud-storage credentials** (embedded engine → object store). The ambient cloud chain owns these; OmniGraph only consumes them.

## Design

### 1. The address space and resolution

Every OmniGraph address is a tuple:

```
(locus, graph, sub-state, credential)
  locus      = embedded(storage URI)  XOR  remote(server endpoint)     # E1, E2
  graph      = a storage URI (embedded)  |  a graph_id on a server (remote)  # E3
  sub-state  = branch | snapshot                                       # E4
  credential = cloud-storage chain (embedded) | server auth (remote)   # E5
```

The config's job is **name → this tuple**. Two nouns express it:

- **`servers:`** — named remote endpoints (+ auth-by-reference). First-class addressable.
- **`graphs:`** — named graph locators (embedded or remote). For remote graphs these are *optional aliases*; a server alone is addressable without them.

**Resolution of `--graph X`** (the single rule, applied identically everywhere):

```
1. graphs.X exists?                  → that locator (Embedded or Remote)        # local alias wins
2. X is "srv/gid" and servers.srv?   → Remote { server: srv, graph_id: gid }    # qualified, no alias needed
3. defaults.server set?              → Remote { server: defaults.server, graph_id: X }
4. otherwise                         → error (unknown graph; no default server)
```

`/` is disallowed in a local alias name, so `srv/gid` is unambiguous (the `docker registry/image` pattern). Step 1 may resolve to either variant; steps 2–3 always resolve `Remote`. Snapshot/branch pins from the entry (or `defaults`) attach to the resolved locator and are overridable by `--branch` / `--snapshot`.

**With no `--graph`:** bare commands use `defaults.graph` (a graph alias). `defaults.server` is **not** a fallback graph — it only supplies the server for step 3 above when an explicit but otherwise-unknown id is passed. So `omnigraph query` → `defaults.graph`; `omnigraph query --graph production` (no alias `production`, no `/`) → `production` on `defaults.server`.

This yields **three addressing tiers**, all valid in either config layer:

| Tier | You write | You get | Ceremony |
|---|---|---|---|
| Env, no file | `OMNIGRAPH_SERVER=https://…` + token | reach any hosted graph by id | zero |
| `servers:` entry | a named endpoint (+ auth-by-ref) | reach **any** graph it hosts as `server/graph_id` | one entry per *server* |
| `graphs:` entry | a local alias → `{server, graph_id, branch, snapshot}` | short name, branch pin, multi-homing | one entry per *aliased graph* |

### 2. The typed locator (`storage:` vs `server:`)

The shipped model is one `uri: String` plus `is_remote_uri` sniffing at ~16 dispatch sites. That conflates two structurally different addresses: an **embedded** graph is a complete self-contained address (one storage URI = one graph), while a **remote** graph is a *server endpoint + a `graph_id`* (one server hosts N graphs). The *resolved* address is therefore a **typed locator**, not a string:

```rust
enum GraphLocator {
    Embedded { storage: Storage },                     // a complete graph on an object store
    Remote   { server: ServerId, graph_id: GraphId },  // which server + which graph (+ server auth)
}
```

A `graphs:` entry resolves into this **once**; downstream code dispatches on the variant instead of re-sniffing a scheme at each call site.

**The key names the locus** — so neither the value's scheme nor a comment is load-bearing:

| Locus | Key | Value |
|---|---|---|
| Embedded | **`storage:`** | a storage location (string or block, below) |
| Remote | **`server:`** | a name in `servers:` (its `endpoint` + auth resolve by name) |
| Remote graph id | **`graph_id:`** | the id on that server — **defaults to the entry key** |

An entry has `storage:` **xor** `server:`; the deserializer rejects both and neither.

**`storage:` is a string-or-block.** The bare scalar covers the common case; the block form gives per-graph object-store options a home (region/endpoint/profile) without a future breaking change, and keeps `uri:` as the precise word for "location" exactly where it is now unambiguous (`storage.uri` is always embedded):

```yaml
dev:  { storage: s3://team/dev.omni }            # scalar sugar ⇒ storage: { uri: s3://team/dev.omni }
prod:
  storage:
    uri: s3://team/prod.omni
    region: eu-west-1
    endpoint: https://minio.local                # S3-compatible override
    profile: team-deploy                          # named cloud profile (env-only — see note)
```

Shipped flat `uri:` becomes a deprecated alias mapped to `storage.uri` with a load-time warning.

**Validation (Lance 6.0.1):** `region`/`endpoint` are threadable per-graph today — Lance accepts per-dataset `storage_options` (`builder.rs:165-176,305`) and omnigraph currently hardcodes `storage_options: None` (`namespace.rs:228,376`); wiring them is omnigraph-internal, no Lance change. **`profile` is the exception** — `AWS_PROFILE` is env-only in both Lance and omnigraph's `AmazonS3Builder::from_env()` (`storage.rs:284`), so `storage.profile` is **scoped out of v1** unless omnigraph resolves the profile to concrete credentials itself. `region`/`endpoint` land in V2 (engine threading); `profile` stays a documented Open Question.

### 3. Invalid configs are rejected by design

The DX rule: **a config field is either honored or rejected, never silently ignored.** The loader has two phases:

1. Parse YAML into a raw, origin-preserving shape (`base_dir`, layer, path), with **`deny_unknown_fields`** so a typo errors instead of becoming a silent no-op.
2. Convert once into a typed, role-aware resolved config. Every command receives the resolved form.

```rust
struct Config {                  // identical schema at both layers; deny_unknown_fields
    version:  u32,               // schema version — forward-compat + clean deprecation gate
    servers:  Map<ServerId, Server>,
    graphs:   Map<GraphName, GraphEntry>,
    defaults: Defaults,
    serve:    Serve,             // host-role serving config (see §5/§9)
    aliases:  Map<AliasName, Alias>,
    query:    QueryRoots,        // client-role: search roots for ad-hoc `--query <path>` .gq files
}

enum GraphEntry {
    Embedded(EmbeddedGraph),     // storage: present
    Remote(RemoteGraph),         // server: present
}
struct EmbeddedGraph { storage: Storage, branch: Option<Branch>, snapshot: Option<Version>,
                       policy: Option<PolicyFile>, queries: Map<Name, QueryDef> }
struct RemoteGraph   { server: ServerId, graph_id: GraphId, branch: Option<Branch>, snapshot: Option<Version> }
```

This makes the rules structural rather than advisory:

- A graph entry must specify **exactly one** locator (`storage:` xor `server:`).
- `policy:` and `queries:` are valid **only** on `Embedded` entries — they define the capability surface of a graph this process opens directly. A `Remote` entry points at a server that owns its own policy and stored queries.
- `omnigraph-server` may serve only `Embedded` entries; a server manifest entry with `server:` is rejected (a server must not proxy another server).
- A `Remote` entry discovers stored queries from the server (`GET /queries`) and invokes them (`POST /queries/{name}`); it never defines `queries:` locally.

Examples that must fail fast:

```yaml
graphs:
  bad1: { storage: s3://b/prod.omni, server: prod-us }      # invalid: storage xor server
  bad2: { server: prod-us, graph_id: production,
          policy: { file: ./p.yaml } }                       # invalid: remote policy lives on the server
```

`omnigraph config view --resolved --show-origin` is the user-facing debugger: it prints the final `Embedded`/`Remote` locator and the origin layer of every honored field. Fields that cannot be honored fail validation first; they never appear in the resolved view.

### 4. Layered config — global-first, uniform schema, project-optional

**Posture: global-first, project-optional.** The CLI is primarily a *client*, so it sits on the global-first side of the axis — like `kubectl`/`aws`/`gh`/`docker`. The **global user config is the primary, self-sufficient default**; the project file is an optional repo-scoped override (and, when present, the deployment manifest). `omnigraph query --graph prod` must work from any directory with no project file.

**One raw schema, both layers, each self-sufficient.** Do not specialize the format by layer. Run the same role-aware validation everywhere (§3): a layer may define graphs, defaults, servers, and aliases, but fields meaningless for a resolved variant are rejected, not ignored.

| Layer | Required? | Typical use | Path |
|---|---|---|---|
| Global | no | **the default** — solo/agent's entire config; shared servers+creds for teams | `~/.omnigraph/config.yaml` |
| Project | no | **opt-in** — repo-scoped overrides + the committed deployment manifest | `./omnigraph.yaml` |

**Precedence (low → high):** built-in defaults < global < active-context state (§5) < project < env vars < CLI flags. With no project file it collapses to built-in < global < state < env < flags.

**Merge semantics — "closest layer wins, at the smallest meaningful unit":**
- **Settings objects** (`defaults`, `serve`) → deep-merge per field: a project sets `defaults.graph` and inherits the global `defaults.output_format`.
- **Named-resource maps** (`servers`, `graphs`, `aliases`) → union by key; on a collision the **higher-precedence** layer's entry **replaces** the lower wholesale (no field-level deep-merge within an entry — replace makes the entry self-contained and predictable). Per-graph `queries:` are not a top-level map; they merge as part of their owning `graphs` entry (replaced with it).
- **Server identity follows trust, not precedence (security).** Precedence and trust run *opposite* for the project layer: project is **higher-precedence** (it wins value merges, above) but **lower-trust** (a repo an agent can edit or a clone can ship). A `servers:` entry's `endpoint` and `auth` are its **identity**, and identity follows trust — a lower-trust layer may add *endpoint-only* servers and graph aliases, but may **not** (a) redefine the `endpoint` of a server a higher-trust layer defined, nor (b) carry a `servers.<name>.auth` block — *client* credential sourcing — at all (no `command`/`file`/`keychain`/`token` sourcing; `command` would be repo-authored RCE). Both are rejected. (`serve.auth`, the secret-free server-side *accept* config, is unaffected — it is exactly what a committed deployment manifest carries; §6.) Without this, a project file could repoint `servers.prod.endpoint` or inject `auth.command` and, since credentials key by name, harvest or execute against the user's `prod` identity. The credential trust model in §7 enforces the consuming side.
- **Lists** → replace, never append.
- **Scalars** → higher layer wins.
- **Relative paths carry their origin's `base_dir`** — a `queries:` `.gq` path or a `policy.file` resolves against the directory of the layer it was defined in.
- **Inspectable (non-negotiable):** `config view --resolved --show-origin` prints each final value and the layer that set it.

### 5. File layout, naming, and the secrets boundary

The layout splits on the **two boundaries that are actually irreducible — secrecy and scope — and never on role**:

| Axis | Real boundary? | Why |
|---|---|---|
| Secrecy (secret vs secret-free) | **yes, hard** | Security + AX: a secret-bearing file in the repo is exfiltratable by an agent and committable by a human. |
| Scope (user-global vs project-local) | **yes, hard** | Different lifecycle, owner, and VCS status. |
| Role (client vs server) | **no, soft** | On a laptop they collapse (E1); in prod they are different *repos* sharing a schema. Role is which sections are filled, not which file. |

```
~/.omnigraph/                       # global, user-scoped, machine-local, NEVER in VCS
├── config.yaml                     # servers + personal graphs + defaults + aliases   (SECRET-FREE)
├── credentials                     # INI, [server] → token, 0600, gitignored   (FALLBACK; keychain preferred)
├── cache/                          # remote catalogs (GET /graphs), OAuth token cache — rm -rf safe
└── state/                          # active-context (omnigraph use), session logs

<repo>/omnigraph.yaml               # project = deployment manifest, committed, portable   (SECRET-FREE)
<repo>/schema.pg, queries/*.gq, policies/*.yaml

# secrets at rest:  OS keychain  omnigraph:<server>   (preferred — no plaintext file)
# secrets in CI:    OMNIGRAPH_BEARER_TOKEN[_<SERVER>] env
```

**Naming decisions (best-practice + de-conflicted; breaking where it removes ambiguity):**

| Shipped | This RFC | Why |
|---|---|---|
| `server:` (self) vs `servers:` (remote) | **`serve:`** vs `servers:` | Two keys one letter apart with opposite meaning is the worst ambiguity in the current schema. `serve:` = "config when I serve"; `servers:` = "remotes I target." |
| `uri:` (graph-entry top level) | **`storage:`** (string-or-block; `uri:` nested) | `uri:` conflated embedded/remote (§2). |
| `cli:` block | folded into **`defaults:`** | "default graph/branch/format/actor" is one concept; no consumer-specific block. |
| top-level `policy:` / `queries:` | **removed** | per-graph only; deletes the dual-site reconciliation machinery. "Single-graph mode" = a one-entry `graphs:` map. |
| `bearer_token_env:` (per-graph) | **`servers.<>.auth.bearer.token.env`** | auth is per-server (§6); old field kept as a legacy alias. |
| `auth.env_file` (project dotenv) | **deprecated (warned)** | no secret-bearing file in the project tree. |
| `aliases.<>.query: <path>` + `command:` | **`aliases.<>.query: <name>`** (reference) | an alias references a *defined* query; read/mutate inferred (§8). |
| `project: { name }` | **removed** | no consumer. |
| *(none)* | **`version: 1`** + `deny_unknown_fields` | forward-compat; typos error rather than no-op. |
| `query.roots:` | **retained** | resolves ad-hoc `--query <relative>.gq`; orthogonal to the alias/registry model. |

Conventions kept: **snake_case** keys; **plural maps** keyed by name; **`~/.omnigraph/config.yaml`** global (named `config` — the universal convention) + **`./omnigraph.yaml`** project (app-named manifest). `OMNIGRAPH_HOME` overrides the global dir; `OMNIGRAPH_CONFIG` overrides the config file path; `$XDG_CONFIG_HOME` honored if set, but `~/.omnigraph/` is canonical.

**Active context is *state*, not declarative config.** `omnigraph use <graph>` writes `~/.omnigraph/state/active.yaml` (a thin `{server, graph}`), leaving the user-authored `config.yaml` pristine — avoiding kube's comment-stripping rewrite of `~/.kube/config`. It slots into precedence between global and project (§4).

**Four hard rules (promote to invariants):**
1. **No secret in any `*.yaml`, ever** — global or project. Secrets: keychain → `credentials` (INI, `0600`) → env.
2. **No secret-bearing file in the project tree.** (Kills project-local `.env.omni`; kept as a warned compat path, removed next major.)
3. **The project tree carries capability + targeting, never identity.** A project layer may *target* servers and define graphs, but it may not assert a server's identity — redefining a higher-layer server's `endpoint`/`auth` is rejected (§4), and credentials are endpoint-bound (§7). This is the AX guarantee that makes "hand an agent a repo" safe by construction.
4. **`config.yaml` ⊇ `omnigraph.yaml` schema; scope is the only difference.** Same parser, role-aware validation, `config view --resolved` is the disambiguator.

### 6. Auth — method × source are orthogonal

The shipped code knows only bearer-from-env. Two independent axes must be separated:

- **Method** = *what kind of credential/protocol*: `bearer`, `oauth`, `mtls`, `none`. Exactly one per server.
- **Source** = *where secret material is read from*: `env`, `file`, `command`, `keychain`. Reusable wherever a secret is needed.

OAuth is **not** "just another token source": it has an interactive flow, endpoints (issuer/client_id/scopes), and refresh semantics, and its tokens are minted by `omnigraph login` and cached in the keychain — never in config. So it is a *method* with its own fields.

```rust
// servers.<name>.auth — fully optional; absent ⇒ implicit bearer chain keyed by name
enum Auth {
    Bearer { token: SecretSource },
    None,                                   // explicitly unauthenticated (not accidental)
    // Reserved — shape-stable but not implemented in v1 (own milestone, see Rollout V6):
    OAuth  { issuer: Url, client_id: String, scopes: Vec<String>, audience: Option<String> },
    Mtls   { cert: SecretSource, key: SecretSource },
}
enum SecretSource {
    Env(String),           // env:      OMNIGRAPH_BEARER_TOKEN_PROD
    File(PathBuf),         // file:     /run/secrets/og-token
    Command(Vec<String>),  // command:  [vault, read, -field=token, secret/og]  (argv list, no shell)
    Keychain(String),      // keychain: omnigraph:prod
}
```

**Externally-tagged** (the key names the method/source), consistent with §2 — a field under `oauth:` cannot leak into `bearer:`.

| Method / source | Use case | YAML |
|---|---|---|
| *(omit `auth:`)* | the common case | implicit chain (below) |
| `bearer.token.env` | CI / secrets-manager fixed var | `auth: { bearer: { token: { env: OG_PROD_TOKEN } } }` |
| `bearer.token.file` | k8s/docker mounted secret | `auth: { bearer: { token: { file: /run/secrets/og } } }` |
| `bearer.token.command` | Vault / cloud IAM / `gh auth token` | `auth: { bearer: { token: { command: [vault, read, -field=token, secret/og] } } }` |
| `bearer.token.keychain` | pin a non-default keychain entry | `auth: { bearer: { token: { keychain: omnigraph:prod } } }` |
| `oauth` | SaaS / SSO — `omnigraph login` device flow | `auth: { oauth: { issuer: https://auth.og.cloud, client_id: og-cli, scopes: [graph.read, graph.write] } }` |
| `mtls` | client-cert networks | `auth: { mtls: { cert: { file: ./client.pem }, key: { file: /run/secrets/og-key.pem } } }` (key off the repo tree — hard rule 2) |
| `none` | open dev server | `auth: { none: {} }` |

**Scope (v1): only `bearer` and `none` are implemented.** `oauth` and `mtls` are **reserved** — the enum shape is fixed (so adding them later is not a breaking re-key, per Hyrum's Law), but a config selecting them errors with "auth method not yet supported." Client-side OAuth login (device flow, token cache, refresh) is a later milestone (Rollout V6); **server-side OIDC validation is owned by the Federated Auth workstream (MR-956 RFC 0001)** — `serve.auth.oauth` (below) is its YAML home and may land on its own timeline. mTLS is V6.

**Auth is per-server, not per-graph.** One credential authenticates you to a *server*; Cedar then authorizes per graph. The shipped per-graph `bearer_token_env` is the wrong grain for a multi-graph world (it repeats across every graph on a server); it survives as a legacy alias for `servers.<n>.auth.bearer.token.env`.

**The `command` source** runs locally with the operator's own privileges, so a `servers.<name>.auth` block — `command` especially — is **rejected from a lower-trust (project) layer** (§4): it is honored only from global/trusted config, never from a repo, so it adds no remote-execution surface. The `auth:` union is method-tagged so adding a method later is a new variant, not a re-key (Hyrum's Law: the field name is a contract once shipped).

**Server-side accept config is separate and secret-free** (it validates incoming credentials; it is not a credential) and lives under `serve:`:

```yaml
serve:
  auth:
    bearer: { enabled: true }                                  # tokens via OMNIGRAPH_SERVER_BEARER_TOKEN* env
    oauth:                                  # reserved shape; verifier owned by MR-956 RFC 0001
      issuers:                              # LIST from day one — scalar→list would be a breaking re-key
        - issuer:  https://auth.og.cloud
          audience: og-api
          tenant_claim: org_id              # → ResolvedActor.tenant_id (None in Cluster, Some in Cloud)
          # actor_claim / scope_claim / jwks_* field schema owned by MR-956 RFC 0001
  policy: { file: ./policies/server.yaml }                      # server-level Cedar (management endpoints)
  # bind/workers are 12-factor: --bind today (OMNIGRAPH_BIND is proposed, not yet implemented), never committed here
```

**Reserved for cloud (shape only; see Non-Goals).** Two forward-compat shapes ship in v0.8.x so the multi-tenant Cloud tier is additive, not a breaking re-key: (1) `serve.auth.oauth.issuers` is a **list** carrying `tenant_claim` (→ `ResolvedActor.tenant_id`, already present at `identity.rs:189`) — the verifier and full field schema (`jwks_*`, `clock_skew`, actor/scope claims) are **owned by MR-956 RFC 0001**, which this block is the YAML home for; this RFC reserves only the top-level shape and defers fields there, so there is **one** OIDC schema, not two. (2) `serve.policy` is a **tagged source keyed at the `policy` level** (like `storage:`/`auth:`) — `file` today, `directory`/`manifest` reserved for per-tenant Cedar bundles — so adding variants is additive, with **no `source:` wrapper** (which would be a needless re-key). Both stay parse-but-reject until implemented.

### 7. Credential resolution and connection tiers

**Implicit chain for server `<name>`** (when `auth:` is omitted), keyed by name, reusing the shipped env var:
1. **`OMNIGRAPH_BEARER_TOKEN_<NAME>`** (name-derived, upper-snake), else **`OMNIGRAPH_BEARER_TOKEN`** for the active server — the CI/headless override.
2. **OS keychain** `omnigraph:<name>` — the preferred interactive store; written by `omnigraph login <name>`.
3. **`~/.omnigraph/credentials`** — INI profile keyed by server name (`0600`, git-ignored):
   ```ini
   [prod-us]
   token = …
   [prod-eu]
   token = …
   ```

**Credential trust model (security).** Two rules close the credential-redirection path:
1. *Implicit/ambient credentials apply only to trusted-origin servers.* The implicit chain above (env-by-name, keychain-by-name, profile) is consulted **only when the server's identity — its `endpoint` — came from a trusted layer** (global config, or an explicit operator source). A server whose identity is introduced by a lower-trust (project) layer never auto-consumes an ambient credential: it is **unauthenticated (local-dev) by default**, and authenticated use requires either promoting it to a trusted layer (a global `servers.<name>`) or an operator-supplied credential at invocation — a `--token-from <env|file|command>` flag (operator-trust, not repo-supplied; a future addition, §10). This is what makes env-by-name safe: a raw `OMNIGRAPH_BEARER_TOKEN_<NAME>` carries no issued-for endpoint, so it is trustworthy only when the *name → endpoint* binding it rides on is itself trusted.
2. *login-written credentials additionally bind to their endpoint.* `omnigraph login <server>` records `(name, endpoint)`; at use, the keychain/profile token is released only if the resolved endpoint still matches, erroring otherwise (`server 'prod' resolved to <endpoint>, which does not match the endpoint this credential was issued for`). This catches a trusted server whose endpoint later changes.

Together with the §4 identity rule (a lower-trust layer can neither repoint a trusted server nor carry `servers.<name>.auth`), ambient credentials cannot be redirected to an attacker endpoint.

**Forward-compat (cloud, reserved; see Non-Goals).** Endpoint-binding keys a credential to `(name, endpoint)`, but a multi-org user on **one** cloud endpoint holds many tokens that all bind to that endpoint — so endpoint-binding alone cannot disambiguate them, and the credential identity unit becomes `(server, organization)`. Reserve `omnigraph:<server>[/<org>]` keychain keying and `[<server>/<org>]` profile sections now (additive). The org is server-resolved from the token (never a client-asserted field), so this is a storage-keying concern only.

If `auth:` is set, that source is used (no fallthrough). `omnigraph login <server>` writes/rotates only that server's secret (keychain preferred; OAuth, when implemented (V6), runs the device flow and caches tokens in the keychain → `~/.omnigraph/cache/oauth/`). There is **no `credentials.yaml`** and no inlined secret. *Convention for the floor, explicit for control.*

**Cloud-storage credentials** for embedded `storage:` graphs come from the ambient cloud chain (`AWS_*`, instance roles, `~/.aws/credentials`), optionally narrowed by `storage.profile`/`storage.region`/`storage.endpoint` (§2). OmniGraph never stores object-store secrets.

**Three connection tiers** (the zero-config floor):
1. **Env vars** — `OMNIGRAPH_SERVER=https://…` + token: fileless remote (the `DATABASE_URL` floor; `OMNIGRAPH_SERVER` is new).
2. **Global `config.yaml`** — named `servers:` (+ optional graph aliases) for multi-server setups.
3. **Project `omnigraph.yaml`** — project-pinned graphs/aliases, committed.

### 8. Stored queries (definitions) vs. aliases (invocations)

A stored query and a CLI alias are different concepts; do not collapse them, but do remove their overlap:

- **Definition** (`.gq` source + a `queries:` entry) lives next to the **embedded graph entry that owns it** — for a hosted graph, the deployment manifest read by `omnigraph-server`. It is the capability surface (Cedar-gated when served, MCP-visible when exposed). It never lives on a `Remote` entry.
- **Discovery** ("what can I call?") is fetched from the server (`GET /queries`, Cedar-filtered) at connect time.
- **Invocation** is remote (`POST /queries/{name}`) or embedded (open the graph, read the same manifest).
- **Alias** = a client-side *saved invocation* that **references** a defined query and binds invocation context — it never defines a `.gq`:

```yaml
graphs:
  prod:
    storage: s3://team/prod.omni
    queries:
      find_user: { file: ./queries/find_user.gq, mcp: { expose: true, tool_name: lookup_user } }

aliases:
  owner: { graph: prod, query: find_user, branch: review, format: table, args: [name] }
```

This is the **capability-as-code guarantee for agents**: an agent can only invoke tools the server's committed, reviewed config exposes; it cannot define a new tool at runtime. Making the alias a *reference* (not a second definition site with an inline `.gq` path and an explicit `command`) removes the "alias and query with the same name are different namespaces" footgun and the duplicate-definition drift, while keeping saved-invocation ergonomics. Read vs mutate is inferred from the referenced definition.

### 9. Server-mode disambiguation (the V2 prerequisite)

**What the server serves.** `serve.graphs: [<name>, …]` selects which embedded `graphs:` entries this process serves (default: **all** embedded entries). It subsumes the removed `server.graph` (a one-element list). Mode is derived from the served count: one ⇒ single, many ⇒ multi.

**Canonical wire id.** Every served graph has a canonical `graph_id` — its `serve.graphs` selection name, or `default` for a bare-URI server started with no config. The server **always mounts `/graphs/{graph_id}/…`**. The legacy flat routes (`/query`, `/branches`, …) remain **only when exactly one graph is served**, as a compat alias bound to that graph. `GET /graphs` returns the served set (one entry in single mode — today's single-mode 405 is removed) and stays `graph_list`-gated — so with default-deny on server-scoped actions, single-mode `GET /graphs` returns **403 unless a `serve.policy` authorizes `graph_list`** (405→403, not →200). **Open decision (validated):** the wire `graph_id` (`default` for a bare-URI server) and the Cedar *resource* id (today the normalized URI, `graph_resource_id_for_selection`) differ for anonymous graphs; either accept the split or align the anonymous Cedar id to `default` (a policy-identity break for existing single-graph deployments).

**Client.** The client config is **mode-agnostic**: a `Remote` locator always carries `graph_id`, and the client always builds `/graphs/{graph_id}/…`. It never needs to know a server's deploy mode.

This avoids shipping two URL shapes for the same operation depending on a config mode (a Hyrum's-Law liability) and lets the existing CLI remote paths be rewired once to the prefixed form (and migrated off the deprecated `/read`/`/change`). The fallback, if route unification is deferred, is a cached `GET /graphs` probe in `~/.omnigraph/cache/` (the catalog already returns each `graph_id`); it is strictly worse and not preferred. **V2 is gated on route unification.**

**Forward-compat (cloud, reserved; see Non-Goals).** The unified registry stays keyed by **`GraphKey { tenant_id: Option<TenantId>, graph_id }`** — already shipped in MR-668 (`identity.rs:116`, `tenant_id = None` in Cluster/embedded). Folding `Single`/`Multi` into one registry (V2) must **not** flatten it to `graph_id`-only: Cloud mode sets `tenant_id = Some(...)` from the token's `org_id`, two tenants may each own `production`, and `GET /graphs` becomes tenant-scoped (filtered to the resolved tenant; cross-tenant default-deny). Tenant is resolved from the token, never the path.

### 10. CLI surface

- `omnigraph login <server>` — interactive auth; stores the token in the keychain (`omnigraph:<server>`) or the `[<server>]` profile (`0600`); runs the OAuth device flow for `oauth` servers (V6). The `gh auth login` analog.
- `omnigraph use <graph>` — set the active context; writes `~/.omnigraph/state/active.yaml`. The `kubectl config use-context` analog.
- `omnigraph config view [--resolved] [--show-origin] [<graph>]` — print the merged config and, with `--resolved`, the final locator plus the origin layer of every field.
- `--token-from <env|file|command>` (future) — an operator-supplied one-shot credential, to authenticate against a server whose identity is *not* in a trusted layer (§7). Operator-trust, never repo-supplied.
- All existing verbs gain `--graph <name>` (the shipped flag is `--target`, kept as a deprecated alias); resolution (§1) decides embedded vs remote transparently.

### 11. Init, login, bootstrap — three tiers

| Tier | Command | Scope | What it does | Status |
|---|---|---|---|---|
| **User route** | `omnigraph login [<server>]` | user (`~/.omnigraph/`) | auth + write `config.yaml`/`credentials`; first-run global setup | this RFC (unbuilt) |
| **Thin project init** | `omnigraph init` | project, in-place | create graph + `scaffold_config_if_missing`; refuse-if-exists or `--force` | exists; `--force` purge unbuilt |
| **Fat bootstrap** | `omnigraph quickstart [--template <t>] [--auto]` | project | scaffold + seed + serve + agent prompt file | unbuilt (needs `serve`) |

Design positions: **split `init` (project) from `login` (user)** — never one command writing to both `$HOME` and the project; **`init` is in-place + refuse-if-exists** (cargo/prisma default); **interactive for humans, `--auto`/`OMNIGRAPH_AGENT_MODE` for automation** (any prompt → fail with a repair hint); **templates are a `--template` flag** on the fat tier; **secrets-on-scaffold rule** — anything that writes a token keeps it out of VCS (keychain preferred; `credentials` is `0600` and git-ignored).

## Concrete shape

**Global** `~/.omnigraph/config.yaml` (per-user, secret-free):
```yaml
version: 1
servers:
  prod:  { endpoint: https://og.internal:8080 }        # auth omitted ⇒ implicit chain keyed by name
  cloud:
    endpoint: https://api.og.cloud
    auth: { oauth: { issuer: https://auth.og.cloud, client_id: og-cli, scopes: [graph.read, graph.write] } }  # reserved/future (V6)
graphs:
  personal: { storage: ~/graphs/personal.omni, branch: main }
  review:   { server: cloud, graph_id: production, branch: review }   # optional pinned remote alias
defaults: { server: cloud, graph: personal, output_format: table, actor: ragnor }
aliases:
  people: { graph: personal, query: list_people }
```

**Project** `./omnigraph.yaml` (committed, secret-free, portable — read by CLI *and* server):
```yaml
version: 1
graphs:
  production:                                  # embedded ⇒ served; capability surface lives here
    storage: s3://team-bucket/prod.omni
    policy:  { file: ./policies/prod.yaml }
    queries:
      find_user: { file: ./queries/find_user.gq, mcp: { expose: true, tool_name: lookup_user } }
  staging:                                     # remote ⇒ a target; no policy/queries (server-owned)
    server: prod
    graph_id: prod
    branch: review
defaults: { graph: production, branch: main, output_format: table }
serve:
  graphs: [production]                          # which embedded graphs to serve (default: all)
  auth:   { bearer: { enabled: true } }         # bind via --bind (OMNIGRAPH_BIND proposed; see Rollout)
  policy: { file: ./policies/server.yaml }
```

**Credentials** `~/.omnigraph/credentials` (INI, `0600`, git-ignored — fallback when no keychain):
```ini
[prod]
token = …
```
`omnigraph login prod` writes the keychain entry `omnigraph:prod` (preferred) or this profile; `OMNIGRAPH_BEARER_TOKEN_PROD` overrides for CI. No token fields in any YAML; no committable secrets.

## DX

1. **One command surface, two loci.** `query --graph dev` (embedded) and `--graph staging` (remote) are the same command; only resolution differs.
2. **Point at a server, use it.** A `servers:` entry reaches every graph the server hosts as `server/graph_id` *if you know the id* — no per-graph declaration. (Listing what exists needs the `graph_list` permission, which the server may default-deny.) `omnigraph login <server>` once, then every target resolves.
3. **Multi-server × multi-graph is the default.** `prod-us` and `prod-eu` both serving `production` is two `servers:` entries (or two graph aliases) — Helix cannot express this.
4. **Solo-first.** Everything in `~`, no project required.
5. **Laptop-to-fleet on one schema.** Local = one `omnigraph.yaml` (both roles); prod = role-split across repos. No second format.

## AX (agent experience)

1. **One flat resolved context.** graph→server→endpoint→token resolves before the agent sees anything; `config view --resolved` flattens it. The agent reasons about tools, not topology.
2. **Secrets are outside the repo and trust-gated.** No secret-bearing file in the repo (hard rule 2); tokens live in the keychain / global layer / env, and ambient credentials apply only to trusted-origin servers (§7). A repo-confined agent cannot read a token, and cannot exfiltrate one by repointing or introducing a server — the §7 trust model and §4 identity rule withhold it. See the threat model below for the precise boundary.
3. **Branch/snapshot-pinned contexts** (E4) — hand an agent a `branch: review` / `--snapshot v42` graph and its reads are reproducible and cannot see uncommitted main-line state.
4. **Capabilities are a GitOps'd artifact** (E6) — which graphs exist, which stored-query tools it may call, and which Cedar rules gate them are all in version-controlled config. Powers change only via a reviewed PR + restart.
5. **Config + policy compose.** Config = "where am I pointed + which token"; Cedar = "what may I do there." Orthogonal.

**Threat model & secret boundary.** The agent/repo boundary is a trust boundary, held by three rules: (1) secrets live outside the repo — keychain or `~/.omnigraph/`, never project config or the tree (hard rule 2); (2) a lower-trust layer cannot redefine a server's identity (§4); (3) credentials bind to an endpoint, so a redirected server cannot harvest a token (§7). Caveat — "outside the agent's reach" means the **repo-confined** surface: a shell-capable agent with `$HOME` access can still read `~/.omnigraph/credentials`, so the OS keychain (no plaintext at rest) is the stronger posture and the default `login` target.

## GitOps — three surfaces, secrets in none

| Surface | Repo | Contents | Deploy | Secrets |
|---|---|---|---|---|
| Server deployment config | infra/deploy repo | `graphs:`, policy, `queries:` + `.gq` | commit → CI → **restart** | none — by-reference |
| Project client config | app repo | `graphs:` → embedded storage or remote server+graph | committed, read by CLI/agent | none |
| Global user config | machine-local `~` | `servers:` + creds-by-ref | `omnigraph login` writes it | refs only |

## Comparison

| Property | kubeconfig | Helix | git | compose | **OmniGraph (this RFC)** |
|---|---|---|---|---|---|
| Named remote endpoints + creds-by-ref | ✅ | ✅ | partial | partial | ✅ (global `servers`) |
| Global + project layering, uniform schema | ✗ | ✗ | ✅ | ✗ | ✅ |
| Embedded OR remote under one name | ✗ | ✗ | n/a | ✗ | ✅ (E1) |
| Server self-sufficient (no per-graph declare) | ✅ | ✗ | n/a | n/a | ✅ (E3) |
| Multi-server × multi-graph | ✅ | ✗ | n/a | n/a | ✅ (E2) |
| Branch/snapshot in the address | ✗ | ✗ | partial | ✗ | ✅ (E4) |
| Agent tool surface in the repo | ✗ | ✗ | n/a | n/a | ✅ (E6) |
| Pluggable auth methods (bearer/oauth/mtls) | ✅ (exec) | partial | ✗ | ✗ | ✅ |
| Concept count | 3 | 1 | 2 | 1 | **2 (servers/graphs)** |

## Divergence & single source of truth

The test (engineering integrated over time): does this design *prevent* divergence between the three surfaces — CLI, config, HTTP routes — by construction, or merely reduce today's instances?

**Structurally prevented:**
- **config ↔ CLI** — one noun (`graphs:`/`--graph`); a graph address resolves **once** into a typed `GraphLocator` (§2) that downstream dispatches on, instead of re-sniffing `is_remote_uri` at ~17 sites. A new command receives the resolved locator and cannot re-derive "server or file?" wrong. *Enforcement points:* a shared `GraphArgs` (one flag definition) and routing **every** command through the resolver — the current bare-`resolve_uri` re-sniff sites must be converted, not left.
- **config ↔ HTTP capability surface** — `policy:`/`queries:` live at exactly one site (the owning `Embedded` graph entry), read identically by the embedded CLI and the server; the dual top-level/per-graph reconciliation is deleted.

**Reduced, not prevented — the residual axis:**
- **CLI ↔ HTTP routes.** Route unification (§9) makes the path *shape* uniform, and *body* types are already shared (the CLI imports `api::*` DTOs, so a DTO change breaks CLI compilation — a compile-time guard). But **path strings stay hand-duplicated**: the server declares routes (`.route("/branches", …)`) and the CLI hand-writes the matching strings (`remote_url(&uri, "/branches")`), and the `omnigraph-ts` SDK is generated from a *vendored* `openapi.json` snapshot. So a new endpoint still forks three ways (server route + CLI client call + SDK re-vendor). Unification removes the *mode* divergence (flat vs nested) and the `/read`-vs-`/query` drift — not the structure that generates path divergence.

**The structural move that would close it (recorded, not in scope):** a shared route/operation table (path+method consts) consumed by both the server router and the CLI client, and/or generating the CLI's HTTP client from the same OpenAPI spec the SDK uses (the CLI is the only hand-maintained parallel client). Given ~17 slowly-growing endpoints and compile-shared bodies, this does not block the RFC — but **V2 is the cheap moment to add the shared path constants**, since it touches every path anyway.

**Net liability:** every duplicate-site count goes down (≈17 sniff sites → 1 locator; 2 route shapes → 1; dual policy/queries → 1; per-graph token → per-server; silent-ignore → honored-or-rejected). The added surface (merge+provenance engine, keychain, layered loader) is centralized — lower ongoing liability *provided* every command routes through the single resolver.

## Migration / breaking changes

Gated behind `version:`. `version: 1` is this schema; a missing `version:` is read as legacy (the shipped shape) with deprecation warnings.

**Compat aliases (legacy honored, warned):**
- `--target` flag → `--graph` (deprecated alias).
- `uri:` → `storage.uri`.
- `cli:` block fields → `defaults:`.
- `server:` (self) → `serve:`.
- `auth.env_file` dotenv → honored but warned (secrets-in-repo); removed next major.
- `bearer_token_env:` (legacy graph-local) → see "Renamed / migrated" below.

**Removed (hard errors under `version: 1`):**
- Top-level `policy:` / `queries:` — move to the owning `graphs.<name>` entry.
- `project.name` — no consumer.
- A `Remote` graph entry with local `policy:`/`queries:`; a `serve:` manifest with a `server:` graph locator; an alias with an inline `.gq` path.

**Renamed / migrated:**
- `server.graph` (single-graph selector) → **`serve.graphs: [<name>]`** (a one-element served set; §9). Not a removal — the "define many graphs, serve a subset" capability is preserved.
- **Legacy remote graph + credential mapping.** A legacy remote `{ uri, bearer_token_env }` has *no named server*, and its `uri` may already smuggle the multi-graph hack (`https://host/graphs/{gid}`). Under `version: 1` the migration **strips the trailing `/graphs/{gid}` suffix**: `https://host[/path]/graphs/{gid}` → `endpoint: https://host[/path]` (the full prefix, **including any reverse-proxy path**), `graph_id: gid`; a `uri` with no `/graphs/{gid}` suffix → `endpoint: <uri>`, `graph_id: <graph_name>`. It emits `servers.<name> = { endpoint, auth: { bearer: { token: { env: <VAR> } } } }` (treated as trusted on migrate) and rewrites the graph to `{ server: <name>, graph_id }`. Splitting the `/graphs/{gid}` suffix is required — otherwise V2's always-`/graphs/{id}/…` client would build `https://host/graphs/{gid}/graphs/<name>`. In legacy mode (no `version:`) the graph-local credential keeps working unchanged.

**Posture flips:**
- **Global-first.** The CLI gains a global discovery layer below the project file; existing project-only workflows are unchanged (project still overrides global).
- **Secrets out of the repo.** Project-local `.env.omni` is deprecated; bearer secrets live only in the keychain / `~/.omnigraph/credentials` / env.
- **Auth keyed by server name** (keychain / `[<server>]` profile / `OMNIGRAPH_BEARER_TOKEN_<SERVER>`), with explicit `auth:` sources for control. `OMNIGRAPH_BEARER_TOKEN` (the shipped name) is reused — **no new `OMNIGRAPH_TOKEN`**.

## Open questions

- **Keychain crate + name-derivation.** Keychain is the primary credential store, so it is on the critical path: macOS Keychain first, the `0600` profile file as fallback; Linux Secret Service / `pass` later. Open: which keyring crate, and the exact `OMNIGRAPH_BEARER_TOKEN_<SERVER>` derivation (upper-snake, non-alnum → `_`).
- **OAuth flow specifics (V6, not v1).** Device-authorization vs auth-code+PKCE as the default `login` flow; token-cache location and refresh-failure UX. The enum reserves the shape; implementation is deferred.
- **OIDC ownership / timeline (cloud).** `serve.auth.oauth`'s shape is reserved here; its verifier + field schema are MR-956 RFC 0001's. If Federated Auth lands before V6, server-side OIDC validation ships on its timeline, not this RFC's — the two must converge on one schema (the reserved `issuers:`-list + `tenant_claim`), never a second OIDC config.
- **`storage:` block scope.** How much object-store config to honor per graph (region/endpoint/profile) vs. delegating entirely to the ambient chain. Start minimal.
- **Single-file vs `KUBECONFIG`-style list.** `OMNIGRAPH_CONFIG` single path first; colon-joined list later if demand appears.
- **`config.yaml` vs `omnigraph.yaml` deep convergence.** Out of scope: one registry with embedded + remote invocation surfaces is the long-term end state for `queries:`/`aliases:`.

## Implementation — breadboard + slices

**Bold** = NEW. The new layered-config + resolver + auth code lands in a **new `omnigraph-config` crate** depended on by `omnigraph-cli` and `omnigraph-server`, so neither the CLI nor YAML parsing pulls in the HTTP server stack. **Caveat (validated):** config extraction alone does *not* shed the dependency — the CLI also imports ~20 `omnigraph_server::api::*` wire DTOs (`main.rs:20-27`). Fully realizing "CLI doesn't pull Axum" needs a companion **`omnigraph-api-types`** crate (the DTOs); otherwise the CLI keeps the server dep for DTOs. `QueryRegistry` stays in `omnigraph-server` (it is `omnigraph-compiler`-coupled, `queries.rs:18-22`) — only the serde types move; `PolicyEngine` is already standalone in `omnigraph-policy`.

### Places

| # | Place | What |
|---|---|---|
| P1 | Disk | `~/.omnigraph/{config.yaml, credentials, cache/, state/}` + project `omnigraph.yaml` |
| P2 | Config resolution | every command: load layers → merge → resolve `--graph` → resolve auth |
| P3 | Command execution | embedded engine OR remote HTTP client |
| P4 | Remote `omnigraph-server` | existing HTTP surface (+ route unification, §9) |
| P5 | Scaffold | `login` / `init` / `quickstart` |

### Affordances

| # | Place | Affordance | NEW? | Wires |
|---|---|---|---|---|
| U1 | P1 | `~/.omnigraph/config.yaml` (operator edits) | **N** | → N1 |
| U2 | P1 | project `./omnigraph.yaml` | — | → N1 |
| U4 | P3 | `omnigraph <verb> --graph <name>` (any command) | — | → N14 |
| U5 | P5 | `omnigraph login [<server>]` | **N** | → N11 |
| U6 | P5 | `omnigraph init` / `quickstart [--template]` | partly | → N12/N13 |
| U7 | P2 | `omnigraph use` / `config view --resolved --show-origin` | **N** | → N10 |
| N0 | P2 | **`omnigraph-config` crate** — shared schema, loader, resolver, auth | **N** | hosts N1–N9 |
| N1 | P2 | `load_layered_config()` — global (N3) + state (N3b) + project (cwd), `deny_unknown_fields` | **N** | → N2 |
| N2 | P2 | **merge engine** — deep-merge settings; replace named-resource entries/lists; retain per-field origin | **N⚠️** | → N5, N10 |
| N3 | P2 | global-dir resolver — `OMNIGRAPH_CONFIG` / `OMNIGRAPH_HOME` else `~/.omnigraph/` | **N** | → N1 |
| N3b | P2 | active-context state — `~/.omnigraph/state/active.yaml` | **N** | → N1 |
| N5 | P2 | `resolve_graph(name, merged)` — three-tier (§1) → typed `GraphLocator`; rejects invalid role/field combos | **N⚠️** | → N6 |
| N6 | P3 | `GraphConn` — `Embedded(engine)` \| `Remote(http)` dispatch | **N⚠️** | → N7, N8 |
| N7 | P3 | embedded path — `Omnigraph::open(storage)` (existing) | — | → engine |
| N8 | P3 | HTTP-client path — **rewire existing reqwest calls to `/graphs/{id}/…`; migrate off `/read`,`/change`** | **extend** | → P4, N9 |
| N9 | P2 | `resolve_auth(server)` — method×source (§6): explicit `auth:` else implicit chain keyed by name (reuses `OMNIGRAPH_BEARER_TOKEN`); **enforces the §7 credential trust model (trusted-origin + endpoint-binding) before releasing a token** | **N⚠️** | → N8 |
| N10 | P2 | `config view` handler — merged + per-field origin (needs N2) | **N** | → U7 |
| N11 | P5 | `login` handler — interactive auth (bearer; OAuth device flow in V6) → keychain / `credentials` (0600) + `.gitignore` | **N⚠️** | → S_global |
| N12 | P5 | `init` handler — `scaffold_config_if_missing`; refuse-if-exists / `--force` | partly | → S_project |
| N13 | P5 | `quickstart` handler — scaffold + `--template` + seed + serve + agent prompt | **N⚠️** | → S_project |
| N14 | P3 | agent-mode wrapper — `OMNIGRAPH_AGENT_MODE`: JSON, structured errors, never-prompt, typed exit codes | **N⚠️** | → N1 |
| N15 | P4 | **server route unification** — `serve.graphs` selects served set; canonical `graph_id` per graph; always mount `/graphs/{id}/…`; flat = compat alias only when one graph served; `GET /graphs` lists served set | **N⚠️** | → P4 |

### Slices (vertical, each demo-able)

| # | Slice | Demo |
|---|---|---|
| **V0** | **Foundations (no behavior change)** | extract `omnigraph-config` (+ `omnigraph-api-types`); add `version:` + `deny_unknown_fields`; build the layered-config fixture harness + keychain `SecretStore` seam; relocate the 11 `config.rs` tests. `cargo test --workspace` green, no functional change. |
| **V1** | **Global layer + merge + `config view`** | Config in `~/.omnigraph/`; `config view --resolved --show-origin` from any dir → merged result with per-field origin; embedded commands work global-first with no project file |
| **V2** | **Typed locator + route unification + remote client** | Define a `server:` graph (or `server/graph_id`); `query --graph prod` hits the server `curl`-free against `/graphs/{id}/…`; embedded `--graph dev` still local. *Gated on N15.* |
| **V3** | **Auth model + `login` + credential trust model** | `omnigraph login prod` (bearer) → keychain; per-server resolution with the §7 trust model (trusted-origin + endpoint-binding) + the §4 identity rule (the security model); V2 works with no manual env |
| **V4** | **Thin-init hardening + quickstart + templates** | `quickstart --template person-knows` scaffolds + seeds + serves; `init --force` purges |
| **V5** | **Agent-mode** | `OMNIGRAPH_AGENT_MODE=1 omnigraph query …` → JSON + structured errors + typed exit codes; never-prompt |
| **V6** | **OAuth / mTLS (reserved methods)** | implement the reserved `oauth` (device flow, token cache, refresh, OIDC server-side validation) and `mtls`; the enum shape ships in V3, so this is additive |

### Phase detail (sizing, gates, exit)

Sizes from the 2026-06-02 code audit (six parallel validators). **V0** is a prerequisite the original slices folded into "land first."

**V0 — Foundations** *(M–L; gates everything; no behavior change)*
- Extract `omnigraph-config` (schema + `load_config` + resolvers — clean, only std/serde/clap deps). Keep `QueryRegistry` in `omnigraph-server` (compiler-coupled); move only serde types; import `PolicyEngine` from `omnigraph-policy` directly. Decide/extract `omnigraph-api-types` (the `api::*` DTOs) to actually shed the CLI's server dep.
- `version:` + `deny_unknown_fields`, version-gated (no-version = legacy-lenient with compat aliases; `version: 1` = strict).
- Build the two missing test seams — a layered-config fixture harness (`TempHome` + `OMNIGRAPH_HOME`/XDG env isolation) and a keychain `SecretStore` trait + in-memory fake; relocate the 11 `config.rs` tests (`config.rs:567-948`). Record both in `testing.md`.
- Exit: `cargo test --workspace --locked` green; no functional change.

**V1 — Layered config + typed locator** *(L; the long pole)*
- N3 global-dir resolver; N1 layered load; **N2 merge engine + per-field provenance** (replaces the single `base_dir` — the hardest net-new piece; it gates both `config view` *and* the §7 trusted-origin rule); N3b active-context state + `omnigraph use`.
- Typed `GraphLocator` + `resolve_graph` (§1); rewrite the ~17 dispatch sites; delete `is_remote_uri` (`main.rs:686`).
- Schema reshape: `cli:`→`defaults:`, `server:`→`serve:`, `uri:`→`storage:` (string-or-block; region/endpoint, **profile scoped out**), remove top-level `policy:`/`queries:` (delete the coherence machinery `config.rs:356-421`), drop `project.name`. Fix `resolve_policy_tooling_graph_selection`.
- `--graph` canonical + `--target` alias (extract a shared `GraphArgs` first — the flag is duplicated 23×); `config view --resolved --show-origin`; migrate `scaffold_config_if_missing` (`main.rs:1547`) to `version: 1`.
- Exit: CLI works global-first with no project file; embedded behavior unchanged.

**V2 — Route unification + remote client** *(L; closes the substantive gap; gated on V0 server-side, V1 client-side)*
- Server: add `serve.graphs`; **unwind the `Single`/`Multi` bifurcation** (`GraphRouting`/`ServerConfigMode` + ~4 branch sites) into one registry; always `.nest("/graphs/{graph_id}",…)` (`lib.rs:1170-1175`); flat = compat alias when one graph served; `GET /graphs` served set (403-by-default without `serve.policy`); resolve the wire-vs-Cedar `graph_id` decision (§9).
- Client (N8): `remote_url` takes `graph_id` → `/graphs/{id}/…`; `/read`→`/query`, `/change`→`/mutate` (drop `legacy_change_request_body`); locator guards for `load`/`lint`/`schema plan`/`optimize`/`cleanup`.
- Engine: thread `storage.region`/`endpoint` → `Omnigraph::open` → `namespace.rs:228,376` + `S3StorageAdapter` (`storage.rs:284`).
- OpenAPI/SDK: regen `openapi.json` (`OMNIGRAPH_UPDATE_OPENAPI=1`), rewrite the exact allow-lists (`openapi.rs:162,1120`), re-vendor `omnigraph-ts` (its `transport.ts` is already prefixed — runtime aligns, op-id names churn).
- Tests: **make `system_remote.rs` hermetic** (it is entirely `#[ignore]`'d today — the central gap-closer has zero enforced coverage); route-mode matrix; legacy `/graphs/{gid}` URI-split migration.

**V3 — Credential trust model + login** *(L; the security phase; needs V1 provenance)*
- `servers:` + `Auth` union (`bearer`/`none` impl; `oauth`/`mtls` reserved-error) × `SecretSource`; `resolve_auth` keyed by server name (`rust-ini` reusable from the lock tree); **trusted-origin rule** (unblocked by V1 provenance) + **endpoint-binding**; reject project-layer `servers.auth`/`command`. `omnigraph login` (bearer → keychain via `keyring` 4.0.1, feature-gated, headless graceful-degrade — **check MSRV 1.88** against the toolchain); `serve.auth.bearer.enabled`; `OMNIGRAPH_SERVER` env floor.

**V4 — Init/quickstart** *(S–M)* — `quickstart --template`, `init --force`. **V5 — Agent-mode** *(S–M)* — `OMNIGRAPH_AGENT_MODE`. **V6 — OAuth/mTLS** *(L; deferred)* — client `oauth2`/`openidconnect` + device flow + token cache/refresh; server OIDC/JWKS via `jsonwebtoken` (already in the lock tree); `AuthSource::Oidc` is already reserved (`identity.rs:163`).

### Critical path & parallelization

```
V0 (crate + api-types + version gate + test seams)
        ├──────────────► V2-server (serve.graphs + route unwind)   ← needs only serve.graphs; develop alongside V1
        │                         │
V1 (N2 provenance + typed locator + schema reshape + config view + --graph)
        │                         │
        ├────► V2-client (remote rewire) ── gated on V2-server ────┘
        ├────► V3 (auth union + trusted-origin[needs N2] + login + keychain)
        └────► V4, V5 (ride V1)          V6 (rides V3; large, independent)
```

Long poles: **N2 merge+provenance**, the **typed-locator rewrite**, the **server Single/Multi unwind**. Startable early in parallel: V2-server (server-only), the `storage:` engine threading, and the mechanical `--graph` rename.

### Validation findings (2026-06-02 code audit)

Six parallel validators confirmed the RFC's code claims and surfaced these plan-shaping facts (folded into the phases above):
1. Config extraction alone does not shed Axum from the CLI — it also imports `api::*` DTOs → V0 adds `omnigraph-api-types`.
2. N2 merge+provenance gates both `config view` and the trusted-origin rule → it is the V1 linchpin; the auth trust model cannot precede config layering.
3. Route unification is not green-field — it unwinds the deliberate `Single`/`Multi` split and forces an `openapi.json` regen + `omnigraph-ts` re-vendor (SDK runtime already prefixed; op-ids churn).
4. `storage.profile` is env-only in Lance and omnigraph → scoped out of v1; `region`/`endpoint` are feasible now (Lance accepts per-dataset `storage_options`).
5. `system_remote.rs` is entirely `#[ignore]`'d → V2 must make it hermetic or rewrites land green-then-break.
6. Two test seams (layered-config fixtures, keychain) are missing and on the critical path → built in V0.

## Rollout

**V0 → V1 → V2 → V3 → V4 → V5 → V6.** V0–V1 are the foundation; V2 closes the substantive client→server gap (gated on server route unification, N15); **V3 lands the auth model and the credential-redirection security fix (a gate, not optional polish)**; V4–V5 are ergonomics; V6 implements the reserved auth methods. (`OMNIGRAPH_BIND` is a small additive server task — the binary honors `--bind`/`server.bind` only, `lib.rs:899` — not a prerequisite.) Evaluate after V2 against early-adopter and agent-onboarding signal.

## Prior art

- kubeconfig (clusters / users / contexts; `KUBECONFIG`; `kubectl config view`; `current-context`)
- Helix CLI v2 (`helix.toml` local+enterprise blocks; `~/.helix/config`; `~/.helix/credentials`)
- AWS CLI (`~/.aws/config` + `~/.aws/credentials` split; named profiles; `credential_process`)
- gh / kubelogin (OAuth device flow; keychain token storage)
- git (`~/.gitconfig` + `.git/config`; `--show-origin`)
- Cargo (`Cargo.toml` manifest + `~/.cargo/config.toml` + `~/.cargo/credentials.toml`)
- Supabase / Prisma (one project manifest; connection via `DATABASE_URL` env)
- 12-factor app (config that varies by deploy lives in the environment)
