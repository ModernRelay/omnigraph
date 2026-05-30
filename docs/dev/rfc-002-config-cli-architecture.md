# RFC: Config & CLI Architecture — Layered Config, Client Targeting, File Naming

**Status:** Proposed
**Date:** 2026-05-30
**Tickets:** MR-668 (multi-graph server, shipped — the dependency this builds on), MR-969 (stored queries + MCP — supplies the in-repo agent tool surface), MR-973 (quickstart / onboarding), MR-974 (agent setup surface), MR-981 (agent-friendly CLI hardening)
**Target release:** v0.8.x (tentative; phased — see Rollout)

## Summary

OmniGraph today has a single config file, `omnigraph.yaml`, read both by the CLI (operating the embedded engine) and by `omnigraph-server` (hosting graphs). There is **no client-side configuration that targets a *running server*** — to talk to a deployed `omnigraph-server` you drop to `curl` or the `omnigraph-ts` client. This is the one real gap in an otherwise coherent design (storage-URI addressing, multi-graph routing, per-graph policy).

This RFC defines the config and CLI architecture that closes that gap, derived from first principles — *working backwards from what OmniGraph uniquely enables* rather than copying kubeconfig / `helix.toml`. The result:

1. A **global-first layered config** — user-global (`~/.config/omnigraph/`) is the **primary, self-sufficient default**; per-project (`./omnigraph.yaml`) is an *optional* override + deployment manifest. One uniform schema, both layers optional; the CLI works from any directory with **no project file** (the `kubectl`/`aws`/`gh` posture), unlike today's project-anchored behavior.
2. A single unifying noun — the **target** — that resolves a name to a concrete `(locus, graph, sub-state, credential)` tuple, where the locus is **embedded (storage URI) XOR remote (server endpoint)**.
3. A **multi-server × multi-graph** client model (OmniGraph hosts N graphs per server and there are M servers — unlike Helix's one-cluster-one-graph).
4. **Credentials by reference** (env var / keychain), never inlined, with a separate `credentials` file (AWS pattern) so every committed/GitOps'd surface is secret-free.
5. A **file-naming** decision: project and server config are **the same artifact, same name** (`omnigraph.yaml`); the only differently-named file is the user-global `config.yaml`, justified by **scope, not role**.

The design optimizes jointly for **DX** (one command surface across embedded and remote; clone-and-go) and **AX** (agent experience: one flat resolved context, secrets structurally unreachable, branch-pinned reproducible reads, and a GitOps'd capability surface).

## Motivation

Three problems, in priority order:

- **No client→server targeting config.** The moment an operator stands up `omnigraph-server` — for bearer auth + Cedar at a network boundary + admission control + multi-graph routing — the CLI can't address it. `curl` is the fallback. There is no named, switchable, credential-carrying way to say "run this against `prod` on the team server."
- **Multi-server × multi-graph has no first-class expression.** OmniGraph genuinely runs N graphs per server across M servers. The same graph is **multi-homed** — `s3://b/prod` may be `prod` on server A, `production` on server B, and opened directly by the CLI. Today's flat `graphs:` map (name→storage-URI) can't express "graph `production` on server `prod-eu`."
- **Solo-first and embedded-first are unserved by the remote story.** A solo developer with no projects should define everything in `~`. A developer iterating locally (embedded, no server) and then pointing at staging (remote) should change *one word*, not learn a second command surface.

MR-668 shipped the server side (multiple graphs per server). MR-969 ships the in-repo agent tool surface (stored queries / MCP). This RFC supplies the **client and config layer** that lets humans and agents target that surface coherently — the foundation under MR-973 / MR-974 / MR-981.

## Non-Goals

- **A control plane / dashboard for config.** Operators edit files and (for servers) restart. No runtime config-mutation API. Matches the MR-668 / MR-969 operational model.
- **Hot reload.** Restart-only for server-side config, matching MR-668 and MR-969.
- **Embedding secrets in any config file.** Credentials are by-reference; a separate `credentials` file or the OS keychain holds tokens. Never a `*.yaml` that could be committed.
- **Renaming the project manifest by role.** No `omnigraph.server.yaml` / `omnigraph.client.yaml`. Role lives in sections, not filenames (see Design §3).
- **Dropping embedded mode.** Embedded-first is load-bearing for the file-naming decision; this RFC assumes it stays.
- **Cross-graph / cross-server tool listing in MCP.** Clients loop over per-graph catalogs (a MR-969 non-goal, restated).

## Background

OmniGraph runs on Lance 6.x: typed nodes/edges in per-type Lance datasets, atomic multi-table commits via a `__manifest` table, branchable and time-travelable. The CLI (`omnigraph`) operates the **embedded engine** directly against a storage URI — no HTTP client in its runtime dependencies. `omnigraph-server` (Axum) is a *separate* HTTP front-end over the same engine, with bearer auth + per-graph Cedar (MR-668). The two read the same `omnigraph.yaml` but never connect to each other.

The six **irreducible enablers** that drive the design (referenced as E1–E6 below):

| # | Enabler | Consequence |
|---|---|---|
| E1 | A graph is a **self-contained storage URI**; the substrate (object store + manifest CAS) is the source of truth — no server required to read/write. | A graph is addressable **directly (embedded)**, not only via a server. |
| E2 | A server hosts **many graphs**; **many servers** exist. | The remote address space is **`{server} × {graph_id}`**. |
| E3 | The same graph is **multi-homed** under different per-locus names. | **Name ≠ identity.** Resolution is mandatory. |
| E4 | **Branch / commit / snapshot** are first-class addressable sub-state. | An address is *graph @ branch/snapshot*, not just graph. |
| E5 | Enforcement is **two-layered**: engine-layer Cedar (`_as` writers, works embedded) + HTTP-boundary bearer+Cedar (server only). | *How* you reach a graph determines *which* enforcement applies. |
| E6 | **Stored queries / MCP tools are a per-graph registry defined in the project config** (MR-969). | The **agent tool surface is version-controlled in the repo**. |

Competitors collapse dimensions OmniGraph keeps live: **Helix** fuses E2+E3 (one cluster = one graph); **namidb** fuses E1+E3 into the URI (`s3://b?ns=prod`) and serves one namespace per process. OmniGraph has all of E1–E6 at once, so its config resolves a richer space — but the richness is *earned* by capability.

## Design

### 1. The address space and the `target` abstraction

Every OmniGraph address is a tuple:

```
(locus, graph, sub-state, credential)
  locus      = embedded(URI)  XOR  remote(server-endpoint)        # E1, E2
  graph      = a URI (embedded)  |  a graph_id on a server (remote) # E3
  sub-state  = branch | snapshot                                   # E4
  credential = cloud-storage creds (embedded) | bearer token (remote) # E5
```

The config's only job is **name → this tuple**. Define one noun — a **target** — that resolves to either shape:

```yaml
targets:
  dev:                       # embedded — substrate-direct (E1)
    uri: s3://team-bucket/dev.omni
    branch: main             # sub-state (E4)
  staging:                   # remote — resolves a server by reference (E2/E3)
    server: staging          # → looked up in `servers`
    graph: prod              # graph_id on that server
    branch: review
```

`--target staging` resolves: project `targets.staging` → `{server: staging, graph: prod, branch: review}` → `servers.staging` → `{endpoint, token-by-ref}` → final `(remote(https://…), prod, review, $TOKEN)`. Embedded targets skip the server hop and use cloud-storage credentials.

**Two concepts, not kubeconfig's three.** kube splits cluster / user / context; that 3-way split is its most-cursed UX. A target *bundles* server+graph+branch+defaults under one name; the **only** thing split out is `servers`, because endpoints+credentials are shared across many targets and are secret-bearing (different ownership and rate-of-change; see §2). Result: **2 nouns — `servers` and `targets`.** Embedded `targets` (`uri:`) subsume today's `graphs:` entries.

### 2. Layered config — global-first, uniform schema, project-optional

**Posture: global-first, project-optional.** OmniGraph's CLI is primarily a *client* (it operates against graphs and servers, embedded or remote), so it sits on the **global-first** side of the CLI-config axis — like `kubectl` / `aws` / `gh` / `docker`, and unlike *project-first* tools (`git` / `cargo` / `terraform`) whose primary config is per-repo. The **global user config is the primary, self-sufficient default**; the project file is an *optional* repo-scoped override (and, when present, the deployment manifest). `omnigraph query --target prod` must work from **any directory with no project file**, exactly as `kubectl get pods --context prod` works from anywhere. *(This is a deliberate flip from today, where the CLI reads `./omnigraph.yaml` and does not even walk parent dirs — i.e. today it is project-anchored.)*

**Rule: the two layers share ONE schema, and each is fully self-sufficient** (the git-layering mechanism — same schema at both levels; you never need a repo to have a working config). Do **not** specialize the layers. Anything — `servers`, `targets`, `defaults`, `queries`, `aliases`, `policy` — is expressible at either layer.

This makes the **zero-project case the default, not an edge case**: a solo user (or an agent) defines everything in `~/.config/omnigraph/config.yaml` — servers, embedded + remote targets, defaults, even a personal server's `graphs:`/`queries:` — and **never creates a project file**. A team adds `./omnigraph.yaml` only when it wants repo-scoped overrides or a committed, GitOps'd deployment manifest. Global-first does **not** forbid project files; it stops *requiring* them (the kubectl model: `~/.kube/config` is sufficient and default; per-project kubeconfigs are opt-in via `KUBECONFIG`).

| Layer | Required? | Typical use | Path |
|---|---|---|---|
| Global | no | **the default** — solo/agent's entire config; shared servers+creds for teams; even a personal server's graphs/queries | `~/.config/omnigraph/config.yaml` |
| Project | no | **opt-in** — repo-scoped overrides + the committed deployment manifest (graphs, queries, policy) | `./omnigraph.yaml` |

**Precedence (low → high):** built-in defaults < global < project < env vars < CLI flags. With no project file it collapses to **built-in < global < env < flags** — the common global-only path.

**Merge semantics (predictable, not magical):**
- **Maps** (`servers`, `targets`, `queries`, `aliases`) → **key union**; on a key collision the higher layer's entry **replaces** the lower wholesale (no field-level deep-merge within an entry — that is where surprise lives).
- **Scalars** (`defaults.target`, `output_format`) → higher layer wins.
- **Relative paths carry their origin's base_dir.** A `queries:` entry's `.gq` path, or a `policy.file`, resolves against the directory of the layer it was *defined in* — global entries under `~/.config/omnigraph/`, project entries under the project dir.

### 3. Roles, and the file-naming decision (same name for project = server)

`omnigraph.yaml` carries two *roles* that diverge in prod and collapse on a laptop:

- **Server role** (read by `omnigraph-server`): `graphs:` (graph_id → URI), per-graph `policy.file`, **`queries:` — the stored-query/MCP registry lives here**, plus serving knobs.
- **Client role** (read by the CLI/agent): `servers:`, `targets:`, `defaults:`, `aliases:`.

**Project config and server config are the same artifact, hence the same name.** The server *serves the project*: the file that says "these graphs exist, with these stored queries and this policy" is simultaneously the project manifest and the server's deploy config. Role is distinguished by which *sections* are populated, never by filename. Readers ignore sections that are not theirs (today's file already does this with `cli:` vs `server:`).

**Why not kube's role-split.** Two coherent models exist: (A) one project file with role-sections (Helix `helix.toml` holds both `[local.dev]` and `[enterprise.production]`; compose; Cargo), and (B) deployment-manifest strictly separate from client config (kubectl — you never put a context in `deployment.yaml`). kube is the sharpest topological analog (multi-server × multi-graph, one client targeting many), so B has a real claim. The tiebreaker is **E1: OmniGraph is embedded-first.** In embedded mode the manifest's `graphs:` *is* the local target list — manifest and local-client-view are the same object, so splitting them (B) fights the grain and forces two files for local work. kube splits because it has **no** embedded mode (client always remote+global). So: take the half kube is right about — *remote* client targeting (`servers:`, endpoints, creds) is a separate concern in a separate **user-global** file (`config.yaml`, like `~/.kube/config`); reject the half it is wrong about for us — do **not** split the *project* layer by role. **The second name (`config.yaml`) is justified by scope (user-global), not role.** *(If OmniGraph ever dropped embedded mode and went pure-remote, model B's strict split would become cleanest.)*

### 4. File naming

Principles from the field: **XDG for global** (gh) over legacy `~/.app/` (aws/kube/helix); **separate the secret file** (aws `config` vs `credentials`); **project-root manifest keeps the app-named file** (`Cargo.toml`, `package.json`); **`.yaml`, not `.yml`**; keep OmniGraph's established names. Only the *global* layer and *credentials* are new decisions.

| Artifact | Path / name | Why |
|---|---|---|
| Project = server config (one artifact) | `./omnigraph.yaml` | **Keep.** Root manifest like `Cargo.toml` / `compose.yaml` / `helix.toml`. Same name for both roles because it is one file. In prod the server's deploy repo and an app repo each have their own `omnigraph.yaml` — same name, different repos. |
| Global user config | `~/.config/omnigraph/config.yaml` | **XDG** (`$XDG_CONFIG_HOME` honored). Named `config.yaml` *not* `omnigraph.yaml` — the name signals scope. Holds the full schema so a solo user needs nothing else. Mirrors `~/.config/gh/config.yml`, `~/.cargo/config.toml`. |
| Credentials | `~/.config/omnigraph/credentials.yaml` (mode `0600`) — or OS keychain (preferred) | **Separate file, AWS pattern.** Written by `omnigraph login`; secret-only; stricter perms. With by-reference creds this file is optional (token lives in env/keychain; nothing secret on disk). |
| Cedar policy | `./policies/<env>.yaml` + `<env>.tests.yaml` | **Keep.** Referenced by `policy.file`. |
| Schema | `./*.pg` (e.g. `schema.pg`) | **Keep.** |
| Stored queries | `./queries/*.gq` | **Keep.** `.gq` sources referenced by the `queries:` registry. |

**Env / override precedence (the `KUBECONFIG` analog):**
- `OMNIGRAPH_CONFIG=/path` — explicit config file, highest precedence.
- `$XDG_CONFIG_HOME` → global dir; fall back to `~/.config/omnigraph/`, then legacy `~/.omnigraph/` if present.
- Per-server token resolution: `token_env:` (env var) → `credentials.yaml` entry → OS keychain. Operator-chosen env var names use the `OMNIGRAPH_` / `OG_` prefix by convention.

### 5. Credentials, connection tiers, and bind portability (12-factor)

**Credentials are by-reference everywhere, never inlined at any layer.** The reason to *keep* them in `~` is that the project file is typically committed/shared — not a schema constraint. This single rule makes the design safe for git (project file shareable) and for agents (no inline secrets anywhere they can read).

**Three connection tiers** (Supabase/Prisma teach the zero-config floor):
1. **Env vars** — `OMNIGRAPH_SERVER=https://…` + `OMNIGRAPH_TOKEN=…`: zero-config remote, no file (the `DATABASE_URL` floor).
2. **Global `config.yaml`** — named `servers:` + `targets:` for multi-server setups (the AWS-profiles convenience).
3. **Project `omnigraph.yaml`** — project-pinned targets/graphs, committed.

**Keep `omnigraph.yaml` a *portable* manifest (12-factor).** Deploy-specific runtime that varies per environment — the **bind host/port**, worker counts — should be supplied by **`--bind` / `OMNIGRAPH_BIND` (flags/env)**, *not* a committed `server.bind:` baked into the manifest. A manifest that hardcodes `0.0.0.0:8080` is not portable across deploys and leaks an environment detail into a version-controlled file. The same-named `omnigraph.yaml` stays portable across deploys precisely because the volatile, per-environment knobs live in env/flags (12-factor config), while the stable, portable definition (graphs, queries, policy) lives in the file. This is the one concrete lesson taken from kube's model-B without adopting its file split: portability via env/flags, not via a second file.

### 6. Where stored queries live: defined locally, invoked remotely

A stored query splits across two axes; do not conflate them:
- **Definition** (`.gq` source + `queries:` entry) lives in the **deployment manifest** (server-role `omnigraph.yaml` + the `.gq` files), GitOps'd. Local to the *deployment/project*, never in a client's connection config.
- **Discovery** ("what tools exist for me?") is fetched from the **server** (Cedar-filtered `GET /queries` / MCP catalog) at connect time.
- **Invocation** is **remote** (client → server, HTTP/MCP) — or **embedded** (the CLI opens the graph directly and reads the same manifest).

So the client carries *pointers to servers*, not query definitions; it **discovers and invokes**, never defines. This is the **capability-as-code guarantee for agents**: an agent can only invoke tools the server's *committed, reviewed* config exposes — it **cannot define a new tool at runtime**. Definition is structurally outside the agent's reach.

`queries:` (manifest, server/remote, Cedar-gated, MCP) and `aliases:` (manifest, embedded CLI only) overlap — both are "named `.gq` queries in the manifest." This RFC keeps them siblings (the MR-969 decision); the clean long-term is **one registry, two invocation surfaces** (embedded + remote), with `aliases:` subsumed. Out of scope here.

#### Reconciling `aliases:` with the role model

`aliases:` is the pre-MR-969, **client-role, embedded-only, ungated** ancestor of `queries:`. An alias bundles `command` (read/change), `query` (`.gq` path), `name` (symbol), `args` (positional param names), and `graph`/`branch`/`format` defaults; the CLI runs it embedded. The server never reads it. So:

- **Role:** `aliases:` is **client-role** (CLI behavior) → it may live in **both** the user-global `config.yaml` and the project manifest, layered. `queries:` is **server-role** → it lives **only** in the deployment manifest (the server reads the manifest, not `~`). *Who reads it determines where it can live.*
- **Difference:** `aliases:` = embedded invocation, no gating, explicit `command`, bundles client defaults + positional args. `queries:` = remote (+future embedded), Cedar + `mcp.expose`, **infers** read/mutate, bundles only MCP settings.
- **Convergence:** decompose an alias — *definition* (name→.gq+symbol) → `queries:` (the superset: typed, validated, gated, multi-surface, no redundant `command`); *target/branch/format* → client invocation context (`--target`/`--branch`/`--format` or `defaults:`), not baked per-query; *positional `args`* → thin CLI sugar or dropped (agents/services use named JSON params). End-state: one `queries:` registry + the client config model subsumes `aliases:`.
- **v1:** keep `aliases:` unchanged. Footgun worth a load-time warn: an alias and a query with the same name in one manifest are different namespaces invoked differently (`--alias X` vs `POST /queries/X`).

### 7. CLI surface

- `omnigraph login <server>` — interactive auth; writes `credentials.yaml` (0600) or the keychain. The `gh auth login` analog.
- `omnigraph use <target>` — set the active target (writes the appropriate layer). The `kubectl config use-context` analog.
- `omnigraph config view [--resolved] [<target>]` — print the merged config and, with `--resolved`, the final tuple **plus the origin layer of every field** (the `git config --show-origin` / `kubectl config view` analog). Resolution is never a mystery.
- All existing verbs (`query`, `mutate`, `load`, `schema`, `branch`, …) gain `--target <name>`; resolution decides embedded vs remote transparently.

### 8. Concrete shape

**Global** `~/.config/omnigraph/config.yaml` (per-user, secret-free):
```yaml
servers:
  prod-us:  { endpoint: https://og-us.internal:8080,  token_env: OG_PROD_US_TOKEN }
  prod-eu:  { endpoint: https://og-eu.internal:8080,  token_env: OG_PROD_EU_TOKEN }
  staging:  { endpoint: https://og-staging.internal:8080, token_env: OG_STAGING_TOKEN }
defaults:
  target: dev
```

**Project** `./omnigraph.yaml` (committed, secret-free, portable — no `server.bind`):
```yaml
targets:
  dev:      { uri: s3://team-bucket/dev.omni, branch: main }   # embedded
  staging:  { server: staging, graph: prod, branch: review }   # remote
  prod-us:  { server: prod-us, graph: production }
  prod-eu:  { server: prod-eu, graph: production }             # multi-server, same graph name
defaults: { target: dev, output_format: table }
queries:  { find_user: { file: ./queries/find_user.gq, mcp: { expose: true } } }
aliases:  { ... }
```

**Credentials** `~/.config/omnigraph/credentials.yaml` (0600) — optional; tokens or keychain refs.

## DX

1. **One command surface, two loci.** `query --target dev` (embedded) and `--target staging` (remote) are the same command; only resolution differs. Change one word, not a mental model.
2. **Clone-and-go.** Project config names servers+graphs; teammate runs `omnigraph login staging` once and every target resolves. The git + `gh auth login` model.
3. **Multi-server × multi-graph is the default.** Targets reference `server` by name; `servers` is a global named map; graphs are per-server. `prod-us` and `prod-eu` both serving `production` is two target entries — Helix cannot express this.
4. **Solo-first.** Everything in `~`, no project required.
5. **Laptop-to-fleet on one schema.** Local = one `omnigraph.yaml` (both roles); prod = role-split across repos. No second format to learn.

## AX (agent experience)

1. **One flat resolved context, never a config to navigate.** target→server→endpoint→token resolves *before* the agent sees anything. The agent reasons about tools, not topology (the LLM-safe-surface principle extended to config).
2. **Secrets are structurally outside the agent's reach.** The repo it operates in has no tokens; they are in the global layer / keychain, outside its view. An agent *cannot* exfiltrate a prod token from project config because it is not there.
3. **Branch/snapshot-pinned contexts** (E4) — hand an agent a `branch: review` / `--snapshot v42` target and its reads are reproducible and cannot see uncommitted main-line state. No kubeconfig analog.
4. **The agent's capabilities are a GitOps'd artifact** (E6) — which graphs exist, which stored-query tools it may call, and which Cedar rules gate them are all in the version-controlled server config. Powers change only via a reviewed PR, deployed by restart. Infrastructure-as-code for what the AI can do.
5. **Config + policy compose.** Config = "where am I pointed + which token"; Cedar = "what may I do there." Orthogonal; no enforcement logic leaks into config.

## GitOps — three surfaces, secrets in none

| Surface | Repo | Contents | Deploy | Secrets |
|---|---|---|---|---|
| Server deployment config | infra/deploy repo | `graphs:`, policy, **`queries:` + `.gq` files** | commit → CI → **server restart** (no hot reload) | none — by-reference |
| Project client config | app repo | `targets:` → servers/graphs | committed, read by CLI/agent | none |
| Global user config | **not GitOps'd** — machine-local `~` | `servers:` + creds-by-ref | `omnigraph login` writes it | refs only (like `~/.kube/config`) |

## Comparison

| Property | kubeconfig | Helix | git | compose | **OmniGraph (this RFC)** |
|---|---|---|---|---|---|
| Named remote endpoints + creds-by-ref | ✅ | ✅ | partial | partial | ✅ (global `servers`) |
| Global + project layering, uniform schema | ✗ | ✗ | ✅ | ✗ | ✅ |
| Embedded OR remote under one name | ✗ | ✗ | n/a | ✗ | ✅ (E1) |
| Multi-server × multi-graph | ✅ | ✗ | n/a | n/a | ✅ (E2) |
| Branch/snapshot in the address | ✗ | ✗ | partial | ✗ | ✅ (E4) |
| Agent tool surface in the repo | ✗ | ✗ (separate bundle) | n/a | n/a | ✅ (E6) |
| Project manifest renamed by role | — | no | — | no | **no** |
| Concept count | 3 | 1 | 2 | 1 | **2 (servers/targets)** |

## Migration / backwards compatibility

- **Additive.** Today's `omnigraph.yaml` (`graphs:`, `cli:`, `server:`, `aliases:`, `policy:`) keeps working unchanged. `graphs:` entries are equivalent to embedded `targets:` with a `uri:`; both resolve.
- **`targets:` is new** and optional. `servers:` is new and optional. Absent → today's behavior.
- **Global `~/.config/omnigraph/config.yaml` is new.** Absent → only project + env + flags, exactly as now. Its addition is the **global-first posture flip**: today the CLI is project-anchored (reads `./omnigraph.yaml`, no parent walk); the global config becomes the new primary discovery path so the CLI works with no project file. Existing project-only workflows are unchanged (project still overrides global); the flip is additive — it adds a fallback layer below the project file, it does not remove the project file.
- **`graphs:` → `targets:` is an evolution, not a break.** Both can coexist; `targets:` is the superset (adds remote + branch pinning). A future cleanup may alias `graphs:` to embedded `targets:`.
- **`server.bind` stays supported** but documentation steers operators to `--bind` / `OMNIGRAPH_BIND` for portability; no removal.

## Open questions

- **`graphs:` vs `targets:` naming churn.** Do we rename `graphs:` → `targets:` (with a deprecation alias) or keep `graphs:` for embedded and add `targets:` for remote? Leaning: keep both, document `targets:` as the superset.
- **Keychain integration scope.** macOS Keychain first (matches operator practice), with a `0600` file fallback; Linux Secret Service later?
- **Project-local `servers:`.** Allowed (e.g. a localhost dev server), merged with global. Confirm creds stay by-reference even for project-local servers (yes).
- **`aliases:` ⇄ `queries:` convergence.** Out of scope here; tracked separately. One registry with embedded + remote invocation surfaces is the target end state.
- **Single-file `KUBECONFIG`-style list.** Do we support `OMNIGRAPH_CONFIG` pointing at multiple files (colon-joined), or a single file only? Start single; revisit if demand appears.

## Rollout

Phased, each independently useful:

1. **Env-var remote tier** — `OMNIGRAPH_SERVER` + `OMNIGRAPH_TOKEN`, plus an HTTP client mode in the CLI. Zero-config remote; unblocks `curl`-free server use. (Smallest; highest leverage.)
2. **Global `config.yaml` + `servers:`/`targets:` + resolution + `config view`.** Named multi-server targeting, layered with the project file.
3. **`omnigraph login` + `credentials.yaml` / keychain.** Credential management.
4. **`omnigraph use` + branch/snapshot-pinned targets.** Active-context switching; reproducible agent contexts.

Phases 1–2 close the substantive gap; 3–4 are ergonomics. Evaluate after phase 2 against early-adopter and agent-onboarding (MR-973 / MR-974) signal.

## Prior art

- kubeconfig (clusters / users / contexts; `KUBECONFIG`; `kubectl config view`)
- Helix CLI v2 (`helix.toml` local+enterprise instance blocks; `~/.helix/config`; `~/.helix/credentials`)
- AWS CLI (`~/.aws/config` + `~/.aws/credentials` split; named profiles; `credential_process`)
- git (`~/.gitconfig` + `.git/config`; `--show-origin`)
- Cargo (`Cargo.toml` manifest + `~/.cargo/config.toml`)
- Supabase / Prisma (one project manifest; connection via `DATABASE_URL` env)
- 12-factor app (config that varies by deploy lives in the environment)
