# RFC: Restructure the CLI Around Explicit Planes

**Status:** Proposed
**Date:** 2026-06-13
**Audience:** CLI/server/cluster maintainers
**Builds on:** [rfc-009-unify-access-paths.md](rfc-009-unify-access-paths.md)
(Phases 3a–3c landed — the embedded/remote data-plane fork is now one
`GraphClient` enum; this RFC **expands RFC-009 Phase 4** from a narrow
embedded-vs-remote capability table into the full plane model, and leaves
Phase 5 route alignment where it is),
[rfc-007-operator-config.md](rfc-007-operator-config.md) (operator
`--server`/`--graph`/`--target` addressing — the surfaces this RFC makes
uniform across planes),
[rfc-008-deprecate-omnigraph-yaml.md](rfc-008-deprecate-omnigraph-yaml.md).
**Sequencing:** post-v0.7.0, after RFC-009 Phase 3c (done).

## Summary

The CLI silently spans **three planes** — data, storage/maintenance, and
control — and forces the operator to know which plane each verb lives on *and*
address a graph differently per plane. The same graph you query as
`--server prod --graph knowledge` you must maintain as
`s3://bucket/knowledge.omni`. Plane restrictions (`graphs list` is server-only,
`optimize` is storage-only) are *accidental* — discovered by hitting a cryptic
error, not *declared*.

This RFC makes the plane model **explicit and coherent** with three moves:

1. **One graph-addressing model** across every verb (`--target`/`--graph`/
   positional URI/`--server`), resolving to a storage URI for maintenance and a
   remote client for data — instead of two different ways to name one graph.
2. **A declared, per-subcommand capability surface** (RFC-009 Phase 4): each
   verb declares its plane(s); wrong-plane invocations get an honest "this is
   storage-plane, `--server` doesn't apply" error from one table, not scattered
   `bail!`s.
3. **Plane-grouped `--help`** so the model is legible at a glance.

No new server feature. Storage maintenance stays off the wire — deliberately.

## Current state of affairs

The CLI has 23 top-level commands. They divide into three planes, addressed
three different ways:

| Plane | Verbs | Reaches the graph by | Addressing surface |
|---|---|---|---|
| **Data** | `query`, `mutate`, `load`, `ingest`, `branch *`, `snapshot`, `export`, `commit *`, `schema show/apply`, `graphs list` | embedded engine **or** HTTP server (one `GraphClient`, 15 call sites) | positional URI **or** `--target` / `--graph` / `--server` (config aliases) |
| **Storage / maintenance** | `init`, `optimize`, `repair`, `cleanup`, `schema plan`, `queries validate` | embedded engine **only**, directly on storage (`file://` or `s3://`) | positional URI **or** `--target` — **no `--server` / `--graph`** |
| **Control** | `cluster validate/plan/apply/approve/status/refresh/import/force-unlock` | a cluster **directory** (`file://` or `s3://`), not a graph URI | `--config <dir>` |

### What's confusing (validated facts)

1. **Two names for one graph.** Data verbs resolve `--server prod --graph
   knowledge` through `apply_server_flag` (16 call sites). Maintenance verbs use
   `resolve_uri`/`resolve_local_uri` and accept only a positional URI or
   `--target` — so to compact the graph you *query* as `--server prod --graph
   knowledge` you must *type* `s3://bucket/knowledge.omni`. One graph, two
   addressing vocabularies.

2. **Plane restrictions are accidental, not declared.** `graphs list` is
   server-only and `optimize`/`repair`/`cleanup`/`init` are storage-only purely
   by code shape. Point `optimize` at an `https://` URL and you get whatever
   `Omnigraph::open` says about an https URI — accidental error text that, per
   Hyrum's Law, is already someone's dependency. The capability is real but
   unstated.

3. **The split is per-subcommand, and the family names hide it.** `schema plan`
   is storage-only (`resolve_local_uri`) while `schema show`/`schema apply` are
   data-plane (the graph client). `queries validate` opens the graph to
   typecheck while `queries list` only reads the registry config. The plane is
   a property of the *subcommand*, not the family.

4. **Maintenance has no server/cluster counterpart at all.** There is no HTTP
   route and no `cluster` subcommand for `optimize`/`cleanup`/`repair` (verified:
   nothing in the server route table, nothing in `omnigraph-cluster/src`). For a
   server-backed deployment you run the *same CLI* against the storage URI,
   out-of-band from the serving process. This is correct (maintenance is
   heavyweight, destructive, single-operator — it should not be a multi-tenant
   HTTP surface), but it is **undocumented in the CLI's own shape**, so it reads
   as an omission rather than a decision.

5. **`init` has a hidden control-plane twin.** Bare `init` creates a single
   graph from storage; in cluster mode the equivalent is `cluster apply`
   (graph-creation stage, with ledger/recovery/approval semantics). Same intent,
   two entry points, no signpost between them.

6. **Flat `--help`.** All 23 commands list as one undifferentiated block, so the
   plane a verb belongs to is tribal knowledge.

The net effect: a new operator must already know OmniGraph's plane architecture
to predict which flags work on which verb and how to name a graph. The CLI does
not teach its own model.

## Target CLI ergonomics

The throughline: **you name a graph one way, and the CLI tells you what works
where.** Simple examples of the end state:

### One name for a graph, everywhere

A config target `knowledge` works on every verb that touches that graph:

```bash
omnigraph query    --target knowledge --query q.gq          # data (embedded or remote, auto)
omnigraph load     --target knowledge --data rows.jsonl     # data
omnigraph optimize --target knowledge                       # maintenance (resolves to its storage URI)
omnigraph cleanup  --target knowledge --keep 10 --confirm
omnigraph repair   --target knowledge --confirm
```

The positional URI form still works everywhere, unchanged:

```bash
omnigraph optimize s3://bucket/knowledge.omni
```

### Data plane: same command, embedded or remote

You don't pick "local vs server" syntax — resolution decides:

```bash
omnigraph query ./local.omni                     --query q.gq   # opens engine directly
omnigraph query --server prod --graph knowledge  --query q.gq   # over HTTP
omnigraph query --target knowledge               --query q.gq   # whichever the config says
```

### Maintenance: `--target` must resolve to direct storage (loud if not)

```bash
$ omnigraph optimize --target prod
error: `--target prod` resolves to a remote server (https://prod…).
       `optimize` is a storage-plane command and needs direct storage access.
       Pass the graph's s3://… URI, or use --cluster <dir> --graph <id>.
```

Cluster-managed graphs get an explicit, intentional path (no implicit
`cluster.yaml` peeking):

```bash
omnigraph optimize --cluster ./cluster --graph knowledge
```

### Wrong-plane = one honest, stable error

```bash
$ omnigraph optimize --server prod
error: `optimize` is a storage-plane command; `--server` addresses the data
       plane and does not apply here. Use --target <name> or a storage URI.

$ omnigraph graphs list ./local.omni
error: `graphs list` needs a remote multi-graph server (http/https) today.
       (Embedded cluster-catalog enumeration is planned — RFC-009.)
```

### `--help` teaches the model

```
DATA PLANE        run against a graph (embedded or --server)
  query  mutate  load  branch  snapshot  export  commit  schema show  schema apply

STORAGE / MAINTENANCE   direct storage access; no server
  init  optimize  repair  cleanup  schema plan  queries validate

CONTROL PLANE     manage a cluster directory
  cluster

INSPECT / SESSION
  graphs list  queries list  lint  policy  embed  login  logout  config
```

### Exceptions, signposted (not silent)

```bash
omnigraph init --schema s.pg ./new.omni            # plain path: fine

$ omnigraph init --target knowledge --schema s.pg  # cluster-managed target: redirected
error: `knowledge` is a cluster-managed graph. Create it via `cluster apply`
       (which records ledger + recovery + approvals), not `init`.
```

**In one line:** one way to name a graph, the right flags accepted per verb, and
a CLI that tells you its planes instead of making you memorize them.

## Proposed shape (mechanism)

### One addressing model for every graph-addressing verb

Route **all** graph-addressing verbs — data *and* maintenance — through one
resolver that turns `(positional URI | --target | --graph | --server)` into
either a **storage URI** (`file://`/`s3://`) → embedded execution, or a **remote
`GraphClient`** → HTTP execution, per the verb's declared plane.

**Authority rule (the precedence must not be silent).** `--target` is an
operator/legacy target lookup; `cluster.yaml` is a *different* authority surface
(read only by `cluster` commands and `--cluster` boot). A maintenance verb must
not quietly consult both and invent a precedence. The rule:

- A maintenance verb's `--target` resolves through the **operator/legacy**
  config and its URI must already be **direct storage**; a target that resolves
  to a remote (`http(s)://`) URL **fails loudly** (see the example above).
- **Cluster-managed graphs are addressed explicitly** via `--cluster <dir>
  --graph <id>`, so reading cluster state is an intentional mode — never an
  implicit fallback between operator config and `cluster.yaml`.

### A declared, per-subcommand capability surface (RFC-009 Phase 4, expanded)

One table, **per subcommand** (family-level rows hide exactly the cases the
table exists to make non-accidental):

| Command | Data (embedded) | Data (remote) | Storage (direct) | Config / session | Notes |
|---|---|---|---|---|---|
| `query`, `mutate`, `load`, `ingest` | ✅ | ✅ | — | — | `ingest` is the deprecated alias of `load` |
| `branch create/list/delete/merge` | ✅ | ✅ | — | — | |
| `snapshot`, `export`, `commit list/show` | ✅ | ✅ | — | — | |
| `schema show` | ✅ | ✅ | — | — | |
| `schema apply` | ✅ | ✅ | — | — | declarative alternative: `cluster apply` |
| `schema plan` | — | — | ✅ | — | local resolver today |
| `queries validate` | — | — | ✅ | — | opens the graph to typecheck |
| `init` | — | — | ✅ | — | cluster-managed graphs → `cluster apply` |
| `optimize`, `repair`, `cleanup` | — | — | ✅ | — | |
| `graphs list` | (later) | ✅ | — | — | remote today; embedded-cluster later (RFC-009) |
| `queries list` | — | — | — | ✅ | reads the registry config; no graph |
| `lint` | — | — | ✅ | ✅ | `--schema` file, or opens a local graph |
| `policy validate/test/explain` | — | — | — | ✅ | reads policy files + config |
| `embed` | — | — | — | ✅ | local tooling (files + embedding API) |
| `login`, `logout`, `config`, `version` | — | — | — | ✅ | session / config; no graph |

The resolver consults this table. A wrong-plane invocation produces one honest,
stable message instead of N ad-hoc `bail!`s and accidental `open` errors.

### Plane-grouped `--help`

Group the command list by plane (the `--help` block shown under Target CLI
ergonomics). Cosmetic, zero behavior change, highest legibility-per-line.

### Maintenance stays off the wire (decision, not omission)

This RFC **does not** add server routes for `optimize`/`cleanup`/`repair`:

- **Serving = the server.** Multi-tenant, safe-for-many-callers data plane.
- **Storage maintenance = the CLI against storage**, addressed uniformly,
  run by an operator or a scheduled job with storage access.

Adding maintenance-over-HTTP would re-introduce a heavyweight, destructive
multi-tenant surface and *add* a plane rather than clarify the three we have.
A future cluster-driven maintenance reconciler (scheduled compaction/GC as a
control-plane policy) is explicitly **out of scope** — net-new design (who runs
it, with what resource bounds), not a CLI restructure.

### `init` is an explicit exception (decision)

Direct-storage `init` against a plain URI/target stays. But if a target resolves
to a **cluster-managed** graph root, `init` **refuses and signposts** `cluster
apply` (which records ledger, recovery, and approval artifacts) rather than
initializing that root out of band. This closes the "hidden twin" of the current
state.

## Compatibility

Additive and low-risk:

- **`--target`/`--graph` on maintenance verbs** is new capability; the positional
  URI form keeps working unchanged.
- **Grouped `--help`** is cosmetic.
- **Capability-surface error text** changes the message you get on a wrong-plane
  or misaddressed invocation. Per Hyrum's Law that text is observable; the change
  is deliberate, release-noted, and replaces an *accidental* `Omnigraph::open`
  string with a *stable, declared* one — a net improvement, but flagged.

No engine, server, or wire-protocol change. The work is CLI-internal: the shared
resolver, the capability table, and help grouping.

## Test plan

Extend the existing CLI suites rather than adding a duplicate harness:

- **`parity_matrix.rs`** — capability exclusions (the per-subcommand plane table
  becomes the source of truth for which verbs are remote-only / storage-only).
- **`cli_data.rs`** — maintenance wrong-plane errors (`optimize --server`,
  `optimize --target <remote>`), and `--target` resolving to direct storage.
- **`cli_schema_config.rs`** — `graphs list` plane behavior, `schema plan`
  vs `schema show/apply` plane split, and plane-grouped `--help` output.
- **`system_local.rs`** — `--server` / operator-targeting edge cases end-to-end.

Pin the new wrong-plane error strings deliberately: this RFC is intentionally
replacing accidental `Omnigraph::open` strings with stable capability errors, and
those strings become observable behavior (Hyrum).

## Relationship to RFC-009

RFC-009 Phase 4 was scoped as "declared plane capabilities" for the
embedded-vs-remote axis only. This RFC **subsumes and broadens** that phase into
the full three-plane, per-subcommand model (adds uniform maintenance addressing,
the authority rule, and help grouping). RFC-009 Phase 5 (remote `load` →
`/load` route alignment) is unaffected and remains in RFC-009.

**`graphs list` reconciliation:** RFC-009's answered open question (pinned in
`parity_matrix.rs`'s exclusions comment) targets `graphs list` becoming
Both-capability once the embedded arm enumerates the cluster catalog. This RFC
**aligns** with that rather than superseding it: the capability table shows
`graphs list` as remote today, embedded-cluster later.

## Open questions

1. **Capability-table location** — a CLI-internal const, or surfaced (e.g. in
   `--help` and a machine-readable `omnigraph capabilities` for tooling)?
2. **`--cluster <dir> --graph <id>` for maintenance** — does the maintenance
   command resolve the storage URI from the applied cluster state, or from the
   declared `cluster.yaml`? (Applied state is the truth the server serves;
   declared config may be ahead of it.)

## Review comments (Codex, 2026-06-13)

Overall take: the direction is right. The planes already exist; making them
declared in code, help text, and error messages should reduce operator surprise.
Keeping storage maintenance off HTTP is also the right boundary: `optimize`,
`repair`, and `cleanup` are direct-storage operator actions, not a multi-tenant
serving surface.

Before implementation, tighten these points:

1. **Resolver authority needs a sharper rule.** The proposal says maintenance
   resolves storage URIs "from `cluster.yaml` / operator config", but those are
   different authority surfaces. Today `--target` is an operator/legacy
   graph-target lookup; cluster config is read by `cluster` commands and by
   `--cluster` server boot. Do not make a maintenance command silently consult
   both and pick a precedence. Either:
   - `--target` on maintenance means an operator/legacy target whose URI is
     already direct storage, with remote targets failing loudly; or
   - add an explicit cluster-root/config resolver for this case, so reading
     cluster state is an intentional mode.

   **Resolution (accepted):** both — `--target` resolves through operator/legacy
   config and must be direct storage (remote → loud fail); cluster-managed graphs
   use the explicit `--cluster <dir> --graph <id>` resolver. See *Authority
   rule* under Proposed shape.

2. **`graphs list` conflicts with RFC-009's target shape.** This RFC classifies
   `graphs list` as remote-only, while RFC-009's answered open question says it
   becomes Both-capability once the embedded arm enumerates the cluster catalog.
   Pick one direction here: either this RFC explicitly supersedes that target,
   or the capability table should show `graphs list` as remote today and
   embedded-cluster later.

   **Resolution (accepted):** align, don't supersede. The table shows `graphs
   list` remote-today / embedded-cluster-later. See *Relationship to RFC-009*.

3. **The capability table should be per subcommand, not per family.** The
   family-level rows hide the exact cases the table is supposed to make
   non-accidental. At minimum, call out:
   - `schema plan` as local/storage-backed today, while `schema show` and
     `schema apply` route through the graph client;
   - `queries validate` versus `queries list`, which do not have the same
     plane shape;
   - `lint`, `policy`, `embed`, `login`, `logout`, `config`, and `version`, so
     enumeration/session/tooling commands are intentionally classified instead
     of falling outside the model.

   **Resolution (accepted):** the capability table is now per-subcommand and
   classifies every command, including the session/tooling group.

4. **`init` should be an explicit exception.** Direct-storage `init` is fine.
   A cluster-managed graph should be created by `cluster apply`, with ledger,
   recovery, and approval semantics. If a named target resolves to a
   cluster-managed graph root, `init` should signpost `cluster apply` rather
   than quietly initializing that root out of band.

   **Resolution (accepted):** promoted from open question to a decision. See
   *`init` is an explicit exception*.

Testing notes for the implementation slice:

- Extend the existing CLI suites rather than adding a new duplicate harness:
  `parity_matrix.rs` for capability exclusions, `cli_data.rs` for maintenance
  wrong-plane errors, `cli_schema_config.rs` for `graphs list` / help behavior,
  and `system_local.rs` for `--server` / operator-targeting edge cases.
- Pin the new wrong-plane error strings deliberately. This RFC is intentionally
  replacing accidental `Omnigraph::open` strings with stable capability errors,
  and those strings become observable behavior.

  **Resolution (accepted):** captured as the *Test plan* section.
