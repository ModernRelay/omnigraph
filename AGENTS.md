# OmniGraph — Agent Guide

This file is the always-on map for AI coding agents (Claude Code, Codex, Cursor, Cline) working in this codebase. It is loaded into context on every turn, so it stays as a **map plus the rules and invariants that need to be in scope at all times** — the encyclopedia content lives under [`docs/`](docs/). When you need depth, follow a pointer.

**Required reading every session, every change:**

1. **[docs/dev/invariants.md](docs/dev/invariants.md)** — the architectural invariants and deny-list. Apply to every PR, not only architecture work.
2. **[docs/dev/lance.md](docs/dev/lance.md)** — the curated index of upstream Lance docs. **Consult it before every task** to identify which Lance pages are relevant. **Then fetch every page in the matching domain section, plus every page that is even slightly relevant** — not just the page whose title most obviously matches the task. Behavior is interlocked across pages (transactions reference index lifecycle; index lifecycle references compaction; compaction references row-id lineage), and skipping a "slightly relevant" page is how alignment misses happen. The index itself is not a substitute for reading the pages — never act on the index alone. **Always fetch the FULL page content, not summaries** — use `curl -sL <url> | pandoc -f html -t markdown` or paste the rendered page text manually. Tools that summarize pages (like Claude's `WebFetch`) drop load-bearing details — we have caught alignment misses (default flags, `pub(crate)` blockers, three-page sub-specs hidden behind navigation hubs) only after dumping the full markdown.
3. **[docs/dev/testing.md](docs/dev/testing.md)** — the test-coverage map. **Always check what already covers your change before writing a new test.** Extending an existing test (an assertion, a fixture row, a parameterization) is preferred over a duplicated `init_and_load()` block. Walk the before-every-task checklist to identify existing coverage, run those tests as a clean baseline, and only add a new test fn or file when no existing one owns the area.

Tools that support `@`-imports (Claude Code) auto-include all three files via the imports below — note these must sit at column 0 (not inside a blockquote) for the parser to recognize them. Other agents (Codex, Cursor, Cline, …) must open them explicitly at the start of each session.

@docs/dev/invariants.md
@docs/dev/lance.md
@docs/dev/testing.md

`CLAUDE.md` is a symlink to this file — there is exactly one source of truth. Edit `AGENTS.md`.

**Version surveyed:** 0.8.1
**Workspace crates:** `omnigraph-compiler`, `omnigraph` (engine), `omnigraph-policy`, `omnigraph-api-types` (shared HTTP wire DTOs), `omnigraph-cluster`, `omnigraph-cli`, `omnigraph-server`
**Storage substrate:** Lance 9.x (columnar, versioned, branchable; 9.0.0-beta.21 git-rev pin until 9.0.0 stable)
**License:** MIT
**Toolchain:** Rust stable, edition 2024

---

## Start here — what is this?

OmniGraph is a typed property-graph engine built as a coordination layer over many Lance datasets. Highlights:

- **Storage**: per node/edge table lifetime a separate Lance dataset; multi-dataset commits coordinated atomically through internal `__manifest` schema v6. The v5 stable-identity contract remains intact: manifest ownership, OCC, recovery, and identity-derived paths under `nodes/` and `edges/` use the immutable table pair, while `table_key` is only the current public alias. V6 additionally creates every graph table with exact non-null physical `id` as Lance's unenforced PK and fences every production insert/upsert on that field.
- **Languages**: a `.pg` schema language and a `.gq` query language, both Pest-based, with typed IR. Persisted accepted SchemaIR v2 owns one graph identity domain, one monotonic no-reuse allocator, rename-stable type/property IDs, and node/edge table-incarnation IDs.
- **Multi-modal querying**: vector ANN (`nearest`), full-text (`search`/`fuzzy`/`match_text`/`bm25`), Reciprocal Rank Fusion (`rrf`), and graph traversal (`Expand`, anti-join `not { … }`) in one runtime.
- **Branches and commits across the whole graph**: Git-style — every successful publish appends to a commit DAG; merges are three-way at the row level.
- **Atomic per-query writes**: `mutate_as` and `load` accumulate insert/update batches into an in-memory `MutationStaging.pending` per touched table. Strict insert and upsert both route through the sealed exact-`id`, filter-bearing adapter; bare Lance Append is test-only. Their RFC-022 adapter resolves or rejects relevant recovery intents before base capture, captures `(native branch id, exact graph_head, schema identity)`, then rechecks recovery and authority under schema → branch → table gates. Mutation/Load keeps one keyed transaction per table and rejects more than 8,192 rows or 32 MiB before recovery arm; mutation update scans stream into the remaining table budget after pending-key shadowing, with blob sizes checked before payload reads. It then arms an identity-bearing recovery-v9 sidecar containing exact Lance transaction identities + pre-minted lineage, commits each table with zero transparent conflict retries, confirms the achieved effects, and publishes under the same token. Unrelated retryable pre-effect authority movement may fully reprepare without changing logical mode. A proven effect-free strict conflict returns `KeyConflict` only after a fresh exact-ID probe; no exact match triggers bounded full strict-mode reprepare, while an effect-free upsert conflict also fully reprepares. Any earlier effect or ambiguity returns `RecoveryRequired` with the sidecar retained. Strict read-set conflicts return `ReadSetChanged`. Deletes stage through the same path. D₂ at parse time remains the constructive (insert/update) XOR destructive (delete) boundary.
- **HTTP server**: Axum + utoipa OpenAPI, bearer auth (SHA-256 hashed, optional AWS Secrets Manager). Cedar policy enforcement is engine-wide — every `_as` writer calls `Omnigraph::enforce(action, scope, actor)`, so HTTP, CLI, and embedded SDK consumers all hit the same gate. **Cluster-only boot** (RFC-011): the server always boots from a cluster directory (`--cluster <dir | s3://…>`, RFC-005) and serves N graphs (N ≥ 1) under multi-graph routes (`/graphs/{graph_id}/...` + read-only `GET /graphs` enumeration); there are no single-graph flat routes and no positional-URI boot. Per-graph + server-level Cedar policies. Runtime add/remove (`POST /graphs`, `DELETE /graphs/{id}`) is not exposed — operators run `cluster apply` and restart.
- **CLI** with two-surface config (RFC-007/008): the team-owned cluster directory (`cluster.yaml`) plus the per-operator `~/.omnigraph/config.yaml` (servers, clusters, credentials, actor, profiles, aliases, defaults). Graphs are addressed via `--store`/`--server`/`--cluster`/`--profile`/operator defaults (RFC-011). Multi-format output (json/jsonl/csv/kv/table).

Throughout the docs, capabilities are split into **L1 — Inherited from Lance** vs **L2 — Added by OmniGraph**.

---

## Architecture at a glance

```
CLI (omnigraph)        HTTP Server (omnigraph-server, Axum)
        │                            │
        └─────────────┬──────────────┘
                      ▼
           omnigraph-compiler  ── Pest grammars, catalog, IR, lowering, lint, migration plan
                      │
                      ▼
           omnigraph (engine)  ── ManifestCoordinator, CommitGraph, RunRegistry, GraphIndex (CSR/CSC), exec
                      │
                      ▼
              Lance 9.x         ── columnar Arrow, fragments, per-dataset versions/branches, indexes
                      │
                      ▼
        Object store (file / s3 / RustFS / MinIO / S3-compat)
```

Full diagram and concurrency model: [docs/dev/architecture.md](docs/dev/architecture.md).

---

## Where to find each topic

| Area | Read |
|---|---|
| **User docs entry point (public CLI/API/operator docs)** | **[docs/user/index.md](docs/user/index.md)** |
| **Developer docs entry point (architecture, invariants, testing, internals)** | **[docs/dev/index.md](docs/dev/index.md)** |
| **Architectural invariants & deny-list (read before any non-trivial proposal or review)** | **[docs/dev/invariants.md](docs/dev/invariants.md)** |
| **Lance docs index — fetch upstream Lance docs by problem domain** | **[docs/dev/lance.md](docs/dev/lance.md)** |
| **Test coverage map — what's covered, what helpers to reuse, before-every-task checklist** | **[docs/dev/testing.md](docs/dev/testing.md)** |
| The canon — linear internal narrative of the whole system (philosophy, read/write/crash walkthroughs, exclusions, risk register, roadmap) | [docs/dev/canon.md](docs/dev/canon.md) |
| Architecture, L1/L2 framing, concurrency model | [docs/dev/architecture.md](docs/dev/architecture.md) |
| Storage layout, `__manifest` schema, URI schemes, S3 env vars | [docs/user/concepts/storage.md](docs/user/concepts/storage.md) |
| `.pg` schema language, types, constraints, annotations, migration planning | [docs/user/schema/index.md](docs/user/schema/index.md) |
| Schema-lint codes (`OG-XXX-NNN`), families, severity, suppression | [docs/user/schema/lint.md](docs/user/schema/lint.md) |
| `.gq` query language, MATCH/RETURN/ORDER, IR ops, lint codes | [docs/user/queries/index.md](docs/user/queries/index.md) |
| Mutations — insert/update/delete, D2, atomicity | [docs/user/mutations/index.md](docs/user/mutations/index.md) |
| Search funcs (`nearest`/`bm25`/`rrf`), hybrid ranking | [docs/user/search/index.md](docs/user/search/index.md) |
| Indexes (BTREE / inverted / vector / graph topology) | [docs/user/search/indexes.md](docs/user/search/indexes.md) |
| Embeddings (engine client, env vars, `@embed`) | [docs/user/search/embeddings.md](docs/user/search/embeddings.md) |
| Concepts — what OmniGraph is, L1/L2 framing | [docs/user/concepts/index.md](docs/user/concepts/index.md) |
| Quickstart — init → load → query → branch | [docs/user/quickstart.md](docs/user/quickstart.md) |
| Branches, commit graph, system branches | [docs/user/branching/index.md](docs/user/branching/index.md) |
| Snapshots & time travel | [docs/user/branching/time-travel.md](docs/user/branching/time-travel.md) |
| Three-way merge and conflict kinds (user-facing) | [docs/user/branching/merge.md](docs/user/branching/merge.md) |
| Transactions and atomicity (per-query atomic; branches as multi-query transactions) | [docs/user/branching/transactions.md](docs/user/branching/transactions.md) |
| Direct-publish write path (staging, D2, recovery sidecars; the former Run state machine) | [docs/dev/writes.md](docs/dev/writes.md) |
| Three-way merge and conflict kinds | [docs/dev/merge.md](docs/dev/merge.md) |
| Diff / change feed (`diff_between`, `diff_commits`) | [docs/user/branching/changes.md](docs/user/branching/changes.md) |
| Query execution, mutation execution, bulk loader, `load` vs `ingest` | [docs/dev/execution.md](docs/dev/execution.md) |
| `optimize` (compaction) and `cleanup` (version GC) | [docs/user/operations/maintenance.md](docs/user/operations/maintenance.md) |
| Upgrade across a storage-format change (export/import rebuild) | [docs/user/operations/upgrade.md](docs/user/operations/upgrade.md) |
| Versioning & compatibility policy (release / wire / storage strict-single-version / Lance) | [docs/dev/versioning.md](docs/dev/versioning.md) |
| Cluster operator guide (deploy/manage clusters, approvals, recovery, serving) | [docs/user/clusters/index.md](docs/user/clusters/index.md) |
| Cedar policy actions, scopes, CLI | [docs/user/operations/policy.md](docs/user/operations/policy.md) |
| HTTP server endpoints, auth, error model, body limits | [docs/user/operations/server.md](docs/user/operations/server.md) |
| CLI quick-start | [docs/user/cli/index.md](docs/user/cli/index.md) |
| CLI command surface and config schema (`~/.omnigraph/config.yaml`) | [docs/user/cli/reference.md](docs/user/cli/reference.md) |
| Audit / actor tracking | [docs/user/operations/audit.md](docs/user/operations/audit.md) |
| Error taxonomy and result serialization | [docs/user/operations/errors.md](docs/user/operations/errors.md) |
| Install (binary / Homebrew / source / channels) | [docs/user/install.md](docs/user/install.md) |
| Deployment (binary / container / S3-local testing / auth / build variants) | [docs/user/deployment.md](docs/user/deployment.md) |
| CI / release workflows | [docs/dev/ci.md](docs/dev/ci.md) |
| Branch protection policy (declarative, applied via `scripts/apply-branch-protection.sh`) | [docs/dev/branch-protection.md](docs/dev/branch-protection.md) |
| Constants & tunables cheat sheet | [docs/user/reference/constants.md](docs/user/reference/constants.md) |
| RFC process — public contribution and maintainer design-series tracks | [docs/rfcs/README.md](docs/rfcs/README.md) |
| Per-version release notes | [docs/releases/](docs/releases/) |

---

## First principle: engineering is programming integrated over time

Software engineering is **programming integrated over time** (Winters, *Software Engineering at Google*). A line of code costs you at every future read, refactor, migration, and dependent change — not just at write-time. So the operative question for any change is: **which option has the lower ongoing liability?** Not "shorter now," not "fastest to ship," but which leaves the codebase narrower in the long run. **Complexity should be earned** — by demonstrated correctness, performance, or future-shape cost; never by speculation.

This is a decision lens, not a code-size rule. It cuts both ways. Sometimes the lower-liability option is:

- **More code.** A centralized dispatcher costs more lines than an ad-hoc heal hook, but each future change adds a match arm instead of a new hook scattered through the engine.
- **Less code.** Three similar lines that may diverge later cost less to maintain than a premature abstraction that has to be retrofitted every time a caller deviates.
- **DRYing.** Two copies of business logic that must stay in sync are a perpetual drift risk.
- **Duplication.** Two callers that look similar today but have independent evolution pressure shouldn't be wedged through a shared helper just because the lines match.
- **Removal.** A "just in case" code path with no caller is pure surface area: tests for it, docs that mention it, future changes that have to consider it.
- **Addition.** A migration framework, a typed error variant, a feature flag — each adds code now and lowers the cost of every future change in its surface.
- **A new abstraction**, when the absence forces every consumer to re-derive the same logic. Or **flattening one**, when the abstraction has accumulated more special-cases than the code it replaced.

When evaluating a design, ask: *"what does this look like after 5 more changes like it?"* If the answer is "this converges to one shape", cost is bounded. If it's "this forks every time", the option is mortgaging the future for present convenience — pick differently.

The same lens has a structural corollary: **one source of truth, cheaply derived.** Lance and the manifest are the source of truth; everything else is a derived view. Maintaining a parallel copy invites drift that compounds over time, and re-deriving a view from the full source on every call makes its cost grow with history. Both are liabilities integrated over time, so both are ruled out the same way: hold a warm derived view and refresh it with a cheap probe, never shadow the source or rebuild from it cold. Invariant 15 in [docs/dev/invariants.md](docs/dev/invariants.md) states this; invariants 1 (respect the substrate) and 7 (indexes are derived state) are instances.

### Tiebreakers when liability alone is silent

- **Correctness > simplicity > performance.** Lexicographic — give up performance for simpler code; give up simplicity for correct code; never give up correctness. The deny-list ("no silent failures," "no acks before durable persistence," "no reads of partial commits") is this rule's hard floor.
- **Reversibility shapes evidence demand.** Reversible changes wait for evidence: prefer prod metrics over napkin math over RFCs. Irreversible changes (substrate choice, on-disk format, database guarantees) earn an RFC, because by the time prod tells you they were wrong, you've shipped years of dependent code. Reviewers should spot both failure modes — RFC-ing a one-line config, and measuring-your-way into a substrate decision.

The always-on rules below and the deny-list in [docs/dev/invariants.md](docs/dev/invariants.md) are specific applications of this principle; when the rules are silent, fall back to it.

---

## Always-on rules (load these into your working memory)

These are architectural rules that need to be in scope on every change. They're framed at the level that survives renames and refactors — the deeper implementation specifics (function names, lock names, branch-prefix conventions, enforcement points) live in the per-area docs and may evolve. The full architectural invariants and deny-list are in [docs/dev/invariants.md](docs/dev/invariants.md); the deny-list is the fastest first-pass when reviewing any change.

1. **Multi-dataset publish is atomic across the whole graph.** A graph commit flips every relevant sub-table version visible together, in one manifest write. Don't introduce code paths that publish per sub-table outside the unified publish path — that loses cross-table snapshot isolation.
2. **Snapshot isolation per query.** A query holds one snapshot for its lifetime. Don't re-read the current head mid-query.
3. **Mutations are atomic at the commit boundary.** Multi-statement change queries publish one commit. Don't commit per-statement.
4. **Bearer-token plaintext never persists in process memory.** Tokens are hashed at startup; auth uses constant-time comparison; the actor id is server-resolved from the hash match and must not be settable by the client.
5. **Reads always see the current index state for the branch they're reading.** Indexes track the branch head, not historical snapshots. If you change index lifecycle, preserve this guarantee.
6. **Stable schema identity survives renames, not lifetimes.** Accepted SchemaIR v2 is the authority for graph-domain type/property IDs and node/edge table incarnations. A supported rename preserves those IDs, the dataset, path, and Lance history; drop/re-add mints a new declaration identity, incarnation, path, and version sequence. Never infer identity from an alias, path, Lance version, field ID, or branch ref.
7. **Logical contract over physical state.** Physical state (index coverage, fragment layout, compaction versions, staged writes) is derived and rebuildable; it must never fail a logical operation. Check preconditions against logical state and let reconciliation converge the physical state idempotently — genuine logical conflicts still fail loudly. This is the rule rules 1–6 instantiate; full statement and applications in [docs/dev/invariants.md](docs/dev/invariants.md).
8. **One source of truth, cheaply derived.** Lance and the manifest are the source of truth; runtime state is a derived view of them. Don't maintain a parallel copy that can drift, and don't re-derive a view from cold storage on every call (that makes cost grow with history). Hold it warm, refresh with a cheap probe.

### Deny-list (fast-pass review filter — full reasoning in [docs/dev/invariants.md](docs/dev/invariants.md))

If a proposal fits one of these, the burden is on the proposer to justify why this case is the exception:

- Synchronous-inline index updates for indexes expensive to build (vector ANN, FTS) — use the reconciler pattern.
- Custom WAL / transaction manager / buffer pool — Lance owns these.
- Job queue for state derivable from manifest — reconciler pattern instead.
- Per-feature lowering for shapes that share a structure (interfaces, wildcards, alternation) — use one mechanism.
- Eager materialization of cross-products in multi-hop — factorize; flatten only when needed.
- Ad-hoc IN-list filtering when SIP fits.
- String-flattened SQL filter generation when structured pushdown is available.
- In-process-only `Dataset` impls — `Send + Sync`, remote descriptors.
- Cost-blind plan choice — lowering-order execution is not a planner.
- Hidden statistics — if a metric matters for plan choice, it must be exposed through the trait surface.
- Side-channels for query semantics — search modes, mutations, polymorphism are first-class IR concepts.
- Discarding rank in retrieval — score and rank propagate as columns.
- State that drifts from the manifest — derive from observable state.
- Cloud-only correctness fixes — correctness is always OSS.
- Forking the codebase for Cloud — trait-extension only.
- Hand-rolling something Lance already does — check the spec first.
- Shadowing the source of truth with a maintained parallel copy, or re-deriving a derived view from cold storage per call (cost then scales with history). Hold it warm and refresh cheaply.
- Mutating in place state that should be immutable (Lance fragments, index segments) — new segments instead.
- Silent failures — OOM, timeout, partial result must all be surfaced and bounded.
- Shipping observable behavior as if it weren't part of the contract — output ordering, error-message text, timestamp precision, default-flag values, latency profile. Per Hyrum's Law, every observable behavior gets depended on once shipped; don't expose what you don't want to commit to.

---

## Build, test, lint

Rust stable workspace (edition 2024). `protoc` is a build dependency (`brew install protobuf` / `apt-get install protobuf-compiler libprotobuf-dev`). **Crate dir ≠ package name** for the engine: the directory is `crates/omnigraph` but its Cargo package is `omnigraph-engine` (use that in `-p`). The CLI binary built from `omnigraph-cli` is named `omnigraph`.

```bash
cargo build --workspace --locked              # build everything
cargo test  --workspace --locked              # the canonical CI gate (matches CI exactly)
cargo run -p omnigraph-cli -- <args>          # run the `omnigraph` CLI from source
cargo run -p omnigraph-server -- --cluster <dir|s3://...> --bind 0.0.0.0:8080   # run the server from source

# Run one crate / one test file / one test fn
cargo test -p omnigraph-engine --test traversal           # one integration-test file (see docs/dev/testing.md)
cargo test -p omnigraph-engine --test writes concurrent   # one test fn by name substring
cargo test -p omnigraph-engine some_inline_test -- --nocapture   # show stdout

# Feature-gated suites (each is its own job in CI, not part of the default run)
cargo test -p omnigraph-engine --features failpoints --test failpoints   # fault injection
cargo build -p omnigraph-server --features aws   # AWS Secrets Manager bearer-token source
```

S3-backed tests (`s3_storage`, and the S3 paths in server/CLI system tests) **skip** unless `OMNIGRAPH_S3_TEST_BUCKET` + `AWS_*` (incl. `AWS_ENDPOINT_URL_S3` for non-AWS) are set; CI runs them against containerized RustFS. To run RustFS/MinIO yourself, see [docs/user/deployment.md](docs/user/deployment.md) → *Testing against S3 locally*.

CI does **not** run `clippy` or `rustfmt` as gates — but `cargo test --workspace --locked` is the exact gate, so run it before pushing. Two non-test CI checks: `scripts/check-agents-md.sh` (doc cross-link integrity — run it after moving/renaming docs) and OpenAPI drift (`crates/omnigraph-server/tests/openapi.rs` regenerates `openapi.json`; set `OMNIGRAPH_UPDATE_OPENAPI=1` to update the checked-in copy when a server/API change is intentional).

## CocoIndex code search

This repo has CocoIndex project settings in `.cocoindex_code/settings.yml`. Use `ccc` when semantic search or structural AST search is faster than reading files manually.

```bash
ccc doctor                         # verify settings, model, matched files, index health
ccc index                          # build or refresh the local index
ccc status                         # show indexed file/chunk counts by language
ccc search "query pipeline"        # semantic search across indexed chunks
ccc search --lang rust "manifest publish recovery"
ccc search --path 'docs/dev/*' "write path"
ccc search --refresh "recent change"  # refresh index before searching
ccc grep 'fn \NAME(\(ARGS*\))' --lang rust   # structural AST search, no index needed
ccc grep 'Operation::\NAME { \(FIELDS*\) }' crates/
```

Generated index databases under `.cocoindex_code/` are local-only and ignored; keep `settings.yml` tracked.

---

## Quick-reference flows

```bash
# Initialize an S3-backed graph
omnigraph init --schema ./schema.pg s3://my-bucket/graph.omni

# Bulk load
omnigraph load --data ./seed.jsonl --mode overwrite s3://my-bucket/graph.omni

# Load a review batch onto its own branch (--from forks it if missing)
omnigraph load --branch review/2026-04-25 --from main --mode merge --data ./batch.jsonl s3://my-bucket/graph.omni

# Run a hybrid (vector + BM25) query — ad-hoc .gq against a store (positional = query name)
omnigraph query --query ./queries.gq find_similar \
  --params '{"q":"trends in AI safety"}' --format table --store s3://my-bucket/graph.omni

# Plan + apply schema migration
omnigraph schema plan  --schema ./next.pg s3://my-bucket/graph.omni
omnigraph schema apply --schema ./next.pg s3://my-bucket/graph.omni --json

# Merge review branch back
omnigraph branch merge review/2026-04-25 --into main s3://my-bucket/graph.omni

# Compact, preview any uncovered drift, then repair/GC after review
omnigraph optimize s3://my-bucket/graph.omni
omnigraph repair s3://my-bucket/graph.omni
omnigraph repair --confirm s3://my-bucket/graph.omni
# For suspicious/unverifiable drift only after deliberate review:
# omnigraph repair --force --confirm s3://my-bucket/graph.omni
omnigraph cleanup  --keep 10 --older-than 7d s3://my-bucket/graph.omni
omnigraph cleanup  --keep 10 --older-than 7d --confirm s3://my-bucket/graph.omni

# Stand up the HTTP server (token from env)
OMNIGRAPH_SERVER_BEARER_TOKEN=xxxx \
  omnigraph-server --cluster s3://my-bucket/cluster --bind 0.0.0.0:8080

# Cedar policy explain
omnigraph policy explain --cluster ./company-brain --graph knowledge --actor act-alice --action change --branch main
```

---

## Capability matrix — "Lens by default vs. added by OmniGraph"

| Capability | L1 (Lance default) | L2 (OmniGraph adds) |
|---|---|---|
| Columnar storage on object store | ✅ Arrow/Lance | URI normalization, S3 env-var plumbing |
| Per-dataset versioning + time travel | ✅ | `snapshot_at_version`, `entity_at`, snapshot-pinned reads across many tables |
| Stable schema + table identity | — | Persisted accepted SchemaIR v2 owns one graph identity domain and a shared monotonic no-reuse allocator for nonzero type, property, and table-incarnation IDs. Internal manifest schema v5 introduced identity-keyed table registration/version/tombstone rows, OCC, recovery ownership, and `nodes/{stable_table_id:016x}-{table_incarnation_id:016x}` / `edges/{stable_table_id:016x}-{table_incarnation_id:016x}` paths; v6 preserves that contract and adds keyed fencing. `table_key` is a mutable alias. A pure type rename changes only that alias and keeps the same dataset/path/version/history; drop/re-add mints a new type/property identity, incarnation, path, and independent Lance version sequence. This binary serves v6 only; v5/0.9.x and older graphs use export/import rebuild. |
| Per-dataset branches | ✅ | **Graph-level** refs are logically atomic through authoritative `__manifest` `BranchContents`; native create/delete crash gaps are classified and reclaimed under a single-writer-process boundary; live names are path-prefix-disjoint; data-table forks are lazy; system branches are filtered |
| Atomic single-dataset commits | ✅ | **Multi-table publish uses three layers**, not one Lance primitive: (1) each table's Lance effect, (2) one identity-aware `__manifest` CAS for graph visibility, and (3) recovery for the gap between them. All five active graph-visible writers (`MutationStaging::commit_all`, SchemaApply, BranchMerge, EnsureIndices, Optimize) emit an identity-bearing schema-v9 `__recovery/{ulid}.json` before their first durable effect and delete it only after manifest publication. Every pin, effect, registration, rename, tombstone, and output slot carries `(stable_table_id, table_incarnation_id)`; established payload names such as `protocol_v3`, `protocol_v4`, `protocol_v7`, and `protocol_v8` are wire-field names, not the emitted sidecar version. Pre-v9 files are explicitly historical and are never upgraded or assigned identity from aliases, paths, matching versions, or serde defaults. Under final schema → branch → table gates, Mutation/Load, SchemaApply, BranchMerge, and EnsureIndices prove the manifest/HEAD baseline, pre-mint exact Lance transactions + fixed lineage, and durably confirm complete outcomes before publication; `Armed` is rollback-only and `EffectsConfirmed` rolls forward only under the captured authority. Optimize uses the same v9 identity envelope with bounded maintenance provenance because Lance does not expose caller-minted maintenance transactions. Read-write open recovers all-or-nothing by identity, publishing one batch forward or restoring owned effects then publishing the restored versions; foreign or buried effects fail closed, and completed recovery is audited in `_graph_commit_recoveries.lance`. A pure SchemaApply type rename is metadata-only and keeps identity/path/version/index history; a simultaneous property change rewrites that same dataset, while only AddType is first-touch. Schema staging, the fixed manifest outcome, and catalog publication remain ordered; read capture binds one refreshed snapshot to one accepted SchemaIR-derived catalog under the schema gate, and read-only open refuses an incoherent fixed outcome. Enrolled write entry points and `refresh` also perform roll-forward-only in-process healing. These gates remain process-local; destructive recovery against a live writer in another process still requires a distributed fence. Engine writes stay behind sealed `TableStorage` `stage_*` + `commit_staged` APIs; deletes and the beta.21 one-segment full-table vector build use those staged v9 adapters, while Lance issue [#6666](https://github.com/lance-format/lance/issues/6666) remains relevant only to generic multi-segment exact vector publication. |
| Compaction (`compact_files`) + reindex (`optimize_indices`) | ✅ | `omnigraph optimize` orchestrates over all node/edge tables with bounded physical concurrency and one graph visibility envelope. Under schema → main → every accepted-catalog table gate it loads one identity-bound catalog/snapshot, skips uncovered HEAD drift, and plans only productive tables. One identity-bearing recovery-v9 `SidecarKind::Optimize` envelope pins the complete set before any table HEAD movement; each productive task runs `compact_files`, Lance `optimize_indices` (incremental coverage merge, not retrain), and declared-missing index materialization, but no task publishes independently. After all tasks settle, one maintenance-class monotonic manifest CAS publishes every still-needed identity pointer plus one graph lineage commit; a pointer already at or beyond the achieved version is converged and omitted. Thus two changed tables become visible together, a no-work run creates no sidecar/lineage, and any post-arm error returns `RecoveryRequired`. Full v9 recovery rolls the complete set forward in one batch or compensates a partial set before visibility. Main remains held through final physical-only `__manifest` compaction. Optimize's bounded maintenance classifier carries stable table identity but has no exact caller-minted transaction/authority/fixed-lineage proof, so destructive recovery retains the documented single-writer-process boundary until Lance exposes a stable maintenance transaction API and OmniGraph has distributed fencing. It **commits even with no compaction work if index coverage is stale**; reports untrainable vector-only work as pending without pinning it; **refuses on an unrecovered graph**; **skips uncovered HEAD > manifest drift** with `DriftNeedsRepair`; and **compacts blob-bearing tables**. |
| Repair uncovered drift | — | `omnigraph repair` explicitly classifies uncovered table `HEAD > manifest` drift: verified maintenance drift (`ReserveFragments`/`Rewrite`) can be published with `--confirm`; suspicious or unverifiable drift requires `--force --confirm`. Sidecar-covered crash residuals still recover automatically on open. |
| Cleanup (`cleanup_old_versions`) | ✅ | `omnigraph cleanup` derives requested `--keep` / `--older-than` cutoffs from each table's available versions; Lance refs plus OmniGraph's live-lazy-branch and recovery floors may retain additional versions. It fails closed on unopenable pins, recovery intent, or uncovered main-table HEAD drift |
| BTREE / inverted (FTS) / vector indexes | ✅ | `@index`/`@key` declares intent; the physical index is derived state that never fails a logical op. Built per column through one chokepoint (`build_indices_on_dataset_for_catalog`, type-dispatched by `node_prop_index_kind`: enum + orderable scalar → BTREE, free-text String → FTS, Vector → vector); idempotent; lazy across branches. **Schema apply and mutation/load build no indexes inline**: they publish only logical data/schema effects, leaving physical intent pending. `ensure_indices` first runs the roll-forward-only recovery barrier before base capture or planning, then materializes every declared-but-missing artifact for one table through one staged mixed CreateIndex transaction under its identity-bearing recovery-v9 authority/lineage/delta envelope; it continues to report untrainable Vector columns as pending. `Armed` is rollback-only, `EffectsConfirmed` rolls forward only while captured authority holds, and first-touch refs carry exact identity. `optimize` separately restores fragment coverage through its bounded identity-bearing v9 maintenance envelope. |
| Strict insert / upsert ingestion | ✅ transaction conflict filters + uncommitted fragment staging | Internal schema v6 makes logical mode explicit. General strict insert and upsert use the sealed exact-`id`, forced-v2 MergeInsert adapter; strict insert exact-probes its pinned parent before minting `omnigraph.insert_absence=v1`, while an all-new upsert may mint the same optional certificate only from its completed effect statistics. Mutation/Load remains one transaction per table, capped before arm at 8,192 rows / 32 MiB. BranchMerge's proven all-new route accepts only a complete certificate chain plus final source/target native-incarnation checks; it stages bounded fragments with `InsertBuilder`, commits them as exact-`id` filtered `Update` transactions, and performs zero target preflights, target merge joins, or committed Appends. Missing or malformed proof falls back to the general ordered diff. Raw Lance writers are outside the supported graph-writer topology, and the certificate is an internal, non-cryptographic capability. The final five-pair production gate passed at 10K (3.875× median; 24,297,472-byte max paired RSS overhead) and 100K (~3.886×; 32,604,160 bytes). |
| Vector search | ✅ | `nearest()` query op; embedding pipeline (Gemini / OpenAI clients); `@embed` in schema |
| Full-text search | ✅ | `search/fuzzy/match_text/bm25` query ops |
| Hybrid ranking | — | `rrf(...)` Reciprocal Rank Fusion in one runtime |
| Graph traversal | — | CSR/CSC topology index, `Expand` IR op, variable-length hops, `not { }` anti-join |
| Schema language | — | `.pg` + Pest grammar + identity-free canonical `SchemaShape`; persisted accepted SchemaIR v2 + identity-bound catalog + interfaces + constraints + annotations. Source supplies shape and rename hints, never authoritative IDs. |
| Query language | — | `.gq` + Pest grammar + IR + lowering + linter |
| Schema migration planning | — | ID-driven `plan_schema_migration` + `apply_schema` step types + `__schema_apply_lock__`. Exact-name matches retain identity; explicit supported renames preserve type/property IDs and table incarnation. Pure type rename is a manifest alias update over the same path/version; drop/re-add mints a new lifetime and cannot inherit the old tombstone or fencing authority. |
| Commit graph (DAG) across whole graph | — | Lineage (linear + merge parents, ULID ids, actor) stored as `graph_commit`/`graph_head` rows in `__manifest`, written in the same publish CAS as the table-version rows (RFC-013 Phase 7 — atomic with the graph commit). The in-memory commit graph is a pure projection of those rows; the legacy `_graph_commits.lance` / `_graph_commit_actors.lance` tables are **retired** (a fresh graph creates neither) |
| Per-query atomic writes | — | In-memory `MutationStaging.pending` accumulator + one exact staged Lance transaction per touched table + identity-bearing recovery-v9 confirmation + exact branch-head publisher CAS (single manifest commit per `mutate_as` / `load`); D₂ parse-time rule keeps inserts/updates and deletes from mixing |
| Three-way row-level merge | — | `OrderedTableCursor` + `StagedTableWriter`, structured `MergeConflictKind` |
| Change feeds | — | `diff_between` / `diff_commits` with manifest fast path + ID streaming |
| Cedar policy | — | Per-graph actions plus server-scoped actions (see [docs/user/operations/policy.md](docs/user/operations/policy.md) for the current list), branch / target_branch / protected scopes, validate/test/explain CLI. **Engine-wide enforcement** (MR-722): every `_as` writer (`apply_schema_as`, `mutate_as`, `load_as` — the deprecated `ingest_as` shims route through it — `branch_create_as` / `branch_create_from_as`, `branch_delete_as`, `branch_merge_as`) calls `Omnigraph::enforce(action, scope, actor)` — HTTP, CLI, embedded SDK all hit the same gate. |
| HTTP server | — | Axum, OpenAPI via utoipa, bearer auth (SHA-256, AWS Secrets Manager option), `authorize_request` at the HTTP boundary (resolves bearer→actor, applies admission control), NDJSON streaming export, **cluster-only boot (RFC-011): always `--cluster <dir | s3://…>`, serving N graphs (N ≥ 1) under multi-graph routes + read-only `GET /graphs` enumeration + per-graph + server-level Cedar policies. Add/remove graphs via `cluster apply` and restart.** |
| CLI with config | — | two-surface config (team `cluster.yaml` dir + per-operator `~/.omnigraph/config.yaml`), scope addressing (`--store`/`--server`/`--cluster`/`--profile`/defaults, RFC-011), aliases, multi-format output (json/jsonl/csv/kv/table) |
| Audit / actor tracking | — | `_as` write APIs + actor map in commit graph |
| Local S3 testing | — | run RustFS/MinIO + the `AWS_*` env; see [docs/user/deployment.md](docs/user/deployment.md) → *Testing against S3 locally* |
| Agent skill | — | `skills/omnigraph` — operational playbook for driving Omnigraph; install with `npx skills add ModernRelay/omnigraph@omnigraph` |

The supported SDK graph-write set is closed primarily by Rust visibility: the
raw storage, handle-cache, and coordinator modules are crate-private, while a
public snapshot opens tables through a read-only facade whose scan builder
executes reads without exposing Lance's raw `Scanner` or physical plan. The defense-in-depth registry in
`crates/omnigraph/tests/forbidden_apis.rs` classifies every public async inherent
`Omnigraph` method and loader convenience, every crate-visible async coordinator
method, and exact per-file occurrences of the registered durable-call shapes,
including recovery. It also rejects known direct Lance open/mutation shapes
outside exact gateway files. A new supported writer or durable gateway must
update that registry and its protocol coverage. The source scanner is not a
Rust macro-expansion or alias-analysis engine; visibility is the structural
boundary.

---

## Maintenance contract for agents

When you change something user-visible, **update the relevant `docs/user/<area>.md` in the same change**. Use [docs/user/index.md](docs/user/index.md) for public behavior and [docs/dev/index.md](docs/dev/index.md) for contributor/internal mechanics. Pointers from this file to those docs must keep working — CI enforces cross-link integrity via `scripts/check-agents-md.sh`.

When proposing or reviewing a non-trivial change, walk [docs/dev/invariants.md](docs/dev/invariants.md) — at minimum the deny-list and review checklist. Add to the deny-list when a new anti-pattern surfaces; relaxing an invariant requires the same review process as code.

Rules:

1. **Update in the same PR.** New endpoint, query function, CLI flag, env var, constant, schema construct, or invariant: update both the source code and the doc in the same change. Never split documentation drift into a follow-up.
2. **Bump version on release.** When a release boundary crosses (e.g. v0.3.1 → v0.3.2), update the version line at the top of this file and add a `docs/releases/<version>.md` describing the user-visible delta. Update [docs/dev/architecture.md](docs/dev/architecture.md) only if the architecture itself changed.
3. **Write OSS-facing release notes.** Release docs are public project history. Describe capabilities, behavior changes, breaking changes, upgrade notes, and user impact; do not reference private ticket systems, internal codenames, or planning shorthand that an outside contributor cannot inspect.
4. **Keep versioning coherent.** A release bump must update every published crate manifest, local path dependency constraint, `Cargo.lock`, generated API metadata such as `openapi.json`, and this file's surveyed version. Do not leave mixed package versions unless the release plan explicitly calls for them.
5. **Keep docs audience-neutral.** Prefer stable public identifiers (versions, PR numbers, public issue links, crate names, endpoint names) over organization-specific labels. If internal context is useful for maintainers, translate it into a durable public rationale before committing it.
6. **Don't lie.** If a section becomes wrong but you can't rewrite it fully right now, replace the wrong line with `*(stale — needs update after <change>)*` rather than leaving silently incorrect text. Then fix it ASAP.
7. **Re-verify before recommending.** If you cite a flag, env var, endpoint, or constant to the user or in code, grep for it in source first. Memory and docs go stale; the code is authoritative.
8. **Keep AGENTS.md short.** This file is always loaded into agent context, so every added line has a recurring context-window cost. Prefer pointers and terse invariants here; put detail in `docs/`.
9. **Keep AGENTS.md a map, not an encyclopedia.** New deep content goes into `docs/`. Add an entry to "Where to find each topic" instead of pasting prose into this file. The "Always-on rules" section is the exception — it's for invariants that should always be in scope.
10. **Re-read on schema/query/IR changes.** Edits to `schema.pest`, `query.pest`, `ir/lower.rs`, `query/typecheck.rs`, or `query/lint.rs` should trigger a re-read of [docs/user/schema/index.md](docs/user/schema/index.md), [docs/user/queries/index.md](docs/user/queries/index.md), and [docs/dev/execution.md](docs/dev/execution.md) to confirm they still describe reality.
11. **Always make smaller commits.** Each commit does one thing, compiles, and passes tests; mechanical refactors land separately from the behavior changes they enable.
12. **Test-first for bug fixes.** When fixing an identified bug, write a regression test that reproduces the failure first. Confirm it fails against the current code with the predicted symptom (not an unrelated error). Then land the fix in a separate commit and confirm the test turns green. The test commit lands just before the fix commit so the red → green pair is visible in `git log` and a reviewer can check out the test commit alone and reproduce the failure.
13. **Correct by design over symptomatic patches.** When a bug surfaces, identify the root cause and make the fix correct by construction. Don't patch the symptom. If the design admits the bug class, the fix is to close the class, not to add a guard around the latest instance. A symptomatic patch is acceptable only as a stop-gap, with an explicit note in the commit message and a follow-up issue tracking the design fix.

CI check: `scripts/check-agents-md.sh` verifies that docs links in this file and the audience indexes resolve, and that every canonical doc is linked from either [docs/user/index.md](docs/user/index.md) or [docs/dev/index.md](docs/dev/index.md). Run it locally before opening a PR if you've moved or renamed docs.
