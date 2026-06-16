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

**Version surveyed:** 0.7.0
**Workspace crates:** `omnigraph-compiler`, `omnigraph` (engine), `omnigraph-policy`, `omnigraph-api-types` (shared HTTP wire DTOs), `omnigraph-cluster`, `omnigraph-cli`, `omnigraph-server`
**Storage substrate:** Lance 7.x (columnar, versioned, branchable)
**License:** MIT
**Toolchain:** Rust stable, edition 2024

---

## Start here — what is this?

OmniGraph is a typed property-graph engine built as a coordination layer over many Lance datasets. Highlights:

- **Storage**: per node/edge type a separate Lance dataset; multi-dataset commits coordinated atomically through one `__manifest` table.
- **Languages**: a `.pg` schema language and a `.gq` query language, both Pest-based, with a typed IR.
- **Multi-modal querying**: vector ANN (`nearest`), full-text (`search`/`fuzzy`/`match_text`/`bm25`), Reciprocal Rank Fusion (`rrf`), and graph traversal (`Expand`, anti-join `not { … }`) in one runtime.
- **Branches and commits across the whole graph**: Git-style — every successful publish appends to a commit DAG; merges are three-way at the row level.
- **Atomic per-query writes**: `mutate_as` and `load` accumulate insert/update batches into an in-memory `MutationStaging.pending` per touched table; one `stage_*` + `commit_staged` per table runs at end-of-query, then `ManifestBatchPublisher::publish` commits the manifest atomically with per-table `expected_table_versions` CAS. A mid-query failure leaves Lance HEAD untouched on staged tables — no drift, no run state machine, no staging branches. Deletes still inline-commit; D₂ at parse time prevents inserts/updates and deletes from coexisting in one query.
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
              Lance 7.x         ── columnar Arrow, fragments, per-dataset versions/branches, indexes
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
| Code ownership (CODEOWNERS source of truth, roles, regeneration) | [docs/dev/codeowners.md](docs/dev/codeowners.md) |
| Branch protection policy (declarative, applied via `scripts/apply-branch-protection.sh`) | [docs/dev/branch-protection.md](docs/dev/branch-protection.md) |
| Constants & tunables cheat sheet | [docs/user/reference/constants.md](docs/user/reference/constants.md) |
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
6. **Stable type IDs survive renames.** Schema migration relies on identity that's stable across rename — don't mint new IDs on rename.
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
| Per-dataset branches | ✅ | **Graph-level** branches (atomic across all sub-tables), lazy fork, system branch filtering |
| Atomic single-dataset commits | ✅ | **Multi-table publish via three layers**, NOT a single Lance primitive: (1) per-table Lance `commit_staged` for the data write, (2) `__manifest` row-level CAS via `ManifestBatchPublisher` for cross-table ordering, (3) the open-time recovery sweep for the residual gap between (1) and (2). All three layers ship; the five migrated writers (`MutationStaging::finalize`, `schema_apply`, `branch_merge`, `ensure_indices`, `optimize_all_tables`) write a `__recovery/{ulid}.json` sidecar before Phase B and delete it after Phase C. The next `Omnigraph::open` (gated on `OpenMode::ReadWrite`) runs the sweep in `db/manifest/recovery.rs`: classify, decide all-or-nothing per sidecar, roll forward via single `ManifestBatchPublisher::publish` or roll back via `Dataset::restore` followed by a manifest publish of the restored version (so both directions converge to `manifest == HEAD` — no residual drift), and record an audit row in `_graph_commit_recoveries.lance` (queryable via `omnigraph commit list --filter actor=omnigraph:recovery`). The write entry points (`load_as`, `mutate_as`, `apply_schema_as`, `branch_merge_as`) and `refresh` additionally run an in-process roll-forward-only heal (serialized against live writers via the per-table write queues), so a long-lived server converges on its next write without restart; only rollback-eligible sidecars still defer to the next read-write open (a future background reconciler's goal). Engine writes route through a sealed `TableStorage` trait (`db.storage()`) exposing only `stage_*` + `commit_staged` + reads; the inline-commit residuals (`delete_where`, `create_vector_index`) are split onto a separate sealed `InlineCommitResidual` trait reached via `db.storage_inline_residual()` (MR-854), so the default surface cannot couple a write with a HEAD advance — §1 holds by construction. `delete_where` and `create_vector_index` stay inline until upstream Lance ships a public two-phase API ([#6658](https://github.com/lance-format/lance/issues/6658), [#6666](https://github.com/lance-format/lance/issues/6666)); `LoadMode::Overwrite` uses Lance `Overwrite` staged transactions. |
| Compaction (`compact_files`) + reindex (`optimize_indices`) | ✅ | `omnigraph optimize` orchestrates over all node/edge tables, bounded concurrency; per table runs `compact_files` **then Lance `optimize_indices`** (folds appended/rewritten fragments back into existing indexes — incremental merge, not retrain) and **publishes the resulting version to `__manifest`** (so the manifest tracks the Lance HEAD — required for reads to observe the work and for schema apply / strict writes to pass their HEAD-vs-manifest precondition), under the per-`(table, main)` write queue with `SidecarKind::Optimize` recovery coverage spanning both ops; **commits even with no compaction work if index coverage is stale**; **refuses on an unrecovered graph**; **skips uncovered HEAD > manifest drift** with `DriftNeedsRepair`; **skips blob-bearing tables** (reported via `TableOptimizeStats.skipped`, not silent; reindex is skipped for them too today), gated on `LANCE_SUPPORTS_BLOB_COMPACTION` until the upstream blob-v2 compaction-decode bug is fixed (see [docs/dev/invariants.md](docs/dev/invariants.md) Known Gaps) |
| Repair uncovered drift | — | `omnigraph repair` explicitly classifies uncovered table `HEAD > manifest` drift: verified maintenance drift (`ReserveFragments`/`Rewrite`) can be published with `--confirm`; suspicious or unverifiable drift requires `--force --confirm`. Sidecar-covered crash residuals still recover automatically on open. |
| Cleanup (`cleanup_old_versions`) | ✅ | `omnigraph cleanup` with `--keep` / `--older-than` policy |
| BTREE / inverted (FTS) / vector indexes | ✅ | `@index`/`@key` declares intent; the physical index is derived state that never fails a logical op. Built per column through one chokepoint (`build_indices_on_dataset_for_catalog`, type-dispatched by `node_prop_index_kind`: enum + orderable scalar → BTREE, free-text String → FTS, Vector → vector); idempotent; lazy across branches. **Schema apply builds nothing** (records intent only); `load`/`mutate` build inline but **defer an untrainable Vector column** (no trainable vectors yet) as *pending* rather than aborting. `ensure_indices`/`optimize` is the reconciler that materializes declared-but-missing indexes and restores coverage of appended/rewritten fragments (`optimize_indices`), reporting still-pending columns (see Compaction row). |
| `merge_insert` upsert | ✅ | `LoadMode::Merge`, mutation `update`/`insert`/`delete` lowering |
| Vector search | ✅ | `nearest()` query op; embedding pipeline (Gemini / OpenAI clients); `@embed` in schema |
| Full-text search | ✅ | `search/fuzzy/match_text/bm25` query ops |
| Hybrid ranking | — | `rrf(...)` Reciprocal Rank Fusion in one runtime |
| Graph traversal | — | CSR/CSC topology index, `Expand` IR op, variable-length hops, `not { }` anti-join |
| Schema language | — | `.pg` + Pest grammar + catalog + interfaces + constraints + annotations |
| Query language | — | `.gq` + Pest grammar + IR + lowering + linter |
| Schema migration planning | — | `plan_schema_migration` + `apply_schema` step types + `__schema_apply_lock__` |
| Commit graph (DAG) across whole graph | — | `_graph_commits.lance` with linear + merge parents, ULID ids, actor map |
| Per-query atomic writes | — | In-memory `MutationStaging.pending` accumulator + `stage_*` / `commit_staged` per touched table at end-of-query + publisher CAS via `commit_with_expected` (single manifest commit per `mutate_as` / `load`); D₂ parse-time rule keeps inserts/updates and deletes from mixing |
| Three-way row-level merge | — | `OrderedTableCursor` + `StagedTableWriter`, structured `MergeConflictKind` |
| Change feeds | — | `diff_between` / `diff_commits` with manifest fast path + ID streaming |
| Cedar policy | — | Per-graph actions plus server-scoped actions (see [docs/user/operations/policy.md](docs/user/operations/policy.md) for the current list), branch / target_branch / protected scopes, validate/test/explain CLI. **Engine-wide enforcement** (MR-722): every `_as` writer (`apply_schema_as`, `mutate_as`, `load_as` — the deprecated `ingest_as` shims route through it — `branch_create_as` / `branch_create_from_as`, `branch_delete_as`, `branch_merge_as`) calls `Omnigraph::enforce(action, scope, actor)` — HTTP, CLI, embedded SDK all hit the same gate. |
| HTTP server | — | Axum, OpenAPI via utoipa, bearer auth (SHA-256, AWS Secrets Manager option), `authorize_request` at the HTTP boundary (resolves bearer→actor, applies admission control), NDJSON streaming export, **cluster-only boot (RFC-011): always `--cluster <dir | s3://…>`, serving N graphs (N ≥ 1) under multi-graph routes + read-only `GET /graphs` enumeration + per-graph + server-level Cedar policies. Add/remove graphs via `cluster apply` and restart.** |
| CLI with config | — | two-surface config (team `cluster.yaml` dir + per-operator `~/.omnigraph/config.yaml`), scope addressing (`--store`/`--server`/`--cluster`/`--profile`/defaults, RFC-011), aliases, multi-format output (json/jsonl/csv/kv/table) |
| Audit / actor tracking | — | `_as` write APIs + actor map in commit graph |
| Local S3 testing | — | run RustFS/MinIO + the `AWS_*` env; see [docs/user/deployment.md](docs/user/deployment.md) → *Testing against S3 locally* |
| Agent skill | — | `skills/omnigraph` — operational playbook for driving Omnigraph; install with `npx skills add ModernRelay/omnigraph@omnigraph` |

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
