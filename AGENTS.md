# OmniGraph — Agent Guide

This file is the always-on map for AI coding agents (Claude Code, Codex, Cursor, Cline) working in this repo. It is loaded into context on every turn, so it stays as a **map plus the rules and invariants that need to be in scope at all times** — the encyclopedia content lives under [`docs/`](docs/). When you need depth, follow a pointer.

**Required reading every session, every change:**

1. **[docs/invariants.md](docs/invariants.md)** — the architectural invariants and §IX deny-list. Apply to every PR, not only architecture work.
2. **[docs/lance.md](docs/lance.md)** — the curated index of upstream Lance docs. **Consult it before every task** to identify which Lance pages are relevant. **Then fetch every page in the matching domain section, plus every page that is even slightly relevant** — not just the page whose title most obviously matches the task. Behavior is interlocked across pages (transactions reference index lifecycle; index lifecycle references compaction; compaction references row-id lineage), and skipping a "slightly relevant" page is how alignment misses happen. The index itself is not a substitute for reading the pages — never act on the index alone. **Always fetch the FULL page content, not summaries** — use `npx mdrip <url>` (or `npx mdrip --max-chars 200000 <url>` for very long pages). Tools that summarize pages (like Claude's `WebFetch`) drop load-bearing details — we have caught alignment misses (default flags, `pub(crate)` blockers, three-page sub-specs hidden behind navigation hubs) only after dumping the full markdown. If `npx mdrip` is unavailable, fall back to `curl <url> | pandoc -f html -t markdown` or paste the rendered page text manually; never act on a summarized fetch alone.
3. **[docs/testing.md](docs/testing.md)** — the test-coverage map. **Always check what already covers your change before writing a new test.** Extending an existing test (an assertion, a fixture row, a parameterization) is preferred over a duplicated `init_and_load()` block. Walk the before-every-task checklist to identify existing coverage, run those tests as a clean baseline, and only add a new test fn or file when no existing one owns the area.

Tools that support `@`-imports (Claude Code) auto-include all three files via the imports below — note these must sit at column 0 (not inside a blockquote) for the parser to recognize them. Other agents (Codex, Cursor, Cline, …) must open them explicitly at the start of each session.

@docs/invariants.md
@docs/lance.md
@docs/testing.md

`CLAUDE.md` is a symlink to this file — there is exactly one source of truth. Edit `AGENTS.md`.

**Version surveyed:** 0.4.0
**Workspace crates:** `omnigraph-compiler`, `omnigraph` (engine), `omnigraph-cli`, `omnigraph-server`
**Storage substrate:** Lance 4.x (columnar, versioned, branchable)
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
- **HTTP server**: Axum + utoipa OpenAPI, bearer auth (SHA-256 hashed, optional AWS Secrets Manager), Cedar policy gating.
- **CLI** driven by a single `omnigraph.yaml`; multi-format output (json/jsonl/csv/kv/table).

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
           omnigraph (engine)  ── ManifestRepo, CommitGraph, RunRegistry, GraphIndex (CSR/CSC), exec
                      │
                      ▼
              Lance 4.x         ── columnar Arrow, fragments, per-dataset versions/branches, indexes
                      │
                      ▼
        Object store (file / s3 / RustFS / MinIO / S3-compat)
```

Full diagram and concurrency model: [docs/architecture.md](docs/architecture.md).

---

## Where to find each topic

| Area | Read |
|---|---|
| **Architectural invariants & deny-list (read before any non-trivial proposal or review)** | **[docs/invariants.md](docs/invariants.md)** |
| **Lance docs index — fetch upstream Lance docs by problem domain** | **[docs/lance.md](docs/lance.md)** |
| **Test coverage map — what's covered, what helpers to reuse, before-every-task checklist** | **[docs/testing.md](docs/testing.md)** |
| Architecture, L1/L2 framing, concurrency model | [docs/architecture.md](docs/architecture.md) |
| Storage layout, `__manifest` schema, URI schemes, S3 env vars | [docs/storage.md](docs/storage.md) |
| `.pg` schema language, types, constraints, annotations, migration planning | [docs/schema-language.md](docs/schema-language.md) |
| `.gq` query language, MATCH/RETURN/ORDER, search funcs, mutations, IR ops, lint codes | [docs/query-language.md](docs/query-language.md) |
| Indexes (BTREE / inverted / vector / graph topology) | [docs/indexes.md](docs/indexes.md) |
| Embeddings (compiler + engine clients, env vars, `@embed`) | [docs/embeddings.md](docs/embeddings.md) |
| Branches, commit graph, snapshots, system branches | [docs/branches-commits.md](docs/branches-commits.md) |
| Direct-publish writes (the former Run state machine, now demoted to publisher CAS) | [docs/runs.md](docs/runs.md) |
| Three-way merge and conflict kinds | [docs/merge.md](docs/merge.md) |
| Diff / change feed (`diff_between`, `diff_commits`) | [docs/changes.md](docs/changes.md) |
| Query execution, mutation execution, bulk loader, `load` vs `ingest` | [docs/execution.md](docs/execution.md) |
| `optimize` (compaction) and `cleanup` (version GC) | [docs/maintenance.md](docs/maintenance.md) |
| Cedar policy actions, scopes, CLI | [docs/policy.md](docs/policy.md) |
| HTTP server endpoints, auth, error model, body limits | [docs/server.md](docs/server.md) |
| CLI quick-start | [docs/cli.md](docs/cli.md) |
| CLI command surface and `omnigraph.yaml` schema | [docs/cli-reference.md](docs/cli-reference.md) |
| Audit / actor tracking | [docs/audit.md](docs/audit.md) |
| Error taxonomy and result serialization | [docs/errors.md](docs/errors.md) |
| Install (binary / Homebrew / source / channels) | [docs/install.md](docs/install.md) |
| Deployment (binary / container / RustFS bootstrap / auth / build variants) | [docs/deployment.md](docs/deployment.md) |
| CI / release workflows | [docs/ci.md](docs/ci.md) |
| Constants & tunables cheat sheet | [docs/constants.md](docs/constants.md) |
| Per-version release notes | [docs/releases/](docs/releases/) |

---

## First principle: minimize ongoing liability

Every decision — adding code, removing code, picking an abstraction, choosing a layer, writing a doc paragraph — carries an ongoing maintenance cost. Before any change, ask: **which option has the lower ongoing cost over time?** Not "shorter now," not "fastest to ship," but which leaves the codebase narrower in the long run.

This is a decision lens, not a code-size rule. It cuts both ways. Sometimes the lower-liability option is:

- **More code.** A centralized dispatcher costs more lines than an ad-hoc heal hook, but each future change adds a match arm instead of a new hook scattered through the engine.
- **Less code.** Three similar lines that may diverge later cost less to maintain than a premature abstraction that has to be retrofitted every time a caller deviates.
- **DRYing.** Two copies of business logic that must stay in sync are a perpetual drift risk.
- **Duplication.** Two callers that look similar today but have independent evolution pressure shouldn't be wedged through a shared helper just because the lines match.
- **Removal.** A "just in case" code path with no caller is pure surface area: tests for it, docs that mention it, future changes that have to consider it.
- **Addition.** A migration framework, a typed error variant, a feature flag — each adds code now and lowers the cost of every future change in its surface.
- **A new abstraction**, when the absence forces every consumer to re-derive the same logic. Or **flattening one**, when the abstraction has accumulated more special-cases than the code it replaced.

When evaluating a design, ask: *"what does this look like after 5 more changes like it?"* If the answer is "this converges to one shape", cost is bounded. If it's "this forks every time", the option is mortgaging the future for present convenience — pick differently.

The always-on rules below and the §IX deny-list in [docs/invariants.md](docs/invariants.md) are specific applications of this principle; when the rules are silent, fall back to it.

---

## Always-on rules (load these into your working memory)

These are architectural rules that need to be in scope on every change. They're framed at the level that survives renames and refactors — the deeper implementation specifics (function names, lock names, branch-prefix conventions, enforcement points) live in the per-area docs and may evolve. The full architectural invariants and deny-list are in [docs/invariants.md](docs/invariants.md); §IX (deny-list) is the fastest first-pass when reviewing any change.

1. **Multi-dataset publish is atomic across the whole graph.** A graph commit flips every relevant sub-table version visible together, in one manifest write. Don't introduce code paths that publish per sub-table outside the unified publish path — that loses cross-table snapshot isolation.
2. **Snapshot isolation per query.** A query holds one snapshot for its lifetime. Don't re-read the current head mid-query.
3. **Mutations are atomic at the commit boundary.** Multi-statement change queries publish one commit. Don't commit per-statement.
4. **Bearer-token plaintext never persists in process memory.** Tokens are hashed at startup; auth uses constant-time comparison; the actor id is server-resolved from the hash match and must not be settable by the client.
5. **Reads always see the current index state for the branch they're reading.** Indexes track the branch head, not historical snapshots. If you change index lifecycle, preserve this guarantee.
6. **Stable type IDs survive renames.** Schema migration relies on identity that's stable across rename — don't mint new IDs on rename.

### Deny-list (fast-pass review filter — full reasoning in [docs/invariants.md §IX](docs/invariants.md))

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
- Mutating in place state that should be immutable (Lance fragments, index segments) — new segments instead.
- Silent failures — OOM, timeout, partial result must all be surfaced and bounded.

---

## Quick-reference flows

```bash
# Initialize an S3-backed repo
omnigraph init --schema ./schema.pg s3://my-bucket/repo.omni

# Bulk load
omnigraph load --data ./seed.jsonl --mode overwrite s3://my-bucket/repo.omni

# Branch + ingest a review batch
omnigraph branch create --from main review/2026-04-25 s3://my-bucket/repo.omni
omnigraph ingest --branch review/2026-04-25 --data ./batch.jsonl s3://my-bucket/repo.omni

# Run a hybrid (vector + BM25) query
omnigraph read --query ./queries.gq --name find_similar \
  --params '{"q":"trends in AI safety"}' --format table s3://my-bucket/repo.omni

# Plan + apply schema migration
omnigraph schema plan  --schema ./next.pg s3://my-bucket/repo.omni
omnigraph schema apply --schema ./next.pg s3://my-bucket/repo.omni --json

# Merge review branch back
omnigraph branch merge review/2026-04-25 --into main s3://my-bucket/repo.omni

# Compact + GC (preview, then confirm)
omnigraph optimize s3://my-bucket/repo.omni
omnigraph cleanup  --keep 10 --older-than 7d s3://my-bucket/repo.omni
omnigraph cleanup  --keep 10 --older-than 7d --confirm s3://my-bucket/repo.omni

# Stand up the HTTP server (token from env)
OMNIGRAPH_SERVER_BEARER_TOKEN=xxxx \
  omnigraph-server s3://my-bucket/repo.omni --bind 0.0.0.0:8080

# Cedar policy explain
omnigraph policy explain --actor act-alice --action change --branch main
```

---

## Capability matrix — "Lens by default vs. added by OmniGraph"

| Capability | L1 (Lance default) | L2 (OmniGraph adds) |
|---|---|---|
| Columnar storage on object store | ✅ Arrow/Lance | URI normalization, S3 env-var plumbing |
| Per-dataset versioning + time travel | ✅ | `snapshot_at_version`, `entity_at`, snapshot-pinned reads across many tables |
| Per-dataset branches | ✅ | **Graph-level** branches (atomic across all sub-tables), lazy fork, system branch filtering |
| Atomic single-dataset commits | ✅ | **Atomic multi-dataset publish** via `__manifest` + `ManifestBatchPublisher`. Engine-internal write APIs sit behind a sealed `TableStorage` trait (MR-793) — `stage_*` + `commit_staged` are the only paths to advance Lance HEAD; inline-commit Lance APIs are not reachable through the trait surface |
| Compaction (`compact_files`) | ✅ | `omnigraph optimize` orchestrates over all node/edge tables, bounded concurrency |
| Cleanup (`cleanup_old_versions`) | ✅ | `omnigraph cleanup` with `--keep` / `--older-than` policy |
| BTREE / inverted (FTS) / vector indexes | ✅ | `ensure_indices` builds them on every relevant column; idempotent; lazy across branches |
| `merge_insert` upsert | ✅ | `LoadMode::Merge`, mutation `update`/`insert`/`delete` lowering |
| Vector search | ✅ | `nearest()` query op; embedding pipeline (Gemini / OpenAI clients); `@embed` in schema |
| Full-text search | ✅ | `search/fuzzy/match_text/bm25` query ops |
| Hybrid ranking | — | `rrf(...)` Reciprocal Rank Fusion in one runtime |
| Graph traversal | — | CSR/CSC topology index, `Expand` IR op, variable-length hops, `not { }` anti-join |
| Schema language | — | `.pg` + Pest grammar + catalog + interfaces + constraints + annotations |
| Query language | — | `.gq` + Pest grammar + IR + lowering + linter |
| Schema migration planning | — | `plan_schema_migration` + `apply_schema` step types + `__schema_apply_lock__` |
| Commit graph (DAG) across whole repo | — | `_graph_commits.lance` with linear + merge parents, ULID ids, actor map |
| Per-query atomic writes | — | In-memory `MutationStaging.pending` accumulator + `stage_*` / `commit_staged` per touched table at end-of-query + publisher CAS via `commit_with_expected` (single manifest commit per `mutate_as` / `load`); D₂ parse-time rule keeps inserts/updates and deletes from mixing |
| Three-way row-level merge | — | `OrderedTableCursor` + `StagedTableWriter`, structured `MergeConflictKind` |
| Change feeds | — | `diff_between` / `diff_commits` with manifest fast path + ID streaming |
| Cedar policy | — | 8 actions, branch / target_branch / protected scopes, validate/test/explain CLI |
| HTTP server | — | Axum, OpenAPI via utoipa, bearer auth (SHA-256, AWS Secrets Manager option), policy gating, NDJSON streaming export |
| CLI with config | — | `omnigraph.yaml`, aliases, multi-format output (json/jsonl/csv/kv/table) |
| Audit / actor tracking | — | `_as` write APIs + actor map in commit graph |
| Local RustFS bootstrap | — | `scripts/local-rustfs-bootstrap.sh` one-shot S3-backed dev environment |

---

## Maintenance contract for agents

When you change something user-visible, **update the relevant `docs/<area>.md` in the same change**. Pointers from this file to that doc must keep working — CI enforces cross-link integrity via `scripts/check-agents-md.sh`.

When proposing or reviewing a non-trivial change, walk [docs/invariants.md](docs/invariants.md) — at minimum the §IX deny-list and §X review checklist. Add to the deny-list when a new anti-pattern surfaces; relaxing an invariant requires the same review process as code.

Rules:

1. **Update in the same PR.** New endpoint, query function, CLI flag, env var, constant, schema construct, or invariant: update both the source code and the doc in the same change. Never split documentation drift into a follow-up.
2. **Bump version on release.** When a release boundary crosses (e.g. v0.3.1 → v0.3.2), update the version line at the top of this file and add a `docs/releases/<version>.md` describing the user-visible delta. Update [docs/architecture.md](docs/architecture.md) only if the architecture itself changed.
3. **Don't lie.** If a section becomes wrong but you can't rewrite it fully right now, replace the wrong line with `*(stale — needs update after <change>)*` rather than leaving silently incorrect text. Then fix it ASAP.
4. **Re-verify before recommending.** If you cite a flag, env var, endpoint, or constant to the user or in code, grep for it in source first. Memory and docs go stale; the code is authoritative.
5. **Keep AGENTS.md a map, not an encyclopedia.** New deep content goes into `docs/`. Add an entry to "Where to find each topic" instead of pasting prose into this file. The "Always-on rules" section is the exception — it's for invariants that should always be in scope.
6. **Re-read on schema/query/IR changes.** Edits to `schema.pest`, `query.pest`, `ir/lower.rs`, `query/typecheck.rs`, or `query/lint.rs` should trigger a re-read of [docs/schema-language.md](docs/schema-language.md), [docs/query-language.md](docs/query-language.md), and [docs/execution.md](docs/execution.md) to confirm they still describe reality.

CI check: `scripts/check-agents-md.sh` verifies that every `docs/*.md` link in this file resolves and that every doc in the canonical set is linked. Run it locally before opening a PR if you've moved or renamed docs.
