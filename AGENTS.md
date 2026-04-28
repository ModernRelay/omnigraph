# OmniGraph — Agent Guide

This file is the always-on map for AI coding agents (Claude Code, Codex, Cursor, Cline) working in this repo. It is loaded into context on every turn, so it stays as a **map plus the rules and invariants that need to be in scope at all times** — the encyclopedia content lives under [`docs/`](docs/). When you need depth, follow a pointer.

`CLAUDE.md` is a symlink to this file — there is exactly one source of truth. Edit `AGENTS.md`.

**Version surveyed:** 0.3.1
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
- **Transactional runs**: ephemeral `__run__<id>` branches for isolated mutation, fast-path or merge-path publish.
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
| Architecture, L1/L2 framing, concurrency model | [docs/architecture.md](docs/architecture.md) |
| Storage layout, `__manifest` schema, URI schemes, S3 env vars | [docs/storage.md](docs/storage.md) |
| `.pg` schema language, types, constraints, annotations, migration planning | [docs/schema-language.md](docs/schema-language.md) |
| `.gq` query language, MATCH/RETURN/ORDER, search funcs, mutations, IR ops, lint codes | [docs/query-language.md](docs/query-language.md) |
| Indexes (BTREE / inverted / vector / graph topology) | [docs/indexes.md](docs/indexes.md) |
| Embeddings (compiler + engine clients, env vars, `@embed`) | [docs/embeddings.md](docs/embeddings.md) |
| Branches, commit graph, snapshots, system branches | [docs/branches-commits.md](docs/branches-commits.md) |
| Runs (transactional graph mutations, `__run__<id>`, publish paths) | [docs/runs.md](docs/runs.md) |
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

## Always-on rules (load these into your working memory)

These invariants need to be in scope on every change — they're the ones that quietly break if forgotten.

1. **`__manifest` is the atomic-publish boundary.** Multi-dataset commits flip via a single `ManifestBatchPublisher` write. Don't introduce code paths that publish per sub-table outside the batch publisher — you'll lose snapshot isolation across tables.
2. **`nearest($x.vec, $q)` requires a `LIMIT`.** The compiler enforces it, but if you're touching the query lowering or executor, don't break this rule. ANN without a limit is unbounded.
3. **Snapshot isolation per query.** A query holds one `Snapshot` for its lifetime. Don't read against `db.head()` mid-query; use the snapshot bound at lowering time.
4. **Run isolation lives on `__run__<id>` branches.** Mutations inside `begin_run` … `publish_run` must go through `run_branch`, not `target_branch`. Publish picks fast-path (target unmoved) or merge-path (three-way).
5. **Schema apply is serialized via `__schema_apply_lock__`.** Concurrent `apply_schema` is not safe. Don't bypass the lock.
6. **`branch_list()` filters internal branches.** `__run__…` and `__schema_apply_lock__` must not appear in user-visible listings, exports, or policy-scoped operations. If you add a new system branch, follow the `__name__` prefix convention and add it to the filter.
7. **Bearer-token plaintext never persists in process memory.** Tokens are SHA-256 hashed at startup; comparison uses `subtle::ConstantTimeEq`. The actor id is server-resolved from the hash match — it must not be settable by the client.
8. **Mutations are atomic at the manifest commit boundary.** Multi-statement `change` queries publish one commit. Don't commit per-statement.
9. **Indexes are built on the branch head, not on a snapshot.** Reads always see the current index state. Lazy fork: a branch that hasn't mutated a sub-table reuses the source's index until the first write.
10. **Stable type IDs survive renames.** Schema migration uses `stable_type_id` (kind+name hashed at first sight). Don't mint new IDs on rename.

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
| Atomic single-dataset commits | ✅ | **Atomic multi-dataset publish** via `__manifest` + `ManifestBatchPublisher` |
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
| Transactional runs | — | `_graph_runs.lance`, `__run__<id>` ephemeral branches, fast-path & merge-path publish |
| Three-way row-level merge | — | `OrderedTableCursor` + `StagedTableWriter`, structured `MergeConflictKind` |
| Change feeds | — | `diff_between` / `diff_commits` with manifest fast path + ID streaming |
| Cedar policy | — | 10 actions, branch / target_branch / protected scopes, validate/test/explain CLI |
| HTTP server | — | Axum, OpenAPI via utoipa, bearer auth (SHA-256, AWS Secrets Manager option), policy gating, NDJSON streaming export |
| CLI with config | — | `omnigraph.yaml`, aliases, multi-format output (json/jsonl/csv/kv/table) |
| Audit / actor tracking | — | `_as` write APIs + actor maps in commit & run datasets |
| Local RustFS bootstrap | — | `scripts/local-rustfs-bootstrap.sh` one-shot S3-backed dev environment |

---

## Maintenance contract for agents

When you change something user-visible, **update the relevant `docs/<area>.md` in the same change**. Pointers from this file to that doc must keep working — CI enforces cross-link integrity via `scripts/check-agents-md.sh`.

Rules:

1. **Update in the same PR.** New endpoint, query function, CLI flag, env var, constant, schema construct, or invariant: update both the source code and the doc in the same change. Never split documentation drift into a follow-up.
2. **Bump version on release.** When a release boundary crosses (e.g. v0.3.1 → v0.3.2), update the version line at the top of this file and add a `docs/releases/<version>.md` describing the user-visible delta. Update [docs/architecture.md](docs/architecture.md) only if the architecture itself changed.
3. **Don't lie.** If a section becomes wrong but you can't rewrite it fully right now, replace the wrong line with `*(stale — needs update after <change>)*` rather than leaving silently incorrect text. Then fix it ASAP.
4. **Re-verify before recommending.** If you cite a flag, env var, endpoint, or constant to the user or in code, grep for it in source first. Memory and docs go stale; the code is authoritative.
5. **Keep AGENTS.md a map, not an encyclopedia.** New deep content goes into `docs/`. Add an entry to "Where to find each topic" instead of pasting prose into this file. The "Always-on rules" section is the exception — it's for invariants that should always be in scope.
6. **Re-read on schema/query/IR changes.** Edits to `schema.pest`, `query.pest`, `ir/lower.rs`, `query/typecheck.rs`, or `query/lint.rs` should trigger a re-read of [docs/schema-language.md](docs/schema-language.md), [docs/query-language.md](docs/query-language.md), and [docs/execution.md](docs/execution.md) to confirm they still describe reality.

CI check: `scripts/check-agents-md.sh` verifies that every `docs/*.md` link in this file resolves and that every doc in the canonical set is linked. Run it locally before opening a PR if you've moved or renamed docs.
