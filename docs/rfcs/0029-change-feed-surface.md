---
type: spec
title: "RFC-029 — Bounded change-feed surface (HTTP + CLI)"
description: Exposes the existing engine diff/change-feed as a bounded, tiered HTTP and CLI surface, and closes the unbounded materialization, missing projection, and gated-summary defects that currently make it unsafe to expose.
status: draft
tags: [eng, rfc, changes, diff, http, cli, branching, omnigraph]
timestamp: 2026-07-26
owner: OmniGraph maintainers
---

# RFC-029: Bounded change-feed surface (HTTP + CLI)

**Status:** Draft
**Date:** 2026-07-26
**Author track:** Maintainer design series
**Depends on:** [RFC-028](0028-stable-schema-identity.md)'s stable table identity
(the diff pairs tables by `(stable_table_id, table_incarnation_id)`, which is
what makes it schema-drift-safe)
**Surveyed:** OmniGraph 0.9.0 (`main`); Lance 9.0.0 stable
**Audience:** engine, server, CLI, and docs maintainers

---

## 0. Decision summary

The engine has a complete change-detection capability that no consumer can
reach. This RFC exposes it as a **tiered, mandatorily-bounded** read surface —
`GET /graphs/{id}/diff/summary`, `GET /graphs/{id}/diff`, and `omnigraph diff` —
and first closes three defects that make the current implementation unsafe to
expose.

It deliberately does **not** add diff to the `.gq` query language. That decision
and its reversal condition are recorded in §4.

## 1. Motivation

`Omnigraph::diff_between` (`crates/omnigraph/src/db/omnigraph.rs:2228`) and
`diff_commits` (`:2248`) resolve read targets and delegate to
`changes::diff_snapshots`. The types are already shaped for a review UI:

```
ChangeOp: Insert | Update | Delete
EntityKind: Node | Edge
EntityChange { table_key, kind, type_name, id, op, manifest_version, endpoints? }
ChangeFilter { kinds?, type_names?, ops? }
ChangeSet { from_version, to_version, branch, changes[], stats }
```

**Nothing outside engine tests calls any of it.** There is no HTTP route (19
paths in `openapi.json`, none diff-related), no CLI command (`omnigraph change`
is a visible alias for `mutate`), no wire DTOs, and no `parity_matrix` entry.

The motivating consumer is branch review: a UI can show branch *names* and
commit *ids*, but never contents-at-a-glance.

## 2. Defects that block naive exposure

### 2.1 Unbounded materialization

`ChangeSet.changes` is a fully-materialized `Vec` with no limit or cursor
anywhere in `changes/mod.rs`. A diff of a large branch returns every change row
in one response.

### 2.2 No projection; signatures retain every column

`diff_table_cross_branch` (`changes/mod.rs:314`) calls:

```rust
storage.scan(ds, None, None, Some(ordering))   // projection=None, filter=None
```

— all columns, all rows, **both** snapshots — then `extract_rows_with_signature`
(`:544`) stringifies every non-`_row_` column into a `signature: String` via
`array_value_to_string`. The module has zero mentions of `Vector` or `Blob`, so
embedding and blob payloads are stringified and retained per row.

Two aggravating factors:

- `same_lineage` is `f.table_branch == t.table_branch` (`:216`). For a
  `main` vs `feature` review diff, every table the branch **actually wrote** has
  a different `table_branch` and therefore takes this path — precisely the
  tables a review UI wants.
- `diff_table_added` (`:402`) and `diff_table_removed` (`:420`) also call
  `scan_all_rows_ordered`, computing full signatures that they then **discard
  entirely** — every row is unconditionally an insert or a delete. This is pure
  waste with no semantic purpose.

### 2.3 Summary is gated behind the expensive path

`compute_stats(&changes)` (`:223`) folds the already-materialized vector, so the
cheap "roughly what changed" question costs a full diff. This is backwards from
every comparable system.

Exposing 2.1–2.3 over HTTP would hit invariant 13 and the deny-list item
*"Silent failures — OOM, timeout, partial result must all be surfaced and
bounded."*

## 3. Design

### 3.1 Tiers

| Tier | Cost | Consumer use |
|---|---|---|
| **Summary** — counts + types affected | must not materialize rows | review landing view |
| **Rows** — paged `EntityChange` list | bounded by `limit` + `cursor` | drill-down |
| **Enrichment** — property values | existing `entity_at_target` | row detail |

`EntityChange` carries identity, op, and endpoints — **no property values** —
and `entity_at` is documented as "on-demand enrichment". That existing split is
correct and is retained: **diff returns identity; `.gq` reads values.**

### 3.2 Bounds are mandatory

`from` and `to` are required parameters. `limit` carries a server-enforced
ceiling. `cursor` is keyset-based on the diff's existing deterministic ordering
(`table_key`, then `id`), matching the stance already documented on
`server_commit_list`: *"a future `cursor`/`limit` pagination will be
keyset-based on that same order."*

This follows the prior art without exception (§5): no comparable system exposes
an unbounded whole-dataset diff.

### 3.3 Signature becomes a fixed-width digest

`ScannedRow.signature` changes from `String` to a fixed-width digest. Per row,
each non-`_row_` column is folded into one SHA-256 hasher (`sha2` is already a
direct engine dependency), truncated to 128 bits:

- `DataType::FixedSizeList(..)` (Vector) and `DataType::LargeBinary` (Blob) hash
  their **raw value bytes**, skipping string formatting entirely.
- Every other type hashes the bytes of its `array_value_to_string` rendering,
  preserving today's comparison semantics exactly.

This bounds retained per-row memory to 16 bytes regardless of schema width — a
strict superset of special-casing vector/blob, since a wide text column stops
being retained too. The Arrow mapping is fixed by `ScalarType::to_arrow()`
(`omnigraph-compiler/src/types.rs:55–68`), so **no catalog capture is needed**.

Keeping this below the catalog is deliberate: `diff_snapshots` pairs tables by
`entry.identity` with the comment *"Logical pairing never depends on either
name"*, which is what makes the diff safe across schema drift. Introducing a
typed catalog dependency here would forfeit that property.

**Semantics are preserved, not narrowed.** An embedding-only or blob-only change
is still reported as `Update`. Dropping those columns instead would have been
cheaper but would silently lose changes — rejected.

### 3.4 Added/removed tables stop computing signatures

`diff_table_added` / `diff_table_removed` switch to an identity-only projection
(`id`, plus `src`/`dst` for edges). No behavior change; removes the waste in
§2.2.

### 3.5 Authorization

Two independent `PolicyAction::Read` checks, one per side, each with
`branch: Some(side)`.

Note the constraint that forces this shape:
`PolicyAction::uses_target_branch_scope()`
(`crates/omnigraph-policy/src/lib.rs:96`) is
`BranchCreate | SchemaApply | BranchDelete | BranchMerge` — **`Read` is not in
it** — so a `target_branch` rule on `read` is rejected by `validate()` (`:355`).
Reusing the two-sided `branch`/`target_branch` shape from `server_branch_merge`
would therefore fail policy validation. Two `Read` calls correctly require
access to both sides and need **zero policy-vocabulary changes**.

### 3.6 Route naming

`GET /diff/summary` and `GET /diff`. **Not** `/changes`: `POST /change` already
exists as the deprecated alias for `mutate`, and the near-collision between a
mutation endpoint and a diff endpoint would be actively misleading.

## 4. Why this is a route, not `.gq`

Invariant 10 says query semantics belong in the IR, which argues for a query
source. Two facts override it:

1. **`.gq` has no read-target vocabulary.** The grammar
   (`crates/omnigraph-compiler/src/query/query.pest`, 117 lines) has zero
   mentions of branch, snapshot, version, or as-of. Targets are request-level
   (`ReadRequest.branch` / `.snapshot`). Diff needs *two* targets, so this
   starts with inventing target addressing in the grammar.
2. **Snapshot binds per query, not per source.** `capture_read_view`
   (`crates/omnigraph/src/exec/query.rs:38–41`) returns `(resolved, catalog)` as
   a pair under the schema-publication gate, and invariant 3 requires one
   coherent accepted view per operation. Diff is inherently two-snapshot.

SQL systems expose diff as table functions cheaply because `AS OF` binds **per
table reference**; Datomic because time binds **per database value**. OmniGraph
binds per query. That is the actual gap.

**Reversal condition.** The unified `Source` operator — already roadmap
(`docs/dev/invariants.md:483`, `docs/dev/canon.md:782`) — introduces per-source
binding. If and when it lands (motivated by policy pushdown and imports, where
all sources share one snapshot and one catalog), diff becomes a natural `Source`
and this route can delegate to it. Nothing in this RFC forecloses that: the tier
split, the mandatory bounds, and the identity-only payload are all preserved
under that model.

Explicitly rejected: a `diff: {from, to}` field on `POST /query`. That is the
deny-listed *"side-channels for query semantics… transport flags"* and would
smuggle a two-snapshot execution model past the typechecker and planner.

## 5. Prior art

- **Dolt** — tiered and mandatory-bounded. `dolt_diff_summary` / `dolt_diff_stat`
  (cheap) versus `dolt_diff_<table>` / `DOLT_DIFF()` (row-level, requires both
  `from_commit` and `to_commit`). `diff_type` ∈ `added`/`modified`/`removed`.
  Schema drift is handled by deriving the diff relation's columns from **both**
  revisions as `from_X`/`to_Y` pairs. Shipped `--skinny`/`--include-cols` in
  2025 to stop emitting every column — independent convergence on §2.2.
- **Delta Lake** — `table_changes(...)` with reserved `_change_type`,
  `_commit_version`, `_commit_timestamp`; **opt-in** via
  `delta.enableChangeDataFeed` because materializing change data is not free.
- **Datomic** — no diff API. Narrows the database value (`d/since`, `d/history`,
  `tx-range`) and reuses Datalog. Viable because its schema is universal and
  append-only, which OmniGraph's typed catalog with drop/re-add incarnations is
  not.
- **Git** — tiered detail (`--stat`, `--name-only`, full patch), bounded by
  pathspec.

## 6. Evidence

Per invariant 14, evidence at the boundary that changed:

- **Engine** — `crates/omnigraph/tests/changes.rs` extended (not duplicated) for
  limit/cursor, digest semantics including an embedding-only update, and
  summary-without-materialization.
- **Cost** — a budget in `warm_read_cost.rs` on the shared `helpers::cost`
  harness asserting diff work does not scale with untouched-table count.
- **Server** — `data_routes.rs`, including a 403 proving both sides are gated.
- **Parity** — `parity_matrix.rs` gains `diff` so embedded and remote arms are
  compared.

## 7. Out of scope

Streaming NDJSON diff (the `/export` pattern) — the paged tiers cover the review
use case; add later if a bulk consumer appears. Diff-as-`Source` (§4). Schema
diff, which `schema plan` already owns.
