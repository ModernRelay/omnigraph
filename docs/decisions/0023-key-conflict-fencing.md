# Key-conflict fencing

**Decides:** every insertion into a keyed graph table carries Lance's exact-id key-existence filter, so two concurrent writes that insert one id cannot both commit.
**Contract:** keeps E3, E4, E5, write:2, write:5
**Code:** `crates/omnigraph/src/table_store.rs` (`stage_keyed_write`, `INSERT_ABSENCE_PROPERTY`), `crates/omnigraph/src/storage_layer.rs`, `crates/omnigraph/src/exec/staging.rs`, `crates/omnigraph/src/exec/merge.rs`, `crates/omnigraph-catalog/src/lib.rs`, `crates/omnigraph/tests/forbidden_apis.rs`, `crates/omnigraph/tests/lance_surface_guards.rs`
**Cited as:** RFC 0023

## Context

Two writers can read a base where id K is absent, stage disjoint fragments
that hold K, and both commit: each caller gets success and the table holds a
duplicate id. Every later upsert, edge lookup and merge then works on a broken
identity. Lance has the primitive: an `Update` that carries a
`KeyExistenceFilter` over the unenforced primary key conflicts with another
that inserted the same key. Lance's compatibility check is directional: a bare
`Append` or an unfiltered `Update` that commits second after a filtered one
succeeds. The filter therefore protects only if no keyed write omits it.

## Decision

- A node or edge table's primary key is its system id column (`__id`, or `id`
  on a graph with the legacy spellings), non-null Utf8, set as Lance's
  unenforced primary key when the table is created. An edge's `src` and `dst`
  are ordinary columns.
- Every keyed stage checks that the pinned dataset declares exactly that key.
  Overwrite, rename and schema rewrite keep its field id and metadata.
- No production route stages a bare `Append` on a keyed table. `stage_append`
  compiles for tests only, and `forbidden_apis.rs` pins it.
- Strict insert probes the pinned base for every id, then stages a pure
  insertion `Update` that carries the exact-id filter. Upsert uses Lance's v2
  merge-insert with `use_index(false)`, because the indexed path attaches no
  filter. The adapter refuses a staged transaction whose filter is not over
  exactly the id field.
- A strict insert never becomes an upsert under contention. An id present at
  the base, or a same-id conflict a fresh probe confirms, is `KeyConflict`. A
  substrate conflict with no visible id prepares again in strict mode.
- A mutation or load stages one transaction per keyed table and refuses more
  than 8,192 rows or the session write budget (32 MiB by default) with
  `ResourceLimitExceeded` before any effect. A branch merge chains chunks under
  the same caps, at most 1,024 data transactions per table.
- A transaction may carry `omnigraph.insert_absence = v1`: every key in its
  filter was proven absent from its parent. A merge skips the target join only
  when every transaction in the source interval carries it and passes the
  structural history check; its rows still commit as a filtered `Update`.
- Fencing covers same-id insertion only. `@unique` on different ids, edge
  endpoints and cardinality rely on authority revalidation
  ([Unified graph-write protocol](0022-unified-write-path.md)).

## Rejected

- **Edge key on (`src`, `dst`):** an edge's identity is its id; moving an
  endpoint updates the row, and unkeyed parallel edges are allowed.
- **Annotating tables inside an older-format graph:** an older binary can still
  take an unfiltered route, and the directional check lets it commit second.
- **Fast-forward `Append` for merges of new rows:** "every row is new" is
  inferred from a snapshot that a concurrent same-id writer can invalidate.
- **One merge-insert transaction per merge delta:** Lance builds that join in
  an unbounded memory pool, so no memory bound could be claimed.
- **A lock table or custom transaction manager:** Lance already owns the
  conflict primitive (invariant 1).
