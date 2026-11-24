# Edge keys

**Decides:** an edge type may declare `@key(@src, @dst, ...)`, and a keyed edge's id is the canonical encoding of its key, so the same logical edge written on two branches is one entity that the merge converges or conflicts on.
**Contract:** keeps E3, write:2, write:5, merge:1, merge:2, schema:3
**Code:** `crates/omnigraph-compiler/src/schema/parser.rs`, `crates/omnigraph-compiler/src/catalog/schema_ir.rs`, `crates/omnigraph-compiler/src/catalog/schema_plan.rs`, `crates/omnigraph/src/exec/mutation.rs`, `crates/omnigraph/src/loader/mod.rs`, `crates/omnigraph/tests/integration/branching.rs`, `crates/omnigraph/tests/integration/validators.rs`, `crates/omnigraph/tests/integration/merge_truth_table.rs`
**Cited as:** RFC 0044

## Context

Every unkeyed edge insert mints a fresh ULID. The three-way merge walks rows
by id, so the same relationship inserted on both sides of a fork never meets
in the walk, and the merge keeps two rows for one fact. Traversal hides the
duplicate, but counts, aggregates, bound-edge traversals and exports see it.
Nodes avoid this because `@key` derives their id. Edges had no way to
declare identity, so the duplicate was a structural outcome, not a merge bug.

## Decision

- An edge body may hold one `@key` group. It must include both `@src` and
  `@dst`, and may add scalar properties that follow the node key-column
  rules (non-null scalars; no list, Blob or vector). A type-level or
  property-level `@key` on an edge is refused.
- A keyed edge's id is the canonical key encoding nodes use
  (`canonical_key_id`): a JSON array of per-column canonical strings,
  endpoints first, then scalar members by stable property identity. Ids
  derive from values, never from property names, so a rename keeps them.
- Every id source derives. A mutation insert into a keyed edge type stages
  an upsert; a load derives the id after endpoint resolution and refuses a
  supplied id unless it equals the derivation exactly. A null key column is
  refused.
- The merge is unchanged: identical rows under one derived id converge,
  divergent ones report `divergent_insert` with the derived id.
- The key is emitted as the unique constraint with `is_key: true`, so a
  `@unique` group over the same column set is subsumed by identity and
  needs no committed lookup.
- A key is declared only when the edge type is created. Adding, removing or
  changing a key on an existing type is refused by schema plan; a migration
  that rewrites ids is out of scope.
- A schema with an edge key carries the `edge-keys` feature in its accepted
  IR, so a release without the feature refuses the graph instead of
  minting ULIDs on keyed tables. Removing the last edge key drops the
  feature. The feature-set scheme itself belongs to the system column
  namespace decision.
- Unkeyed edge types keep multiset semantics: fresh ULIDs, parallel edges,
  and keep-both on merge.
- Identity lives in the id, not in a side record, and Lance sees only an
  ordinary `Utf8` id.

## Rejected

- **Document `@unique(@src, @dst)` and stop:** it only refuses where
  convergence is correct, pays a committed lookup on every write and merge,
  and is opt-in, so the obvious schema falls into the duplicate.
- **Collapse bit-identical edges at merge time without a declaration:** it
  guesses intent for multiset types, where identical parallel edges are
  legitimate, and makes a merge stricter than the same inserts on one branch.
- **A post-merge pass that removes duplicates:** cleans derived state for a
  fact construction can prevent, and serves wrong counts in between.
- **Hash the key into the id:** node keys already use the plaintext
  encoding, and one encoding keeps export, import and debugging symmetric.
  Key values therefore appear in ids, conflicts and logs, as for nodes.
- **Key every edge type by its endpoints by default:** breaks every existing
  graph and every parallel-edge schema; it can be revisited on this
  mechanism.
