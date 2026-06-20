# RFC: Node Identity — Surrogate Reference Key, Mutable Natural Key, Resolve-on-Write Edges

**Status:** Proposed (design validated against the engine code, 2026-06-20)
**Date:** 2026-06-19
**Tickets:** iss-gq-expose-system-id-column (expose the system id column in GQ — the keystone), iss-lint-edge-endpoint-requires-key (author-time lint for keyless edge endpoints), iss-714 / MR-714 (cross-batch + cross-version uniqueness for `@key`/`@unique`), dec-918 / MR-918 (ADR — stable row IDs, accepted; this RFC defines how the logical layer sits above it)
**Target release:** pre-1.0 contract change to identity semantics; phased (see Rollout). Warrants a `docs/releases/` note and an ADR on merge because it changes a documented contract (`@key` immutability, edge-resolution semantics, merge-matching key).
**Decisions (confirmed):** (1) keep the `@key` annotation and redefine its semantics — no new `@id`/`@natural` split. (2) Implement in scope order: PR 1 (expose `id` + reserve the name) and PR 2 (cross-version uniqueness) first; defer the mutable-`@key` loosening (PR 3–6) to a follow-up.

## Summary

Today OmniGraph fuses three identity roles into one value. A node's `@key` property *is* its `id` column, *is* the string an edge stores in `src`/`dst`, *is* the key three-way merge matches rows by. Because edges hold that string directly with no indirection, `@key` has to be immutable, and the mutation path enforces this (`crates/omnigraph/src/exec/mutation.rs:1104`: "cannot update @key property — delete and re-insert instead"). Users experience this as "my primary key is useless because I can't rename it."

This RFC decomposes identity into two layers, the design every mature graph and versioned database converges on:

1. An **immutable surrogate `id`** that is the sole edge-resolution and merge-matching key, derived from the initial `@key` value and then frozen (a ULID for keyless nodes, exactly as today).
2. A **mutable natural key** (`@key` / `@unique`) that is a freely-updatable unique attribute, addressable in queries and edge authoring, resolved to the surrogate at write time.

The surrogate is **derived-then-frozen**, not random-per-write. This is the load-bearing choice: it makes independent inserts of the same natural key on two branches converge to one surrogate, so three-way merge unifies them instead of silently duplicating. Under this choice the change requires **zero data migration** of existing graphs, because their `id` columns and edge endpoints already hold exactly the frozen-surrogate value.

## Motivation

The root flaw is one value carrying three jobs with no indirection between them:

- **Reference identity** — what edges resolve against, so it must be stable.
- **Entity identity** — what makes "the same node" across independent branches, so merge can match it.
- **Natural key** — the human-meaningful value a user wants to be able to change.

Edges store the raw `id` string and resolve by string membership against the `id` column (`crates/omnigraph/src/loader/mod.rs:507`). Because the reference job pins the value immutable, the natural-key job dies: a rename would orphan every edge pointing at the old value, and there is no cascade and no surrogate to fall back on. So the engine forbids the rename outright. The immutability is forced by the fusion, it is not a deliberate feature.

Three already-tracked tickets are facets of this one gap. iss-gq-expose-system-id-column wants `$node.id` projectable so the auto-generated id becomes a real referenceable key ("the Mongo `_id` model"). iss-lint-edge-endpoint-requires-key wants an author-time warning when a node used as an edge endpoint has no `@key` and is therefore unreferenceable. iss-714 wants `@key`/`@unique` uniqueness enforced beyond a single batch. They resolve cleanly once identity is decomposed.

## Validated facts (external databases)

This design was checked against Lance, LanceDB, HelixDB, Dolt, Datomic, Neo4j, Dgraph, ArangoDB, TigerGraph, and MongoDB. The findings:

- **The two-layer split is the dominant pattern.** Neo4j (relationships target `elementId`, application keys live under uniqueness constraints), Datomic (refs store immutable entity ids, `:db.unique/identity` + lookup refs address by domain value), Dgraph (edges point at `uid`, the `xid` upsert pattern resolves external keys), and HelixDB (edges store the system `u64` `ID`, `UNIQUE INDEX` is a separate lookup key) all separate an immutable reference target from a separate unique lookup key. HelixDB is the closest analog: a graph-plus-vector engine that gives every node an implicit immutable `ID` and treats the human key as a distinct `UNIQUE INDEX`.

- **A clean rule explains the user's pain:** *the mutability of the user-facing key equals the absence of edges pointing at it.* The databases that store the user key in the edge (ArangoDB `_from`/`_to` hold `collection/_key`; TigerGraph's write-once primary id; MongoDB refs hold `_id`) all freeze the user key. OmniGraph today is the ArangoDB regime: `id` is built from `@key`, they are welded, both freeze. The databases that interpose a surrogate (Neo4j, Datomic, Dgraph, HelixDB) all permit a mutable user key.

- **Edges resolve user keys to the surrogate at write time** everywhere it is possible to rename: Neo4j `MERGE`, Datomic lookup refs, Dgraph upsert blocks. This is the mechanism we adopt.

- **Primary keys are immutable because foreign keys have no indirection.** Postgres `ON UPDATE CASCADE` exists precisely to copy a changed key into referencing rows; the cascade is the cost of the missing indirection. The surrogate is the indirection.

- **The merge constraint forces derive-then-freeze, and the alternative is a silent-duplication bug.** Dolt merges strictly by primary key and keeps no hidden row identity. Its own guidance to use random UUID keys avoids *false collisions* but, read fully, is a cautionary tale: random per-branch ids unify *nothing*, so the same real entity inserted on two branches becomes two rows after merge, a silent correctness bug rather than a loud conflict. The fix Dolt's keyless content-addressing, Datomic's `:db.unique/identity` upsert, and deterministic UUIDv5 all embody is a **content-derived-then-frozen** id: two branches that independently derive the same id from the same natural key collide and unify on merge.

- **Lance fixes the substrate boundary.** Lance stable row ids are a per-dataset manifest counter (`next_row_id`), not content-derived, not portable across datasets or branches, and Lance has no branch-merge primitive at all. The unenforced primary key is immutable once set and uniqueness is enforced only at merge-insert time. So the logical edge reference must stay a reproducible **string** key; a Lance stable row id can only ever be an in-dataset physical accelerator. This is the dec-918 boundary: stable row ids accelerate traversal, they are never the logical edge value.

The convergent lesson: a separate immutable surrogate is the price of a mutable user-facing key, it is the well-trodden design, and for a branch/merge engine the surrogate must be derived-then-frozen rather than random.

## Validated against the engine code (2026-06-20)

Every load-bearing assumption was checked against the current tree. The findings de-risk the design and reshape the rollout:

- **`id` is already a separate physical Arrow column**, distinct from the `@key` property column, on both nodes (`crates/omnigraph-compiler/src/catalog/mod.rs:219-228`, schema = `[id: Utf8, ...props]`) and edges (`:273-277`, `[id, src, dst, ...props]`). Decoupling `id` from `@key` needs no new column. The zero-migration claim holds: for existing data `id` == the `@key` value, and edge `src`/`dst` already hold that same string.
- **Three-way merge already matches rows by the `id` column** (`crates/omnigraph/src/exec/merge.rs:459-483`, `min_cursor_id`; conflicts classified by full-row signature at `:581-622`), and the update path already keys its Merge stage on `["id"]` (`exec/staging.rs:381`). So "merge matches by the surrogate" and "id stays frozen on update" are **already the behavior**, not new code: `apply_assignments` (`mutation.rs:1191`) writes only assigned columns, so a rename moves the key column and leaves `id` untouched.
- **`$node.id` is rejected only by a typecheck gate** (`crates/omnigraph-compiler/src/query/typecheck.rs:950`, `id` absent from `NodeType.properties`); projection already resolves `{var}.id` (`exec/projection.rs:368`) and filters lower to the `id` column. Exposure is a one-spot special-case, not an addition of `id` to `properties` (which would pollute the insert / required-field / `arrow_schema` loops that all assume properties exclude `id`).
- **Uniqueness is intra-batch only today** (`loader/mod.rs:1348-1387` via `composite_unique_key` at `:1405`). Cross-version enforcement against committed rows is the one genuinely new mechanism and the hard prerequisite for a mutable `@key` (iss-714).
- **Correction to an earlier draft:** there is **no camelCase property-name normalization** in the scan path (only edge *type names* are case-folded, `catalog/mod.rs:109-118`). The iss-990 adjacency note was inaccurate; the only `id`-collision concern is a user property literally named `id`.
- **A user property named `id` crashes at init today.** `catalog/mod.rs:220` injects `Field::new("id", Utf8)` and `:221` then pushes a field per declared property, so a schema declaring a property named `id` produces two `id` fields and Lance aborts dataset creation with a duplicate-field error. There is no guard. Reserving `id` (PR 1) turns that opaque Lance crash into a clear author-time schema error, and is required anyway because exposing `$node.id` makes a user `id` property ambiguous.

## Design

### Identity decomposition

| Role | Today (fused) | This RFC |
|---|---|---|
| Reference identity (edge `src`/`dst` target) | `@key` string (or ULID) | immutable surrogate `id` |
| Entity identity (merge matching key) | `@key` string (or ULID) | immutable surrogate `id` |
| Natural key (human lookup, addressable) | `@key`, immutable | `@key` / `@unique`, **mutable** unique attribute |

### The surrogate `id`

- Always present, non-nullable, immutable for the life of the node. This is the only value edges store and the only value merge matches by.
- **Derived-then-frozen.** At first insert the surrogate is set to the node's initial `@key` value (for keyless nodes, a generated ULID, exactly as today). It is frozen from that point: a later `@key` rename does not touch it.
- Exposed in GQ for projection, filtering, and edge authoring (iss-gq-expose-system-id-column).
- The immutability guard that sits on `@key` today moves here, onto `id`, which is never user-settable after insert.

Deriving the surrogate from the initial `@key` (rather than minting an independent random ULID) is what gives merge convergence and a zero-cost data migration in one stroke. The same physical `id` column that exists today already holds these values.

**Seeding is derive-once-then-resolve.** The `id = @key` derivation already happens at insert (`build_node_batch`, `execute_insert`) and stays. The freeze has two halves. On `update` it is automatic: `apply_assignments` never writes `id`, so a rename moves only the key column. On a later `load`/`merge` that references a renamed node by its *current* key, the insert-time derivation must become **resolve-or-seed**: look up an existing node by key (or id), reuse its frozen `id` if found, and seed a fresh `id = key` only when absent. Blindly re-deriving `id = key` would mint a new surrogate and duplicate the renamed node. That lookup is the same indexed key→id primitive cross-version uniqueness builds. Cross-branch merge unification holds only for keyed nodes; keyless nodes seed a random ULID and do not unify.

### The natural key

- `@key` (and `@unique`) become ordinary mutable unique attributes. The parse-time syntax of `.pg` is unchanged; `slug: String @key` still compiles. The semantics loosen: `@key` no longer means "the immutable identity," it means "a unique, addressable, mutable natural key that seeds the surrogate at first insert."
- Updating a `@key` value succeeds (the current rejection is removed). Edges are unaffected because they hold the surrogate.
- Uniqueness is enforced cross-version, not only intra-batch (iss-714), because a mutable unique field has to stay unique against already-committed rows.

### Edges resolve on write

- Edge authoring accepts an endpoint reference that is either a surrogate `id` or a natural-key value. The loader/mutation path resolves it to the surrogate through the unique index and stores the surrogate in `src`/`dst`. This is the Datomic-lookup-ref / Dgraph-xid / Neo4j-MERGE shape.
- After resolution the edge is value-independent: renaming the endpoint's natural key never dangles it.
- Because `id` is frozen to the *original* key, authoring an edge by the original key keeps working forever with no change; resolve-on-write is only needed to author an edge by a node's *renamed current* key, so it ships last (PR 6).

### Merge matches by the surrogate

- Three-way merge matches nodes by the frozen surrogate `id` (already true in the code), never by the mutable natural key. This sidesteps Dolt's documented failure mode where a key change reads as delete-plus-insert and loses row lineage.
- Independent same-`@key` inserts on two branches derive the same surrogate, so merge unifies them. This is the desired clean-merge path, achieved with no edge rewriting.

### Reference-level changes

- **Storage**: no new column. The physical `id` column and the `@key` property column already coexist; this RFC lets them diverge after a rename instead of holding them equal.
- **Loader** (`loader/mod.rs`): `build_node_batch` adopts resolve-or-seed (derive `id = @key` on first insert; resolve to the existing frozen `id` on a later load/merge by current key). Edge endpoint resolution (`collect_node_ids_with_pending` and callers) gains natural-key-to-surrogate resolution. A new `enforce_unique_constraints_cross_version` (sibling of the intra-batch fn) reuses `composite_unique_key` and the `@key` BTREE for a batched indexed lookup.
- **Mutation** (`exec/mutation.rs`): remove the `@key`-update rejection (`:1104-1112`); add an `id`-immutability guard; route edge endpoint references through resolution; call the cross-version uniqueness check at the three existing intra-batch sites (`:1008`, `:1196`, loader `:471`).
- **Merge**: no production change (already keys on `id`); reuse `loader::composite_unique_key` for composite natural keys.
- **GQ** (compiler typecheck): special-case `id` in `PropAccess` typecheck (`typecheck.rs:950`) to expose it as a projectable/filterable system column for keyed and keyless types (iss-gq-expose-system-id-column); projection already resolves `{var}.id`. Do not add `id` to `NodeType.properties`. Reserve `id` at catalog/lint time so a user property named `id` is rejected at authoring (fixing the current Lance init crash).
- **Schema/compiler**: `@key` keeps its catalog representation; its documented meaning changes from "immutable identity" to "mutable natural key + surrogate seed."

## Breaking change & compatibility

Under derive-then-freeze the on-disk format does not change and **no data migration is required**. Existing keyed nodes already store the surrogate value in both the `id` column and every referencing edge endpoint. The deliberate contract changes:

| Surface | Change | Direction |
|---|---|---|
| `.pg` syntax | unchanged; `@key` semantics loosen | non-breaking syntax |
| On-disk data / edges | none (values already correct) | non-breaking |
| GQ `$node.id` | additive projection/filter | additive |
| Schema with a property named `id` | currently crashes at Lance init; now rejected at schema authoring with a lint | bug fix + tightening |
| `update` on `@key` | error becomes success | safe direction |
| Edge authoring | accepts natural key OR id (superset) | additive |
| Merge | matches by surrogate; identical to today until a rename is used | non-breaking for existing flows |
| Cross-version uniqueness (iss-714) | loads with duplicate keys that previously slipped through now fail loudly | **tightening** (also a bug fix) |
| `@key == id` contract | stops holding after a rename | observable-contract loosening (Hyrum), manifests only on use |

Choosing an independent random ULID surrogate instead would force a full rewrite of every `id` and every edge endpoint, a breaking whole-graph migration. Derive-then-freeze avoids it. Per the project's pre-release no-compat posture, the tightening and the loosened contract are taken head-on rather than shimmed.

## Invariants & deny-list check

See [invariants.md](invariants.md).

- **Logical contract over physical state / invariant 15:** the logical identity stays a string surrogate (source-of-truth, portable across branches/merge); Lance stable row ids remain a derived physical accelerator. No parallel copy, no cold re-derivation. Aligned.
- **Manifest-atomic visibility (1, 2):** unchanged; this is a row-semantics change, not a publish-path change.
- **Merge correctness (9):** strengthened. Surrogate-keyed merge removes the rename-loses-lineage hazard and the random-surrogate silent-duplication hazard.
- **Schema identity survives renames (8):** complementary. That invariant is about *type/property* identity in migration; this RFC is about *node* identity. Both move identity off mutable strings.
- **Deny-list:** no custom WAL/transaction manager; no per-table publishing; no shadow copy; no eventual-consistency fallback. The new write-time cost (cross-version uniqueness + natural-key resolution) is a single structured pushdown per touched table against the `@key` BTREE (a batched `key IN (...)` lookup over the index), bounded by distinct keys written, so it stays within invariant 15 rather than re-deriving from cold storage.

## Testing

In scope (PR 1 + PR 2):

- `end_to_end.rs` (PR 1): `RETURN $n.id` and `WHERE $n.id = '…'` on keyed and keyless types; a user property named `id` is rejected at schema build.
- `validators.rs` (PR 2): cross-version uniqueness (commit a row, then a duplicate-key load/insert fails), self-update exclusion, `LoadMode::Overwrite` does not false-positive.
- `warm_read_cost.rs` (PR 2): cost-budget — cross-version uniqueness on an N-row insert performs object-ops bounded by distinct keys written, **flat across committed-table size and commit-history depth**, on a history-deep fixture (guards against a regression to a full scan).

Deferred (PR 3–6): `writes.rs`/`staged_writes.rs` (rename keeps `id` frozen, edges still resolve), `merge_truth_table.rs`/`composite_flow.rs` (rename cell, two-branch same-`@key` unify, zero-migration regression).

Per the test-first rule (AGENTS.md #12), write the failing regression first for PR 2 and PR 4. Prefer extending the owning test file over a new one ([testing.md](testing.md)).

## Drawbacks & alternatives

- **Rename does not unify across branches.** If branch A renames an entity's `@key` and branch B independently inserts a fresh node under that new name, their frozen surrogates differ and merge treats them as two entities. This is arguably correct (a rename and a coincidental fresh node are not obviously the same thing) and is the price of a mutable natural key. The alternative (merge by the mutable key) reintroduces Dolt's lineage loss, which is worse.
- **Residual: cross-process uniqueness race.** The cross-version check reads committed state under the query snapshot, so two processes can each pass their lookup and both commit a colliding key (the publisher CAS does not enforce uniqueness — the same one-winner-CAS exposure documented for forks/recovery in invariants.md). Single-process and intra-query uniqueness are fully closed; the cross-process residual is a known-gap addition, not full closure.
- **Alternative: independent random surrogate + merge-time reconciliation.** Rejected: Dolt's evidence shows random surrogates silently duplicate across merge, and reconciliation needs an edge-rewrite pass. Derive-then-freeze gets unification for free.
- **Alternative: keep fusing identity (ArangoDB regime).** Rejected: it is exactly today's behavior and the source of the complaint. Acceptable only if immutable slugs are acceptable, which they are not for this use case.
- **Alternative: Lance stable row ids as the logical edge value.** Rejected on the substrate boundary: per-dataset, not portable across branches/merge, no merge primitive. They remain a physical accelerator only.

## Reversibility

The on-disk format is unchanged, so the storage decision is reversible. The contract change (mutable `@key`, surrogate-keyed merge, exposed `id`) is observable and, per Hyrum, becomes depended upon once shipped, so it earns release notes and an ADR. The merge-matching-key choice is the least reversible piece and is the right candidate for the ADR that records the derive-then-freeze rationale and its three external precedents (UUIDv5, Datomic identity upsert, Dolt keyless content-addressing).

## Open questions

1. **Annotation naming — DECIDED: keep `@key`** and redefine it (mutable natural key + surrogate seed), with the surrogate as the implicit `id`. No `.pg` churn; matches the HelixDB implicit-`ID` shape. (The `@id`/`@natural` split was rejected for churn.)
2. **Surrogate for keyless nodes referenced by edges.** A keyless node now has a usable, projectable ULID surrogate (iss-gq-expose-system-id-column). Does iss-lint-edge-endpoint-requires-key downgrade from a correctness lint to a usability hint once keyless endpoints are referenceable? Probably yes; keep the lint as advisory.
3. **Composite natural keys.** `composite_unique_key` (`loader/mod.rs:1405`) is the single keying function for intra-batch, cross-version, edge resolution, and merge — never `key_property()` (which returns only the first key column). Confirmed as the one path.
4. **`id` name collision — DECIDED: reserve the name.** A user property named `id` currently crashes at Lance dataset creation (two `id` fields). PR 1 rejects it at schema build with a lint code (`docs/user/schema/lint.md`), turning the init crash into an author-time error.
5. **Sequencing against dec-918.** Stable row ids land as the physical traversal accelerator independently; this RFC's logical surrogate does not block on them.

## Relationship to prior work

- **dec-918 (stable row IDs):** complementary and bounded by the substrate-boundary analysis above. Stable row ids are physical; this RFC's surrogate is the logical edge value.
- **iss-gq-expose-system-id-column:** the keystone and PR 1 of this RFC.
- **iss-lint-edge-endpoint-requires-key:** subsumed; becomes advisory once keyless surrogates are referenceable.
- **iss-714 / MR-714:** a hard dependency of the mutable-natural-key step (uniqueness must hold cross-version once the key can change); shipped as PR 2 ahead of the loosening.
- **Rename-stable type identity (invariants.md Known Gap):** parallel concern at the schema layer; both move identity off mutable strings.

## Rollout

Tightening lands before loosening. **In scope now: PR 1 + PR 2** — they deliver the referenceable surrogate and fix the iss-714 uniqueness bug without loosening any contract. PR 3–6 are a deferred follow-up, specified for continuity.

1. **PR 1 — Expose `id` + reserve the name** (iss-gq-expose-system-id-column). Typecheck special-case (`typecheck.rs:950`); reject a user property named `id` at catalog/lint. Pure addition; also fixes the current init crash. *(in scope)*
2. **PR 2 — Cross-version uniqueness** (iss-714). Indexed, batched key→id lookup against the `@key` BTREE, with self-update exclusion, the staging shadow, and a `LoadMode::Overwrite` short-circuit; cost bounded by distinct keys written, flat in history (invariant 15). Bug fix and hard prerequisite for a mutable `@key`. *(in scope)*
3. **PR 3 — Relocate the immutability guard onto `id`** (mechanical). *(deferred)*
4. **PR 4 — Make `@key` mutable**: remove the rejection (`mutation.rs:1104-1112`); `id` frozen on update is automatic; the `load`/`merge` path adopts resolve-or-seed. Release note + merge ADR land here. *(deferred)*
5. **PR 5 — Merge rename + two-branch-unify tests** (no production code; merge already keys by `id`). *(deferred)*
6. **PR 6 — Edge resolve-on-write** (reordered after the indexed-lookup helper from PR 2, and only needed once renames exist; authoring by the original key keeps working without it). *(deferred)*

PR 1 alone covers most of the user's need (a stable, referenceable id) and removes the `id`-property crash.
