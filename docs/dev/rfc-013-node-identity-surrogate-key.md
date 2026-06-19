# RFC: Node Identity — Surrogate Reference Key, Mutable Natural Key, Resolve-on-Write Edges

**Status:** Proposed
**Date:** 2026-06-19
**Tickets:** iss-gq-expose-system-id-column (expose the system id column in GQ — the keystone), iss-lint-edge-endpoint-requires-key (author-time lint for keyless edge endpoints), iss-714 / MR-714 (cross-batch + cross-version uniqueness for `@key`/`@unique`), dec-918 / MR-918 (ADR — stable row IDs, accepted; this RFC defines how the logical layer sits above it)
**Target release:** pre-1.0 contract change to identity semantics; phased (see Rollout). Warrants a `docs/releases/` note and an ADR on merge because it changes a documented contract (`@key` immutability, edge-resolution semantics, merge-matching key).

## Summary

Today OmniGraph fuses three identity roles into one value. A node's `@key` property *is* its `id` column, *is* the string an edge stores in `src`/`dst`, *is* the key three-way merge matches rows by. Because edges hold that string directly with no indirection, `@key` has to be immutable, and the mutation path enforces this (`crates/omnigraph/src/exec/mutation.rs`: "cannot update @key property — delete and re-insert instead"). Users experience this as "my primary key is useless because I can't rename it."

This RFC decomposes identity into two layers, the design every mature graph and versioned database converges on:

1. An **immutable surrogate `id`** that is the sole edge-resolution and merge-matching key, derived from the initial `@key` value and then frozen (a ULID for keyless nodes, exactly as today).
2. A **mutable natural key** (`@key` / `@unique`) that is a freely-updatable unique attribute, addressable in queries and edge authoring, resolved to the surrogate at write time.

The surrogate is **derived-then-frozen**, not random-per-write. This is the load-bearing choice: it makes independent inserts of the same natural key on two branches converge to one surrogate, so three-way merge unifies them instead of silently duplicating. Under this choice the change requires **zero data migration** of existing graphs, because their `id` columns and edge endpoints already hold exactly the frozen-surrogate value.

## Motivation

The root flaw is one value carrying three jobs with no indirection between them:

- **Reference identity** — what edges resolve against, so it must be stable.
- **Entity identity** — what makes "the same node" across independent branches, so merge can match it.
- **Natural key** — the human-meaningful value a user wants to be able to change.

Edges store the raw `id` string and resolve by string membership against the `id` column (`crates/omnigraph/src/loader/mod.rs`). Because the reference job pins the value immutable, the natural-key job dies: a rename would orphan every edge pointing at the old value, and there is no cascade and no surrogate to fall back on. So the engine forbids the rename outright. The immutability is forced by the fusion, it is not a deliberate feature.

Three already-tracked tickets are facets of this one gap. iss-gq-expose-system-id-column wants `$node.id` projectable so the auto-generated id becomes a real referenceable key ("the Mongo `_id` model"). iss-lint-edge-endpoint-requires-key wants an author-time warning when a node used as an edge endpoint has no `@key` and is therefore unreferenceable. iss-714 wants `@key`/`@unique` uniqueness enforced beyond a single batch. They resolve cleanly once identity is decomposed.

## Validated facts

This design was checked against Lance, LanceDB, HelixDB, Dolt, Datomic, Neo4j, Dgraph, ArangoDB, TigerGraph, and MongoDB. The findings:

- **The two-layer split is the dominant pattern.** Neo4j (relationships target `elementId`, application keys live under uniqueness constraints), Datomic (refs store immutable entity ids, `:db.unique/identity` + lookup refs address by domain value), Dgraph (edges point at `uid`, the `xid` upsert pattern resolves external keys), and HelixDB (edges store the system `u64` `ID`, `UNIQUE INDEX` is a separate lookup key) all separate an immutable reference target from a separate unique lookup key. HelixDB is the closest analog: a graph-plus-vector engine that gives every node an implicit immutable `ID` and treats the human key as a distinct `UNIQUE INDEX`.

- **A clean rule explains the user's pain:** *the mutability of the user-facing key equals the absence of edges pointing at it.* The databases that store the user key in the edge (ArangoDB `_from`/`_to` hold `collection/_key`; TigerGraph's write-once primary id; MongoDB refs hold `_id`) all freeze the user key. OmniGraph today is the ArangoDB regime: `id` is built from `@key`, they are welded, both freeze. The databases that interpose a surrogate (Neo4j, Datomic, Dgraph, HelixDB) all permit a mutable user key.

- **Edges resolve user keys to the surrogate at write time** everywhere it is possible to rename: Neo4j `MERGE`, Datomic lookup refs, Dgraph upsert blocks. This is the mechanism we adopt.

- **Primary keys are immutable because foreign keys have no indirection.** Postgres `ON UPDATE CASCADE` exists precisely to copy a changed key into referencing rows; the cascade is the cost of the missing indirection. The surrogate is the indirection.

- **The merge constraint forces derive-then-freeze, and the alternative is a silent-duplication bug.** Dolt merges strictly by primary key and keeps no hidden row identity. Its own guidance to use random UUID keys avoids *false collisions* but, read fully, is a cautionary tale: random per-branch ids unify *nothing*, so the same real entity inserted on two branches becomes two rows after merge, a silent correctness bug rather than a loud conflict. The fix Dolt's keyless content-addressing, Datomic's `:db.unique/identity` upsert, and deterministic UUIDv5 all embody is a **content-derived-then-frozen** id: two branches that independently derive the same id from the same natural key collide and unify on merge.

- **Lance fixes the substrate boundary.** Lance stable row ids are a per-dataset manifest counter (`next_row_id`), not content-derived, not portable across datasets or branches, and Lance has no branch-merge primitive at all. The unenforced primary key is immutable once set and uniqueness is enforced only at merge-insert time. So the logical edge reference must stay a reproducible **string** key; a Lance stable row id can only ever be an in-dataset physical accelerator. This is the dec-918 boundary: stable row ids accelerate traversal, they are never the logical edge value.

The convergent lesson: a separate immutable surrogate is the price of a mutable user-facing key, it is the well-trodden design, and for a branch/merge engine the surrogate must be derived-then-frozen rather than random.

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

### The natural key

- `@key` (and `@unique`) become ordinary mutable unique attributes. The parse-time syntax of `.pg` is unchanged; `slug: String @key` still compiles. The semantics loosen: `@key` no longer means "the immutable identity," it means "a unique, addressable, mutable natural key that seeds the surrogate at first insert."
- Updating a `@key` value succeeds (the current rejection is removed). Edges are unaffected because they hold the surrogate.
- Uniqueness is enforced cross-version, not only intra-batch (iss-714), because a mutable unique field has to stay unique against already-committed rows.

### Edges resolve on write

- Edge authoring accepts an endpoint reference that is either a surrogate `id` or a natural-key value. The loader/mutation path resolves it to the surrogate through the unique index and stores the surrogate in `src`/`dst`. This is the Datomic-lookup-ref / Dgraph-xid / Neo4j-MERGE shape.
- After resolution the edge is value-independent: renaming the endpoint's natural key never dangles it.

### Merge matches by the surrogate

- Three-way merge matches nodes by the frozen surrogate `id`, never by the mutable natural key. This sidesteps Dolt's documented failure mode where a key change reads as delete-plus-insert and loses row lineage.
- Independent same-`@key` inserts on two branches derive the same surrogate, so merge unifies them. This is the desired clean-merge path, achieved with no edge rewriting.

### Reference-level changes

- **Storage**: no new column. The physical `id` column and the `@key` property column already coexist; this RFC lets them diverge after a rename instead of holding them equal.
- **Loader** (`loader/mod.rs`): `build_node_batch` stops requiring `id == @key` on subsequent writes; the surrogate is read from the committed row on update, derived on first insert. Edge endpoint resolution (`collect_node_ids_with_pending` and callers) gains natural-key-to-surrogate resolution.
- **Mutation** (`exec/mutation.rs`): remove the `@key`-update rejection; add the `id`-immutability guard; route edge endpoint references through resolution.
- **Merge**: match on the surrogate column; reuse `loader::composite_unique_key` for composite natural keys.
- **GQ** (compiler scan/projection layer): expose `id` as a projectable/filterable system column for keyed and keyless types (iss-gq-expose-system-id-column); mind the adjacent camelCase-lowercasing scan path (iss-990).
- **Schema/compiler**: `@key` keeps its catalog representation; its documented meaning changes from "immutable identity" to "mutable natural key + surrogate seed."

## Breaking change & compatibility

Under derive-then-freeze the on-disk format does not change and **no data migration is required**. Existing keyed nodes already store the surrogate value in both the `id` column and every referencing edge endpoint. The deliberate contract changes:

| Surface | Change | Direction |
|---|---|---|
| `.pg` syntax | unchanged; `@key` semantics loosen | non-breaking syntax |
| On-disk data / edges | none (values already correct) | non-breaking |
| GQ `$node.id` | additive projection/filter | additive |
| `update` on `@key` | error becomes success | safe direction |
| Edge authoring | accepts natural key OR id (superset) | additive |
| Merge | matches by surrogate; identical to today until a rename is used | non-breaking for existing flows |
| Cross-version uniqueness (iss-714) | loads with duplicate keys that previously slipped through now fail loudly | **tightening** (the one breaking-direction change; also a bug fix) |
| `@key == id` contract | stops holding after a rename | observable-contract loosening (Hyrum), manifests only on use |

Choosing an independent random ULID surrogate instead would force a full rewrite of every `id` and every edge endpoint, a breaking whole-graph migration. Derive-then-freeze avoids it. Per the project's pre-release no-compat posture, the tightening and the loosened contract are taken head-on rather than shimmed.

## Invariants & deny-list check

See [invariants.md](invariants.md).

- **Logical contract over physical state / invariant 15:** the logical identity stays a string surrogate (source-of-truth, portable across branches/merge); Lance stable row ids remain a derived physical accelerator. No parallel copy, no cold re-derivation. Aligned.
- **Manifest-atomic visibility (1, 2):** unchanged; this is a row-semantics change, not a publish-path change.
- **Merge correctness (9):** strengthened. Surrogate-keyed merge removes the rename-loses-lineage hazard and the random-surrogate silent-duplication hazard.
- **Schema identity survives renames (8):** complementary. That invariant is about *type/property* identity in migration; this RFC is about *node* identity. Both move identity off mutable strings.
- **Deny-list:** no custom WAL/transaction manager; no per-table publishing; no shadow copy; no eventual-consistency fallback. The one new write-time cost (natural-key-to-surrogate resolution) goes through the existing unique index, not an ad-hoc IN-list.

## Testing

- Extend `validators.rs` (unique/`@key` enforcement across JSONL/insert/update) for the new mutable-`@key` path and cross-version uniqueness (iss-714).
- Extend `writes.rs` and `staged_writes.rs` for resolve-on-write edge endpoints (natural key and surrogate both accepted; rename leaves edges intact).
- Extend `merge_truth_table.rs` / `merge.rs` coverage with a `setProperty`-on-`@key` rename case and a two-branch same-`@key` insert that must unify on the surrogate.
- Add a GQ read test for `$node.id` projection/filter on keyed and keyless types (iss-gq-expose-system-id-column acceptance).
- A regression test that an existing fixture graph (id == `@key`) reads and merges identically after the change (zero-migration proof), per the test-first rule in AGENTS.md.
- Prefer extending the owning test file over a new one, per [testing.md](testing.md).

## Drawbacks & alternatives

- **Rename does not unify across branches.** If branch A renames an entity's `@key` and branch B independently inserts a fresh node under that new name, their frozen surrogates differ and merge treats them as two entities. This is arguably correct (a rename and a coincidental fresh node are not obviously the same thing) and is the price of a mutable natural key. The alternative (merge by the mutable key) reintroduces Dolt's lineage loss, which is worse.
- **Alternative: independent random surrogate + merge-time reconciliation.** Rejected: Dolt's evidence shows random surrogates silently duplicate across merge, and reconciliation needs an edge-rewrite pass. Derive-then-freeze gets unification for free.
- **Alternative: keep fusing identity (ArangoDB regime).** Rejected: it is exactly today's behavior and the source of the complaint. Acceptable only if immutable slugs are acceptable, which they are not for this use case.
- **Alternative: Lance stable row ids as the logical edge value.** Rejected on the substrate boundary: per-dataset, not portable across branches/merge, no merge primitive. They remain a physical accelerator only.

## Reversibility

The on-disk format is unchanged, so the storage decision is reversible. The contract change (mutable `@key`, surrogate-keyed merge, exposed `id`) is observable and, per Hyrum, becomes depended upon once shipped, so it earns release notes and an ADR. The merge-matching-key choice is the least reversible piece and is the right candidate for the ADR that records the derive-then-freeze rationale and its three external precedents (UUIDv5, Datomic identity upsert, Dolt keyless content-addressing).

## Open questions

1. **Annotation naming.** Recommendation: keep `@key` and redefine it (mutable natural key + surrogate seed), with the surrogate as the implicit `id`. The alternative is an explicit split (`@id` immutable surrogate vs `@natural`/`@key` mutable unique). Keeping `@key` avoids `.pg` churn and matches the HelixDB implicit-`ID` shape.
2. **Surrogate for keyless nodes referenced by edges.** A keyless node now has a usable, projectable ULID surrogate (iss-gq-expose-system-id-column). Does iss-lint-edge-endpoint-requires-key downgrade from a correctness lint to a usability hint once keyless endpoints are referenceable? Probably yes; keep the lint as advisory.
3. **Composite natural keys.** Confirm `loader::composite_unique_key` is the single resolution path for composite-`@key` endpoint resolution and merge matching, so there is one keying function.
4. **`id` name collision** with a user-declared `id` property when exposing the system column. Decide precedence or reserve the name.
5. **Sequencing against dec-918.** Stable row ids land as the physical traversal accelerator independently; this RFC's logical surrogate does not block on them.

## Relationship to prior work

- **dec-918 (stable row IDs):** complementary and bounded by the substrate-boundary analysis above. Stable row ids are physical; this RFC's surrogate is the logical edge value.
- **iss-gq-expose-system-id-column:** the keystone and Phase 1 of this RFC.
- **iss-lint-edge-endpoint-requires-key:** subsumed; becomes advisory once keyless surrogates are referenceable.
- **iss-714 / MR-714:** a hard dependency of the mutable-natural-key step (uniqueness must hold cross-version once the key can change).
- **Rename-stable type identity (invariants.md Known Gap):** parallel concern at the schema layer; both move identity off mutable strings.

## Rollout

1. Expose `id` in GQ (iss-gq-expose-system-id-column). Prerequisite; nothing references a surrogate it cannot see.
2. Edge resolve-on-write: accept natural-key or id, store the surrogate. Backward-compatible (today's input is the surrogate already).
3. Decouple `id` from `@key` in the loader/mutation paths; move the immutability guard to `id`.
4. Make `@key` mutable: remove the update rejection; land cross-version uniqueness (iss-714).
5. Merge matches by the surrogate; add the rename and two-branch-unify tests.

Steps 1 and 2 deliver most of the user value (a stable referenceable id plus human-friendly authoring) before any contract loosens.
