---
rfc: "0044"
title: "Edge keys: derived edge identity"
track: maintainer
status: draft
implementation: in-progress
authors:
  - azimafroozeh
created: 2026-08-31
updated: 2026-09-01
discussion: https://github.com/ModernRelay/omnigraph/issues/583
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0044: Edge keys: derived edge identity

> A term set in ***bold italics*** is being defined at that exact spot.

## Summary

`@key` becomes legal on edge types. A keyed edge type derives its physical id
from its ***edge key***, the declared column tuple that identifies one logical
edge, which must include `src` and `dst` and may add scalar properties. The
derivation is the one `@key` nodes already use: the canonical key encoding of
`canonical_node_id` (one column keeps its scalar spelling, a composite
becomes a JSON array of the per-column canonical strings; an edge key is
always the composite arm, since `src` and `dst` are mandatory). Because the
id is a pure
function of the key, the same logical edge (the same endpoint ids and key
values) inserted on two branches mints the same id, meets in the three-way
merge walk, and follows the node rules that already exist: identical rows
converge to one row, divergent rows surface a typed `MergeConflict`. Issue
#583's silent duplicate (the same edge inserted on both sides of a branch
fork, merged into two stored rows) becomes unrepresentable for keyed types,
at every write door, by construction.

Two boundaries do not change. An edge type that declares no key keeps today's
multiset semantics: fresh ULID ids, parallel edges legal, and the documented
keep-both merge outcome. And the storage format is untouched: a derived id is
an ordinary `Utf8` value in the existing id primary-key column, so Lance sees
nothing new.

## Motivation

Issue #583, found by the deterministic simulation harness (localized from
seed 10228's operation transcript, later re-derived independently by the
in-tree DST nightly): fork a branch, insert the same edge on both sides,
merge. The merge accepts and the
store holds two physical rows for one logical relationship.

The mechanism is structural, not a defect in the merge. The three-way walk
(`stage_streaming_table_merge`, `exec/merge.rs`) keys rows on the table primary
key `id`. Every edge insert mints a fresh ULID (`exec/mutation.rs:1235`), so
two independently inserted rows for one logical edge never meet in the walk;
each side reads as inserted on one side only and is taken. The row-level
conflict classification (`MergeConflictKind::DivergentInsert`) fires only when
the same id diverges, which two fresh ULIDs cannot produce. Nodes are immune
because `@key` derives their id from declared identity: convergent inserts
collide on id and the walk already converges rows that agree
(`exec/merge.rs:1520`, "Both sides changed but agree") and conflicts rows that
differ. Edges are the one entity class with no way to declare identity: the
schema parser rejects `@key` on edges outright
(`omnigraph-compiler/src/schema/parser.rs:990`).

The result is silently wrong data rather than an error. The gated traversal
suppresses the second row in its visited set, so the default query shape shows
correct results while counts, aggregates, bound-edge traversals
(`$a $e:knows $b`), ranking over edge multiplicity, and exports all see the
duplicate. The merge truth table pins this today as the documented current
contract (`tests/merge_truth_table.rs:600`, the `(AddEdge, AddEdge)` cell
keeps both rows) and defers the endpoint-identity question as "tracked
separately". This RFC is that separate track.

The existing mitigation is real but insufficient. A schema may declare
`@unique(src, dst)` on an edge type; the merge evaluates declared constraints
over the merge delta and refuses with `MergeConflictKind::UniqueViolation`,
and this exact scenario is tested (`tests/branching.rs:2823`). Three gaps
remain. First, it is rejection-only: two branches recording the same fact must
be resolved by hand where convergence is the obviously right outcome. Second,
it is a validator, so every write and every merge pays a committed-index
lookup for a property a derived id would guarantee for free
(`validate.rs:222`: edge `@unique` groups take the committed lookup precisely
because "Edges have no `@key`"). Third, it is opt-in and undiscovered: #583
was filed by a maintainer, from a schema written the obvious way.

## User and operational behavior

A keyed edge type is declared with the constraint that already exists for
nodes:

```
node Person {
    name: String @key
}

edge Knows: Person -> Person {
    @key(src, dst)
}
```

- **Insert.** Inserting a keyed edge derives the id from the key columns and
  stages the row as an upsert, exactly as `@key` node inserts do today
  (`exec/mutation.rs:1220`, `PendingMode::Upsert`). Inserting Diana knows
  Alice twice on one branch yields one row; a second insert whose non-key
  properties differ updates the row. The coalescing and last-write-wins
  order is the keyed-node contract unchanged: within one mutation, later
  commands win for the same derived id; across mutations, the later commit
  wins. A null value in a key column is refused with the existing typed
  error shape. On the mutation path the derivation is the only id source: a
  GQ `insert` carries no id, and the keyed-node arm likewise never reads
  one (`mutation.rs:1171`). On the load surfaces a supplied `id` is refused
  unless it exactly equals the derivation. A delete and an insert of the
  same derived id cannot share one mutation: a query is constructive or
  destructive, never both (the existing engine-wide refusal), so
  delete-then-reinsert spans two mutations, where the later commit wins;
  both the refusal and the re-creation are pinned by the write-path tests.
- **Key columns are immutable.** Edge types accept no update at all (the
  typechecker refuses every edge update, so key immutability is structural):
  a keyed edge changes by inserting its key again with the new values (an
  upsert), an unkeyed edge by delete and re-insert. The mutations guide
  states this rule.
  `@rename_from` on a key property keeps existing ids: the id derives from
  values, never from property names.
- **Load.** The load path derives keyed-edge ids identically, so a keyed
  edge in load input follows the load-mode contract keyed nodes have today
  (`merge` coalesces on the derived id, `append` reports a key conflict,
  `overwrite` replaces). The per-mode outcomes land in the mutations guide
  as part of this RFC's documentation work.
- **Merge.** Two branches inserting the same keyed edge converge to one row
  with no conflict. Two branches inserting the same key with different
  non-key properties surface `MergeConflictKind::DivergentInsert`
  (`divergent_insert` on the wire), exactly as keyed nodes do. The
  conflict's entity id is the derived id in the catalog's key order,
  endpoints first (src, then dst), then scalar members by stable property
  identity (for `@key(src, dst)` a JSON array of the two endpoint node ids;
  those elements are themselves ULIDs when the endpoint node type declares
  no `@key`). No
  new conflict kind and no new resolution flow is introduced: a conflict
  resolves through the branching guide's existing reconcile-and-re-merge
  contract, which gains a keyed-edge example in this RFC's documentation
  work.
- **Convergence is as strong as endpoint identity.** When an endpoint node
  type declares no `@key`, nodes inserted independently on each branch are
  distinct rows with distinct ids, so keyed edges to them derive distinct
  ids and remain distinct after the merge. The keyed-edge guarantee composes
  with keyed endpoint types.
- **Derived ids are generation-blind for change consumers.** A delete plus
  an identical re-insert of the same key across versions resurrects the
  same id, so the id-ordered snapshot diff reports no change (and a
  modified re-insert reports an update, not a delete plus create). Keyed
  nodes already have this contract; keyed edges enter it knowingly.
- **Constraints.** An edge `@unique` group over exactly the key's column set
  is subsumed by identity and skips its committed lookup, mirroring the
  node rule (`validate.rs:196`, "the key IS the identity"). The comparison
  is on column sets: uniqueness is order-free, and schema-shape
  normalization stores constraint tuples lexically, so no spelling order
  survives to the runtime. Other `@unique`
  groups, edge referential integrity, and `@card` are unchanged; `@card` now
  counts converged rows, which is the logical count.
- **Unkeyed edge types are untouched.** No key, no behavior change: ULID ids,
  parallel edges remain distinct in bound-edge traversal
  (`docs/user/queries/index.md:53`), and the keep-both merge outcome stays the
  documented contract.
- **Discovery.** `GET /schema` and `omnigraph schema show` report edge keys
  through the same schema output surface that reports node keys today.

## Design

Six localized changes:

1. **Grammar and IR.** The parser accepts at most one `@key` group in an edge
   body (the node multiplicity rule), and the body-level group is the only
   accepted spelling: of the parser's three `@key`-on-edge refusals, only the
   body-group site (`parser.rs:990`) relaxes. A type-level `@key` annotation
   stays refused exactly as it is on nodes (the grammar rejects the
   parenthesized form in annotation position outright; a bare `@key`
   annotation is refused at `parser.rs:734`, the node twin at
   `parser.rs:704`), and a property-level `@key` on an edge property stays
   refused (`parser.rs:827`), because a single-property key can never include
   both endpoints. Two parser strictness rules ride along: the property
   names `id`, `src`, `dst`, `from`, and `to` become reserved on edge
   declarations (such a property would shadow a physical column or collide
   with the insert's from/to parameters, splitting identity between write
   doors), and a repeated `@key` member is refused at parse time on nodes
   and edges alike (shape normalization previously collapsed the repeat
   silently). Both are parse-time refusals of new declarations only.
   Every edge key must include both `src`
   and `dst` and may add scalar properties; a key omitting an endpoint is
   refused at declaration time, so two edges between different endpoint
   pairs can never collide on one id. Key columns obey exactly the node
   key-column rules: non-null scalars declared non-nullable, with list,
   blob, and vector columns refused, and the canonical spelling per scalar
   type is the one `canonical_scalar_key` already defines
   (`loader/mod.rs:2907`). The accepted `SchemaIR` records the key on the
   edge type the way it records node keys (`ConstraintIR` already models
   `src`/`dst` as system field references: `FieldRefIR::System` at
   `schema_ir.rs:205`, minted from `src`/`dst` on edge types at
   `schema_ir.rs:901`).
2. **Id derivation.** The write path routes keyed edge inserts through the
   existing canonical key encoding (`loader/mod.rs:2866`,
   `canonical_node_id`, generalized in name only). `src` and `dst` contribute
   the endpoint node ids, which are already canonical strings; at derivation
   time those values arrive under the insert's `from`/`to` parameters and
   are aliased to `src`/`dst` (`mutation.rs:362`), and the parser's `@key`
   arm gains the same `src`/`dst` system-column allowance its `@unique` arm
   already has (`parser.rs:1036`). The encoding is deterministic and
   unambiguous: one column keeps the scalar spelling, a composite encodes as
   a JSON array in the catalog's key order, endpoints first (src, then dst),
   then scalar members by stable property identity, extending the keyed-node
   rename-proof rule (a composite node key already orders by stable property
   id, so a rename cannot change physical tuple identity), so user data
   cannot forge a collision.
3. **Write mode.** Keyed edge inserts switch from `StrictInsert` to the
   `Upsert` pending mode and from `MutationOpKind::Insert` to `Merge` for
   pre-write metadata capture (`mutation.rs:1205`), matching the keyed-node
   arm; keyless edge inserts keep today's path bit for bit. The op-kind
   switch also moves keyed edge inserts into the keyed-node retry class: a
   concurrent authority move mid-mutation discards and repreparses with a
   bounded retry instead of surfacing a strict-insert conflict.
4. **Validation.** `constraints_for` emits the edge key as
   `Constraint::Unique { is_key: true }`, dropping the committed lookup the
   equivalent `@unique` group pays today.
5. **Load path.** Both loader edge-id sites (`build_edge_batch`,
   `loader/mod.rs:1508`; `normalize_strict_edge_rows`, `loader/mod.rs:1703`)
   derive the id for keyed edge types, and an explicit id that does not
   equal the derivation is refused with a typed error on both surfaces,
   with exact equality (mirroring `normalize_strict_node_rows`,
   `loader/mod.rs:1680`). Keyed edges deliberately do not inherit the node
   load path's legacy-spelling acceptance (`explicit_id_matches_node_key`,
   `loader/mod.rs:3048`), which exists only for pre-composite node exports
   and would accept a bare endpoint id as an edge id. On the bulk-load site the
   derivation consumes the remapped canonical endpoint ids, after
   `node_id_remap` resolution (`loader/mod.rs:1528`). The strict site has no
   remap step and none is added: its `src`/`dst` values are consumed
   verbatim, which is sound because the strict node surface admits only
   exact canonical ids (`loader/mod.rs:1680`) and end-of-load edge
   referential integrity refuses any endpoint that is not a stored node id,
   so a row that would derive from a non-canonical endpoint fails the load
   before commit. Change 4's soundness argument (the id
   is a pure function of the key) holds only because every id source
   derives, from canonical inputs.
6. **Version acceptance.** `validate_schema_ir` moves from exact equality
   (`schema_ir.rs:1067`) to accepting the supported version set {2, 3};
   3 is the edge-key number, assigned here at acceptance. The
   deliberate v1 rejection is unchanged (pinned by the v1-rejection test
   beside `validate_schema_ir`, `schema_ir.rs:1735`). One stamping rule
   owns every case: an accepted schema is stamped with the highest
   `ir_version` its declared features require. Everything else derives
   from it: 3 is minted only when a schema declares an edge key; an
   unkeyed schema accepted by the new binary stamps the base number, as
   does a fresh init without edge keys; and a schema apply that removes
   the last keyed edge type (by drop-and-re-add) re-stamps the base
   number, restoring downgrade-safety. How `ir_version` composes with
   other in-flight schema features is deliberately not decided here: a
   single linear scalar cannot express independent schema features
   (raised in review on #546), and the joint scheme is deferred to a
   dedicated versioning RFC.

The merge is deliberately not on the list. Convergence and conflict for
same-id rows are existing, tested walk behavior; feeding edges deterministic
ids is the entire fix. Identity lives in the id path itself, not in a side
record: the precedent is table incarnation identity and RFC 0042's
incarnation-suffixed branch refs, both of which rejected a registry row for
identity-in-the-path. No new durable record, authority, or recovery step is
introduced.

## Invariants

- **Invariant 6 (stable identity) is extended in principle.** The accepted
  SchemaIR, which owns type, property, and table-incarnation identities
  today, becomes the declared owner of edge row identity as well. The
  mechanism precedent is the node `@key` derivation (`canonical_node_id`;
  `validate.rs:196`, "the key IS the identity"), not a reinterpretation of
  the invariant's current text. Nothing infers identity from an alias,
  path, or index.
- **Invariant 8 (integrity failures are loud) is strengthened.** The silent
  keep-both outcome for convergent keyed inserts becomes either declared
  convergence or a typed conflict. No constraint is silently weakened;
  unkeyed types keep their documented semantics rather than a hidden one.
- **Invariant 13 (evidence matches the boundary)** is why this is an RFC: id
  derivation is contract, and the evidence section names compatibility and
  refusal owners. The crash and rebuild posture is inherited rather than
  new: the change adds no durable effect or commit step (a derived id is
  ordinary data on the existing staged write path), so the existing
  recovery and failpoint owners continue to cover it.
- **Deny-list:** nothing on it is touched: no job queue for state derivable
  from accepted manifest state (the rejected duplicate-removal pass in
  Alternatives is exactly that shape), no maintained parallel truth, and no
  raw writer outside the sealed storage boundary.

## Compatibility and reversibility

- **Schema vintage.** Design change 6 carries the guarantees. To the
  operator of an existing graph: the new binary accepts both supported
  numbers (2 and 3, with the deliberate v1 rejection
  unchanged), so existing graphs open unchanged, and a schema that
  declares no edge key stamps the base number even when applied by
  the new binary (change 6's single stamping rule), so an unkeyed
  deployment stays downgrade-safe. Only a
  schema that declares an edge key mints version 3; from that point an
  old binary refuses the graph with the existing hard error
  (`schema_ir.rs:1067`), so downgrade after keying fails closed instead of
  misreading identity. The edge-key number is fixed here at acceptance,
  not at implementation time: 3. How the version composes with PR #546's
  system columns and later schema features is deferred to a dedicated
  versioning RFC, per change 6; RFC 0040's unresolved question 1 tracks
  it.
- **Existing graphs** are unaffected until a schema apply introduces an edge
  key. In this RFC's scope, a key may be declared only when the edge type is
  created (at init, or in a schema apply that adds the type). Declaring,
  removing, or altering a key (adding, dropping, or reordering its columns)
  on an existing edge type is refused with a typed error;
  the existing schema-plan refusal covers every existing type, empty ones
  included (drop and re-add an empty type to key it). The id rewrite a
  populated type would require is a migration and out of scope.
- **Released versions.** v0.10.0 and every earlier version carry the #583
  behavior. The workaround for released versions is `@unique(src, dst)`,
  which their merge path already enforces; the documentation half of this
  change (schema page and branching guide naming the multiset default and
  the `@unique(src, dst)` declaration) applies to released behavior and can
  ship independently.
- **Reversibility.** Reverting means the parser stops accepting NEW edge
  `@key` declarations at a later vintage while the derivation and validation
  code for already-keyed tables stays; their keyed insert, load, and merge
  semantics survive the revert. For a binary without the machinery, the
  refusal splits by vintage: a pre-edge-key binary refuses keyed-edge
  graphs through today's exact-equality check, while a later-vintage binary
  that removes the machinery cannot rely on version acceptance to refuse an
  old number and must carry an explicit refusal of every `ir_version`
  minted with edge keys. Without that refusal such a binary would open the
  graph and mint ULIDs on keyed tables, reintroducing #583 on the very
  tables whose schema declares immunity. Ids already derived are ordinary
  `Utf8` primary-key values and survive as data either way; no storage or
  wire format changed in either direction.

## Alternatives

- **Document `@unique(src, dst)` and stop (the minus-one-mechanism design).**
  The refusal path exists and is tested; this alternative is a docs PR. It
  loses on three counts stated in Motivation: rejection-only where
  convergence is correct, a committed lookup on every write and merge for a
  guarantee a derived id makes structural, and an unsafe-by-default posture
  that a maintainer's own schema fell into. If convergence-on-identical is
  judged not worth an id-derivation contract, this competitor wins and #583
  closes as documentation.
- **Content-based duplicate collapse at merge time, no declaration.** Collapse born-on-both
  edge rows that are bit-identical. Rejected: it guesses intent for multiset
  types where two identical parallel edges are legitimate data, and it makes
  a merge stricter than the same two inserts on one branch, an asymmetry with
  no principled defense.
- **A post-merge reconciliation pass that removes duplicates.** Rejected:
  derived state cleanup for a fact the write path can make unrepresentable;
  scans where construction suffices; and the window between merge and pass
  still serves wrong counts.
- **Hashed key derivation (fixed-length, non-leaking ids).** Deriving the
  id as a hash of the key instead of its plaintext canonical encoding would
  cap id length and keep key values out of ids. Rejected for consistency:
  node `@key` already chose the plaintext encoding, and one encoding
  everywhere keeps export, import, and debugging symmetric. The inherited
  costs are accepted knowingly: key values appear in ids and therefore in
  conflict reports, logs, and exports (a key over a sensitive scalar makes
  the id itself sensitive), ids have no length cap, and an `append`-mode
  key conflict discloses that a relationship exists, all exactly as for
  keyed nodes today. A future hashing change would be its own vintage.
- **Key every edge type by `(src, dst)` by default, with an opt-out.** The
  safest default and the largest break: every existing graph migrates and
  every parallel-edge schema must discover the opt-out. Deferred, not
  rejected: it builds on exactly this RFC's machinery, and the default
  question can be reopened once keyed edges exist.

## Evidence and tests

Owners to extend, per the testing map:

- `tests/branching.rs` edge-uniqueness family: keyed-edge variants of the
  composite tests (converge on identical, `DivergentInsert` on divergent
  non-key properties, distinct pairs merge cleanly).
- `tests/merge_truth_table.rs`: the `(AddEdge, AddEdge)` cell gains a keyed
  twin asserting convergence; the unkeyed cell stays, its comment updated
  from "tracked separately" to naming this RFC as the resolution.
- `omnigraph-dst`: the model's H-A born-on-both carve-out retires, since
  unkeyed keep-both is documented contract rather than an illegal state.
  The model's edge reads are visited-gated membership, so the set
  representation (`Model.edges` as `BTreeSet<(String, String)>`) predicts
  the merged MEMBERSHIP correctly for keyed and unkeyed types alike;
  physical row counts, which membership cannot see, are pinned by the
  targeted scenarios: `dst_merge_duplicates_born_on_both_edge`
  (reclassified from bug pin to multiset-contract pin, still asserting two
  physical rows) and its keyed twin
  `dst_keyed_born_on_both_edge_converges` (one row). Count-level fleet
  modeling for unkeyed edges (a multiset `Model.edges`) is deliberately
  out of scope.
- The `ir_version` acceptance and refusal owner is the compiler's schema-IR
  validation tests beside `validate_schema_ir` (`schema_ir.rs`); the CLI
  cross-version harness
  (`crates/omnigraph-cli/tests/crossversion_upgrade.rs`) gains a keyed-edge
  scenario only if its release-binary surface needs one.
- Write-path tests extend the existing owners `tests/writes.rs` (insert
  modes) and `tests/validators.rs` (constraint outcomes): keyed insert
  upsert on one branch, null key column refusal (on the load surface, where
  the column builder refuses before derivation), supplied-id refusal on
  load, the constructive-or-destructive refusal of a mixed delete+insert
  mutation and the across-mutations delete-then-reinsert re-creation,
  `@unique` subsumption.

Acceptance threshold: issue #583's repro lands in-tree as the keyed-edge
variant of the `tests/branching.rs` edge-uniqueness family, so the gate is
executable from the repository alone: the same `Knows` edge inserted on both
sides of a fork, once per side, then merged. With `@key(src, dst)` declared,
the merge returns 4 rows from both the plain and the bound-edge traversal,
with no conflict. The unkeyed control keeps its current behavior, which
is the documented multiset outcome: 4 from the plain traversal (the visited
gate suppresses the duplicate) and 5 from the bound-edge traversal. Until
the keyed variant lands, the in-tree anchor for today's behavior is the DST
pin `dst_merge_duplicates_born_on_both_edge` (`scenarios.rs`; its fixture
asserts gated 1 vs bound 2).

## Rollout

1. **One implementation PR.** All six design changes (grammar and IR, id
   derivation, write mode, validation subsumption, the load-path derivation
   and refusal, and the version-acceptance change carrying the `ir_version`
   bump), the truth-table and DST updates, and the user docs (schema page:
   the declaration and the derived-id encoding; branching guide: the
   multiset default, the released-versions workaround, a keyed-edge
   conflict example; mutations guide: the insert-identity bullets and the
   load-mode table). Existing graphs and unkeyed types are bit-for-bit
   unaffected at runtime; the one new refusal is parse-time only, on new
   declarations (the reserved edge property names and the repeated key
   member, change 1). Issue #583 closes with it; `implementation` advances to
   `complete` on merge.
2. **Out of scope, later work:** migrating an existing populated edge type to
   a key (id rewrite), and any revisit of the unkeyed default.

## Unresolved questions

None. The one settle-before-acceptance candidate (whether every edge key
must include both endpoints) is settled in Design change 1: it must. The
edge-key `ir_version` is fixed at acceptance: 3 (change 6, Compatibility).
The cross-feature versioning scheme is out of this RFC's scope and
deferred to a dedicated versioning RFC, per change 6; RFC 0040's
unresolved question 1 tracks it.

## Decision log

- 2026-08-31: drafted from issue #583.
- 2026-09-01: review (ragnorc, #592 and #546): implementation-time
  assignment of the `ir_version` rejected; the edge-key number is fixed
  at acceptance as 3. The cross-feature scheme (a linear scalar cannot
  express independent features, such as RFC 0040's spellings beside edge
  keys) is deliberately not solved in this RFC and is deferred to a
  dedicated versioning RFC.
