---
rfc: "2026-10-12-schema-apply-with-live-branches"
title: "Schema apply with live branches"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-12
updated: 2026-10-15
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Measure the apply-time branch check's requests, bytes and time at 1, 16 and 256 live branches and across growing released history, cold and warm, with a checked-in cost instrument, and fix its work budget from that measurement."
---

# RFC: Schema apply with live branches

> A term set in ***bold italics*** is defined at that spot; it is plain text afterwards.

## Summary

Allow `schema apply` on main while other branches are live. Each branch keeps
the schema contract it has until a merge brings a newer one into it. Schema
apply itself stays main-only.

Every contract gains a ***schema sequence***: a counter that each schema apply
on main increments and that merges carry forward. It orders contracts. Content
identity cannot, because a change and its reversal reproduce an earlier
identity.

A merge never combines data written under two schemas. For each table, the
merge compares its stored columns and full-text declarations in the base,
source and target contracts. Where they are not identical, at most one side
may have changed the table since the merge base; the merge takes that side's
version whole. Any other case is refused with a typed error. Schema apply
refuses up front when a live branch has already changed a table it would
change.

The counter needs storage stamp 17, written by each branch's first
publication. Older binaries refuse the graph once main carries it. The design
does not add schema apply on named branches, row conversion between contracts,
or a way to apply one change to every branch.

## Motivation

Schema apply refuses with "schema apply requires a graph with only main"
whenever any other branch exists, and cluster deployment refuses the same
change in its preflight. An operator must merge or delete every branch before
any migration. Long-lived branches, such as review branches and agent working
branches, therefore block every schema change on their graph.

Two shipped decisions make a narrower rule possible:

- [Schema contract in the manifest](2026-09-30-schema-contract-in-manifest.md)
  gives every branch's `__manifest` its own `schema_contract` row. Branch
  creation inherits the source's row, ordinary publications carry it forward,
  and live reads and writes on a branch already resolve that branch's row.
- [Detached-only tables](2026-09-21-detached-only-tables.md) makes every graph
  write a detached version of a shared dataset. Schema apply's table effects
  create new versions and leave every version another branch pins untouched.

Removing the refusal alone is not enough. A prototype that lifted only the
apply refusal and the merge's equal-contract check (Evidence and tests) showed
three failures when main was merged into a branch on the older contract:

- after a dropped property, the merge succeeded as a fast forward, the branch
  pinned main's migrated table under its old contract, a fresh open accepted
  the state, and the first query of the dropped property failed with
  `stored node:Person has no declared field 'age'`;
- after an added or renamed property, the merge failed before any effect with
  an internal "schema-gated change row is missing column" error;
- after the branch had edited the migrated table, the merge returned `Merged`
  without applying the migration, so a commit with a newer-contract parent
  carried the older contract.

## User and operational behavior

### Applying while branches are live

`omnigraph schema apply` on main succeeds while other branches exist, unless a
live branch changed, since its merge base with main, a table the apply
changes. The refusal names every such branch and table and gives two remedies:
merge the branch into main first, or delete it. Schema preview and plan report
the same list without refusing. A preview's list is advisory, including a
stored deployment preview; the check at apply is authoritative.

An apply ***changes a table*** when it adds, renames or drops one of the
type's properties, declares a full-text `@index` on it, or drops the type.
Renaming a type, extending an enum, adding a non-full-text `@index`, and
editing descriptions or instructions change no table; those applies never
refuse because of a branch.

The check runs in the applying process and does not fence writers in other
processes. A branch may still change such a table after the apply; the merge
rule below then refuses that table.

### Working on a branch with an older contract

Reads, queries, mutations, loads, Blob writes, exports and index maintenance on
a branch use that branch's contract, as today. `schema show --branch <name>`
and `GET /schema?branch=<name>` show it and require read access to that
branch. `branch list` and `branch show` report each branch's contract identity,
its schema sequence, and whether it is behind main's.

Stored queries are validated against main's contract at deployment. Running
one on a branch whose contract lacks a name it uses, through the CLI, HTTP or
MCP, fails with the ordinary query type error.

A read by snapshot or commit id uses the current contract of the branch that
wrote that commit, with tables rebound by stable identity as historical reads
already do.

### Merging across contracts

The same rule applies to a merge between any two branches, main included:

- The result carries the contract with the larger schema sequence.
- A table whose stored columns or full-text declarations differ among the
  base, source and target contracts takes the version of the one side that
  changed it since the merge base. If both sides changed it differently, the
  merge is refused with HTTP 409 and reason `schema_diverged`, naming each
  table and side.
- Every other table merges as today.

In the common cases this means:

- **Main into a branch behind it** brings main's contract and changes, unless
  the branch changed a table that main's newer contract changed.
- **A branch behind main into main** is allowed under the same condition;
  main's contract does not change.
- A merge whose base predates both sides' contracts, which a criss-cross
  history can produce, is refused for every table whose columns differ from
  the base and that both sides changed. Merging main into each side first
  resolves it.
- A merge requested `as commit` is refused before any effect when it would
  change the target's contract; unpublished commits carry no schema change.
  A catch-up merge advances the target's head, so unpublished commits and
  unattached snapshots rooted at the earlier head expire as they do after
  any other publication.

`branch merge --dry-run` runs the same capture and classification and reports
the same outcome, including `schema_diverged` and the contract the merge would
publish, without effects.

A branch that changed a table a newer contract changed cannot merge that table
in either direction. Its rows stay readable and exportable on the branch; the
work is redone on a branch created from main.

### Dropped data

`optimize` and `cleanup` erase a dropped property's values once no retained
version references the old data files. Cleanup retains every live branch's
head and the version it was created from, so the values persist until every
branch created before the drop is deleted. Merging main into such a branch
does not release its creation version.

### Older binaries

The first publication by this release stamps a branch's `__manifest` 17.
Binaries that serve stamps up to 16 refuse a graph whose main is stamped 17,
before any read. `omnigraph upgrade` stamps every live branch at once for
operators who want the fence in place before the first schema apply. The
stamp is checked at open, so stopping older processes that already hold the
graph open before the first schema apply with live branches remains an
operator obligation.

## Design

### Schema sequence

The `schema_contract` row and every graph lineage row gain `schema_sequence:
u64`, stored beside the contract head rather than inside it. A commit's
sequence is therefore available from its lineage row without opening its
snapshot. The archived schema object serializes the head with the source and
IR, so keeping the sequence outside it leaves `schema_content_hash` and every
archived object a function of schema content alone. The first stamp-17
publication of a branch therefore archives nothing new for an unchanged
schema.

- Every operation that writes a contract on main publishes it with main's
  sequence plus one. Today those are schema apply and the system-column
  upgrade; a future contract writer, such as the node identity upgrade
  proposed in [ModernRelay/omnigraph#884](https://github.com/ModernRelay/omnigraph/pull/884),
  must do the same.
- A merge publishes the contract of the side with the larger sequence. Equal
  sequences must name byte-identical contract rows; a difference is an
  integrity error.
- Every other publication carries the branch's contract row unchanged, as
  today.
- A contract row written before stamp 17 has no sequence and reads as 0.

Sequence 0 is sound for every pre-17 row. Before this release, schema apply
and the system-column upgrade required a graph with only main, so every live
branch held the same contract. The first apply under this release publishes on
main and stamps main 17 in that publication, so no contract row with a
sequence can exist beside a pre-17 contract that differs from it.

The sequence is part of the contract's identity for comparison:
`same_graph_state`, the authority token and the accepted-catalog cache key
include it. Two contracts with equal content but different sequences are
different occurrences.

A branch's sequence never decreases along its ancestry. Any operation that
moves a branch to an earlier state, such as the restore in the branch
operations v2 draft ([ModernRelay/omnigraph#677](https://github.com/ModernRelay/omnigraph/pull/677)),
must keep the branch's current contract and sequence or refuse.

Content identity cannot order contracts. The identity is a hash of the
serialized SchemaIR, and rename hints are not part of the IR, so renaming
`Person` to `Human` and back reproduces the original identity exactly.
Ordering by "the base equals one side" then misorders a history in which a
branch catches up to the intermediate contract while main returns to the
original one.

### Stored signature

The ***stored signature*** of a table in a contract is derived from that
contract's physical catalog schema for the table:

- each stored column's stable property id, name, Arrow type, nullability and
  Blob encoding, in catalog order;
- the set of properties carrying a full-text `@index`, which schema apply
  declares on the table itself.

A contract that does not register the table gives it no signature. Enum values
are not part of the signature: enums are stored as strings, and widening
leaves every stored row valid. Non-full-text indexes are derived state and are
not part of it either. The signature is a superset of the change feed's
schema fingerprint (name, Arrow type, nullability, stable property id and Blob
marker), so a table uniform in a merge never crosses a change-feed schema
boundary.

A table is ***uniform*** in a merge when the source and target contracts
either both register it or both do not, and every one of the three contracts
that registers it gives it the same signature. A table the base lacks but both
sides register with one signature is therefore uniform: its base image is
empty, and today's merge treats its rows as inserts. Otherwise the table is
***migrated*** in that merge.

`plan_schema_migration` is not used to compute this. Two accepted IRs that
differ only by a property added and later dropped produce an
unclassified-change step there, while the signature comparison correctly
reports no difference.

### Merge rule

For every table identity in the three contracts:

1. **Uniform:** today's merge routes, with aliases from the merged contract.
   All three images have the same stored columns by property identity, so the
   by-name row comparison stays sound.
2. **Migrated, source and target registrations equal:** nothing to merge; the
   shared version stays.
3. **Migrated, exactly one side's registration differs from the base's:** the
   result is that side's version, whole. That side's contract must give the
   table the merged contract's signature, or no signature when the merged
   contract drops it. Otherwise the merge is refused.
4. **Migrated, both sides differ from the base and from each other:** refused
   with `schema_diverged`.

"Differs from the base" uses the merge's existing `same_manifest_state`
comparison by stable identity: path, published version, native ref, row count
and version metadata. The registration clock and alias are ignored.

Case 1 covers the equal-contract case only when the base also has that
contract. A criss-cross base can predate both sides' contracts. For example,
a property dropped and re-added under the same name gets a new property id.
The by-name comparator would then treat an old value of 7 and a new value of 7
as unchanged and silently drop a write. The signature comparison routes that
table to cases 2 to 4 instead.

### Merge planning

1. Capture the source and target contracts and catalogs separately, each
   validated against its own snapshot, and record each side's contract
   identity, sequence included, in its own authority token. Read the base's
   contract head from its lineage row and its IR from the base snapshot or the
   retained schema content. The equal-contract refusal in merge capture is
   removed.
2. When all three contract identities are equal, the merge proceeds as today,
   matching tables by stable identity.
3. Otherwise, choose the merged contract by sequence, classify every table by
   signature, and apply the merge rule. A refusal happens before any effect.
4. Plan with the merged catalog. Tables are matched across base, source and
   target by stable identity; aliases come from the merged catalog. Today the
   merge matches tables by alias, so a renamed or dropped type fails.
5. When case 3 takes the source's version, the target adopts the source's pin.
   - Its validation delta is computed on keys only: inserted and deleted ids,
     and `src` and `dst` for edges. It feeds cross-table referential and
     cardinality checks against the merged state, including old and new
     endpoints of edges whose endpoint types were renamed.
   - Columns are never compared across contracts; today adoption compares rows
     by column name and fails or misreports, as the prototype showed.
   - Value constraints are not re-checked, because the adopted version is
     whole and was valid under its contract.
   - The adopted pin carries its own full-text declarations and index
     metadata.

Every supported migration keeps existing rows valid: added properties must be
nullable, only `@index` can be added as a constraint, enums only widen, and
type, nullability, key and other constraint changes are refused by the planner.
The adoption step relies on this to skip row re-validation. A future migration
that tightens validation without changing stored columns must either enter the
stored signature or refuse while a live branch has changed an affected table.
Adding a required or unique property is one such migration. So are several
steps proposed in the polymorphic types draft
([ModernRelay/omnigraph#884](https://github.com/ModernRelay/omnigraph/pull/884)):
narrowing an edge endpoint, removing `implements`, and adding an interface
uniqueness constraint.

### Merge publication

When the merged contract differs from the target's, the merge's one manifest
commit contains:

- the merged contract row, copied byte for byte from the side that holds it,
  with its sequence;
- `RenameTable` for every type the merged contract renames;
- `RegisterTable` and the source's pin for every type only the source
  registers;
- `Tombstone` for every type the merged contract drops;
- the merge's ordinary table updates.

Today merge publishes only table updates, and the publisher carries the
target's contract row forward. The fast-forward test compares contracts,
sequence included, so a catch-up fast forward publishes the contract too.

Before publishing, the merge checks that the contract it publishes has the
larger of the two sides' sequences. This turns the prototype's third failure,
a merge commit with an older contract than a parent, into an internal error
instead of a silent state.

The publisher refuses a `SchemaContract` change that carries no lineage
update, and refuses one whose sequence is lower than the branch's current
sequence. Ordinary writers detect a contract change through their exact
graph-head precondition, so every contract change must advance the head.

### Apply-time check

After planning, under the exclusive schema permit and before any table
effect, schema apply computes the tables it changes: those whose signature
differs between main's current and desired contracts. It then checks every
live branch:

1. List live branches once.
2. For each branch, open its `__manifest` and scan its hot rows once. Build its
   commit graph over the handle's shared history cache, as cleanup does.
3. Compute its merge base with main through `CommitGraph::merge_base`.
4. Read the base's table rows from the captured buffers when the base is
   buffered, else from one history record read. The merge capture path and
   `pinned_graph_commit` are not reused: the first refuses differing contracts
   and replaces a one-entry authority cache per branch, and the second opens a
   full snapshot. `Omnigraph::merge_base` and the merge dry run are built on
   the merge capture, so they accept differing contracts once merge does, but
   they build a full write capture per call. A base written by a since-deleted
   branch is accepted here.
5. Refuse when the branch's registration of a changed table differs from the
   base's. Tables the branch can already not merge because of an earlier
   apply are reported, not refused.
6. Re-list the branch inventory and compare each branch's incarnation and
   head with the first listing; a difference repeats the check for that
   branch.

The merge-base search loads both block registries and builds an index before
its bounded walk (100,000 visits or 32 MiB). Those limits do not cover that
loading. The check therefore runs under its own per-branch budget on registry
bytes, index size and history reads, fixed by the cost gate in `blocked_on`.
A branch that exceeds it, or the walk limits, refuses the apply with a
"cannot prove" message naming the branch.

Cheaper predicates were examined and rejected. The registration clock and the
staging witness misreport tables a branch adopted from main or from a parent
branch. Comparing with main's current pin refuses whenever main itself wrote
the table after the branch forked.

### Reads and maintenance

- Snapshot-id reads and the change-feed baseline bind the current contract of
  the branch that wrote the commit, which target resolution already records.
  Today they use the handle's bound branch, which is main in production, so a
  lagging branch's commits would be typed with main's contract. Historical
  reads bind properties by stable identity since
  [ModernRelay/omnigraph#959](https://github.com/ModernRelay/omnigraph/pull/959);
  that binding takes both its catalog and the live view that its coverage
  check compares pins with from the handle's bound branch, and both must come
  from the writing branch.
- This rule types queries. Decoding change images is a separate rule:
  [RFC 0030](0030-cdc-time-travel.md)'s phase C6, still design-stage, decodes
  each endpoint under its own archived schema, which this RFC leaves
  unchanged. Until C6 lands, a branch's change feed reports a schema boundary
  at the commit where the branch takes a contract that changes a table, as it
  does at a schema apply on main today.
- The accepted-catalog cache holds one entry today. It becomes a small cache
  keyed by contract identity, so serving branches on two contracts does not
  rebuild the catalog and miss the compiled-query cache on every switch.
- `optimize` and `repair` plan with the catalog of their captured authority,
  and write staging takes system-column spellings from the write's own
  catalog instead of the handle's.
- The system-column upgrade keeps its own only-main refusal, so every branch
  keeps one system-column spelling.

### Surfaces

- Schema apply drops its only-main checks in the apply, reconcile and
  settlement paths.
- Cluster deployment drops its preflight refusal; main's contract remains the
  deployment authority and drift check.
- `GET /schema` and `schema show` gain an optional branch. It is authorized as
  a read of that branch; today the route has no branch scope.
- Branch list and show gain the contract identity, the schema sequence and a
  behind-main flag.
- Merge errors gain `schema_diverged`; apply errors gain
  `branch_changed_table`.
- `omnigraph upgrade` gains the stamp-only route to 17.

### Contract changes that restrict access

Some contract changes narrow what an actor may read without changing a stored
column. The actor views draft
([ModernRelay/omnigraph#951](https://github.com/ModernRelay/omnigraph/pull/951))
places types and properties in the accepted contract. Under this RFC a
placement added on main would not reach a branch behind main, and restricted
actors could still read the newly placed data there. A change that restricts
access must therefore take effect on every branch at once. Either access
decisions bind from main's current contract by stable identity, or such a
change refuses while any other branch is live. `GET /schema?branch=` returns
the requesting actor's view of that branch's contract, and the actor-view
catalog memo and the accepted-catalog cache share one key.

### Lance behavior relied on

Reviewed against unmodified Lance 11.0.0 source and the table schema,
transaction, branch and tag, versioning and data evolution pages.

- A detached commit writes a new, randomly numbered manifest and never changes
  an existing version, so main's table effects cannot alter a branch's pin.
- `Project` (rename, drop) and `Merge` (nullable add over unchanged fragments)
  write no data file.
- Field ids are unique only within one line of versions. Two detached commits
  from the same version allocate the same next id, and Lance's schema page says
  a future branch merge "must not … match the fields by name." OmniGraph never
  combines data files from two lines of versions: a merge restages rows
  through keyed writes or adopts one whole pinned version. Reads bind columns
  by field id and ignore type, and `fix_schema` silently renumbers duplicate
  ids, so fragment grafting such as [RFC 0001](0001-fragment-adopt-branch-merge.md)
  would need a shared id allocator first.
- When a table version has no full-text segment, Lance searches it with its
  default simple tokenizer. OmniGraph's analyzer declaration, an untrained
  segment written by schema apply, is what carries the configured analyzer.
  This is why a full-text `@index` enters the stored signature.

## Invariants

- **2, one publication door:** schema apply still publishes once on main. A
  catch-up merge publishes the contract, table lifecycle changes and pins in
  its one manifest commit.
- **3, coherent view:** the merge captures each side's contract with its
  snapshot and the base's with the base, and revalidates both sides' contract
  identities, sequence included, before effects.
- **4, publishes once:** unchanged; the catch-up adds manifest changes, not a
  second publication.
- **5, crash convergence:** case 3 stages nothing for the taken table;
  adoption is a pointer change. Other tables keep today's detached staging,
  which a failed merge leaves unreachable.
- **6, stable identity:** tables and columns are matched by stable identity,
  aliases are only names, and no branch mints an identity.
- **7, derived state:** full-text declarations travel with the version that
  carries them, so a merge never leaves a full-text index without its
  analyzer. Other index coverage is reconciled by `ensure_indices`.
- **8, loud integrity failures:** every case the merge rule excludes is a
  typed refusal; the prototype's silent outcomes become refusals or internal
  errors.
- **10, trust:** the branch-scoped schema read is authorized per branch;
  other policy is unchanged (Unresolved questions).
- **11, bounded work:** the apply check runs under an explicit per-branch
  budget that covers registry loading, index construction and history reads.
- **12, one source of truth:** the contract row stays the authority. The
  schema sequence is part of that row, written only by the operations that
  write the row, not a parallel record.

The support boundary in [invariants](../dev/invariants.md) changes from "Schema
apply retains its main-only, single-live-branch restriction" to "Schema apply
is main-only; other branches keep their contract until a merge brings a newer
one into them". The apply check is a process-local precondition, not
distributed fencing; the merge rule is the authority. No deny-list shape is
added.

## Compatibility and reversibility

This is a storage format change. The internal manifest stamp becomes 17, and
the binary serves stamps 14 to 17. Each branch is stamped 17 by its first
publication under this binary, as stamps 15 and 16 were introduced, and
`omnigraph upgrade` gains the stamp-only route. Contract rows and lineage
rows without a sequence read as sequence 0, which is sound as stated in
Schema sequence; a merge between a branch still at 16 and one at 17 compares
0 with the other side's sequence. No open proposal claims stamp 17. The
integer node identity draft in
[ModernRelay/omnigraph#884](https://github.com/ModernRelay/omnigraph/pull/884)
also needs a stamp; whichever lands first takes 17.

A binary that serves stamps up to 16 refuses a graph whose main is stamped 17.
This is the fence divergent contracts need: such a binary assumes every branch
shares main's contract. Its historical reads match properties by name and
type, so a property dropped and re-added under the same name would return old
values without refusal. Any publication on main stamps it 17, so a graph
cannot hold a divergent contract while an older binary can still open it.

The wire changes are a new 409 reason, a new apply refusal, optional branch
parameters and new fields in branch listings.

There is no in-place downgrade. Rolling back means restoring a backup taken
before the first stamp-17 publication; writes after it are absent from that
backup. Reverting only the code that allows divergence, while keeping stamp
17, reinstates both refusals. Existing lagging branches then stay readable and
writable but cannot merge across contracts; an operator merges what still can
merge, then deletes or exports the rest.

## Alternatives

- **Keep the main-only rule.** No liability, but branches keep blocking every
  schema change.
- **Order contracts by content identity.** Equal identity does not mean the
  same point in history: a reversed change reproduces an earlier identity, and
  the order then undoes a later change.
- **Put the counter inside the SchemaIR.** Every apply would get a distinct
  identity, but the IR hash would no longer depend only on schema meaning, and
  cluster drift checks and prepared applies compare that hash with one computed
  from source. Keeping the counter beside the IR leaves the hash unchanged.
- **Compare only the two endpoint contracts.** A criss-cross base can be older
  than both sides, and the by-name row comparator then hides changes to
  re-created properties.
- **Convert rows between contracts during merge.** Renamed columns map by
  stable property id and added ones fill with null; only drops conflict. It
  needs projection of the base and older side into the newer schema inside the
  three-way cursors, plus new conflict kinds. It builds on everything this RFC
  adds and is deferred to Future work.
- **Apply to every branch in turn.** Lance has no multi-branch commit, so it
  cannot be one publication. It races each branch's writers, deletes branch
  data on a dropped type, and re-pins every branch's tables so later merges
  compare against an old-schema base, which needs row conversion anyway.
- **A schema-only sync for lagging branches.** Applying main's contract to a
  branch's own pins has the same re-pinning problem.
- **Schema apply on any branch.** Type, property and incarnation ids come from
  one counter per contract, and table paths derive from ids, so two branches
  evolving independently would collide on ids and on dataset paths. It needs a
  graph-wide id allocator and a three-way schema merge, each its own RFC.

## Evidence and tests

### Code survey

Each assumption above was checked in code at commit `a3af0c85`; the facts
quoted in Motivation and Design were rechecked at `f53bf4fb`, and the stamp,
the refusals, merge capture and the publisher again at `fe69c039`:

- Contract rows: only schema apply and the system-column upgrade produce
  `ManifestChange::SchemaContract` outside tests; the publisher carries the
  branch's row forward otherwise and refuses contract changes in unpublished
  commits and unattached snapshots.
- Identity: the contract identity hashes the serialized SchemaIR; renames keep
  ids and the allocator, and rename hints are not stored in the IR.
- Binding: `open_write_txn` and the branch arm of `capture_read_view` resolve
  the branch's own contract; the compiled-query cache is keyed by catalog
  object; the server re-typechecks each call against the branch.
- Physical isolation: branch writes stage detached on the dataset their
  inherited registration names; cleanup retains every live branch's head and
  creation version.
- Planner totality: added properties must be nullable, `AddConstraint` is
  limited to `@index`, enums only widen. Type renames, enum widening,
  non-full-text indexes and metadata changes have no table effect; a
  full-text `@index` declares its analyzer on the table.
- Merge: equal-contract capture, one catalog for all images, alias matching,
  a row comparator that matches columns by name, publication of `Update`
  only, publisher identity matching with an alias check, and a deterministic
  merge base with a smallest-id tie break.
- Stamps: binaries serve a closed range of main-manifest stamps and refuse a
  higher one before any read; stamps 15 and 16 were written by first
  publication.

### Prototype (2026-10-12)

At `a3af0c85`, a local patch lifted schema apply's only-main refusal and the
merge's equal-contract refusal and gave each merge side its own contract
identity. The standard test graph applied a migration on main while `feature`
existed, then merged:

| Case | Observed |
|---|---|
| Drop `Person.age`, merge main into untouched `feature` | `FastForward`; `feature` pins main's Person version, keeps `age` in its contract, opens cleanly, and fails `stored node:Person has no declared field 'age'` on query |
| Add `nickname`, same merge | Error before effects: missing column `nickname` |
| Rename `age` to `years`, same merge | Error before effects: missing column `years` |
| `feature` edits Alice, main drops `age`, merge main into `feature` | `Merged`; migration not applied; commit carries the older contract |
| `feature` adds a company, main drops `age`, merge `feature` into main | `Merged`; main keeps its contract and Person version; three companies |

A unit test ran the real `merge_base_search` and its bounded variant over three
hand-built histories, labeling each commit with an apply counter. Across all
294 ordered pairs, no base had a larger counter than either side. The test
also checked the earlier draft's rule, which ordered contracts by equality with
the base. That rule never named the smaller counter as newer, and it refused
two pairs: one criss-cross pair in each order, where the nearest common
ancestor predates both sides' catch-ups. The labels were distinct per apply,
so the test did not model a reversed change; content identity fails there,
which is why this design stores the sequence. The prototype is not committed;
the tests below replace it.

### Review

A model review of the first draft, checked against the code, found five gaps
that this revision closes:

- content identity cannot order contracts;
- an equal-contract merge over an older base can silently drop a write to a
  re-created property;
- a full-text `@index` is a table effect;
- older binaries are not safe on a graph with divergent contracts;
- the merge-base search's registry loading is not covered by its walk limits.

It also found the gaps in tests and surfaces listed below.

### Required tests

| Owner | Proof |
|---|---|
| `tests/schema_apply.rs` | Apply with a live untouched branch leaves its pins and contract; refusal names each branch and changed table; non-table changes never refuse; a full-text `@index` counts as a change; preview lists affected branches; budget and walk-limit refusals; incarnation recheck |
| `tests/integration/merge_truth_table.rs` | Each schema change in both directions between main and a branch and between two named branches, with and without the other side changing the table; type add, rename and drop; `Person → Human → Person` with a branch caught up to the middle contract; a property dropped and re-added under the same name with an older base; criss-cross refusal; the published contract has the larger sequence; the dry run reports the same outcome |
| `tests/merge_fast_forward.rs` | Catch-up fast forward publishes the contract and reads no rows of taken tables |
| `omnigraph-catalog` tests | The ordering property over every pair of commits, including content-equal contracts with different sequences; sequence 0 for pre-17 rows; the publisher refuses a lower sequence or a contract change without lineage |
| `tests/failpoints.rs`, `tests/detached_commit_matrix.rs` | A catch-up merge interrupted before publication leaves the branch's contract and pins; after publication both are complete |
| `tests/integration/point_in_time.rs`, `tests/integration/changes.rs` | Snapshot reads and change-feed baselines of a lagging branch's commits use that branch's contract; branch delete and recreate during snapshot binding |
| `tests/integration/search.rs` | Full-text search on a branch after catch-up uses the declared analyzer before any index rebuild |
| Unpublished and unattached owners | `as commit` catch-up refused before effects; unpublished commits and unattached snapshots rooted at a target expire on catch-up; candidates on unaffected branches are untouched |
| `tests/policy_engine_chassis.rs` | Catch-up authorization; branch-scoped schema read |
| Server `schema_routes`, `data_routes`, `mcp`; CLI `cli_schema_config` | `GET /schema?branch=` and its authorization, `schema show --branch`, the 409 reason, the apply refusal, stored queries on a lagging branch over HTTP and MCP |
| Cluster `tests.rs` | Deployment no longer refuses a graph with branches |
| `db/upgrade/tests.rs`, CLI `crossversion_upgrade.rs` | Stamp 17 on first publication and by the stamp-only route; a stamp-16 binary refuses a stamp-17 main |
| Other merge owners | Legacy pre-v11 table forks, Blob tables, inherited interface properties and renamed edge endpoints under a catch-up |
| `tests/integration/engine_v2_plan_replay.rs` | A plan bound before a catch-up merge refuses replay after it |
| DST harness | Schema operations with live branches; the model keeps one contract and sequence per branch |
| Cost instrument | The `blocked_on` gate |

Row-visible cases also belong in GQT, which first needs a schema-apply step.
The one-language draft
([ModernRelay/omnigraph#670](https://github.com/ModernRelay/omnigraph/pull/670))
proposes the statement such a step would use; that format change is separate.

## Rollout

1. Stamp 17 and the schema sequence: sequence 0 for older rows, increments by
   schema apply and the system-column upgrade, the publisher's sequence and
   lineage checks, and the upgrade route. Contracts still cannot diverge, so
   nothing else changes.
2. Bind snapshot reads, the change-feed baseline, `optimize`, `repair` and
   write staging to the right contract, and key the accepted-catalog cache by
   contract identity.
3. Teach merge the signature rule, identity matching, key-only adoption and
   contract publication. Production cannot reach it yet; tests create
   divergent contracts through a test-only path.
4. Lift the apply, reconcile, settlement and cluster refusals, and add the
   apply check, the surfaces, the user and developer docs, the operator guides
   and skills that state the main-only rule, the invariants support boundary
   and a changelog note.

`implementation` becomes `in-progress` with step 1 and `complete` after step 4.

## Future work: converting nullable property adds

Under this RFC, adding a nullable property refuses whenever a live branch
changed that type, although the change loses nothing. A follow-up amendment can
admit it. The merge would project base and older-side rows of such a table
into the newer schema by stable property id, filling the new column with null,
and run today's three-way comparison and conflict kinds. Only nullable
additions would qualify at first. Renames and drops stay under the strict rule
until their conversion and conflict semantics are specified.

## Unresolved questions

1. Should schema apply offer an override that applies despite branches that
   changed a table it changes, leaving them unable to merge it? The draft
   refuses with no override.
2. Is `BranchMerge` on the target enough for a catch-up merge that changes the
   target's contract, or does it also need `SchemaApply` on that branch? The
   draft requires only `BranchMerge`, since the change was authorized when it
   was applied to main.

## Decision log

- 2026-10-12: Drafted from a code survey of the schema apply, merge, publisher,
  catalog binding and collector paths, a review of the Lance 11.0.0 sources and
  pages named above, and the prototype recorded in Evidence and tests.
- 2026-10-12: Revised after review. A stored schema sequence replaces ordering
  by equality of content identity. A per-table stored-signature check over the
  base and both sides replaces the endpoint-only migrated set. Full-text
  `@index` declarations count as table effects. Storage stamp 17 fences older
  binaries, replacing the claim that they stay safe. The apply check gains a
  work budget and an incarnation recheck. The dropped-data wording,
  merges between named branches, branch-scoped schema reads and the test plan
  were corrected.
- 2026-10-12: Aligned with open proposals. The sequence lives beside the
  contract head so archived schema content stays content-only for the change
  feed contract (#964, which landed in RFC 0030 through #966).
  Historical-property binding (#959) takes its catalog and coverage view from
  the writing branch. Contract writers proposed in
  #884 increment the sequence, and its validation-tightening steps must enter
  the signature or refuse with live branches. Access-restricting contract
  changes from #951 take effect on every branch at once. Restore in #677 must
  not lower a branch's sequence.
