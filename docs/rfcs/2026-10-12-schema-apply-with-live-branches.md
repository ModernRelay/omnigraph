---
rfc: "2026-10-12-schema-apply-with-live-branches"
title: "Schema apply with live branches"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-12
updated: 2026-10-12
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Measure the apply-time branch check's manifest requests at 1, 16 and 256 live branches with a checked-in cost instrument."
---

# RFC: Schema apply with live branches

> A term set in ***bold italics*** is defined at that spot; it is plain text afterwards.

## Summary

Allow `schema apply` on main while other branches are live. Each branch keeps
the schema contract it has until a merge brings main's newer contract into it.
Schema apply itself stays main-only.

A merge between two branches with different contracts is allowed only when it
never has to combine data written under two schemas. A table is
***migrated*** when the change between the two contracts alters its stored
columns or drops its type. Every migrated table must be unchanged since the
merge base on the side with the older contract; the merge then uses the newer
side's version of that table whole and publishes the newer contract. Any other
cross-contract merge is refused with a typed error. Schema apply refuses up
front when a live branch has already changed a table the apply would migrate.

The design adds no stored state and no format change. It does not add schema
apply on named branches, row conversion between contracts, or a way to apply
one change to every branch.

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
  write a detached version of a shared dataset. Schema apply's metadata-only
  `Project` and `Merge` commits on main create new versions and leave every
  version another branch pins untouched.

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
live branch changed a table the plan migrates since its merge base with main.
That refusal names every such branch and table and gives two remedies: merge
the branch into main first, or delete it. Schema preview and plan report the
same list without refusing.

A change migrates a table when it adds, renames or drops one of the type's
properties, or drops the type. Renaming a type, extending an enum, adding an
`@index`, and editing descriptions or instructions migrate no table; those
changes never refuse because of a branch.

The check runs in the applying process. It does not fence writers in other
processes: a branch may still change a migrated table after the apply, and the
merge rule below then refuses that table.

### Working on a branch with an older contract

Reads, queries, mutations, loads, Blob writes, exports and index maintenance on
a branch use that branch's contract, as today. `schema show --branch <name>`
and `GET /schema?branch=<name>` show it. `branch list` and `branch show` report
each branch's contract identity and whether it is behind main's.

Stored queries are validated against main's contract at deployment. Running
one on a branch whose contract lacks a name it uses fails with the ordinary
query type error.

A read by snapshot or commit id uses the current contract of the branch that
wrote that commit, with tables rebound by stable identity as historical reads
already do.

### Merging across contracts

- **Main into a branch behind it** brings main's contract and main's changes.
  It is allowed when the branch changed no migrated table since the merge
  base. The resulting commit carries main's contract.
- **A branch behind main into main** is allowed under the same condition.
  Main's contract does not change.
- Any other merge between different contracts is refused with HTTP 409 and
  reason `schema_diverged`, naming each table and the side that changed it.
- When neither side's contract equals the merge base's contract, which a
  criss-cross history can produce, the merge is refused with the same reason.
  Merging main into each side first resolves it.
- A merge requested `as commit` is refused when it would change the target's
  contract; unpublished commits carry no schema change.

A branch that changed a migrated table cannot merge that table in either
direction. Its rows stay readable and exportable on the branch; the work is
redone on a branch created from main.

### Dropped data

`optimize` and `cleanup` erase a dropped property's values once no retained
version references the old data files. A live branch on an older contract
retains those versions, so the values persist until the branch merges main or
is deleted. This already applies to retained versions; live branches make it
common.

## Design

### Contract order

Only schema apply and the system-column upgrade write a `schema_contract` row,
and both publish on main. Every merge publishes the newer of its two parents'
contracts (Merge publication). Contracts therefore never decrease along
ancestry, and every contract a branch holds is one main held.

A merge reads three contracts: source, target and the merge base's. The merge
base is the existing deterministic best common ancestor. The order follows
from them:

| Base, source, target | Result |
|---|---|
| source equals target | No contract work; today's merge. |
| base equals source, target differs | Target is newer. |
| base equals target, source differs | Source is newer. |
| base equals neither | Refuse with `schema_diverged`. |

Because contracts never decrease along ancestry, a base equal to one side
proves the other side is newer. No stored epoch or ordering field is needed.
The base's contract identity comes from the base snapshot the merge already
opens when the target head is not the base, or from the base commit's lineage
row, which records it.

### Migrated tables

The merge compares the older and newer accepted SchemaIR by stable table
identity. A table is migrated when the newer IR lacks it, or when its stored
column list differs: property identity, name, type and nullability in catalog
order, which is what the physical Arrow schema of each catalog records.

`plan_schema_migration` is not used for this. Two accepted IRs that differ only
by a property added and later dropped produce an unclassified-change step
there, while the column comparison correctly reports no migration.

The rule holds for a migrated table when the older side's registration equals
the base's by identity: same path, published version, native ref, row count
and version metadata. The registration clock and alias are ignored, which is
what the merge's existing `same_manifest_state` comparison does.

### Merge planning

1. Capture the source and target contracts and catalogs separately, each
   validated against its own snapshot, and record each side's contract
   identity in its own authority token. The equal-contract refusal in merge
   capture is removed.
2. Order the contracts. If they differ, compute the migrated set and check the
   rule; a failure refuses before any effect.
3. Plan with the newer catalog. Tables are matched across base, source and
   target by stable identity, and aliases come from the newer catalog. Today
   the merge matches tables by alias, so a renamed or dropped type fails.
4. A table that is not migrated has the same stored columns in all three
   images, because the base holds the older side's contract. The existing
   classification and staging routes apply to it unchanged.
5. A migrated table takes the newer side's version.
   - When the newer side is the target, the older side is unchanged since the
     base, which the existing classifier already skips.
   - When the newer side is the source, the target adopts the source's pin.
     Its validation delta is computed on keys only: inserted and deleted ids,
     and `src` and `dst` for edges. It feeds cross-table referential and
     cardinality checks against the merged state. Columns are never compared
     across contracts; today adoption compares rows by column name and fails
     or misreports as the prototype showed. Value constraints are not
     re-checked, because the adopted version is whole and was valid under the
     newer contract.

Every supported migration keeps existing rows valid: added properties must be
nullable, only `@index` can be added as a constraint, enums only widen, and
type, nullability, key and other constraint changes are refused by the planner.
The catch-up merge relies on this to skip row re-validation. A future migration
that tightens validation, such as adding a required or unique property, must
re-validate the older side's rows or extend the migrated set.

### Merge publication

When the target holds the older contract, its one manifest commit contains:

- the newer contract row, copied byte for byte from the source;
- `RenameTable` for every renamed type;
- `RegisterTable` and the source's pin for every added type;
- `Tombstone` for every dropped type;
- the merge's ordinary table updates.

Today merge publishes only table updates, and the publisher carries the
target's existing contract row forward. The fast-forward test compares
contracts, so a catch-up fast forward must publish the contract too.

Before publishing, the merge checks that the contract it will publish is the
newer parent's. This turns the prototype's third failure, a merge commit with
an older contract than a parent, into an internal error instead of a silent
state.

The publisher refuses a `SchemaContract` change that carries no lineage
update. Ordinary writers detect a contract change through their exact
graph-head precondition, so every contract change must advance the head.

### Apply-time check

After planning, under the exclusive schema permit and before any table effect,
schema apply checks every live branch:

1. List live branches once.
2. For each branch, open its `__manifest` and scan its hot rows once. Build its
   commit graph over the handle's shared history cache, as cleanup does.
3. Compute its merge base with main through `CommitGraph::merge_base`.
4. Read the base's table rows from the captured buffers when the base is
   buffered, else from one history record read. The merge capture path and
   `pinned_graph_commit` are not reused: the first refuses differing contracts
   and replaces a one-entry authority cache per branch, and the second opens a
   full snapshot. A base written by a since-deleted branch is accepted here.
5. Compare each table the plan migrates by identity.
6. Re-list the branch inventory and compare heads, as the collector does, to
   narrow the window for in-process changes.

A merge-base walk that exceeds the existing ancestry limits (100,000 visits or
32 MiB) refuses the apply with a "cannot prove" message naming the branch.

Cheaper predicates were examined and rejected. The registration clock and the
staging witness misreport tables a branch adopted from main or from a parent
branch, and comparing with main's current pin refuses whenever main itself
wrote the table after the branch forked.

### Reads and maintenance

- Snapshot-id reads and the change-feed baseline bind the current contract of
  the branch that wrote the commit, which target resolution already records.
  Today they use the handle's bound branch, which is main in production, so a
  lagging branch's commits would be typed with main's contract.
- The accepted-catalog cache holds one entry today. It becomes a small cache
  keyed by contract identity, so serving branches on two contracts does not
  rebuild the catalog and miss the compiled-query cache on every switch.
- `optimize` and `repair` plan with the catalog of their captured authority,
  and write staging takes system-column spellings from the write's own
  catalog instead of the handle's.
- The system-column upgrade keeps its own only-main refusal, so every branch
  keeps one system-column spelling.

### Surfaces

Schema apply drops its only-main checks in the apply, reconcile and
settlement paths. Cluster deployment drops its preflight refusal; main's
contract remains the deployment authority and drift check. `GET /schema` and
`schema show` gain an optional branch. Branch list and show gain the contract
identity and a behind-main flag. Merge errors gain `schema_diverged`, and
apply errors gain `branch_changed_migrated_table`.

### Lance behavior relied on

Reviewed against unmodified Lance 11.0.0 source and the table schema,
transaction, branch and tag, versioning and data evolution pages.

- A detached commit writes a new, randomly numbered manifest and never changes
  an existing version, so main's evolution cannot alter a branch's pin.
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

## Invariants

- **2, one publication door:** schema apply still publishes once on main. A
  catch-up merge publishes the contract, table lifecycle changes and pins in
  its one manifest commit.
- **3, coherent view:** the merge captures each side's contract with its
  snapshot and the base's with the base, and revalidates both sides' contract
  identities before effects, as it revalidates heads today.
- **4, publishes once:** unchanged; the catch-up adds manifest changes, not a
  second publication.
- **5, crash convergence:** the catch-up stages nothing new for migrated
  tables; adoption is a pointer change. Other tables keep today's detached
  staging, which a failed merge leaves unreachable.
- **6, stable identity:** tables are matched by stable identity, aliases are
  only names, and no branch mints an identity.
- **7, derived state:** an adopted version brings its own index metadata;
  missing coverage is reconciled by `ensure_indices`.
- **8, loud integrity failures:** every cross-contract case the rule excludes
  is a typed refusal. The prototype's silent outcomes become refusals or
  internal errors.
- **10, trust:** policy is unchanged; see Unresolved questions.
- **11, bounded work:** the apply check costs one manifest open and scan per
  live branch, plus the existing bounded merge-base walk.
- **12, one source of truth:** contract rows remain the authority; order is
  derived from lineage and nothing is stored beside them.

The support boundary in [invariants](../dev/invariants.md) changes from "Schema
apply retains its main-only, single-live-branch restriction" to "Schema apply
is main-only; other branches keep their contract until a merge brings main's
contract into them". The apply check is a process-local precondition, not
distributed fencing; the merge rule is the authority. No deny-list shape is
added.

## Compatibility and reversibility

There is no storage or format change: per-branch contract rows exist since
storage format 13, and graphs stay at the current stamp. The wire changes are
a new 409 reason and optional branch parameters.

An older binary on a graph with lagging branches stays safe by refusal. It
refuses merges across contracts, refuses schema apply while branches exist,
and serves live branch reads with each branch's own contract. Its snapshot-id
reads of a lagging branch's commits use main's contract and fail loudly on
changed properties.

Reverting the code reinstates both refusals. Existing lagging branches remain
readable and writable but cannot merge across contracts. An operator merges
what still can merge, then deletes or exports the rest.

## Alternatives

- **Keep the main-only rule.** No liability, but branches keep blocking every
  schema change.
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
  branch's own pins has the same re-pinning problem: every later merge of those
  tables becomes a cross-schema comparison against the old base.
- **Schema apply on any branch.** Type, property and incarnation ids come from
  one counter per contract, and table paths derive from ids, so two branches
  evolving independently would collide on ids and on dataset paths. It needs a
  graph-wide id allocator and a three-way schema merge, each its own RFC.
- **A stored contract epoch.** The base-equality order with refusal already
  orders every merge the rule allows.

## Evidence and tests

### Code survey

Each assumption above was checked in code at commit `a3af0c85`:

- Contract rows: only schema apply and the system-column upgrade produce
  `ManifestChange::SchemaContract` outside tests; the publisher carries the
  branch's row forward otherwise and refuses contract changes in unpublished
  commits.
- Binding: `open_write_txn` and the branch arm of `capture_read_view` resolve
  the branch's own contract; the compiled-query cache is keyed by catalog
  object; the server re-typechecks each call against the branch.
- Physical isolation: branch writes stage detached on the dataset their
  inherited registration names; cleanup retains every live branch's head and
  fork point.
- Planner totality and the catalog-only steps (`RenameType`, `ExtendEnum`,
  `@index`, metadata): no table effect, no type name in the stored schema, and
  `AddConstraint` limited to `@index`.
- Merge: equal-contract capture, one catalog for all images, alias matching,
  adoption's by-name row comparison, publication of `Update` only, publisher
  identity matching with an alias check, and the deterministic merge base with
  a smallest-id tie break.

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
hand-built histories with contract labels, checking every ordered pair of
commits. No base had a newer contract than either side, and the base-equality
order never named the older side as newer. Two of 294 ordered pairs were
refused, one criss-cross pair in each order. There, two feature branches had
merged each other and then caught up to main commits four commits apart. The
nearest common ancestor, a branch commit two hops from each head, predates
both catch-ups; the main commit holding the older side's contract is six hops
away in total. The prototype is not committed; the tests below replace it.

### Required tests

| Owner | Proof |
|---|---|
| `tests/schema_apply.rs` | Apply with a live untouched branch leaves its pins and contract; refusal names each branch and migrated table; non-migrating changes never refuse; preview lists affected branches; walk-limit refusal |
| `tests/integration/merge_truth_table.rs` | Every migration kind in both directions, with the older side changing or not changing the migrated table; type add, rename and drop in catch-up; criss-cross refusal; the commit carries the newer contract |
| `tests/merge_fast_forward.rs` | Catch-up fast forward publishes the contract and reads no rows of migrated tables |
| `omnigraph-catalog` commit graph tests | The contract-order property over every pair of commits |
| `tests/failpoints.rs`, `tests/detached_commit_matrix.rs` | A catch-up merge interrupted before publication leaves the branch's contract and pins; after publication both are complete |
| `tests/integration/point_in_time.rs`, `tests/integration/changes.rs` | Snapshot reads and change-feed baselines of a lagging branch's commits use that branch's contract |
| `tests/policy_engine_chassis.rs` | The catch-up merge's authorization |
| Server `schema_routes`, `data_routes`; CLI `cli_schema_config` | `GET /schema?branch=`, `schema show --branch`, the 409 reason and the apply refusal |
| Cluster `tests.rs` | Deployment no longer refuses a graph with branches |
| DST harness | Schema operations with live branches; the model keeps one contract per branch |
| Cost instrument | The `blocked_on` gate: manifest requests of the apply check per live branch |

Row-visible cases also belong in GQT, which first needs a schema-apply step;
that format change is separate.

## Rollout

1. Bind snapshot reads, the change-feed baseline, `optimize`, `repair` and
   write staging to the right contract, and key the accepted-catalog cache by
   contract identity. No behavior changes while all contracts are equal.
2. Teach merge the cross-contract rule, identity matching, key-only adoption
   and catch-up publication. Production cannot reach it yet; tests create
   divergent contracts through a test-only path.
3. Lift the apply, reconcile, settlement and cluster refusals, add the apply
   check, the surfaces, the user and developer docs, the invariants support
   boundary and a changelog note.

`implementation` becomes `in-progress` with step 1 and `complete` after step 3.

## Future work: converting nullable property adds

Under this RFC, adding a nullable property refuses whenever a live branch
changed that type, although the change loses nothing. A follow-up amendment can
admit it: the merge projects base and older-side rows of such a table into the
newer schema by stable property id, filling the new column with null, and runs
today's three-way comparison and conflict kinds. Only nullable additions would
qualify at first. Renames and drops stay under the strict rule until their
conversion and conflict semantics are specified.

## Unresolved questions

1. Should schema apply offer an override that applies despite branches that
   changed a migrated table, leaving them unable to merge it? The draft
   refuses with no override.
2. Is `BranchMerge` on the target enough for a catch-up merge that changes the
   target's contract, or does it also need `SchemaApply` on that branch? The
   draft requires only `BranchMerge`, since the change was authorized when it
   was applied to main.

## Decision log

- 2026-10-12: Drafted from a code survey of the schema apply, merge, publisher,
  catalog binding and collector paths, a review of the Lance 11.0.0 sources and
  pages named above, and the prototype recorded in Evidence and tests.
