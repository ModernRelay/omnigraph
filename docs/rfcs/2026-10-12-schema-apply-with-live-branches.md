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
  - "Measure the apply-time branch check and cross-contract merge preparation (requests, bytes, time) at 1, 16 and 256 live branches and across growing released history, cold and warm, with a checked-in cost instrument, and fix the check's work budget from that measurement."
---

# RFC: Schema apply with live branches

> A term set in ***bold italics*** is defined at that spot; it is plain text afterwards.

## Summary

Allow `schema apply` on main while other branches are live. Each branch keeps
the schema contract it has until a merge brings a newer one into it. Schema
apply itself stays main-only.

Every contract gains a ***schema sequence***: a counter that each contract
write on main increments and that merges carry forward. It orders contracts.
Content identity cannot, because a change and its reversal reproduce an
earlier identity.

A merge between branches with different contracts brings every version it
compares, the merge base included, to the newer contract with the same
metadata-only Lance commits schema apply uses. It then runs today's merge
unchanged. Added nullable properties, renames and full-text index
declarations are reconciled this way; conflicts are today's row-level
conflicts. The one refusal is a drop: a merge, and a schema apply on main, is
refused when it would drop a property or type that a branch changed since its
merge base, because the branch's changes would be lost.

The counter needs storage stamp 17, written by each branch's first
publication. Older binaries refuse the graph once main carries it. The design
does not add schema apply on named branches, conversion of rows outside
Lance's own schema evolution, or a way to apply one change to every branch.

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
  an internal "schema-gated change row is missing column" error, because the
  merge compares rows by column name across versions with different columns;
- after the branch had edited the migrated table, the merge returned `Merged`
  without applying the migration, so a commit with a newer-contract parent
  carried the older contract.

Under this RFC the common migrations no longer wait for branches. Adding a
nullable property to a type that agent branches write, renaming a property,
and declaring a full-text index all apply while those branches are live, and
the branches merge afterwards with ordinary row-level conflicts. Dropping a
property or type that a live branch changed still refuses until that branch is
merged or deleted.

## User and operational behavior

### Applying while branches are live

`omnigraph schema apply` on main succeeds while other branches exist. It
refuses only when it would drop a property or type that a live branch changed
since its merge base with main. The refusal names each such branch and table
and gives two remedies: merge the branch into main first, or delete it. Schema
preview and plan report the same list without refusing. A preview's list is
advisory, including a stored deployment preview; the check at apply is
authoritative.

A branch ***changed*** a table when its rows differ from the merge base's
rows. Index maintenance, compaction and other physical work that leaves the
rows equal does not count.

The check runs in the applying process and does not fence writers in other
processes. A branch may still change such a table after the apply; the merge
then refuses that table.

### Working on a branch with an older contract

Reads, queries, mutations, loads, Blob writes, exports and index maintenance on
a branch use that branch's contract, as today. `schema show --branch <name>`
and `GET /schema?branch=<name>` show it and require read access to that
branch. `branch show` reports the branch's contract identity, its schema
sequence and whether it is behind main's; `branch list` does not, so listing
stays one ref read per branch.

Stored queries are validated against main's contract at deployment. Running
one on a branch whose contract lacks a name it uses, through the CLI, HTTP or
MCP, fails with the ordinary query type error.

A read by snapshot or commit id is typed with main's current contract, as
today, with tables and properties bound by stable identity. Main always holds
the newest contract, so every table and property any commit can reference is
either in it or was dropped by it. A property the commit's version lacks is
refused, as historical reads already refuse it. To read a lagging branch's
data under that branch's own contract, read the branch.

### Merging across contracts

A merge between any two branches, main included:

- publishes the contract with the larger schema sequence;
- brings the base, source and target versions of each table whose stored
  columns or full-text declarations differ to that contract, without
  rewriting data files;
- then merges rows as today, with today's conflict kinds;
- is refused with HTTP 409 and reason `schema_diverged` when the newer
  contract drops a property or type that one side changed since the merge
  base. The error names each table and side.

`branch merge --dry-run` runs the same capture and classification and reports
the same outcome, including `schema_diverged` and the contract the merge would
publish, without effects.

A merge requested `as commit` is refused before any effect when it would
change the target's contract; unpublished commits carry no schema change. A
catch-up merge advances the target's head, so unpublished commits and
unattached snapshots rooted at the earlier head expire as they do after any
other publication. A catch-up merge can tombstone a type on its target when
the newer contract dropped it.

A branch that changed a type or property that main later drops cannot merge
that table in either direction. Its rows stay readable and exportable on the
branch; the work is redone on a branch created from main.

### Dropped data

`optimize` and `cleanup` erase a dropped property's values once no retained
version references the old data files. Cleanup retains every live branch's
head and the version it was created from, so the values persist until every
branch created before the drop is deleted. Merging main into such a branch
does not release its creation version.

### Older binaries

The first publication by this release stamps a branch's `__manifest` 17.
Binaries that serve stamps up to 16 refuse a stamped `__manifest` on every
scan and before every publication, not only at open. An older process that
already holds the graph open therefore fails at its next read or write of
main once main is stamped. It can still work on a branch that is still at
stamp 16, under that branch's own contract, which this RFC permits.
`omnigraph upgrade` stamps every live branch at once for operators who want
older processes fenced from every branch.

## Design

### Schema sequence

The `schema_contract` row gains `schema_sequence: u64`, stored beside the
contract head rather than inside it. The archived schema object serializes the
head with the source and IR, so keeping the sequence outside it leaves
`schema_content_hash` and every archived object a function of schema content
alone. Graph lineage rows and `__history` extents do not carry the sequence:
the merge orders the two sides by their live contract rows and compares the
base by content, so no historical sequence is read.

- Every operation that writes a contract on main publishes it with main's
  sequence plus one. Today those are schema apply and the system-column
  upgrade; a future contract writer, such as the node identity upgrade
  proposed in [ModernRelay/omnigraph#884](https://github.com/ModernRelay/omnigraph/pull/884),
  must do the same.
- A merge publishes the contract of the side with the larger sequence. Equal
  sequences on the two sides must name byte-identical contract rows; a
  difference is an integrity error.
- Every other publication carries the branch's contract row unchanged, as
  today.
- A contract row written before stamp 17 has no sequence and reads as 0.

Sequence 0 is sound because no two live branches' contract rows that both read
0 differ. Every contract write before this release required a graph with only
main, so all live branches shared one contract until the first stamped write.
A merge base may still hold a sequence-0 contract with different content, for
example from main's history before an earlier apply. That is expected: the
base is compared by content, never by sequence.

The sequence is part of the contract's identity for comparison:
`same_graph_state`, the authority token and the accepted-catalog cache key
include it. Two contracts with equal content but different sequences are
different occurrences. `SchemaContractDigest`, which the cluster ledger
persists and prepared applies compare, stays content-only. A prepared apply
already binds the base manifest version, branch identifier and graph head, so
a reversed change cannot satisfy a stale preparation, and the ledger format
does not change.

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

### Stored signature and evolution steps

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
marker).

For each table, the merge compares each version's signature with the merged
contract's and derives the ***evolution*** between them, matched by stable
property id. A step is ***lossless*** when it adds a nullable column, renames
a column, or adds a full-text declaration. It is ***lossy*** when it drops a
column or the merged contract drops the type. A version that does not register
a table the merged contract registers, such as the base of a type added since,
is an empty version of it. Every step schema apply supports is one of these
five. `plan_schema_migration` is not used to derive them. Two
accepted IRs that differ only by a property added and later dropped produce an
unclassified-change step there, while the signature comparison correctly
reports no difference.

### Merge rule

1. The merged contract is the side's contract with the larger sequence.
2. For each table, a lossy step on the source's or target's version is
   allowed only when that side did not change the table since the merge base
   (Logical change). Otherwise the merge is refused with `schema_diverged`
   before any effect. A lossy step on the base's version alone is always
   allowed: neither side holds the dropped data.
3. Every version whose signature differs from the merged contract's is brought
   to it (Merge preparation).
4. The merge runs today's classification, staging, validation and
   publication against the merged catalog, matching tables across the three
   versions by stable identity.

Because every version has the same stored columns after step 3, the merge's
by-name row comparison is sound. A criss-cross base that predates both sides'
contracts is handled the same way. For example, a property dropped and
re-added under the same name gets a new property id. The base's old column is
dropped and the new one added as null in the base's prepared version, so two
sides that each wrote the new property conflict as a `DivergentUpdate` instead
of one value silently winning.

### Logical change

Whether a side changed a table since the base depends only on rows. The
existing cheap paths answer first: equal registrations, then the merge's
lineage candidate discovery, which compares the base's and the side's data,
overlay and deletion files by identity and reports no candidates when they
match. Index-only commits and schema evolution keep those files, so they cost
nothing here.

Only when candidates remain does the merge compare them. It brings the base's
version to the side's signature with the same preparation and runs the ordered
comparison over the candidates. Compaction therefore changes cost, not the
answer. The comparison runs under the merge's existing memory and spill
bounds, and only for a table where a lossy step meets a side whose files
differ from the base's.

### Merge preparation

Bringing a version to the merged contract uses `TableStore::stage_schema_evolution`
on that version, exactly as schema apply evolves main's tables:

- renamed and dropped columns commit as a detached `Project`;
- added nullable columns commit as a detached `Merge` over the unchanged
  fragments;
- a full-text declaration is staged as schema apply stages it.

No data file is written and no row is read. Lance reads an added column as
null in fragments that lack it. A version prepared only for comparison, such
as the base's, is never published: it is a detached commit that no
`__manifest` version names, and `cleanup` reclaims it under the existing
staging-witness rules. A version that becomes a result is published by the
merge's one manifest commit:

- When the result stages rows onto the target, the target's prepared version
  is the base of that chain.
- When the result adopts the source's version, the target registers the
  source's prepared version.
- An adopted version keeps its own full-text declarations and index metadata,
  and its validation delta is computed between the prepared base and the
  prepared source, which share a schema.

Every supported migration keeps existing rows valid: added properties must be
nullable, only `@index` can be added as a constraint, enums only widen, and
type, nullability, key and other constraint changes are refused by the planner.
Validation therefore checks only the merge's selected delta under the merged
catalog, as today. A future migration that tightens validation without
changing stored columns must either enter the stored signature as a lossy step
or refuse while a live branch has changed an affected table. Adding a required
or unique property is one such migration. So are several steps proposed in the
polymorphic types draft
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
- the merge's ordinary table updates, whose pins name prepared versions where
  a table needed preparation.

Today merge publishes only table updates, and the publisher carries the
target's contract row forward. The fast-forward test compares contracts,
sequence included, so a catch-up fast forward publishes the contract too.

Before publishing, the merge checks that the contract it publishes has the
larger of the two sides' sequences. This turns the prototype's third failure,
a merge commit with an older contract than a parent, into an internal error
instead of a silent state.

The publisher refuses a `SchemaContract` change that carries no lineage
update, and refuses one whose sequence is lower than the branch's current
sequence. Ordinary writers detect a contract change twice: write revalidation
compares the captured contract identity, and the exact graph-head
precondition fails because every contract change advances the head. Catalog
and engine tests that commit a contract-only change without lineage through
`commit_changes` move to the new rule.

### Apply-time check

The check is the merge rule's step 2, evaluated for each live branch against
main's planned contract. It shares the merge's implementation, so an apply
refusal and a later merge refusal cannot disagree. After planning, under the
exclusive schema permit and before any table effect:

1. Compute the tables the apply drops or drops columns from. Only these can
   refuse.
2. List live branches once. For each branch that registers such a table, open
   its `__manifest` and scan its hot rows once, build its commit graph over the
   handle's shared history cache, and compute its merge base with main.
3. Read the base's table rows from the captured buffers when the base is
   buffered, else from one history record read. The merge capture path and
   `pinned_graph_commit` are not reused: the first replaces a one-entry
   authority cache per branch and builds a full write capture, and the second
   opens a full snapshot. `Omnigraph::merge_base` and the merge dry run are
   built on the merge capture, so they inherit the merge's rule but cost more
   per branch. A base written by a since-deleted branch is accepted here.
4. Apply Logical change to each such table. Refuse when the branch changed
   one. Tables a branch can already not merge because of an earlier apply are
   reported, not refused.
5. Re-list the branch inventory and compare each branch's incarnation and head
   with the first listing; a difference repeats the check for that branch.

The merge-base search loads both block registries and builds an index before
its bounded walk (100,000 visits or 32 MiB). Those limits do not cover that
loading. The check therefore runs under its own per-branch budget on registry
bytes, index size, history reads and comparison work, fixed by the cost gate
in `blocked_on`. A branch that exceeds it, or the walk limits, refuses the
apply with a "cannot prove" message naming the branch.

Cheaper predicates were examined and rejected. Registration equality alone
lets index maintenance flip the outcome. The registration clock and the
staging witness misreport tables a branch adopted from main or from a parent
branch. Comparing with main's current pin refuses whenever main itself wrote
the table after the branch forked.

### Reads and maintenance

- Reads by snapshot or commit id keep today's binding to main's current
  contract, with tables and properties bound by stable identity since
  [ModernRelay/omnigraph#959](https://github.com/ModernRelay/omnigraph/pull/959).
  The rule is total: it needs no writing branch, so it holds for commits of
  deleted or recreated branches, unpublished commits and unattached
  snapshots, and it never infers identity from a branch name. Main is the
  binding the handle uses in production; a handle bound elsewhere binds main
  explicitly.
- A live branch's change-feed baseline reads that branch's head and binds that
  branch's contract while the incarnation captured at the cut is live. The
  feed already re-proves the incarnation and ends when it changes.
- Decoding change images is a separate rule: [RFC 0030](0030-cdc-time-travel.md)'s
  phase C6, still design-stage, decodes each endpoint under its own archived
  schema, which this RFC leaves unchanged. Until C6 lands, a branch's change
  feed reports a schema boundary at the commit where the branch takes a
  contract that changes a table, as it does at a schema apply on main today.
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
- `branch show` gains the contract identity, the schema sequence and a
  behind-main flag.
- Merge errors gain `schema_diverged`; apply errors gain
  `branch_changed_dropped_table`.
- `omnigraph upgrade` gains the stamp-only route to 17.
- `openapi.json` is regenerated for the new parameter, fields and reasons.

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
  an existing version, so main's table effects and a merge's preparation
  cannot alter another version.
- `Project` (rename, drop) and `Merge` (nullable add over unchanged fragments)
  write no data file. A field that no data file of a fragment holds reads as
  null, and a field id the schema no longer lists is not read.
- Field ids are unique only within one line of versions. Two detached commits
  from the same version allocate the same next id, and Lance's schema page says
  a future branch merge "must not … match the fields by name." OmniGraph never
  combines data files from two lines of versions: a merge restages rows
  through keyed writes or adopts one whole version, and preparation evolves
  each version along its own line. Reads bind columns by field id and ignore
  type, and `fix_schema` silently renumbers duplicate ids, so fragment
  grafting such as [RFC 0001](0001-fragment-adopt-branch-merge.md) would need a
  shared id allocator first.
- When a table version has no full-text segment, Lance searches it with its
  default simple tokenizer. OmniGraph's analyzer declaration, an untrained
  segment written by schema apply, is what carries the configured analyzer,
  as the guards from
  [ModernRelay/omnigraph#905](https://github.com/ModernRelay/omnigraph/pull/905)
  pin. This is why a full-text `@index` enters the stored signature.

## Invariants

- **2, one publication door:** schema apply still publishes once on main. A
  cross-contract merge publishes the contract, table lifecycle changes and
  prepared pins in its one manifest commit.
- **3, coherent view:** the merge captures each side's contract with its
  snapshot and the base's with the base, and revalidates both sides' contract
  identities, sequence included, before effects.
- **4, publishes once:** unchanged; preparation adds detached commits, not a
  publication.
- **5, crash convergence:** prepared versions are detached commits that no
  `__manifest` version names until the merge publishes. A failed merge leaves
  them unreachable, and `cleanup` reclaims them under the existing staging
  witness.
- **6, stable identity:** tables and columns are matched by stable identity,
  aliases are only names, snapshot reads never infer identity from a branch
  name, and no branch mints an identity.
- **7, derived state:** whether a merge or apply refuses depends only on rows.
  Index maintenance, compaction and fragment layout change its cost.
  Full-text declarations travel with the version that carries them.
- **8, loud integrity failures:** the only cross-contract refusal is typed,
  and the prototype's silent outcomes become ordinary merges or refusals.
- **10, trust:** the branch-scoped schema read is authorized per branch;
  access-restricting contract changes reach every branch at once.
- **11, bounded work:** the apply check runs under an explicit per-branch
  budget covering registry loading, index construction, history reads and
  comparison work; merge preparation is metadata-only per table.
- **12, one source of truth:** the contract row stays the authority. The
  schema sequence is part of that row, written only by the operations that
  write the row, not a parallel record.

The support boundary in [invariants](../dev/invariants.md) changes from "Schema
apply retains its main-only, single-live-branch restriction" to "Schema apply
is main-only; other branches keep their contract until a merge brings a newer
one into them". The apply check is a process-local precondition, not
distributed fencing; the merge rule is the authority. No deny-list shape is
added: refusals never depend on index coverage, fragment layout or staged
state.

## Compatibility and reversibility

This is a storage format change. The internal manifest stamp becomes 17, and
the binary serves stamps 14 to 17. Each branch is stamped 17 by its first
publication under this binary, as stamps 15 and 16 were introduced, and
`omnigraph upgrade` gains the stamp-only route. The change is one field in the
`schema_contract` row; lineage rows, `__history` extents, archived schema
objects and the cluster ledger are unchanged. Rows without a sequence read as
0, which is sound as stated in Schema sequence; a merge between a branch still
at 16 and one at 17 compares 0 with the other side's sequence. No open
proposal claims stamp 17. The integer node identity draft in
[ModernRelay/omnigraph#884](https://github.com/ModernRelay/omnigraph/pull/884)
also needs a stamp; whichever lands first takes 17.

A binary that serves stamps up to 16 refuses a stamped `__manifest` on every
scan and before every publication. This is the fence divergent contracts
need: such a binary assumes every branch shares main's contract and knows
neither the sequence nor prepared versions. Any publication on main stamps it
17, so a graph cannot hold a divergent contract while an older binary can
still read or write main.

The wire changes are a new 409 reason, a new apply refusal, an optional branch
parameter and new fields on `branch show`.

There is no in-place downgrade. Rolling back means restoring a backup taken
before the first stamp-17 publication; writes after it are absent from that
backup. Reverting only the code that allows divergence, while keeping stamp
17, reinstates both refusals. Existing lagging branches then stay readable and
writable but cannot merge across contracts; an operator merges what still can
merge, then deletes or exports the rest.

## Alternatives

- **Keep the main-only rule.** No liability, but branches keep blocking every
  schema change.
- **Take one side's version whole and refuse otherwise.** The first revision
  of this RFC refused whenever both sides changed a table whose columns
  differed, and decided "changed" by registration. Index maintenance on a
  branch then flipped outcomes, which invariant 7 and the deny-list forbid,
  and the most common migration, a nullable add on a type branches write,
  stayed blocked. Preparing every version removes both problems with less
  merge-specific code.
- **Convert rows inside the merge's cursors.** Mapping columns by property id
  and filling added ones with null in the cursors reproduces what Lance's own
  schema evolution already does at read, and adds a second conversion path
  that every route must use. Preparation reuses schema apply's evolution.
- **Decide "changed" by data-file identity.** Compaction rewrites files
  without changing rows, so outcomes would still depend on layout. File
  identity stays the cheap path, with a row comparison behind it.
- **Order contracts by content identity.** Equal identity does not mean the
  same point in history: a reversed change reproduces an earlier identity, and
  the order then undoes a later change.
- **Put the counter inside the SchemaIR, or in lineage rows.** In the IR,
  every apply would get a distinct identity, but the IR hash would no longer
  depend only on schema meaning, and cluster drift checks and prepared applies
  compare that hash with one computed from source. In lineage rows, it would
  need the buffered-row inheritance rule and a second `__history` format
  change, and nothing reads a historical sequence.
- **Stamp at the first contract write instead of the first publication.**
  Rollback would stay open for operators who never apply while branches are
  live, but the writer would have to write both row layouts depending on a
  branch's stamp. Stamps 15 and 16 set the single-layout precedent.
- **Bind snapshot reads to the writing branch's contract.** It is undefined
  once that branch is deleted, binds a recreated branch's contract by name,
  and does not cover unpublished commits or unattached snapshots. Main's
  current contract is total.
- **Apply to every branch in turn.** Lance has no multi-branch commit, so it
  cannot be one publication. It races each branch's writers and deletes branch
  data on a dropped type.
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
  commits and unattached snapshots. The archived schema object embeds the
  head; `SchemaContractDigest` is `deny_unknown_fields` and persisted in the
  cluster ledger.
- Identity: the contract identity hashes the serialized SchemaIR; renames keep
  ids and the allocator, and rename hints are not stored in the IR.
- Binding: `open_write_txn` and the branch arm of `capture_read_view` resolve
  the branch's own contract; snapshot reads use the handle's bound branch,
  main in production; the compiled-query cache is keyed by catalog object;
  the server re-typechecks each call against the branch.
- Physical isolation: branch writes stage detached on the dataset their
  inherited registration names; cleanup retains every live branch's head and
  creation version; `optimize` runs on main.
- Planner totality: added properties must be nullable, `AddConstraint` is
  limited to `@index`, enums only widen, and removing an `@index` is refused.
  Type renames, enum widening, non-full-text indexes and metadata changes have
  no table effect; a full-text `@index` declares its analyzer on the table.
- Merge: equal-contract capture, one catalog for all images, alias matching,
  a row comparator that matches columns by name, lineage candidates by file
  identity, publication of `Update` only, publisher identity matching with an
  alias check, and a deterministic merge base with a smallest-id tie break.
  `ensure_indices_on` and the full-text rebuild publish new branch pins
  without changing rows.
- Stamps: every manifest scan and every publish refuses an unsupported stamp;
  stamps 15 and 16 were written by first publication.

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

The second and third rows are the by-name comparison across differing
columns that preparation removes. Under this RFC the first three cases merge,
the fourth refuses with `schema_diverged`, and the fifth merges as observed.

A unit test ran the real `merge_base_search` and its bounded variant over three
hand-built histories, labeling each commit with an apply counter. Across all
294 ordered pairs, no base had a larger counter than either side. The labels
were distinct per apply, so the test did not model a reversed change; content
identity fails there, which is why this design stores the sequence. The
prototype is not committed; the tests below replace it.

### Required tests

| Owner | Proof |
|---|---|
| `tests/schema_apply.rs` | Apply with live branches leaves their pins and contracts; an add, rename or full-text `@index` never refuses; a drop refuses only for a branch that changed the table, naming it; an index rebuild or compaction on the branch, before or after the apply, never changes the outcome; preview lists affected branches; budget and walk-limit refusals; incarnation recheck |
| `tests/integration/merge_truth_table.rs` | Each schema change in both directions between main and a branch and between two named branches, with and without either side changing the table; type add, rename and drop; the base prepared to the newer contract; `Person → Human → Person` with a branch caught up to the middle contract; a property dropped and re-added under the same name with an older base reports `DivergentUpdate`; criss-cross bases; the published contract has the larger sequence; the dry run reports the same outcome |
| `tests/merge_fast_forward.rs` | Catch-up fast forward publishes the contract; preparation writes no data file and reads no rows of untouched tables |
| `omnigraph-catalog` tests | The ordering property over every pair of commits, including content-equal contracts with different sequences; sequence 0 for pre-17 rows; the publisher refuses a lower sequence or a contract change without lineage |
| `tests/failpoints.rs`, `tests/detached_commit_matrix.rs`, collector tests | A cross-contract merge interrupted before publication leaves the branch's contract and pins; prepared versions stay unreachable and are reclaimed; after publication both are complete |
| `tests/integration/point_in_time.rs`, `tests/integration/changes.rs` | Snapshot reads of a lagging branch's commits under main's contract, including after the branch is deleted or recreated, and of unpublished commits and unattached snapshots; change-feed baselines of a lagging branch use its contract and end when its incarnation changes |
| `tests/integration/search.rs` | Full-text search on a branch after catch-up uses the declared analyzer before any index rebuild |
| Unpublished and unattached owners | `as commit` catch-up refused before effects; unpublished commits and unattached snapshots rooted at a target expire on catch-up; candidates on unaffected branches are untouched |
| `tests/policy_engine_chassis.rs` | Catch-up authorization, including a tombstone on the target; branch-scoped schema read |
| Server `schema_routes`, `data_routes`, `mcp`; CLI `cli_schema_config` | `GET /schema?branch=` and its authorization, `schema show --branch`, `branch show` fields, the 409 reason, the apply refusal, stored queries on a lagging branch over HTTP and MCP, OpenAPI drift |
| Cluster `tests.rs` | Deployment no longer refuses a graph with branches; the ledger's `schema_contracts` digest is unchanged by a sequence increment |
| `db/upgrade/tests.rs`, CLI `crossversion_upgrade.rs` | Stamp 17 on first publication and by the stamp-only route; a stamp-16 binary refuses a stamp-17 main at open and, while already open, at its next scan and publication |
| `tests/integration/engine_v2_plan_replay.rs` | A plan bound before a catch-up merge refuses replay after it |
| Other merge owners | Legacy pre-v11 table forks, Blob tables, inherited interface properties and renamed edge endpoints under a cross-contract merge |
| DST harness | Schema operations with live branches; the model keeps one contract and sequence per branch |
| Cost instrument | The `blocked_on` gate |

Row-visible cases also belong in GQT, which first needs a schema-apply step.
The one-language draft
([ModernRelay/omnigraph#670](https://github.com/ModernRelay/omnigraph/pull/670))
proposes the statement such a step would use; that format change is separate.

## Rollout

1. Stamp 17 and the schema sequence: sequence 0 for older rows, increments by
   contract writers on main, the publisher's sequence and lineage checks, and
   the upgrade route. Contracts still cannot diverge, so nothing else changes.
2. Bind change-feed baselines, `optimize`, `repair` and write staging to the
   right contract, make snapshot reads bind main explicitly, and key the
   accepted-catalog cache by contract identity.
3. Teach merge preparation, logical change, identity matching and contract
   publication. Production cannot reach it yet; tests create divergent
   contracts through a test-only path.
4. Lift the apply, reconcile, settlement and cluster refusals, and add the
   apply check, the surfaces, the user and developer docs, the operator guides
   and skills that state the main-only rule, the invariants support boundary
   and a changelog note.

`implementation` becomes `in-progress` with step 1 and `complete` after step 4.

## Future work: drops that do not discard changes

A drop refuses whenever the side holding the dropped property changed the
table, even if none of its changes touched that property. A follow-up can
compare only the dropped columns between the base and that side, and refuse
only when a dropped value changed. Type drops stay as specified.

## Unresolved questions

1. Should schema apply offer an override that drops a property or type despite
   branches that changed it, leaving them unable to merge that table? The
   draft refuses with no override.
2. Is `BranchMerge` on the target enough for a catch-up merge that changes the
   target's contract, including a tombstone of a type main dropped? The draft
   requires only `BranchMerge`, since the change was authorized when it was
   applied to main.

## Decision log

- 2026-10-12: Drafted from a code survey of the schema apply, merge, publisher,
  catalog binding and collector paths, a review of the Lance 11.0.0 sources and
  pages named above, and the prototype recorded in Evidence and tests.
- 2026-10-12: Revised after a model review of the first draft, checked against
  the code. A stored schema sequence replaced ordering by equality of content
  identity. A per-table check over the base and both sides replaced the
  endpoint-only check, after the review showed an older criss-cross base
  could hide a write to a re-created property. Full-text `@index`
  declarations became table effects. Storage stamp 17 fenced older binaries,
  replacing the claim that they stay safe. The apply check gained a work
  budget and an incarnation recheck.
- 2026-10-12: Aligned with open proposals. The sequence lives beside the
  contract head so archived schema content stays content-only for the change
  feed contract (#964, which landed in RFC 0030 through #966).
  Historical-property binding (#959) now governs snapshot reads. Contract
  writers proposed in #884 increment the sequence, and its
  validation-tightening steps must become lossy steps or refuse with live
  branches. Access-restricting contract changes from #951 take effect on every
  branch at once. Restore in #677 must not lower a branch's sequence.
- 2026-10-15: Revised after PR review. Registration equality decided
  refusals, so index maintenance could flip outcomes (invariant 7, deny-list);
  "changed" now means changed rows, with file identity as the cheap path.
  Preparing every version to the newer contract replaced whole-version
  selection, so lossless steps merge with ordinary conflicts and only drops
  refuse. Snapshot reads keep main's contract because the writing-branch rule
  was not total. The sequence moved out of lineage rows, and the ledger digest
  stays content-only. The stamp fence holds on every scan and publication, and
  stamping at first contract write is recorded as rejected. `branch list`
  stays cheap.
