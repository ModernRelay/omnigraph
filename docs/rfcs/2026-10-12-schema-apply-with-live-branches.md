---
rfc: "2026-10-12-schema-apply-with-live-branches"
title: "Schema apply with live branches"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-12
updated: 2026-10-20
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Measure the apply-time branch check (requests, bytes, time) at 1, 16 and 256 live branches and across growing released history, cold and warm, with a checked-in cost instrument, and fix the check's work budget from that measurement."
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

This RFC takes the narrowest rule that never combines data written under two
schemas. A merge between branches with different contracts takes the newer
side's version of every table whose stored columns or full-text declarations
differ, and refuses when the older side changed such a table since the merge
base. Schema apply refuses when a live branch changed a table the apply would
change. "Changed" means changed rows: index maintenance and compaction do not
count. No merge or check rewrites, converts or prepares a version, so reads,
dry runs and previews stay free of effects and the merge keeps its fast paths.

Each later relaxation in Future work only turns refusals into merges and needs
no format change. The counter needs storage stamp 17, written by each branch's
first publication. An older binary fails at its next read or write of main
once main carries it; a branch still at stamp 16 stays usable under its own
contract. The design does not add schema apply on named branches or a way to
apply one change to every branch.

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

Under this RFC a migration no longer waits for branches that did not change
the tables it touches, and those branches merge with main afterwards in
either direction. A migration still waits for a branch that changed one of
those tables. A branch that changes such a table after the migration cannot
merge that table until it is redone on a branch created from main; merging
main into lagging branches promptly after a migration avoids this.

## User and operational behavior

### Applying while branches are live

`omnigraph schema apply` on main succeeds while other branches exist unless,
for some live branch and some table the apply changes, merging main into that
branch right after the apply would be refused. That is the case when the
branch changed the table since its merge base with main, or when that base
holds the table in another shape (a criss-cross history). It includes a table
the branch could already not merge because of an earlier apply: such a table
refuses every later apply that changes it, until the branch is deleted or its
work is redone on a branch created from main. The refusal names each branch
and table and gives the remedies: merge the branch into main first where that
merge is still allowed, or delete the branch.

A third result is `cannot prove`: the check ran out of its work budget for a
branch before reaching an answer. It names the branch and refuses the apply.

Schema preview and plan report the same branches and tables without refusing.
A preview's list is advisory, including a stored deployment preview; the check
at apply is authoritative.

An apply ***changes a table*** when it adds, renames or drops one of the
type's properties, declares a full-text `@index` on it, or drops the type.
Renaming a type, extending an enum, adding a non-full-text `@index`, and
editing descriptions or instructions change no table; those applies never
refuse because of a branch.

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
sequence, whether it is behind main's, and the tables it can no longer merge;
`branch list` does not, so listing stays one ref read per branch.

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
- takes, for each table whose stored columns or full-text declarations differ
  among the merge base, source and target contracts, the newer side's version
  whole;
- is refused with HTTP 409 and reason `schema_diverged` when the older side
  changed such a table since the merge base. The error names each table and
  side;
- merges every other table as today.

In the common cases this means:

- **Main into a branch behind it** brings main's contract and changes, unless
  the branch changed a table that main's newer contract changed.
- **A branch behind main into main** is allowed under the same condition;
  main's contract does not change.
- A merge whose base predates the older side's contract, which a criss-cross
  history can produce, is refused for every table whose columns differ
  between that base and the older side. Merging main into each side first
  resolves it.

`branch merge --dry-run` reports the same outcome, including `schema_diverged`
and the contract the merge would publish. It stays a read: the rule needs no
write.

A merge requested `as commit` is refused before any effect when it would
change the target's contract; unpublished commits carry no schema change. A
catch-up merge advances the target's head, so unpublished commits and
unattached snapshots rooted at the earlier head expire as they do after any
other publication. A catch-up merge can tombstone a type on its target when
the newer contract dropped it.

A merge publishes once, so a `schema_diverged` refusal refuses the whole
merge, not only the named table: none of the branch's commits reach the target.
A branch that changed a table that a newer contract changed therefore cannot
merge with that side in either direction. Its rows stay readable and
exportable on the branch, and all of its work, the other tables included, is
redone on a branch created from main.

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
the merge orders the two sides by their live contract rows and reads the base
by content, so no historical sequence is read.

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
Branch creation at a commit ([ModernRelay/omnigraph#1016](https://github.com/ModernRelay/omnigraph/pull/1016))
keeps this true. It compares the commit's contract with the contract at the
captured head of the branch that wrote the commit and refuses a difference as
`schema_changed`, and it writes no contract row: the new branch's first
`__manifest` version is a clone of the version that published the commit, so
it carries that commit's row, with its sequence once stamp 17 writes one.

Under this RFC that refusal can become narrower: refuse only a differing
contract that has no sequence. A differing contract that carries a sequence is
an older contract, and the started branch is then a lagging branch that has
changed nothing since its merge base, the commit itself. Merging it back, and
the apply check against it, follow the rules below unchanged. Because the
sequence is part of the compared identity, a commit from before a rename and
back compares as different, which the narrower refusal allows. A merge base may hold a sequence-0
contract with different content, for example from main's history before an
earlier apply. That is expected: the base is read by content, never by
sequence.

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
marker).

A table is ***uniform*** in a merge when the source and target contracts
either both register it or both do not, and every one of the three contracts
that registers it gives it the same signature. A table the base lacks but both
sides register with one signature is therefore uniform: its base image is
empty, and today's merge treats its rows as inserts. Otherwise the table is
***migrated*** in that merge.

`plan_schema_migration` is not used to compute this. Two accepted IRs that
differ only by a property added and later dropped produce an
unclassified-change step there, while the signature comparison correctly
reports no difference. The same comparison of main's current and desired
contracts gives the tables an apply changes.

### Merge rule

The merged contract is the side's contract with the larger sequence; call the
other side the ***older side***. Route selection is today's: for each table,
merge compares the registrations of the base, source and target versions and
chooses to skip it, adopt one side's version, or walk its rows. A uniform table
merges by that route unchanged; all three versions have the same stored
columns, so the by-name row comparison stays sound.

A migrated table merges only if:

1. the source and target registrations are equal, in which case the shared
   version stays; or
2. the base and the older side give the table the same signature, and the
   older side did not change the table since the base (Logical change). The
   result is the newer side's version, whole: kept when the newer side is the
   target, adopted when it is the source, or tombstoned when the merged
   contract drops the type.

Otherwise the merge is refused with `schema_diverged` before any effect.

Condition 1 is safe because every contract write that changes a table's
signature re-pins that table in the same publication, so a version shared by
both sides already has the merged contract's signature. The merge checks this
and fails closed: a shared version whose signature differs from the merged
contract's is an integrity error, not a merge. The
newer side's version already carries the merged contract's signature,
including its full-text declarations and index metadata, so no version is
evolved, prepared or converted. When the older side's registration equals the
base's, condition 2 is today's adopt or skip route, and the catch-up of an
untouched table costs a pointer change.

Adopting the newer side's version of a migrated table still needs a
validation delta for cross-table checks. Today the adopt route builds it by
comparing the base's and the source's rows by column name, which is the
cross-shape comparison the prototype's add and rename cases failed on. For a
migrated table the delta is computed on keys only: inserted and deleted `__id`
values, and `__src` and `__dst` for edges, which every shape shares because
schema apply never changes system columns and the system-column upgrade
refuses while other branches are live. The delta feeds the referential and
cardinality checks against the merged state, including old and new endpoints
of edges whose endpoint types were renamed. Value constraints are not
re-checked: the adopted version is whole and was valid under the newer
contract. Every supported migration keeps existing rows valid: added
properties must be nullable, only `@index` can be added as a constraint,
enums only widen, and type, nullability, key and other constraint changes are
refused by the planner. A constraint is removed only together with the
dropped property it names
([ModernRelay/omnigraph#1062](https://github.com/ModernRelay/omnigraph/pull/1062)),
which loosens validation. A future migration that tightens validation without
changing stored columns, such as adding a required or unique property or the
narrowing steps proposed in
[ModernRelay/omnigraph#884](https://github.com/ModernRelay/omnigraph/pull/884),
must enter the stored signature or refuse while a live branch has changed an
affected table.

Condition 2's signature test is what keeps every comparison within one shape.
In the normal case the merge base descends from the main commit that wrote the
older side's contract. The base and the older side then share a contract,
because sequences never decrease along ancestry, and the test holds for every
table. Only a criss-cross base can predate the older side's contract. For
example, a property dropped and re-added under the same name gets a new
property id; a base from before the drop fails the test for that table, and
the merge refuses instead of comparing old and new values by name.

Equal sequences with a migrated table occur only with such a base, since the
two sides then share a contract. Condition 2 then fails for both sides, so the
table merges only when its registrations are equal.

### Logical change

Whether the older side changed a table since the base depends only on rows,
and condition 2 guarantees the two versions have the same shape. The existing
cheap paths answer first:

1. Equal registrations: unchanged.
2. The merge's lineage candidate discovery compares the two versions' data,
   overlay and deletion files by identity. A discovery that completes with no
   candidates means unchanged. Index-only commits keep those files, so an
   index rebuild costs nothing here.
3. Otherwise the merge compares the candidate rows. A discovery that does not
   complete is not an empty result: when its candidate budget trips, or it
   does not apply (Blob tables, differing table paths such as pre-v11 forks,
   a failed version pin, pins without stable row ids or the exact-id key), the
   merge compares the two versions' rows in order. Compaction therefore
   changes cost, not the answer. Because condition 2 has already proved both
   versions have one shape, a user column missing from either side during this
   comparison is an integrity error, as the comparator already reports it
   (`schema-gated change row is missing column`), never "unchanged".

Only the older side is checked: the newer side's version is taken whole
whether or not it changed. Every step is a read of committed versions under
the merge's existing memory and spill bounds.

### Merge publication

When the merged contract differs from the target's, the merge's one manifest
commit contains:

- the merged contract row, copied byte for byte from the source, with its
  sequence;
- `RenameTable` for every type the merged contract renames;
- `RegisterTable` and the source's pin for every type only the source
  registers;
- `Tombstone` for every type the merged contract drops;
- the merge's ordinary table updates.

The publisher already applies these changes in one batch, registrations first.
Today merge emits only table updates, and the publisher carries the target's
contract row forward. The fast-forward test compares contracts, sequence
included, so a catch-up fast forward publishes the contract too.

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
`commit_changes` move to the new rule. A future batching publisher, such as
group commit ([ModernRelay/omnigraph#785](https://github.com/ModernRelay/omnigraph/pull/785)),
composes entries onto a later pin and loses the head precondition per entry.
It must refuse an entry captured under another contract identity, sequence
included.

### Apply-time check

The check is the merge rule evaluated, for each live branch, against main's
planned contract, as if main were merged into that branch right after the
apply. It shares the merge's implementation, so an apply refusal and a later
merge refusal cannot disagree. Like the merge rule, it only reads committed
versions. After planning, under the exclusive schema permit and before any
table effect:

1. Compute the tables the apply changes. Only these can refuse.
2. List live branches once. For each branch that registers such a table, open
   its `__manifest` and scan its hot rows once, build its commit graph over the
   handle's shared history cache, and compute its merge base with main.
3. Read the base's table rows from the captured buffers when the base is
   buffered, else from one history record read. The merge capture path and
   `pinned_graph_commit` are not reused: the first replaces a one-entry
   authority cache per branch and builds a full write capture, and the second
   opens a full snapshot. A base written by a since-deleted branch is accepted
   here.
4. Apply condition 2 to each such table, and refuse exactly when it fails:
   the branch changed the table since the base, or the base gives the table
   another signature. There is no other predicate. A table the branch could
   already not merge because of an earlier apply fails condition 2 again and
   refuses this apply too, so an apply refusal and the later merge refusal
   always agree.
5. Re-list the branch inventory and compare each branch's incarnation and head
   with the first listing; a difference repeats the check for that branch.

The merge-base search loads both block registries and builds an index before
its bounded walk (100,000 visits or 32 MiB). Those limits do not cover that
loading. The check therefore runs under its own per-branch budget on registry
bytes, index size, history reads and comparison work, fixed by the cost gate
in `blocked_on`. A branch that exceeds it, or the walk limits, refuses the
apply with `cannot prove`, naming the branch; it is the third result, beside
success and a condition-2 refusal, and the user section lists it. This budget refusal is
the one outcome that can depend on physical state: a table whose rows need an
ordered comparison costs more after an index rebuild or compaction moved its
registration. The answer, whenever it is reached, depends only on rows.

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
- `branch show` gains the contract identity, the schema sequence, a
  behind-main flag and the tables the branch can no longer merge. A detailed
  branch listing, such as the one proposed in
  [ModernRelay/omnigraph#1012](https://github.com/ModernRelay/omnigraph/pull/1012),
  may add the same fields at its own measured cost.
- Merge errors gain `schema_diverged`; apply errors gain
  `branch_changed_table` and, for the budget result, `branch_check_unproven`.
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
transaction, branch and tag, versioning and data evolution pages, and
rechecked against Lance 13.0.0, the version main pins since
[ModernRelay/omnigraph#1072](https://github.com/ModernRelay/omnigraph/pull/1072):
`Manifest::max_field_id` still derives from the schema and the referenced
data files, `fix_schema` and the default-tokenizer fallback are unchanged, and
detached commits still take random versions.

- A detached commit writes a new, randomly numbered manifest and never changes
  an existing version, so main's table effects cannot alter a branch's pin.
- `Project` (rename, drop) and `Merge` (nullable add over unchanged fragments)
  write no data file, so schema apply's own evolution keeps the base's data
  files, which lineage discovery compares by identity.
- Field ids are unique only within one line of versions. Two detached commits
  from the same version allocate the same next id, and Lance's schema page says
  a future branch merge "must not … match the fields by name." OmniGraph never
  combines data files from two lines of versions: a merge restages rows
  through keyed writes or adopts one whole version, and this RFC compares rows
  only between versions of one shape. Reads bind columns by field id and
  ignore type, and `fix_schema` silently renumbers duplicate ids, so fragment
  grafting such as [RFC 0001](0001-fragment-adopt-branch-merge.md) would need a
  shared id allocator first.
- When a table version has no full-text segment, Lance searches it with its
  default simple tokenizer. OmniGraph's analyzer declaration, an untrained
  segment written by schema apply, carries the configured analyzer, as the
  guard `fts_untrained_segment_applies_its_analyzer_to_rows_it_does_not_cover`
  pins. This is why a full-text `@index` enters the stored signature, and why
  the merge takes the newer side's version with its declaration whole. Since
  [ModernRelay/omnigraph#1066](https://github.com/ModernRelay/omnigraph/pull/1066)
  a full-text call on a property with no declared segment is refused with
  `full_text_index_required` instead of answering with that tokenizer, so a
  version published without its declaration would refuse searches rather
  than mis-rank them; the signature rule keeps such a version from being
  published.
- Lance 13 has no public surface that reads a version through an uncommitted
  schema change. This RFC needs none, because it never compares versions of
  different shapes.

## Invariants

- **2, one publication door:** schema apply still publishes once on main. A
  cross-contract merge publishes the contract, table lifecycle changes and
  pins in its one manifest commit.
- **3, coherent view:** the merge captures each side's contract with its
  snapshot and the base's with the base, and revalidates both sides' contract
  identities, sequence included, before effects.
- **4, publishes once:** unchanged; the catch-up adds manifest changes, not a
  publication.
- **5, crash convergence:** a migrated table stages nothing; the newer side's
  version is kept or adopted. Other tables keep today's detached staging,
  which a failed merge leaves unreachable. Checks, dry runs and previews write
  nothing.
- **6, stable identity:** tables and columns are matched by stable identity,
  aliases are only names, snapshot reads never infer identity from a branch
  name, and no branch mints an identity.
- **7, derived state:** whether a merge or apply refuses depends only on rows.
  Index maintenance, compaction and fragment layout change cost; the apply
  check's budget refusal is the one outcome they can reach, and it is a
  bounded-work outcome under invariant 11, not a logical answer. Full-text
  declarations travel with the version that carries them.
- **8, loud integrity failures:** every cross-contract refusal is typed, and
  the prototype's silent outcomes become ordinary merges or refusals.
- **10, trust:** the branch-scoped schema read is authorized per branch;
  access-restricting contract changes reach every branch at once.
- **11, bounded work:** the apply check runs under an explicit per-branch
  budget covering registry loading, index construction, history reads and
  comparison work.
- **12, one source of truth:** the contract row stays the authority. The
  schema sequence is part of that row, written only by the operations that
  write the row, not a parallel record.

The support boundary in [invariants](../dev/invariants.md) changes from "Schema
apply retains its main-only, single-live-branch restriction" to "Schema apply
is main-only; other branches keep their contract until a merge brings a newer
one into them". The apply check is a process-local precondition, not
distributed fencing; the merge rule is the authority. No deny-list shape is
added: a refusal never rests on index coverage, fragment layout or staged
state, and the budget refusal is a stated resource bound.

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
nothing of the sequence. Any publication on main stamps it 17, so a graph
cannot hold a divergent contract while an older binary can still read or
write main.

The wire changes are a new 409 reason, a new apply refusal, an optional branch
parameter and new fields on `branch show`.

There is no in-place downgrade. Rolling back means restoring a backup taken
before the first stamp-17 publication; writes after it are absent from that
backup. Reverting only the code that allows divergence, while keeping stamp
17, reinstates both refusals. Existing lagging branches then stay readable and
writable but cannot merge across contracts; an operator merges what still can
merge, then deletes or exports the rest.

The relaxations in Future work need no format change: each uses the sequence
and signatures this RFC stores, and each only turns a refusal into a merge.

## Alternatives

- **Keep the main-only rule.** No liability, but branches keep blocking every
  schema change.
- **Bring every version to the newer contract before merging.** The 10-15
  revision of this RFC committed a metadata-only evolution of the base and
  both sides, then ran today's merge, so only drops refused. Those commits are
  writes on shared datasets, which turned dry runs, previews and the apply
  check into writes. Preparing before route selection also made every
  prepared table miss the skip and adopt routes, so a catch-up restaged main's
  delta instead of adopting main's version.
- **Map rows to the newer shape at read time.** Renaming columns by stable
  property id and filling added ones with null lets both sides change a table
  whose columns differ, without writes. It adds a mapping that every
  cross-shape read in merge must use. It is deferred to Future work rather
  than rejected.
- **Decide "changed" by registration or data-file identity alone.**
  Registration equality lets an index rebuild flip an outcome, and file
  identity lets compaction flip it. Both stay as cheap paths in front of a
  row comparison.
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
quoted in Motivation and Design were rechecked at `f53bf4fb` and
`fe69c039`, the stamp, the refusals, merge capture and route selection, the
publisher and the dry run at `9575b044`, and all of these again at `7fd9cc90`,
after the Lance 13.0.0 upgrade:

- Contract rows: only schema apply and the system-column upgrade produce
  `ManifestChange::SchemaContract` outside tests; the publisher carries the
  branch's row forward otherwise, applies registrations, renames, tombstones
  and updates in one batch, and refuses contract changes in unpublished
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
  limited to `@index`, enums only widen, and removing an `@index` from a
  property that stays is refused; a drop removes only the constraints that
  name nothing but the dropped property (#1062).
  Type renames, enum widening, non-full-text indexes and metadata changes have
  no table effect; a full-text `@index` declares its analyzer on the table.
- Merge: equal-contract capture, one catalog for all images, alias matching,
  route selection by registration equality, a row comparator that matches
  columns by name, lineage candidates by file identity that require one Lance
  schema across the pins and exclude Blob tables, differing paths and pins
  without stable row ids, publication of `Update` only, publisher identity
  matching with an alias check, and a deterministic merge base with a
  smallest-id tie break. The dry run takes no schema permit and commits
  nothing. `ensure_indices_on` and the full-text rebuild publish new branch
  pins without changing rows.
- Stamps: every manifest scan and every publish refuses an unsupported stamp;
  stamps 15 and 16 were written by first publication; main is at 16.
- Branch creation at a commit (#1016, merged) compares the commit's contract
  with the contract at the captured head of the branch that wrote it, refuses
  a difference as `schema_changed`, and writes no contract row.
- Full-text calls on a property without a declared segment refuse with
  `full_text_index_required` (#1066).

### Prototype (2026-10-12)

At `a3af0c85`, a local patch lifted schema apply's only-main refusal and the
merge's equal-contract refusal and gave each merge side its own contract
identity. The standard test graph applied a migration on main while `feature`
existed, then merged:

| Case | Observed | Under this RFC |
|---|---|---|
| Drop `Person.age`, merge main into untouched `feature` | `FastForward`; `feature` pins main's Person version, keeps `age` in its contract, opens cleanly, and fails `stored node:Person has no declared field 'age'` on query | Adopts main's Person version and publishes main's contract |
| Add `nickname`, same merge | Error before effects: missing column `nickname` | Same adoption |
| Rename `age` to `years`, same merge | Error before effects: missing column `years` | Same adoption |
| `feature` edits Alice, main drops `age`, merge main into `feature` | `Merged`; migration not applied; commit carries the older contract | Refused with `schema_diverged` |
| `feature` adds a company, main drops `age`, merge `feature` into main | `Merged`; main keeps its contract and Person version; three companies | Merges as observed |

A unit test ran the real `merge_base_search` and its bounded variant over three
hand-built histories, labeling each commit with an apply counter. Across all
294 ordered pairs, no base had a larger counter than either side. The labels
were distinct per apply, so the test did not model a reversed change; content
identity fails there, which is why this design stores the sequence. The
prototype is not committed; the tests below replace it.

### Required tests

| Owner | Proof |
|---|---|
| `tests/schema_apply.rs` | Apply with live untouched branches leaves their pins and contracts; a change to a table a branch changed refuses, naming branch and table; non-table changes never refuse; an index rebuild or compaction on the branch, before or after the apply, never changes the outcome; a Blob table with an index build on the branch, then a drop on main; preview lists affected branches and writes nothing; a second apply on a table the branch can already not merge refuses with the same branch and table; `cannot prove` on budget and walk limits; incarnation recheck |
| `tests/integration/merge_truth_table.rs` | Each schema change in both directions between main and a branch and between two named branches, with and without the older side changing the table; type add, rename and drop; `Person → Human → Person` with a branch caught up to the middle contract; a property dropped and re-added under the same name with a criss-cross base refuses; the published contract has the larger sequence; a refusal publishes none of the branch's tables; an adopted migrated table's validation delta reads only system columns and still catches an orphaned edge; a shared version whose signature differs from the merged contract is an integrity error; a lineage discovery that does not complete falls through to the ordered comparison; the dry run reports the same outcome and writes nothing |
| `tests/integration/merge_fast_forward.rs` | A catch-up adopts main's version of each migrated table and skips untouched ones: route assertions and `ordered_cursor_scan_calls`; a fast forward publishes the contract |
| `omnigraph-catalog` tests | The ordering property over every pair of commits, including content-equal contracts with different sequences; sequence 0 for pre-17 rows; the publisher refuses a lower sequence or a contract change without lineage |
| `tests/failpoints.rs`, `tests/detached_commit_matrix.rs` | A cross-contract merge interrupted before publication leaves the branch's contract and pins; after publication both are complete |
| `tests/integration/point_in_time.rs`, `tests/integration/changes.rs` | Snapshot reads of a lagging branch's commits under main's contract, including after the branch is deleted or recreated, and of unpublished commits and unattached snapshots; change-feed baselines of a lagging branch use its contract and end when its incarnation changes |
| `tests/integration/search.rs` | Full-text search on a branch after catch-up uses the declared analyzer before any index rebuild |
| Unpublished and unattached owners | `as commit` catch-up refused before effects; unpublished commits and unattached snapshots rooted at a target expire on catch-up; candidates on unaffected branches are untouched |
| `tests/policy_engine_chassis.rs` | Catch-up authorization, including a tombstone on the target; branch-scoped schema read |
| Server `schema_routes`, `data_routes`, `mcp`; CLI `cli_schema_config` | `GET /schema?branch=` and its authorization, `schema show --branch`, `branch show` fields, the 409 reason, the apply refusal, stored queries on a lagging branch over HTTP and MCP, OpenAPI drift |
| Cluster `tests.rs` | Deployment no longer refuses a graph with branches; the ledger's `schema_contracts` digest is unchanged by a sequence increment |
| `db/upgrade/tests.rs`, CLI `crossversion_upgrade.rs` | Stamp 17 on first publication and by the stamp-only route; a stamp-16 binary refuses a stamp-17 main at open and, while already open, at its next scan and publication |
| `tests/integration/engine_v2_plan_replay.rs` | A plan bound before a catch-up merge refuses replay after it |
| `tests/integration/branching.rs` | A start at a commit whose contract differs from the writing branch's head refuses with `schema_changed`; once relaxed, a differing contract with a sequence starts a lagging branch and one without a sequence still refuses |
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
3. Teach merge the signature classification, the logical-change check,
   identity matching and contract publication. Production cannot reach it
   yet; tests create divergent contracts through a test-only path.
4. Lift the apply, reconcile, settlement and cluster refusals, and add the
   apply check, the surfaces, the user and developer docs, the operator guides
   and skills that state the main-only rule, the invariants support boundary
   and a changelog note.

`implementation` becomes `in-progress` with step 1 and `complete` after step 4.

## Future work: relaxations

Each relaxation only turns a `schema_diverged` refusal into a merge, keeps
every outcome that succeeds today, and needs no format change. They are listed
in order of increasing machinery.

1. **Upgrade the older side's version when the newer side's change was schema
   only.** When the older side changed a migrated table but the newer side's
   version has the base's data files, the result is the older side's rows in
   the newer shape. The merge can stage the same metadata-only evolution
   schema apply uses on the older side's version, inside its own effects
   after every refusal, and publish it. Drops stay refused, since they would
   discard the older side's values. This unblocks a branch that wrote a table
   after main changed only its schema.
2. **Map rows to the newer shape at read time.** When both sides changed a
   migrated table, read the base and the older side through a mapping by
   stable property id that renames columns, fills added ones with null and
   omits dropped ones, then run today's three-way comparison and conflict
   kinds. Physical evolution stays for the version a merge publishes, and a
   guard proves the mapping reads what the committed evolution reads. Lineage
   discovery then compares stored signatures, not Lance field ids. This
   unblocks tables both sides write after a migration.
3. **Drops that do not discard changes.** Compare only the dropped columns
   between the base and the side that still holds them, and refuse only when
   a dropped value changed.

## Unresolved questions

1. Should schema apply offer an override that applies despite branches that
   changed a table it changes, leaving them unable to merge that table? The
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
  validation-tightening steps must enter the signature or refuse with live
  branches. Access-restricting contract changes from #951 take effect on every
  branch at once. Restore in #677 must not lower a branch's sequence.
- 2026-10-15: Revised after PR review. Registration equality had decided
  refusals, so index maintenance could flip outcomes; "changed" became changed
  rows, with file identity as the cheap path. Every merge version was brought
  to the newer contract before merging, so only drops refused. Snapshot reads
  kept main's contract because a writing-branch rule was not total. The
  sequence moved out of lineage rows, the ledger digest stayed content-only,
  the stamp fence was corrected and `branch list` stayed cheap.
- 2026-10-17: Narrowed after a second PR review and a maintainer decision to
  take the lower-liability rule first. Preparing versions wrote to shared
  datasets inside reads and defeated the skip and adopt routes. The merge now
  takes the newer side's version of each migrated table whole, refuses when
  the older side changed it, and compares rows only between versions of one
  shape, so checks, dry runs and previews stay reads and catch-up keeps its
  pointer adoption. Upgrading the older side, read-time mapping and narrower
  drops moved to Future work as relaxations. The budget refusal is stated as
  the one physical dependence. Group commit (#785), branch creation at a
  commit (#1016) and branch details (#1012) were aligned, and a test owner
  path was corrected.
- 2026-10-20: Revised after a maintainer review of `4fb15738` and a check of
  main through `7fd9cc90`. The apply check has one success predicate, the
  merge rule's condition 2, so a table a branch can already not merge refuses
  every later apply that changes it; `cannot prove` is named as the third
  result. The adopted table's validation delta is keys-only again, since
  today's adopt route compares rows by column name. A refusal is stated to
  refuse the whole merge, an unfinished lineage discovery falls through to the
  ordered comparison, a shared version is checked against the merged
  signature, and the summary's older-binary fence matches the user section.
  Branch creation at a commit (#1016, merged) is described as it landed, with
  the narrower refusal this RFC permits. Lance facts were rechecked on 13.0.0
  (#1072), and the full-text refusal (#1066) and constraint removal with a
  dropped property (#1062) were recorded.
