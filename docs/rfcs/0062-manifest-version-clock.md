---
rfc: "0062"
title: "Manifest version as the table registration clock"
track: maintainer
status: draft
implementation: in-progress
authors:
  - azimafroozeh
created: 2026-09-07
updated: 2026-09-09
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0062: Manifest version as the table registration clock

> Number provisional: this RFC takes the registry's next available number at
> drafting time; the number is re-checked when the PR opens.

> A term set in ***bold italics*** is being defined at that exact spot.

## Summary

Every `table_version` and `table_tombstone` row in `__manifest` carries the
`__manifest` dataset version that the publish writing the row lands in, as the
trailing segment of its row key `object_id`; no column is added. The
***manifest version*** is the Lance version of the
`__manifest` dataset itself. A branch's ***lineage*** is the rows visible on its
manifest: the parent's rows up to the fork version, then the branch's own
publishes. A ***native ref*** is the Lance branch a table's data lives on
(`native_dataset_branch` in `DatasetEntry`; `None` is the root lineage). A
***registration*** is a `table_version` row: it says that on this branch the
table with `stable_table_id` / `table_incarnation_id` is published at Lance
version `table_version` of the native ref `table_branch`.

The snapshot projection, the fold that picks a table's current registration,
picks it by the greatest `manifest_version` visible on the branch, not by the
greatest `table_version` (the table's own Lance version, a per-native-ref
counter). Registration rows key on `(identity, manifest_version)`, so a publish
never overwrites an earlier registration.

What does not change: the physical pointer `(table_version, table_branch)` a
registration resolves to, the Lance data tables and their native refs, the
single `__manifest` publish CAS, and the recovery protocol. The writer's
expected-version pin (the `expected_table_versions` map a publish is checked
against) stays a Lance data version and gains the native ref it was read on.
The collision guard (the publisher
check that rejects a different row at an occupied `(identity, number)`, "table
version N already exists ... with different state") and the merge route
predicate `adopt_requires_target_lineage` of
[PR #630](https://github.com/ModernRelay/omnigraph/pull/630) become
unnecessary and are removed.

## Motivation

Lance numbers a branch's versions continuing from its fork point, so `main` and
a branch forked at v2 both mint v3, v4, v5 on separate histories.
`table_version` therefore orders registrations only inside one native ref.
`__manifest` today has no column that orders registrations across native refs,
and three code paths in the projection and the row key compare or key on the
number alone:

| Path | What it does with the number | Defect |
|---|---|---|
| `fold_parts` (`db/manifest/state.rs:228`) | keeps the registration with the greatest `table_version` per identity | an adopted registration with a lower number is invisible: [PR #630](https://github.com/ModernRelay/omnigraph/pull/630)'s round trip, the GQ logic case `merge_adopt_lower_source_version_keeps_edges.gqt`, silent row loss after a merge that reports fast-forward |
| `version_object_id(identity, table_version)` (`db/manifest/layout.rs:180`) plus the publish merge-insert on `object_id` with `when_matched(UpdateAll)` (`publisher.rs:930`) | a registration at an occupied `(identity, number)` replaces the stored row | the owner-branch handoff arm exists only to make that replacement safe, and the guard next to it rejects every other equal-number registration |
| the collision guard (`publisher.rs:455`) | rejects a different row at an occupied `(identity, number)` | rejected a legal same-branch re-registration ([#473](https://github.com/ModernRelay/omnigraph/issues/473), since fixed by the `reregisters_stored_row` arm, `publisher.rs:463`); rejects the equal-number cross-ref adopt loudly, the GQ logic case `merge_adopt_equal_source_version_collides.gqt` |

The same number comparison also sits in the publish precondition:
`check_expected_table_versions` (`publisher.rs:826`) compares the writer's
pinned `table_version` with the greatest registered number. Two registrations
with equal numbers on different native refs satisfy the same pin. This RFC
closes that gap: the pin carries the native ref (Design).

[PR #630](https://github.com/ModernRelay/omnigraph/pull/630) restores per-branch monotonicity by discipline: one predicate in the
merge code routes an adopt whose source number is at or below the target's
through the target-lineage delta writer, so the registered number always
grows. Every future path that registers a table must honour the same rule, and
the equal-number case pays a data write for what is logically a pointer move.
This RFC gets the order by construction: the value that already orders every
publish on a branch is written on the row.

## User and operational behavior

- GQ, the CLI, and the HTTP API do not change shape. `native_dataset_branch`
  and `published_dataset_version` in snapshot responses keep their meaning.
  `manifest_version` stays internal; `graph_manifest_version` on the snapshot
  already orders snapshots, and no consumer (CLI printer, change feed, cluster)
  reads a per-table manifest version. A per-table response field is deferred
  until a reader needs to order two registrations of one table.
- A branch merge that adopts a source registration whose `table_version` is at
  or below the target's registers a pointer switch and the target reads the
  adopted state. Today it either drops rows silently (lower) or fails with the
  collision message (equal). After `branch merge S into T` reports
  `fast_forward`, every row T reads for a table is the row S read for it at
  merge time, whatever the two tables' Lance version numbers.
- The error text `table version {n} already exists for identity {id} ({key})
  with different state` no longer exists. A manifest holding two rows at one
  identity and manifest version, or a row stamped above the scanned version,
  is refused on read as `manifest_internal` (Design). No legal publish is
  refused for its version number: a merge that adopts a registration at an
  occupied number succeeds, and a merge whose adopt re-registers the stored row
  publishes nothing (`AdoptPublish::Nothing`). A write whose pinned registration
  now wins on another native ref is refused as `ReadSetChanged`
  (`native_ref:<table>`).
- The registration-clock design introduced graph storage schema v7 after v6.
  The combined implementation uses v8 for RFC 0042's retirement metadata while
  retaining v7's row-key meaning. The storage policy requires strict v8 normal
  open, with explicit registered conversion or export/rebuild for older inputs.
  [RFC 0064](0064-explicit-storage-upgrades.md) composes qualified standalone
  v6-to-v7 registration conversion with metadata-only v7-to-v8 conversion.
  Normal open refuses v6/v7 before table decoding; older binaries refuse the
  new stamp. Neither refusal changes the graph.
- The export/rebuild fallback is per branch: `omnigraph export --branch <b>`
  once per live branch, each stream loaded into its own graph with `init` and `load`. Branch
  topology, shared ancestry, commit history and historical snapshots are not
  carried over (`docs/user/operations/upgrade.md`). The cutover quiesces
  writers, finishes recovery on the old binary before the export, builds the new
  graph at a parallel root, and never runs a mixed fleet against either root. A
  qualified deployment can instead use RFC 0064's offline converter to preserve
  branch topology and retained history.

## Design

### One value already computed, carried in the row key

The `__manifest` Arrow schema (`state.rs:128`) is unchanged. The manifest
version of a registration or tombstone row is the trailing 20-digit segment of
its `object_id` (`table_version:{stable:016x}:{inc:016x}:{manifest_version:020}`,
`layout.rs`), decoded on scan by `manifest_version_from_object_id`. `object_id`
is projected on every read path already, so the scan reads nothing new; a
column would add one read per manifest fragment on every page and the
change-feed cost gate (`tests/changes_cost.rs`) caps manifest reads per page
independent of history.

The value is `new_manifest_version`, computed as `dataset.version().version + 1`
on the opened manifest and already written on `graph_commit` rows
(`state.rs:974`, in the overloaded `table_version` column). It moves above
`build_pending_rows`, which today runs before the computation site at
`publisher.rs:1120` inside `match lineage { Some(..) }`. Lance commits at the
reloaded `manifest.version + 1` and may rebase over a concurrent merge-insert
whose key bloom filter does not intersect. The `graph_head:<branch>` row every
lineage publish writes forces key intersection, so a concurrent publish always
surfaces as `RowLevelCasContention` and the retry rebuilds the rows from a
reloaded dataset; a publish that carries no lineage writes no such row and is
not covered. The publisher asserts
`new_dataset.version().version == new_manifest_version` after `merge_rows` and
fails `manifest_internal` otherwise. The error names the landed version and says
the commit is durable. The mis-stamped rows stay readable and below every later
manifest version, so the fold order is unaffected; the path is reachable only by a publish
without lineage, which exists behind `#[cfg(test)]` today. No allocation, no second counter, no new
authority: the manifest version is the Lance version of the `__manifest`
dataset, and the row key records which one wrote the row. A registration's
***clock*** is its manifest version read as an order.

`DatasetEntry` gains `manifest_version: u64`, decoded from the row key at
`state.rs:753`.

### The manifest version is a total order on a branch

Each graph branch's `__manifest` is a Lance branch of the graph's `__manifest`,
forked at the parent's current version (`db/manifest.rs:1664`
`create_branch_recoverably(&mut ds, &native, self.version())`), and the
publisher opens the manifest on the active branch (`publisher.rs:235`). A
branch's lineage holds the parent's rows up to the fork version, inherited as
the branch's base fragments, then the branch's own publishes. Inherited rows
carry manifest versions at or below the fork version; own rows carry strictly
greater ones, one per publish. So within
any branch's view the manifest version is unique per publish and strictly
increasing in publish order. It is not unique across branches (`main` and
`feature` both continue from the fork number), and it does not need to be: the
projection folds one branch's rows at a time, and a merge writes its
registration on the target's manifest at the target's next version.

The order holds because no path copies a `table_version` or `table_tombstone`
row from one manifest into another; a merge writes a fresh registration at the
target's clock. The draft RFC 0058's ancestry import copies `graph_commit` rows
only and keeps this.

### The fold keeps the greatest clock

`fold_parts` keeps, per identity, the registration with the greatest
`manifest_version`. The tombstone filter (`state.rs:444`) compares
`manifest_version` too: a registration is live when its manifest version is
greater than the identity's greatest tombstone manifest version. The stable
pre-sort at `state.rs:815` orders by `(identity, manifest_version)`. Ties are
impossible within a lineage because one publish writes at most one registration
and one tombstone per identity, and this RFC adds the check that makes that
true: `claimed_identities` in `build_pending_rows` is a set of `TableIdentity`,
so a second `Update` or a second `Tombstone` for one identity in one batch is
refused before the merge-insert. Today's "claimed twice" check
(`publisher.rs:442`) keys on `(identity, table_version)`, so two `Update`s for
one identity at different numbers pass it, and under the new key both rows would
carry one `object_id` and the merge-insert would abort on the duplicate.

The pointer a winning registration resolves to is unchanged:
`(published_dataset_version, native_dataset_branch)` opens the Lance dataset,
exactly as today. The number keeps its physical meaning and loses its ordering
duty.

The other sites that pick a registration by the number move with the fold:
`latest_visible_per_identity` (`publisher.rs:625`) and `is_live_identity`
(`:675`) select by `manifest_version`, and `fold_inputs`'s `version_map` and the
`existing_versions` lookup re-key on `(identity, manifest_version)` together
with its remaining consumers. The numbers those sites report to the
publish precondition stay Lance data versions; the ref the pin gains is checked
beside them. The
version-GC floor at `optimize.rs:1326` takes a `min` of
`published_dataset_version` per dataset path over registrations it first
filters to the root lineage (`native_dataset_branch.is_none()`,
`optimize.rs:1299`), so it already compares inside one native ref and stays;
`optimize.rs:893` and the chain-completeness scan at `recovery.rs:8954` stay as
they are, both in-lineage.

### The pin names the registration's native ref

`TableVersionExpectation` gains `native_ref`:
`NativeRefPin { Unchecked, Exact(Option<String>) }`, `Exact(None)` being the
root lineage. `Exact(ref)` is what a producer that read a registration pins,
`Unchecked` what one that pinned a bare data version pins.
`check_expected_table_versions` keeps its data-version comparison and then
refuses a winning registration on another native ref as `ReadSetChanged`
(`native_ref:<table>`), so two registrations with equal Lance numbers on
different refs no longer satisfy one pin. Tombstone winners and `Unchecked`
pins compare the version only. The recovery sidecar and the 409 payload are
unchanged: the ref comes from the registration or sidecar slot the producer
already holds.

The producers that pin `Unchecked` are the recovery slot replays (the sidecar
records the ref an attempt publishes on, not the ref its pin was read on) and
every registration pin at version 0 (nothing was read). Recovery replays under
recovery ownership before any writer or merge runs on the graph
(`heal_pending_recovery_sidecars_for_write`), so a same-number registration on
another ref cannot land between a sidecar's arm and its replay; recording the
read ref in the sidecar is a follow-up that needs sidecar schema v10.

### A registration never overwrites a row

`version_object_id` and `tombstone_object_id` key on
`(identity, manifest_version)`. Because the manifest version of a publish is
new by construction, a registration never matches an existing row: the
merge-insert's `when_matched(UpdateAll)` arm never fires for these row kinds.
The owner-branch handoff (an in-place replacement of the row at the same
number with a new `table_branch`, `publisher.rs:457`) becomes an ordinary new
registration with a greater clock. The collision guard, the
`is_owner_branch_handoff` and `reregisters_stored_row` predicates, and the
`existing_versions` lookup keyed by number are removed. A duplicate
`(identity, manifest version)` cannot be written (a publish's manifest version
is new, and one publish carries one row per identity); a manifest that holds two
rows at one identity and clock is refused as `manifest_internal` by the
whole-catalog scan and by the delta fold alike, and a row whose clock is above
the scanned version is refused by `require_clock_at_or_below` on both paths.

This amends [RFC 0028](0028-stable-schema-identity.md) §4.5: the trailing
`{version:020}` of `table_version:` and `table_tombstone:` object ids becomes
the manifest version that wrote the row, so the spelling reads
`table_version:{stable:016x}:{inc:016x}:{manifest_version:020}`
(`layout.rs:180`). The table's Lance version stays in the row's `table_version`
column. The amendment is recorded in RFC 0028's decision log in the same PR.

A tombstone row keeps `table_version` as the sealed data version;
`latest_visible_per_identity` reports it as `actual` when a tombstone is the
identity's newest row. The `existing_tombstones` guard (`publisher.rs:510`) goes with the
collision guard, and recovery's redundant-tombstone filter (`recovery.rs:7744`)
stays and now also supplies the tombstone pin's native ref. A registration and a
tombstone of one identity in the same publish would share a clock;
`build_pending_rows` refuses the pair as `OmniError::manifest`, so the fold's tie
rule (tombstone wins) is never exercised on a legal manifest. A registration of a
tombstoned identity is refused before the merge-insert as `OmniError::manifest`:
a dropped table is never re-registered, since re-adding a type mints a new
incarnation. A tombstone for an identity that is already sealed is refused the
same way.

`graph_commit` rows keep the manifest version in the overloaded `table_version`
column at this stamp; retiring that overload belongs to the draft RFC 0058. Init
rows are not written by a publish and carry `GENESIS_MANIFEST_VERSION`.
`build_pending_rows` spells the manifest version into each row's `object_id`;
`entries_to_batch` spells it from `DatasetEntry::manifest_version`, and
`manifest_rows_batch` verifies every key with `manifest_version_from_object_id`;
the scan projection list is unchanged.

### Merge publishes exact source endpoints

When a table's target equals the merge base, a named target adopts the source's
exact `native_dataset_branch`, published table version, and version metadata.
This includes a target that currently owns a different fork. The source table
version may be below, equal to, or above the previous target table version;
the new registration's manifest version makes it authoritative.

Main retains its target-lineage delta route for a source on a named ref.
`reregisters_current_entry` still compares the complete physical registration,
so an unchanged exact endpoint can be omitted. Pointer candidates still carry
their validation delta through the shared constraint evaluator; avoiding table
writes does not imply avoiding scans or validation.

A replaced target fork can remain needed by another graph branch or by Lance
ancestry. RFC 0042's unique table-fork names let a later first-touch write create
a fresh ref from the adopted exact version without inspecting or reclaiming
the old ref. Explicit cleanup reclaims unused forks after its full protection
proof. The base manifest version in a fork name describes preparation and does
not replace this RFC's publication order.

### Worked example (the lower-number case, table `Knows`)

`M` is `main`'s manifest version when `feature` is forked.

| Step | Registration written `(table_version, table_branch)` | Manifest lineage and version | Today's winner on the reading branch | Winner under this RFC |
|---|---|---|---|---|
| seed on `main` | `(2, None)` | `main` at `M` | | |
| fork `feature` | none (inherits) | `feature` manifest forked at `M` | | |
| three edge writes on `feature` | `(3, f)`, `(4, f)`, `(5, f)` | `feature` `M+1`, `M+2`, `M+3` | `(5, f)` | `(5, f)` |
| merge `feature` into `main` (delta onto `main`'s lineage) | `(3, None)` | `main` `M+1` | `(3, None)` | `(3, None)` |
| insert on `main` | `(4, None)` | `main` `M+2` | `(4, None)` | `(4, None)` |
| merge `main` into `feature` (pointer switch) | `(4, None)` | `feature` `M+4` | `(5, f)`: the adopted row loses on number, and its `object_id` collides with `(4, f)` (equal row counts, so the handoff arm replaces that row in place) | `(4, None)`: greatest clock |

Under the RFC every row on `feature`'s manifest has a distinct clock, greater
always means later, and `main`'s `M+1`, `M+2` rows are never in `feature`'s
fold.

### Inherited from Lance versus added here

Inherited: per-branch version numbering that continues from the fork point,
branch inheritance of the parent's fragments, the dataset version as the CAS
target. Lance assigns the number itself: `commit_transaction` sets the target
version to the current manifest version plus one on every attempt
(`rust/lance/src/io/commit.rs:1350`), and no commit API takes a
caller-chosen version. What a caller can attach to a commit is
`transaction_properties` (a string map persisted on the transaction), a tag,
or a detached commit outside the mainline history (`CommitBuilder::with_detached`).
None of these orders rows, so the clock is read from the version Lance
assigns, never chosen. Added: the projection rule and the row key.

## Invariants

- **2, one graph-content publication door.** Unchanged: the clock is spelled
  into the row keys of the same merge-insert as every other row of the publish.
  No second write, no second version source.
- **3, every operation uses one coherent accepted view.** Strengthened: a pin
  that names its ref is refused when the winner at the same Lance number sits on
  another native ref.
- **6, stable identity survives renames, not lifetimes.** Unchanged: the clock
  orders registrations of one identity; it never identifies a table. The
  identity columns stay the key of the fold.
- **8, integrity failures are loud.** The collision guard is removed because
  the condition it guarded cannot be written; the read-side refusals in Design
  ("A registration never overwrites a row") keep the failure loud.
- **12, one source of truth, cheaply derived.** The clock is the `__manifest`
  dataset's own version, an authoritative value already durable in every
  publish. Nothing cached becomes authoritative.

Deny-list: none touched. Support boundary: unchanged.

## Compatibility and reversibility

- **Storage.** New row keys whose trailing segment changes meaning, from the
  Lance data version to the manifest version: an incompatible manifest shape,
  since a v6 row key decoded by v7 would read a data version as a clock.
  `INTERNAL_MANIFEST_SCHEMA_VERSION` and `MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION`
  move together; v6/v7 graphs are refused by normal open. This clock was
  introduced at v7; the current combined implementation uses v8 because RFC
  0042's native-ref retirement also changes storage interpretation. RFC 0064
  qualifies explicit offline v6 → v7 → v8 and v7 → v8 routes without widening
  serving admission. The rejected MemWAL experiment's v7 to v19 stamps never
  shipped and remain unsupported inputs (`docs/dev/versioning.md`).
- **Converter.** `__manifest` is created with `enable_stable_row_ids: true`
  (`db/manifest/graph.rs:138`), so Lance's `_row_last_updated_at_version`
  system column holds, for every existing row, the manifest version that last
  wrote it. RFC 0064's v6-to-v7 handler derives registration clocks from pinned
  source provenance, retaining old snapshots with explicit v6 decoding. Its
  owned pending intent, receipts and main-last activation preserve retry
  ownership across the composed v8 route.
- **Wire.** Unchanged: no response field is added or re-typed, and the 409
  payload's `expected_published_dataset_version` and
  `actual_published_dataset_version` keep their meaning.
- **Recovery sidecar.** Schema v9 stays. The per-table baseline keeps deriving
  its pins from Lance data versions, and replay compares them as it does today.
- **RFC 0024 (draft).** Durable table heads compares the pin against a mutable
  head (`0024:279`, `:303`). If that RFC is resumed, the head carries the
  manifest version and its owner-branch handoff transition disappears, because
  an adopt at an equal number is then an ordinary new registration.
- **Reverting.** Reverting the code before any graph is written at the new
  stamp costs nothing. After that, reverting means another rebuild across a
  stamp, the same cost as adopting.
- **Pin.** Shape change only (`native_ref` on `TableVersionExpectation`); the
  sidecar and the 409 payload are unchanged (Design).

## Alternatives

| Alternative | What it is | Why not |
|---|---|---|
| Do nothing | keep number ordering | the three defects in Motivation, one of them silent row loss |
| [PR #630](https://github.com/ModernRelay/omnigraph/pull/630)'s routing predicate | `adopt_requires_target_lineage` sends any adopt with `source ≤ target` number through the target-lineage delta writer, so the registered number always grows on the target | correct today, but the order is a rule every registering path must keep, not a property of the row; the equal-number case pays a data write for a pointer move; the equal-number pin gap in the precondition stays. the pointer route can leave an old fork for explicit cleanup, while RFC 0042's unique names keep reclamation off the next write |
| Order by Lance's `_row_last_updated_at_version` on `__manifest`, keep the current key | stable row ids are on, so the value exists for every row | the row key still collides on equal numbers, so the handoff replacement stays. That replacement does order correctly: with the guard removed, `when_matched(UpdateAll)` stamps the replaced row with the publishing version and this order wins both merge cases (`lance_version_columns.rs:212`). It is rejected for its consequences instead: a replaced row disappears from the delta fold and from the row-immutability argument of Invariant 8, and every fold must project a Lance system column |
| Re-key only: `object_id` on `(identity, table_version, table_branch)`, keep number ordering | removes the collision and the handoff arm | the lower case still loses: `(4, None)` at a lower number than `(5, f)` stays invisible; and the equal-number tie stays, since the fold's `>=` keeps whichever row the scan yields first |
| Re-key on `(identity, table_version, table_branch)` and order the fold by `_row_last_updated_at_version` | the two rows above combined, each dismissed by the defect the other fixes | it fixes all three defects with no new column and no stamp, so it wins on compatibility and loses on authority: the logical fold would take its order from a Lance physical system column (the invariants' governing principle, nearest deny-list item "a logical precondition based on ... staged layout"), the column's value under `__manifest` in-place rewrite (`manifest.rs:1316`) is unverified, and inherited fragments carry Lance-owned stamps |
| [PR #630](https://github.com/ModernRelay/omnigraph/pull/630) generalised | every adopt, not only one at or below the target's number, writes a delta on the target's lineage | the order is still a rule every registering path must keep rather than a property of the row, and every pointer move pays a data write, now including the fast-forward that copies nothing |
| Order the fold by Lance's `_rowid` | stable row ids are on, so every row has one | a merge-insert update keeps the replaced row's id, so the order fails wherever the key still collides; it needs the re-key first, and with the re-key the manifest version is already on the row |
| Counter per table identity | greatest clock in the snapshot plus one, allocated inside the publish | a second sequence to maintain and backfill, ordering the same publishes the manifest version already orders |
| Graph-global clock | one counter across all branches | needs a cross-branch CAS; manifests are per lineage and the projection only ever compares rows inside one lineage, so a global order is unneeded |
| Vector clocks | per-native-ref counters carried as a vector | solves concurrent writers without a central order; each branch already has one, its manifest CAS |

Precedent audit: `graph_commit` rows already carry the manifest version that
wrote them (`state.rs:974`) and the lineage head is chosen by the greatest
manifest version first, then `created_at`, then commit id (`head_lineage_row`,
`state.rs:926`). This RFC applies the same rule to table registrations. The
draft RFC 0058 replaces greatest-version head selection for lineage;
registrations differ from commit rows in that no path imports them from another
manifest. `table_incarnation_id` is the precedent for "a value in the row, no
side record".

## Evidence and tests

- Existing owners to extend: `crates/omnigraph/src/db/manifest/tests.rs` (fold,
  tombstone, precondition, and the guard's tests, which turn into
  key-distinctness tests; the stamp tests at `tests.rs:2091`);
  `crates/omnigraph/tests/lifecycle.rs` for the stamp on open;
  `docs/dev/merge.md` cost tests, which cap a fast-forward's manifest opens and
  scans at three and must stay at three (no scan is added).
- Owners this RFC rewrites: the `branching.rs` merge tests and the
  `merge_fast_forward.rs` arm arriving with
  [PR #630](https://github.com/ModernRelay/omnigraph/pull/630) assert the delta
  route, that the merged entry's `published_dataset_version` is above the
  target's and that an empty adoption does not advance the physical target HEAD.
  Under the clock those merges are pointer switches and the first two invert;
  the HEAD assertion holds (a pointer switch writes no data).
- Cross-version, the storage axis this stamp bump owes:
  `crates/omnigraph-cli/tests/crossversion_upgrade.rs`
  `current_v9_refuses_and_rebuilds_genuine_v6_and_v6_refuses_v9` (skips unless
  `OMNIGRAPH_V6_BIN` names a released 0.10.x binary): the new binary refuses a
  v6 graph naming the export binary, the old binary refuses the new stamp, and
  an export rebuilds with row, vector and blob fidelity;
  `migrations.rs` checks the current v8 stamp, including refusal of v7.
- GQ logic tests (`.gqt`, `crates/omnigraph-gqt/cases/`), all carried by this
  PR: `merge_adopt_lower_source_version_keeps_edges.gqt` and
  `merge_adopt_equal_source_version_collides.gqt` (red on `main` before this
  change, the two shapes in Motivation),
  `merge_adopt_lazy_target_below_inherited_version.gqt`,
  `merge_adopt_equal_version_return_trip.gqt` (the target merges back and
  keeps the rows it adopted), and the guard case
  `issue_473_merge_net_zero_branch.gqt` for a same-branch re-registration
  ([#473](https://github.com/ModernRelay/omnigraph/issues/473)), green before
  and after.
- Manifest unit tests (`db/manifest/tests.rs`): a later registration at the
  same Lance version wins by clock and the earlier row survives beside it; a
  later registration with a lower Lance version on another native ref wins the
  fold; a pin naming one native ref is refused when the winner at the same
  number sits on another ref; a tombstone for a sealed identity is refused (the
  equal-clock refusals on the scan and the delta fold are detector-only, no
  test feeds a duplicate); two registrations of one
  identity in one batch are refused and the
  batch lands nothing; the warm post-publish fold and a fresh reopen agree field
  for field after a lower-Lance-version registration; a registration of a
  tombstoned identity is refused; every registration's `manifest_version` equals
  the manifest version after its publish; the owner-branch handoff tests keep
  their assertions unchanged.
- Rust, for what the format cannot express: every registration's
  `manifest_version` equals the manifest version after its publish (the unit
  tests above); the publisher's refusal of a commit that landed at a version
  other than the stamped one is untested (reachable only by a lineage-less
  publish) and, with a failpoint-driven CAS-retry test, is a follow-up.
- DST: the merge operations in `crates/omnigraph-dst` already drive every adopt
  route; `Oracle::MergePrediction` and the whole-world per-branch detector
  (`detectors.rs:124`) are the acceptance checks; a detector asserting that a
  branch's fold never contains another branch's post-fork registration is a
  follow-up.
- Upstream surfaces surveyed: Lance branch version numbering and fork
  inheritance (`shallow_clone` keeps the parent counter); `_row_created_at_version`
  and `_row_last_updated_at_version` semantics under merge-insert
  (`crates/omnigraph/tests/lance_version_columns.rs`).

## Rollout

1. This RFC and its implementation ship in one PR, based on `main` with
   [PR #630](https://github.com/ModernRelay/omnigraph/pull/630) merged: the row key,
   the `DatasetEntry` field, projection, v7 row-key semantics under the current
   v8 stamp required by RFC 0042's native retirement, the
   batch-duplicate refusal, the collision guard and handoff arm removed, the
   pin's native ref, the read-side `manifest_internal` refusals, the RFC 0028
   amendment, the `docs/dev/versioning.md` and
   `docs/user/operations/upgrade.md` rows, the release note, and the tests
   above. It also renames the local `manifest_version` in `repair.rs:198`,
   which holds a Lance data version and collides with the clock's name. A stamp
   bump is a cutover, so the pieces cannot ship separately. The release names
   both this registration-clock change and RFC 0042's retirement metadata as
   storage changes requiring explicit conversion or the export/rebuild fallback.
2. Ordering against [PR #630](https://github.com/ModernRelay/omnigraph/pull/630):
   that PR landed first (2026-09-08), so this PR removes
   `adopt_requires_target_lineage`, the routing it selects, its
   `docs/dev/merge.md` paragraphs and release-note entry, and re-asserts its
   `branching.rs`, `merge_fast_forward.rs` and `failpoints.rs` tests under the
   clock: an adopt at or below the target's number registers the source's
   exact endpoint as a pointer switch on named targets and stages no rows.
   A later first-touch write creates a unique fork under RFC 0042; the
   historical fork/index gap remains recorded below and requires regression
   evidence for the new route.
3. Optional: the one-shot converter, if a graph that cannot be rebuilt by
   export and load appears.

On maintainer approval `status` moves to `accepted` in this PR;
`implementation` moves to `complete` when it merges.

## Unresolved questions

1. The original rebuild-only scope is amended by RFC 0064's qualified explicit
   routes. Its remaining backend, cluster and acceptance gates govern further
   expansion; export/rebuild remains the fallback for unsupported inputs.

## Decision log

- 2026-09-07: drafted after
  [PR #630](https://github.com/ModernRelay/omnigraph/pull/630) showed a
  lower-numbered adopt is invisible to the fold.
- 2026-09-08: rebased onto
  [PR #630](https://github.com/ModernRelay/omnigraph/pull/630). Its lazy-target
  test writes on the target after the merge; under this RFC that adopt is the
  `AdoptPublish::Fork` arm, and the write fails with `NotFound` on the parent
  branch's `_indices/` (Lance
  [#7840](https://github.com/lance-format/lance/issues/7840)). The same
  failure exists on `main` before this RFC for a newer source on that arm
  (probe on the PR #630 tree), and routing the lazy target through the delta
  writer instead fails the same way when the target's inherited registration
  is itself on a branch ref, so the arm stays and the lazy-target test stops
  after its reads. Follow-up: the merge's index handling after a fork from a
  branch ref.
- 2026-09-09: RFC 0042 now separates table-fork names from graph-branch native
  refs and records ownership in `TableVersionMetadata.table_fork_owner`.
  Named targets adopt source pointers even when they own a fork, and later
  writes allocate fresh names. Cleanup handles unused forks. Publication order
  remains the graph branch's manifest version.
