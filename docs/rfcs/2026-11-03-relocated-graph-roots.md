---
rfc: "2026-11-03-relocated-graph-roots"
title: "Relocated graph roots"
track: maintainer
status: draft
implementation: not-started
authors:
  - Ragnor Comerford (@ragnorc)
created: 2026-11-03
updated: 2026-11-03
discussion: "https://github.com/ModernRelay/omnigraph/issues/1233"
supersedes: []
superseded_by: []
blocked_on:
  - "RustFS and Azurite relocation owners pass in CI on the implementation pull request"
  - "The genuine predecessor journeys (Storage Upgrade Compatibility, V5 ↔ V10 Format Fence) pass with the fail-closed base rule"
---

# RFC: Relocated graph roots

## Summary

A whole copy of a graph root, opened at a new location on the same kind of
backend, serves every branch, tag, and retained snapshot. It reads only
objects under the root it was opened at. When a dataset of the graph names a
base outside that root, or a copied object is missing, the open refuses. It
never reads the object at the old location.

To do this, Omnigraph stops treating one class of stored Lance base paths as
authoritative. These are the bases Lance's shallow clone records for a
branch of the same dataset. Omnigraph derives each such base from the opened
location, in the handle's memory, after every open and checkout. Persisted
bytes do not change, the storage format stamp does not change, and Lance is
not patched. Every other base is refused.

This RFC partly replaces one sentence of
[RFC 0029](0029-azure-blob-storage.md) and one alternative row of
[RFC 0064](0064-explicit-storage-upgrades.md). Neither RFC owns relocation, so
the rule has one home here, and both point to it on acceptance (see
[Rollout](#rollout)).

## Motivation

Issue [#1233](https://github.com/ModernRelay/omnigraph/issues/1233)
reproduces this on v0.14.0 and on main. A retained branch of a graph copied
to a new path fails with `Not found` for an object under the original path.
The copied root holds that object at the same relative path.

The cause is Lance 13's `Dataset::create_branch`. It is a shallow clone, and
it records `Operation::Clone.ref_path = source_location.uri`, the absolute
URI of the source branch. `Manifest::shallow_clone` stores that URI as a
`BasePath` and sets `base_id` on every inherited data, deletion, and index
file. Every later version copies the map forward. A child branch adds its
parent's `tree/<parent>` URI.

The prototype
(`ragnorc/omnigraph:proto/1233-branch-base-follows-root`) found a worse case
on main. When the original root is still present, a copy that lacks an
inherited object reads it from the original root without any error. A
command addressed to one root returns rows from another. That breaks the
root as the unit of authority (invariants 3 and 12).

Users need this to work. The upgrade guide makes a whole-root backup the
rollback unit, and the maintenance guide makes it the recovery unit after an
external cleanup. A backup that must stand alone, at another path or on
another machine, does not serve a graph's branches today.

Lance's [table layout](https://lance.org/format/table/layout/) page, under
"Dataset Portability", already draws this line:

- A dataset whose files resolve through relative paths works at a new
  location with no manifest update.
- A dataset with several bases, "such as in shallow clones", keeps the
  original base locations when only its root is copied. To port it, copy the
  bases and "update the manifest's base_paths array".

A branch is that shallow clone. Its base is a branch of the same dataset,
inside the copied root. This rule is the port step the page names, done in
the handle's memory rather than written back.

An issue-sized fix is not enough here. The fix decides when Omnigraph may
override a persisted substrate field. That is a compatibility rule every
later writer must keep.

## User and operational behavior

### The relocation contract

A ***whole-root copy*** is every object under a graph root, copied while no
process writes the graph, the same condition as a backup. Paths stay the
same relative to the root, on a backend of the same kind:

- a local path to another local path;
- S3 to S3, including another prefix or another bucket;
- Azure to Azure, including another prefix or another container.

A standalone process that opens a whole-root copy at its new URI gets these
behaviors:

- It serves `main`, every live branch, every tag, every retained snapshot,
  commit history, and the change feed, with the same results as the
  original at the moment of the copy.
- It writes, merges, and creates branches. It runs `optimize` and `cleanup`,
  and their effects stay inside the copy.
- It reads and writes only objects under the opened root, for graph-internal
  bases. The original location is never consulted, whether it is gone,
  intact, or different.
- When an object a graph-internal base names is missing from the copy, it
  fails with the existing `NotFound` storage failure, and the message names
  the opened root.
- When a dataset of the graph carries a base that is not graph-internal (see
  [Design](#design)), it refuses with a `Permanent` storage failure that names
  the dataset and the base id. It refuses before it reads any file through
  that base.

A same-path restore, putting a backup back at the location it came from,
works as it does today.

### Outside the contract

The following stay unsupported or unchanged:

- **Moves between backend kinds** (local to S3, S3 to Azure). RFC 0029's
  export and import workflow is still the route.
- **A cluster-managed graph root at a new location.** That is adoption, and
  adoption stays unsupported. A same-path restore of a cluster root is
  unchanged.
- **External Blob URIs.** They are absolute by contract, so a copy still reads
  the same external source under the external-Blob policy.
- **Copies taken while a writer runs, and partial copies.** A partial copy
  fails on its first missing object. It is not repaired.
- **Older binaries.** They keep resolving the recorded paths, so a relocated
  root fails on them as it does today.

### Documentation

`docs/user/concepts/storage.md` gains the contract. The upgrade and
maintenance backup text says that a backup restores at the same path or a
new one. Troubleshooting names the two refusals.

## Design

### Inherited Lance behavior

This section describes Lance 13.0.0 (`lance`, `lance-table`, `lance-io`):

- `Dataset::create_branch` builds `Operation::Clone { ref_name: source_branch,
  ref_path: source_location.uri }`. `source_location` is
  `branch_location().find_branch(source_branch)`, a branch of the same
  dataset.
- The commit path (`io/commit.rs`) reloads the source manifest from
  `ref_path`. It calls `shallow_clone`, which copies the source's persisted
  `base_paths` and adds `BasePath::new(id, ref_path, ref_name, true)`. The
  name is the source branch (`None` or `main` for main), and
  `is_dataset_root` is true.
- A `base_id` file's path comes from `Dataset::manifest.base_paths`, read
  on every call: `data_file_dir_for_base`, `dataset_dir_for_deletion`, and
  `indice_files_dir`. A local store opens the resulting absolute path
  directly (`LocalObjectReader`), so an object-store wrapper cannot redirect
  the read.
- **The store for a base is not re-read.** `object_store(Some(id))` builds it
  from the base's URI on first use and keeps it in `base_object_stores`, a
  `pub(crate)` cache that clones share. Only the private `set_manifest`
  clears it, and only when `base_paths` change. Replacing the public
  `Dataset::manifest` `Arc` does not clear it. A store opened at the old URI
  therefore survives a later rewrite of the map. A handle would then compute
  each path from the new map and read it through the old store, which on S3
  or Azure is another bucket or container.
- `checkout_by_ref` builds a new handle, with an empty cache, from the
  persisted manifest. The exception is a checkout of the version the handle
  already holds (`already_checked_out`): it returns `self.clone()`, which
  shares the cache. `Dataset::with_object_store` is the public clone that
  starts an empty cache.
- A commit uses the handle's in-memory map only when the handle holds the
  transaction's `read_version` (`commit_transaction`). Otherwise Lance checks
  out `read_version` from disk, and the new version carries the recorded
  bases.
- `BranchLocation::find_main` and `find_branch(name)` derive every branch
  location of a dataset from the opened location.
- No Lance API renames a branch. Bases are added by `Operation::Clone`, by
  `add_bases` (`Operation::UpdateBases`), and by the `WriteParams` fields
  `initial_bases`, `target_bases`, and `target_base_names_or_paths`.

### Base provenance

Every base of every graph dataset comes from one operation: Lance's shallow
clone of a branch of the same dataset, called through
`lance_clone::create_branch`.

- Current binaries clone only `__manifest`, from
  `branch_control::create_branch_recoverably`.
- The storage upgrade does not clone. `publish_conversion` checks out each
  existing branch and overwrites it, and `Manifest::new_from_previous` copies
  the predecessor's `base_paths`. An upgraded graph therefore carries the
  old binary's clone bases. The genuine predecessor journeys in `blocked_on`
  are the evidence that those bases have the shape below.
- Binaries before storage format v11 also created table forks this way.
- The production source has exactly one `.create_branch(` call into Lance.
  `forbidden_apis.rs` already pins that count. No Omnigraph writer uses
  `add_bases`, `initial_bases` (`commit.rs` passes `None`), `target_bases`, or
  `target_base_names_or_paths`. The implementation adds guards that refuse
  each of them.

### Graph-internal branch bases

Let a graph dataset be opened at location `L`, and let `M = L.find_main()`.
A base `b` is a ***graph-internal branch base*** when two things hold:

- `b.is_dataset_root` is true;
- the path part of `b.path` ends with `/<D>` when `b.name` selects main
  (`None` or `main`), or with `/<D>/tree/<b.name>` otherwise. `<D>` is the
  last path segment of `M`: `__manifest`, or a table's directory.

That suffix is exactly the relative location Lance's shallow clone records
for that name. It equals the suffix of `M.find_branch(b.name)` past `M`'s
parent.

After every open and every checkout, Omnigraph does the following for a
graph dataset:

1. It replaces `b.path` with `M.find_branch(b.name).uri` for every
   graph-internal branch base. It writes a clone of the manifest, so the
   session's cached manifest is untouched. It installs the new map on a
   handle with an empty base-store cache (`Dataset::with_object_store` of the
   handle's own primary store), before any base on that handle resolves. No
   other handle shares that cache, so no store opened at a recorded URI can
   serve it. A handle whose bases already name the opened root is returned
   as it is, so applying the rule twice changes nothing. Its cache can hold
   only stores opened at those same URIs, because the door is the only code
   that replaces the map.
2. It refuses the handle when any other base is present (a `Permanent`
   storage failure). This includes a base whose name selects a different
   tree from the one its path records, a base that is not a dataset root,
   and any path outside the shape above.

Before the comparison, a recorded path loses its query string and any
trailing `/`. On Windows, `\` is also read as `/`, because Lance's
`get_root_path` accepts both separators.

One residual is accepted. A base Omnigraph did not write, whose name selects
main and whose path still ends in `/<D>`, is rebound rather than refused.
The writer inventory says no such base occurs, and even then the read stays
under the opened root.

A rebound base points under the opened root, so its files are read through
the opened root's store and binding. A file missing there fails there. The
failure keeps today's class: a `NotFound` storage failure, reported as
`internal` with the `refresh` action. That action is wrong for a missing
copied object, which a retry does not bring back; this RFC does not change
it. The message names the path under the opened root, never the recorded
path.

### One door

The rule lives in one function in `omnigraph-core` (`branch_base`). It runs
in two places:

- the single dataset opener (`instrumentation::open_dataset` and its
  variants);
- one checkout function that replaces bare `Dataset::checkout_branch`,
  `checkout_version`, `checkout_latest`, and `checkout_tag`.

The catalog exposes that checkout as its function for every `__manifest`
checkout. A `forbidden_apis.rs` rule refuses a bare checkout, and every
Lance base-adding API, in engine, core, and catalog production sources.

### Two path rules, one owner each

Omnigraph holds two kinds of absolute location for a graph dataset. Each has
one owner:

- **A table pin's `manifest_path`.** Omnigraph writes it into a catalog row.
  `full_manifest_object_store_path` (`omnigraph-core` `metadata.rs`) rebuilds
  it from the opened root and the table's relative path when it is read.
  This RFC does not change it.
- **A Lance `BasePath`.** Lance writes it into a manifest. `branch_base`
  rebinds it at the open door. `branch_base` is the only code that classifies
  bases. That covers the refusal, the collector's borrowed origins, and the
  relocation bit.

The relocation bit (`manifest_has_external_base_paths`,
`SnapshotDataset::has_external_base_paths`, and the benchmark's
`relocation_self_contained`) is true today whenever `base_paths` is
non-empty, so every branched graph counts as not relocatable. Under this
contract it asks `branch_base` instead: a dataset has an external base only
if one of its bases is not graph-internal. Such a dataset is refused at open,
so an openable graph is self-contained. No third path rule may be added; a
new location fact names one of these two owners.

### Mismatched names fail closed

A base whose name selects one tree while its path records another is
refused. It is neither rebound nor read at its recorded path. The evidence:

- `shallow_clone` writes the name and the path from one `ref_name`, in one
  manifest.
- Lance has no branch rename, and Omnigraph never calls `add_bases`.
- So neither Omnigraph nor Lance writes a mismatched shape. It can come only
  from a foreign writer or from corruption, which invariant 8 requires to
  fail loudly.
- Keeping the recorded path would be the cross-tree read the contract
  removes. Rebinding by name alone would guess which of two disagreeing
  facts is true.

### Pre-v11 table forks: the rule extends to tables and the collector

Forks created before storage format v11 are Lance branches of user tables.
Registrations that name them remain valid ([lance.md](../dev/lance.md),
"Table forks"). Their bases have the same shape, written by the same clone.
The rule therefore applies to every graph dataset, not only `__manifest`.
The alternative, refusing relocation while forks exist, was rejected for
four reasons:

- **Detection costs the same.** Refusal needs the same comparison between the
  recorded and the opened location. Extending only changes the outcome from
  an error to a rebind.
- **Tables already open through one opener.** A fork is opened by its tree
  URI (`open_dataset_entry`, then `instrumentation::open_pinned_dataset`, or
  `table_store`'s `checkout_branch`), so it runs the same door.
- **The collector needs it.** Its `borrowed_origins` resolves each fork
  base's path. Without the rule, a cleanup on a relocated root would protect
  the original location's files. It could then delete the copy's parent-table
  files that a retained fork still borrows. Refusal would avoid that loss
  only by refusing cleanup on every relocated root with a fork.
- **There is a fixture to extend.** The existing legacy-fork fixture
  (`collector.rs`, `cleanup_keeps_legacy_native_origins_and_reclaims_unrelated_payloads`)
  can carry a relocated copy and a cleanup run.

Native-fork reclamation (`collect_native_forks`) follows native refs and
their parent chain, not bases, so it needs no change.

### What new manifests persist

Nothing new is persisted. What a new version records depends on how it is
written:

- **A publish whose handle holds the read version.** It records the opened
  root's absolute URI for every base, ancestors included, because
  `Manifest::new_from_previous` copies the handle's in-memory map. That is
  what Lance writes for any clone made at that location.
- **A publish whose handle does not hold the read version.** Lance checks out
  `read_version` from disk, and the new version keeps the recorded bases.
- **A branch created on a relocated root.** `create_branch` always reloads
  the source manifest, so the new branch records the opened URI for its
  direct source and keeps its ancestors' recorded URIs.

The rule covers all three.

Existing manifests and clone transaction files are never rewritten, so local
e-tags and branch identity do not change.

### S3 and Azure

The contract is the same on every supported backend:

- A rebound base lies under the opened root, in its bucket or container, so
  the opened handle's binding (credentials, endpoint, provider options)
  serves it.
- A copy into another bucket or container therefore needs no access to the
  source bucket, and it never uses that access.
- The [Azure preview](0029-azure-blob-storage.md) boundary still applies to
  live Azure writes.

### Lance compatibility fence

The implementation adds this row to the fence table in
[lance.md](../dev/lance.md):

> **Branch bases.** Lance 13 `create_branch` records the source branch's
> absolute URI as a dataset-root `BasePath` named for that branch, and
> `shallow_clone` copies the source's bases forward. A `base_id` file's path
> comes from `Dataset::manifest.base_paths`, read on every call. Its store
> comes from the `base_object_stores` cache, built from the base's URI on
> first use and shared by clones. Replacing the public map does not clear
> it. After each open and checkout, Omnigraph rebinds every base whose
> recorded path ends in the suffix its name selects to `find_branch(name)`
> of the opened location. It does this in memory (`branch_base`), on a
> handle with a fresh cache, before any base resolves, and refuses every
> other base. Drop the in-memory
> rebinding for new bytes at the bump whose clone records a location
> relative to the dataset. Keep it for existing bytes.

A `lance_surface_guards.rs` test pins the stock behavior. Under stock Lance,
a copied dataset with a branch reads its inherited file at the original
path. That test goes red at the bump that changes it.

## Invariants

- **1, respect the substrate.** Lance owns the base mechanism. Its layout
  guide names rewriting `base_paths` as the port step. Omnigraph applies that
  rewrite in memory for the one shape its own writers produce. It adds no
  storage primitive.
- **2 to 5, publication and recovery.** These are unchanged. No write path,
  publication, or recovery protocol changes. The rule rewrites nothing on
  disk.
- **3, one coherent view.** It is strengthened. A read can no longer combine
  objects from two roots.
- **6, identity.** It is unchanged. Identity, ownership, and liveness still
  come from registrations, native refs, and `BranchContents`. A base name
  only selects which location Lance itself derives. It never makes a dataset
  part of the graph. The opened root does.
- **8, integrity failures are loud.** The rule does not reinterpret an
  unknown persisted shape. It applies Lance's documented meaning (the
  location of a source branch) to the shape whose provenance the writer
  inventory proves, and it refuses every other shape.
- **12, one source of truth.** It is strengthened. The stored URI was a
  second, frozen copy of a fact the opened location already holds.
- **lance.md, "Names never establish table ownership."** The rule
  establishes no ownership. The suffix test is a consistency check that
  refuses shapes outside the inventory. Fork liveness and reclamation still
  follow refs and incarnations.

No deny-list item applies.

## Compatibility and reversibility

The change is read-side only:

- **Persisted bytes.** None change. The storage format and graph stamp are
  unchanged.
- **Older binaries.** They read new bytes as before. On a root relocated and
  then written by a new binary:
  - a version from a same-version publish records the new location for every
    base, so an older binary reads it there;
  - a new branch, a publish from a stale handle, and every version not
    rewritten keep recorded ancestor bases, and an older binary still fails
    on them, as it does today.
- **Reverting.** Reverting restores the current behavior for every graph,
  with nothing to migrate.
- **Possible break.** The one possible break is the fail-closed refusal. A
  graph whose bases do not match the shape would stop opening. The writer
  inventory says no such graph exists. The genuine predecessor journeys
  (bytes written by the released v0.10.0 and v0.11.0, the stamp-13 build, and
  the Lance 11 build) are the evidence gate in `blocked_on`.

## Alternatives

| Approach | Disposition |
|---|---|
| Document the path limit only | Leaves the backup hole and the silent cross-tree read. Rejected as the only change. The documentation ships with the rule. |
| Materialize branches by copying inherited files (lance#7515) | Not in Lance 13 to 15. It must run before the move, so it does not repair a copy that has already moved. It duplicates catalog files and adds a maintenance step. Rejected. |
| A `relocate` command that rewrites `base_paths` with new manifest versions | Historical versions keep the old bases, so the reader rule is still needed. It also adds a commit per branch and per fork after every move. Rejected. |
| Persist a root-relative `BasePath.path` | Lance specifies an absolute path, and `uri_to_url` would resolve a relative one against the working directory. That needs a format stamp and a Lance change, and still a reader rule for existing bytes. Rejected. |
| An object-store wrapper or a registry `extract_path` rewrite | Local reads bypass the wrapper. The registry is process-wide, so two copies of one backup open in one process would need opposite rewrites. Rejected. |
| `DatasetBuilder::with_serialized_manifest` | It opens the dataset again, with I/O: `load_by_uri` still calls `resolve_version_location`, which HEADs an attached version's manifest. The in-memory door adds no I/O, and that holds only when it runs before any base read. Rejected. |
| Rebind graph-internal bases but keep the recorded path for any other base | It keeps a cross-tree read for exactly the shapes that cannot be explained. Rejected in favor of refusal. |
| Refuse relocation while pre-v11 forks exist | It costs the same detection and serves less. See [Design](#pre-v11-table-forks-the-rule-extends-to-tables-and-the-collector). |
| Wait for a Lance release | It fixes neither existing bytes nor current graphs. The upstream issue is [lance#10661](https://github.com/lance-format/lance/issues/10661). |

## Evidence and tests

Evidence already gathered by the prototype, on local files:

- **Owner test** in `integration/branching.rs`, covering an unwritten retained
  branch, a child of a written branch, and a branch-only snapshot lookup
  with an unwritten sibling. It also covers a same-path restore, a divergent
  original, a missing original, a write and a branch create on a rebound
  branch, and no fallback. On main, a copy missing one inherited object
  returned 4 rows read from the original path.
- **In-source rule tests** for the fences.
- **Gates.** The GQT corpus, DST (`cost_table.txt` unchanged, because the rule
  does no I/O), the workspace tests, and both Clippy graphs were run.

Required by the implementation, all extending existing owners:

- **Fail-closed rule tests**: a mismatched name, a data-only base, an outside
  path, and a `main`-named main base.
- **A relocated cleanup** in `integration/maintenance.rs` (or `collector.rs`
  when internals are needed), with a retained branch and a tagged snapshot,
  reading every retained snapshot afterwards.
- **A relocated pre-v11 fork** with a cleanup run, extending the legacy-fork
  collector fixture.
- **RustFS**: a copy into another prefix and into another bucket, in the
  `s3_storage` owner. **Azurite**: another prefix and another container, run
  by the Azurite job.
- **Guards**: the `forbidden_apis.rs` guards (one checkout door, one base
  classifier, no other writer of `Dataset::manifest`) and the
  `lance_surface_guards.rs` stock-behavior guard.
- **The cache rule**: a handle that resolved a base store at the recorded
  URI before the rebind does not read through that store afterwards. Across
  buckets on RustFS, that stale store would read the wrong bucket.
- **The relocation bit**: it is reported self-contained for a branched graph
  whose bases are all graph-internal.
- **The genuine predecessor journeys**, unchanged and green.

Upstream surfaces reviewed: Lance 13.0.0 `dataset.rs` (`create_branch`,
`resolve_reference`, `checkout_by_ref`, `get_manifest`, `load_manifest`,
base resolution, `add_bases`), `dataset/branch_location.rs`,
`dataset/builder.rs`, `io/commit.rs`, `lance-table` `format/manifest.rs`
(`BasePath`, `shallow_clone`), and `lance-io` `object_store.rs` (`open`,
`has_direct_local_paths`, `uri_to_url`). Pages read in full:
[table layout](https://lance.org/format/table/layout/),
[branch and tag format](https://lance.org/format/table/branch_tag/),
[tags and branches guide](https://lance.org/guide/tags_and_branches/), and
[Blob v2](https://lance.org/guide/blob/).

## Rollout

1. **Acceptance.** It changes the status to `accepted`. The accepting change
   adds a note under each title below.
   - Under [RFC 0029](0029-azure-blob-storage.md):
     `> **Relocated graph roots disposition:** [Relocated graph roots](2026-11-03-relocated-graph-roots.md) supplies the test this RFC asks for, for a whole-root copy on the same kind of backend: such a copy is supported, and a base outside the root or a missing copied object refuses. Moving a root between backend kinds stays unsupported, and the export and import workflow below stays the route.`
   - Under [RFC 0064](0064-explicit-storage-upgrades.md):
     `> **Relocated graph roots disposition:** an upgrade still assumes no root copy or rename. A whole-root backup may be restored at its own location or at a new one on the same kind of backend, under [Relocated graph roots](2026-11-03-relocated-graph-roots.md).`
2. **The implementation pull request**, draft
   [#1235](https://github.com/ModernRelay/omnigraph/pull/1235), carries the
   rule, the door and its guards, the fork and collector extension, the
   relocation bit, the object-store evidence, the fence row, the user
   documentation, and a changelog note.
   `implementation` moves to `in-progress` when it is opened against an
   accepted RFC, and to `complete` when it merges with its evidence green.
3. **Upstream.** [lance#10661](https://github.com/lance-format/lance/issues/10661)
   links the layout page's portability text to `create_branch`'s absolute
   `ref_path`. When Lance records a relative same-dataset location, the fence
   row's drop condition applies to new bytes only.

## Unresolved questions

None block acceptance. The two evidence gates are in `blocked_on`.

## Decision log

- 2026-11-03: Drafted from issue #1233 and the prototype. Chose one new RFC,
  not amendments to RFC 0029 and RFC 0064, for three reasons:
  - the rule is a compatibility decision that neither RFC owns;
  - amending both would state it twice;
  - the README prescribes a disposition note when an accepted RFC replaces
    part of an earlier one.

  Decided on a fail-closed refusal of every base that is not a
  graph-internal branch base (mismatched names included), on extending the
  rule to pre-v11 table forks and the collector, and on support for copies
  within one backend kind (another prefix, bucket, or container) but not
  across kinds.
- 2026-11-03: Revised after review. It now states:
  - the base-store cache limit, and that the door installs the rebound map
    on a fresh, unshared cache before any base resolves;
  - that the storage upgrade overwrites (`publish_conversion`) rather than
    clones;
  - what a same-version publish, a stale-handle publish, and a new branch
    persist;
  - the layout page's shallow-clone port step;
  - one owner each for pin paths and `BasePath`s, the relocation bit
    included;
  - the builder's I/O as the reason to reject it, the `NotFound` class and
    message path, separator normalization, and the main-name residual.

  Linked the filed upstream issue (lance#10661) and draft #1235.
