---
rfc: "0042"
title: "Incarnation-suffixed native branch refs"
track: maintainer
status: accepted
implementation: complete
authors:
  - OmniGraph maintainers
created: 2026-08-30
updated: 2026-09-09
discussion: https://github.com/ModernRelay/omnigraph/issues/562
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0042: Incarnation-suffixed native branch refs

## Summary

Every life of a graph branch owns its own native Lance ref, named
`{logical}.{ULID}`. The user-facing branch name stays the only public
identity; the suffix is minted at `branch_create` and never shown, accepted, or
addressed through a public entry point. Because a recreated branch lives at a
new native ref, its owned `tree/` paths and session-cache keys cannot alias
those of its dead predecessor.

Each first-touch table fork has a separate name:
`fork.{owner incarnation ULID}.m{base manifest version}.{graph commit ULID}`.
Legacy owners without an incarnation use `legacy` in that position. The name
has at most 80 ASCII bytes, including a 20-digit manifest version. Logical
names and their URL encoding cannot lengthen table refs or exhaust the local
filesystem's temporary-filename limit. The base version and commit ID come
from the existing write attempt. Building the name
adds no storage request, and the existing recovery sidecar persists it before
fork creation. The name distinguishes attempts; the published manifest version
orders registrations within each graph branch (RFC 0062).

Native ref metadata in the `__manifest` dataset is the authority for branch
existence. Deletion marks the exact native ref retired through stock Lance's
public metadata API. That ref preserves physical parent history for surviving
branches. Resolving a logical name validates the retirement metadata and takes
the single unretired incarnation; it does not use a second registry.

## Motivation

Issue #562: deleting a branch and recreating it under the same name reused the
same storage paths. Manifest versions restart, so `tree/b0/_versions/1.manifest`
named different bytes across lives, and a warm handle's per-URI file-metadata
cache served the dead life's entries to the new one — failing with
`all columns in a record batch must have the same length` or silently returning
stale rows. Late-settling deletes of the old fork could also land on the new
life's bytes.

Clearing caches on lifecycle events cannot close this: a third in-process
handle, a long-lived handle in another process, or an in-flight cache loader
inserting after the clear are all outside what a clear can reach. Putting the
life's identity in the path makes the staleness unrepresentable instead.

## User and operational behavior

- `branch create`, `branch delete`, `branch list`, reads, writes, merges,
  policy scopes, recovery sidecars, and `graph_head:<branch>` rows all use the
  logical name. Nothing user-visible changes for ordinary names.
- One new name rule: a branch name may not contain a path segment ending in
  `.` followed by 26 Crockford-base32 characters (an incarnation-shaped
  suffix). Such a name is refused on every entry point, reads included, so an
  internal native ref is never addressable through the API or visible to
  Cedar. Ordinary dotted names such as `release.1.2` stay legal.
- The existing ancestor/descendant rule on logical names (`review` and
  `review/alice` cannot coexist) is unchanged.
- `branch delete` writes retirement metadata on the exact native manifest
  ref, removes its logical authority, and leaves native storage for explicit
  `cleanup`. Deletion works when descendants still depend on that manifest's
  history. No background reclaim task is started.
- `cleanup` proves which exact table refs remain needed by live snapshots,
  recovery, tags, and Lance ancestry before reclaiming unused forks. Absence
  of the original logical owner is not sufficient deletion authority.
- A merge can switch a named target to the source's exact table ref and
  version, including when the target already owns a fork. Its next write can
  create a new unique fork while older borrowers keep their pinned rows.
- `native_dataset_branch` in dataset entries (HTTP snapshot responses) now
  carries the suffixed native name. It was already a physical detail field.
- The name resolver recognizes legacy bare refs, whose native name equals the
  logical name. Name interpretation needs no separate stamp; native retirement
  requires internal manifest schema v8. Qualified v6/v7 graphs use the explicit
  offline routes in [RFC 0064](0064-explicit-storage-upgrades.md); other formats
  retain export/rebuild guidance.
  Deleting and recreating a branch always mints a fresh suffix.

## Design

- `branch_names` owns the naming contract: `mint_incarnation`,
  `native_branch_name`, `split_native_branch_name` (a suffix is recognized only
  as the final segment's `.` plus exactly 26 Crockford characters; anything
  else is a legacy bare name), `logical_branch_name`,
  `ensure_logical_branch_name`, and `resolve_native_branch`.
- Resolution happens once per manifest-branch open in the layout module and is
  carried on the `ManifestCoordinator`, `GraphCoordinator`, and `Snapshot` as
  `native_branch`. Zero live incarnations is `BranchNotFound`; two or more is a
  typed conflict that names both and asks for `cleanup`, never a guess.
- `table_fork_name` uses only the captured owner's incarnation suffix,
  base graph-manifest version, and pre-minted graph commit ID. A legacy owner
  uses the fixed `legacy` marker; the unique graph commit ID separates attempts
  across legacy owners, and table datasets provide separate ref namespaces.
  No logical name or path component is copied into the table ref. The existing
  sidecar pins that exact name before any native fork effect. Resuming the
  same intent reuses its saved name, including older spellings; a new attempt
  receives a new commit ID.
- `TableVersionMetadata.table_fork_owner` records the captured native owner
  in the existing registration metadata. Physical sidecar pins retain the
  intended owner, and confirmed metadata and rollback publications preserve
  it. Pointer adoption copies the source metadata without changing its owner.
  Namespace metadata roundtrips the field as `omnigraph.table_fork_owner`.
- Ownership is read from that metadata, never inferred from the fork's
  spelling. With absent metadata, only exact native-ref equality preserves
  legacy ownership. An older legal name resembling the new format remains a
  borrowed ref. Names do not establish stable table identity.
- Existing owned writes keep the accepted physical fork ref. First-touch
  Mutation, Load, EnsureIndices, and row-writing merges create their prepared
  unique refs from exact inherited versions. They perform no orphan scan or
  reclamation to allocate a name. Sidecar `branch`, write-queue keys, gates,
  lineage intents, and policy scopes stay logical.
- Native retirement uses public `Branches::replace_metadata` to set
  `omnigraph.retired_manifest_branch` on the existing `_refs/branches/` entry.
  The version-1 JSON marker binds `native_branch` and the full `identifier`.
  Unknown fields, unsupported versions, or identity mismatches fail closed.
  This single metadata update publishes deletion and preserves unrelated
  metadata. A lost acknowledgement is resolved by exact marker readback;
  missing physical authority does not prove completed retirement.
- Stock Lance `get` and `list` still expose every physical ref for ancestry
  and maintenance. Engine logical helpers validate retirement metadata and
  select unretired entries. Cached write admission checks the same identifier
  lookup's metadata at the existing request count. Cold logical enumeration
  includes retained refs. Writes do not reclaim storage.
- Explicit cleanup derives exact live table references and closes over native
  ancestry before deleting a fork. Retired manifest refs are reclaimed only as
  unused leaves after tag, lineage, and physical-path dependencies are checked.
  Required parents survive even after their original graph branch is deleted.
  Unpublished private forks and unused former forks remain until maintenance.
  A count-only policy prunes retained dataset versions and permits collection
  of unused forks; it does not count graph commits. Explicit `older_than` also
  retains a fork if any tree or native ref object is recent, before
  closing over dependencies. A retirement metadata update extends this grace
  period for an old tree. These ref metadata listings occur only in age-based cleanup.
- A warm handle whose native ref is retired or gone re-resolves the logical name through
  the ref list: a recreated branch yields the replacement's identity (a
  guaranteed mismatch), a deleted one yields `BranchNotFound`. Change-feed and
  Blob live-branch reads map a vanished named fork to their existing
  incarnation refusal rather than a retention gap; a fork whose tree survives
  with a reclaimed version remains a gap.
- Lance sees only an opaque ref name; dataset paths, and therefore cache keys,
  derive from it. `lance_surface_guards` pins that a suffixed name is accepted,
  lives at a sibling path of the bare name, and that a recreation under a new
  suffix never collides with a lingering dead tree.

## Invariants

- Invariant 2 (one publication door) is unchanged: branch control still
  publishes through the manifest; no new authority is introduced.
- Invariant 3 (one coherent view) is strengthened: a captured native name can
  only ever mean one life.
- Invariant 5 (recovery): the prepared fork name and owner are durable before
  creation. Effect classification and compensation remain exact; leaving
  unreachable private forks for cleanup does not publish them.
- Invariant 6 (stable identity) is honored: nothing infers identity from the
  logical name; the suffix is the branch-life identity in the path.
- Invariant 11 (bounded hot-path work): cached named-write admission uses the
  existing exact ref read. Cold branch enumeration reads all retained physical
  refs and filters retirement metadata; cleanup reclaims unneeded refs.
  This is an explicit cost limitation of retaining stock Lance refs.
- Invariant 12 (one source of truth): native ref metadata is the logical
  registry. Retired refs preserve exact physical identity and ancestry without
  granting logical liveness or maintaining another branch-status projection.

## Compatibility and reversibility

- Unique table forks add optional ownership metadata to existing registration
  and recovery payloads. Their names introduce no allocation object or graph
  publication. The manifest-version clock remains governed by RFC 0062.
- Native retirement introduces versioned native-ref metadata and requires
  strict internal manifest schema v8. Older binaries must refuse because they
  would expose retained physical refs as live graph branches.
  Qualified v7 graphs use RFC 0064's metadata-only v7-to-v8 handler, preserving
  physical history. Source retirement metadata refuses rather than being
  reinterpreted as valid v8 state.
- Absent owner metadata keeps only the exact legacy ownership rule. A binary
  that does not understand the new ownership contract is not a supported
  writer for graphs using unique table forks.
- Legacy bare and suffixed refs remain valid native names. Graph binaries that
  predate schema v8 are unsupported readers and writers of v8 graphs.
- Reverting across the retirement storage boundary requires export and rebuild;
  merely restoring an older binary would lose the retirement-metadata contract.

## Alternatives

- **A branch registry row in the root `__manifest`** (PR #578): the row is the
  authority, the ref follows it via CAS commits and forward recovery. It
  serializes cross-process racing creates, which are outside the documented
  single-writer-process boundary, at the cost of a second source of truth to
  keep converged, a root-manifest commit per lifecycle operation, a filtered
  scan over all manifest fragments on every branch-bound open, a ref creation
  on the read path, a mixed-version resurrection hazard, and a public `--`
  naming break. Rejected on liability.
- **Clearing session caches on lifecycle events**: cannot reach other handles,
  other processes, or in-flight loaders. Rejected.
- **Logical status rows or a retirement work queue**: rejected. Native ref
  metadata supplies branch authority and preserves the physical lifetime in
  one object. There is no queue whose completion determines liveness. Only
  the native-retirement portion of
  RFC 0058 is implemented here; merged-ancestry retention and historical snapshot
  guarantees remain draft work.

## Evidence and tests

- `branching.rs`: delete/recreate yields distinct native names, the recreated
  fork is named by the new incarnation, listing shows logical names,
  incarnation-shaped names are refused on create and read.
- `maintenance.rs`: cleanup must preserve exact live endpoints and their
  native ancestors while reclaiming unused forks, including after same-name
  recreation and a child's first write.
- `failpoints.rs`: a lost-ack fork delete followed by recreate leaves the dead
  fork as garbage that the write never heals in place or reads; cleanup
  reclaims it. Existing recovery, merge, and change-feed ABA owners were
  converted to address forks by native name.
- `long_branch_names_first_touch.gqt`: long ASCII, hierarchical, and Unicode
  logical names remain writable and preserve main's rows. `branch_names` unit
  tests pin the 80-byte physical-name bound at `u64::MAX`, including legacy owners.
- `lance_surface_guards.rs`: suffixed names are valid and path-disjoint.
- `omnigraph-dst`: the reborn-branch cache-poison repro (seed 9401 standalone
  and the seed 10133 wide face) run as regression pins.
- Upstream surfaces reviewed on Lance 10.0.0: branch/tag format and operational
  guide (name rules, `tree/{name}` layout, ref-absence semantics), table
  versioning, and cleanup.

## Rollout

Single change. Ships with the tests above and the documentation in the
branching user guide, the write and recovery developer guides, the Lance
compatibility fence table, and the release note.

## Unresolved questions

Follow-ups outside this implementation: a name-only live-ref listing to drop
the per-ref GET on branch-bound opens; RFC 0058's merged-ancestry retention and
historical snapshot guarantees. Explicit cleanup owns retired manifest trees.

## Decision log

- 2026-08-30: Accepted by the maintainers as the fix for #562 over the
  registry-row design in PR #578; comparison recorded in the Alternatives
  section.
- 2026-09-09: Implemented unique table-fork names and explicit ownership in
  `TableVersionMetadata.table_fork_owner`. Named-target adoption preserves the
  source owner and switches exact pointers; first-touch writes do not reclaim
  old forks. Explicit cleanup owns unused-fork reclamation.
- 2026-09-09: Native retirement uses identity-bound metadata on stock Lance
  refs; no Lance dependency is vendored or patched. Internal manifest schema
  v8 fences older binaries. Explicit cleanup preserves required parents and
  collects unused retired leaves. Unique table names use bounded incarnation
  and commit components. Cached named-write admission adds no storage request;
  cold branch enumeration includes retained retired refs.
