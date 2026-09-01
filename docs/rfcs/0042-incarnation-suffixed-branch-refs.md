---
rfc: "0042"
title: "Incarnation-suffixed native branch refs"
track: maintainer
status: accepted
implementation: complete
authors:
  - OmniGraph maintainers
created: 2026-08-30
updated: 2026-08-30
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
new native ref, it never shares a `tree/` path, a table fork, or a session
cache entry with its dead predecessor. This is the trick already used for table
incarnations, applied to branches.

The Lance ref list of the `__manifest` dataset remains the only authority for
branch existence. No registry row, transaction, or recovery step is added:
resolving a logical name is listing the refs and taking the single live
incarnation.

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
- `branch delete` acknowledges the manifest publication, which is the real
  commit. Physical reclaim of the old life's forks is eventual: `cleanup`
  reconciles any fork whose native name is not a live incarnation, including a
  fork left by a delete whose cleanup step failed. A late-settling delete can
  only touch the dead life's path, which nothing references.
- `native_dataset_branch` in dataset entries (HTTP snapshot responses) now
  carries the suffixed native name. It was already a physical detail field.
- Existing branches keep their bare ref name; a bare ref is a legacy
  incarnation whose native name equals its logical name. No migration or format
  stamp is needed. The first delete/recreate of such a branch mints a suffix.

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
- Table forks copy the manifest branch's native name. Write paths, first-touch
  forks, EnsureIndices, branch merge target naming, sidecar table pins, and the
  test-only publish seam all address forks by native name. Sidecar `branch`,
  write-queue keys, gates, lineage intents, and policy scopes stay logical;
  sidecar shape validation compares a pin's logical form to the sidecar branch.
- The orphan reconciler and `classify_fork_ref` compare fork names against the
  live *native* set, so a dead incarnation's fork is an orphan even while its
  logical name is live.
- A warm handle whose native ref is gone re-resolves the logical name through
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
- Invariant 5 (recovery) is unchanged: create is one native-ref step; delete
  residue is derived garbage on a dead path.
- Invariant 6 (stable identity) is honored: nothing infers identity from the
  logical name; the suffix is the branch-life identity in the path.
- Invariant 11 (bounded hot-path work): a branch-bound open lists the ref
  registry once — cost scales with the number of live branches, not history.
- Invariant 12 (one source of truth): the ref list stays the registry. The
  deny-list item "maintained parallel truth" is exactly what this design avoids.

## Compatibility and reversibility

- Internal manifest schema v6 and recovery sidecar schema v9 are unchanged.
- Legacy bare refs resolve unchanged; suffixed refs are ordinary Lance branch
  names, so an older binary can still open and read them by native name, but
  must not run branch lifecycle operations against a suffixed graph.
- Reverting is a code change: suffixed refs remain valid Lance refs, and a
  reverted binary would treat each as a distinct legacy branch named by its
  full native name.

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
- **Quarantine or phased deletes**: adds phases and state to reason about for a
  problem the path already solves. Rejected.

## Evidence and tests

- `branching.rs`: delete/recreate yields distinct native names, the recreated
  fork is named by the new incarnation, listing shows logical names,
  incarnation-shaped names are refused on create and read.
- `maintenance.rs`: cleanup reclaims a dead incarnation's fork while the logical
  branch is live and keeps the live fork.
- `failpoints.rs`: a lost-ack fork delete followed by recreate leaves the dead
  fork as garbage that the write never heals in place or reads; cleanup
  reclaims it. Existing recovery, merge, and change-feed ABA owners were
  converted to address forks by native name.
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

None for acceptance. Follow-ups out of scope: a name-only ref listing to drop
the per-ref GET on branch-bound opens; a sweeper for a dead life's manifest
tree when a delete crashed between ref removal and tree removal (inert garbage
today).

## Decision log

- 2026-08-30: Accepted by the maintainers as the fix for #562 over the
  registry-row design in PR #578; comparison recorded in the Alternatives
  section.
