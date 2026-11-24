# Graph diffs, change feeds and retained history

**Decides:** entity changes are derived on read from two pinned graph snapshots, in graph vocabulary, through one enumerator that the commit diff, the change feed, the baseline and the graph diff share, with a caller-owned cursor and no persisted change log.
**Contract:** keeps E1, E5, read:5, read:6, read:7, read:12, E7
**Code:** `crates/omnigraph/src/changes`, `crates/omnigraph/src/exec/merge.rs`, `crates/omnigraph/tests/integration/changes.rs`, `crates/omnigraph/tests/integration/changes_cost.rs`
**Cited as:** RFC 0030

## Context

Agents need to know what a commit, a branch or a range of history changed, and
consumers need to tail a branch without missing a commit. Lance keeps per-table
versions and row-version stamps, but it has no complete deleted-row stream, and
a delta is exact only on a handle checked out at the end version. A change log
table, a WAL or a server-side cursor registry would be a second source of truth
beside `__manifest` lineage.

## Decision

- A change is an entity: kind, opaque graph type identity and type name,
  logical `id`, operation, and exact images. An insert carries `after`, an
  update `before` and `after`, a delete `before`; edge endpoints belong to each
  image. No table, dataset, fragment, version or row address is public.
- Insert, update and delete are defined only by the two graph-visible states.
  Equal images are no change. Row-version stamps, transaction records and
  fragment metadata only select candidates; the exact ordered comparison of
  both pinned versions is the authority and the fallback.
- The commit diff compares one commit with its first parent and carries the
  cause (commit, parents, authored branch, actor, `authored_at`) once per block.
  The parentless root has no diff.
- The feed delivers a branch's first-parent chain in order. A merge commit's
  block is relative to its first parent and names the merged parent. Physical
  commits give empty blocks and still advance the cursor.
- A page token continues one bounded response; a feed cursor resumes after a
  complete commit and is returned only on a block's terminal page. Both are
  opaque, versioned, checksummed, and bind graph, branch incarnation and filter
  scope; any cross-use or mismatch is a typed refusal.
- Feed start is explicit: `Now` (default), `AfterCommit`, `Beginning`. A start
  off the branch's first-parent chain is refused.
- The baseline exports the entity snapshot at one captured head and returns a
  cursor after it. It is the only reset after a gap.
- Retention is checked per participant version, never against a watermark. A
  reclaimed participant is `ChangeFeedGap`; a page is atomic and never advances
  past it.
- The graph diff compares two read targets, or a branch with its merge base,
  with no cause and no folding of intermediate history. A name that is both a
  branch and a commit is refused. The branch form requires `read` on both
  branches, continuations included. `branch merge --dry-run` is a read that
  publishes nothing.
- Entity and schema diffs are separate. A pair whose paired lifetimes differ in
  user schema refuses with `ChangeSchemaBoundary`; nothing is synthesized.
- Filters select kind, type name and operation only.
- Ordered scans sort keys only, run with spilling under an explicit memory and
  scratch envelope, and refuse when spilling is bypassed. One legal change that
  exceeds the page byte target is delivered alone, never truncated.
- Discovery across a window with no adjacency proof is in
  [change discovery by fragment identity](2026-10-26-change-discovery-by-fragment-identity.md).

## Rejected

- **A persisted change log or delete tombstones:** a second record beside the
  rows, changing every write path and the format, for what pinned snapshots
  already answer.
- **Lance stamps or deletion vectors as authority:** overwrite and restore make
  a reused `id` look new, and deletion vectors omit compacted and whole-fragment
  deletes.
- **One token for paging and resuming:** a caller could checkpoint inside a
  commit and skip the rest of it.
- **A scalar oldest-readable watermark:** cleanup leaves holes per table, so one
  floor would be false.
- **An `--at-time` read by binary search over commit time:** commit time is
  writer-clock authorship time and not monotonic.

## Not built

- Newest-first entity history over the same enumerator.
- Crossing a schema boundary by decoding each endpoint under its archived
  schema.
- A derived publication time.
