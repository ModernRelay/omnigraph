---
rfc: "2026-10-10-merge-base-block-registry"
title: "Block registry for merge base lookup"
track: maintainer
status: draft
implementation: in-progress
authors:
  - Azim Afroozeh
created: 2026-10-10
updated: 2026-10-10
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Block registry for merge base lookup

> A term set in ***bold italics*** is being defined at that exact spot;
> it is used in plain text everywhere after.

Code references are at `main` `980498d6` plus this change's tree unless
marked. Lance references are at the pinned 11.0.0 crates.

## Summary

`branch merge` resolves its ***merge base***, the newest commit both branches
descend from (a commit both descend from that no other such commit descends
from, with ties broken by the fewest commits to the two heads summed, then the
higher generation, then the smaller commit id), by walking the commit graph
from the two heads. Commits older than a branch's `__manifest` buffer live in
`__history` blocks, and the walk reads one block object per block it crosses,
so a cold merge against a long history pays one GET per block between the
heads and the base.

This RFC adds a ***block registry*** to each branch's `__manifest`: two row
kinds, `history_block` and `history_merge`, that record the shape of every
released history block the branch wrote or merged (the run of slots, the
generation of its first slot, its parent, and every merge edge inside it).
The walk crosses a registered block without reading it and reads `__history`
only for commits no row covers and for final candidates tied on distance and
generation, whose records determine the textual-ID tie break.
The rows live in Lance fragments of `__manifest`: a ***hot fragment***
rewritten by every publish that holds the current rows plus the newest
registry rows, and ***cold fragments*** reused by later versions. A publish
that releases or imports enough new registry rows repacks the smaller cold
fragments; other publishes leave their files untouched. The registry is derived state: `__history`
stays the authority and a block without rows is walked as it is today.

The new row kinds are storage format 15. Format 14 graphs are served as they
are and each branch is stamped 15 by its first publish under this binary, in
the same compare-and-swap that writes its first rows; nothing is converted
offline and nothing is backfilled.

## Motivation

The merge base walk (`commit_graph.rs`, `lazy_merge_base_search`) pops
commits by generation and reads a commit it does not hold from `__history`,
batched by id, one object GET per block crossed and cached per handle. The
cost grows with the blocks between the two heads and the base. The number of
commits per block depends on their encoded size and the release budget.
The walk reads whole blocks to learn a chain it could know from a
few numbers: within a block, slot `n`'s first parent is slot `n - 1` and the
generation rises by one per slot, more at a merge.

The commit id already names the block and the slot
(`hb1.<block ulid>.<decimal slot>.<nonce>`, `publisher.rs`), so a registry that
records, per released run, the block, the slot range, the first slot's
generation and parent, and the merge edges inside the run, lets the walk step
from a block's last slot to its first in one hop and to the parent block from
there. Keeping that registry in `__manifest` rows, which every publish
rewrites, would make every publish copy the growing history, so the rows need
a place that a publish names but does not rewrite.

## User and operational behavior

`branch merge` keeps its selected base, merged rows and conflict outcome: the
registry changes what the walk reads, not what it selects. A merge whose
heads' histories carry no rows (a graph written before this change, or a
block whose run failed the chain check below) reads blocks as before.

Storage format 15 is served together with 14 (`migrations.rs`,
`MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION` 14, `INTERNAL_MANIFEST_SCHEMA_VERSION`
15). A format 14 branch opens unchanged and is stamped 15 by its first
publish under this binary; a binary of format 14 then refuses the branch
before any open, naming the stamp, as every stamp bump does. A standalone
format 8, 9 or 13 graph reaches 15 through the offline `omnigraph upgrade`,
which now converts to the current format. Upgrade every binary before the
graph.

An operator sees registry rows as `__manifest` fragments: one hot fragment
per version, and cold fragments whose files are shared by many versions. No
new object namespace is created and no cleanup path changes; `__manifest`
versions are retained as before.

## Design

### Rows

Two `object_type`s join `__manifest`'s rows (`lib.rs`,
`OBJECT_TYPE_HISTORY_BLOCK`, `OBJECT_TYPE_HISTORY_MERGE`), carried by the same
packed `record` struct as every other row. The struct gains three children
after the replaced-table fields, `block_id`, `first_slot` and `last_slot`
(`record.rs`, `REGISTRY_FIELDS`); a format 14 record lacks exactly these
children and the reader fills them with nulls.

- A `history_block` row describes one ***released run***, a first-parent chain
  of slots `first_slot..=last_slot` of one block, as `BlockRun`
  (`state.rs`): the block, the slot range, the generation of `first_slot` and
  `first_slot`'s parent commit id (none for a genesis run). A later slot's
  generation is its distance from `first_slot` plus the jumps of the merge
  edges between them.
- A `history_merge` row describes one merge commit inside a released run, as
  `MergeEdge`: the block, the slot, the generation the merge gave the slot
  and the merged parent's commit id.

A branch's registry (`LineageRegistry`) is the sorted set of the runs and
merge edges of every block its branch wrote or merged, one copy of each. The
runs of one block may overlap after repeated merges and describe one chain.

### Chain check at release

The publisher builds the rows of the commits a publish releases
(`LineageRegistry::of_released`): one run per block the ids name, split where
the block or the slot sequence changes, and one edge per merged parent. It
refuses a run whose commits are not the chain their ids claim: a slot's first
parent must be the slot before it, and its generation one above it, or more
at a merge. A refused run gets no rows, the publish logs a warning and goes
on, and the walk reads that block from `__history` (`publisher.rs`,
`released_registry`). The check is what lets a reader trust a run without
reading the block.

A merge copies the captured source's registry into the target's
(`publish_with_precondition`, `merged_parent.registry`), so a merge commit's
second parent is reachable through rows the target now holds.

### Fragments

A publish is a Lance `Overwrite` with zero retries (`commit.rs`,
`overwrite`): Lance's strict overwrite, the one commit it never rebases over
a concurrent version, so the version number stays the put-if-absent
compare-and-swap. The overwrite's fragment list carries the previous
version's cold fragments by reference and a freshly written hot fragment
last. Lance gives every listed fragment a fresh id, so the hot fragment is
the one with the highest id and a cold file is known by its path; the cold
files are neither read nor written by an ordinary publish.

The hot fragment holds the head, the buffer, the `table` rows, the schema
contract and up to `REGISTRY_HOT_ROWS` (64) registry rows, about 100 bytes
each. When a publish leaves more than that in the hot set, it moves every
registry row into a cold fragment of its own (`LineageRegistry::cold_push`).
Cold fragments are repacked by the chain rule of git's `commit-graph`
(`fragments_to_repack`): the push joins existing cold fragments smallest
first while a fragment holds fewer than twice the rows joined so far, so the
fragments of a version hold geometrically growing row counts and a row is
rewritten a logarithmic number of times. A push reads only the fragments it
joins.

The overwrite rebases nothing, so a stale publish still loses its
compare-and-swap; an `Operation::Update` was tried and rejected because Lance
rebases it over a concurrent commit it deems compatible and the stale publish
lands at the next version.

### Reads

Open scans the hot fragment alone (`state.rs`, `hot_scanner`,
`Scanner::with_fragments`): the open path reads no cold fragment and decodes
the same rows it did at format 14 plus at most 64 registry rows.

A merge reads both heads' registries (`CommitGraph::registry`): the hot rows
the handle already holds plus the cold fragments of the head's `__manifest`
version, cached by data-file path in the handle's `HistoryCache`, because
fragment ids repeat across Lance branches of `__manifest`. Above the
`COLD_REGISTRY_FILES` threshold of 64, cached files not named by the current
read are dropped. The two registries merge into one `RegistryIndex`.

The walk addresses a commit by its slot when a run covers it and by its id
otherwise (`Node::Slot`, `Node::Id`). Popping a covered slot costs no read:
its generation comes from the run, its merged parent from the merge row, and
its first parent is the slot below, or the run's parent at `first_slot`.
Between one event and the next (a merge row, a slot the walk already holds,
the run's first slot) the walk crosses the slots in one step, charging 64
bytes to its byte budget per crossed segment. An uncovered commit is fetched
by id as today, and a block the registry does not describe is walked slot by
slot from its records. To resolve final candidates tied on distance and
generation, it reads their records (`read_commit_at_slot_in` for a candidate
reached by slot); these candidates can span multiple blocks. A run whose generation
disagrees with a record the walk did read is an integrity error
(`registry_disagrees`), never a silent preference.

The selection rule is unchanged: among the lowest common ancestors, the
fewest commits to the two heads summed, then the higher generation, then the
smaller commit id (`Candidate`, `pick`). Visits stay bounded by
`LAZY_MERGE_VISITS` and bytes by `LAZY_MERGE_BYTES`.

## Invariants

- `__history` is the authority for released commits; the registry is derived
  from it at release and is never read where a record is needed. A slot no
  run describes is read from its block.
- A run's rows assert the chain rule for every slot they cover; the publisher
  checks that rule before writing them and writes nothing for a run that
  fails it.
- The single publication door is unchanged: the rows and the fragments that
  carry them are published in the one `__manifest` compare-and-swap that
  publishes the head. There is no second head, pointer, or namespace.
- Ordinary publish cost does not depend on the count of released blocks: a
  publish writes the hot fragment and names the cold fragments; a push writes
  one cold fragment and reads only the fragments it joins.
- Generic `optimize` leaves the registry fragments intact. The catalog publisher
  owns their layout and writes the hot rows as exactly one fragment.
- A merge imports only registry rows absent from its captured target. Both
  registries are loaded for the walk; subtraction adds no publication-time scan.
- Open cost does not depend on the registry size beyond the hot bound.
- The merge base selected with rows equals the one selected without them, for
  every graph where both walks finish (the random-DAG oracle in the catalog
  tests pins this).

## Compatibility and reversibility

Format 15 is a format 14 row set plus registry rows: the packed `record`
gains three children at the end, a format 14 record lacks exactly those
children, and the reader fills them. Normal open serves 14 and 15; a format
14 branch is read as it is and stamped 15 by its first publish under this
binary, in the compare-and-swap that writes its first rows. A format 14
binary refuses a format 15 branch on its stamp before any open, as it would
refuse the unknown `object_type` if it read the rows. The offline
`omnigraph upgrade` converts a standalone format 8, 9 or 13 graph to 15; it
has no 14 to 15 step, because a 14 graph needs none.

No row exists until a block closes under this binary, so an upgraded graph's
first merges read blocks as before and gain rows as releases happen; there is
no backfill and the design needs none. Removing the registry is a format
bump that drops the two row kinds and the three children; the walk without
rows is the walk this change keeps as its fallback.

## Alternatives

| Approach | Tradeoff | Decision |
|---|---|---|
| History lookup only (the design minus the registry) | Smallest durable surface. A cold merge reads the history blocks between the heads and the base; the count grows with released history. | Kept as the fallback for blocks without rows. |
| One row per commit in `__manifest`, git `commit-graph` style | Cuts the reads but the walk still pops one node per commit, so a base 100,000 commits deep reaches `LAZY_MERGE_VISITS`. | Rows per run and per merge edge: the walk pops per block and per merge. |
| Every registry row in the hot fragment | No cold read on merge, but every publish rewrites the growing registry. | Hot bound of 64 rows, the rest in cold fragments a publish names. |
| One registry object per closed block under a sibling `__lineage/` namespace, addressed by the block id in the commit id | No stamp bump and no pointer, but one GET per block crossed again, a second namespace to enumerate and clean, and objects a publish cannot name. | Not taken: the cost it removes is the one the registry exists to remove. |
| Bounded JSON pages under an `__ancestry_index/` namespace, rooted by a string in `__manifest` schema metadata | Adds a separate namespace and JSON metadata root; smaller pages can require more object reads than the history blocks they replace. | Withdrawn. |
| A recorded merge base on the merge commit | Reuses an exact pair of heads that never recurs as a merge request once the merge commit exists; cannot prune the generation-ordered walk. | Not taken. |

Precedents in the tree: `__history` blocks are immutable files written
before the `__manifest` compare-and-swap that names them; the registry
follows that order and differs only in where it lives, inside `__manifest`'s
own fragment list, so the compare-and-swap that publishes the head is the
pointer and no second namespace needs enumeration or cleanup. The `__manifest`
Create commit stays the existence authority; registry rows never decide
whether a store or a commit exists.

## Evidence and tests

Catalog mechanism tests: a publish whose hot registry overflows writes a cold
fragment and the hot fragment after it, open reads the hot fragment alone, a
cold read returns the rows, an ordinary publish names the cold file again
without rewriting it, and pushes repack by the chain rule
(`cold_registry_fragments_are_pushed_reused_and_repacked`,
`repack_follows_the_commit_graph_chain_rule`); the walk over registry runs
selects the same base as the complete graph and never reads a covered block
(`lazy_merge_over_registry_runs_matches_complete_and_skips_covered_blocks`);
a stale publish still loses its compare-and-swap; `of_released` splits runs
at a block or slot gap, records a merge edge, and refuses a run that breaks
the chain rule; a format 14 record decodes with the three children absent.

GQT cases pin the selection: criss-cross ancestry, repeated merges and
fast-forwards through a merged parent, a delete surviving a merge after an
older branch merge, and the cold merge after the target merged an older
branch (`merge_base_*.gqt`, `issue_773_*.gqt`,
`cold_merge_base_after_target_merged_older_branch.gqt`).

## Rollout

1. Rows, stamp 15 and the fragment layout land together with the walk over
   runs; a graph gains rows as blocks close under the new binary.
2. Compare the checked-in merge corpus and catalog mechanism tests against
   the walk without rows.
3. Version retention for `__manifest`, which would also reclaim cold files no
   retained version names, is outside this change.

## Unresolved questions

- `REGISTRY_HOT_ROWS` (64) and `COLD_REGISTRY_FILES` (64) are implementation
  constants chosen from the row size; neither is measured against a graph
  with thousands of released blocks.

## Decision log

No external decisions recorded.
