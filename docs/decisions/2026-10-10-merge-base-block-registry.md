# Block registry for merge-base lookup

**Decides:** each branch's `__manifest` carries derived `history_block` and `history_merge` rows describing every released `__history` block it wrote or merged, so the merge-base walk crosses a described block without reading it.
**Contract:** keeps E5, E7, merge:1, merge:6
**Code:** `crates/omnigraph-catalog/src/commit_graph.rs`, `crates/omnigraph-catalog/src/state.rs`, `crates/omnigraph-catalog/src/commit.rs`, `crates/omnigraph-catalog/src/publisher.rs`, `crates/omnigraph-catalog/src/record.rs`

## Context

`branch merge` finds the newest commit both heads descend from by walking the commit graph by generation. Commits older than a branch's buffer live in `__history` blocks, so a cold merge read one block object per block it crossed between the heads and the base, and the cost grew with released history. The commit id `hb1.<block>.<slot>.<nonce>` already names the block and slot, and within a block slot `n`'s first parent is slot `n - 1`, so a few numbers per block describe its chain. Keeping those numbers in rows that every publish rewrites would make every publish copy the growing history.

## Decision

- A `history_block` row describes one released run of one block: the slot range, the generation of its first slot and that slot's parent. A `history_merge` row describes one merge commit inside a run: its slot, its generation and its merged parent.
- At release the publisher builds the rows and checks the chain their ids claim. A run that fails the check gets no rows and a warning; the walk reads that block from `__history`.
- A merge copies the captured source's registry rows that the target lacks into the target, so the merge commit's second parent stays reachable through rows the target holds.
- Rows ride the one `__manifest` publication. Up to `REGISTRY_HOT_ROWS` (64) sit in the hot fragment every publish rewrites; past that they move to a cold fragment that later versions name by reference. Cold fragments repack by the chain rule of git's `commit-graph`, so a row is rewritten a logarithmic number of times. Open scans the hot fragment alone.
- The walk crosses a covered run in one step and reads records only for uncovered commits and for final candidates tied on distance and generation. A run that disagrees with a record the walk did read is an integrity error.
- `__history` stays the authority. The registry never changes the selected base: among the lowest common ancestors, the fewest commits to the two heads summed, then the higher generation, then the smaller commit id. Visits and bytes stay bounded.
- The rows are storage format 15; there is no backfill, and a graph gains rows as blocks close.

## Rejected

- **No registry, history reads only:** the cold merge cost grows with released history; it stays as the fallback for blocks without rows.
- **One row per commit, as git `commit-graph` does:** the walk still pops one node per commit, so a deep base exhausts the visit budget.
- **Every registry row in the hot fragment:** every publish rewrites the growing registry.
- **One registry object per block under a sibling namespace:** one read per block crossed again, plus a second namespace to enumerate and clean.
- **A recorded merge base on the merge commit:** the exact pair of heads never recurs once the merge commit exists, and it cannot prune the walk.
