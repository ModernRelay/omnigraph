# Legacy commit area of the storage upgrade

**Decides:** the offline storage upgrade writes every pre-upgrade graph commit once into immutable Lance files under `__history/legacy/`, found through flat digest-checked locator objects, so every old commit id stays addressable.
**Contract:** keeps E7
**Code:** `crates/omnigraph-catalog/src/history.rs`, `crates/omnigraph/src/db/upgrade.rs`, `crates/omnigraph/src/db/upgrade/tests.rs`, `crates/omnigraph-cli/tests/integration/crossversion_upgrade.rs`
**Cited as:** RFC 0068

## Context

Since the `__history` layout, a branch's `__manifest` holds its head and a
byte-bounded buffer of commits, and older commits live in immutable Lance
files under `__history`, addressed by the commit id (`hb1.<block>.<slot>.<nonce>`),
whose name is its address. Commits written before the upgrade (source stamps
8, 9 and 13) have ULID ids, which name no address. The upgrade must keep them
readable by id, through `commit show` and lineage walks, without a structure
that any later writer maintains.

## Decision

- The upgrade writes the legacy area once, with conditional creates, before it
  activates the graph; no later operation writes it.
- `legacy/data/<n>.lance` files are history extents: the same Lance V2_2
  columns, writer and reader as native blocks, each holding one writer's
  consecutive own commits in first-parent order.
- `legacy/locator/` holds id shards (`OGLI0001`, commit id to file and row),
  writer shards (`OGLW0001`, one entry per writer) and one directory
  (`OGLD0001`) that lists every data file and shard with its SHA-256. Each
  object fits one lineage read (`TAIL_BYTES`).
- The upgrade reads every object again against those digests before
  activation. A first-parent gap refuses the upgrade; a merged parent absent
  from the source is kept on its commit and listed as known absent.
- A lookup by id on a handle that has read the directory costs one shard read
  and one data-file read. A root born on the current layout pays one directory
  miss per handle.
- After a ref is converted its `__manifest` holds the head alone, so the
  legacy files are the only copy of the earlier commits and the locator is
  derived from them; neither is rewritten, so they cannot drift.

## Rejected

- **A Lance dataset with a scalar index on commit id:** brings a manifest, a
  version chain, a transaction log and index files for content one fenced
  offline process writes once, and a lookup would read a manifest and index
  pages instead of one bounded shard.
- **A single-level id map:** at a million commits it is larger than the
  history extent cache holds.
