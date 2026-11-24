# The manifest version as the table registration clock

**Decides:** a table's pins on a branch are ordered by the `__manifest` version of the publish that wrote each pin, never by the table's Lance version number, and a write's pin names the native ref it read.
**Contract:** keeps E1, E3, write:3, merge:1, merge:2
**Code:** `crates/omnigraph-catalog/src/state.rs`, `crates/omnigraph-catalog/src/publisher.rs`, `crates/omnigraph-catalog/src/lib.rs`, `crates/omnigraph/src/exec/merge.rs`
**Cited as:** RFC 0062

## Context

Lance numbers a branch's versions on from its fork point, so main and a branch forked at version 2 both mint 3, 4 and 5 on separate histories. A table's Lance version orders pins only inside one native ref. Ordering a table's registrations by that number made a merge that adopted a lower-numbered source pin invisible, with silent row loss after a reported fast-forward, and made an equal-numbered adopt collide. Two pins with one number on different refs also satisfied the same write precondition.

## Decision

- Every pin carries `manifest_version`, the `__manifest` version its publish lands at. The latest publish on the branch is the current pin; a greater Lance number never outranks a later publish.
- Within one branch's view the manifest version is unique per publish and increases in publish order. It is not compared across branches; a merge writes its pin on the target at the target's next version.
- The pinned Lance version keeps only its physical meaning: with `table_branch` it opens the dataset.
- A merge whose target table equals the base adopts the source's exact pin, whatever its Lance number relative to the target's.
- `TableVersionExpectation` carries `NativeRefPin`: `Exact(ref)` for a producer that read a registration, `Unchecked` for one that pinned a bare version. An `Exact` pin whose winner sits on another ref is refused as `ReadSetChanged` (`native_ref:<table>`).
- One publish carries at most one change per table identity. A dropped identity is never registered again; re-adding a type mints a new incarnation.

## Rejected

- **Route every low-numbered adopt through a delta written on the target's lineage:** order becomes a rule every registering path must keep, not a property of the row, and an equal-numbered adopt pays a data write for a pointer move.
- **Order by Lance's `_row_last_updated_at_version` on `__manifest`:** the logical order would come from a Lance physical system column, and a replaced row leaves the delta fold.
- **Re-key the row on `(identity, table_version, table_branch)` and keep number order:** removes the collision, but a later lower-numbered pin still loses and equal numbers still tie.
- **A counter per table identity:** a second sequence to maintain and backfill, ordering the publishes the manifest version already orders.
- **One graph-global clock:** needs a compare-and-swap across branches, and the projection only ever compares rows inside one lineage.
