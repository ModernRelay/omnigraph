# Writes carried over row-preserving publications

**Decides:** a write whose branch moved only by commits that change no row (Lance `Rewrite` and `CreateIndex`) is re-anchored on the current head and rebased by Lance's own conflict resolver before its first effect, and any write sent back prepares again once under the branch's writer gates.
**Contract:** keeps E1, E5, E7, write:3, maintain.changes
**Code:** `crates/omnigraph/src/db/omnigraph.rs` (`revalidate_write_txn_carrying`, `row_preserving_tables`), `crates/omnigraph/src/table_store.rs` (`row_preserving_chain`, `rebase_staged_write`), `crates/omnigraph/src/db/reprepare.rs`, `crates/omnigraph/tests/failpoints.rs` (`issue_1331` tests), `crates/omnigraph/tests/lance_surface_guards.rs`

## Context

A mutation, a load or a Blob write prepares outside the writer gates and revalidates its captured authority under them. A strict comparison sent the write back on any publication in between, including an `optimize` that changed no row. Under steady writes on object storage, writes moved in lock-step with maintenance and strict updates and deletes starved. Lance itself treats `Append` as compatible with `Rewrite`, and `Update` or `Delete` as conflicting only on overlapping fragments. Omnigraph never asked, because every graph-table write is a detached commit, which runs no conflict pass.

## Decision

- Under the gates, a moved head is carried only when the schema IR hash, identity domain and identity version are unchanged; the same table keys are registered with the same identity, dataset path and native ref; every moved table kept its `entity_count` and its version did not go down; and every moved pin reaches the captured pin through `read_version` links that are all `Rewrite` or `CreateIndex`, at most `ROW_PRESERVING_CHAIN_MAX_LINKS` (32) per table.
- The rule reads what the commits record, never who published them. No marker on a graph commit says "maintenance".
- Any failed check returns the strict path's `ReadSetChanged` on `graph_head:<branch>`. An unreadable link (removed by `cleanup`) is a failed check, not a storage fault.
- A carried write keeps its catalog, branch, caller head, policy decision and staged data, and takes the current snapshot, head and probe. A table it only read keeps its moved pin. A staged table goes through `TransactionRebase` over the links, records the current pin as its read version, and commits detached there, so compaction stays and the table's history stays one chain.
- Lance decides compatibility. An update, delete or upsert of a row in a fragment a rewrite replaced is a conflict, and the attempt is sent back.
- A caller's named head (`expected_head`, `--if-commit`) is never carried, and neither is an unattached base.
- A write sent back, for any cause, reruns its request (its query, or its input) on the current head. Its next attempt holds the branch's writer gates from capture through publication (`PreEffectReprepares`). Every publisher on the branch in the process, optimize included, takes that gate, so a write finishes within two attempts in one process. Publications by another process are bounded by `MAX_PRE_EFFECT_REPREPARES` (32).
- A later attempt of a write that selects existing rows (update, delete, overwrite load, Blob write) returns `ReadSetChanged` if the branch incarnation or accepted schema differs from its first attempt's. An insert, append or merge goes on.
- The publisher, its exact pin CAS and `ExactGraphHead` are unchanged. The graph commit is parented on the head it was carried to.
- Optimize does not wait for writers.

## Rejected

- **Carry only when the staged tables did not move:** writes add the small fragments optimize compacts, so the common case is a write to the table just compacted.
- **Maintenance loses (the write's pin replaces the compacted pin):** compaction of hot tables starves, the publisher's exact pin CAS turns into a judgment about other commits, and table histories fork so `read_version` walks break.
- **Mark maintenance commits:** a format change that trusts the publisher's claim instead of checking the tables, and every future maintenance path must keep it true.
- **Keep or narrow the writer yield (optimize waits for in-flight writes):** it schedules maintenance around the writers it hurts, keeps a second record of writer state, does not bound writers that send each other back, and gives up after a timeout.
- **Keep strict writes strict (`ReadSetChanged` for an overtaken update or delete):** once other writers are bounded, strict writes lose nearly every race, and the caller's only remedy is to resend the same request, which the rerun does.
