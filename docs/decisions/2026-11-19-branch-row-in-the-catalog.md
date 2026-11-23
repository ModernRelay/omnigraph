# Branch existence in the catalog

**Decides:** a graph branch exists exactly when main's `__manifest` holds a live `branch` row for its native name `{logical}.{ULID}`, published under main's version compare-and-swap; Lance ref files are not written.
**Contract:** keeps E1, E2, E4, branch:1, branch:4, write+branch, branch+branch, merge:1
**Code:** `crates/omnigraph-catalog/src/branch_rows.rs`, `crates/omnigraph-catalog/src/lib.rs`, `crates/omnigraph-catalog/src/publisher.rs`, `crates/omnigraph-catalog/src/writer_epoch.rs`, `crates/omnigraph-core/src/branch_names.rs`, `crates/omnigraph-core/src/lance_clone.rs`, `crates/omnigraph/src/db/omnigraph/optimize.rs`

## Context

Every operation opens main's catalog, but branch existence lived in Lance's ref directory `__manifest/_refs/branches/`. Each existence check listed that directory and read files. A delete rewrote the ref with a retirement marker, archived it in the tree and unlinked it: three unconditional writes to mutable keys, each with its own recovery rule. The catalog and the directory were two records of one fact. A recreated branch must also never share a path or cache key with its dead predecessor.

## Decision

- Each branch life has a native name `{logical}.{ULID}`; the suffix is minted at create, never shown or accepted on a public surface, and is the life's identity in its tree path `__manifest/tree/<native>/`. A name without a canonical ULID suffix is a legacy life whose logical name is the whole name.
- The row is a config entry of main's version file, `omnigraph.branch.<native>`, holding Lance's `BranchIdentifier`, the source branch, the source version it forks from, the creation time and the attempt id. No data file is written for it.
- The row, the retired row and a tree's tombstone publish through the publisher as a strict `Overwrite` with zero Lance retries; the publisher's reload-and-retry loop is the only retry.
- Create publishes the row first, then writes the tree as a shallow clone. Name, ancestor/descendant (`review` beside `review/alice`) and source-liveness checks rerun on every attempt. A row with no tree is a not-ready branch, refused with `branch_not_ready`.
- Delete publishes a tombstone on the branch's own chain, settles its history into `__history`, then writes the retired row (value `-`) on main. A write that loses to the tombstone is refused `branch_deleted`.
- The row and the retired row assert main's writer epoch; the tombstone asserts nothing, so a dead claimant never pins a branch.
- Every existence and identifier read takes the rows of the main the operation holds. On a held main, `branch list` sends no request.
- `cleanup` reclaims every tree no live row names, with no age rule, except trees a tag, a retention root, a merge base or a kept tree's `base_paths` need. It then removes retired entries with a config-only `UpdateConfig`, deferring on conflict.
- The rows exist from storage format 18; below it the ref protocol runs unchanged, and the stamp route writes one row per live ref, then unlinks the ref files.

## Rejected

- **The ref file as the registry:** every existence read is a listing plus a file read, and a delete is three unconditional writes with three recovery rules.
- **The registration row with the ref file kept beside it:** two records of one fact; the ref is never read once the row exists, and its unconditional marker rewrite can recreate an unlinked ref.
- **The row in the catalog's data file:** a data-file put on every create and delete for under two hundred bytes.
- **`UpdateConfig` for the row:** Lance rebases it past concurrent commits, so the uniqueness check is not rerun against the version it lands on.
- **Tree then row:** a tree with no row would be either a create in flight or an orphan, and the collector would need an age to tell them apart.
- **The retired row alone, no tombstone:** a branch publisher reads its own chain, not main, so its publish could land on a tree nothing resolves.
- **Clearing caches on lifecycle events instead of the ULID suffix:** a clear cannot reach other handles, other processes or a loader in flight.
