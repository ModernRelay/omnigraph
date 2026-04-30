# Runs — REMOVED (MR-771)

The Run state machine and `__run__<id>` staging branches were removed in
MR-771. `mutate_as` and `load` now write **directly to the target table**
and call `ManifestBatchPublisher::publish` once at the end with
`expected_table_versions` (the per-table manifest versions captured before
the first write). Cross-table OCC is enforced inside the publisher; the
publisher's row-level CAS on `__manifest` is the single fence.

## What this means in practice

- No `RunRecord`, no `_graph_runs.lance`, no `_graph_run_actors.lance`.
- No `omnigraph run *` CLI subcommands and no `/runs/*` HTTP endpoints.
- No `__run__<id>` staging branches. (Legacy on-disk artifacts from
  pre-MR-771 repos are inert; MR-770 sweeps them in production.)
- Cancelled mutation futures leave **no graph-level state** — only orphaned
  Lance fragments, which the existing `omnigraph cleanup` pipe reclaims.

## Read-your-writes within a multi-statement mutation

A `.gq` query with multiple ops (e.g. `insert Person … insert Knows …`)
must observe earlier ops' writes when validating later ops (referential
integrity, edge cardinality). After demotion this is implemented via an
in-process `MutationStaging` accumulator in
`crates/omnigraph/src/exec/mutation.rs`:

- On the first touch of each table, the pre-write manifest version is
  captured into `expected_versions[table_key]`.
- Subsequent ops on the same table re-open the dataset at the locally
  staged Lance version (bypassing the manifest, which has not been
  committed yet) so they see prior writes.
- One `commit_with_expected(updates, expected_versions)` at the end
  publishes the lot atomically. Cross-table conflicts surface as
  `ManifestConflictDetails::ExpectedVersionMismatch`.

This upholds [docs/invariants.md §VI.23](invariants.md) (atomicity per
query) and §VI.25 (read-your-writes within a multi-statement mutation —
previously aspirational, now upheld).

## Conflict shape

Concurrent writers to the same `(table, branch)` produce exactly one
success and one failure. The losing writer's error is
`OmniError::Manifest` with kind `Conflict` and details
`ManifestConflictDetails::ExpectedVersionMismatch { table_key, expected,
actual }`. The HTTP server maps this to **409 Conflict** with body
`{"error": "...", "code": "conflict", "manifest_conflict": { "table_key":
"...", "expected": N, "actual": M }}` — see [docs/server.md](server.md).

## Audit

`actor_id` lands in `_graph_commits.lance` via `record_graph_commit` (no
intermediate run record). Audit history is queried via `omnigraph commit
list`.

## Migration code

`db/manifest/migrations.rs` does not change. Active deletion of
`_graph_runs.lance` belongs in MR-770 (the production sweep) — this PR
stops *creating* run state but does not destroy legacy bytes on disk.

## Known limitation: mid-query partial failure on the same table

A multi-statement `.gq` mutation where op-N writes a Lance fragment
successfully and op-N+1 then fails leaves the touched table at
`Lance HEAD = manifest_version + 1`. The query is atomic at the manifest
level (the publisher never publishes, so reads at the pinned manifest
version do *not* see op-N's data), but the *next* mutation against the
same table fails loudly with
`ManifestConflictDetails::ExpectedVersionMismatch` because
`ensure_expected_version` enforces strict equality between Lance HEAD and
the manifest's pinned version.

**Why the engine doesn't auto-rollback**: Lance's `Dataset::restore()` is
*not* a rewind — it appends a new commit (containing the desired
historical version's data) and advances HEAD further. There is no Lance
API to delete a committed version. A proper fix requires writing each
mutation's per-table fragments to a *transient Lance branch* on the
sub-table, then fast-forwarding main on success or dropping the branch
on failure. That work is tracked as a follow-up to MR-771; in the
meantime:

- **In practice this is rare.** Most schema-language validation
  (`@key`, `@enum`, `@range`, intra-batch uniqueness, edge-endpoint
  existence) runs *before* any Lance write inside the failing op, so
  single-statement mutations never trip this. The narrow path is
  multi-statement queries (`insert ... insert ...`,
  `insert ... update ...`) where a late op fails on validation that
  depends on earlier ops' staged data.
- **Workaround**: callers that hit this should refresh the handle and
  retry the mutation; if Lance HEAD remains drifted the
  `omnigraph cleanup` command will GC the orphan version once a later
  successful commit on the same table moves HEAD past it. (`cleanup`
  cannot reclaim an orphan that *is* the current Lance HEAD; that case
  needs the per-table-branch follow-up to fully heal.)

The cancellation case (future drop mid-mutation) has the same shape and
the same workaround.
