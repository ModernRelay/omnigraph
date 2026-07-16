# Merging (three-way) and Conflicts

`exec/merge.rs`.

## Strategy

The fast-forward all-new case first attempts an inductive substrate proof before
entering the general ordered merge. Its durable link is the exact Lance
transaction property `omnigraph.insert_absence = "v1"`: every key encoded by
that transaction's exact-`id` inserted-row filter was proven absent from the
transaction's effective parent.

The sealed general keyed adapter can mint that certificate in two cases:

- `StrictInsert` has completed its exact target-ID preflight and staged the
  corresponding pure insertion-only transaction. Certification is mandatory
  for that shape and fails closed if its filter or effect is unfamiliar.
- An all-new `Upsert` may be certified only when Lance's completed statistics
  report one attempt that inserted every source row and updated, deleted, and
  skipped zero rows, and the same structural checks pass. This is an optional
  optimization: failure to certify an otherwise valid upsert leaves the write
  valid and simply makes its history ineligible for this shortcut.

BranchMerge admits the no-target-preflight route only when the **complete**
retained `(base_version, source_version]` chain verifies:

- base and source name the same stable table identity/path, use stable row IDs,
  and have an exact non-null UTF-8 `id` PK. Lance
  `BranchIdentifier::find_referenced_version` must place the base at the exact
  graph merge-base version (same-ref equality is required when both snapshots
  name one ref);
- the transaction count equals the numeric version interval, and every link
  has a UUID, reads exactly the previous version, and carries the exact v1
  property;
- every link is a pure insertion-only `Operation::Update`: no removed or
  updated fragments, modified fields, merged generations, or updated-fragment
  offsets; at least one new fragment; `RewriteRows` mode; and an inserted-row
  filter over exactly the physical `id` field ID;
- `fields_for_preserving_frag_bitmap` equals the source schema's **full nested
  preorder** of field IDs. This prevents existing indexes on top-level or
  nested fields from falsely claiming coverage of the new fragments;
- every new fragment reports `physical_rows`, and the total across the chain
  equals the manifest row-count delta.

The pinned source is then read by `_row_created_at_version` directly. A
read-only normalizer lazily compacts one retained-parent Arrow slice at a time
and splits/coalesces ordinary or blob-materialized rows into the same
8,192-row / 32-MiB boundaries used by the writer. It does not enable Lance's
row-only `strict_batch_size`, which can concatenate past the byte target. Those
exact boundaries are pre-minted into the existing recovery chain.

Physical publication on this proven route performs **no target ID preflight
and no target merge join**. Lance's public `InsertBuilder` is used only to stage
immutable fragment files. Before commit, OmniGraph replaces its uncommitted
`Append` descriptor with the filtered insertion-only `Update` shape above,
including the full nested schema preorder and a freshly validated v1
certificate. No Lance `Append` is committed. The output transaction therefore
forms the next valid link in the proof chain: a later branch generation can
prove and replay it again rather than falling back merely because this route
created the source rows.

The exact source and target native `BranchIdentifier`s are rechecked under the
final schema → branch → table gates, together with source manifest/HEAD
agreement and the existing target's ordinary manifest/HEAD baseline. Numeric
path/version equality cannot substitute for either incarnation because a ref
can be deleted and recreated at the same version. A post-proof ABA is typed
`ReadSetChanged` before the sidecar. The proven data-replay route is
deliberately not admitted for a first-touch lazy target; that case keeps the
existing ref-only fork/adopt path and creates no target data transaction.

Missing or cleaned transaction files, an unknown property value, a gap,
unfamiliar operation, incomplete schema preorder, row-total mismatch, or a
non-descendant ref is an optimization miss and falls back to the general
ordered diff and ordinary fenced writes. The v1 property is not a signature or
a trust mechanism for arbitrary Lance writers: raw direct-Lance mutation of a
graph table is unsupported, and the marker is accepted only together with the
complete structural, ancestry, row-count, schema, and authority proof above.

This shortcut removes the base scan, row signatures, target merge join, and
temporary delta Lance write without collapsing the delta into one transaction.
Beta.21's forced-v2 merge constructs its own unbounded DataFusion runtime, so
one whole-delta transaction is not described or accepted as pool-bounded. The
normalizer hard-bounds every writer chunk and retains only one
approximately-sized Lance raw emission plus bounded working pieces; Lance's
approximate `batch_size_bytes` target is not described as a hard raw-decoder
cap.

The final predeclared 2026-07-15 production acceptance series ran five matched
pairs at each size against the labeled direct-Lance comparator. At 10K rows,
median operation time was 31 ms versus 8 ms (**3.875×**) and maximum signed
paired peak-RSS overhead was 24,297,472 bytes. At 100K, the medians were 136 ms
versus 35 ms (**about 3.886×**) and maximum overhead was 32,604,160 bytes.
Both are below the fixed 5× / 64-MiB gates; every production route assertion,
exact-content verification, and setup/operation/verification phase passed.

The general route remains an ordered, row-by-row cursor merge:

- `OrderedTableCursor` scans each table sorted by `id` and supports peek/pop
  matching. Every production cursor explicitly sets both Lance scanner limits:
  **8,192 rows and 32 MiB decoded bytes per batch**; it never inherits the
  process/environment default for this retained transform.
- `StagedTableWriter` copies rows into owned batches and flushes actual chunks at
  the first of **8,192 rows or 32 MiB of Arrow memory** into a temporary Lance
  dataset (`OMNIGRAPH_MERGE_STAGING_DIR`). Blob columns are materialized under
  that byte budget; declared external-blob sizes are checked in aggregate before
  reading payloads, so small descriptors cannot hide one oversized row. This
  copy is required because Lance's merge-insert builder has no `WriteParams`
  hook for `allow_external_blob_outside_bases`; staged Overwrite retains external
  references because it does accept `WriteParams`.
- New-row chunks use exact-`id` `StrictInsert`; changed-row chunks use exact-`id`
  `Upsert`. The complete ordered chain is pre-minted under one `protocol_v4`
  recovery sidecar, with at most **1,024 logical data transactions per table**.
  A larger row or plan returns typed `ResourceLimitExceeded` before sidecar arm.
- Exact recovery scans at most **1,026 versions**: the 1,024 logical data
  transactions plus headroom for one allowed derived `CreateIndex` tail and one
  compensating `Restore`. The restore headroom is required because recovery can
  crash after restoring the table but before publishing the manifest outcome.
- The merge runs per sub-table, but all chunks become graph-visible through one
  atomic manifest update. Once the sidecar is armed, any chunk conflict retains
  recovery ownership and returns `RecoveryRequired`; the merge never retries
  semantically around a committed prefix.
- Integrity validation projects staged deltas to `id`/`src`/`dst` plus scalar
  properties and streams those batches under the same row/byte scanner limits.
  Because the unified evaluator currently needs one cross-table `ChangeSet`,
  exact projected Arrow bytes are charged before each batch is retained against
  one deterministic, operation-wide **32 MiB** budget (deleted-ID clones are
  charged conservatively too). Crossing it returns typed
  `ResourceLimitExceeded` before a recovery sidecar or table effect; this keeps
  many individually valid chunks/tables from reassembling into an unbounded
  scalar delta.

## Outcome enum

`MergeOutcome { AlreadyUpToDate | FastForward | Merged }`

## Conflict types (`error.rs`)

```
MergeConflictKind:
  DivergentInsert        // same id inserted on both branches
  DivergentUpdate        // updated differently on both branches
  DeleteVsUpdate         // one side deletes, other updates
  OrphanEdge             // edge references a node deleted by the other side
  UniqueViolation
  CardinalityViolation
  ValueConstraintViolation
```

Returned as `OmniError::MergeConflicts(Vec<MergeConflict { table_key, row_id?, kind, message }>)`. The HTTP server surfaces this as a 409 with structured `merge_conflicts[]` (top 3 + "+N more").
