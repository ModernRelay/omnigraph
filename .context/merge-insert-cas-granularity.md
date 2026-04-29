# MergeInsertBuilder CAS granularity (Lance 4.0.0)

**Status:** investigated (2026-04-28)
**Consumed by:** publisher per-table version API ticket — see "Implication" below.
**Companion ticket:** zombie-run investigation, `Prerequisite` section under "The demotion".
**Repro:** `.context/scratch/merge-insert-cas-repro/` — `cargo test --release -- --nocapture`. Both tests pass against `lance = "=4.0.0"`, demonstrating the answer.

## TL;DR

**Lance v4.0.0 supports row-level CAS for `MergeInsertBuilder` — but only when the join-key columns are annotated `lance-schema:unenforced-primary-key=true`.** Without that annotation, two concurrent `execute_reader()` calls inserting the same key into disjoint fragments **both succeed silently**, producing duplicate rows under the same key.

Our `__manifest` schema (`crates/omnigraph/src/db/manifest/state.rs:44-60`) does **not** carry that annotation, so today's `GraphNamespacePublisher::publish` has **no row-level CAS protection**. The TOCTOU window between `load_publish_state` and the merge-insert commit is open — two concurrent publishers computing the same `(table_key, table_version)` row both land it.

## Implication for the publisher API ticket

Take both layers, not one or the other:

1. **Annotate `object_id` (or the version-row subset) `lance-schema:unenforced-primary-key=true`** in `manifest_schema()` so Lance enforces row-level CAS at commit time. This closes the silent-duplicate hole that exists today, independent of the new feature.
2. **Add a pre-check phase in `publish` that validates `expected_table_versions` against the manifest snapshot** loaded by `load_publish_state`. The pre-check covers "expected unchanged" assertions for tables the caller is *not* writing to (Lance's row-level CAS only covers rows we *are* writing).
3. **Set `merge_builder.conflict_retries(0)`** and let the publisher's own caller-level retry loop handle the rebase: refresh manifest, re-run pre-check, re-attempt merge-insert. This gives strict atomicity for the per-table expected-version contract; Lance's auto-rebase would otherwise let our commit through against unfamiliar manifest state.

The ticket's "Approach 1 (parameter-only)" by itself is **insufficient** — Lance row-level CAS only catches collisions on rows we are emitting; it does not enforce "expected unchanged" for untouched tables. The ticket's "Approach 2 (pre-check + CAS)" is correct **only if** the row-level CAS is also enabled, otherwise the pre-check is TOCTOU-vulnerable and the existing publisher already has the silent-duplicate bug.

## How Lance does it (source-quoted)

### 1. Filter is built only when ON columns match the unenforced primary key

`rust/lance/src/dataset/write/merge_insert/exec/write.rs:209-221`:

```rust
// Check if ON columns match the schema's unenforced primary key
let field_ids: Vec<i32> = params
    .on
    .iter()
    .filter_map(|name| dataset.schema().field(name).map(|f| f.id))
    .collect();
let pk_field_ids: Vec<i32> = dataset
    .schema()
    .unenforced_primary_key()
    .iter()
    .map(|f| f.id)
    .collect();
let is_primary_key = !pk_field_ids.is_empty() && field_ids == pk_field_ids;
```

The filter is then attached only when `is_primary_key`:
`rust/lance/src/dataset/write/merge_insert/exec/write.rs:903-928`:

```rust
let inserted_rows_filter = if is_primary_key {
    // ... build KeyExistenceFilter (bloom or exact set)
};
// ...
inserted_rows_filter: inserted_rows_filter.clone(),
```

The PK is sourced from field metadata; from `inserted_rows.rs:158`:

> `/// Tracks keys of inserted rows for conflict detection.`
> `/// Only created when ON columns match the schema's unenforced primary key.`

### 2. Conflict detection compares the bloom filters at commit time

`rust/lance/src/io/commit/conflict_resolver.rs::check_update_txn` (line 328+):

```rust
match (self_inserted_rows_filter, other_inserted_rows_filter) {
    (Some(self_keys), Some(other_keys)) => {
        // ...
        let Ok((has_intersection, _maybe_false_positive)) =
            self_keys.intersects(other_keys)
        else { /* treat as conflict */ };
        if has_intersection {
            return Err(self.retryable_conflict_err(other_transaction, other_version));
        }
    }
    (Some(_), None) => {
        // Pessimistic: filter present on this side only -> conflict.
        return Err(self.retryable_conflict_err(other_transaction, other_version));
    }
    _ => {}
}
```

`RetryableCommitConflict` is consumed by the merge-insert retry loop (`rust/lance/src/dataset/write/retry.rs:97`); when retries exhaust it surfaces as `Error::TooMuchWriteContention`.

### 3. Without the filter — fragment overlap only

If the filter is `None` on one side (no PK annotation), `check_update_txn` falls through to fragment-overlap checks (line 382+). For two `Operation::Update` transactions where each writer's merge-insert emits a **new** fragment with the new row, neither side's `updated_fragments` / `removed_fragment_ids` overlaps the other's `modified_fragment_ids`. The conflict resolver returns `Ok` and **both commits land**.

This is exactly what merge_insert does on a not-matched insert: `rust/lance/src/dataset/write/merge_insert.rs:1574` builds `Operation::Update` with `inserted_rows_filter: None` (the v1 path) — though the more common v2/`FullSchemaMergeInsertExec` path also leaves the filter `None` unless `is_primary_key` flipped to true at plan time.

### 4. Today's `__manifest` schema does NOT have the annotation

`crates/omnigraph/src/db/manifest/state.rs:44-60`:

```rust
pub(super) fn manifest_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("object_id", DataType::Utf8, false),  // <-- no metadata
        // ...
    ]))
}
```

`crates/omnigraph/src/db/manifest/publisher.rs:286` joins on `object_id`. With no PK metadata, `is_primary_key` is `false` for every commit; `inserted_rows_filter` is always `None`; the fallback fragment-overlap check accepts the duplicate.

## Empirical confirmation

`.context/scratch/merge-insert-cas-repro/src/lib.rs` builds two tests against a `__manifest`-shaped schema, deliberately using `MergeInsertBuilder` with the same `on=["object_id"]`, both writers based on the same dataset version:

- `without_pk_annotation_concurrent_inserts_both_succeed` — passes. Test output: `[without_pk] duplicate rows after both commits: 2`.
- `with_pk_annotation_concurrent_inserts_second_fails` — passes. Test output: `[with_pk] second writer correctly rejected with TooMuchWriteContention`.

Run:
```sh
cd .context/scratch/merge-insert-cas-repro
cargo test --release -- --nocapture
```

## Recommended publisher implementation outline

1. In `manifest_schema()` (`crates/omnigraph/src/db/manifest/state.rs:44`): attach `lance-schema:unenforced-primary-key=true` metadata to `object_id`.
2. Extend `ManifestBatchPublisher::publish` (`crates/omnigraph/src/db/manifest/publisher.rs:42-44`) to take `expected_table_versions: HashMap<String, u64>` (empty = back-compat).
3. In `GraphNamespacePublisher::publish`, after `load_publish_state` (`publisher.rs:77`), reduce `existing_versions` to "latest non-tombstoned version per table" (mirror `state.rs:65-94`) and reject any expectation that doesn't match with a typed `manifest_conflict { table_key, expected, actual }`. Add the structured variant on `ManifestError` (`crates/omnigraph/src/error.rs:5-46`).
4. Switch `merge_builder.conflict_retries(5)` to `(0)` (`publisher.rs:290`) so a concurrent commit fails fast instead of silently rebasing past our pre-check.
5. Wrap the publish in a bounded retry loop at the publisher level: on `TooMuchWriteContention` or `CommitConflict`, refresh manifest, re-run pre-check, re-attempt — bounded retries (5).
6. Existing tests at `crates/omnigraph/src/db/manifest/tests.rs:680-728` are the right place to extend with stale-expected-version cases.

## Out of scope here

- Whether to apply the same PK annotation to `_graph_commits.lance` / `_graph_runs.lance` — separate review when those code paths land in scope.
- Schema-migration story for existing manifests written before the annotation lands. Adding field metadata is a non-breaking schema change in Lance, but worth confirming with one round of `optimize` semantics before deploying.
- Whether the fast-fail (`conflict_retries(0)` + caller-level retry) is preferable to Lance's built-in rebase. The argument here is correctness: Lance's rebase is "transparent merge", which is the wrong semantic for an OCC contract.
