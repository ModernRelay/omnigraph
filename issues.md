# Omnigraph Validated Issues

Validated 2026-03-29 against the live workspace and `cargo test --workspace` (`388 passed, 2 ignored`).

This document keeps only issues confirmed by code inspection or test-inventory review. Stale or unverified claims from the previous draft were removed.

---

## Critical

### C1. Public transactional load/mutate replays writes instead of publishing the staged run snapshot

**Files:** `crates/omnigraph/src/loader/mod.rs:52-100`, `crates/omnigraph/src/exec/mod.rs:2666-2745`

Both public entry points stage work on `run.run_branch`, then replay the same operation onto the target branch instead of promoting the staged snapshot. For ULID-backed inserts, the second write can produce different IDs from the staged run branch. The run record is then marked published against the target snapshot, not the staged snapshot that was actually validated for drift.

**Fix:** Reuse the staged run snapshot by calling the existing publish/promote path instead of replaying the operation.

---

## High

### H1. `typecheck_filter` silently accepts non-scalar comparisons

**File:** `crates/omnigraph-compiler/src/query/typecheck.rs:816-880`

Filter validation only runs inside the scalar/scalar match arm. If either side resolves to `Node` or `Aggregate`, the function falls through to `Ok(())`.

**Fix:** Return a type error when either side of a comparison is non-scalar.

### H2. Server handlers block inside async contexts with `block_in_place` + `block_on`

**File:** `crates/omnigraph-server/src/lib.rs`

Handlers are already `async fn`, but they acquire the DB lock and then call `tokio::task::block_in_place(|| Handle::current().block_on(...))`. This defeats cooperative scheduling and risks deadlock or poor latency under load.

**Fix:** Replace the wrappers with direct `.await`.

### H3. Server serializes all traffic behind a global `Mutex<Omnigraph>`

**File:** `crates/omnigraph-server/src/lib.rs:39-57`

`AppState` stores `Arc<Mutex<Omnigraph>>`, so read-only routes contend with writes and each other.

**Fix:** Use `RwLock` or open short-lived read snapshots per request.

### H4. `RuntimeCache` keeps an unbounded `GraphIndex` map

**File:** `crates/omnigraph/src/runtime_cache.rs:22-46`

`graph_indices` only shrinks on `invalidate_all()`. A long-lived process that touches many snapshots can accumulate old topologies indefinitely.

**Fix:** Add a bounded cache or eviction policy.

### H5. Manifest/commit/run/index readers panic on schema drift or corruption

**Files:** `crates/omnigraph/src/db/manifest.rs:404-456`, `crates/omnigraph/src/db/commit_graph.rs:194-258`, `crates/omnigraph/src/db/run_registry.rs:185-286`, `crates/omnigraph/src/graph_index/mod.rs:149-168`

These paths use `.unwrap()` on `column_by_name()` and `downcast_ref()` chains. Corrupted or migrated datasets panic instead of returning structured errors.

**Fix:** Replace unwrap chains with `ok_or_else(...)` and propagate `OmniError`.

### H6. Cross-type traversal can emit duplicate destination rows

**File:** `crates/omnigraph/src/exec/mod.rs:1631-1669`

For cross-type traversals, the `visited` set is intentionally bypassed (`if !same_type || visited.insert(neighbor)`), so diamond-shaped graphs can duplicate hydrated destinations.

**Fix:** Track visited destinations for cross-type traversals too.

---

## Medium

### M1. Commit and run lookup paths do full-table scans

**Files:** `crates/omnigraph/src/db/commit_graph.rs:180-258`, `crates/omnigraph/src/db/run_registry.rs:174-286`, `crates/omnigraph/src/db/graph_coordinator.rs:288-310`

`CommitGraph::load_commits()` and `RunRegistry::load_runs()` scan entire datasets. `resolve_commit()`, `head_commit()`, `get_run()`, and `list_runs()` build on top of those scans.

**Fix:** Add filter pushdown or a narrower lookup structure for commit IDs and run IDs.

### M2. `parse_string_lit` strips quotes but does not decode escapes

**Files:** `crates/omnigraph-compiler/src/query/parser.rs:707-712`, `crates/omnigraph-compiler/src/schema/parser.rs:301`

Escapes like `\"`, `\\`, `\n`, and `\t` survive as literal backslash sequences.

**Fix:** Implement standard string escape decoding.

### M3. Edge-name case-fold collisions silently overwrite lookup entries

**File:** `crates/omnigraph-compiler/src/catalog/mod.rs:288-291`

`edge_name_index` inserts `lowercase_first_char(&edge.name)` without collision checks.

**Fix:** Reject ambiguous folded names during catalog construction.

### M4. `lower.rs` unwraps edge catalog lookup

**File:** `crates/omnigraph-compiler/src/ir/lower.rs:203`

`catalog.lookup_edge_by_name(...).unwrap()` is a production panic site if the lowerer is ever invoked with divergent state.

**Fix:** Convert the unwrap to an internal error.

### M5. Server accepts unbounded request bodies

**File:** `crates/omnigraph-server/src/lib.rs:165-176`

`build_app()` does not install `DefaultBodyLimit`, so `/read` and `/change` accept arbitrarily large JSON bodies.

**Fix:** Add a bounded body limit appropriate for expected query sizes.

### M6. Change signatures include Lance `_row_*` version columns

**File:** `crates/omnigraph/src/changes/mod.rs:531-567`

`extract_rows_with_signature()` concatenates all column values into the signature, including `_row_created_at_version` and `_row_last_updated_at_version`. Cross-branch comparisons can therefore report updates even when user-visible data is unchanged.

**Fix:** Exclude engine-managed `_row_*` columns from signature generation.

### M7. Nullable vectors are stored as zero vectors rather than nulls

**File:** `crates/omnigraph/src/loader/mod.rs:547-572`

Missing nullable vectors extend `flat_values` with zeros and build the `FixedSizeListArray` with `None` for the null bitmap.

**Fix:** Construct a null bitmap for absent nullable vectors.

### M8. `resolve_query_path` has CWD-dependent behavior

**File:** `crates/omnigraph-server/src/config.rs:177-191`

`query.exists()` is checked before the config-relative lookup, so the same config can resolve differently depending on process working directory.

**Fix:** Resolve relative paths against `base_dir` first.

### M9. `list_branches` can duplicate `main`

**File:** `crates/omnigraph/src/db/manifest.rs:384-393`

The function prepends `"main"` and then appends all Lance branch names without deduplication.

**Fix:** Filter or deduplicate `main`.

### M10. Server error classification relies on message substring matching

**File:** `crates/omnigraph-server/src/lib.rs:131-143`

HTTP status mapping for manifest errors is driven by string fragments such as `"version drift"` and `"retry"`.

**Fix:** Use structured error kinds rather than message text.

### M11. CLI creates a new `reqwest::Client` per remote request

**File:** `crates/omnigraph-cli/src/main.rs:347-359`

Each remote call rebuilds the client and connection pool.

**Fix:** Reuse a single client for the process.

### M12. CLI column discovery loses query projection order

**File:** `crates/omnigraph-cli/src/read_format.rs:199-206`

`columns()` sorts keys alphabetically instead of preserving the order implied by the query projection.

**Fix:** Preserve first-seen order rather than sorting.

---

## Low

### L1. `_in_negation` parameter is unused

**File:** `crates/omnigraph-compiler/src/query/typecheck.rs:520`

The parameter is dead code and suggests an unfinished validation hook.

### L2. `bool_lit` parsing relies entirely on grammar correctness

**File:** `crates/omnigraph-compiler/src/query/parser.rs:669-672`

Any non-`"true"` token becomes `false`.

### L3. NaN/Infinity serialize as JSON `null`

**File:** `crates/omnigraph-compiler/src/json_output.rs:118-128`

`serde_json::Number::from_f64` returns `None` for NaN/Inf and the code falls back to `Value::Null`.

### L4. `SourceSpan::new` widens zero-width spans

**File:** `crates/omnigraph-compiler/src/error.rs:9-13`

Zero-width spans are normalized to width 1.

### L5. Edge lookup only folds the first character

**File:** `crates/omnigraph-compiler/src/catalog/mod.rs:92-106`

`"Knows"` and `"knows"` match, but `"KNOWS"` does not.

### L6. `StorageAdapter` remains synchronous

**File:** `crates/omnigraph/src/storage.rs:7-11`

The current trait uses blocking filesystem-style methods, which will be awkward for future remote backends.

### L7. `csv_escape` does not quote fields containing `\r`

**File:** `crates/omnigraph-cli/src/read_format.rs:263-269`

This misses an RFC 4180 quoting case.

---

## Confirmed Test Gaps

- `crates/omnigraph/tests/fixtures/context.pg` and `context.jsonl` exist but are not referenced by tests.
- `friend_counts()` in `crates/omnigraph/tests/fixtures/test.gq` is present but not exercised.
- `Omnigraph::entity_at()` (version-pinned lookup) lacks a direct test; current coverage exercises `entity_at_target()` and `snapshot_at_version()` separately.
- `CommitGraph::merge_base()` has no direct test for complex DAG shapes.
- Empty JSONL input (`load_jsonl(..., "")`) has no dedicated regression test.
- `fuzzy_search_tolerates_typos()` only verifies that execution does not error.
- `bm25_returns_ranked_results()` and `rrf_fuses_vector_and_text()` assert broad success/limit behavior, not concrete ranking contracts.
