//! Cost-budget tests for native branch CONTROL operations (create/delete) on the
//! shared `helpers::cost` harness — the branch-op sibling of `write_cost.rs`
//! (mutations) and `merge_cost.rs` (merge).
//!
//! The gated term: `branch_delete` must prove no surviving branch still
//! references the deleted branch's per-table forks. That dependency check reads
//! ONLY each foreign branch's manifest `table_branch` entries, so its per-branch
//! cost budget is ONE manifest-only snapshot read (`__manifest` open + state
//! scan). Historically the check performed a full cold target resolve per
//! foreign branch — before coordinator opens decoded state and lineage from one
//! coherent scan, that was TWO `__manifest` opens/scans per branch, and it
//! always added a schema-contract re-read + full recompile + re-validation per
//! branch — making deletion O(branches × history) on un-compacted graphs. This
//! pin holds the slope at one manifest-only read per surviving branch so no
//! future change can reintroduce a second per-branch scan (lineage load,
//! per-branch resolve, or any other per-branch authority read).
//!
//! Like the other cost tests, the body runs on a 64 MiB-stack thread: the
//! debug-build engine futures plus the `cost_harness`/`measure` task-local
//! layers overflow the default 2 MiB test stack.
#![recursion_limit = "512"]

mod helpers;

use std::future::Future;

use helpers::cost::{IoCounts, cost_harness, local_graph, measure};

/// Run an async test body on a thread with a large stack (see module docs).
fn on_big_stack<F>(body: impl FnOnce() -> F + Send + 'static)
where
    F: Future<Output = ()>,
{
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(body());
        })
        .unwrap()
        .join()
        .unwrap();
}

/// Per-foreign-branch budget for the delete dependency check: ONE manifest-only
/// snapshot (one `__manifest` open + state scan) per surviving branch. On this
/// fixture that is exactly 1 read iop and 1 internal open per branch; the
/// historical two-scan cold resolve measured exactly 2 of each, so any change
/// that reintroduces a second per-branch `__manifest` read re-trips this gate.
/// `SLOPE_SLACK` absorbs incidental constant-ish noise without admitting a
/// second per-branch open.
const DELETE_PER_BRANCH_BUDGET: u64 = 1;
const SLOPE_SLACK: u64 = 2;

/// `branch_delete`'s dependency check visits every surviving branch, so its
/// `__manifest` reads necessarily scale with branch count — this gate bounds the
/// SLOPE (reads per surviving branch), not the total. Sibling branches (created
/// straight off main, never written) keep every other delete precondition
/// identical across the sweep: no path-prefix children, no lineage descendants,
/// and constant `__manifest` content (native branch create/delete emits no
/// lineage rows), so the fixed per-delete overhead cancels in the delta and the
/// slope isolates the per-branch dependency-check cost.
#[test]
fn branch_delete_manifest_reads_bounded_per_surviving_branch() {
    on_big_stack(|| {
        cost_harness(async {
            let dir = tempfile::tempdir().unwrap();
            let db = local_graph(&dir).await;

            let mut curve: Vec<(u64, IoCounts)> = Vec::new();
            let mut created = 0u64;
            for n in [4u64, 12] {
                while created < n {
                    db.branch_create(&format!("sib_{created:02}"))
                        .await
                        .unwrap();
                    created += 1;
                }
                let victim = format!("victim_{n}");
                db.branch_create(&victim).await.unwrap();
                let (res, io) = measure(db.branch_delete(&victim)).await;
                res.unwrap();
                eprintln!(
                    "surviving branches={} (+main): DELETE manifest_reads={} data_reads={} \
                     internal_open_count={}",
                    n, io.manifest_reads, io.data_reads, io.internal_open_count
                );
                curve.push((n, io));
            }

            let (n_lo, n_hi) = (curve[0].0, curve[1].0);
            let added_branches = n_hi - n_lo;
            let budget = added_branches * DELETE_PER_BRANCH_BUDGET + SLOPE_SLACK;
            let read_delta = curve[1]
                .1
                .manifest_reads
                .saturating_sub(curve[0].1.manifest_reads);
            let open_delta = curve[1]
                .1
                .internal_open_count
                .saturating_sub(curve[0].1.internal_open_count);
            assert!(
                read_delta <= budget && open_delta <= budget,
                "branch_delete __manifest cost grew {read_delta} reads / {open_delta} opens \
                 across {added_branches} added surviving branches (budget {budget}) — the \
                 dependency check must read one manifest-only snapshot per surviving branch, \
                 not a full cold resolve (state + lineage scans + schema contract) per branch",
            );
        })
    });
}

// Append this module to the existing branch_control_cost.rs on BOTH sources.
// Deliberately ignored: these are debug-profile diagnostic timings, not CI gates.
#[cfg(test)]
mod topology_diagnostics {
    use super::{IoCounts, cost_harness, helpers, local_graph, measure, on_big_stack};
    use futures::TryStreamExt;
    use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget};
    use serde_json::{Value, json};
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    const HISTORY: usize = 16;
    const REPETITIONS: usize = 3;
    type Row = BTreeMap<String, Option<String>>;
    type Pin = (String, Option<String>, u64, u64);

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TableView {
        pin: Pin,
        rows: Vec<Row>,
    }
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct BranchView {
        effective_head: String,
        history: Vec<String>,
        tables: BTreeMap<String, TableView>,
    }
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct NativeView {
        main_version: u64,
        // Exact native name -> (serialized registry entry, physical HEAD).
        refs: BTreeMap<String, (String, u64)>,
    }
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct GraphView {
        branches: BTreeMap<String, BranchView>,
        native: BTreeMap<String, NativeView>,
    }

    fn micros(start: Instant) -> u64 {
        u64::try_from(start.elapsed().as_micros()).unwrap()
    }

    fn io_json(io: IoCounts) -> Value {
        json!({
            "manifest_reads": io.manifest_reads,
            "manifest_read_bytes": io.manifest_read_bytes,
            "manifest_scans": io.manifest_scan_count,
            "internal_opens": io.internal_open_count,
            "version_probes": io.version_probes,
            "data_reads": io.data_reads,
            "data_writes": io.data_writes,
            "data_opens": io.data_open_count,
        })
    }

    // Keep the public query AST and the shared scalar fixture. Every aging write
    // changes one existing value; no insert, schema change or table-count growth.
    async fn set_age(db: &Omnigraph, branch: &str, name: &str, age: i64) {
        db.mutate(
            branch,
            helpers::MUTATION_QUERIES,
            "set_age",
            &helpers::mixed_params(&[("$name", name)], &[("$age", age)]),
        )
        .await
        .unwrap();
    }

    fn expected_age(tables: &mut BTreeMap<String, TableView>, name: &str, age: i64) {
        let person = tables.get_mut("node:Person").unwrap();
        let mut matches = 0;
        for row in &mut person.rows {
            if row.get("name").and_then(Option::as_deref) == Some(name) {
                assert!(row.contains_key("age"));
                row.insert("age".into(), Some(age.to_string()));
                matches += 1;
            }
        }
        assert_eq!(matches, 1, "fixture update must hit exactly one person");
        person.rows.sort();
    }

    fn assert_contents(actual: &BranchView, expected: &BTreeMap<String, TableView>) {
        assert_eq!(
            actual.tables.keys().collect::<Vec<_>>(),
            expected.keys().collect::<Vec<_>>()
        );
        for (key, table) in &actual.tables {
            assert_eq!(
                table.rows, expected[key].rows,
                "complete logical rows differ in {key}"
            );
            assert_eq!(table.pin.3, expected[key].pin.3);
        }
    }

    // Independent fresh verifier: checking the fixture never primes either
    // operation handle's retained coordinator/cache. All reads are outside timers.
    async fn capture(uri: &str) -> GraphView {
        let db = Omnigraph::open(uri).await.unwrap();
        let mut names = db.branch_list().await.unwrap();
        names.sort();
        assert_eq!(names.len(), names.iter().collect::<BTreeSet<_>>().len());
        let mut branches = BTreeMap::new();
        let mut paths = BTreeSet::from([format!("{uri}/__manifest")]);
        for branch in names {
            let snapshot = db.snapshot_of(ReadTarget::branch(&branch)).await.unwrap();
            let mut tables = BTreeMap::new();
            for entry in snapshot.datasets() {
                paths.insert(format!("{uri}/{}", entry.dataset_path));
                let table = snapshot.open_dataset(&entry.type_key).await.unwrap();
                let mut scanner = table.scan();
                scanner.batch_size(16);
                let mut stream = scanner.try_into_stream().await.unwrap();
                let mut rows = Vec::new();
                while let Some(batch) = stream.try_next().await.unwrap() {
                    assert!(rows.len() + batch.num_rows() <= 16, "not a tiny fixture");
                    for row_index in 0..batch.num_rows() {
                        let mut row = BTreeMap::new();
                        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
                            // Exclude only the two physical version values whose
                            // changes are verified through exact table pins. Keep
                            // every other column, including any underscore name.
                            if matches!(
                                field.name().as_str(),
                                "_row_created_at_version" | "_row_last_updated_at_version"
                            ) {
                                continue;
                            }
                            let value = if column.is_null(row_index) {
                                None
                            } else {
                                Some(
                                    arrow_cast::display::array_value_to_string(
                                        column.as_ref(),
                                        row_index,
                                    )
                                    .unwrap(),
                                )
                            };
                            row.insert(field.name().clone(), value);
                        }
                        assert!(row.get("id").and_then(Option::as_deref).is_some());
                        if entry.type_key.starts_with("edge:") {
                            assert!(row.get("src").and_then(Option::as_deref).is_some());
                            assert!(row.get("dst").and_then(Option::as_deref).is_some());
                        }
                        rows.push(row);
                    }
                }
                rows.sort();
                assert_eq!(rows.len() as u64, entry.entity_count);
                assert!(!rows.is_empty());
                tables.insert(
                    entry.type_key.clone(),
                    TableView {
                        pin: (
                            entry.dataset_path.clone(),
                            entry.native_dataset_branch.clone(),
                            entry.published_dataset_version,
                            entry.entity_count,
                        ),
                        rows,
                    },
                );
            }
            assert_eq!(tables.len(), 4);
            assert_eq!(
                tables.values().map(|table| table.rows.len()).sum::<usize>(),
                11
            );
            let mut history =
                db.list_commits((branch != "main").then_some(branch.as_str()))
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|c| {
                        json!({
                    "id": c.graph_commit_id, "branch": c.graph_branch,
                    "manifest_version": c.graph_manifest_version, "parent": c.parent_commit_id,
                    "merged_parent": c.merged_parent_commit_id, "actor": c.actor_id,
                    "created_at": c.created_at,
                }).to_string()
                    })
                    .collect::<Vec<_>>();
            history.sort();
            let effective_head = db
                .resolve_snapshot(&branch)
                .await
                .unwrap()
                .as_str()
                .to_string();
            branches.insert(
                branch,
                BranchView {
                    effective_head,
                    history,
                    tables,
                },
            );
        }
        let mut native = BTreeMap::new();
        for path in paths {
            let dataset = helpers::open_dataset_head(&path, None).await;
            let mut refs = BTreeMap::new();
            for (name, metadata) in dataset.list_branches().await.unwrap() {
                let head = dataset.checkout_branch(&name).await.unwrap();
                refs.insert(
                    name,
                    (
                        serde_json::to_value(metadata).unwrap().to_string(),
                        head.version().version,
                    ),
                );
            }
            native.insert(
                path,
                NativeView {
                    main_version: dataset.version().version,
                    refs,
                },
            );
        }
        GraphView { branches, native }
    }

    async fn aged_graph(dir: &tempfile::TempDir) -> (Omnigraph, GraphView, usize) {
        let db = local_graph(dir).await;
        let original = capture(db.uri()).await;
        let count_before = db.list_commits(None).await.unwrap().len();
        for index in 0..HISTORY {
            set_age(&db, "main", "Alice", if index % 2 == 0 { 31 } else { 30 }).await;
        }
        let count_after = db.list_commits(None).await.unwrap().len();
        assert_eq!(count_after - count_before, HISTORY);
        let aged = capture(db.uri()).await;
        assert_contents(&aged.branches["main"], &original.branches["main"].tables);
        assert_eq!(aged.branches.len(), 1);
        assert!(aged.native.values().all(|state| state.refs.is_empty()));
        (db, aged, count_after)
    }

    fn assert_existing_native_unchanged(before: &GraphView, after: &GraphView) {
        assert_eq!(
            before.native.keys().collect::<Vec<_>>(),
            after.native.keys().collect::<Vec<_>>()
        );
        for (path, state) in &before.native {
            assert_eq!(state.main_version, after.native[path].main_version);
            for (name, entry) in &state.refs {
                assert_eq!(Some(entry), after.native[path].refs.get(name));
            }
        }
    }

    fn assert_fork(before: &GraphView, after: &GraphView, parent: &str, child: &str, uri: &str) {
        assert_eq!(after.branches.len(), before.branches.len() + 1);
        for (name, view) in &before.branches {
            assert_eq!(Some(view), after.branches.get(name));
        }
        let inherited = &after.branches[child];
        assert_eq!(inherited.tables, before.branches[parent].tables);
        assert_eq!(
            inherited.effective_head,
            before.branches[parent].effective_head
        );
        assert_existing_native_unchanged(before, after);
        for (path, state) in &before.native {
            assert_eq!(
                after.native[path].refs.len(),
                state.refs.len() + usize::from(path == &format!("{uri}/__manifest"))
            );
        }
    }

    async fn timed_fork(db: &Omnigraph, source: &str, child: &str) -> Value {
        let ((result, elapsed), io) = measure(async {
            let start = Instant::now();
            let result = db
                .branch_create_from(ReadTarget::branch(source), child)
                .await;
            (result, micros(start))
        })
        .await;
        result.unwrap();
        json!({"operation": "leaf_create_from", "wall_us": elapsed, "io": io_json(io)})
    }

    async fn timed_delete(db: &Omnigraph, branch: &str) -> Value {
        let ((result, acknowledgement, operation_start), io) = measure(async {
            let start = Instant::now();
            let result = db.branch_delete(branch).await;
            (result, micros(start), start)
        })
        .await;
        result.unwrap();
        let wait = Instant::now();
        db.wait_for_fork_reclaims().await;
        let wait_us = micros(wait);
        let completion_us = micros(operation_start);
        json!({"operation": "leaf_delete", "wall_us": acknowledgement,
               "post_ack_reclaim_wait_us": wait_us, "complete_wall_us": completion_us,
               "io": io_json(io), "io_boundary": "foreground acknowledgement only; reclaim is timed separately"})
    }

    fn emit(
        kind: &str,
        variant: Value,
        repetition: usize,
        history_count: usize,
        operations: Vec<Value>,
    ) {
        // Emission happens only after complete logical/native verification.
        println!(
            "TOPOLOGY_DIAGNOSTIC_JSON {}",
            json!({
                "schema": "branch-topology-diagnostic-v1", "case": kind, "variant": variant,
                "repetition": repetition, "history_commits_applied": HISTORY,
                "main_history_count_after_age": history_count, "logical_rows_per_branch": 11,
                "populated_tables": 4, "verified": true, "claim_eligible": false,
                "debug_assertions_enabled": cfg!(debug_assertions),
            "runtime_boundary": "existing on_big_stack current-thread Tokio runtime; join permits two in-flight futures, not two CPU workers",
            "profile_boundary": "supplemental test binary; debug timings must not be combined with release scenario matrix",
                "cache_boundary": "fresh fixture and operation handles; fixture verification uses separate handles; later operations in each fixture reuse the operation handle",
                "operations": operations,
            })
        );
    }

    #[test]
    #[ignore = "manual tiny H16 topology diagnostic; record both source/binary receipts"]
    fn branch_topology_depth_diagnostic() {
        on_big_stack(|| {
            cost_harness(Box::pin(async {
                for depth in [1_usize, 2] {
                    for repetition in 0..REPETITIONS {
                        let dir = tempfile::tempdir().unwrap();
                        let (db, main_only, history_count) = aged_graph(&dir).await;
                        let uri = db.uri().to_string();
                        let mut parent = "main".to_string();
                        for level in 1..=depth {
                            let child = format!("level-{level}");
                            db.branch_create_from(ReadTarget::branch(&parent), &child)
                                .await
                                .unwrap();
                            set_age(&db, &child, "Alice", 30 + level as i64).await;
                            parent = child;
                        }
                        // Keep live width and number of setup branch writes
                        // constant. Only the parent relation/depth changes.
                        let padding_count = 2 - depth;
                        if padding_count == 1 {
                            db.branch_create("padding").await.unwrap();
                            set_age(&db, "padding", "Alice", 32).await;
                        }
                        let before = capture(&uri).await;
                        assert_eq!(before.branches.len(), 3);
                        let manifest =
                            helpers::open_dataset_head(&format!("{uri}/__manifest"), None).await;
                        let refs = manifest.list_branches().await.unwrap();
                        let mut previous_native = None;
                        if padding_count == 1 {
                            let mut expected = main_only.branches["main"].tables.clone();
                            expected_age(&mut expected, "Alice", 32);
                            assert_contents(&before.branches["padding"], &expected);
                            let padding_native = helpers::graph_native_ref(&uri, "padding").await;
                            assert!(refs[&padding_native].parent_branch.is_none());
                        }
                        for level in 1..=depth {
                            let name = format!("level-{level}");
                            let mut expected = main_only.branches["main"].tables.clone();
                            expected_age(&mut expected, "Alice", 30 + level as i64);
                            assert_contents(&before.branches[&name], &expected);
                            let native = helpers::graph_native_ref(&uri, &name).await;
                            assert_eq!(refs[&native].parent_branch, previous_native);
                            previous_native = Some(native);
                        }
                        let operation = Omnigraph::open(&uri).await.unwrap();
                        let fork = timed_fork(&operation, &parent, "leaf").await;
                        assert_fork(&before, &capture(&uri).await, &parent, "leaf", &uri);
                        // Own one leaf table so deletion exercises real native reclaim.
                        set_age(&db, "leaf", "Bob", 46).await;
                        let owned = capture(&uri).await;
                        let mut leaf_expected = before.branches[&parent].tables.clone();
                        expected_age(&mut leaf_expected, "Bob", 46);
                        assert_contents(&owned.branches["leaf"], &leaf_expected);
                        let leaf_ref = owned.branches["leaf"].tables["node:Person"]
                            .pin
                            .1
                            .as_ref()
                            .unwrap();
                        assert!(helpers::is_incarnation_of(leaf_ref, "leaf"));
                        let ((refusal, elapsed), io) = measure(async {
                            let start = Instant::now();
                            let refusal = operation.branch_delete(&parent).await;
                            (refusal, micros(start))
                        })
                        .await;
                        let error = refusal.unwrap_err();
                        assert!(
                            error.to_string().contains("still depends on it"),
                            "unexpected refusal: {error}"
                        );
                        assert_eq!(
                            capture(&uri).await,
                            owned,
                            "ancestor refusal changed authority/content/native state"
                        );
                        let refusal = json!({"operation": "ancestor_delete_refusal", "wall_us": elapsed,
                                         "expected_refusal": true, "io": io_json(io)});
                        let deleted = timed_delete(&operation, "leaf").await;
                        assert_eq!(
                            capture(&uri).await,
                            before,
                            "leaf reclamation must restore the exact surviving fixture"
                        );
                        emit(
                            "nested_depth",
                            json!({"parent_depth": depth, "leaf_depth": depth + 1, "padding_siblings": padding_count,
                                   "branches_before_leaf": before.branches.len(), "branches_with_leaf": 4,
                                   "setup_named_branch_writes": 2}),
                            repetition,
                            history_count,
                            vec![fork, refusal, deleted],
                        );
                    }
                }
            }))
        });
    }

    #[test]
    #[ignore = "manual tiny H16 modern/bare native-ref diagnostic; supported legacy naming only"]
    fn branch_topology_legacy_ref_diagnostic() {
        on_big_stack(|| {
            cost_harness(Box::pin(async {
                for bare in [false, true] {
                    for repetition in 0..REPETITIONS {
                        let dir = tempfile::tempdir().unwrap();
                        let (db, main_only, history_count) = aged_graph(&dir).await;
                        let uri = db.uri().to_string();
                        if bare {
                            // Existing branching.rs legacy fixture seam: forge only a
                            // bare graph manifest ref on an exclusively owned tempdir.
                            let mut manifest =
                                helpers::open_dataset_head(&format!("{uri}/__manifest"), None)
                                    .await;
                            let version = manifest.version().version;
                            manifest
                                .create_branch("source", version, None)
                                .await
                                .unwrap();
                        } else {
                            db.branch_create("source").await.unwrap();
                        }
                        let source_native = helpers::graph_native_ref(&uri, "source").await;
                        assert_eq!(source_native == "source", bare);
                        set_age(&db, "source", "Alice", 41).await;
                        let before = capture(&uri).await;
                        let mut expected = main_only.branches["main"].tables.clone();
                        expected_age(&mut expected, "Alice", 41);
                        assert_contents(&before.branches["source"], &expected);
                        let operation = Omnigraph::open(&uri).await.unwrap();
                        let fork = timed_fork(&operation, "source", "leaf").await;
                        let first_fork = capture(&uri).await;
                        assert_fork(&before, &first_fork, "source", "leaf", &uri);
                        // No source writes occur between these two captures.
                        // The same operation handle can reuse its exact source.
                        let mut repeated_fork =
                            timed_fork(&operation, "source", "leaf-repeat").await;
                        repeated_fork["operation"] = json!("repeat_source_create_from");
                        repeated_fork["cache_boundary"] = json!(
                            "same handle and unchanged exact source authority immediately after the first fork; independent verifier opens outside timing"
                        );
                        assert_fork(
                            &first_fork,
                            &capture(&uri).await,
                            "source",
                            "leaf-repeat",
                            &uri,
                        );
                        operation.branch_delete("leaf-repeat").await.unwrap();
                        operation.wait_for_fork_reclaims().await;
                        assert_eq!(
                            capture(&uri).await,
                            first_fork,
                            "untimed repeat-leaf cleanup must restore the first-fork state"
                        );
                        set_age(&db, "leaf", "Bob", 46).await;
                        let owned = capture(&uri).await;
                        let mut leaf_expected = before.branches["source"].tables.clone();
                        expected_age(&mut leaf_expected, "Bob", 46);
                        assert_contents(&owned.branches["leaf"], &leaf_expected);
                        assert!(helpers::is_incarnation_of(
                            owned.branches["leaf"].tables["node:Person"]
                                .pin
                                .1
                                .as_ref()
                                .unwrap(),
                            "leaf"
                        ));
                        let leaf_delete = timed_delete(&operation, "leaf").await;
                        assert_eq!(capture(&uri).await, before);
                        let mut source_delete = timed_delete(&operation, "source").await;
                        source_delete["operation"] = json!("source_delete");
                        assert_eq!(
                            capture(&uri).await,
                            main_only,
                            "modern/bare source cleanup changed main or left native refs"
                        );
                        emit(
                            "native_ref_style",
                            json!({"source_native_style": if bare { "legacy-bare" } else { "modern-incarnation" }}),
                            repetition,
                            history_count,
                            vec![fork, repeated_fork, leaf_delete, source_delete],
                        );
                    }
                }
            }))
        });
    }

    async fn timed_merge(
        db: &Omnigraph,
        source: &str,
        target: &str,
        active: &AtomicUsize,
        peak: &AtomicUsize,
    ) -> (MergeOutcome, u64) {
        let now_active = active.fetch_add(1, Ordering::SeqCst) + 1;
        peak.fetch_max(now_active, Ordering::SeqCst);
        let start = Instant::now();
        let result = db.branch_merge(source, target).await;
        let elapsed = micros(start);
        active.fetch_sub(1, Ordering::SeqCst);
        (result.unwrap(), elapsed)
    }

    #[test]
    #[ignore = "manual tiny H16 serial/two-in-flight merge diagnostic; aggregate I/O only"]
    fn branch_topology_contention_diagnostic() {
        on_big_stack(|| {
            cost_harness(Box::pin(async {
                for concurrent in [false, true] {
                    for repetition in 0..REPETITIONS {
                        let dir = tempfile::tempdir().unwrap();
                        let (db, _, history_count) = aged_graph(&dir).await;
                        let uri = db.uri().to_string();
                        for (source, target, source_age, target_age) in [
                            ("source-a", "target-a", 41, 42),
                            ("source-b", "target-b", 51, 52),
                        ] {
                            db.branch_create(source).await.unwrap();
                            db.branch_create(target).await.unwrap();
                            set_age(&db, source, "Alice", source_age).await;
                            set_age(&db, target, "Bob", target_age).await;
                        }
                        let before = capture(&uri).await;
                        assert_eq!(before.branches.len(), 5);
                        let a = Omnigraph::open(&uri).await.unwrap();
                        let b = Omnigraph::open(&uri).await.unwrap();
                        let active = AtomicUsize::new(0);
                        let peak = AtomicUsize::new(0);
                        // Exactly one aggregate measure: nested per-merge measure
                        // calls would reset/steal the same ambient manifest tracker.
                        let ((result_a, result_b, total), io) = measure(async {
                            let start = Instant::now();
                            let work_a = timed_merge(&a, "source-a", "target-a", &active, &peak);
                            let work_b = timed_merge(&b, "source-b", "target-b", &active, &peak);
                            let (result_a, result_b) = if concurrent {
                                tokio::join!(work_a, work_b)
                            } else {
                                (work_a.await, work_b.await)
                            };
                            (result_a, result_b, micros(start))
                        })
                        .await;
                        assert_eq!(result_a.0, MergeOutcome::Merged);
                        assert_eq!(result_b.0, MergeOutcome::Merged);
                        assert_eq!(active.load(Ordering::SeqCst), 0);
                        assert_eq!(
                            peak.load(Ordering::SeqCst),
                            if concurrent { 2 } else { 1 },
                            "requested overlap was not exercised"
                        );
                        let after = capture(&uri).await;
                        assert_eq!(
                            after.branches.keys().collect::<Vec<_>>(),
                            before.branches.keys().collect::<Vec<_>>()
                        );
                        for branch in ["main", "source-a", "source-b"] {
                            assert_eq!(after.branches[branch], before.branches[branch]);
                        }
                        let mut allowed_native_heads = BTreeSet::new();
                        for (source, target, age) in
                            [("source-a", "target-a", 41), ("source-b", "target-b", 51)]
                        {
                            let mut expected = before.branches[target].tables.clone();
                            expected_age(&mut expected, "Alice", age);
                            assert_contents(&after.branches[target], &expected);
                            assert_ne!(
                                after.branches[target].effective_head,
                                before.branches[target].effective_head
                            );
                            for key in before.branches[target].tables.keys() {
                                if key != "node:Person" {
                                    assert_eq!(
                                        after.branches[target].tables[key],
                                        before.branches[target].tables[key]
                                    );
                                }
                            }
                            let old = &before.branches[target].tables["node:Person"].pin;
                            let new = &after.branches[target].tables["node:Person"].pin;
                            assert_eq!((&new.0, &new.1, new.3), (&old.0, &old.1, old.3));
                            assert!(new.2 > old.2);
                            allowed_native_heads
                                .insert((format!("{uri}/{}", old.0), old.1.clone().unwrap()));
                            allowed_native_heads.insert((
                                format!("{uri}/__manifest"),
                                helpers::graph_native_ref(&uri, target).await,
                            ));
                            let head = after.branches[target]
                                .history
                                .iter()
                                .map(|r| serde_json::from_str::<Value>(r).unwrap())
                                .find(|r| {
                                    r["id"].as_str()
                                        == Some(after.branches[target].effective_head.as_str())
                                })
                                .unwrap();
                            assert_eq!(
                                head["parent"].as_str(),
                                Some(before.branches[target].effective_head.as_str())
                            );
                            assert_eq!(
                                head["merged_parent"].as_str(),
                                Some(before.branches[source].effective_head.as_str())
                            );
                        }
                        assert_eq!(
                            after.native.keys().collect::<Vec<_>>(),
                            before.native.keys().collect::<Vec<_>>()
                        );
                        for (path, old) in &before.native {
                            let new = &after.native[path];
                            assert_eq!(new.main_version, old.main_version);
                            assert_eq!(
                                new.refs.keys().collect::<Vec<_>>(),
                                old.refs.keys().collect::<Vec<_>>()
                            );
                            for (name, (metadata, version)) in &old.refs {
                                assert_eq!(&new.refs[name].0, metadata);
                                if allowed_native_heads.contains(&(path.clone(), name.clone())) {
                                    assert!(new.refs[name].1 > *version);
                                } else {
                                    assert_eq!(new.refs[name].1, *version);
                                }
                            }
                        }
                        emit(
                            "distinct_target_merges",
                            json!({"schedule": if concurrent { "two-in-flight" } else { "serial" }}),
                            repetition,
                            history_count,
                            vec![
                                json!({"operation": "merge_pair", "first_merge_wall_us": result_a.1,
                                     "second_merge_wall_us": result_b.1, "pair_wall_us": total,
                                     "peak_in_flight": peak.load(Ordering::SeqCst), "completed_merges": 2,
                                     "aggregate_io": io_json(io),
                                     "latency_boundary": "each timer starts when its future is first polled; includes scheduling/gate wait; pair includes both completions",
                                     "io_boundary": "one shared foreground meter across tokio::join!, no per-merge attribution or spawned-task I/O claim"}),
                            ],
                        );
                    }
                }
            }))
        });
    }
}
