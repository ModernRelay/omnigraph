//! Keep the RFC-023 decision instrument aligned with production safety caps.
//! The pure planner lives beside the bench and is included here so the normal
//! test suite exercises its row, byte, and recovery-chain boundaries.

#[path = "../benches/scenarios/rfc023_limits.rs"]
mod rfc023_limits;

#[path = "../benches/scenarios/child_protocol.rs"]
mod child_protocol;

#[path = "helpers/request_delay.rs"]
mod request_delay;

#[tokio::test]
async fn request_delay_wraps_real_store_calls_and_survives_child_tasks() {
    use lance_io::utils::tracking_store::IOTracker;
    use object_store::{ObjectStore, ObjectStoreExt};
    use request_delay::{RequestDelay, with_request_delay};
    use std::sync::Arc;
    let delay = RequestDelay::default();
    let store: Arc<dyn ObjectStore> = with_request_delay(delay.clone(), async {
        request_delay::wrap_counter(Arc::new(IOTracker::default()))
            .wrap("memory", Arc::new(object_store::memory::InMemory::new()))
    })
    .await;
    let path = object_store::path::Path::from("fixture");
    store.put(&path, "payload".into()).await.unwrap();
    assert_eq!(delay.calls(), 0, "setup must not receive injection");
    {
        let _active = delay.activate(17);
        let child_store = store.clone();
        let child_path = path.clone();
        let child = tokio::spawn(async move { child_store.head(&child_path).await.unwrap() });
        let local = store.get(&path);
        let (head, get) = tokio::join!(child, local);
        assert_eq!(head.unwrap().size, 7);
        assert_eq!(get.unwrap().bytes().await.unwrap().as_ref(), b"payload");
        assert_eq!(
            delay.calls(),
            2,
            "each wrapped call must observe the retained controller"
        );
    }
    store.head(&path).await.unwrap();
    assert_eq!(delay.calls(), 2, "verification must not receive injection");
}

fn product_const(source: &str, name: &str) -> u64 {
    let marker = format!("const {name}:");
    let declaration = source
        .split(';')
        .find(|declaration| declaration.contains(&marker))
        .unwrap_or_else(|| panic!("production declaration for {name} is missing"));
    let expression = declaration
        .split_once('=')
        .unwrap_or_else(|| panic!("production declaration for {name} has no value"))
        .1;
    expression
        .split('*')
        .map(|factor| {
            factor
                .trim()
                .replace('_', "")
                .parse::<u64>()
                .unwrap_or_else(|_| {
                    panic!("production declaration for {name} is not a numeric product")
                })
        })
        .try_fold(1_u64, u64::checked_mul)
        .unwrap_or_else(|| panic!("production declaration for {name} overflows u64"))
}

/// The benchmark planner deliberately lives outside the engine crate, so its
/// private constants cannot be imported directly. Pin their source
/// declarations here: changing a production cap now fails the ordinary test
/// suite until the decision instrument is updated in the same change.
#[test]
fn benchmark_caps_match_production() {
    let storage = include_str!("../src/storage_layer.rs");
    assert_eq!(
        product_const(storage, "KEYED_WRITE_MAX_ROWS"),
        rfc023_limits::KEYED_WRITE_MAX_ROWS as u64
    );
    assert_eq!(
        product_const(storage, "KEYED_WRITE_MAX_BYTES"),
        rfc023_limits::KEYED_WRITE_MAX_BYTES
    );

    let recovery = include_str!("../src/db/manifest/recovery.rs");
    assert_eq!(
        product_const(recovery, "MAX_BRANCH_MERGE_DATA_TRANSACTIONS"),
        rfc023_limits::RECOVERY_MAX_TRANSACTIONS as u64
    );

    let merge = include_str!("../src/exec/merge.rs");
    assert_eq!(
        product_const(merge, "PURE_INSERT_HISTORY_MAX_VERSIONS"),
        rfc023_limits::PURE_INSERT_HISTORY_MAX_VERSIONS as u64
    );
}

/// Pin the phased measurement boundary in the ordinary test suite. Setup,
/// operation, and verification must be separate children; the compatibility
/// peak must be the operation child's `wait4` HWM; and neither measured arm may
/// perform final-state scans before the fresh verification child runs.
#[test]
fn adopt_comparator_is_phased_and_streams_only_the_operation_substitution() {
    let harness = include_str!("../benches/scenarios.rs");
    let controller = harness
        .split_once("fn run_phased_adopt_once")
        .expect("phased adopt controller")
        .1
        .split_once("/// Reap `pid` with `wait4`")
        .expect("phased adopt controller boundary")
        .0;
    assert!(controller.contains("phased_child_args(args, \"setup\""));
    assert!(controller.contains("phased_child_args(args, \"operation\""));
    assert!(controller.contains("phased_child_args(args, \"verify\""));
    assert!(controller.contains("phased_child_args(args, \"setup\", fixture_root, false)"));
    assert!(controller.contains("phased_child_args(args, \"operation\", fixture_root, true)"));
    assert!(controller.contains("phased_child_args(args, \"verify\", fixture_root, false)"));
    assert!(controller.contains("if setup.exit_status != 0"));
    assert!(controller.contains("if operation.exit_status != 0"));
    assert!(controller.contains("map_or(0, |verify| verify.exit_status)"));
    assert!(harness.contains("if !apply_cap"));
    assert!(harness.contains("child_args.memory_cap_mb = None"));
    assert!(controller.contains("\"peak_rss_bytes\": operation_peak_rss_bytes"));
    assert!(controller.contains("\"setup_peak_rss_bytes\""));
    assert!(controller.contains("\"controller_peak_rss_bytes\""));
    assert!(controller.contains("\"operation_peak_rss_bytes\""));
    assert!(controller.contains("\"verify_peak_rss_bytes\""));
    assert!(harness.contains(".args([\"rev-parse\", \"HEAD^{tree}\"]"));
    assert!(harness.contains("child_protocol::parse_child_records"));
    assert!(harness.contains("CHILD_PROTOCOL_EXIT_STATUS"));
    assert!(harness.contains("if aggregate_exit_status != 0"));
    assert!(harness.contains("std::process::exit(aggregate_exit_status as i32)"));
    assert!(harness.contains("else if run.exit_status == 78"));
    assert_eq!(
        harness.matches("println!(\"{record}\")").count(),
        1,
        "the parent must emit exactly one aggregate record per requested run"
    );

    let source = include_str!("../benches/scenarios/rfc023.rs");
    let setup = source
        .split_once("pub(super) async fn fenced_adopt_setup")
        .expect("setup phase")
        .1
        .split_once("/// Phase 2 baseline")
        .expect("setup phase boundary")
        .0;
    assert_eq!(
        setup.matches("Omnigraph::init(").count(),
        1,
        "the persisted fixture must have one common OmniGraph initializer"
    );

    let operation = source
        .split_once("pub(super) async fn fenced_adopt_operation")
        .expect("adopt operation phase")
        .1;
    let operation = operation
        .split_once("/// Phase 3:")
        .expect("operation phase boundary")
        .0;
    let fresh_open = operation
        .find("Omnigraph::open(uri)")
        .expect("fresh operation open");
    let pre_hwm = operation
        .find("operation_pre_peak_rss_bytes")
        .expect("pre-operation HWM");
    let selection = operation
        .find("if args.baseline")
        .expect("comparator selection");
    assert!(fresh_open < pre_hwm && pre_hwm < selection);
    assert!(operation.contains("operation_post_peak_rss_bytes"));
    assert!(
        !operation.contains("count_rows("),
        "measured operation phase must not scan final rows"
    );
    assert!(
        operation
            .find("source_head_builder(uri, &source_snapshot)")
            .unwrap()
            < pre_hwm,
        "source ref resolution must be common unmeasured preparation"
    );
    assert!(
        !operation[pre_hwm..].contains("snapshot_of("),
        "the measured operation must not read final graph state"
    );

    let baseline = source
        .split_once("async fn direct_lance_append_baseline")
        .expect("direct baseline")
        .1
        .split_once("/// Phase 2: measured bulk all-new operation")
        .expect("baseline function boundary")
        .0;
    assert!(baseline.contains("let source_table = source_builder"));
    assert!(
        !source.contains(".with_branch(\"adopt-source\","),
        "physical source opens must use captured native refs, not logical branch names"
    );
    assert_eq!(
        source
            .matches("source_head_builder(uri, &source_snapshot)")
            .count(),
        3
    );
    let source_opener = source
        .split_once("fn source_head_builder")
        .unwrap()
        .1
        .split_once("fn adopt_fixture_root")
        .unwrap()
        .0;
    assert!(source_opener.contains("entry.dataset_path"));
    assert!(source_opener.contains("entry.native_dataset_branch.as_deref()"));
    assert!(source_opener.contains("builder.with_branch(native_ref, None)"));
    assert!(baseline.contains(".with_session(main_table.session())"));
    assert!(baseline.contains(".filter(\"id LIKE 'adopt-new-%'\")"));
    assert!(baseline.contains(".execute_stream(source)"));
    assert!(baseline.contains("WriteMode::Append"));
    assert!(
        !baseline.contains("try_collect"),
        "the all-new delta must not be collected before direct Append"
    );
    assert!(!baseline.contains("count_rows("));
    assert!(!baseline.contains("snapshot_of("));

    assert!(setup.contains("setup_fingerprint"));
    assert!(setup.contains("setup_main_rows"));
    assert!(setup.contains("setup_source_rows"));
    assert!(setup.contains("setup_main_dataset_version"));
    assert!(setup.contains("setup_source_dataset_version"));

    let verify = source
        .split_once("pub(super) async fn fenced_adopt_verify")
        .expect("verify phase")
        .1
        .split_once("// general-merge-updates:")
        .expect("verify phase boundary")
        .0;
    assert!(verify.contains("Omnigraph::open(uri)"));
    assert!(verify.contains("manifest_visible_final_rows"));
    assert!(verify.contains("physical_main_rows"));
    assert!(verify.contains("physical_source_rows"));
    assert!(verify.contains("physical_main_content"));
    assert!(verify.contains("physical_source_content"));
    assert!(verify.contains("manifest_main_content"));
    assert!(verify.contains("manifest_source_content"));
    assert!(verify.contains("canonical_row_contract_sha256"));
    assert!(verify.contains("complete_domain"));
    assert!(verify.contains("base_domain"));
    assert!(verify.contains("VerificationTable::Snapshot(&manifest_main)"));
    assert!(verify.contains("VerificationTable::Snapshot(&manifest_source)"));
    assert!(
        !source.contains("open_graph_visible_dataset"),
        "graph-visible verification must scan the pinned SnapshotDataset directly"
    );
    assert!(
        !verify.contains("with_branch(\"adopt-source\", Some("),
        "raw Lance must not reconstruct a manifest-pinned branch version"
    );
    assert!(
        !verify.contains("count_rows("),
        "fresh verification must prove exact content, not only counts"
    );

    let exact_verifier = source
        .split_once("async fn verify_id_content")
        .expect("exact content verifier")
        .1
        .split_once("/// Phase 3:")
        .expect("exact content verifier boundary")
        .0;
    assert!(exact_verifier.contains(".project(&[\"id\", \"slug\", \"embedding\"])"));
    for limit in [
        "scanner.batch_size(scan_batch_rows)",
        "scanner.batch_size_bytes(scan_batch_bytes_target)",
    ] {
        assert_eq!(
            exact_verifier.matches(limit).count(),
            2,
            "both physical and manifest-pinned exact scans need row and byte limits"
        );
    }
    assert!(exact_verifier.contains("compact_oversized_verification_slice(batch)"));
    assert_eq!(
        verify
            .matches("source_plan.estimated_full_batch_bytes")
            .count(),
        4,
        "every exact view must use the conservative source-plan byte target"
    );
    assert!(
        !source.contains("scanner.strict_batch_size(true)"),
        "Lance 11 rejects strict row batching combined with a byte target"
    );
    assert!(exact_verifier.contains("duplicate ID in verified content"));
    assert!(exact_verifier.contains("verified ID domain has a missing slot"));
    assert!(exact_verifier.contains("verify_fixture_vector("));
    assert!(exact_verifier.contains("exact_seen_bitset_max_bytes"));
    assert!(source.contains("fenced_calls, source_plan.transaction_count as u64"));
    assert!(source.contains("let fenced_calls = probes.stage_fenced_insert_calls()"));
    assert!(source.contains("\"probe_stage_fenced_insert_calls\""));
    assert!(source.contains("\"probe_stage_fenced_insert_rows\""));
    assert!(source.contains("probes.stage_merge_insert_calls(),\n        0"));
    assert!(source.contains("\"source_scan_batch_plan\": source_plan.transaction_count"));
    assert!(source.contains("\"planned_transaction_count\": source_plan.transaction_count"));
    assert!(source.contains("\"observed_transaction_count\": fenced_calls"));
    assert!(source.contains("ordered_cursor_scan_calls, 0"));
    assert!(source.contains("\"probe_ordered_cursor_scan_calls\": ordered_cursor_scan_calls"));
    assert!(source.contains("strict_insert_preflight_calls, 0"));
    assert!(
        source.contains("\"probe_strict_insert_preflight_calls\": strict_insert_preflight_calls")
    );
    assert!(source.contains("\"operation_wall_us\": operation_wall_us"));
    assert!(source.contains("\"probe_validation_scan_batches\""));
    assert!(source.contains("\"probe_phase_us\": merge_phase_metrics(&probes)"));
    assert!(source.contains("\"keyed_stage_total\": probes.keyed_stage_total_us()"));
    assert!(source.contains("\"keyed_commit_total\": probes.keyed_commit_total_us()"));
    assert!(source.contains("\"probe_proven_insert_raw_batch_calls\""));
    assert!(source.contains("\"probe_proven_insert_raw_batch_max_bytes\""));
}

#[test]
fn child_record_protocol_rejects_missing_duplicate_malformed_and_non_object_evidence() {
    let valid = child_protocol::parse_child_records(
        "{\"memory_cap_status\":{}}\n{\"scenario_metrics\":{}}\n",
        0,
    );
    assert!(valid.protocol_error.is_none());
    assert_eq!(valid.records.len(), 2);

    for stdout in [
        "{\"memory_cap_status\":{}}\n",
        "{\"memory_cap_status\":{}}\n{\"scenario_metrics\":{}}\n{\"scenario_metrics\":{}}\n",
        "not-json\n{\"memory_cap_status\":{}}\n{\"scenario_metrics\":{}}\n",
        "{\"memory_cap_status\":null}\n{\"scenario_metrics\":{}}\n",
        "{\"memory_cap_status\":{}}\n{\"scenario_metrics\":null}\n",
    ] {
        assert!(
            child_protocol::parse_child_records(stdout, 0)
                .protocol_error
                .is_some(),
            "child protocol unexpectedly accepted {stdout:?}"
        );
    }

    let refusal = child_protocol::parse_child_records("{\"memory_cap_status\":{}}\n", 78);
    assert!(
        refusal.protocol_error.is_none(),
        "a refused child reports cap evidence but no scenario metrics"
    );
}

#[test]
fn general_update_reports_completed_classifiers_and_keeps_update_semantics() {
    let source = include_str!("../benches/scenarios/rfc023.rs");
    let operation = source
        .split_once("pub(super) async fn general_merge_operation")
        .unwrap()
        .1
        .split_once("async fn verify_fixture_row")
        .unwrap()
        .0;
    assert!(operation.contains("probes.completed_full_walk_classification_calls()"));
    assert!(operation.contains("probes.completed_lineage_classification_calls()"));
    assert!(operation.contains("full_walk_classifications + lineage_classifications > 0"));
    assert!(!operation.contains("ordered_cursor_scan_calls > 0"));
    assert!(operation.contains("MergeOutcome::Merged"));
    assert!(
        operation.contains(
            "probes.stage_merge_insert_rows() + probes.stage_known_present_update_rows()"
        )
    );
    assert!(operation.contains("\"classifier_route\": classifier_route"));
    assert!(operation.contains("args.delta_rows as u64"));
    assert!(operation.contains("args.delta_rows as u64 * args.tables as u64"));
    assert!(operation.contains("delay.activate(args.io_delay_ms)"));
    assert!(operation.contains("\"io_delay_calls\": delay.calls()"));
    assert!(!operation.contains("snapshot_of("));
    assert!(source.contains("pub(super) async fn general_merge_verify"));
    let wrapper = operation.find("helpers::cost::cost_harness").unwrap();
    let open = operation.find("Omnigraph::open(uri)").unwrap();
    let prewarm = operation
        .find("fixture_controls::prewarm(&db, args)")
        .unwrap();
    let measure = operation[prewarm..].find("helpers::cost::measure").unwrap() + prewarm;
    let timer = operation
        .find("let operation_start = Instant::now()")
        .unwrap();
    assert!(wrapper < open && open < prewarm && prewarm < measure && measure < timer);
    assert!(operation.contains("fixture_controls::io_metrics(\"open\", &open_io)"));
    assert!(operation.contains("operation_io_metrics(&io)"));
    assert!(operation.contains("\"operation_open_us\""));
    let setup = source
        .split_once("pub(super) async fn general_merge_setup")
        .unwrap()
        .1
        .split_once("pub(super) async fn general_merge_operation")
        .unwrap()
        .0;
    assert!(
        setup.find("age_fixture(&db, args)").unwrap()
            < setup
                .find("db.branch_create(GENERAL_MERGE_SOURCE_BRANCH)")
                .unwrap()
    );
    let verify = source
        .split_once("pub(super) async fn general_merge_verify")
        .unwrap()
        .1;
    assert!(
        verify
            .contains("args.rows <= 256 || args.history_commits > 0 || args.retired_branches > 0")
    );
    assert!(verify.contains("verify_general_all_rows(&table, args, true)"));
    assert!(verify.contains("verify_general_all_rows(&source_table, args, false)"));
    assert!(verify.contains("for index in 0..args.tables"));
    assert!(verify.contains("merge changed the source head or exact table pins"));
    for field in [
        "setup_table_count",
        "setup_total_main_rows",
        "setup_total_source_rows",
        "setup_tables",
    ] {
        assert!(
            setup.contains(field),
            "missing multi-table fixture receipt {field}"
        );
    }
    assert!(source.contains("scanner.batch_size(256)"));
    assert!(source.contains("duplicate aged fixture ID"));
    assert!(source.contains("aged fixture row missing"));
}

#[test]
fn branch_controls_reuse_phased_isolation_and_verify_exact_branch_views() {
    let harness = include_str!("../benches/scenarios.rs");
    let source = include_str!("../benches/scenarios/branch_control.rs");
    assert!(harness.contains("branch_control::is_scenario(&args.scenario)"));
    for phase in ["setup", "operation", "verify"] {
        assert!(harness.contains(&format!("branch_control::{phase}(args).await")));
    }
    for argument in [
        "--branches",
        "--tables",
        "--history-commits",
        "--retired-branches",
        "--cache-state",
        "--manifest-layout",
    ] {
        assert!(
            harness.matches(argument).count() >= 3,
            "workload dimensions must parse, propagate, and be documented"
        );
    }
    assert!(source.contains("args.branches == 0 || args.tables == 0 || args.runs == 0"));
    assert!(source.contains("if args.baseline"));
    let operation = source
        .split_once("pub(super) async fn operation")
        .unwrap()
        .1
        .split_once("pub(super) async fn verify")
        .unwrap()
        .0;
    let timer = operation.find("let started = Instant::now()").unwrap();
    assert!(operation.find("Omnigraph::open(").unwrap() < timer);
    for call in [
        "db.branch_create(TARGET)",
        "db.branch_create_from(",
        "db.branch_list()",
        "db.branch_delete(TARGET)",
    ] {
        assert!(operation.find(call).unwrap() > timer);
    }
    assert!(!operation.contains("branch_view("));
    assert!(!operation.contains("snapshot_of("));
    assert!(operation.contains("\"completed_operations\": 1"));
    assert!(operation.contains("\"rss_boundary\""));
    let acknowledgement = operation.find("let operation_wall_us").unwrap();
    let reclaim_join = operation.find("db.wait_for_fork_reclaims().await").unwrap();
    let completion = operation.find("let operation_complete_wall_us").unwrap();
    assert!(acknowledgement < reclaim_join && reclaim_join < completion);
    assert!(operation.contains("\"post_ack_reclaim_wait_us\""));
    assert!(operation.contains("\"operation_complete_wall_us\""));
    let prewarm = operation
        .find("fixture_controls::prewarm(&db, args)")
        .unwrap();
    let first_read = operation
        .find("fixture_controls::first_read(&db, TARGET, args.tables)")
        .unwrap();
    assert!(prewarm < timer && completion < first_read);
    assert!(operation.contains("fixture_controls::io_metrics(\"open\", &open_io)"));
    assert!(
        operation.find("std::fs::write(").unwrap()
            > operation.find("let operation_wall_us").unwrap()
    );
    assert!(source.contains("created.tables, fixture.branches[parent].tables"));
    assert!(source.contains("!refs.contains_key(native_ref)"));
    assert!(source.contains("assert_eq!(verified_reclaimed_table_refs, args.tables)"));
    let compact = source
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    assert!(compact.contains("entry.native_ref.as_deref()"));
    assert!(compact.contains("dataset.list_branches().await"));
    assert!(compact.contains("created.effective_head,fixture.branches[parent].effective_head"));
    assert!(compact.contains(".resolve_snapshot(branch)"));
    assert!(compact.contains("assert_eq!(listed,names,"));
    assert!(compact.contains("assert_ne!(source.tables,branches[\"main\"].tables,"));
    assert!(source.contains("\"verification_passed\": true"));
    assert!(
        operation.find("helpers::cost::cost_harness").unwrap()
            < operation.find("Omnigraph::open(").unwrap()
    );
    assert!(operation.find("helpers::cost::measure").unwrap() < timer);
    assert!(operation.find("operation_io_metrics(&io)").unwrap() > completion);
    let setup = source
        .split_once("pub(super) async fn setup")
        .unwrap()
        .1
        .split_once("pub(super) async fn operation")
        .unwrap()
        .0;
    assert!(
        setup.find("age_fixture(&db, args)").unwrap()
            < setup.find("for branch in 0..args.branches").unwrap()
    );
    assert!(harness.contains("rfc023_scenarios::validate_fixture_age(&args)"));
    for cache in ["cold", "warm"] {
        for layout in ["uncompacted", "compacted"] {
            rfc023_limits::validate_view_controls(cache, layout).unwrap();
        }
    }
    for (cache, layout) in [
        ("hot", "compacted"),
        ("cold", "optimized"),
        ("", "uncompacted"),
    ] {
        assert!(rfc023_limits::validate_view_controls(cache, layout).is_err());
    }
    let aging = include_str!("../benches/scenarios/rfc023.rs");
    assert!(aging.contains("args.age_options_supplied && !supported"));
    assert!(aging.contains("args.history_commits > 256"));
    assert!(aging.contains("!args.history_commits.is_multiple_of(2)"));
    assert!(aging.contains("args.retired_branches > 32"));
    assert!(
        aging.contains("args.rows > 256 || args.dims > 16 || args.branches > 8 || args.tables > 8")
    );
    let age = aging
        .split_once("pub(super) async fn age_fixture")
        .unwrap()
        .1
        .split_once("pub(super) fn operation_io_metrics")
        .unwrap()
        .0;
    assert!(age.contains("after.checked_sub(before)"));
    assert!(age.contains("Some(args.history_commits)"));
    assert!(age.contains("verify_fixture_row(&table, \"base\", 0, args.dims, args.seed)"));
    assert!(age.contains("db.wait_for_fork_reclaims().await"));
    assert!(age.contains("retired native fork was not reclaimed"));
    assert!(age.contains("retirement must not publish on main"));
    assert!(!age.contains("Dataset::write"));
    for field in [
        "setup_history_commits_applied",
        "setup_main_history_after_age",
        "setup_retired_branches_applied",
        "setup_age_content_verified",
        "operation_io_manifest_reads",
        "operation_io_data_reads",
        "operation_io_boundary",
    ] {
        assert!(aging.contains(field), "missing age/IO evidence {field}");
    }
    let controls = include_str!("../benches/scenarios/fixture_controls.rs");
    assert!(setup.contains("fixture_controls::prepare_layout(uri, args)"));
    assert!(controls.contains("compaction changed a retained manifest cell"));
    assert!(controls.contains("versions.is_subset(&retained)"));
    assert!(controls.contains("compaction changed branch history/head/table pins"));
    assert!(controls.contains("compaction changed native branch identity or registry"));
    assert!(controls.contains("compacted arm must perform physical work"));
    assert!(controls.contains("one payload row from every table"));
    assert!(!controls.contains("cleanup_old_versions("));
    assert!(!controls.contains(".optimize().await"));
}
