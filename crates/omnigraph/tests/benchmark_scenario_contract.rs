//! Keep the RFC-023 decision instrument aligned with production safety caps.
//! The pure planner lives beside the bench and is included here so the normal
//! test suite exercises its row, byte, and recovery-chain boundaries.

#[path = "../benches/scenarios/rfc023_limits.rs"]
mod rfc023_limits;

#[path = "../benches/scenarios/child_protocol.rs"]
mod child_protocol;

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
    assert_eq!(
        source.matches("Omnigraph::init(").count(),
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
        !operation.contains("snapshot_of("),
        "measured operation phase must not scan graph-visible final state"
    );

    let baseline = source
        .split_once("async fn direct_lance_append_baseline")
        .expect("direct baseline")
        .1
        .split_once("/// Phase 2: measured bulk all-new operation")
        .expect("baseline function boundary")
        .0;
    assert!(baseline.contains(".with_branch(\"adopt-source\", None)"));
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

    let setup = source
        .split_once("pub(super) async fn fenced_adopt_setup")
        .expect("setup phase")
        .1
        .split_once("/// Phase 2 baseline")
        .expect("setup phase boundary")
        .0;
    assert!(setup.contains("setup_fingerprint"));
    assert!(setup.contains("setup_main_rows"));
    assert!(setup.contains("setup_source_rows"));
    assert!(setup.contains("setup_main_table_version"));
    assert!(setup.contains("setup_source_table_version"));

    let verify = source
        .split_once("pub(super) async fn fenced_adopt_verify")
        .expect("verify phase")
        .1;
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
        "graph-visible verification must scan the pinned SnapshotTable directly"
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
    assert!(exact_verifier.contains("scanner.batch_size(scan_batch_rows)"));
    assert!(exact_verifier.contains("scanner.batch_size_bytes(scan_batch_bytes_target)"));
    assert!(exact_verifier.contains("compact_oversized_verification_slice(batch)"));
    assert_eq!(
        verify
            .matches("source_plan.estimated_full_batch_bytes")
            .count(),
        4,
        "every exact view must use the conservative source-plan byte target"
    );
    assert_eq!(
        exact_verifier
            .matches("scanner.strict_batch_size(true)")
            .count(),
        2,
        "both physical and manifest-pinned exact scans need a hard output-row ceiling"
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
