//! The public catalog and the measured adapter share these owning-layer tests.
use crate::gqt_case::*;
use crate::gqt_runner::{execute_gqt_rep_signaled, validate_sample};
use crate::runner::{MeasurementSignals, RunnerResult};
use omnigraph_gqt_core::StepKind;
use std::path::{Path, PathBuf};

const END_TO_END: [(&str, StepKind); 14] = [
    ("e2e-merge-all-changed", StepKind::BranchMerge),
    ("e2e-merge-all-new", StepKind::BranchMerge),
    ("e2e-merge-diverged-updates", StepKind::BranchMerge),
    ("e2e-mixed-load", StepKind::Load),
    ("e2e-branch-create", StepKind::BranchCreate),
    ("e2e-branch-create-from", StepKind::BranchCreate),
    ("e2e-branch-list", StepKind::BranchList),
    ("e2e-branch-delete", StepKind::BranchDelete),
    ("e2e-branch-adopt-untouched", StepKind::BranchMerge),
    ("e2e-branch-adopt-written", StepKind::BranchMerge),
    ("e2e-branch-first-write", StepKind::Mutate),
    ("e2e-nearest-prefilter", StepKind::Query),
    ("e2e-nearest-nprobes-one", StepKind::Query),
    ("e2e-rrf-traversal", StepKind::Query),
];
const QUERY_SHAPES: [&str; 14] = [
    "e2e-query-scan",
    "e2e-query-wide-scan",
    "e2e-query-filter",
    "e2e-query-lookup",
    "e2e-query-count",
    "e2e-query-grouped",
    "e2e-query-top-people",
    "e2e-query-friends",
    "e2e-query-filtered-friends",
    "e2e-query-no-friends",
    "e2e-query-count-bare",
    "e2e-query-destination-projection",
    "e2e-query-grouped-fanout",
    "e2e-query-destination-search",
];
const TRAVERSAL_SHAPES: [(&str, bool); 5] = [
    ("e2e-traversal-hop1", true),
    ("e2e-traversal-hop2", true),
    ("e2e-traversal-hop3", true),
    ("e2e-traversal-selective-csr", false),
    ("e2e-traversal-selective-indexed", false),
];

fn query_shape_cases() -> impl Iterator<Item = (&'static str, bool)> {
    QUERY_SHAPES
        .into_iter()
        .map(|id| (id, true))
        .chain(TRAVERSAL_SHAPES)
}

macro_rules! scenario_tests {
    ($run:ident, $covered:ident, [$($test:ident => $name:literal),* $(,)?]) => {
        $(
            #[tokio::test]
            async fn $test() {
                $run($name).await;
            }
        )*
        const $covered: &[&str] = &[$($name),*];
    };
}

fn catalog() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks")
}
fn recorded_catalog() -> crate::catalog::Catalog {
    let mut catalog = crate::catalog::Catalog::load(&catalog().join("benchmarks.yaml")).unwrap();
    catalog.definition.defaults.environment =
        Some(GqtEnvironment::embedded(crate::case::Backend::LocalFs {
            filesystem: crate::case::LocalFilesystem::Apfs,
            storage_class: crate::case::LocalStorageClass::NvmeSsd,
        }));
    catalog.definition.defaults.protocol.reset = Some(crate::case::ResetMode::LocalClonefile);
    catalog
}
fn plan(name: &str) -> PlannedGqt {
    recorded_catalog().plan(name).unwrap()
}
#[derive(Debug, Default)]
struct Signals {
    events: Vec<&'static str>,
    elapsed: Option<u64>,
}
impl MeasurementSignals for Signals {
    fn ready(&mut self) -> RunnerResult<()> {
        self.events.push("ready");
        Ok(())
    }
    fn settled(&mut self, elapsed: u64) -> RunnerResult<()> {
        self.events.push("settled");
        self.elapsed = Some(elapsed);
        Ok(())
    }
}
async fn run_sample(
    plan: &PlannedGqt,
) -> Result<(crate::gqt_runner::GqtRepObservation, Signals), crate::runner::RunnerError> {
    run_sample_with_logical(plan)
        .await
        .map(|(sample, signals, _)| (sample, signals))
}
async fn run_sample_with_logical(
    plan: &PlannedGqt,
) -> Result<
    (
        crate::gqt_runner::GqtRepObservation,
        Signals,
        crate::dataset_identity::DatasetLogicalV1,
    ),
    crate::runner::RunnerError,
> {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("active");
    let scratch = directory.path().join("scratch");
    std::fs::create_dir(&scratch).unwrap();
    let (logical, _) = crate::dataset_worker::build_dataset(
        &plan.dataset_build_plan().unwrap(),
        root.to_str().unwrap(),
        &scratch,
        None,
    )
    .await
    .unwrap();
    let physical =
        crate::reset::digest_physical_tree(&root, crate::reset::TraversalLimits::default())
            .unwrap();
    let metadata =
        crate::reset::digest_metadata_tree(&root, crate::reset::TraversalLimits::default())
            .unwrap();
    let bound = plan
        .bind(&logical.logical_content_sha256, &logical.algorithm)
        .unwrap();
    let mut signals = Signals::default();
    let sample =
        execute_gqt_rep_signaled(1, &root, &physical, &metadata, &bound, &mut signals).await?;
    validate_sample(
        &sample,
        &bound,
        1,
        &crate::gqt_protocol::PreparationProofV2::Embedded {
            physical_digest: physical,
            metadata_digest: metadata,
        },
        sample.elapsed_us,
        false,
    )
    .unwrap();
    assert_eq!(signals.events, vec!["ready", "settled"]);
    assert_eq!(signals.elapsed, Some(sample.elapsed_us));
    Ok((sample, signals, logical))
}
#[test]
fn original_pairs_and_groups_preserve_pre_migration_identity() {
    let catalog = recorded_catalog();
    let baseline: serde_json::Value =
        serde_json::from_str(include_str!("../tests/fixtures/gqt-catalog-identity.json")).unwrap();
    let baseline = baseline.as_object().unwrap();
    assert_eq!(baseline.len(), 40);
    for (id, old) in baseline {
        let scenario = catalog
            .definition
            .scenarios
            .iter()
            .find(|scenario| scenario.id == *id)
            .unwrap_or_else(|| panic!("original scenario {id} is missing"));
        let plan = catalog.plan(&scenario.id).unwrap();
        plan.revalidate().unwrap();
        assert_eq!(
            old["planned_sha256"], plan.planned_sha256,
            "{}",
            scenario.id
        );
        assert_eq!(old["recipe_sha256"], plan.recipe_sha256);
        assert_eq!(old["workload_sha256"], plan.queries.sha256);
        assert_eq!(old["needs_indices"], plan.needs_indices);
        let bound = plan
            .bind(
                &"a".repeat(64),
                crate::dataset_identity::DATASET_LOGICAL_ALGORITHM,
            )
            .unwrap();
        assert_eq!(old["bound_point_id"], bound.point_id);
        assert_eq!(
            old["measured_step"],
            serde_json::to_value(&plan.definition.workload.measured_step).unwrap()
        );
        assert_eq!(
            old["environment"],
            serde_json::to_value(&plan.definition.environment).unwrap()
        );
        assert_eq!(
            old["protocol"],
            serde_json::to_value(&plan.definition.protocol).unwrap()
        );
        assert_eq!(
            old["cache_condition"],
            serde_json::to_value(&plan.cache_condition).unwrap()
        );
        assert_eq!(
            old["repetitions"],
            scenario.expand(&catalog.definition.defaults).1
        );
    }
    let groups: std::collections::BTreeMap<String, Vec<(String, u32)>> =
        serde_json::from_str(include_str!("../tests/fixtures/gqt-catalog-groups.json")).unwrap();
    assert_eq!(groups.len(), 8);
    let selection = |name: Option<&str>| {
        catalog
            .resolve(name, None)
            .unwrap()
            .runs
            .into_iter()
            .map(|run| (run.case.id().to_owned(), run.repetitions))
            .collect::<Vec<_>>()
    };
    for (group, expected) in &groups {
        assert_eq!(selection(Some(group)), *expected, "group {group}");
    }
    assert_eq!(selection(None), groups["local-fast"]);
    assert_eq!(
        selection(Some("search")),
        vec![
            ("nearest-ranks-updated-embedding".into(), 1),
            ("nearest-skips-deleted-rows-and-fills-limit".into(), 1),
        ]
    );
}

#[test]
fn end_to_end_catalog_selects_complete_public_operations() {
    use crate::case::{EnginePreparation, PageCacheCondition, ProcessLifecycle};

    let catalog = crate::catalog::Catalog::load(&catalog().join("benchmarks.yaml")).unwrap();
    let selected = catalog.resolve(Some("end-to-end"), None).unwrap();
    assert_eq!(
        selected
            .runs
            .iter()
            .map(|run| (run.case.id(), run.repetitions))
            .collect::<Vec<_>>(),
        END_TO_END
            .iter()
            .map(|(id, _)| (*id, 1))
            .collect::<Vec<_>>()
    );
    for (name, kind) in END_TO_END {
        let p = catalog.plan(name).unwrap();
        p.revalidate().unwrap();
        p.dataset_build_plan().unwrap().revalidate().unwrap();
        crate::gqt_runner::preflight_acquisition_budget(&p, 1).unwrap();
        let parsed = p.queries.parse().unwrap();
        let steps = workload_steps(&parsed).unwrap();
        let measured = &p.definition.workload.measured_step;
        assert_eq!(measured.ordinal, 1, "{name}");
        assert_eq!(steps[0].kind, kind, "{name}");
        assert_eq!(steps[0].source.trim(), measured.text.trim(), "{name}");
        let restart = steps
            .iter()
            .position(|step| step.kind == StepKind::Restart)
            .unwrap_or_else(|| panic!("{name}: fresh-handle verification is missing"));
        assert!(
            steps[restart + 1..]
                .iter()
                .any(|step| step.kind == StepKind::Query),
            "{name}: reopen must be followed by an explicit row check"
        );
        assert_eq!(
            p.cache_condition.process,
            ProcessLifecycle::FreshPerRepetition
        );
        assert_eq!(p.cache_condition.engine, EnginePreparation::PreparationOnly);
        assert_eq!(
            p.cache_condition.page_cache,
            PageCacheCondition::Uncontrolled
        );
        assert_eq!(p.cache_condition.iterations, 0);
    }
}

async fn end_to_end_scenario(name: &str) {
    let kind = END_TO_END
        .iter()
        .find(|(id, _)| *id == name)
        .unwrap_or_else(|| panic!("{name}: not an END_TO_END scenario"))
        .1;
    let p = plan(name);
    let (sample, _) = run_sample(&p)
        .await
        .unwrap_or_else(|error| panic!("{name}: {error}"));
    assert_eq!(sample.outcome, "expectations-passed", "{name}");
    assert!(sample.verification.selected_assertion_passed, "{name}");
    assert!(sample.verification.following_assertions > 0, "{name}");
    assert_eq!(
        sample.steps.iter().filter(|step| step.ordinal == 1).count(),
        1,
        "{name}: one complete public operation is measured"
    );
    assert_eq!(
        sample.merge.is_some(),
        kind == StepKind::BranchMerge,
        "{name}"
    );
}

scenario_tests!(
    end_to_end_scenario,
    END_TO_END_SCENARIO_TESTS,
    [
        end_to_end_e2e_merge_all_changed => "e2e-merge-all-changed",
        end_to_end_e2e_merge_all_new => "e2e-merge-all-new",
        end_to_end_e2e_merge_diverged_updates => "e2e-merge-diverged-updates",
        end_to_end_e2e_mixed_load => "e2e-mixed-load",
        end_to_end_e2e_branch_create => "e2e-branch-create",
        end_to_end_e2e_branch_create_from => "e2e-branch-create-from",
        end_to_end_e2e_branch_list => "e2e-branch-list",
        end_to_end_e2e_branch_delete => "e2e-branch-delete",
        end_to_end_e2e_branch_adopt_untouched => "e2e-branch-adopt-untouched",
        end_to_end_e2e_branch_adopt_written => "e2e-branch-adopt-written",
        end_to_end_e2e_branch_first_write => "e2e-branch-first-write",
        end_to_end_e2e_nearest_prefilter => "e2e-nearest-prefilter",
        end_to_end_e2e_nearest_nprobes_one => "e2e-nearest-nprobes-one",
        end_to_end_e2e_rrf_traversal => "e2e-rrf-traversal",
    ]
);

#[test]
fn end_to_end_scenario_tests_cover_every_catalog_scenario() {
    assert_eq!(
        END_TO_END_SCENARIO_TESTS,
        END_TO_END.iter().map(|(id, _)| *id).collect::<Vec<_>>()
    );
}

#[test]
fn query_shape_catalog_selects_complete_queries_with_bounded_plans() {
    use crate::case::{EnginePreparation, PageCacheCondition, ProcessLifecycle};

    let catalog = crate::catalog::Catalog::load(&catalog().join("benchmarks.yaml")).unwrap();
    for (group, expected) in [
        ("query-shapes", QUERY_SHAPES.to_vec()),
        (
            "traversal",
            TRAVERSAL_SHAPES.iter().map(|(id, _)| *id).collect(),
        ),
    ] {
        let selected = catalog.resolve(Some(group), None).unwrap();
        assert_eq!(
            selected
                .runs
                .iter()
                .map(|run| (run.case.id(), run.repetitions))
                .collect::<Vec<_>>(),
            expected.into_iter().map(|id| (id, 1)).collect::<Vec<_>>(),
            "{group}"
        );
    }
    for (name, warm) in query_shape_cases() {
        let p = catalog.plan(name).unwrap();
        p.revalidate().unwrap();
        p.dataset_build_plan().unwrap().revalidate().unwrap();
        crate::gqt_runner::preflight_acquisition_budget(&p, 1).unwrap();
        let parsed = p.queries.parse().unwrap();
        let steps = workload_steps(&parsed).unwrap();
        let measured = &p.definition.workload.measured_step;
        let selected_index = usize::from(warm);
        assert_eq!(measured.ordinal, selected_index + 1, "{name}");
        assert_eq!(steps.len(), selected_index + 3, "{name}");
        assert_eq!(steps[selected_index].kind, StepKind::Query, "{name}");
        assert_eq!(
            steps[selected_index].source.trim(),
            measured.text.trim(),
            "{name}"
        );
        if warm {
            assert_eq!(steps[0].kind, StepKind::Query, "{name}");
            assert_eq!(steps[0].source.trim(), measured.text.trim(), "{name}");
        }
        assert_eq!(
            steps[selected_index + 1].kind,
            StepKind::Restart,
            "{name}: verification must reopen the fixture"
        );
        assert_eq!(steps[selected_index + 2].kind, StepKind::Query, "{name}");
        assert_eq!(
            steps[selected_index + 2].source.trim(),
            measured.text.trim(),
            "{name}: the reopened handle must return the same complete result"
        );
        assert_eq!(
            p.cache_condition.process,
            ProcessLifecycle::FreshPerRepetition
        );
        assert_eq!(
            p.cache_condition.engine,
            if warm {
                EnginePreparation::WarmedByProgram
            } else {
                EnginePreparation::PreparationOnly
            },
            "{name}"
        );
        assert_eq!(
            p.cache_condition.page_cache,
            if warm {
                PageCacheCondition::ProgramConditioned
            } else {
                PageCacheCondition::Uncontrolled
            },
            "{name}"
        );
        assert_eq!(p.cache_condition.iterations, u32::from(warm), "{name}");
        if !warm {
            assert!(
                p.needs_indices,
                "{name}: pinned traversal needs prepared indexes"
            );
        }
    }
}

async fn query_shape_scenario(name: &str) {
    let p = plan(name);
    let (sample, _) = run_sample(&p)
        .await
        .unwrap_or_else(|error| panic!("{name}: {error}"));
    assert_eq!(sample.outcome, "expectations-passed", "{name}");
    assert!(sample.verification.selected_assertion_passed, "{name}");
    assert!(sample.verification.following_assertions > 0, "{name}");
    assert_eq!(
        sample
            .steps
            .iter()
            .filter(|step| step.ordinal == p.definition.workload.measured_step.ordinal)
            .count(),
        1,
        "{name}: one complete query is measured"
    );
    assert!(sample.merge.is_none(), "{name}");
}

scenario_tests!(
    query_shape_scenario,
    QUERY_SHAPE_SCENARIO_TESTS,
    [
        query_shape_e2e_query_scan => "e2e-query-scan",
        query_shape_e2e_query_wide_scan => "e2e-query-wide-scan",
        query_shape_e2e_query_filter => "e2e-query-filter",
        query_shape_e2e_query_lookup => "e2e-query-lookup",
        query_shape_e2e_query_count => "e2e-query-count",
        query_shape_e2e_query_grouped => "e2e-query-grouped",
        query_shape_e2e_query_top_people => "e2e-query-top-people",
        query_shape_e2e_query_friends => "e2e-query-friends",
        query_shape_e2e_query_filtered_friends => "e2e-query-filtered-friends",
        query_shape_e2e_query_no_friends => "e2e-query-no-friends",
        query_shape_e2e_query_count_bare => "e2e-query-count-bare",
        query_shape_e2e_query_destination_projection => "e2e-query-destination-projection",
        query_shape_e2e_query_grouped_fanout => "e2e-query-grouped-fanout",
        query_shape_e2e_query_destination_search => "e2e-query-destination-search",
        query_shape_e2e_traversal_hop1 => "e2e-traversal-hop1",
        query_shape_e2e_traversal_hop2 => "e2e-traversal-hop2",
        query_shape_e2e_traversal_hop3 => "e2e-traversal-hop3",
        query_shape_e2e_traversal_selective_csr => "e2e-traversal-selective-csr",
        query_shape_e2e_traversal_selective_indexed => "e2e-traversal-selective-indexed",
    ]
);

#[test]
fn query_shape_scenario_tests_cover_every_catalog_scenario() {
    assert_eq!(
        QUERY_SHAPE_SCENARIO_TESTS,
        query_shape_cases().map(|(id, _)| id).collect::<Vec<_>>()
    );
}

#[test]
fn content_identity_excludes_authored_locations_and_selectors() {
    let p = plan("tiny-read");
    let mut moved = p.clone();
    moved.definition.id = "renamed-case".into();
    moved.definition.workload.queries = "elsewhere/query.gqt".into();
    moved.definition.fixture = GqtFixture::Dataset {
        path: "elsewhere/dataset.gqt".into(),
    };
    if let DatasetRecipe::Gqt { source } = &mut moved.dataset {
        source.stem = "renamed_dataset".into()
    }
    moved.queries.stem = "renamed_queries".into();
    moved.case_digest = crate::model::typed_sha256(&moved.definition).unwrap();
    moved.revalidate().unwrap();
    assert_eq!(
        p.bind(
            &"a".repeat(64),
            crate::dataset_identity::DATASET_LOGICAL_ALGORITHM
        )
        .unwrap()
        .point_id,
        moved
            .bind(
                &"a".repeat(64),
                crate::dataset_identity::DATASET_LOGICAL_ALGORITHM
            )
            .unwrap()
            .point_id
    );
    assert_ne!(
        p.bind(
            &"a".repeat(64),
            crate::dataset_identity::DATASET_LOGICAL_ALGORITHM
        )
        .unwrap()
        .point_id,
        p.bind(
            &"b".repeat(64),
            crate::dataset_identity::DATASET_LOGICAL_ALGORITHM
        )
        .unwrap()
        .point_id
    );
}
#[test]
fn selected_echo_loop_write_prefix_and_implicit_suffix_refuse() {
    let p = plan("tiny-read");
    let c = p.queries.parse().unwrap();
    let mut changed = p.definition.workload.measured_step.clone();
    changed.text.push_str(" changed");
    assert!(admit_queries(&c, &changed).is_err());
    let header = p.queries.text.split("--- query").next().unwrap();
    let implicit = format!(
        "{header}{}\n--- expect unordered\n{{\"key\":\"a\",\"val\":1}}\n{{\"key\":\"b\",\"val\":2}}\n--- expect shape\nkey: String\nval: I64\n\n--- restart\n",
        p.definition.workload.measured_step.text
    );
    let c = omnigraph_gqt_core::parse_case("implicit_suffix", &implicit).unwrap();
    assert!(admit_queries(&c, &p.definition.workload.measured_step).is_err());
    let first = p.queries.text.find("--- query").unwrap();
    let second = p.queries.text[first + 3..].find("--- query").unwrap() + first + 3;
    let looped = format!(
        "{}--- loop $i 0 2\n{}--- endloop\n{}",
        &p.queries.text[..first],
        &p.queries.text[first..second],
        &p.queries.text[second..]
    );
    let c = omnigraph_gqt_core::parse_case("selected_loop", &looped).unwrap();
    assert!(admit_queries(&c, &p.definition.workload.measured_step).is_err());
    let mut written = p.queries.text.clone();
    written.insert_str(
        first,
        "--- mutate\nbranch create \"child\" from main\n--- expect ok\n",
    );
    let c = omnigraph_gqt_core::parse_case("write_prefix", &written).unwrap();
    let mut selection = p.definition.workload.measured_step.clone();
    selection.ordinal = 2;
    assert!(admit_queries(&c, &selection).is_err());
    let dst = p.queries.text.replace(
        "target: omnigraph-engine",
        "target: omnigraph-engine-dst\n    seeds: [1]",
    );
    let c = omnigraph_gqt_core::parse_case("dst_only", &dst).unwrap();
    assert!(admit_queries(&c, &p.definition.workload.measured_step).is_err());
}
#[tokio::test]
async fn selected_read_restart_mutation_branch_and_single_load_use_engine_receipts() {
    for name in [
        "tiny-read",
        "tiny-restart",
        "tiny-post-reopen",
        "tiny-mutate",
        "tiny-branch",
        "tiny-load",
    ] {
        let p = plan(name);
        let (sample, _) = run_sample(&p)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(
            sample
                .steps
                .iter()
                .filter(|s| s.ordinal == p.definition.workload.measured_step.ordinal)
                .count(),
            1
        );
        assert!(sample.verification.following_assertions > 0);
        assert!(sample.merge.is_none());
    }
}
async fn bounded_scenario(name: &str) -> crate::dataset_identity::DatasetLogicalV1 {
    let p = plan(name);
    let (sample, _, logical) = run_sample_with_logical(&p)
        .await
        .unwrap_or_else(|e| panic!("{name}: {e}"));
    assert!(sample.verification.following_assertions > 0, "{name}");
    let main = logical.branches.iter().find(|b| b.name == "main").unwrap();
    if name.starts_with("very-long-history-32-") {
        assert_eq!(main.history_commits, 34, "{name}");
    }
    logical
}

scenario_tests!(
    bounded_scenario,
    BOUNDED_SCENARIO_TESTS,
    [
        bounded_very_long_history_32_read => "very-long-history-32-read",
        bounded_very_long_history_32_write => "very-long-history-32-write",
        bounded_very_long_history_32_reopen => "very-long-history-32-reopen",
        bounded_hot_table_idle_base => "hot-table-idle-base",
        bounded_hot_table_idle_tables_4 => "hot-table-idle-tables-4",
        bounded_hot_table_idle_branches_4 => "hot-table-idle-branches-4",
        bounded_hot_table_idle_branches_4_aged => "hot-table-idle-branches-4-aged",
        bounded_parallel_tables_1 => "parallel-tables-1",
        bounded_parallel_tables_2 => "parallel-tables-2",
        bounded_parallel_tables_4 => "parallel-tables-4",
        bounded_identity_lifecycle => "identity-lifecycle",
        bounded_repeated_deletion_recreation_rows => "repeated-deletion-recreation-rows",
        bounded_repeated_deletion_recreation_branches => "repeated-deletion-recreation-branches",
        bounded_threshold_crossings_before => "threshold-crossings-before",
        bounded_threshold_crossings_at => "threshold-crossings-at",
        bounded_threshold_crossings_after => "threshold-crossings-after",
        bounded_threshold_crossings_production_control => "threshold-crossings-production-control",
    ]
);

/// Equal current content across three different histories is one claim, so
/// the three `equal-current-*` scenarios stay in one test.
#[tokio::test]
async fn bounded_equal_current_histories_share_content_identity() {
    assert!(
        BOUNDED_SCENARIO_TESTS
            .iter()
            .all(|name| !name.starts_with("equal-current-")),
        "an equal-current scenario outside this test loses its cross-history claim"
    );
    let mut equal_current = Vec::new();
    for name in [
        "equal-current-narrow-one-batch",
        "equal-current-wide-one-batch",
        "equal-current-wide-four-batches",
    ] {
        let logical = bounded_scenario(name).await;
        let main = logical.branches.iter().find(|b| b.name == "main").unwrap();
        equal_current.push((name, main.clone()));
    }
    assert_eq!(equal_current.len(), 3);
    let narrow = &equal_current[0].1;
    let wide = &equal_current[1].1;
    let batched = &equal_current[2].1;
    assert_eq!(narrow.content_sha256, wide.content_sha256);
    assert_eq!(narrow.content_sha256, batched.content_sha256);
    assert_eq!(narrow.schema_sha256, batched.schema_sha256);
    assert_eq!(narrow.history_commits, 18);
    assert_eq!(wide.history_commits, 18);
    assert_eq!(batched.history_commits, 66);
    assert_ne!(narrow.lineage_sha256, batched.lineage_sha256);
}
#[tokio::test]
async fn dataset_seed_indices_precede_updates_and_deletes() {
    for name in [
        "nearest-ranks-updated-embedding",
        "nearest-skips-deleted-rows-and-fills-limit",
    ] {
        let p = plan(name);
        assert!(p.needs_indices);
        assert!(!match &p.dataset {
            DatasetRecipe::Gqt { source } => source.parse().unwrap().needs_indices,
            _ => unreachable!(),
        });
        run_sample(&p)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
    }
}
#[tokio::test]
async fn post_settled_assertion_failure_retains_rejected_evidence() {
    let mut p = plan("tiny-mutate");
    p.queries.text = p.queries.text.replace("\"val\":3", "\"val\":999");
    p.queries.sha256 = sha256_bytes(p.queries.text.as_bytes());
    p.planned_sha256 = p.planned_hash().unwrap();
    let e = run_sample(&p).await.unwrap_err();
    assert_eq!(e.code, "gqt_verification_failed");
    let sample = e.context.gqt_settled_sample.unwrap();
    assert_eq!(sample.outcome, "verification-failed");
    assert!(sample.verification.selected_assertion_passed);
    assert_eq!(sample.steps.iter().filter(|s| s.ordinal == 1).count(), 1);
}
#[tokio::test]
async fn empty_seed_and_unwritten_child_have_reproducible_dataset_identity() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("empty_dataset.gqt");
    std::fs::write(&source,"# issue: none\n# notes: Empty data with a lazy branch.\n--- runner\ntimeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine\n    storage: local-filesystem\n--- schema\nnode Item { key: String @key val: I64 }\n--- seed\n--- mutate\nbranch create \"child\" from main\n--- expect ok\n").unwrap();
    let p = override_sources(plan("tiny-read"), Some(&source), None).unwrap();
    let mut identities = Vec::new();
    for index in 0..2 {
        let active = directory.path().join(format!("active{index}"));
        let scratch = directory.path().join(format!("scratch{index}"));
        std::fs::create_dir(&scratch).unwrap();
        let (logical, _) = crate::dataset_worker::build_dataset(
            &p.dataset_build_plan().unwrap(),
            active.to_str().unwrap(),
            &scratch,
            None,
        )
        .await
        .unwrap();
        crate::dataset_identity::validate(&logical, None).unwrap();
        assert_eq!(logical.branches.len(), 2);
        assert!(logical.branches.iter().all(|b| b.node_tables[0].rows == 0));
        identities.push(logical.logical_content_sha256)
    }
    assert_eq!(identities[0], identities[1]);
}
#[test]
fn request_budget_and_projection_budget_are_admitted_before_io() {
    let mut p = plan("tiny-read");
    p.definition.workload.measured_step.text = "x".repeat(16 * 1024 + 1);
    assert!(validate_definition(&p.definition).is_err());
    let p = plan("tiny-read");
    let b = p
        .bind(
            &"a".repeat(64),
            crate::dataset_identity::DATASET_LOGICAL_ALGORITHM,
        )
        .unwrap();
    assert!(serde_json::to_vec(&b).unwrap().len() < 512 * 1024);
}

async fn authority_fixture() -> crate::gqt_record::GqtRunRecordV1 {
    use crate::gqt_record::{GqtMeasurementsV1, GqtRunIdentityV1, GqtRunRecordV1};
    use crate::record::{
        AcquisitionStatusV1, AcquisitionV1, EvidenceStrengthV1, WallClockSummaryV1,
    };
    let plan = plan("tiny-read");
    let directory = tempfile::tempdir().unwrap();
    let active = directory.path().join("active");
    let scratch = directory.path().join("scratch");
    std::fs::create_dir(&scratch).unwrap();
    let (summary, registered_source_identity) = crate::dataset_worker::build_dataset(
        &plan.dataset_build_plan().unwrap(),
        active.to_str().unwrap(),
        &scratch,
        None,
    )
    .await
    .unwrap();
    let physical =
        crate::reset::digest_physical_tree(&active, crate::reset::TraversalLimits::default())
            .unwrap();
    let template_metadata =
        crate::reset::digest_metadata_tree(&active, crate::reset::TraversalLimits::default())
            .unwrap();
    let bound = plan
        .bind(&summary.logical_content_sha256, &summary.algorithm)
        .unwrap();
    let mut sample = execute_gqt_rep_signaled(
        1,
        &active,
        &physical,
        &template_metadata,
        &bound,
        &mut Signals::default(),
    )
    .await
    .unwrap();
    sample.peak_rss_bytes = Some(1024);
    let legacy = crate::record::tests::valid_record_fixture();
    let mut invocation = legacy.invocation;
    invocation.invocation_id.replace_range(25..26, "C");
    let elapsed = sample.elapsed_us;
    let record = GqtRunRecordV1 {
        format_version: 1,
        invocation,
        run: GqtRunIdentityV1 {
            point_identity_version: 1,
            point_id: bound.point_id,
            point_name: bound.point_name,
            case_id: plan.definition.id.clone(),
            case_digest: plan.case_digest.clone(),
            run_spec: bound.identity,
        },
        sut: crate::gqt_record::GqtSutIdentityV1::Embedded(Box::new(legacy.sut)),
        machine: Some(legacy.machine),
        backend: Some(legacy.backend),
        fixture: Some(crate::dataset_cache::DatasetManifestV1 {
            format_version: 1,
            key: "a".repeat(64),
            recipe_sha256: plan.recipe_sha256,
            engine_digest: crate::dataset_cache::GQT_ENGINE_DIGEST.into(),
            needs_indices: plan.needs_indices,
            reset: plan.definition.protocol.reset,
            handoff: crate::dataset_worker::FixtureBuildHandoff {
                summary,
                registered_source_identity,
                physical,
                template_metadata,
            },
        }),
        dataset_cache_hit: Some(true),
        acquisition: AcquisitionV1 {
            status: AcquisitionStatusV1::Complete,
            requested_repetitions: 1,
            observed_repetitions: 1,
            terminal: None,
        },
        measurements: GqtMeasurementsV1 {
            wall_clock: WallClockSummaryV1 {
                min_us: elapsed,
                p50_us: elapsed,
                max_us: elapsed,
                p95_us: None,
                p95_supported: false,
                evidence: EvidenceStrengthV1::Directional,
            },
            raw_samples: vec![sample],
            layer_presence: legacy.measurements.layer_presence,
            claim_policy: legacy.measurements.claim_policy,
        },
    };
    crate::gqt_record::validate(&record).unwrap();
    record
}
fn rehash_record(record: &mut crate::gqt_record::GqtRunRecordV1) {
    record.run.point_id = crate::model::typed_sha256(&record.run.run_spec).unwrap();
    record.run.point_name = format!(
        "gqt-{}-{}",
        record.run.run_spec.cache_condition.display_label(),
        &record.run.point_id[..12]
    );
}
#[tokio::test]
async fn gqt_authority_refuses_invalid_machine_counters_dataset_and_treatment() {
    let record = authority_fixture().await;
    let mutations: &[fn(&mut crate::gqt_record::GqtRunRecordV1)] = &[
        |r| r.machine.as_mut().unwrap().logical_cores = 0,
        |r| {
            r.measurements.raw_samples[0]
                .logical_store_calls
                .as_mut()
                .unwrap()
                .manifest
                .get = u64::MAX;
            r.measurements.raw_samples[0]
                .logical_store_calls
                .as_mut()
                .unwrap()
                .table
                .get = 1;
        },
        |r| {
            r.measurements.raw_samples[0]
                .control_store_calls
                .as_mut()
                .unwrap()
                .write_text = 1;
            r.measurements.raw_samples[0]
                .control_store_calls
                .as_mut()
                .unwrap()
                .mutation_calls = 0;
        },
        |r| r.fixture.as_mut().unwrap().handoff.summary.branches.clear(),
        |r| r.fixture.as_mut().unwrap().handoff.summary.algorithm = "unknown".into(),
        |r| r.fixture.as_mut().unwrap().handoff.template_metadata.files += 1,
        |r| r.run.run_spec.protocol.deadline_seconds = Some(0),
        |r| {
            r.run.run_spec.measured_step.text =
                r.run
                    .run_spec
                    .measured_step
                    .text
                    .replacen("--- query", "--- queryjunk", 1)
        },
        |r| r.run.run_spec.measured_step.text = "--- load invalid\ninvalid: recipe".into(),
        |r| r.run.run_spec.cache_condition.iterations = 1,
        |r| {
            r.measurements.raw_samples[0].steps[0].kind =
                crate::gqt_runner::GqtOperationKind::Mutate
        },
        |r| r.measurements.raw_samples[0].outcome = "verification-failed".into(),
    ];
    for mutate in mutations {
        let mut changed = record.clone();
        mutate(&mut changed);
        rehash_record(&mut changed);
        assert!(crate::gqt_record::validate(&changed).is_err());
    }
    let mut censored = record.clone();
    censored.acquisition.requested_repetitions = 2;
    censored.acquisition.status = crate::record::AcquisitionStatusV1::Censored;
    censored.acquisition.terminal = Some(crate::record::AcquisitionTerminalV1 {
        failed_repetition: 1,
        stage: crate::record::AcquisitionTerminalStageV1::Runner,
        code: "gqt_verification_failed".into(),
    });
    crate::gqt_record::validate(&censored).unwrap();
    assert!(!censored.claim_eligible());
    censored
        .acquisition
        .terminal
        .as_mut()
        .unwrap()
        .failed_repetition = 2;
    assert!(crate::gqt_record::validate(&censored).is_err());
}
#[tokio::test]
async fn mixed_legacy_and_gqt_archive_rebuilds_and_queries_both_points() {
    let directory = tempfile::tempdir().unwrap();
    let archive = directory.path().join("archive");
    let projection = directory.path().join("projection");
    crate::archive::preflight_archive_publication(&archive).unwrap();
    let legacy = crate::record::tests::valid_record_fixture();
    let gqt = authority_fixture().await;
    let canonical = crate::gqt_record::canonical_bytes(&gqt).unwrap();
    assert_eq!(
        crate::gqt_record::parse(&canonical).unwrap(),
        crate::gqt_record::AnyRunRecordV1::Gqt(Box::new(gqt.clone()))
    );
    crate::archive::publish_record(&archive, &legacy).unwrap();
    let receipt = crate::archive::publish_record(&archive, &gqt).unwrap();
    let built = crate::projection::rebuild_projection(&archive, &projection)
        .await
        .unwrap();
    assert_eq!(built.record_count, 2);
    assert_eq!(built.point_count, 2);
    let points = crate::projection::list_points_page(&projection, 10, None)
        .await
        .unwrap();
    assert_eq!(points.rows.len(), 2);
    let runs =
        crate::projection::list_runs_for_point_page(&projection, &gqt.run.point_id, 10, None)
            .await
            .unwrap();
    assert_eq!(runs.rows.len(), 1);
    assert_eq!(runs.rows[0]["record_sha256"], receipt.record_sha256);
    assert_eq!(runs.rows[0]["invocation_id"], gqt.invocation.invocation_id);
}
#[test]
fn raw_dataset_plan_needs_no_measured_operation_and_unions_query_indexes() {
    let measured = plan("nearest-ranks-updated-embedding");
    let dataset = catalog().join("fixtures/nearest_ranks_updated_embedding.gqt");
    let queries = catalog().join("workloads/nearest_ranks_updated_embedding.gqt");
    let raw = dataset_file(
        &dataset,
        None,
        measured.definition.environment.backend.clone(),
        measured.definition.protocol.reset,
    )
    .unwrap();
    assert!(!raw.needs_indices);
    let indexed = dataset_file(
        &dataset,
        Some(&queries),
        measured.definition.environment.backend.clone(),
        measured.definition.protocol.reset,
    )
    .unwrap();
    assert!(indexed.needs_indices);
    assert_eq!(indexed.recipe_sha256, measured.recipe_sha256);
    assert_eq!(indexed, measured.dataset_build_plan().unwrap());
}
#[test]
fn acquisition_receipts_fit_before_dataset_io() {
    use crate::gqt_runner::{
        GQT_RECORD_ENVELOPE_RESERVE, preflight_acquisition_budget, sample_byte_upper_bound,
    };
    let mut p = plan("tiny-read");
    preflight_acquisition_budget(&p, 10_000).unwrap();
    let first = p.queries.text.find("--- query").unwrap();
    let second = p.queries.text[first + 3..].find("--- query").unwrap() + first + 3;
    p.queries.text = format!(
        "{}--- loop $i 0 4095\n{}--- endloop\n",
        &p.queries.text[..second],
        &p.queries.text[second..]
    );
    p.queries.sha256 = sha256_bytes(p.queries.text.as_bytes());
    p.planned_sha256 = p.planned_hash().unwrap();
    p.revalidate().unwrap();
    preflight_acquisition_budget(&p, 1).unwrap();
    let err = preflight_acquisition_budget(&p, 10_000).unwrap_err();
    assert_eq!(err.code, "gqt_record_budget_exceeded");
    let sample = sample_byte_upper_bound(&p).unwrap();
    let last =
        ((crate::record::MAX_RECORD_BYTES - GQT_RECORD_ENVELOPE_RESERVE) / (sample + 1)) as u32;
    assert!(last > 1 && last < 10_000);
    preflight_acquisition_budget(&p, last).unwrap();
    assert!(preflight_acquisition_budget(&p, last + 1).is_err());
    let mut merge = plan("branch-merge-d50-warm");
    let with_phases = sample_byte_upper_bound(&merge).unwrap();
    merge.definition.protocol.attribution = crate::case::Attribution::Off;
    assert!(sample_byte_upper_bound(&merge).unwrap() < with_phases);
}

#[tokio::test]
async fn completed_gqt_prefix_has_one_owner_and_failed_repetition_is_retained() {
    let record = authority_fixture().await;
    let p = plan("tiny-read");
    let logical = &record.fixture.as_ref().unwrap().handoff.summary;
    let bound = p
        .bind(&logical.logical_content_sha256, &logical.algorithm)
        .unwrap();
    let sample = record.measurements.raw_samples[0].clone();
    let partial = crate::gqt_runner::RunExecution {
        runner_output_version: 1,
        case_id: record.run.case_id,
        case_path: catalog().join("cases/tiny-read.case-v1.yaml"),
        point_id: bound.point_id.clone(),
        point_name: bound.point_name.clone(),
        requested_repetitions: 2,
        bound,
        build: crate::runner::build_evidence(None).unwrap(),
        machine: record.machine.unwrap(),
        environment: Some(crate::environment::LocalEnvironmentEvidence {
            filesystem: "apfs".into(),
            storage_class: "nvme-ssd".into(),
            mount_point: "/test".into(),
            storage_protocol: "local".into(),
            available_bytes: 1024,
            probe: "test",
        }),
        fixture: record.fixture,
        dataset_cache_hit: Some(true),
        server_receipt: None,
        samples: vec![sample.clone()],
        wall_clock: crate::runner::WallClockSummary {
            observed_repetitions: 1,
            min_us: sample.elapsed_us,
            p50_us: sample.elapsed_us,
            max_us: sample.elapsed_us,
            p95_us: None,
            p95_supported: false,
        },
        durable_record: false,
    };
    let mut error = crate::runner::RunnerError::new("worker_failed", "test failure");
    error.context.gqt_partial_run = Some(Box::new(partial));
    error.context.gqt_settled_sample = Some(Box::new(sample));
    error.context.gqt_settled_elapsed_us = Some(7);
    error.context.child_process = Some(crate::runner::ChildProcessEvidence::default());
    error.context.quarantined_workspace = Some(PathBuf::from("/test/quarantined"));
    let before = serde_json::to_value(&error).unwrap();
    assert!(before.get("gqt_partial_run").is_some());
    error.context.clear_completed_prefix();
    let after = serde_json::to_value(&error).unwrap();
    for key in [
        "completed_runs",
        "completed_samples",
        "partial_run",
        "gqt_partial_run",
    ] {
        assert!(after.get(key).is_none(), "{key}");
    }
    for key in [
        "gqt_settled_sample",
        "gqt_settled_elapsed_us",
        "child_process",
        "quarantined_workspace",
    ] {
        assert_eq!(after[key], before[key], "{key}");
    }
}

#[test]
fn complete_frame_bound_accounts_for_paths_and_json_escaping_before_build() {
    let p = plan("tiny-read");
    crate::gqt_supervisor::preflight_plan(&p, Path::new("/cache")).unwrap();
    let huge = PathBuf::from(format!("/{}", "\"".repeat(600_000)));
    assert!(crate::gqt_supervisor::preflight_plan(&p, &huge).is_err());
}

#[test]
fn nested_and_relocated_catalogs_keep_content_identity_and_refuse_aliases() {
    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("first");
    for child in ["cases/nested", "fixtures", "workloads", "suites/group"] {
        std::fs::create_dir_all(first.join(child)).unwrap();
    }
    std::fs::copy(
        catalog().join("fixtures/tiny_graph.gqt"),
        first.join("fixtures/tiny_graph.gqt"),
    )
    .unwrap();
    std::fs::copy(
        catalog().join("workloads/tiny_read.gqt"),
        first.join("workloads/tiny_read.gqt"),
    )
    .unwrap();
    let mut definition = plan("tiny-read").definition;
    definition.fixture = GqtFixture::Dataset {
        path: "../fixtures/tiny_graph.gqt".into(),
    };
    definition.workload.queries = "../workloads/tiny_read.gqt".into();
    let original = serde_yaml::to_string(&definition).unwrap();
    let nested = original
        .replace("../fixtures/", "../../fixtures/")
        .replace("../workloads/", "../../workloads/");
    std::fs::write(first.join("cases/nested/read.case-v1.yaml"), &nested).unwrap();
    let suite = "version: 1\nname: nested\nruns:\n  - case: ../../cases/nested/read.case-v1.yaml\n    repetitions: 1\n";
    std::fs::write(first.join("suites/group/suite-v1.yaml"), suite).unwrap();
    let initial = crate::load_suite(&first.join("suites/group"))
        .into_result()
        .unwrap();
    let moved = directory.path().join("moved");
    std::fs::rename(first, &moved).unwrap();
    let relocated = crate::load_suite(&moved.join("suites/group"))
        .into_result()
        .unwrap();
    assert_eq!(
        initial.runs[0].case.planned_identity(),
        relocated.runs[0].case.planned_identity()
    );
    std::fs::write(
        moved.join("cases/nested/alias.case-v1.yaml"),
        nested.replace("id: tiny-read", "id: alias-read"),
    )
    .unwrap();
    std::fs::write(
        moved.join("suites/group/suite-v1.yaml"),
        format!("{suite}  - case: ../../cases/nested/alias.case-v1.yaml\n    repetitions: 1\n"),
    )
    .unwrap();
    assert!(
        crate::load_suite(&moved.join("suites/group"))
            .into_result()
            .is_err()
    );
}
#[tokio::test]
async fn expected_parameter_error_does_not_claim_a_warming_engine_read() {
    let mut p = plan("tiny-read");
    let prefix = "--- query\nquery invalid($key: String) { match { $i: Item { key: $key } } return { $i.val } }\n--- params\n{\"key\":42}\n--- expect error: params rejected\n\n";
    let at = p.queries.text.find("--- query").unwrap();
    p.queries.text.insert_str(at, prefix);
    p.queries.sha256 = sha256_bytes(p.queries.text.as_bytes());
    p.definition.workload.measured_step.ordinal = 2;
    p.cache_condition = admit_queries(
        &p.queries.parse().unwrap(),
        &p.definition.workload.measured_step,
    )
    .unwrap();
    p.case_digest = crate::model::typed_sha256(&p.definition).unwrap();
    p.planned_sha256 = p.planned_hash().unwrap();
    let error = run_sample(&p).await.unwrap_err();
    assert_eq!(error.code, "gqt_prepare_failed");
    assert!(error.context.gqt_settled_sample.is_none());
}

#[path = "gqt_served_tests.rs"]
mod served;

#[tokio::test]
async fn gqt_record_build_rejects_receipts_outside_the_frozen_program() {
    use crate::gqt_record::GqtSutIdentityV1;
    use crate::gqt_runner::RunExecution;
    use crate::runner::{BuildEvidence, WallClockSummary};

    let record = authority_fixture().await;
    let GqtSutIdentityV1::Embedded(sut) = &record.sut else {
        panic!("expected embedded fixture");
    };
    let build = &sut.build;
    let plan = plan("tiny-read");
    let bound = plan
        .bind(
            &record.run.run_spec.dataset_logical_digest,
            &record.run.run_spec.dataset_identity_algorithm,
        )
        .unwrap();
    let wall = &record.measurements.wall_clock;
    let mut execution = RunExecution {
        runner_output_version: 1,
        case_id: record.run.case_id.clone(),
        case_path: std::path::PathBuf::from("fixture"),
        point_id: bound.point_id.clone(),
        point_name: bound.point_name.clone(),
        requested_repetitions: 1,
        bound,
        build: BuildEvidence {
            source_commit: sut.source_commit.clone(),
            source_tree_dirty: sut.source_tree_dirty,
            cargo_profile: build.profile.clone(),
            cargo_opt_level: build.cargo_opt_level.clone(),
            debug_assertions: build.debug_assertions,
            effective_lance_mem_pool_size: sut.engine.lance_mem_pool_size.clone(),
            target_triple: build.target_triple.clone(),
            rustc_version: build.rustc_version.clone(),
            declared_release_lto: build.declared_release_lto.clone(),
            declared_release_codegen_units: build.declared_release_codegen_units,
            declared_release_strip: build.declared_release_strip,
            cargo_encoded_rustflags_present: build.cargo_encoded_rustflags_present,
            release_profile_environment_overrides_supported: build
                .release_profile_environment_overrides_supported,
            effective_codegen_options_proved: build.effective_codegen_options_proved,
            engine_feature_flags: sut.engine.feature_flags.clone(),
            enabled_techniques: sut.engine.enabled_techniques.clone(),
            worker_executable_sha256: Some(build.worker_executable_sha256.clone()),
        },
        machine: record.machine.clone().unwrap(),
        environment: Some(crate::environment::LocalEnvironmentEvidence {
            filesystem: "apfs".into(),
            storage_class: "nvme-ssd".into(),
            mount_point: "/fixture".into(),
            storage_protocol: "fixture".into(),
            available_bytes: 1,
            probe: "fixture",
        }),
        fixture: record.fixture.clone(),
        dataset_cache_hit: record.dataset_cache_hit,
        server_receipt: None,
        samples: record.measurements.raw_samples.clone(),
        wall_clock: WallClockSummary {
            observed_repetitions: 1,
            min_us: wall.min_us,
            p50_us: wall.p50_us,
            max_us: wall.max_us,
            p95_us: wall.p95_us,
            p95_supported: wall.p95_supported,
        },
        durable_record: false,
    };
    crate::gqt_record::build(&execution, record.invocation.clone(), None).unwrap();
    let suffix = execution.samples[0].steps.last_mut().unwrap();
    assert!(suffix.ordinal > execution.bound.identity.measured_step.ordinal);
    suffix.ordinal = 999;
    let error = crate::gqt_record::build(&execution, record.invocation, None).unwrap_err();
    assert!(
        error.to_string().contains("unknown receipt ordinal"),
        "{error}"
    );
}
