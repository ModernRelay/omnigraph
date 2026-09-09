//! Branch control operations in the existing three-child scenario protocol.
//! Each repetition starts from a newly prepared fixture; only one public
//! operation is timed. Verification compares every accepted table pointer and
//! opens its pinned view, so a wrong-parent fork cannot become a timing sample.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;
use std::time::Instant;

use arrow_array::{Array as _, FixedSizeListArray, Float32Array, StringArray};
use futures::TryStreamExt as _;
use lance::dataset::builder::DatasetBuilder;
use omnigraph::db::{CleanupPolicyOptions, MergeOutcome, Omnigraph, ReadTarget};
use omnigraph::instrumentation::{MergeWriteProbes, with_merge_write_probes};
use omnigraph::loader::LoadMode;
use serde::{Deserialize, Serialize};

use super::{Args, fixture_controls, rfc023_limits, rfc023_scenarios};

const SOURCE: &str = "control-sibling-0";
const TARGET: &str = "control-target";
const EXPECTED_FILE: &str = "branch-control-expected.json";

pub(super) fn is_scenario(name: &str) -> bool {
    matches!(
        name,
        "branch-create"
            | "branch-create-from"
            | "branch-list"
            | "branch-delete"
            | "branch-pointer-adopt-lazy"
            | "branch-pointer-adopt-owned"
            | "branch-first-write"
            | "branch-cleanup"
    )
}

pub(super) fn validate_args(args: &Args) -> Result<(), String> {
    if args.branches == 0 || args.tables == 0 || args.runs == 0 {
        return Err("branch scenarios require nonzero --branches, --tables, and --runs".into());
    }
    if args.baseline {
        return Err(
            "branch scenarios measure one real operation and have no --baseline arm".into(),
        );
    }
    if is_content_scenario(args)
        && (args.rows > 16_384 || args.dims > 16 || args.branches > 8 || args.tables > 8)
    {
        return Err(
            "exact content scenarios require rows <= 16384, dims <= 16, branches/tables <= 8"
                .into(),
        );
    }
    rfc023_limits::derive_chunk_plan(args.dims, "base", args.rows)?;
    if args.child {
        if !matches!(
            args.phase.as_deref(),
            Some("setup" | "operation" | "verify")
        ) || args.fixture_root.as_deref().is_none_or(str::is_empty)
        {
            return Err(
                "branch child requires --phase setup|operation|verify and --fixture-root".into(),
            );
        }
    } else if args.phase.is_some() || args.fixture_root.is_some() {
        return Err("--phase/--fixture-root are internal to branch scenario children".into());
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TableView {
    path: String,
    native_ref: Option<String>,
    version: u64,
    rows: u64,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct BranchView {
    head: Option<String>,
    effective_head: String,
    tables: BTreeMap<String, TableView>,
}

#[derive(Default, Serialize, Deserialize)]
struct Fixture {
    branches: BTreeMap<String, BranchView>,
    payloads: BTreeMap<String, BranchRows>,
    physical: BTreeMap<String, PhysicalTable>,
    collectible: Vec<TableView>,
    required_parent: Option<TableView>,
    required_parent_rows: TableRows,
}

fn root(args: &Args) -> &Path {
    Path::new(
        args.fixture_root
            .as_deref()
            .expect("validated branch fixture root"),
    )
}

async fn branch_view(db: &Omnigraph, branch: &str, tables: usize) -> BranchView {
    let snapshot = db
        .snapshot_of(ReadTarget::branch(branch))
        .await
        .expect("capture branch view");
    let entries = snapshot
        .datasets()
        .map(|entry| {
            (
                entry.type_key.clone(),
                TableView {
                    path: entry.dataset_path.clone(),
                    native_ref: entry.native_dataset_branch.clone(),
                    version: entry.published_dataset_version,
                    rows: entry.entity_count,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        entries.len(),
        tables,
        "every requested table must be populated"
    );
    for (key, entry) in &entries {
        assert!(
            entry.rows > 0,
            "empty table would make the workload vacuous"
        );
        let dataset = snapshot
            .open_dataset(key)
            .await
            .expect("open pinned branch table");
        assert_eq!(
            dataset.count_rows(None).await.expect("count pinned table") as u64,
            entry.rows
        );
    }
    BranchView {
        head: snapshot
            .graph_head((branch != "main").then_some(branch))
            .map(str::to_string),
        // A newly forked branch has no own graph_head row, but inherits the
        // parent's effective snapshot ID until its first content publication.
        effective_head: db
            .resolve_snapshot(branch)
            .await
            .expect("resolve effective branch head")
            .as_str()
            .to_string(),
        tables: entries,
    }
}

pub(super) async fn setup(args: &Args) -> serde_json::Value {
    let root = root(args);
    assert!(
        std::fs::read_dir(root).unwrap().next().is_none(),
        "fixture must start empty"
    );
    let uri = root.to_str().unwrap();
    let started = Instant::now();
    let mut schema = rfc023_scenarios::graph_schema(args.dims);
    for table in 1..args.tables {
        writeln!(schema, "\nnode Extra{table} {{ slug: String @key }}").unwrap();
    }
    let db = Omnigraph::init(uri, &schema)
        .await
        .expect("initialize branch fixture");
    let patterns = rfc023_scenarios::vector_json_patterns(args.dims, args.seed);
    let plan = rfc023_limits::derive_chunk_plan(args.dims, "base", args.rows).unwrap();
    rfc023_scenarios::load_graph_rows(
        &db,
        "main",
        "base",
        args.rows,
        plan.batch_rows,
        &patterns,
        LoadMode::Append,
    )
    .await;
    for table in 1..args.tables {
        let row = format!("{{\"type\":\"Extra{table}\",\"data\":{{\"slug\":\"base\"}}}}\n");
        let loaded = db
            .load("main", &row, LoadMode::Append)
            .await
            .expect("populate scalar table");
        assert_eq!(loaded.nodes_loaded.values().sum::<usize>(), 1);
    }
    let age = rfc023_scenarios::age_fixture(&db, args).await;
    let mut fixture = Fixture::default();
    if args.scenario == "branch-cleanup" {
        fixture.collectible =
            serde_json::from_value(age["setup_retired_table_refs"].clone()).unwrap();
    }
    if args.scenario == "branch-pointer-adopt-owned" {
        db.branch_create(TARGET).await.unwrap();
        db.load(TARGET, &one_row(args, "owner-only"), LoadMode::Append)
            .await
            .unwrap();
    }
    for branch in 0..args.branches {
        let name = format!("control-sibling-{branch}");
        if branch == 0 && args.scenario == "branch-pointer-adopt-owned" {
            db.branch_create_from(ReadTarget::branch(TARGET), &name)
                .await
                .unwrap();
        } else {
            db.branch_create(&name).await.expect("create sibling");
        }
    }
    // A named source that differs from main proves create-from used that
    // source, rather than accidentally measuring another main fork.
    rfc023_scenarios::load_graph_rows(
        &db,
        SOURCE,
        "source-only",
        1,
        1,
        &patterns,
        LoadMode::Append,
    )
    .await;
    if args.scenario == "branch-pointer-adopt-lazy" {
        db.branch_create(TARGET).await.unwrap();
    } else if args.scenario == "branch-first-write" {
        db.branch_create_from(ReadTarget::branch(SOURCE), TARGET)
            .await
            .unwrap();
    } else if args.scenario == "branch-cleanup" {
        prepare_cleanup(args, &db, &mut fixture).await;
    }
    if args.scenario == "branch-delete" {
        db.branch_create(TARGET)
            .await
            .expect("create deletion victim");
        rfc023_scenarios::load_graph_rows(
            &db,
            TARGET,
            "victim-only",
            1,
            1,
            &patterns,
            LoadMode::Append,
        )
        .await;
        for table in 1..args.tables {
            let row = format!("{{\"type\":\"Extra{table}\",\"data\":{{\"slug\":\"victim\"}}}}\n");
            db.load(TARGET, &row, LoadMode::Append)
                .await
                .expect("own victim scalar table");
        }
    }
    let layout = fixture_controls::prepare_layout(uri, args).await;
    let mut names = db.branch_list().await.expect("list prepared branches");
    names.sort();
    let expected_count = args.branches
        + 1
        + usize::from(has_prepared_target(args))
        + 2 * usize::from(args.scenario == "branch-cleanup");
    assert_eq!(names.len(), expected_count);
    let mut branches = BTreeMap::new();
    for branch in names {
        branches.insert(branch.clone(), branch_view(&db, &branch, args.tables).await);
    }
    assert_eq!(
        branches.len(),
        expected_count,
        "branch registry contains duplicates"
    );
    let source = &branches[SOURCE];
    assert_eq!(
        source.tables["node:Chunk"].rows,
        args.rows as u64 + 1 + u64::from(args.scenario == "branch-pointer-adopt-owned")
    );
    assert_ne!(
        source.tables, branches["main"].tables,
        "named source must differ from main"
    );
    for parent in ["main", SOURCE] {
        let view = &branches[parent];
        assert_eq!(
            view.head.as_deref(),
            Some(view.effective_head.as_str()),
            "fixture parents must have real content heads, not synthetic snapshot IDs"
        );
    }
    fixture.branches = branches;
    if is_content_scenario(args) {
        for branch in fixture.branches.keys() {
            fixture
                .payloads
                .insert(branch.clone(), branch_rows(&db, branch).await);
        }
        fixture.physical = physical_tables(&db).await;
    }
    if is_pointer_adoption(args) {
        let target = &fixture.branches[TARGET].tables["node:Chunk"];
        let source = &fixture.branches[SOURCE].tables["node:Chunk"];
        assert_eq!(
            target.native_ref.is_some(),
            args.scenario == "branch-pointer-adopt-owned"
        );
        assert_ne!(target.native_ref, source.native_ref);
    }
    std::fs::write(
        root.join(EXPECTED_FILE),
        serde_json::to_vec(&fixture).unwrap(),
    )
    .unwrap();
    let mut metrics = serde_json::json!({
        "setup_fingerprint": format!("branch-control-v2:rows={}:dims={}:seed={}:branches={}:tables={}:history={}:retired={}", args.rows, args.dims, args.seed, args.branches, args.tables, args.history_commits, args.retired_branches),
        "setup_wall_us": started.elapsed().as_micros() as u64,
        "existing_sibling_branches": args.branches,
        "initial_branch_count_including_main": expected_count,
        "populated_table_count": args.tables,
        "main_chunk_rows": args.rows,
        "scalar_rows_per_extra_table": 1,
        "source_owned_table_count": 1,
        "victim_owned_table_count": if args.scenario == "branch-delete" { args.tables } else { 0 },
        "source_branch": if args.scenario == "branch-create-from" { SOURCE } else { "main" },
        "setup_verified": true,
        "setup_exact_rows_verified": is_content_scenario(args),
        "setup_cleanup_collectible_refs": fixture.collectible.len(),
        "setup_required_native_parent": fixture.required_parent.is_some(),
    });
    metrics
        .as_object_mut()
        .unwrap()
        .extend(age.as_object().unwrap().clone());
    metrics
        .as_object_mut()
        .unwrap()
        .extend(layout.as_object().unwrap().clone());
    metrics
}

pub(super) async fn operation(args: &Args) -> serde_json::Value {
    super::helpers::cost::cost_harness(async {
    let root = root(args);
    let ((mut db, operation_open_us), open_io) = super::helpers::cost::measure(async {
    let open_start = Instant::now();
    let db = Omnigraph::open(root.to_str().unwrap())
        .await
        .expect("open branch fixture");
    let operation_open_us = open_start.elapsed().as_micros() as u64;
    (db, operation_open_us)
    }).await;
    let prewarm = fixture_controls::prewarm(&db, args).await;
    let probes = MergeWriteProbes::default();
    let payload = if args.scenario == "branch-first-write" { one_row(args, "target-only") } else { String::new() };
    let operation_pre_peak_rss_bytes = super::current_process_peak_rss_bytes();
    let ((listed, merge_outcome, operation_wall_us, operation_post_peak_rss_bytes,
        post_ack_reclaim_wait_us, operation_complete_wall_us,
        operation_completion_peak_rss_bytes), io) = super::helpers::cost::measure(async {
    let started = Instant::now();
    let mut merge_outcome = None;
    let listed = match args.scenario.as_str() {
        "branch-create" => {
            db.branch_create(TARGET)
                .await
                .expect("measured branch create");
            None
        }
        "branch-create-from" => {
            db.branch_create_from(ReadTarget::branch(SOURCE), TARGET)
                .await
                .expect("measured branch create-from");
            None
        }
        "branch-list" => Some(db.branch_list().await.expect("measured branch list")),
        "branch-delete" => {
            db.branch_delete(TARGET)
                .await
                .expect("measured branch delete");
            None
        }
        "branch-pointer-adopt-lazy" | "branch-pointer-adopt-owned" => {
            merge_outcome = Some(with_merge_write_probes(probes.clone(), db.branch_merge(SOURCE, TARGET)).await.expect("measured pointer adoption"));
            None
        }
        "branch-first-write" => {
            db.load(TARGET, &payload, LoadMode::Append).await.expect("measured first-touch write");
            None
        }
        "branch-cleanup" => {
            db.cleanup(CleanupPolicyOptions { keep_versions: Some(1), older_than: None }).await.expect("measured explicit cleanup");
            None
        }
        _ => unreachable!("validated branch scenario"),
    };
    let operation_wall_us = started.elapsed().as_micros() as u64;
    let operation_post_peak_rss_bytes = super::current_process_peak_rss_bytes();
    let post_ack_reclaim_wait_us: Option<u64> = None;
    let operation_complete_wall_us = started.elapsed().as_micros() as u64;
    let operation_completion_peak_rss_bytes = super::current_process_peak_rss_bytes();
    (listed, merge_outcome, operation_wall_us, operation_post_peak_rss_bytes,
        post_ack_reclaim_wait_us, operation_complete_wall_us, operation_completion_peak_rss_bytes)
    }).await;
    if is_pointer_adoption(args) {
        assert_eq!(merge_outcome, Some(MergeOutcome::FastForward));
        assert_eq!(io.data_writes, 0, "pointer adoption must not write table objects");
        assert_eq!(probes.stage_fenced_insert_calls(), 0);
        assert_eq!(probes.stage_merge_insert_calls(), 0);
        assert_eq!(probes.stage_append_calls(), 0);
    }
    let first_read = if args.age_options_supplied && matches!(args.scenario.as_str(), "branch-create" | "branch-create-from") {
        fixture_controls::first_read(&db, TARGET, args.tables).await
    } else {
        serde_json::json!({})
    };
    // Persist only the operation's output after timing. The verification child
    // checks list contents against the prepared registry, not merely its size.
    if let Some(listed) = &listed {
        std::fs::write(
            root.join("branch-control-listed.json"),
            serde_json::to_vec(listed).unwrap(),
        )
        .unwrap();
    }
    let mut metrics = serde_json::json!({
        "routing": "production-omnigraph-branch-control",
        "operation": args.scenario,
        "merge_outcome": merge_outcome.map(|_| "fast_forward"),
        "probe_stage_fenced_insert_calls": probes.stage_fenced_insert_calls(),
        "probe_stage_merge_insert_calls": probes.stage_merge_insert_calls(),
        "probe_stage_append_calls": probes.stage_append_calls(),
        "completed_operations": 1,
        "production_path": true,
        "operation_open_us": operation_open_us,
        "operation_wall_us": operation_wall_us,
        "operation_wall_ms": operation_wall_us / 1000,
        "post_ack_reclaim_wait_us": post_ack_reclaim_wait_us,
        "operation_complete_wall_us": operation_complete_wall_us,
        "operation_complete_wall_ms": operation_complete_wall_us / 1000,
        "operation_pre_peak_rss_bytes": operation_pre_peak_rss_bytes,
        "operation_post_peak_rss_bytes": operation_post_peak_rss_bytes,
        "operation_completion_peak_rss_bytes": operation_completion_peak_rss_bytes,
        "operation_hwm_increase_bytes": operation_post_peak_rss_bytes.checked_sub(operation_pre_peak_rss_bytes).filter(|v| *v > 0),
        "measurement_boundary": "exactly one public branch operation after separately recorded fresh open and optional prewarm; operation_wall is acknowledgement, operation_complete_wall records the same foreground completion; delete leaves table forks for explicit cleanup; optional fork first_read is separate and later; setup and final verification run in separate children",
        "rss_boundary": "operation child whole-process wait4 HWM includes runtime, graph open, optional prewarm/first payload read and output recording, excludes setup/verify; pre/post/completion self HWM is not isolated allocation",
        "listed_branch_count": listed.as_ref().map(Vec::len),
    });
    metrics.as_object_mut().unwrap().extend(rfc023_scenarios::operation_io_metrics(&io).as_object().unwrap().clone());
    metrics.as_object_mut().unwrap().extend(fixture_controls::io_metrics("open", &open_io).as_object().unwrap().clone());
    metrics.as_object_mut().unwrap().extend(prewarm.as_object().unwrap().clone());
    metrics.as_object_mut().unwrap().extend(first_read.as_object().unwrap().clone());
    metrics
    }).await
}

pub(super) async fn verify(args: &Args) -> serde_json::Value {
    let root = root(args);
    let started = Instant::now();
    let mut fixture: Fixture =
        serde_json::from_slice(&std::fs::read(root.join(EXPECTED_FILE)).unwrap()).unwrap();
    let db = Omnigraph::open(root.to_str().unwrap())
        .await
        .expect("open branch verification");
    if is_content_scenario(args) {
        return verify_content(args, &db, &fixture).await;
    }
    let mut verified_deferred_table_refs = 0;
    if args.scenario == "branch-delete" {
        let victim = fixture
            .branches
            .remove(TARGET)
            .expect("prepared delete victim");
        assert!(
            db.snapshot_of(ReadTarget::branch(TARGET)).await.is_err(),
            "deleted branch still resolves"
        );
        assert_eq!(victim.tables.len(), args.tables);
        for entry in victim.tables.values() {
            let native_ref = entry
                .native_ref
                .as_deref()
                .expect("victim must own every table");
            let dataset = DatasetBuilder::from_uri(format!("{}/{}", root.display(), entry.path))
                .load()
                .await
                .expect("open surviving physical table for deferred cleanup verification");
            let refs = dataset
                .list_branches()
                .await
                .expect("list physical table refs");
            assert!(
                refs.contains_key(native_ref),
                "delete must leave the former table fork for explicit cleanup"
            );
            verified_deferred_table_refs += 1;
        }
        assert_eq!(verified_deferred_table_refs, args.tables);
    }
    let mut names = db.branch_list().await.expect("list final branches");
    names.sort();
    if matches!(
        args.scenario.as_str(),
        "branch-create" | "branch-create-from"
    ) {
        let parent = if args.scenario == "branch-create-from" {
            SOURCE
        } else {
            "main"
        };
        let created = branch_view(&db, TARGET, args.tables).await;
        assert_eq!(
            created.tables, fixture.branches[parent].tables,
            "fork must retain the exact parent table views"
        );
        assert_eq!(
            created.effective_head, fixture.branches[parent].effective_head,
            "fork must inherit the exact parent's effective head"
        );
        assert!(
            fixture
                .branches
                .insert(TARGET.to_string(), created)
                .is_none()
        );
    }
    assert_eq!(names, fixture.branches.keys().cloned().collect::<Vec<_>>());
    if args.scenario == "branch-list" {
        let mut listed: Vec<String> = serde_json::from_slice(
            &std::fs::read(root.join("branch-control-listed.json")).unwrap(),
        )
        .unwrap();
        listed.sort();
        assert_eq!(
            listed, names,
            "measured list must return the exact registry including main"
        );
    }
    for (branch, expected) in &fixture.branches {
        assert_eq!(
            &branch_view(&db, branch, args.tables).await,
            expected,
            "branch operation changed a surviving branch's accepted table state or head"
        );
    }
    serde_json::json!({
        "verification_passed": true,
        "verified_branch_count_including_main": names.len(),
        "verified_table_views": names.len() * args.tables,
        "verified_deferred_table_refs": verified_deferred_table_refs,
        "verify_wall_us": started.elapsed().as_micros() as u64,
    })
}

fn is_pointer_adoption(args: &Args) -> bool {
    matches!(
        args.scenario.as_str(),
        "branch-pointer-adopt-lazy" | "branch-pointer-adopt-owned"
    )
}

fn is_content_scenario(args: &Args) -> bool {
    is_pointer_adoption(args)
        || matches!(
            args.scenario.as_str(),
            "branch-first-write" | "branch-cleanup"
        )
}

fn has_prepared_target(args: &Args) -> bool {
    is_pointer_adoption(args)
        || matches!(
            args.scenario.as_str(),
            "branch-first-write" | "branch-delete"
        )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PayloadRow {
    id: String,
    embedding_bits: Option<Vec<u32>>,
}

type TableRows = BTreeMap<String, PayloadRow>;
type BranchRows = BTreeMap<String, TableRows>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RefHead {
    version: u64,
    identifier: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PhysicalTable {
    main_version: u64,
    refs: BTreeMap<String, RefHead>,
}

async fn table_rows(
    mut stream: lance::dataset::scanner::DatasetRecordBatchStream,
    key: &str,
) -> TableRows {
    let mut rows = BTreeMap::new();
    while let Some(batch) = stream.try_next().await.expect("read exact fixture rows") {
        assert_eq!(batch.num_columns(), if key == "node:Chunk" { 3 } else { 2 });
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let slugs = batch
            .column_by_name("slug")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(!ids.is_null(row) && !slugs.is_null(row));
            let embedding_bits = batch.column_by_name("embedding").map(|column| {
                let lists = column
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap();
                assert!(!lists.is_null(row));
                let value = lists.value(row);
                let values = value.as_any().downcast_ref::<Float32Array>().unwrap();
                assert_eq!(values.null_count(), 0);
                values
                    .values()
                    .iter()
                    .map(|value| value.to_bits())
                    .collect()
            });
            let previous = rows.insert(
                slugs.value(row).to_string(),
                PayloadRow {
                    id: ids.value(row).to_string(),
                    embedding_bits,
                },
            );
            assert!(previous.is_none(), "duplicate fixture key");
        }
    }
    rows
}

async fn branch_rows(db: &Omnigraph, branch: &str) -> BranchRows {
    let snapshot = db.snapshot_of(ReadTarget::branch(branch)).await.unwrap();
    let mut rows = BTreeMap::new();
    for entry in snapshot.datasets() {
        let table = snapshot.open_dataset(&entry.type_key).await.unwrap();
        let payload = table_rows(
            table
                .scan()
                .batch_size(1024)
                .try_into_stream()
                .await
                .unwrap(),
            &entry.type_key,
        )
        .await;
        assert_eq!(payload.len() as u64, entry.entity_count);
        rows.insert(entry.type_key.clone(), payload);
    }
    rows
}

async fn physical_tables(db: &Omnigraph) -> BTreeMap<String, PhysicalTable> {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let mut tables = BTreeMap::new();
    for entry in snapshot.datasets() {
        let dataset = DatasetBuilder::from_uri(format!("{}/{}", db.uri(), entry.dataset_path))
            .load()
            .await
            .unwrap();
        let mut refs = BTreeMap::new();
        for (native, contents) in dataset.list_branches().await.unwrap() {
            let head = dataset.checkout_branch(&native).await.unwrap();
            refs.insert(
                native,
                RefHead {
                    version: head.version().version,
                    identifier: format!("{:?}", contents.identifier),
                },
            );
        }
        tables.insert(
            entry.dataset_path.clone(),
            PhysicalTable {
                main_version: dataset.version().version,
                refs,
            },
        );
    }
    tables
}

async fn chunk_view(db: &Omnigraph, branch: &str) -> TableView {
    let snapshot = db.snapshot_of(ReadTarget::branch(branch)).await.unwrap();
    let entry = snapshot.dataset("node:Chunk").unwrap();
    TableView {
        path: entry.dataset_path.clone(),
        native_ref: entry.native_dataset_branch.clone(),
        version: entry.published_dataset_version,
        rows: entry.entity_count,
    }
}

fn one_row(args: &Args, prefix: &str) -> String {
    rfc023_scenarios::graph_jsonl_chunk(
        prefix,
        0,
        1,
        &rfc023_scenarios::vector_json_patterns(args.dims, args.seed),
    )
}

fn expected_row(args: &Args, prefix: &str) -> (String, PayloadRow) {
    let key = format!("{prefix}-0000000000");
    let patterns = rfc023_scenarios::vector_json_patterns(args.dims, args.seed);
    let values: Vec<f32> = serde_json::from_str(&format!("[{}]", patterns[0])).unwrap();
    let row = PayloadRow {
        id: key.clone(),
        embedding_bits: Some(values.into_iter().map(f32::to_bits).collect()),
    };
    (key, row)
}

async fn prepare_cleanup(args: &Args, db: &Omnigraph, fixture: &mut Fixture) {
    if fixture.collectible.is_empty() {
        db.branch_create("control-garbage").await.unwrap();
        db.load(
            "control-garbage",
            &one_row(args, "garbage-only"),
            LoadMode::Append,
        )
        .await
        .unwrap();
        fixture
            .collectible
            .push(chunk_view(db, "control-garbage").await);
        db.branch_delete("control-garbage").await.unwrap();
    }
    db.branch_create("control-retained-parent").await.unwrap();
    db.load(
        "control-retained-parent",
        &one_row(args, "parent-only"),
        LoadMode::Append,
    )
    .await
    .unwrap();
    fixture.required_parent = Some(chunk_view(db, "control-retained-parent").await);
    fixture.required_parent_rows = branch_rows(db, "control-retained-parent")
        .await
        .remove("node:Chunk")
        .unwrap();
    db.branch_create_from(
        ReadTarget::branch("control-retained-parent"),
        "control-retained-child",
    )
    .await
    .unwrap();
    db.load(
        "control-retained-child",
        &one_row(args, "child-only"),
        LoadMode::Append,
    )
    .await
    .unwrap();
    assert_eq!(
        db.branch_merge("control-retained-parent", "main")
            .await
            .unwrap(),
        MergeOutcome::FastForward
    );
    db.load("main", &one_row(args, "main-only"), LoadMode::Append)
        .await
        .unwrap();
    assert_eq!(
        db.branch_merge("main", "control-retained-parent")
            .await
            .unwrap(),
        MergeOutcome::FastForward
    );
    let parent = fixture.required_parent.as_ref().unwrap();
    let owner = chunk_view(db, "control-retained-parent").await;
    let child = chunk_view(db, "control-retained-child").await;
    assert_ne!(
        owner.native_ref, parent.native_ref,
        "parent table fork must have been abandoned"
    );
    assert_ne!(
        child.native_ref, parent.native_ref,
        "child must own a descendant table fork"
    );
    verify_required_parent(db, fixture).await;
}

async fn verify_required_parent(db: &Omnigraph, fixture: &Fixture) {
    let parent = fixture.required_parent.as_ref().unwrap();
    let table = DatasetBuilder::from_uri(format!("{}/{}", db.uri(), parent.path))
        .load()
        .await
        .unwrap();
    let parent_table = table
        .checkout_branch(parent.native_ref.as_deref().unwrap())
        .await
        .unwrap();
    let child = chunk_view(db, "control-retained-child").await;
    let child_table = table
        .checkout_branch(child.native_ref.as_deref().unwrap())
        .await
        .unwrap();
    assert_eq!(
        child_table
            .branch_identifier()
            .await
            .unwrap()
            .find_referenced_version(&parent_table.branch_identifier().await.unwrap()),
        Some(parent.version)
    );
    let pinned = parent_table.checkout_version(parent.version).await.unwrap();
    assert_eq!(
        table_rows(
            pinned
                .scan()
                .batch_size(1024)
                .try_into_stream()
                .await
                .unwrap(),
            "node:Chunk"
        )
        .await,
        fixture.required_parent_rows
    );
}

async fn verify_content(args: &Args, db: &Omnigraph, fixture: &Fixture) -> serde_json::Value {
    let mut names = db.branch_list().await.unwrap();
    names.sort();
    assert_eq!(names, fixture.branches.keys().cloned().collect::<Vec<_>>());
    let modifies_target = is_pointer_adoption(args) || args.scenario == "branch-first-write";
    for (branch, expected) in &fixture.branches {
        if modifies_target && branch == TARGET {
            continue;
        }
        assert_eq!(&branch_view(db, branch, args.tables).await, expected);
        assert_eq!(branch_rows(db, branch).await, fixture.payloads[branch]);
    }
    let mut verified_pointer_tables = 0;
    let mut verified_fresh_forks = 0;
    if modifies_target {
        let actual = branch_view(db, TARGET, args.tables).await;
        let before = &fixture.branches[TARGET];
        assert_ne!(actual.effective_head, before.effective_head);
        if is_pointer_adoption(args) {
            assert_eq!(actual.tables, fixture.branches[SOURCE].tables);
            assert_eq!(branch_rows(db, TARGET).await, fixture.payloads[SOURCE]);
            verified_pointer_tables = actual.tables.len();
        } else {
            let mut expected = fixture.payloads[TARGET].clone();
            let (key, row) = expected_row(args, "target-only");
            assert!(
                expected
                    .get_mut("node:Chunk")
                    .unwrap()
                    .insert(key, row)
                    .is_none()
            );
            assert_eq!(branch_rows(db, TARGET).await, expected);
            for (key, old) in &before.tables {
                let new = &actual.tables[key];
                if key == "node:Chunk" {
                    assert_eq!(new.path, old.path);
                    assert_eq!(new.rows, old.rows + 1);
                    assert!(new.native_ref.is_some());
                    assert_ne!(new.native_ref, old.native_ref);
                    verified_fresh_forks += 1;
                } else {
                    assert_eq!(new, old);
                }
            }
        }
    }
    let physical = physical_tables(db).await;
    if is_pointer_adoption(args) {
        assert_eq!(
            physical, fixture.physical,
            "pointer adoption must not alter table refs or HEADs"
        );
    } else if args.scenario == "branch-first-write" {
        let written = chunk_view(db, TARGET).await;
        for (path, before) in &fixture.physical {
            let after = &physical[path];
            assert_eq!(before.main_version, after.main_version);
            for (native, head) in &before.refs {
                assert_eq!(after.refs.get(native), Some(head));
            }
            assert_eq!(
                after.refs.len(),
                before.refs.len() + usize::from(path == &written.path)
            );
            if path == &written.path {
                let native = written.native_ref.as_ref().unwrap();
                assert!(!before.refs.contains_key(native));
                assert_eq!(after.refs[native].version, written.version);
            }
        }
    } else {
        let mut expected = fixture.physical.clone();
        for retired in &fixture.collectible {
            assert!(
                expected
                    .get_mut(&retired.path)
                    .unwrap()
                    .refs
                    .remove(retired.native_ref.as_ref().unwrap())
                    .is_some()
            );
        }
        assert_eq!(
            physical, expected,
            "cleanup must collect garbage and retain required native ancestry"
        );
        verify_required_parent(db, fixture).await;
    }
    if is_pointer_adoption(args) {
        let source = db.snapshot_of(ReadTarget::branch(SOURCE)).await.unwrap();
        let old_target = chunk_view(db, TARGET).await;
        db.load(TARGET, &one_row(args, "verify-only"), LoadMode::Append)
            .await
            .unwrap();
        let written = chunk_view(db, TARGET).await;
        assert_ne!(written.native_ref, old_target.native_ref);
        assert_ne!(
            written.native_ref,
            fixture.branches[TARGET].tables["node:Chunk"].native_ref
        );
        let after_write = physical_tables(db).await;
        for (path, before) in &physical {
            let after = &after_write[path];
            assert_eq!(after.main_version, before.main_version);
            for (native, head) in &before.refs {
                assert_eq!(after.refs.get(native), Some(head));
            }
            assert_eq!(
                after.refs.len(),
                before.refs.len() + usize::from(path == &written.path)
            );
        }
        let after_source = db.snapshot_of(ReadTarget::branch(SOURCE)).await.unwrap();
        for entry in source.datasets() {
            assert!(
                after_source
                    .dataset(&entry.type_key)
                    .unwrap()
                    .same_registration(entry)
            );
        }
        let mut expected = fixture.payloads[SOURCE].clone();
        let (key, row) = expected_row(args, "verify-only");
        assert!(
            expected
                .get_mut("node:Chunk")
                .unwrap()
                .insert(key, row)
                .is_none()
        );
        assert_eq!(branch_rows(db, TARGET).await, expected);
        assert_eq!(branch_rows(db, SOURCE).await, fixture.payloads[SOURCE]);
    }
    serde_json::json!({
        "verification_passed": true,
        "verified_exact_rows": true,
        "verified_branch_count_including_main": names.len(),
        "verified_table_views": names.len() * args.tables,
        "verified_pointer_tables": verified_pointer_tables,
        "verified_fresh_forks": verified_fresh_forks,
        "verified_collected_table_refs": if args.scenario == "branch-cleanup" { fixture.collectible.len() } else { 0 },
        "verified_required_native_parent": args.scenario == "branch-cleanup",
        "verified_post_adopt_isolation": is_pointer_adoption(args),
        "verification_boundary": "all row, pin and physical-head checks and the post-adoption isolation write occur in the separate untimed verification child",
    })
}
