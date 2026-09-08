//! Branch control operations in the existing three-child scenario protocol.
//! Each repetition starts from a newly prepared fixture; only one public
//! operation is timed. Verification compares every accepted table pointer and
//! opens its pinned view, so a wrong-parent fork cannot become a timing sample.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;
use std::time::Instant;

use lance::dataset::builder::DatasetBuilder;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use serde::{Deserialize, Serialize};

use super::{Args, fixture_controls, rfc023_limits, rfc023_scenarios};

const SOURCE: &str = "control-sibling-0";
const TARGET: &str = "control-target";
const EXPECTED_FILE: &str = "branch-control-expected.json";

pub(super) fn is_scenario(name: &str) -> bool {
    matches!(
        name,
        "branch-create" | "branch-create-from" | "branch-list" | "branch-delete"
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize)]
struct Fixture {
    branches: BTreeMap<String, BranchView>,
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
    for branch in 0..args.branches {
        db.branch_create(&format!("control-sibling-{branch}"))
            .await
            .expect("create sibling");
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
    if args.scenario == "branch-delete" {
        db.branch_create(TARGET)
            .await
            .expect("create deletion victim");
        // Own every table on the victim, so delete exercises actual native
        // ref cleanup as well as its surviving-branch dependency checks.
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
    let expected_count = args.branches + 1 + usize::from(args.scenario == "branch-delete");
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
    assert_eq!(source.tables["node:Chunk"].rows, args.rows as u64 + 1);
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
    let fixture = Fixture { branches };
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
    let ((db, operation_open_us), open_io) = super::helpers::cost::measure(async {
    let open_start = Instant::now();
    let db = Omnigraph::open(root.to_str().unwrap())
        .await
        .expect("open branch fixture");
    let operation_open_us = open_start.elapsed().as_micros() as u64;
    (db, operation_open_us)
    }).await;
    let prewarm = fixture_controls::prewarm(&db, args).await;
    let operation_pre_peak_rss_bytes = super::current_process_peak_rss_bytes();
    let ((listed, operation_wall_us, operation_post_peak_rss_bytes,
        post_ack_reclaim_wait_us, operation_complete_wall_us,
        operation_completion_peak_rss_bytes), io) = super::helpers::cost::measure(async {
    let started = Instant::now();
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
        _ => unreachable!("validated branch scenario"),
    };
    let operation_wall_us = started.elapsed().as_micros() as u64;
    let operation_post_peak_rss_bytes = super::current_process_peak_rss_bytes();
    // The public delete acknowledges authority removal before its owned
    // table forks finish reclaiming. Keep that acknowledgement timer intact,
    // then join this handle's work before the operation child exits.
    let post_ack_reclaim_wait_us = if args.scenario == "branch-delete" {
        let reclaim_started = Instant::now();
        db.wait_for_fork_reclaims().await;
        Some(reclaim_started.elapsed().as_micros() as u64)
    } else {
        None
    };
    let operation_complete_wall_us = started.elapsed().as_micros() as u64;
    let operation_completion_peak_rss_bytes = super::current_process_peak_rss_bytes();
    (listed, operation_wall_us, operation_post_peak_rss_bytes,
        post_ack_reclaim_wait_us, operation_complete_wall_us, operation_completion_peak_rss_bytes)
    }).await;
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
        "measurement_boundary": "exactly one public branch operation after separately recorded fresh open and optional prewarm; operation_wall is acknowledgement, operation_complete_wall also waits for delete fork reclaim; optional fork first_read is separate and later; setup and final verification run in separate children",
        "rss_boundary": "operation child whole-process wait4 HWM includes runtime, graph open, optional prewarm/first payload read, delete fork reclaim and output recording, excludes setup/verify; pre/post/completion self HWM is not isolated allocation",
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
    let mut verified_reclaimed_table_refs = 0;
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
                .expect("open surviving physical table for reclaim verification");
            let refs = dataset
                .list_branches()
                .await
                .expect("list physical table refs");
            assert!(
                !refs.contains_key(native_ref),
                "acknowledged delete did not reclaim an owned native table ref"
            );
            verified_reclaimed_table_refs += 1;
        }
        assert_eq!(verified_reclaimed_table_refs, args.tables);
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
        "verified_reclaimed_table_refs": verified_reclaimed_table_refs,
        "verify_wall_us": started.elapsed().as_micros() as u64,
    })
}
