mod helpers;

use std::fs;
#[cfg(feature = "failpoints")]
use std::sync::Arc;

use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::{SchemaMigrationStep, SchemaTypeKind};

use helpers::*;

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn plan_schema_reports_supported_additive_change() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let desired = TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );

    let plan = db.plan_schema(&desired).await.unwrap();
    assert!(plan.supported);
    assert!(plan.steps.iter().any(|step| matches!(
        step,
        SchemaMigrationStep::AddProperty {
            type_kind: SchemaTypeKind::Node,
            type_name,
            property_name,
            ..
        } if type_name == "Person" && property_name == "nickname"
    )));
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn long_lived_handle_uses_the_schema_catalog_bound_to_its_write_token() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema_owner = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    // Open before the migration: this handle's process-local ArcSwap catalog is
    // intentionally stale after `schema_owner` completes the apply.
    let stale_handle = Omnigraph::open(uri).await.unwrap();

    let desired = format!(
        "{}\nnode Project {{\n    name: String @key\n}}\n",
        TEST_SCHEMA.replace(
            "    age: I32?\n}",
            "    age: I32?\n    nickname: String?\n}",
        )
    );
    schema_owner.apply_schema(&desired).await.unwrap();

    let mutation = r#"
query insert_with_nickname($name: String, $age: I32, $nickname: String) {
    insert Person { name: $name, age: $age, nickname: $nickname }
}
"#;
    let inserted = stale_handle
        .mutate(
            "main",
            mutation,
            "insert_with_nickname",
            &mixed_params(
                &[("$name", "mutated-after-schema"), ("$nickname", "fresh")],
                &[("$age", 31)],
            ),
        )
        .await
        .expect("mutation must typecheck and build its batch with the token-bound catalog");
    assert_eq!(inserted.affected_nodes, 1);

    let loaded = stale_handle
        .load(
            "main",
            r#"{"type":"Person","data":{"name":"loaded-after-schema","age":32,"nickname":"fresh"}}"#,
            LoadMode::Merge,
        )
        .await
        .expect("load parsing and validation must use the same token-bound catalog");
    assert_eq!(loaded.nodes_loaded.get("Person"), Some(&1));
    assert_eq!(count_rows(&stale_handle, "node:Person").await, 2);

    // The same stale handle must bind branch-merge planning and conservative
    // branch-control table gates to the accepted contract captured under the
    // schema gate. The warm handle catalog predates Project; consulting it here
    // would fail with `unknown node type` (or omit Project's control queue).
    stale_handle.branch_create("source").await.unwrap();
    stale_handle.branch_create("target").await.unwrap();
    let project_mutation = r#"
query insert_project($name: String) {
    insert Project { name: $name }
}
"#;
    stale_handle
        .mutate(
            "source",
            project_mutation,
            "insert_project",
            &params(&[("$name", "fresh-catalog-project")]),
        )
        .await
        .expect("source write must use the token-bound post-apply catalog");
    let project_before_indices = stale_handle
        .snapshot_of(ReadTarget::branch("source"))
        .await
        .unwrap()
        .entry("node:Project")
        .unwrap()
        .table_version;
    stale_handle
        .ensure_indices_on("source")
        .await
        .expect("index planning must use the same token-bound post-apply catalog");
    let project_after_indices = stale_handle
        .snapshot_of(ReadTarget::branch("source"))
        .await
        .unwrap()
        .entry("node:Project")
        .unwrap()
        .table_version;
    assert!(
        project_after_indices > project_before_indices,
        "the stale handle must discover and build Project's declared key index"
    );
    assert_eq!(
        stale_handle
            .branch_merge("source", "target")
            .await
            .expect("merge planning must use the schema-gated post-apply catalog"),
        MergeOutcome::FastForward
    );
    assert_eq!(
        count_rows_branch(&stale_handle, "target", "node:Project").await,
        1
    );
}

/// Native branch controls must enumerate their conservative table envelope
/// from the accepted catalog captured under the schema gate, not a long-lived
/// handle's pre-apply ArcSwap. Park delete after that envelope is held and prove
/// a legacy Project-only index reconciler cannot cross its table queue.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn stale_handle_branch_delete_gates_tables_added_by_schema_apply() {
    use omnigraph::failpoints::names;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema_owner = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let stale_control = Arc::new(Omnigraph::open(uri).await.unwrap());
    let desired = format!("{TEST_SCHEMA}\nnode Project {{\n    name: String @key\n}}\n");
    schema_owner.apply_schema(&desired).await.unwrap();
    schema_owner.branch_create("target").await.unwrap();
    schema_owner
        .load(
            "target",
            r#"{"type":"Project","data":{"name":"pending-index"}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
    let index_reconciler = Arc::new(Omnigraph::open(uri).await.unwrap());

    let delete_rv =
        helpers::failpoint::Rendezvous::park_first(names::BRANCH_DELETE_POST_TABLE_GATES);
    let delete_handle = Arc::clone(&stale_control);
    let delete_task = tokio::spawn(async move { delete_handle.branch_delete("target").await });
    delete_rv.wait_until_reached().await;

    let index_handle = Arc::clone(&index_reconciler);
    let mut index_task =
        tokio::spawn(async move { index_handle.ensure_indices_on("target").await });
    let index_blocked =
        tokio::time::timeout(std::time::Duration::from_millis(250), &mut index_task)
            .await
            .is_err();
    delete_rv.release();
    assert!(
        index_blocked,
        "stale control catalog omitted the newly-added Project table gate"
    );
    delete_task.await.unwrap().unwrap();

    if tokio::time::timeout(std::time::Duration::from_secs(10), &mut index_task)
        .await
        .is_err()
    {
        index_task.abort();
        let _ = index_task.await;
        panic!("index reconciler did not finish after branch delete released its table gate");
    }
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn mutation_waits_for_mid_apply_schema_gate_then_reprepares() {
    use omnigraph::failpoints::names;

    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(init_and_load(&dir).await);
    let desired = TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );

    // First park the mutation after all validation/staging but before it enters
    // the schema→branch→table effect gates. This fixes the otherwise tiny race
    // window deterministically.
    let mutation_rv =
        helpers::failpoint::Rendezvous::park_first(names::MUTATION_POST_STAGE_PRE_EFFECT_GATE);
    let mutation_db = Arc::clone(&db);
    let mutation_task = tokio::spawn(async move {
        mutation_db
            .mutate(
                "main",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "schema-gated")], &[("$age", 33)]),
            )
            .await
    });
    mutation_rv.wait_until_reached().await;

    // Start schema apply and park it after its staging files (and any table
    // rewrite) exist but before manifest/schema promotion. The outer apply owns
    // the schema-control gate throughout this window.
    let schema_rv =
        helpers::failpoint::Rendezvous::park_first(names::SCHEMA_APPLY_AFTER_STAGING_WRITE);
    let schema_db = Arc::clone(&db);
    let schema_task = tokio::spawn(async move { schema_db.apply_schema(&desired).await });
    schema_rv.wait_until_reached().await;

    mutation_rv.release();
    // Give the already-runnable mutation repeated scheduler turns. It must stay
    // pending on the schema gate; completing here means it either advanced under
    // an in-flight migration or returned a spurious post-prepare failure.
    for _ in 0..128 {
        tokio::task::yield_now().await;
        if mutation_task.is_finished() {
            break;
        }
    }
    assert!(
        !mutation_task.is_finished(),
        "mutation must remain behind the schema-control gate while apply is in flight",
    );

    schema_rv.release();
    schema_task.await.unwrap().unwrap();
    let result = mutation_task
        .await
        .unwrap()
        .expect("insert-only mutation must reprepare under the promoted schema");
    assert_eq!(result.affected_nodes, 1);
    assert_eq!(count_rows(&db, "node:Person").await, 5);
}

/// ReadOnly opens participate in the process-local schema publication gate even
/// though they perform no recovery writes. Park an open immediately before its
/// source/IR/state read: schema apply must not reach staging until that coherent
/// catalog capture finishes.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn read_only_open_holds_schema_gate_through_catalog_capture() {
    use omnigraph::failpoints::names;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let owner = Arc::new(init_and_load(&dir).await);
    let desired = TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );

    let open_rv =
        helpers::failpoint::Rendezvous::park_first(names::OPEN_BEFORE_SCHEMA_CONTRACT_READ);
    let open_uri = uri.clone();
    let open_task = tokio::spawn(async move { Omnigraph::open_read_only(&open_uri).await });
    open_rv.wait_until_reached().await;

    let apply_rv =
        helpers::failpoint::Rendezvous::park_first(names::SCHEMA_APPLY_AFTER_STAGING_WRITE);
    let apply_owner = Arc::clone(&owner);
    let apply_task = tokio::spawn(async move { apply_owner.apply_schema(&desired).await });
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(200),
            apply_rv.wait_until_reached(),
        )
        .await
        .is_err(),
        "schema apply must remain behind the ReadOnly catalog-capture gate",
    );

    open_rv.release();
    let opened = open_task.await.unwrap().unwrap();
    assert!(
        !opened.catalog().node_types["Person"]
            .properties
            .contains_key("nickname"),
        "the serialized open must publish the complete pre-apply catalog"
    );
    apply_rv.wait_until_reached().await;
    apply_rv.release();
    apply_task.await.unwrap().unwrap();
    let current = Omnigraph::open_read_only(&uri).await.unwrap();
    assert!(
        current.catalog().node_types["Person"]
            .properties
            .contains_key("nickname"),
        "the next open must publish the complete post-apply catalog"
    );
}

/// Refresh must reacquire the schema gate after sidecar healing and retain it
/// through the ArcSwap publication. Otherwise a concurrent three-file schema
/// promotion can be interleaved with its contract read.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn refresh_holds_schema_gate_through_catalog_publication() {
    use omnigraph::failpoints::names;

    let dir = tempfile::tempdir().unwrap();
    let owner = Arc::new(init_and_load(&dir).await);
    let stale = Arc::new(Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap());
    let desired = TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );

    let reload_rv =
        helpers::failpoint::Rendezvous::park_first(names::SCHEMA_RELOAD_BEFORE_CONTRACT_READ);
    let refresh_handle = Arc::clone(&stale);
    let refresh_task = tokio::spawn(async move { refresh_handle.refresh().await });
    reload_rv.wait_until_reached().await;

    let apply_rv =
        helpers::failpoint::Rendezvous::park_first(names::SCHEMA_APPLY_AFTER_STAGING_WRITE);
    let apply_owner = Arc::clone(&owner);
    let apply_task = tokio::spawn(async move { apply_owner.apply_schema(&desired).await });
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(200),
            apply_rv.wait_until_reached(),
        )
        .await
        .is_err(),
        "schema apply must remain behind refresh's catalog-publication gate",
    );

    reload_rv.release();
    refresh_task.await.unwrap().unwrap();
    apply_rv.wait_until_reached().await;
    apply_rv.release();
    apply_task.await.unwrap().unwrap();

    stale.refresh().await.unwrap();
    assert!(
        stale.catalog().node_types["Person"]
            .properties
            .contains_key("nickname"),
        "a post-apply refresh must publish the complete new catalog"
    );
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn plan_schema_rejects_when_schema_contract_has_drifted() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let drifted = TEST_SCHEMA.replace("age: I32?", "age: I64?");
    fs::write(dir.path().join("_schema.pg"), drifted).unwrap();

    let err = db.plan_schema(TEST_SCHEMA).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("current _schema.pg no longer matches the accepted compiled schema")
    );
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_noop_returns_not_applied() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let result = db.apply_schema(TEST_SCHEMA).await.unwrap();
    assert!(result.supported);
    assert!(!result.applied);
    assert!(result.steps.is_empty());
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_rejects_when_non_main_branch_exists() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    db.branch_create("feature").await.unwrap();

    let desired = TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );
    let err = db.apply_schema(&desired).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("schema apply requires a graph with only main")
    );
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_unsupported_plan_does_not_advance_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    let desired = TEST_SCHEMA.replace("age: I32?", "age: I64?");
    let err = db.apply_schema(&desired).await.unwrap_err();
    assert!(err.to_string().contains("changing property type"));
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        before_version
    );
}

// ─── Destructive / safety-tier behavior ──────────────────────────────────────
//
// Schema migration v1 accepts:
// - Additive change: add type, add nullable property, add index, rename.
// - DropProperty { Soft } via the schema-lint v1 chassis (commit #3 of MR-694)
//   — the dropped column is removed from the current manifest version but
//   remains reachable via Lance time travel at the prior version, until
//   `omnigraph cleanup` runs. Hard mode (immediate data cleanup) lands in
//   commit #5 gated by `--allow-data-loss`.
//
// Every other destructive shape (drop type, narrow type, add required without
// backfill, remove constraint) still returns an `UnsupportedChange` step that
// surfaces as an error from `apply_schema`. These tests pin the current
// contract so a regression in the planner can't silently change behavior.

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_drops_a_nullable_property_softly_preserves_prior_version() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let people_before = count_rows(&db, "node:Person").await;
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    // Drop `age` from Person. v1 + chassis commit #3 emit
    // `DropProperty { Soft }`; the rewrite path projects to the
    // target schema (no `age`), commits via stage_overwrite. Row
    // counts are unchanged — only the column is dropped from the
    // current schema view.
    let desired = TEST_SCHEMA.replace("    age: I32?\n", "");

    // Confirm the plan emits DropProperty { Soft } (not UnsupportedChange).
    let plan = db.plan_schema(&desired).await.unwrap();
    assert!(plan.supported, "drop-property plan must be supported");
    assert!(
        plan.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::DropProperty {
                type_kind: SchemaTypeKind::Node,
                type_name,
                property_name,
                mode: omnigraph_compiler::DropMode::Soft,
                ..
            } if type_name == "Person" && property_name == "age"
        )),
        "expected DropProperty {{ type=Person, property=age, mode=Soft }} in plan; got {plan:?}",
    );

    let result = db.apply_schema(&desired).await.unwrap();
    assert!(result.supported);
    assert!(result.applied);

    // Manifest advanced; row count unchanged.
    let after_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();
    assert!(
        after_version > before_version,
        "manifest version should advance after soft drop; before={before_version}, after={after_version}",
    );
    assert_eq!(count_rows(&db, "node:Person").await, people_before);

    // (a) Current snapshot: `age` is gone from the dataset schema.
    let current_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let current_ds = current_snapshot.open("node:Person").await.unwrap();
    let current_fields = current_ds
        .schema()
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>();
    assert!(
        !current_fields.iter().any(|f| f == "age"),
        "current Person dataset schema must not include 'age' after soft drop; got fields {current_fields:?}",
    );

    // (b) Time travel: at the pre-drop manifest version, the prior
    // Person dataset version still has `age`. Soft drop is reversible
    // via Lance's version graph until `omnigraph cleanup` runs.
    let pre_drop_snapshot = db.snapshot_at_version(before_version).await.unwrap();
    let pre_drop_ds = pre_drop_snapshot.open("node:Person").await.unwrap();
    let pre_drop_fields = pre_drop_ds
        .schema()
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>();
    assert!(
        pre_drop_fields.iter().any(|f| f == "age"),
        "pre-drop Person dataset schema must still include 'age' (time-travel reversibility); got fields {pre_drop_fields:?}",
    );

    // (c) Reopen consistency: close the engine, reopen, verify the
    // drop is preserved (column still absent from current schema).
    let uri = dir.path().to_str().unwrap().to_string();
    drop(db);
    let reopened = Omnigraph::open(&uri).await.unwrap();
    let reopened_snapshot = reopened
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    let reopened_ds = reopened_snapshot.open("node:Person").await.unwrap();
    let reopened_fields = reopened_ds
        .schema()
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>();
    assert!(
        !reopened_fields.iter().any(|f| f == "age"),
        "after reopen, Person dataset schema must still lack 'age'; got fields {reopened_fields:?}",
    );
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_drops_node_and_referencing_edge_softly() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    // Drop the `Company` node type and the `WorksAt` edge that references it.
    // Per schema-lint v1 chassis commit #4 (MR-694), this emits two
    // `DropType { Soft }` steps; apply tombstones both manifest entries.
    // Lance dataset files are retained, so time-travel back to the
    // pre-drop manifest version still resolves both tables.
    let desired = r#"
node Person {
    name: String @key
    age: I32?
}

edge Knows: Person -> Person {
    since: Date?
}
"#;

    // Confirm the plan emits both DropType { Soft } steps.
    let plan = db.plan_schema(desired).await.unwrap();
    assert!(plan.supported, "drop-type plan must be supported");
    assert!(
        plan.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::DropType {
                type_kind: SchemaTypeKind::Node,
                name,
                mode: omnigraph_compiler::DropMode::Soft,
            } if name == "Company"
        )),
        "expected DropType {{ Node, Company, Soft }} in plan: {plan:?}",
    );
    assert!(
        plan.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::DropType {
                type_kind: SchemaTypeKind::Edge,
                name,
                mode: omnigraph_compiler::DropMode::Soft,
            } if name == "WorksAt"
        )),
        "expected DropType {{ Edge, WorksAt, Soft }} in plan: {plan:?}",
    );

    let result = db.apply_schema(desired).await.unwrap();
    assert!(result.supported);
    assert!(result.applied);

    let after_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();
    assert!(
        after_version > before_version,
        "manifest version should advance after soft type drop; before={before_version}, after={after_version}",
    );

    // (a) Current snapshot: both manifest entries are gone.
    let current_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert!(
        current_snapshot.entry("node:Company").is_none(),
        "current manifest must not list node:Company after soft drop",
    );
    assert!(
        current_snapshot.entry("edge:WorksAt").is_none(),
        "current manifest must not list edge:WorksAt after soft drop",
    );
    // Person + Knows still present (Person wasn't dropped; Knows is in desired).
    assert!(
        current_snapshot.entry("node:Person").is_some(),
        "node:Person must remain in the manifest",
    );

    // (b) Time travel: at the pre-drop manifest version, both dropped
    // tables are still listed. Soft drop is reversible via Lance's
    // version graph until `omnigraph cleanup` runs.
    let pre_drop_snapshot = db.snapshot_at_version(before_version).await.unwrap();
    assert!(
        pre_drop_snapshot.entry("node:Company").is_some(),
        "pre-drop manifest must still list node:Company (time-travel reversibility)",
    );
    assert!(
        pre_drop_snapshot.entry("edge:WorksAt").is_some(),
        "pre-drop manifest must still list edge:WorksAt (time-travel reversibility)",
    );

    // (c) Reopen consistency: drop is preserved across engine restart.
    let uri = dir.path().to_str().unwrap().to_string();
    drop(db);
    let reopened = Omnigraph::open(&uri).await.unwrap();
    let reopened_snapshot = reopened
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    assert!(
        reopened_snapshot.entry("node:Company").is_none(),
        "after reopen, node:Company must still be absent from the current manifest",
    );
    assert!(
        reopened_snapshot.entry("edge:WorksAt").is_none(),
        "after reopen, edge:WorksAt must still be absent from the current manifest",
    );
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_drops_an_edge_type_softly() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    // Drop only the `WorksAt` edge. Per chassis v1 commit #4, this
    // emits `DropType { Edge, WorksAt, Soft }`; apply tombstones the
    // edge:WorksAt manifest entry. The Company node and Person node
    // remain intact.
    let desired = TEST_SCHEMA.replace("\nedge WorksAt: Person -> Company", "");

    let plan = db.plan_schema(&desired).await.unwrap();
    assert!(plan.supported);
    assert!(
        plan.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::DropType {
                type_kind: SchemaTypeKind::Edge,
                name,
                mode: omnigraph_compiler::DropMode::Soft,
            } if name == "WorksAt"
        )),
        "expected DropType {{ Edge, WorksAt, Soft }} in plan: {plan:?}",
    );

    let result = db.apply_schema(&desired).await.unwrap();
    assert!(result.applied);

    let after_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();
    assert!(after_version > before_version);

    let current_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert!(
        current_snapshot.entry("edge:WorksAt").is_none(),
        "current manifest must not list edge:WorksAt",
    );
    // Other tables untouched.
    assert!(current_snapshot.entry("node:Person").is_some());
    assert!(current_snapshot.entry("node:Company").is_some());
    assert!(current_snapshot.entry("edge:Knows").is_some());

    let pre_drop_snapshot = db.snapshot_at_version(before_version).await.unwrap();
    assert!(
        pre_drop_snapshot.entry("edge:WorksAt").is_some(),
        "pre-drop manifest must still list edge:WorksAt",
    );
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_rejects_adding_a_required_property_without_backfill() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    // Add `email: String` (required, non-nullable, no @rename_from). Existing
    // rows have no value to fill in, so this is unsupported in v1.
    let desired = TEST_SCHEMA.replace("    age: I32?\n}", "    age: I32?\n    email: String\n}");
    let err = db.apply_schema(&desired).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("OG-MF-103"),
        "expected schema-lint code OG-MF-103 in error, got: {msg}"
    );
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        before_version
    );
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn plan_schema_for_property_type_narrowing_is_not_supported() {
    // Symmetric companion to `apply_schema_unsupported_plan_does_not_advance_manifest`,
    // which exercises widening (I32 -> I64). Narrowing (I64 -> I32) is also
    // unsupported in v1, and should be flagged at plan time so callers can
    // route to a manual-migration path before invoking apply.
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let initial = TEST_SCHEMA.replace("age: I32?", "age: I64?");
    let mut db = Omnigraph::init(uri, &initial).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    let plan = db.plan_schema(TEST_SCHEMA).await.unwrap();
    assert!(
        !plan.supported,
        "narrowing I64 -> I32 must not be supported"
    );
    assert!(plan.steps.iter().any(|step| matches!(
        step,
        SchemaMigrationStep::UnsupportedChange { code, .. }
            if code.as_deref() == Some("OG-MF-106")
    )));
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_renames_node_type_via_rename_from_and_preserves_rows() {
    // Covers the stable-type-id contract: renaming a type preserves the
    // underlying Lance dataset (by stable id), so existing rows survive the
    // rename and become queryable under the new table key. This is the
    // "supported" half of the destructive-vs-supported boundary that the
    // rejections above cover.
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let people_before = count_rows(&db, "node:Person").await;
    assert!(
        people_before > 0,
        "fixture should seed Person rows for this test to be meaningful"
    );

    // Rename Person -> Human (and the keying property name -> full_name).
    // Edges that referenced Person must update to Human in the same migration.
    let desired = r#"
node Human @rename_from("Person") {
    full_name: String @key @rename_from("name")
    age: I32?
}

node Company {
    name: String @key
}

edge Knows: Human -> Human {
    since: Date?
}

edge WorksAt: Human -> Company
"#;

    let result = db.apply_schema(desired).await.unwrap();
    assert!(result.supported && result.applied);

    // Type rename is emitted as a RenameType step.
    assert!(
        result.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::RenameType {
                type_kind: SchemaTypeKind::Node,
                from,
                to,
            } if from == "Person" && to == "Human"
        )),
        "expected RenameType Person -> Human in {:?}",
        result.steps
    );
    // Property rename rides along under the new type name.
    assert!(
        result.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::RenameProperty {
                type_kind: SchemaTypeKind::Node,
                type_name,
                from,
                to,
            } if type_name == "Human" && from == "name" && to == "full_name"
        )),
        "expected RenameProperty name -> full_name on Human in {:?}",
        result.steps
    );

    // Rows survive: table key now resolves under the new type name and the
    // old key is gone.
    assert_eq!(count_rows(&db, "node:Human").await, people_before);
    assert!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .entry("node:Person")
            .is_none(),
        "old node:Person table key should be unmapped after rename"
    );
}

// ─── Hard-mode drops (chassis v1 commit #5 — --allow-data-loss) ──────────────
//
// Hard mode promotes every `DropMode::Soft` step to `DropMode::Hard` and runs
// `cleanup_old_versions` on affected datasets immediately after the manifest
// publish. For DropProperty Hard, this removes the prior dataset version
// (where the column lived), making `snapshot_at_version(pre_drop)` unable to
// open the dataset at that version. For DropType Hard, the dataset is
// untouched by the schema apply itself (no per-table write), so
// cleanup_old_versions is currently a no-op for it — the dataset directory
// persists. Full orphan-dataset deletion is a separate follow-up.

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_with_allow_data_loss_promotes_drops_to_hard() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let desired = TEST_SCHEMA.replace("    age: I32?\n", "");

    // Default plan (no flag) → Soft.
    let plan_soft = db.plan_schema(&desired).await.unwrap();
    assert!(plan_soft.steps.iter().any(|step| matches!(
        step,
        SchemaMigrationStep::DropProperty {
            mode: omnigraph_compiler::DropMode::Soft,
            ..
        }
    )));

    // With --allow-data-loss → Hard.
    let plan_hard = db
        .plan_schema_with_options(
            &desired,
            omnigraph::db::SchemaApplyOptions {
                allow_data_loss: true,
            },
        )
        .await
        .unwrap();
    assert!(plan_hard.supported);
    assert!(
        plan_hard.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::DropProperty {
                mode: omnigraph_compiler::DropMode::Hard,
                ..
            }
        )),
        "with --allow-data-loss, DropProperty should be promoted to Hard: {plan_hard:?}",
    );
    // Negative: no remaining Soft drops in the promoted plan.
    assert!(
        !plan_hard.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::DropProperty {
                mode: omnigraph_compiler::DropMode::Soft,
                ..
            } | SchemaMigrationStep::DropType {
                mode: omnigraph_compiler::DropMode::Soft,
                ..
            }
        )),
        "promoted plan should have no Soft drops left: {plan_hard:?}",
    );

    // Apply with flag succeeds.
    let result = db
        .apply_schema_with_options(
            &desired,
            omnigraph::db::SchemaApplyOptions {
                allow_data_loss: true,
            },
        )
        .await
        .unwrap();
    assert!(result.applied);
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_hard_drops_property_makes_prior_version_unreachable() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    // Hard drop the `age` column. Soft drop would leave the prior
    // dataset version intact; Hard drop runs cleanup_old_versions on
    // the dataset post-apply, removing the prior version.
    let desired = TEST_SCHEMA.replace("    age: I32?\n", "");
    let result = db
        .apply_schema_with_options(
            &desired,
            omnigraph::db::SchemaApplyOptions {
                allow_data_loss: true,
            },
        )
        .await
        .unwrap();
    assert!(result.applied);

    // Current snapshot: column gone from the dataset schema.
    let current_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let current_ds = current_snapshot.open("node:Person").await.unwrap();
    let current_fields = current_ds
        .schema()
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>();
    assert!(
        !current_fields.iter().any(|f| f == "age"),
        "current Person schema must not include 'age' after hard drop; got {current_fields:?}",
    );

    // Time travel: at the pre-drop manifest version, the entry points
    // at the OLD dataset version which has been cleaned up. Opening
    // the dataset at that snapshot should fail (Lance can't load the
    // dropped version). This is the Hard-mode contract — the prior
    // data is unreachable.
    let pre_drop = db.snapshot_at_version(before_version).await.unwrap();
    let open_result = pre_drop.open("node:Person").await;
    assert!(
        open_result.is_err(),
        "after hard drop + cleanup, pre-drop snapshot.open() must fail (prior version was reclaimed); got {open_result:?}",
    );
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_hard_drops_node_and_edge_with_flag_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    let desired = r#"
node Person {
    name: String @key
    age: I32?
}

edge Knows: Person -> Person {
    since: Date?
}
"#;

    let plan = db
        .plan_schema_with_options(
            desired,
            omnigraph::db::SchemaApplyOptions {
                allow_data_loss: true,
            },
        )
        .await
        .unwrap();
    assert!(plan.supported);
    assert!(
        plan.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::DropType {
                type_kind: SchemaTypeKind::Node,
                mode: omnigraph_compiler::DropMode::Hard,
                ..
            }
        )),
        "with --allow-data-loss, DropType {{ Node }} should be Hard: {plan:?}",
    );
    assert!(
        plan.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::DropType {
                type_kind: SchemaTypeKind::Edge,
                mode: omnigraph_compiler::DropMode::Hard,
                ..
            }
        )),
        "with --allow-data-loss, DropType {{ Edge }} should be Hard: {plan:?}",
    );

    let result = db
        .apply_schema_with_options(
            desired,
            omnigraph::db::SchemaApplyOptions {
                allow_data_loss: true,
            },
        )
        .await
        .unwrap();
    assert!(result.applied);

    let after_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();
    assert!(after_version > before_version);

    // Current manifest: both dropped entries gone.
    let current = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert!(current.entry("node:Company").is_none());
    assert!(current.entry("edge:WorksAt").is_none());

    // NOTE: DropType Hard's cleanup of the orphan dataset directory
    // is a known follow-up (the manifest entry is tombstoned and the
    // dataset's prior versions are cleaned, but the directory itself
    // persists until an orphan-cleanup pass is implemented). For the
    // current contract, the data is *unreachable* via omnigraph
    // (no manifest entry), which is the user-facing guarantee.
}

// Regression (bug 3 / dev-graph iss-848): schema apply records index intent but
// performs no physical index work. That decoupling is load-bearing for a
// `Vector @index` on a 0-row table: Lance cannot train IVF centroids on no
// vectors, yet the logical migration must still succeed. A later
// `ensure_indices` / `optimize` materializes every buildable declaration once
// data exists and reports an untrainable vector column as pending meanwhile.
#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_defers_vector_index_on_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // init does not build indices, so the declared-but-unbuilt vector index
    // sits harmless on the empty table (this is how it survived earlier
    // applies that never touched the table).
    // `slug` is the user @key; omnigraph injects its own internal `id` column,
    // so the key field must not be named `id`.
    let v1 = "node Doc {\n    \
        slug: String @key\n    \
        body: String?\n    \
        embedding: Vector(8) @index\n\
        }\n";
    let mut db = Omnigraph::init(uri, v1).await.unwrap();

    // Add an unrelated scalar @index on `body`. Schema apply must record both
    // declarations without trying to build either one or train the empty vector.
    let v2 = "node Doc {\n    \
        slug: String @key\n    \
        body: String? @index\n    \
        embedding: Vector(8) @index\n\
        }\n";
    let result = db
        .apply_schema(v2)
        .await
        .expect("schema apply must succeed: an empty-table vector @index is deferred, not fatal");
    assert!(result.applied, "the scalar @index change must apply");

    // The deferred declarations are not dropped: after data arrives, the
    // explicit reconciler materializes every buildable index without error.
    load_jsonl(
        &mut db,
        r#"{"type":"Doc","data":{"slug":"d1","body":"hello","embedding":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]}}"#,
        LoadMode::Merge,
    )
    .await
    .expect("loading a Doc with an embedding must succeed");
    db.ensure_indices()
        .await
        .expect("the deferred vector index must build once the table has a trainable vector");
}

// iss-848: adding an `@index` to an existing column is a pure metadata change.
// Schema apply records the intent (the catalog/IR now declares the index) but
// must NOT build the index inline, so the table's data and manifest version are
// untouched. The physical index is materialized later by ensure_indices /
// optimize. Pre-iss-848 the indexed_tables block built the index inline and
// bumped the table version.
#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn index_only_constraint_apply_touches_no_table_data() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let v1 = "node Doc {\n    slug: String @key\n    n: I64\n}\n";
    let mut db = Omnigraph::init(uri, v1).await.unwrap();
    load_jsonl(
        &mut db,
        r#"{"type":"Doc","data":{"slug":"d1","n":1}}"#,
        LoadMode::Merge,
    )
    .await
    .expect("load a Doc");

    let before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry("node:Doc")
        .unwrap()
        .table_version;
    let before_commits = db.list_commits(None).await.unwrap();

    // Add an @index on the existing `n` column.
    let v2 = "node Doc {\n    slug: String @key\n    n: I64 @index\n}\n";
    let result = db
        .apply_schema(v2)
        .await
        .expect("index-only apply must succeed");
    assert!(result.applied, "the @index addition must apply");

    let after = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry("node:Doc")
        .unwrap()
        .table_version;
    assert_eq!(
        before, after,
        "adding an @index must not bump the table version (no inline index build)"
    );
    let after_commits = db.list_commits(None).await.unwrap();
    assert_eq!(
        after_commits.len(),
        before_commits.len() + 1,
        "metadata-only schema apply must still advance graph_head so it arbitrates concurrent prepared writes"
    );
}

// Enum widening (iss-enum-widening-migration): adding variants to an enum is
// a PURE metadata change — the accepted catalog updates, no table data is
// touched, and the widened set is enforced immediately on writes. Narrowing
// stays OG-MF-106-refused.
#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn enum_widening_apply_is_metadata_only_and_accepts_new_variant() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let v1 = "node Ticket {\n    slug: String @key\n    status: enum(todo, doing, done)\n}\n";
    let mut db = Omnigraph::init(uri, v1).await.unwrap();
    load_jsonl(
        &mut db,
        r#"{"type":"Ticket","data":{"slug":"t1","status":"todo"}}"#,
        LoadMode::Merge,
    )
    .await
    .expect("load a Ticket with an original variant");

    let before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry("node:Ticket")
        .unwrap()
        .table_version;
    let before_commits = db.list_commits(None).await.unwrap();

    let v2 =
        "node Ticket {\n    slug: String @key\n    status: enum(todo, doing, done, blocked)\n}\n";
    let result = db.apply_schema(v2).await.expect("enum widening must apply");
    assert!(result.supported, "widening must be a supported plan");
    assert!(result.applied, "widening must apply");

    let after = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry("node:Ticket")
        .unwrap()
        .table_version;
    assert_eq!(
        before, after,
        "enum widening must not bump the table version (metadata-only)"
    );
    let after_commits = db.list_commits(None).await.unwrap();
    assert_eq!(
        after_commits.len(),
        before_commits.len() + 1,
        "metadata-only enum widening must still advance graph_head"
    );

    // The NEW variant is accepted on the write path...
    load_jsonl(
        &mut db,
        r#"{"type":"Ticket","data":{"slug":"t2","status":"blocked"}}"#,
        LoadMode::Merge,
    )
    .await
    .expect("new variant must be accepted after widening");
    // ...an original variant still is...
    load_jsonl(
        &mut db,
        r#"{"type":"Ticket","data":{"slug":"t3","status":"done"}}"#,
        LoadMode::Merge,
    )
    .await
    .expect("original variant must remain accepted");
    // ...and an out-of-set value is still rejected (the fence didn't widen to
    // free text).
    let err = load_jsonl(
        &mut db,
        r#"{"type":"Ticket","data":{"slug":"t4","status":"bogus"}}"#,
        LoadMode::Merge,
    )
    .await;
    assert!(err.is_err(), "out-of-set enum value must still be rejected");
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn enum_narrowing_apply_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let v1 = "node Ticket {\n    slug: String @key\n    status: enum(todo, doing, done)\n}\n";
    let mut db = Omnigraph::init(uri, v1).await.unwrap();

    let narrowed = "node Ticket {\n    slug: String @key\n    status: enum(todo, done)\n}\n";
    let err = db.apply_schema(narrowed).await;
    assert!(err.is_err(), "narrowing must refuse at apply");
    let msg = format!("{}", err.unwrap_err());
    assert!(
        msg.contains("OG-MF-106"),
        "refusal must carry the stable lint code, got: {msg}"
    );

    // The graph stays healthy and writable on the original schema.
    load_jsonl(
        &mut db,
        r#"{"type":"Ticket","data":{"slug":"t1","status":"doing"}}"#,
        LoadMode::Merge,
    )
    .await
    .expect("graph must remain writable after a refused narrowing");
}
