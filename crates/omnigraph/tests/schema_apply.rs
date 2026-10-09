mod helpers;

use base64::Engine;
use lance::index::DatasetIndexExt;
#[cfg(feature = "failpoints")]
use std::sync::Arc;

use omnigraph::db::{
    MergeOutcome, Omnigraph, PreparedSchemaApply, PreparedSchemaSettlement, ReadTarget,
    SchemaApplyReconciliation, SchemaApplySettlement, SchemaNonPublicationProof,
};
use omnigraph::error::{ManifestErrorKind, OmniError};
use omnigraph::loader::LoadMode;
use omnigraph::{BlobContent, ExternalBlobBase, ExternalBlobExecutionScope, ExternalBlobPolicy};
use omnigraph_compiler::{SchemaMigrationStep, SchemaTypeKind};
use omnigraph_core::graph_commit_id::intent_nonce;
use sha2::{Digest, Sha256};

use helpers::*;

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn warmed_wildcard_rechecks_members_after_schema_apply_issue_659() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let source = r#"query neighbor_since() {
        match { $p: Person { name: "Alice" } $f: Person $p $e:* $f }
        return { $f.name, $e.since }
    }"#;
    assert_eq!(
        query_main(&db, source, "neighbor_since", &params(&[]))
            .await
            .unwrap()
            .num_rows(),
        2
    );
    let owner = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    owner
        .apply_schema(&format!("{TEST_SCHEMA}\nedge Likes: Person -> Person\n"))
        .await
        .unwrap();
    let error = query_main(&db, source, "neighbor_since", &params(&[]))
        .await
        .expect_err("new member lacks the property");
    let message = error.to_string();
    assert!(
        message.contains("since")
            && message.contains("every selected edge")
            && message.contains("Likes"),
        "{message}"
    );
}

async fn assert_exact_id_primary_key(db: &Omnigraph, table_key: &str) {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let dataset = snapshot.open_dataset(table_key).await.unwrap();
    let primary_key = dataset
        .schema()
        .unenforced_primary_key()
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        primary_key,
        ["__id"],
        "schema apply must preserve exactly `__id` as the Lance unenforced primary key for {table_key}"
    );
}

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

    let preview = db.preview_schema_apply(&desired).await.unwrap();
    assert_eq!(preview.catalog.node_types.len(), 2);

    let contract = db.schema_contract_digest();
    // The served observational planner uses the accepted in-memory contract;
    // it must neither reopen this root nor wait on a schema gate.
    let parked = dir.path().with_extension("parked");
    std::fs::rename(dir.path(), &parked).unwrap();
    let observed = db.plan_schema_at_contract(&desired, &contract).unwrap();
    std::fs::rename(&parked, dir.path()).unwrap();
    assert_eq!(observed, plan);
    db.apply_schema(&desired).await.unwrap();
    assert!(db.plan_schema_at_contract(&desired, &contract).is_err());
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_interface_evolution_is_identity_only_and_durable() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let mut table_versions_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .datasets()
        .map(|entry| {
            (
                entry.type_key.clone(),
                entry.dataset_path.clone(),
                entry.published_dataset_version,
            )
        })
        .collect::<Vec<_>>();
    table_versions_before.sort();

    let with_interface = format!("interface Named {{\n    name: String\n}}\n\n{TEST_SCHEMA}");
    let added = db.apply_schema(&with_interface).await.unwrap();
    assert!(added.supported && added.applied);
    assert!(added.steps.iter().any(|step| matches!(
        step,
        SchemaMigrationStep::AddType {
            type_kind: SchemaTypeKind::Interface,
            name,
        } if name == "Named"
    )));
    let interface_id = db.catalog().type_id("Named").unwrap();
    let name_property_id = db.catalog().property_id("Named", "name").unwrap();

    let extended_interface =
        format!("interface Named {{\n    name: String\n    alias: String?\n}}\n\n{TEST_SCHEMA}");
    let extended = db.apply_schema(&extended_interface).await.unwrap();
    assert!(extended.supported && extended.applied);
    assert!(extended.steps.iter().any(|step| matches!(
        step,
        SchemaMigrationStep::AddProperty {
            type_kind: SchemaTypeKind::Interface,
            type_name,
            property_name,
            ..
        } if type_name == "Named" && property_name == "alias"
    )));
    assert_eq!(db.catalog().type_id("Named"), Some(interface_id));
    assert_eq!(
        db.catalog().property_id("Named", "name"),
        Some(name_property_id)
    );
    assert!(db.catalog().property_id("Named", "alias").is_some());

    let mut table_versions_after = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .datasets()
        .map(|entry| {
            (
                entry.type_key.clone(),
                entry.dataset_path.clone(),
                entry.published_dataset_version,
            )
        })
        .collect::<Vec<_>>();
    table_versions_after.sort();
    assert_eq!(table_versions_after, table_versions_before);

    let contract_row = omnigraph_catalog::ManifestCoordinator::open(uri)
        .await
        .unwrap()
        .read_schema_contract()
        .await
        .expect("the apply's publish replaced the schema_contract row");
    let accepted_ir = db.catalog().bound_schema_ir().unwrap().clone();
    assert_eq!(
        contract_row.head.schema_ir_hash,
        omnigraph_compiler::schema_ir_hash(&accepted_ir).unwrap()
    );
    assert_eq!(contract_row.source, extended_interface);
    assert_eq!(
        contract_row.ir,
        omnigraph_compiler::schema_ir_pretty_json(&accepted_ir).unwrap()
    );

    drop(db);
    let reopened = Omnigraph::open(uri).await.unwrap();
    assert_eq!(reopened.catalog().type_id("Named"), Some(interface_id));
    assert_eq!(
        reopened.catalog().property_id("Named", "name"),
        Some(name_property_id)
    );
    assert!(reopened.catalog().property_id("Named", "alias").is_some());
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn long_lived_handle_uses_the_schema_catalog_bound_to_its_write_token() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema_owner = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    // Open before the migration: this handle's process-local ArcSwap catalog is
    // intentionally stale after `schema_owner` completes the apply.
    let stale_handle = helpers::session(Omnigraph::open(uri).await.unwrap());

    let desired = format!(
        "{}\nnode Project {{\n    name: String @key\n}}\n",
        TEST_SCHEMA.replace(
            "    age: I32?\n}",
            "    age: I32?\n    nickname: String?\n}",
        )
    );
    schema_owner.apply_schema(&desired).await.unwrap();
    let reopened_after_apply = Omnigraph::open(uri).await.unwrap();
    assert_eq!(reopened_after_apply.catalog().node_types.len(), 3);
    // The same apply exercises both physical schema shapes: Person is rebuilt
    // through a staged overwrite for the added property, while Project is a
    // newly created table incarnation. Neither may drop the immutable v6 PK.
    assert_exact_id_primary_key(&schema_owner, "node:Person").await;
    assert_exact_id_primary_key(&schema_owner, "node:Project").await;
    assert_stable_property_markers(&schema_owner, "node:Person").await;
    assert_stable_property_markers(&schema_owner, "node:Project").await;

    let projects = stale_handle
        .query(
            ReadTarget::branch("main"),
            "query projects() { match { $p: Project } return { $p.name } }",
            "projects",
            &params(&[]),
        )
        .await
        .expect("read capture must refresh the manifest before joining the promoted SchemaIR");
    assert_eq!(projects.num_rows(), 0);

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
    let project_uri = helpers::collector::table_uri(&stale_handle, "Project").await;
    let project_published = |snapshot: omnigraph::db::Snapshot| {
        snapshot
            .dataset("node:Project")
            .unwrap()
            .published_dataset_version
    };
    let project_before_indices = project_published(
        stale_handle
            .snapshot_of(ReadTarget::branch("source"))
            .await
            .unwrap(),
    );
    let detached_before_indices = helpers::collector::detached_versions(&project_uri).await;
    stale_handle
        .ensure_indices_on("source")
        .await
        .expect("index planning must use the same token-bound post-apply catalog");
    let project_after_indices = project_published(
        stale_handle
            .snapshot_of(ReadTarget::branch("source"))
            .await
            .unwrap(),
    );
    let detached_after_indices = helpers::collector::detached_versions(&project_uri).await;
    assert!(
        project_after_indices > project_before_indices
            || detached_after_indices != detached_before_indices,
        "the stale handle must discover and build Project's declared key index, publishing a higher pin and a new detached commit on the inherited location: {project_before_indices} -> {project_after_indices}, {detached_before_indices:?} -> {detached_after_indices:?}"
    );
    assert_eq!(
        stale_handle
            .branch_merge("source", "target")
            .await
            .expect("merge planning must use the schema-gated post-apply catalog")
            .outcome,
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
    use omnigraph::seams::catalog;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema_owner = helpers::session(Omnigraph::init(uri, TEST_SCHEMA).await.unwrap());
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
        helpers::failpoint::Rendezvous::park_first(&catalog::BRANCH_DELETE_POST_TABLE_GATES);
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
    use omnigraph::seams::catalog;

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
        helpers::failpoint::Rendezvous::park_first(&catalog::MUTATION_POST_STAGE_PRE_EFFECT_GATE);
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

    let schema_rv =
        helpers::failpoint::Rendezvous::park_first(&catalog::SCHEMA_APPLY_POST_LOCK_PRE_EFFECT);
    let schema_db = Arc::clone(&db);
    let schema_task = tokio::spawn(async move { schema_db.apply_schema(&desired).await });
    schema_rv.wait_until_reached().await;

    mutation_rv.release();
    // Give the already-runnable mutation repeated scheduler turns. It must stay
    // pending on the schema gate — its SHARED permit parks behind the apply's
    // held EXCLUSIVE permit (RFC 2026-09-18-shared-schema-gate); completing
    // here means it either advanced under an in-flight migration or returned
    // a spurious post-prepare failure.
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

/// A writer holds its shared schema permit through publication.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn parked_writer_blocks_schema_apply() {
    use omnigraph::seams::catalog;

    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(init_and_load(&dir).await);
    let desired = TEST_SCHEMA.replace("    age: I32?\n}", "    age: I32?\n    motto: String?\n}");

    let in_envelope =
        helpers::failpoint::Rendezvous::park_first(&catalog::MUTATION_POST_FINALIZE_PRE_PUBLISHER);
    let writer_db = Arc::clone(&db);
    let writer = tokio::spawn(async move {
        writer_db
            .mutate(
                "main",
                MUTATION_QUERIES,
                "insert_person",
                &mixed_params(&[("$name", "gate-holder")], &[("$age", 27)]),
            )
            .await
    });
    in_envelope.wait_until_reached().await;

    let post_gate =
        helpers::failpoint::Rendezvous::park_first(&catalog::SCHEMA_APPLY_POST_LOCK_PRE_EFFECT);
    let schema_db = Arc::clone(&db);
    let schema_task = tokio::spawn(async move { schema_db.apply_schema(&desired).await });
    // Wall time: an apply past the gate makes store requests before the seam.
    let crossed = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !post_gate.reached() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        crossed.is_err(),
        "schema apply must wait behind a writer's held shared schema permit; it \
         crossed its effect gate while the writer was parked",
    );
    assert!(!schema_task.is_finished());

    in_envelope.release();
    writer
        .await
        .unwrap()
        .expect("the parked writer must publish after release");
    post_gate.wait_until_reached().await;
    post_gate.release();
    schema_task
        .await
        .unwrap()
        .expect("schema apply must complete once the writer's envelope releases");
    assert_eq!(count_rows(&db, "node:Person").await, 5);
}

/// Read-only opens hold the schema gate through coherent catalog capture.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn read_only_open_holds_schema_gate_through_catalog_capture() {
    use omnigraph::seams::catalog;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let owner = Arc::new(init_and_load(&dir).await);
    let original_contract = owner.schema_contract_digest();
    let desired = TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );

    let open_rv =
        helpers::failpoint::Rendezvous::park_first(&catalog::OPEN_BEFORE_SCHEMA_CONTRACT_READ);
    let open_uri = uri.clone();
    let open_task = tokio::spawn(async move { Omnigraph::open_read_only(&open_uri).await });
    open_rv.wait_until_reached().await;

    let apply_rv =
        helpers::failpoint::Rendezvous::park_first(&catalog::SCHEMA_APPLY_POST_LOCK_PRE_EFFECT);
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
    assert_eq!(opened.schema_contract_digest(), original_contract);
    assert!(
        !opened.catalog().node_types["Person"]
            .properties
            .contains_key("nickname"),
        "the serialized open must publish the complete pre-apply catalog"
    );
    apply_rv.wait_until_reached().await;
    apply_rv.release();
    let applied = apply_task.await.unwrap().unwrap();
    let current = Omnigraph::open_read_only(&uri).await.unwrap();
    assert_eq!(current.schema_contract_digest(), applied.contract);
    assert_ne!(current.schema_contract_digest(), original_contract);
    assert_eq!(
        opened.schema_contract_digest(),
        original_contract,
        "the getter is coherent handle-local evidence, not a hidden refresh"
    );
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
    use omnigraph::seams::catalog;

    let dir = tempfile::tempdir().unwrap();
    let owner = Arc::new(init_and_load(&dir).await);
    let stale = Arc::new(Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap());
    let desired = TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    );

    let reload_rv =
        helpers::failpoint::Rendezvous::park_first(&catalog::SCHEMA_RELOAD_BEFORE_CONTRACT_READ);
    let refresh_handle = Arc::clone(&stale);
    let refresh_task = tokio::spawn(async move { refresh_handle.refresh().await });
    reload_rv.wait_until_reached().await;

    let apply_rv =
        helpers::failpoint::Rendezvous::park_first(&catalog::SCHEMA_APPLY_POST_LOCK_PRE_EFFECT);
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
async fn plan_schema_on_unrefreshed_handle_plans_against_the_applied_contract() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema_owner = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let stale_handle = Omnigraph::open(uri).await.unwrap();

    let with_nickname = TEST_SCHEMA.replace("age: I32?", "age: I32?\n    nickname: String?");
    schema_owner.apply_schema(&with_nickname).await.unwrap();

    let plan = stale_handle.plan_schema(TEST_SCHEMA).await.unwrap();
    assert!(
        plan.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::DropProperty { type_name, property_name, .. }
                if type_name == "Person" && property_name == "nickname"
        )),
        "an unrefreshed handle plans against the applied contract, so the original schema drops the added property: {:?}",
        plan.steps
    );
    assert!(
        stale_handle
            .plan_schema(&with_nickname)
            .await
            .unwrap()
            .steps
            .is_empty()
    );
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_noop_returns_not_applied() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = init_and_load(&dir).await;
    let before = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let before_commits = db.list_commits(None).await.unwrap();
    let before_contract = omnigraph_catalog::ManifestCoordinator::read_schema_contract_at(
        uri,
        None,
        before.graph_manifest_version(),
    )
    .await
    .unwrap();

    let result = db.apply_schema(TEST_SCHEMA).await.unwrap();
    assert!(result.supported);
    assert!(!result.applied);
    assert!(result.steps.is_empty());
    assert!(result.commit.is_none());
    assert_eq!(
        result.contract.source_hash,
        format!("{:x}", Sha256::digest(TEST_SCHEMA.as_bytes()))
    );
    assert_eq!(
        result.contract.schema_ir_hash,
        before_contract.head.schema_ir_hash
    );
    assert_eq!(
        result.graph_manifest_version,
        before.graph_manifest_version()
    );
    assert_eq!(db.list_commits(None).await.unwrap(), before_commits);
    assert_eq!(db.schema_contract_digest(), result.contract);

    // Source bytes are part of the accepted contract even when the typed schema
    // has no migration steps. This Rust owner checks publication and table pins,
    // which the query-level logic-test format does not expose.
    let desired = format!("// Deployment source revision\n{TEST_SCHEMA}\n");
    assert!(db.plan_schema(&desired).await.unwrap().steps.is_empty());
    let changed = db.apply_schema(&desired).await.unwrap();
    assert!(changed.supported && changed.applied);
    assert!(changed.steps.is_empty());
    assert_eq!(
        changed.commit.as_ref().unwrap().parent_commit_id.as_deref(),
        Some(before_commits[0].graph_commit_id.as_str())
    );
    assert_eq!(
        changed.contract.source_hash,
        format!("{:x}", Sha256::digest(desired.as_bytes()))
    );
    let after = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(
        after.graph_manifest_version(),
        before.graph_manifest_version() + 1
    );
    assert_eq!(
        changed.graph_manifest_version,
        after.graph_manifest_version()
    );
    assert_eq!(
        db.list_commits(None).await.unwrap().len(),
        before_commits.len() + 1
    );
    assert_eq!(after.datasets().count(), before.datasets().count());
    for entry in before.datasets() {
        assert!(entry.same_registration(after.dataset(&entry.type_key).unwrap()));
    }
    let after_contract = omnigraph_catalog::ManifestCoordinator::read_schema_contract_at(
        uri,
        None,
        after.graph_manifest_version(),
    )
    .await
    .unwrap();
    assert_eq!(after_contract.source, desired);
    assert_eq!(after_contract.ir, before_contract.ir);
    assert_eq!(after_contract.head, before_contract.head);
    assert_eq!(
        changed.contract.schema_ir_hash,
        after_contract.head.schema_ir_hash
    );
    assert_eq!(
        changed.contract.schema_identity_domain,
        after_contract.head.schema_identity_domain
    );
    assert_eq!(
        changed.contract.schema_identity_version,
        after_contract.head.schema_identity_version
    );
    assert_eq!(db.schema_source().as_str(), desired);
    assert_eq!(db.schema_contract_digest(), changed.contract);

    let reopened = Omnigraph::open(uri).await.unwrap();
    assert_eq!(reopened.schema_source().as_str(), desired);
    let repeated = reopened.apply_schema(&desired).await.unwrap();
    assert!(!repeated.applied);
    assert!(repeated.commit.is_none());
    assert_eq!(repeated.contract, changed.contract);
    assert_eq!(reopened.schema_contract_digest(), changed.contract);
    assert_eq!(
        repeated.graph_manifest_version,
        after.graph_manifest_version()
    );
    assert_eq!(
        reopened.list_commits(None).await.unwrap(),
        db.list_commits(None).await.unwrap()
    );
    reopened.branch_create("feature").await.unwrap();
    assert_eq!(
        Omnigraph::open_read_only(uri)
            .await
            .unwrap()
            .schema_contract_digest(),
        changed.contract,
        "reading accepted contract identity does not require a single branch"
    );
}

fn schema_storage_bytes(
    root: &std::path::Path,
) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn visit(
        root: &std::path::Path,
        path: &std::path::Path,
        files: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>,
    ) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                visit(root, &path, files);
            } else {
                files.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut files = std::collections::BTreeMap::new();
    visit(root, root, &mut files);
    files
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn prepared_schema_receipt_reconciles_its_own_publication_after_restart_and_later_write() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = init_and_load(&dir).await;
    let before = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let predecessor = db.list_commits(None).await.unwrap()[0].clone();
    let files_before = schema_storage_bytes(dir.path());
    let desired = TEST_SCHEMA.replace("age: I32?", "age: I32?\n    nickname: String?");
    let (prepared, migration) = db
        .prepare_schema_apply_with_plan_as(&desired, Some("deployer"))
        .await
        .unwrap();
    assert!(migration.supported);
    assert!(migration.steps.iter().any(|step| matches!(
        step,
        SchemaMigrationStep::AddProperty { type_name, property_name, .. }
            if type_name == "Person" && property_name == "nickname"
    )));
    assert!(!prepared.is_noop());
    assert_eq!(prepared.actor(), Some("deployer"));
    assert_eq!(
        prepared.base_manifest_version(),
        before.graph_manifest_version()
    );
    let commit_id = prepared.graph_commit_id().unwrap().to_string();
    let prepared: PreparedSchemaApply =
        serde_json::from_slice(&serde_json::to_vec(&prepared).unwrap()).unwrap();
    assert_eq!(
        schema_storage_bytes(dir.path()),
        files_before,
        "preparation must not stage or publish anything"
    );
    assert!(matches!(
        db.reconcile_schema_apply_as(&prepared, Some("deployer"))
            .await
            .unwrap(),
        SchemaApplyReconciliation::Unknown
    ));
    assert_eq!(
        schema_storage_bytes(dir.path()),
        files_before,
        "missing evidence must not trigger execution or cleanup"
    );
    drop(db);

    let db = helpers::session(Omnigraph::open(uri).await.unwrap());
    let result = db
        .apply_prepared_schema_as(&prepared, Some("deployer"))
        .await
        .unwrap();
    let commit = result.commit.as_ref().unwrap();
    assert!(result.applied);
    assert_eq!(result.steps, migration.steps);
    assert_eq!(
        intent_nonce(&commit.graph_commit_id).unwrap(),
        commit_id,
        "the published ID carries the intent nonce"
    );
    assert_eq!(
        commit.parent_commit_id.as_deref(),
        Some(predecessor.graph_commit_id.as_str())
    );
    assert_eq!(commit.actor_id.as_deref(), Some("deployer"));
    assert_eq!(commit.graph_manifest_version, result.graph_manifest_version);
    assert_eq!(&result.contract, prepared.desired_contract());
    assert_eq!(
        db.get_commit(&commit.graph_commit_id).await.unwrap(),
        *commit
    );
    db.load_jsonl(
        r#"{"type":"Person","data":{"name":"After publication"}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    assert_ne!(
        db.list_commits(None).await.unwrap()[0].graph_commit_id,
        commit_id
    );
    drop(db);

    let files_after = schema_storage_bytes(dir.path());
    let observer = Omnigraph::open_read_only(uri).await.unwrap();
    match observer
        .reconcile_schema_apply_as(&prepared, Some("deployer"))
        .await
        .unwrap()
    {
        SchemaApplyReconciliation::Committed {
            commit: found,
            contract,
        } => {
            assert_eq!(found, *commit);
            assert_eq!(contract, result.contract);
        }
        other => panic!("expected exact committed evidence, got {other:?}"),
    }
    assert_eq!(observer.schema_source().as_str(), desired);
    assert_eq!(
        schema_storage_bytes(dir.path()),
        files_after,
        "read-only reconciliation must change no object bytes or inventory"
    );
    drop(observer);

    // Model loss of the exact retained publication version. Current cleanup
    // prunes table pins but keeps catalog manifests; deleting this precise
    // metadata object makes the evidence loss explicit without changing HEAD.
    let published = lance::Dataset::open(&format!("{uri}/__manifest"))
        .await
        .unwrap()
        .checkout_version(commit.graph_manifest_version)
        .await
        .unwrap();
    published
        .object_store(None)
        .await
        .unwrap()
        .delete(&published.manifest_location().path)
        .await
        .unwrap();
    let db = Omnigraph::open_read_only(uri).await.unwrap();
    let after_prune = schema_storage_bytes(dir.path());
    assert!(matches!(
        db.reconcile_schema_apply_as(&prepared, Some("deployer"))
            .await
            .unwrap(),
        SchemaApplyReconciliation::Unknown
    ));
    assert_eq!(
        schema_storage_bytes(dir.path()),
        after_prune,
        "pruned publication evidence must remain unknown without recreating it"
    );
}

#[cfg(feature = "failpoints")]
#[tokio::test]
#[serial_test::serial]
async fn prepared_schema_reconciliation_distinguishes_unpublished_effects_from_lost_acknowledgement()
 {
    use omnigraph::seams::catalog;

    // The crash matrix owns table durability; this test owns the prepared
    // operation's identity and read-only positive-evidence classification.
    for (seam, published) in [
        (&catalog::SCHEMA_APPLY_POST_TABLE_COMMIT, false),
        (&catalog::SCHEMA_APPLY_AFTER_MANIFEST_COMMIT, true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let db = init_and_load(&dir).await;
        let before = db.list_commits(None).await.unwrap();
        let desired = TEST_SCHEMA.replace("age: I32?", "age: I32?\n    nickname: String?");
        let prepared = db.prepare_schema_apply_as(&desired, None).await.unwrap();
        let error = {
            let _failure = seam.fail_once_at(1);
            db.apply_prepared_schema_as(&prepared, None)
                .await
                .unwrap_err()
        };
        assert!(
            error.to_string().contains(seam.name()),
            "expected reached fault {}, got {error}",
            seam.name()
        );
        if published {
            assert!(
                matches!(&error, OmniError::RecoveryRequired { operation_id, .. } if intent_nonce(operation_id).unwrap() == prepared.graph_commit_id().unwrap())
            );
        }
        let files = schema_storage_bytes(dir.path());
        let cold = Omnigraph::open_read_only(uri).await.unwrap();
        for observer in [db.db().as_ref(), &cold] {
            match observer
                .reconcile_schema_apply_as(&prepared, None)
                .await
                .unwrap()
            {
                SchemaApplyReconciliation::Committed { commit, contract } if published => {
                    assert_eq!(
                        intent_nonce(&commit.graph_commit_id).unwrap(),
                        prepared.graph_commit_id().unwrap()
                    );
                    assert_eq!(
                        commit.parent_commit_id.as_deref(),
                        Some(before[0].graph_commit_id.as_str())
                    );
                    assert_eq!(&contract, prepared.desired_contract());
                }
                SchemaApplyReconciliation::Unknown if !published => {
                    assert_eq!(observer.list_commits(None).await.unwrap(), before);
                }
                other => panic!("wrong reconciliation at {}: {other:?}", seam.name()),
            }
        }
        assert_eq!(schema_storage_bytes(dir.path()), files);
        let fence = db
            .prepare_schema_settlement_as(&prepared, Some("recovery"))
            .await
            .unwrap();
        let result = if published {
            db.settle_prepared_schema_as(&prepared, &fence, Some("recovery"))
                .await
                .unwrap()
        } else {
            // Lose both the neutral publication acknowledgement and the
            // publisher's immediate readback; settlement uses the persisted
            // fence identity to recover its own exact receipt.
            let _ack = catalog::PUBLISH_POST_MERGE_PRE_ACK.fail_once_at(1);
            let _readback = catalog::PUBLISH_READ_BACK.fail_once_at(1);
            db.settle_prepared_schema_as(&prepared, &fence, Some("recovery"))
                .await
                .unwrap()
        };
        match result {
            SchemaApplySettlement::Committed { commit, .. } if published => {
                assert_eq!(
                    intent_nonce(&commit.graph_commit_id).unwrap(),
                    prepared.graph_commit_id().unwrap()
                );
            }
            SchemaApplySettlement::NotPublished {
                proof: SchemaNonPublicationProof::Fence { commit, .. },
            } if !published => {
                assert_eq!(
                    intent_nonce(&commit.graph_commit_id).unwrap(),
                    fence.graph_commit_id().unwrap()
                );
                assert_eq!(db.schema_source().as_str(), TEST_SCHEMA);
                assert!(db.apply_prepared_schema_as(&prepared, None).await.is_err());
            }
            other => panic!("wrong fault settlement: {other:?}"),
        }
    }
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn prepared_schema_rejects_malformed_serialized_intents_before_effects() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let desired = TEST_SCHEMA.replace("age: I32?", "age: I32?\n    nickname: String?");
    let prepared = db
        .prepare_schema_apply_as(&desired, Some("deployer"))
        .await
        .unwrap();
    let before = schema_storage_bytes(dir.path());
    let commits = db.list_commits(None).await.unwrap();
    assert!(commits.len() >= 2);
    for actor in [None, Some("different-deployer")] {
        assert!(db.apply_prepared_schema_as(&prepared, actor).await.is_err());
        assert!(
            db.reconcile_schema_apply_as(&prepared, actor)
                .await
                .is_err()
        );
        assert_eq!(schema_storage_bytes(dir.path()), before);
    }
    for (pointer, replacement, actor) in [
        ("/version", serde_json::json!(1), "deployer"),
        (
            "/desired_contract/source_hash",
            serde_json::json!("00".repeat(32)),
            "deployer",
        ),
        (
            "/desired_contract/schema_ir_hash",
            serde_json::json!("00".repeat(32)),
            "deployer",
        ),
        ("/actor", serde_json::json!("substituted"), "substituted"),
        (
            "/lineage/graph_commit_id",
            serde_json::json!(commits[0].graph_commit_id),
            "deployer",
        ),
        (
            "/lineage/graph_commit_id",
            serde_json::json!(commits[1].graph_commit_id),
            "deployer",
        ),
        (
            "/lineage/graph_commit_id",
            serde_json::json!("not-a-commit-id"),
            "deployer",
        ),
    ] {
        let mut encoded = serde_json::to_value(&prepared).unwrap();
        *encoded
            .pointer_mut(pointer)
            .expect("intent field must exist") = replacement;
        let altered: PreparedSchemaApply = serde_json::from_value(encoded).unwrap();
        assert!(
            db.apply_prepared_schema_as(&altered, Some(actor))
                .await
                .is_err(),
            "tampering {pointer} must refuse execution"
        );
        assert_eq!(
            schema_storage_bytes(dir.path()),
            before,
            "tampering {pointer} must have no effects"
        );
    }
    let fence = db
        .prepare_schema_settlement_as(&prepared, Some("recovery"))
        .await
        .unwrap();
    for (pointer, replacement) in [
        ("/version", serde_json::json!(1)),
        ("/original_digest", serde_json::json!("00".repeat(32))),
        (
            "/lineage/graph_commit_id",
            serde_json::json!(prepared.graph_commit_id().unwrap()),
        ),
        (
            "/lineage/graph_commit_id",
            serde_json::json!(commits[1].graph_commit_id),
        ),
        ("/lineage/graph_commit_id", serde_json::json!("invalid")),
    ] {
        let mut encoded = serde_json::to_value(&fence).unwrap();
        *encoded.pointer_mut(pointer).unwrap() = replacement;
        let altered: PreparedSchemaSettlement = serde_json::from_value(encoded).unwrap();
        assert!(
            db.settle_prepared_schema_as(&prepared, &altered, Some("recovery"))
                .await
                .is_err(),
            "tampering {pointer} must refuse settlement"
        );
        assert_eq!(
            schema_storage_bytes(dir.path()),
            before,
            "invalid settlement must publish nothing"
        );
    }
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn prepared_schema_rejects_a_different_root_and_stale_predecessor_without_effects() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let desired = TEST_SCHEMA.replace("age: I32?", "age: I32?\n    nickname: String?");
    let prepared = db.prepare_schema_apply_as(&desired, None).await.unwrap();
    let settlement = db
        .prepare_schema_settlement_as(&prepared, None)
        .await
        .unwrap();
    let other_dir = tempfile::tempdir().unwrap();
    // A byte-for-byte clone has the same schema domain, native ref identity and
    // predecessor. Only the canonical root distinguishes it from this intent.
    for (relative, bytes) in schema_storage_bytes(dir.path()) {
        let path = other_dir.path().join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }
    let other = Omnigraph::open(other_dir.path().to_str().unwrap())
        .await
        .unwrap();
    let other_before = schema_storage_bytes(other_dir.path());
    assert!(
        other
            .apply_prepared_schema_as(&prepared, None)
            .await
            .is_err()
    );
    assert!(
        other
            .reconcile_schema_apply_as(&prepared, None)
            .await
            .is_err()
    );
    assert_eq!(schema_storage_bytes(other_dir.path()), other_before);

    db.load_jsonl(
        r#"{"type":"Person","data":{"name":"Intervening writer"}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let winner = db.list_commits(None).await.unwrap();
    let files_before = schema_storage_bytes(dir.path());
    assert!(db.apply_prepared_schema_as(&prepared, None).await.is_err());
    assert!(matches!(
        db.reconcile_schema_apply_as(&prepared, None).await.unwrap(),
        SchemaApplyReconciliation::Unknown
    ));
    assert_eq!(db.list_commits(None).await.unwrap(), winner);
    assert_eq!(db.schema_source().as_str(), TEST_SCHEMA);
    assert_eq!(
        schema_storage_bytes(dir.path()),
        files_before,
        "stale intent cannot rebase, stage tables, or rewrite the winner"
    );

    // Foreign replacement at the same main version must invalidate the held
    // engine's cached contract, even though the canonical root is unchanged.
    let noop = db.prepare_schema_apply_as(TEST_SCHEMA, None).await.unwrap();
    let replacement_dir = tempfile::tempdir().unwrap();
    let replacement = init_and_load(&replacement_dir).await;
    replacement
        .load_jsonl(
            r#"{"type":"Person","data":{"name":"Replacement graph"}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
    assert_eq!(
        replacement
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .graph_manifest_version(),
        noop.base_manifest_version()
    );
    let replacement_bytes = schema_storage_bytes(replacement_dir.path());
    drop(replacement);
    std::fs::remove_dir_all(dir.path()).unwrap();
    std::fs::create_dir(dir.path()).unwrap();
    for (relative, bytes) in &replacement_bytes {
        let path = dir.path().join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }
    assert!(db.apply_prepared_schema_as(&noop, None).await.is_err());
    assert_eq!(
        db.settle_prepared_schema_as(&prepared, &settlement, None)
            .await
            .unwrap(),
        SchemaApplySettlement::Unknown,
        "a replacement root's occupied version is not evidence for the old original"
    );
    assert!(matches!(
        db.reconcile_schema_apply_as(&noop, None).await.unwrap(),
        SchemaApplyReconciliation::Unknown
    ));
    assert_eq!(
        schema_storage_bytes(dir.path()),
        replacement_bytes,
        "an old no-op intent cannot certify or modify a replacement graph at the same version"
    );
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn prepared_schema_noop_is_bound_to_exact_source_and_live_authority() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let prepared = db.prepare_schema_apply_as(TEST_SCHEMA, None).await.unwrap();
    assert!(prepared.is_noop());
    assert!(prepared.graph_commit_id().is_none());
    let head = db.list_commits(None).await.unwrap()[0].clone();
    let before = schema_storage_bytes(dir.path());
    let result = db.apply_prepared_schema_as(&prepared, None).await.unwrap();
    assert!(!result.applied);
    assert!(result.commit.is_none());
    assert_eq!(&result.contract, prepared.desired_contract());
    match db.reconcile_schema_apply_as(&prepared, None).await.unwrap() {
        SchemaApplyReconciliation::NoOp {
            graph_manifest_version,
            head_commit_id,
            contract,
        } => {
            assert_eq!(graph_manifest_version, prepared.base_manifest_version());
            assert_eq!(
                head_commit_id.as_deref(),
                Some(head.graph_commit_id.as_str())
            );
            assert_eq!(contract, result.contract);
        }
        other => panic!("expected no-op certificate, got {other:?}"),
    }
    assert_eq!(schema_storage_bytes(dir.path()), before);

    // Native branch creation leaves main's version/head untouched. The
    // single-live-branch restriction still must be rechecked for no-op proof.
    db.branch_create("feature").await.unwrap();
    let branched = schema_storage_bytes(dir.path());
    assert!(matches!(
        db.reconcile_schema_apply_as(&prepared, None).await.unwrap(),
        SchemaApplyReconciliation::Unknown
    ));
    assert!(db.apply_prepared_schema_as(&prepared, None).await.is_err());
    assert_eq!(schema_storage_bytes(dir.path()), branched);
    db.branch_delete("feature").await.unwrap();

    // The schema bytes still match after this write, but the captured authority
    // does not. Reconciliation must not invent a fresh no-op certificate.
    db.load_jsonl(
        r#"{"type":"Person","data":{"name":"Later head"}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let after = schema_storage_bytes(dir.path());
    assert!(matches!(
        db.reconcile_schema_apply_as(&prepared, None).await.unwrap(),
        SchemaApplyReconciliation::Unknown
    ));
    assert!(db.apply_prepared_schema_as(&prepared, None).await.is_err());
    assert_eq!(schema_storage_bytes(dir.path()), after);
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_rejects_when_non_main_branch_exists() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
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
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .graph_manifest_version();

    let desired = TEST_SCHEMA.replace("age: I32?", "age: I64?");
    let err = db.apply_schema(&desired).await.unwrap_err();
    assert!(err.to_string().contains("changing property type"));
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .graph_manifest_version(),
        before_version
    );
}

// ─── Destructive / safety-tier behavior ──────────────────────────────────────
//
// Schema migration v1 accepts:
// - Additive change: add type, add nullable property, add index, rename.
// - DropProperty via the schema-lint v1 chassis (commit #3 of MR-694)
//   and DropType: retention is stated on `SchemaMigrationStep::DropType`
//   and `DropProperty`; see the cleanup tests below.
//
// Every other destructive shape (narrow type, add required without
// backfill, remove constraint) still returns an `UnsupportedChange` step that
// surfaces as an error from `apply_schema`. These tests pin the current
// contract so a regression in the planner can't silently change behavior.

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_drops_a_nullable_property_and_preserves_prior_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let external_dir = tempfile::tempdir().unwrap();
    let external_path = external_dir.path().join("external.bin");
    std::fs::write(&external_path, b"External").unwrap();
    let external_uri = format!("file://{}", external_path.display());
    let canonical_external_uri =
        url::Url::from_file_path(std::fs::canonicalize(&external_path).unwrap())
            .expect("canonical external Blob path is absolute")
            .to_string();
    let external_policy = ExternalBlobPolicy::allow(vec![
        ExternalBlobBase::new(
            url::Url::from_directory_path(external_dir.path())
                .expect("external blob base is absolute"),
            ExternalBlobExecutionScope::EmbeddedOnly,
        )
        .unwrap(),
    ])
    .unwrap();
    let initial = r#"
node Document {
    title: String @key
    content: Blob?
    note: String?
}
"#;
    let db = helpers::session(
        Omnigraph::init(uri, initial)
            .await
            .unwrap()
            .with_external_blob_policy(external_policy)
            .unwrap(),
    );
    let data = [
        serde_json::json!({
            "type": "Document",
            "data": {
                "title": "valid-empty",
                "content": "base64:",
                "note": "drop me",
            },
        }),
        serde_json::json!({
            "type": "Document",
            "data": {
                "title": "neighbor",
                "content": "base64:TmVpZ2hib3I=",
                "note": "drop me too",
            },
        }),
        serde_json::json!({
            "type": "Document",
            "data": {
                "title": "external",
                "content": external_uri,
                "note": "drop me three",
            },
        }),
        serde_json::json!({
            "type": "Document",
            "data": {"title": "null", "content": null, "note": "drop me four"},
        }),
        serde_json::json!({
            "type": "Document",
            "data": {
                "title": "packed",
                "content": format!(
                    "base64:{}",
                    base64::engine::general_purpose::STANDARD.encode(vec![b'p'; 96 * 1024])
                ),
                "note": "drop me five",
            },
        }),
    ]
    .into_iter()
    .map(|row| row.to_string())
    .collect::<Vec<_>>()
    .join("\n");
    db.load_jsonl(&data, LoadMode::Overwrite).await.unwrap();

    // Admission policy is not durable graph data. Reopen with the default
    // deny policy so the rewrite proves that a historical descriptor is
    // carried without re-authorizing or probing its caller-owned target.
    let db = Omnigraph::open(uri).await.unwrap();

    let documents_before = count_rows(&db, "node:Document").await;
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .graph_manifest_version();

    // Drop `note` from Document. v1 + chassis commit #3 emit
    // `DropProperty`; the rewrite path projects to the
    // target schema (no `note`), commits via stage_overwrite. Row
    // counts are unchanged — only the column is dropped from the
    // current schema view.
    let desired = initial.replace("    note: String?\n", "");

    // Confirm the plan emits DropProperty (not UnsupportedChange).
    let plan = db.plan_schema(&desired).await.unwrap();
    assert!(plan.supported, "drop-property plan must be supported");
    assert!(
        plan.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::DropProperty {
                type_kind: SchemaTypeKind::Node,
                type_name,
                property_name,
            } if type_name == "Document" && property_name == "note"
        )),
        "expected DropProperty {{ type=Document, property=note }} in plan; got {plan:?}",
    );

    // An unrelated schema rewrite carries the descriptor, not the external
    // payload. The caller-owned target may be unavailable without blocking
    // schema evolution.
    std::fs::remove_file(&external_path).unwrap();
    let probes = omnigraph::instrumentation::MergeWriteProbes::default();
    let result = omnigraph::instrumentation::with_merge_write_probes(
        probes.clone(),
        db.apply_schema(&desired),
    )
    .await
    .unwrap();
    assert!(result.supported);
    assert!(result.applied);
    // Three managed values (valid empty, inline, packed) in one batched read.
    assert_eq!(probes.blob_managed_batch_read_calls(), 1);
    assert_eq!(probes.blob_payload_read_calls(), 3);
    assert_eq!(probes.external_blob_payload_read_calls(), 0);
    assert_exact_id_primary_key(&db, "node:Document").await;

    // Manifest advanced; row count unchanged.
    let after_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .graph_manifest_version();
    assert!(
        after_version > before_version,
        "manifest version should advance after the drop; before={before_version}, after={after_version}",
    );
    assert_eq!(count_rows(&db, "node:Document").await, documents_before);

    let empty = read_managed_blob_bytes(
        &db,
        ReadTarget::branch("main"),
        node_blob_cell("Document", "valid-empty", "content"),
    )
    .await;
    assert!(empty.is_empty());
    let neighbor = read_managed_blob_bytes(
        &db,
        ReadTarget::branch("main"),
        node_blob_cell("Document", "neighbor", "content"),
    )
    .await;
    assert_eq!(&neighbor[..], b"Neighbor");
    let packed = read_managed_blob_bytes(
        &db,
        ReadTarget::branch("main"),
        node_blob_cell("Document", "packed", "content"),
    )
    .await;
    assert!(packed == vec![b'p'; 96 * 1024], "the packed value changed");
    let null = db
        .read_blob_at(
            ReadTarget::branch("main"),
            node_blob_cell("Document", "null", "content"),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(null, OmniError::Manifest(ref error) if error.kind == ManifestErrorKind::NotFound),
        "the null cell stays null, got {null:?}"
    );
    let external = db
        .read_blob_at(
            ReadTarget::branch("main"),
            node_blob_cell("Document", "external", "content"),
        )
        .await
        .unwrap();
    match external.content {
        BlobContent::External(reference) => {
            assert_eq!(reference.uri, canonical_external_uri);
            assert_eq!(reference.offset, 0);
            assert_eq!(reference.length, None);
        }
        BlobContent::Managed { .. } => {
            panic!("schema rewrite must preserve the external Blob descriptor")
        }
    }

    // (a) Current snapshot: `note` is gone from the dataset schema.
    let current_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let current_ds = current_snapshot
        .open_dataset("node:Document")
        .await
        .unwrap();
    let current_fields = current_ds
        .schema()
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>();
    assert!(
        !current_fields.iter().any(|f| f == "note"),
        "current Document dataset schema must not include 'note' after the drop; got fields {current_fields:?}",
    );

    // (b) Time travel: at the pre-drop manifest version, the prior
    // Document dataset version still has `note`. The drop is reversible
    // via Lance's version graph until `omnigraph cleanup` runs.
    let pre_drop_snapshot = db
        .snapshot_at_graph_manifest_version(before_version)
        .await
        .unwrap();
    let pre_drop_ds = pre_drop_snapshot
        .open_dataset("node:Document")
        .await
        .unwrap();
    let pre_drop_fields = pre_drop_ds
        .schema()
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>();
    assert!(
        pre_drop_fields.iter().any(|f| f == "note"),
        "pre-drop Document dataset schema must still include 'note' (time-travel reversibility); got fields {pre_drop_fields:?}",
    );

    // (c) Reopen consistency: close the engine, reopen, verify the
    // drop is preserved (column still absent from current schema).
    let uri = uri.to_string();
    drop(db);
    let reopened = Omnigraph::open(&uri).await.unwrap();
    assert_exact_id_primary_key(&reopened, "node:Document").await;
    let reopened_snapshot = reopened
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    let reopened_ds = reopened_snapshot
        .open_dataset("node:Document")
        .await
        .unwrap();
    let reopened_fields = reopened_ds
        .schema()
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>();
    assert!(
        !reopened_fields.iter().any(|f| f == "note"),
        "after reopen, Document dataset schema must still lack 'note'; got fields {reopened_fields:?}",
    );

    // A drop followed by a same-name add mints a new stable property and
    // a new Lance field id.  The current selector must not reinterpret the old
    // snapshot's identically-spelled Blob as that new property lifetime.
    let retired_content_snapshot = reopened.resolve_snapshot("main").await.unwrap();
    let without_content = desired.replace("    content: Blob?\n", "");
    reopened.apply_schema(&without_content).await.unwrap();
    reopened.apply_schema(&desired).await.unwrap();
    let lifetime_error = reopened
        .read_blob_at(
            ReadTarget::snapshot(retired_content_snapshot),
            node_blob_cell("Document", "valid-empty", "content"),
        )
        .await
        .expect_err("same-name Blob re-add must not adopt the retired property lifetime");
    assert!(
        matches!(
            lifetime_error,
            OmniError::Manifest(ref error)
                if error.kind == ManifestErrorKind::BadRequest
                    && error.message
                        == "Blob property 'Document.content' belongs to a different property lifetime at the selected target"
        ),
        "retired Blob property lifetime must fail closed; got {lifetime_error:?}"
    );
}

#[tokio::test]
#[cfg(feature = "failpoints")]
#[serial_test::parallel]
async fn schema_apply_rejects_ranged_external_blob_before_arm_or_effects() {
    use helpers::recovery::{branch_head_commit_id, sidecar_operation_ids};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let initial = r#"
node Document {
    title: String @key
    content: Blob?
}
"#;
    let desired = r#"
node Document {
    title: String @key
    content: Blob?
    note: String?
}
"#;
    let db = Omnigraph::init(uri, initial).await.unwrap();
    let table_uri = helpers::seed_ranged_external_blob_row(&db, uri).await;

    let before = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let manifest_before = before.graph_manifest_version();
    let table_before = before
        .dataset("node:Document")
        .unwrap()
        .published_dataset_version;
    let physical_head_before = lance::Dataset::open(&table_uri)
        .await
        .unwrap()
        .version()
        .version;
    let lineage_before = branch_head_commit_id(dir.path(), "main").await.unwrap();
    assert!(sidecar_operation_ids(dir.path()).is_empty());

    let error = db.apply_schema(desired).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot preserve ranged external Blob descriptor"),
        "unexpected schema-apply refusal: {error}"
    );
    let after = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(after.graph_manifest_version(), manifest_before);
    assert_eq!(
        after
            .dataset("node:Document")
            .unwrap()
            .published_dataset_version,
        table_before
    );
    assert_eq!(
        lance::Dataset::open(&table_uri)
            .await
            .unwrap()
            .version()
            .version,
        physical_head_before
    );
    assert_eq!(
        branch_head_commit_id(dir.path(), "main").await.unwrap(),
        lineage_before
    );
    assert!(
        sidecar_operation_ids(dir.path()).is_empty(),
        "ranged descriptor refusal must occur before recovery arm"
    );
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_drops_node_and_referencing_edge() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .graph_manifest_version();

    // Drop the `Company` node type and the `WorksAt` edge that references it.
    // Per schema-lint v1 chassis commit #4 (MR-694), this emits two
    // `DropType` steps; apply tombstones both manifest entries.
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

    // Confirm the plan emits both DropType steps.
    let plan = db.plan_schema(desired).await.unwrap();
    assert!(plan.supported, "drop-type plan must be supported");
    assert!(
        plan.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::DropType {
                type_kind: SchemaTypeKind::Node,
                name,
            } if name == "Company"
        )),
        "expected DropType {{ Node, Company }} in plan: {plan:?}",
    );
    assert!(
        plan.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::DropType {
                type_kind: SchemaTypeKind::Edge,
                name,
            } if name == "WorksAt"
        )),
        "expected DropType {{ Edge, WorksAt }} in plan: {plan:?}",
    );

    let result = db.apply_schema(desired).await.unwrap();
    assert!(result.supported);
    assert!(result.applied);

    let after_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .graph_manifest_version();
    assert!(
        after_version > before_version,
        "manifest version should advance after the type drop; before={before_version}, after={after_version}",
    );

    // (a) Current snapshot: both manifest entries are gone.
    let current_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert!(
        current_snapshot.dataset("node:Company").is_none(),
        "current manifest must not list node:Company after the drop",
    );
    assert!(
        current_snapshot.dataset("edge:WorksAt").is_none(),
        "current manifest must not list edge:WorksAt after the drop",
    );
    // Person + Knows still present (Person wasn't dropped; Knows is in desired).
    assert!(
        current_snapshot.dataset("node:Person").is_some(),
        "node:Person must remain in the manifest",
    );

    // (b) Time travel: at the pre-drop manifest version, both dropped
    // tables are still listed. The drop is reversible via Lance's
    // version graph until `omnigraph cleanup` runs.
    let pre_drop_snapshot = db
        .snapshot_at_graph_manifest_version(before_version)
        .await
        .unwrap();
    assert!(
        pre_drop_snapshot.dataset("node:Company").is_some(),
        "pre-drop manifest must still list node:Company (time-travel reversibility)",
    );
    assert!(
        pre_drop_snapshot.dataset("edge:WorksAt").is_some(),
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
        reopened_snapshot.dataset("node:Company").is_none(),
        "after reopen, node:Company must still be absent from the current manifest",
    );
    assert!(
        reopened_snapshot.dataset("edge:WorksAt").is_none(),
        "after reopen, edge:WorksAt must still be absent from the current manifest",
    );
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_drops_an_edge_type() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .graph_manifest_version();

    // Drop only the `WorksAt` edge. Per chassis v1 commit #4, this
    // emits `DropType { Edge, WorksAt }`; apply tombstones the
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
            } if name == "WorksAt"
        )),
        "expected DropType {{ Edge, WorksAt }} in plan: {plan:?}",
    );

    let result = db.apply_schema(&desired).await.unwrap();
    assert!(result.applied);

    let after_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .graph_manifest_version();
    assert!(after_version > before_version);

    let current_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert!(
        current_snapshot.dataset("edge:WorksAt").is_none(),
        "current manifest must not list edge:WorksAt",
    );
    // Other tables untouched.
    assert!(current_snapshot.dataset("node:Person").is_some());
    assert!(current_snapshot.dataset("node:Company").is_some());
    assert!(current_snapshot.dataset("edge:Knows").is_some());

    let pre_drop_snapshot = db
        .snapshot_at_graph_manifest_version(before_version)
        .await
        .unwrap();
    assert!(
        pre_drop_snapshot.dataset("edge:WorksAt").is_some(),
        "pre-drop manifest must still list edge:WorksAt",
    );
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_rejects_adding_a_required_property_without_backfill() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .graph_manifest_version();

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
            .graph_manifest_version(),
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
    let db = helpers::session(Omnigraph::init(uri, &initial).await.unwrap());
    db.load_jsonl(TEST_DATA, LoadMode::Overwrite).await.unwrap();

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
async fn apply_schema_pure_type_rename_preserves_identity_path_and_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let initial = TEST_SCHEMA.replace("    age: I32?", "    age: I32?\n    avatar: Blob?");
    let data = TEST_DATA.replace(
        r#"{"name": "Alice", "age": 30}"#,
        r#"{"name": "Alice", "age": 30, "avatar": "base64:QXZhdGFy"}"#,
    );
    let db = helpers::session(Omnigraph::init(uri, &initial).await.unwrap());
    db.load_jsonl(&data, LoadMode::Overwrite).await.unwrap();
    db.ensure_indices().await.unwrap();
    let before_snapshot_id = db.resolve_snapshot("main").await.unwrap();
    let before_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let before_version = before_snapshot.graph_manifest_version();
    let before = before_snapshot.dataset("node:Person").unwrap().clone();
    let before_type_id = db.catalog().type_id("Person").unwrap();
    let before_incarnation = db.catalog().table_incarnation_id("Person").unwrap();
    let before_name_property_id = db.catalog().property_id("Person", "name").unwrap();
    let people_before = count_rows(&db, "node:Person").await;
    let before_avatar = db
        .read_blob_at(
            ReadTarget::branch("main"),
            node_blob_cell("Person", "Alice", "avatar"),
        )
        .await
        .unwrap();
    let BlobContent::Managed {
        etag: before_avatar_etag,
        ..
    } = before_avatar.content
    else {
        panic!("fixture avatar must be managed")
    };

    let desired = r#"
node Human @rename_from("Person") {
    name: String @key
    age: I32?
    avatar: Blob?
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
    assert!(result.steps.iter().any(|step| matches!(
        step,
        SchemaMigrationStep::RenameType {
            type_kind: SchemaTypeKind::Node,
            from,
            to,
        } if from == "Person" && to == "Human"
    )));
    assert!(
        !result.steps.iter().any(|step| matches!(
            step,
            SchemaMigrationStep::AddProperty { .. }
                | SchemaMigrationStep::RenameProperty { .. }
                | SchemaMigrationStep::DropProperty { .. }
        )),
        "pure rename unexpectedly planned a table rewrite: {:?}",
        result.steps
    );

    let after_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let after = after_snapshot.dataset("node:Human").unwrap();
    assert_eq!(after.dataset_path, before.dataset_path);
    assert_eq!(
        after.published_dataset_version,
        before.published_dataset_version
    );
    assert_eq!(db.catalog().type_id("Human"), Some(before_type_id));
    assert_eq!(
        db.catalog().table_incarnation_id("Human"),
        Some(before_incarnation)
    );
    assert_eq!(
        db.catalog().property_id("Human", "name"),
        Some(before_name_property_id)
    );
    assert_eq!(count_rows(&db, "node:Human").await, people_before);
    assert!(after_snapshot.dataset("node:Person").is_none());

    let current_avatar = read_managed_blob_bytes(
        &db,
        ReadTarget::branch("main"),
        node_blob_cell("Human", "Alice", "avatar"),
    )
    .await;
    assert_eq!(&current_avatar[..], b"Avatar");
    let renamed_avatar = db
        .read_blob_at(
            ReadTarget::branch("main"),
            node_blob_cell("Human", "Alice", "avatar"),
        )
        .await
        .unwrap();
    let BlobContent::Managed {
        etag: renamed_avatar_etag,
        ..
    } = renamed_avatar.content
    else {
        panic!("renamed avatar must remain managed")
    };
    assert_eq!(
        renamed_avatar_etag, before_avatar_etag,
        "a pure type alias rename over the same exact table version must preserve the Blob ETag"
    );
    let historical_avatar = read_managed_blob_bytes(
        &db,
        ReadTarget::snapshot(before_snapshot_id),
        node_blob_cell("Human", "Alice", "avatar"),
    )
    .await;
    assert_eq!(
        &historical_avatar[..],
        b"Avatar",
        "the current type alias must bind the same stable table identity in a pre-rename snapshot"
    );
    let retired_type_error = db
        .read_blob_at(
            ReadTarget::branch("main"),
            node_blob_cell("Person", "Alice", "avatar"),
        )
        .await
        .expect_err("the retired type alias must not remain addressable");
    assert!(matches!(
        retired_type_error,
        OmniError::Manifest(ref error) if error.kind == ManifestErrorKind::BadRequest
    ));

    let historical_snapshot = db
        .snapshot_at_graph_manifest_version(before_version)
        .await
        .unwrap();
    let historical = historical_snapshot.dataset("node:Person").unwrap();
    assert_eq!(historical.dataset_path, before.dataset_path);
    assert_eq!(historical.dataset_path, after.dataset_path);
    assert_eq!(
        historical.published_dataset_version,
        before.published_dataset_version
    );
    assert!(historical_snapshot.dataset("node:Human").is_none());
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_rename_and_property_drop_then_cleanup_reclaims_source_incarnation() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let before_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let before_manifest_version = before_snapshot.graph_manifest_version();
    let before = before_snapshot.dataset("node:Person").unwrap().clone();

    let desired = r#"
node Human @rename_from("Person") {
    name: String @key
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
    assert!(result.applied);
    assert!(result.steps.iter().any(|step| matches!(
        step,
        SchemaMigrationStep::DropProperty { type_name, .. } if type_name == "Human"
    )));

    let after_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let after = after_snapshot.dataset("node:Human").unwrap();
    assert_eq!(after.dataset_path, before.dataset_path);
    assert!(after.published_dataset_version > before.published_dataset_version);
    assert!(after_snapshot.dataset("node:Person").is_none());
    reclaim_dropped_history(&db).await;
    assert!(
        db.snapshot_at_graph_manifest_version(before_manifest_version)
            .await
            .unwrap()
            .open_dataset("node:Person")
            .await
            .is_err(),
        "cleanup must reclaim the renamed source incarnation's prior version"
    );
}

/// A drop's prior version is a pin that `cleanup` reclaims once `--keep`
/// prunes the `__manifest` version naming it.
async fn reclaim_dropped_history(db: &Session) {
    let stats = db
        .cleanup(omnigraph::db::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .unwrap();
    for row in &stats {
        assert!(row.error.is_none(), "{row:?}");
    }
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
    let uri = dir.path().to_str().unwrap();
    let initial = TEST_SCHEMA.replace("    age: I32?", "    age: I32?\n    avatar: Blob?");
    let data = TEST_DATA.replace(
        r#"{"name": "Alice", "age": 30}"#,
        r#"{"name": "Alice", "age": 30, "avatar": "base64:QXZhdGFy"}"#,
    );
    let db = helpers::session(Omnigraph::init(uri, &initial).await.unwrap());
    db.load_jsonl(&data, LoadMode::Overwrite).await.unwrap();
    let before_snapshot_id = db.resolve_snapshot("main").await.unwrap();
    let before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .clone();
    let before_type_id = db.catalog().type_id("Person").unwrap();
    let before_incarnation = db.catalog().table_incarnation_id("Person").unwrap();
    let before_name_property_id = db.catalog().property_id("Person", "name").unwrap();
    let before_avatar_property_id = db.catalog().property_id("Person", "avatar").unwrap();
    let people_before = count_rows(&db, "node:Person").await;
    assert!(
        people_before > 0,
        "fixture should seed Person rows for this test to be meaningful"
    );

    // Rename Person -> Human, the keying property name -> full_name, and the
    // Blob property avatar -> portrait.
    // Edges that referenced Person must update to Human in the same migration.
    let desired = r#"
node Human @rename_from("Person") {
    full_name: String @key @rename_from("name")
    age: I32?
    portrait: Blob? @rename_from("avatar")
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
    let after_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let after = after_snapshot.dataset("node:Human").unwrap();
    assert_eq!(after.dataset_path, before.dataset_path);
    assert!(
        after.published_dataset_version > before.published_dataset_version,
        "property rename must rewrite the same table rather than rematerialize it"
    );
    assert_eq!(db.catalog().type_id("Human"), Some(before_type_id));
    assert_eq!(
        db.catalog().table_incarnation_id("Human"),
        Some(before_incarnation)
    );
    assert_eq!(
        db.catalog().property_id("Human", "full_name"),
        Some(before_name_property_id)
    );
    assert_eq!(
        db.catalog().property_id("Human", "portrait"),
        Some(before_avatar_property_id)
    );
    assert_eq!(count_rows(&db, "node:Human").await, people_before);
    assert!(
        after_snapshot.dataset("node:Person").is_none(),
        "old node:Person table key should be unmapped after rename"
    );

    let current_portrait = read_managed_blob_bytes(
        &db,
        ReadTarget::branch("main"),
        node_blob_cell("Human", "Alice", "portrait"),
    )
    .await;
    assert_eq!(&current_portrait[..], b"Avatar");

    let historical_error = db
        .read_blob_at(
            ReadTarget::snapshot(before_snapshot_id),
            node_blob_cell("Human", "Alice", "portrait"),
        )
        .await
        .expect_err("the current property alias must not guess across a v6 historical rewrite");
    assert!(
        matches!(
            historical_error,
            OmniError::Manifest(ref error)
                if error.kind == ManifestErrorKind::BadRequest
                    && error.message
                        == "Blob property 'Human.portrait' is unavailable at the selected target"
        ),
        "the current type alias must bind the pre-rename table, then the unavailable current property alias must be BadRequest; got {historical_error:?}"
    );
    for retired_cell in [
        node_blob_cell("Person", "Alice", "avatar"),
        node_blob_cell("Human", "Alice", "avatar"),
    ] {
        let error = db
            .read_blob_at(ReadTarget::branch("main"), retired_cell)
            .await
            .expect_err("retired type/property aliases must not remain addressable");
        assert!(matches!(
            error,
            OmniError::Manifest(ref manifest)
                if manifest.kind == ManifestErrorKind::BadRequest
        ));
    }
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn composite_key_identity_survives_lexically_crossing_property_renames() {
    let initial = r#"
node Pair {
    alpha: String
    zeta: String
    label: String
    @key(alpha, zeta)
}
"#;
    let desired = r#"
node Pair {
    aaaa: String @rename_from("zeta")
    zzzz: String @rename_from("alpha")
    label: String
    @key(aaaa, zzzz)
}
"#;
    let mutation = r#"
query put_pair($aaaa: String, $zzzz: String, $label: String) {
    insert Pair { aaaa: $aaaa, zzzz: $zzzz, label: $label }
}
"#;

    let dir = tempfile::tempdir().unwrap();
    let db = helpers::session(
        Omnigraph::init(dir.path().to_str().unwrap(), initial)
            .await
            .unwrap(),
    );
    db.load_jsonl(
        r#"{"type":"Pair","data":{"alpha":"A","zeta":"Z","label":"before"}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    let canonical_id = r#"["A","Z"]"#;
    assert_eq!(
        collect_column_strings(&read_table(&db, "node:Pair").await, "__id"),
        [canonical_id]
    );
    let alpha_id = db.catalog().property_id("Pair", "alpha").unwrap();
    let zeta_id = db.catalog().property_id("Pair", "zeta").unwrap();

    let applied = db.apply_schema(desired).await.unwrap();
    assert!(applied.supported && applied.applied);
    assert_eq!(
        db.catalog().node_types["Pair"].key.as_deref(),
        Some(&["zzzz".to_string(), "aaaa".to_string()][..]),
        "runtime key order follows stable property identity across lexical crossing"
    );
    assert_eq!(db.catalog().property_id("Pair", "zzzz"), Some(alpha_id));
    assert_eq!(db.catalog().property_id("Pair", "aaaa"), Some(zeta_id));
    assert_eq!(
        collect_column_strings(&read_table(&db, "node:Pair").await, "__id"),
        [canonical_id],
        "schema rewrite must retain the existing physical identity"
    );

    db.mutate(
        "main",
        mutation,
        "put_pair",
        &params(&[("$aaaa", "Z"), ("$zzzz", "A"), ("$label", "after")]),
    )
    .await
    .unwrap();

    let rows = read_table(&db, "node:Pair").await;
    assert_eq!(count_rows(&db, "node:Pair").await, 1);
    assert_eq!(collect_column_strings(&rows, "__id"), [canonical_id]);
    assert_eq!(collect_column_strings(&rows, "label"), ["after"]);
    assert_exact_id_primary_key(&db, "node:Pair").await;
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_drop_then_same_name_readd_mints_new_identity_and_path() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(
        uri,
        r#"
node Person { name: String @key }
node Anchor { name: String @key }
"#,
    )
    .await
    .unwrap();
    let before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .clone();
    let before_type_id = db.catalog().type_id("Person").unwrap();
    let before_incarnation = db.catalog().table_incarnation_id("Person").unwrap();

    db.apply_schema("node Anchor { name: String @key }")
        .await
        .unwrap();
    assert!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .dataset("node:Person")
            .is_none()
    );

    db.apply_schema(
        r#"
node Person { name: String @key }
node Anchor { name: String @key }
"#,
    )
    .await
    .unwrap();
    let after_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let after = after_snapshot.dataset("node:Person").unwrap();
    assert_ne!(db.catalog().type_id("Person"), Some(before_type_id));
    assert_ne!(
        db.catalog().table_incarnation_id("Person"),
        Some(before_incarnation)
    );
    assert_ne!(after.dataset_path, before.dataset_path);
    assert_eq!(after.published_dataset_version, 1);
}

// ─── Drops reclaim at cleanup, never at apply ────────────────────────────────
//
// Apply reclaims nothing: the prior table version (where a dropped column
// lived) stays pinned by the older `__manifest` versions, so
// `snapshot_at_graph_manifest_version(pre_drop)` still reads it. It becomes
// unreachable once `omnigraph cleanup --keep 1` stops retaining those versions
// and the collector reclaims its files.

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn apply_schema_property_drop_is_reclaimed_by_cleanup_not_apply() {
    use arrow_array::Array;
    use futures::TryStreamExt;

    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let before_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .graph_manifest_version();

    // Drop the `age` column. Apply rewrites the table without it.
    let desired = TEST_SCHEMA.replace("    age: I32?\n", "");
    let result = db.apply_schema(&desired).await.unwrap();
    assert!(result.applied);

    // Current snapshot: column gone from the dataset schema.
    let current_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let current_ds = current_snapshot.open_dataset("node:Person").await.unwrap();
    let current_fields = current_ds
        .schema()
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>();
    assert!(
        !current_fields.iter().any(|f| f == "age"),
        "current Person schema must not include 'age' after the drop; got {current_fields:?}",
    );

    // Before cleanup the pre-drop snapshot still reads the dropped column's
    // values from its data files.
    let pre_drop = db
        .snapshot_at_graph_manifest_version(before_version)
        .await
        .unwrap();
    let pre_drop_ds = pre_drop.open_dataset("node:Person").await.unwrap();
    let mut scanner = pre_drop_ds.scan();
    scanner.project(&["age"]).unwrap();
    let batches: Vec<arrow_array::RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ages = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("age")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap()
                .iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    ages.sort_unstable();
    assert_eq!(
        ages,
        [25, 28, 30, 35],
        "before cleanup, the pre-drop snapshot must still read the dropped 'age' values"
    );

    // After `cleanup --keep 1` the pre-drop manifest version is no longer
    // retained, so its table version is reclaimed and the snapshot's
    // entry points at a version that no longer opens.
    reclaim_dropped_history(&db).await;
    let pre_drop = db
        .snapshot_at_graph_manifest_version(before_version)
        .await
        .unwrap();
    let open_result = pre_drop.open_dataset("node:Person").await;
    assert!(
        open_result.is_err(),
        "after the drop + cleanup, pre-drop snapshot.open_dataset() must fail (prior version was reclaimed); got {open_result:?}",
    );
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
    let db = helpers::session(Omnigraph::init(uri, v1).await.unwrap());

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
    db.load_jsonl(r#"{"type":"Doc","data":{"slug":"d1","body":"hello","embedding":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]}}"#, LoadMode::Merge, )
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
    let db = helpers::session(Omnigraph::init(uri, v1).await.unwrap());
    db.load_jsonl(
        r#"{"type":"Doc","data":{"slug":"d1","n":1}}"#,
        LoadMode::Merge,
    )
    .await
    .expect("load a Doc");

    let before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .dataset("node:Doc")
        .unwrap()
        .published_dataset_version;
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
        .dataset("node:Doc")
        .unwrap()
        .published_dataset_version;
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

// A full-text call answers with the index's analyzer at every schema apply.
// Lance applies the analyzer only through a segment of the index; with none,
// its flat path tokenizes bare, so "deep" misses "Deep Learning". Declaring a
// full-text `@index` on a populated table, and a rewrite (a Lance overwrite,
// which drops every index), each publish an untrained segment that carries the
// analyzer; `ensure_indices` later builds the postings.
#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn full_text_analyzer_survives_index_declaration_and_rewrite_issue_904() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let v1 = "node Doc {\n    slug: String @key\n    body: String\n}\n";
    let db = helpers::session(Omnigraph::init(uri, v1).await.unwrap());
    db.load_jsonl(
        r#"{"type":"Doc","data":{"slug":"d1","body":"Deep Learning"}}
{"type":"Doc","data":{"slug":"d2","body":"deep dive"}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let source = r#"query docs($q: String) {
        match { $d: Doc search($d.body, $q) }
        return { $d.slug }
    }"#;
    let deep = || async {
        first_column_sorted(
            &query_main(&db, source, "docs", &params(&[("q", "deep")]))
                .await
                .unwrap(),
        )
    };
    let body_segments = || async {
        let ds = open_pinned_dataset_for_test(&db, "main", "node:Doc").await;
        let body = ds.schema().field("body").unwrap().id;
        ds.load_indices()
            .await
            .unwrap()
            .iter()
            .filter(|index| index.fields.contains(&body))
            .map(|index| index.fragment_bitmap.as_ref().map(|bitmap| bitmap.len()))
            .collect::<Vec<_>>()
    };

    // Declaring the full-text index publishes its analyzer with the contract.
    let declared = "node Doc {\n    slug: String @key\n    body: String @index\n}\n";
    let before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .dataset("node:Doc")
        .unwrap()
        .published_dataset_version;
    assert!(db.apply_schema(declared).await.unwrap().applied);
    let after = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .dataset("node:Doc")
        .unwrap()
        .published_dataset_version;
    assert_eq!(after, before + 1, "the declaration is a table effect");
    assert_eq!(
        body_segments().await,
        [Some(0)],
        "one untrained segment: the analyzer without postings"
    );
    assert_eq!(deep().await, ["d1", "d2"]);

    // A rewrite drops every index; the analyzer is declared again.
    let rewritten =
        "node Doc {\n    slug: String @key\n    body: String @index\n    extra: String?\n}\n";
    assert!(db.apply_schema(rewritten).await.unwrap().applied);
    assert_eq!(body_segments().await, [Some(0)]);
    assert_eq!(deep().await, ["d1", "d2"]);

    // The reconciler builds the postings over the declaration.
    db.ensure_indices().await.unwrap();
    assert_eq!(body_segments().await, [Some(1)]);
    assert_eq!(deep().await, ["d1", "d2"]);
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
    let db = helpers::session(Omnigraph::init(uri, v1).await.unwrap());
    db.load_jsonl(
        r#"{"type":"Ticket","data":{"slug":"t1","status":"todo"}}"#,
        LoadMode::Merge,
    )
    .await
    .expect("load a Ticket with an original variant");

    let before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .dataset("node:Ticket")
        .unwrap()
        .published_dataset_version;
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
        .dataset("node:Ticket")
        .unwrap()
        .published_dataset_version;
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
    db.load_jsonl(
        r#"{"type":"Ticket","data":{"slug":"t2","status":"blocked"}}"#,
        LoadMode::Merge,
    )
    .await
    .expect("new variant must be accepted after widening");
    // ...an original variant still is...
    db.load_jsonl(
        r#"{"type":"Ticket","data":{"slug":"t3","status":"done"}}"#,
        LoadMode::Merge,
    )
    .await
    .expect("original variant must remain accepted");
    // ...and an out-of-set value is still rejected (the fence didn't widen to
    // free text).
    let err = db
        .load_jsonl(
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
    let db = helpers::session(Omnigraph::init(uri, v1).await.unwrap());

    let narrowed = "node Ticket {\n    slug: String @key\n    status: enum(todo, done)\n}\n";
    let err = db.apply_schema(narrowed).await;
    assert!(err.is_err(), "narrowing must refuse at apply");
    let msg = format!("{}", err.unwrap_err());
    assert!(
        msg.contains("OG-MF-106"),
        "refusal must carry the stable lint code, got: {msg}"
    );

    // The graph stays healthy and writable on the original schema.
    db.load_jsonl(
        r#"{"type":"Ticket","data":{"slug":"t1","status":"doing"}}"#,
        LoadMode::Merge,
    )
    .await
    .expect("graph must remain writable after a refused narrowing");
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn schema_settlement_fences_only_the_original_candidate_and_repeats_its_own_receipt() {
    for original_wins in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let db = init_and_load(&dir).await;
        let before = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let desired = TEST_SCHEMA.replace("age: I32?", "age: I32?\n    nickname: String?");
        let original = db
            .prepare_schema_apply_as(&desired, Some("initiator"))
            .await
            .unwrap();
        let encoded = serde_json::to_value(&original).unwrap();
        assert_eq!(encoded["version"], 2);
        let files = schema_storage_bytes(dir.path());
        let fence = db
            .prepare_schema_settlement_as(&original, Some("fence-author"))
            .await
            .unwrap();
        let fence: PreparedSchemaSettlement =
            serde_json::from_slice(&serde_json::to_vec(&fence).unwrap()).unwrap();
        assert_eq!(
            schema_storage_bytes(dir.path()),
            files,
            "issuing settlement has no graph effects"
        );
        let receipt = if original_wins {
            Some(
                db.apply_prepared_schema_as(&original, Some("initiator"))
                    .await
                    .unwrap()
                    .commit
                    .unwrap(),
            )
        } else {
            None
        };
        drop(db);
        let db = helpers::session(Omnigraph::open(uri).await.unwrap());
        let settled = db
            .settle_prepared_schema_as(&original, &fence, Some("adopting-operator"))
            .await
            .unwrap();
        match (&settled, receipt) {
            (SchemaApplySettlement::Committed { commit, contract }, Some(receipt)) => {
                assert_eq!(*commit, receipt);
                assert_eq!(commit.actor_id.as_deref(), Some("initiator"));
                assert_eq!(contract, original.desired_contract());
                assert_eq!(db.schema_source().as_str(), desired);
            }
            (
                SchemaApplySettlement::NotPublished {
                    proof: SchemaNonPublicationProof::Fence { commit, contract },
                },
                None,
            ) => {
                assert_eq!(
                    intent_nonce(&commit.graph_commit_id).unwrap(),
                    fence.graph_commit_id().unwrap()
                );
                assert_eq!(
                    commit.graph_manifest_version,
                    original.base_manifest_version() + 1
                );
                assert_eq!(
                    commit.parent_commit_id.as_deref(),
                    original.base_head_commit_id()
                );
                assert_eq!(commit.actor_id.as_deref(), Some("fence-author"));
                assert_eq!(
                    contract.source_hash,
                    format!("{:x}", Sha256::digest(TEST_SCHEMA.as_bytes()))
                );
                assert_eq!(db.schema_source().as_str(), TEST_SCHEMA);
                let after = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
                for entry in before.datasets() {
                    let kept = after.dataset(&entry.type_key).unwrap();
                    assert_eq!(
                        entry.published_dataset_version,
                        kept.published_dataset_version
                    );
                    assert_eq!(entry.version_metadata, kept.version_metadata);
                }
                assert!(
                    db.apply_prepared_schema_as(&original, Some("initiator"))
                        .await
                        .is_err()
                );
            }
            other => panic!("wrong original/fence winner: {other:?}"),
        }
        let commits = db.list_commits(None).await.unwrap();
        assert_eq!(
            commits[0].graph_manifest_version,
            original.base_manifest_version() + 1
        );
        let files = schema_storage_bytes(dir.path());
        assert_eq!(
            db.settle_prepared_schema_as(&original, &fence, Some("another-operator"))
                .await
                .unwrap(),
            settled
        );
        assert_eq!(
            schema_storage_bytes(dir.path()),
            files,
            "repeating settlement cannot publish a second fence"
        );
        db.load_jsonl(
            r#"{"type":"Person","data":{"name":"Later write"}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
        drop(db);
        let observer = Omnigraph::open_read_only(uri).await.unwrap();
        let files = schema_storage_bytes(dir.path());
        assert_eq!(
            observer
                .settle_prepared_schema_as(&original, &fence, Some("lookup-operator"))
                .await
                .unwrap(),
            settled
        );
        assert_eq!(
            schema_storage_bytes(dir.path()),
            files,
            "later HEAD must not replace the exact result"
        );
    }
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn schema_settlement_proves_foreign_occupancy_but_missing_evidence_stays_unknown() {
    for metadata_only in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let db = init_and_load(&dir).await;
        let desired = format!("{TEST_SCHEMA}\n// original deployment\n");
        let original = db.prepare_schema_apply_as(&desired, None).await.unwrap();
        let fence = db
            .prepare_schema_settlement_as(&original, None)
            .await
            .unwrap();
        if metadata_only {
            let mut catalog = omnigraph_catalog::ManifestCoordinator::open(uri)
                .await
                .unwrap();
            let contract = catalog.read_schema_contract().await.unwrap();
            catalog
                .commit_changes(&[omnigraph_catalog::ManifestChange::SchemaContract(contract)])
                .await
                .unwrap();
        } else {
            let foreign = format!("{TEST_SCHEMA}\n// unrelated schema\n");
            db.apply_schema(&foreign).await.unwrap();
        }
        let candidate_version = original.base_manifest_version() + 1;
        let files = schema_storage_bytes(dir.path());
        let result = db
            .settle_prepared_schema_as(&original, &fence, Some("recovery"))
            .await
            .unwrap();
        match result {
            SchemaApplySettlement::NotPublished {
                proof:
                    SchemaNonPublicationProof::Occupied {
                        graph_manifest_version,
                        head_commit_id,
                        contract,
                    },
            } => {
                assert_eq!(graph_manifest_version, candidate_version);
                assert_ne!(head_commit_id.as_deref(), original.graph_commit_id());
                assert_ne!(&contract, original.desired_contract());
                if metadata_only {
                    assert_eq!(head_commit_id.as_deref(), original.base_head_commit_id());
                }
            }
            other => panic!("expected exact occupied candidate, got {other:?}"),
        }
        assert_eq!(schema_storage_bytes(dir.path()), files);
        assert!(db.apply_prepared_schema_as(&original, None).await.is_err());
        db.load_jsonl(
            r#"{"type":"Person","data":{"name":"Later write"}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
        // Explicitly remove only retained candidate metadata: current cleanup
        // does not prune catalog history. Absence beneath a later HEAD cannot
        // certify the original's outcome or permit a new fence.
        let published = lance::Dataset::open(&format!("{uri}/__manifest"))
            .await
            .unwrap()
            .checkout_version(candidate_version)
            .await
            .unwrap();
        published
            .object_store(None)
            .await
            .unwrap()
            .delete(&published.manifest_location().path)
            .await
            .unwrap();
        let files = schema_storage_bytes(dir.path());
        assert_eq!(
            db.settle_prepared_schema_as(&original, &fence, None)
                .await
                .unwrap(),
            SchemaApplySettlement::Unknown
        );
        assert_eq!(schema_storage_bytes(dir.path()), files);
    }
}

#[tokio::test]
#[cfg_attr(feature = "failpoints", serial_test::parallel)]
async fn schema_settlement_noop_refuses_stale_authority_without_a_fence() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let original = db
        .prepare_schema_apply_as(TEST_SCHEMA, Some("initiator"))
        .await
        .unwrap();
    let fence = db
        .prepare_schema_settlement_as(&original, Some("recovery"))
        .await
        .unwrap();
    assert!(fence.graph_commit_id().is_none());
    let files = schema_storage_bytes(dir.path());
    assert!(matches!(
        db.settle_prepared_schema_as(&original, &fence, Some("adopter"))
            .await
            .unwrap(),
        SchemaApplySettlement::NoOp { .. }
    ));
    assert_eq!(schema_storage_bytes(dir.path()), files);
    db.branch_create("other").await.unwrap();
    let files = schema_storage_bytes(dir.path());
    assert_eq!(
        db.settle_prepared_schema_as(&original, &fence, None)
            .await
            .unwrap(),
        SchemaApplySettlement::NoOpRefused
    );
    assert_eq!(schema_storage_bytes(dir.path()), files);
    db.branch_delete("other").await.unwrap();
    db.load_jsonl(
        r#"{"type":"Person","data":{"name":"Later"}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let files = schema_storage_bytes(dir.path());
    assert_eq!(
        db.settle_prepared_schema_as(&original, &fence, None)
            .await
            .unwrap(),
        SchemaApplySettlement::NoOpRefused
    );
    assert_eq!(schema_storage_bytes(dir.path()), files);
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn prepared_schema_numeric_fence_rejects_metadata_interposed_after_effects() {
    use omnigraph::seams::catalog;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = init_and_load(&dir).await;
    let before = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let desired = TEST_SCHEMA.replace("age: I32?", "age: I32?\n    nickname: String?");
    let original = db.prepare_schema_apply_as(&desired, None).await.unwrap();
    let fence = db
        .prepare_schema_settlement_as(&original, Some("recovery"))
        .await
        .unwrap();
    let apply = Arc::clone(db.db());
    let attempt = original.clone();
    let rendezvous = helpers::failpoint::Rendezvous::park_first(&catalog::PUBLISH_PRE_MERGE);
    let writer = tokio::spawn(async move { apply.apply_prepared_schema_as(&attempt, None).await });
    rendezvous.wait_until_reached().await;
    // Use the catalog publication door without this process's schema gate to
    // model another process winning after our detached effects and CAS checks.
    let mut control = omnigraph_catalog::ManifestCoordinator::open(uri)
        .await
        .unwrap();
    let contract = control.read_schema_contract().await.unwrap();
    control
        .commit_changes(&[omnigraph_catalog::ManifestChange::SchemaContract(contract)])
        .await
        .unwrap();
    assert_eq!(control.version(), original.base_manifest_version() + 1);
    assert_eq!(
        control.exact_graph_head().as_deref(),
        original.base_head_commit_id()
    );
    rendezvous.release();
    let error = writer.await.unwrap().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("prepared_schema_manifest_version"),
        "{error}"
    );
    let reopened = Omnigraph::open_read_only(uri).await.unwrap();
    let after = reopened
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    assert_eq!(
        after.graph_manifest_version(),
        original.base_manifest_version() + 1
    );
    assert_eq!(reopened.schema_source().as_str(), TEST_SCHEMA);
    for entry in before.datasets() {
        assert_eq!(
            after
                .dataset(&entry.type_key)
                .unwrap()
                .published_dataset_version,
            entry.published_dataset_version
        );
    }
    assert!(matches!(
        reopened
            .settle_prepared_schema_as(&original, &fence, Some("recovery"))
            .await
            .unwrap(),
        SchemaApplySettlement::NotPublished {
            proof: SchemaNonPublicationProof::Occupied { .. }
        }
    ));
}
