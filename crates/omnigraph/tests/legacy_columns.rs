//! Legacy-vintage engine paths and persisted schema constraint compatibility.
#![cfg(feature = "failpoints")]

mod helpers;

use std::fs;

use arrow_array::{StringArray, StructArray};
use omnigraph::changes::{ChangeFeedScope, ChangeFilter, ChangeOp, ChangeOpKind};

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::{Literal, SCHEMA_IR_VERSION};

use helpers::*;

const LEGACY_SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I32?
}
node Company {
    name: String
}
edge WorksAt: Person -> Company {
    title: String?
}
"#;

const LEGACY_DATA: &str = r#"{"type":"Person","data":{"id":"Alice","name":"Alice","age":30}}
{"type":"Person","data":{"name":"Bob","age":25}}
{"type":"Company","data":{"id":"company-1","name":"Acme"}}
{"edge":"WorksAt","from":"Alice","to":"company-1","data":{"id":"works-alice","title":"engineer"}}
{"edge":"WorksAt","id":"works-bob","from":"Bob","to":"company-1","data":{}}"#;

#[tokio::test]
async fn legacy_vintage_graph_works_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let mut db = Omnigraph::init_with_legacy_system_columns_for_tests(uri, LEGACY_SCHEMA)
        .await
        .unwrap();

    let ir: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(dir.path().join("_schema.ir.json")).unwrap())
            .unwrap();
    assert_eq!(
        ir["ir_version"].as_u64(),
        Some(u64::from(SCHEMA_IR_VERSION)),
        "the persisted schema authority must record the legacy vintage"
    );
    assert_eq!(
        db.internal_schema_version_of(ReadTarget::branch("main"))
            .await
            .unwrap(),
        8,
        "a legacy-vintage graph is born at `__manifest` stamp 8 (RFC 0040 Compatibility)"
    );
    let snap = snapshot_main(&db).await.unwrap();
    for table_key in ["node:Person", "node:Company", "edge:WorksAt"] {
        let dataset = snap.open_dataset(table_key).await.unwrap();
        let primary_key = dataset
            .schema()
            .unenforced_primary_key()
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            primary_key,
            ["id"],
            "legacy table {table_key} must keep `id` as its Lance unenforced primary key"
        );
        assert!(
            dataset.schema().field("__id").is_none(),
            "legacy table {table_key} must carry no current-vintage `__id` column"
        );
    }
    let works_at = snap.open_dataset("edge:WorksAt").await.unwrap();
    assert!(
        works_at.schema().field("src").is_some() && works_at.schema().field("dst").is_some(),
        "a legacy edge table carries the bare endpoint columns"
    );
    assert!(
        works_at.schema().field("__src").is_none(),
        "a legacy edge table carries no current-vintage `__src` column"
    );

    load_jsonl(&db, LEGACY_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    let loaded_version = version_main(&db).await.unwrap();
    let loaded_commit = snapshot_id(&db, "main").await.unwrap();
    let company_result = query_main(
        &mut db,
        "query company_identity() { match { $c: Company } return { $c.@id, $c.name } }",
        "company_identity",
        &ParamMap::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        collect_column_strings(company_result.batches(), "c.@id"),
        ["company-1"]
    );
    assert_eq!(
        collect_column_strings(company_result.batches(), "c.name"),
        ["Acme"]
    );
    assert_eq!(
        collect_column_strings(&read_table(&db, "node:Company").await, "id"),
        ["company-1"]
    );
    for invalid in [
        r#"{"type":"Company","id":"other","data":{"id":"duplicate","name":"Bad"}}"#,
        r#"{"type":"Company","id":17,"data":{"name":"Bad"}}"#,
        r#"{"edge":"WorksAt","id":"other","from":"Alice","to":"company-1","data":{"id":"duplicate"}}"#,
        r#"{"edge":"WorksAt","id":17,"from":"Alice","to":"company-1","data":{}}"#,
    ] {
        assert!(load_jsonl(&db, invalid, LoadMode::Append).await.is_err());
        assert_eq!(version_main(&db).await.unwrap(), loaded_version);
        assert_eq!(snapshot_id(&db, "main").await.unwrap(), loaded_commit);
        assert_eq!(count_rows(&db, "node:Company").await, 1);
        assert_eq!(count_rows(&db, "edge:WorksAt").await, 2);
    }

    let page = db
        .commit_changes_page(
            loaded_commit.as_str(),
            &ChangeFeedScope::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(page.block.changes.len(), 5);
    assert!(
        page.block
            .changes
            .iter()
            .all(|change| change.op == ChangeOpKind::Insert)
    );
    let company = page
        .block
        .changes
        .iter()
        .find(|change| change.id == "company-1")
        .unwrap();
    assert_eq!(company.after.as_ref().unwrap().properties["name"], "Acme");
    assert!(
        !company
            .after
            .as_ref()
            .unwrap()
            .properties
            .contains_key("id")
    );
    let edge = page
        .block
        .changes
        .iter()
        .find(|change| change.id == "works-alice")
        .unwrap();
    let image = edge.after.as_ref().unwrap();
    assert_eq!(image.properties["title"], "engineer");
    assert!(!image.properties.contains_key("id"));
    assert!(!image.properties.contains_key("src"));
    assert!(!image.properties.contains_key("dst"));
    let endpoints = image.endpoints.as_ref().unwrap();
    assert_eq!(endpoints.from, "Alice");
    assert_eq!(endpoints.to, "company-1");

    let entity = db
        .entity_at("node:Company", "company-1", loaded_version)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entity["@id"], "company-1");
    assert_eq!(entity["name"], "Acme");
    assert!(entity.get("id").is_none());
    let entity = db
        .entity_at_target(ReadTarget::branch("main"), "edge:WorksAt", "works-alice")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entity["@id"], "works-alice");
    assert_eq!(entity["@src"], "Alice");
    assert_eq!(entity["@dst"], "company-1");
    assert!(entity.get("id").is_none());
    assert!(entity.get("src").is_none());
    assert!(entity.get("dst").is_none());

    let exported = db.export_jsonl("main", &[]).await.unwrap();
    let rows = exported
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let company = rows.iter().find(|row| row["type"] == "Company").unwrap();
    assert_eq!(company["id"], "company-1");
    assert_eq!(company["data"]["name"], "Acme");
    assert!(company["data"].get("id").is_none());
    let edge = rows.iter().find(|row| row["id"] == "works-alice").unwrap();
    assert_eq!(edge["from"], "Alice");
    assert_eq!(edge["to"], "company-1");
    assert!(edge["data"].get("id").is_none());
    for legacy in [true, false] {
        let imported_dir = tempfile::tempdir().unwrap();
        let imported_uri = imported_dir.path().to_str().unwrap();
        let imported = if legacy {
            Omnigraph::init_with_legacy_system_columns_for_tests(imported_uri, LEGACY_SCHEMA)
                .await
                .unwrap()
        } else {
            Omnigraph::init(imported_uri, LEGACY_SCHEMA).await.unwrap()
        };
        load_jsonl(&imported, &exported, LoadMode::Overwrite)
            .await
            .unwrap();
        let columns = imported.catalog().system_columns;
        assert_eq!(
            collect_column_strings(&read_table(&imported, "node:Company").await, columns.id),
            ["company-1"]
        );
        let edges = read_table(&imported, "edge:WorksAt").await;
        assert_eq!(
            collect_column_strings(&edges, columns.id),
            ["works-alice", "works-bob"]
        );
        assert_eq!(
            collect_column_strings(&edges, columns.src),
            ["Alice", "Bob"]
        );
        assert_eq!(
            collect_column_strings(&edges, columns.dst),
            ["company-1", "company-1"]
        );
        assert_eq!(imported.export_jsonl("main", &[]).await.unwrap(), exported);
    }

    let people = read_table(&db, "node:Person").await;
    let mut ids = collect_column_strings(&people, "id");
    ids.sort();
    assert_eq!(
        ids,
        ["Alice", "Bob"],
        "reads must resolve the legacy `id` spelling through the catalog"
    );
    let edges = read_table(&db, "edge:WorksAt").await;
    assert_eq!(
        collect_column_strings(&edges, "src"),
        ["Alice", "Bob"],
        "reads must resolve the legacy `src` spelling through the catalog"
    );
    assert_eq!(
        collect_column_strings(&edges, "dst"),
        ["company-1", "company-1"]
    );

    let result = query_main(
        &mut db,
        "query coworkers() {\n    match {\n        $p: Person\n        $p worksat $c\n    }\n    return { $p.name, $c.name }\n}",
        "coworkers",
        &ParamMap::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        result.num_rows(),
        2,
        "traversal must join on the legacy endpoint columns"
    );

    let result = query_main(
        &mut db,
        "query ids() {\n    match {\n        $p: Person\n        $p $w:worksat $c\n    }\n    return { $p.@id, $w.@src, $w.@dst }\n    order { $p.@id asc }\n}",
        "ids",
        &ParamMap::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        collect_column_strings(result.batches(), "p.@id"),
        ["Alice", "Bob"],
        "`$p.@id` must read the legacy `id` column and keep its logical result name"
    );
    assert_eq!(
        collect_column_strings(result.batches(), "w.@src"),
        ["Alice", "Bob"],
        "`$w.@src` must read the legacy `src` column and keep its logical result name"
    );
    assert_eq!(
        collect_column_strings(result.batches(), "w.@dst"),
        ["company-1", "company-1"]
    );

    let objects_result = query_main(
        &mut db,
        "query objects() { match { $p: Person } return { $p } order { $p.@id asc } }",
        "objects",
        &ParamMap::new(),
    )
    .await
    .unwrap();
    assert_eq!(objects_result.num_rows(), 2);
    let objects = objects_result.batches()[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let ids = objects
        .column_by_name("@id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ids.value(0), "Alice");
    assert_eq!(ids.value(1), "Bob");
    assert!(objects.column_by_name("id").is_none());
    assert!(objects.column_by_name("__id").is_none());

    mutate_main(
        &mut db,
        "query raise($name: String, $age: I32) {\n    update Person set { age: $age } where @id = $name\n}",
        "raise",
        &{
            let mut params = ParamMap::new();
            params.insert("name".to_string(), Literal::String("Alice".to_string()));
            params.insert("age".to_string(), Literal::Integer(31));
            params
        },
    )
    .await
    .unwrap();
    mutate_main(
        &mut db,
        "query fire($name: String) {\n    delete Person where @id = $name\n}",
        "fire",
        &{
            let mut params = ParamMap::new();
            params.insert("name".to_string(), Literal::String("Bob".to_string()));
            params
        },
    )
    .await
    .unwrap();
    let people = read_table(&db, "node:Person").await;
    assert_eq!(
        collect_column_strings(&people, "id"),
        ["Alice"],
        "update and delete by key must run on the legacy spellings"
    );
    let edges = read_table(&db, "edge:WorksAt").await;
    assert_eq!(
        collect_column_strings(&edges, "src"),
        ["Alice"],
        "cascade delete must remove Bob's edge via the legacy src column"
    );

    let changed_commit = snapshot_id(&db, "main").await.unwrap();
    let diff = db
        .diff_commits(
            loaded_commit.as_str(),
            changed_commit.as_str(),
            &ChangeFilter::default(),
        )
        .await
        .unwrap();
    assert_eq!(diff.changes.len(), 3);
    assert!(
        diff.changes
            .iter()
            .any(|change| change.id == "Alice" && change.op == ChangeOp::Update)
    );
    assert!(
        diff.changes
            .iter()
            .any(|change| change.id == "Bob" && change.op == ChangeOp::Delete)
    );
    let deleted_edge = diff
        .changes
        .iter()
        .find(|change| change.id == "works-bob")
        .unwrap();
    assert_eq!(deleted_edge.op, ChangeOp::Delete);
    let endpoints = deleted_edge.endpoints.as_ref().unwrap();
    assert_eq!(endpoints.src, "Bob");
    assert_eq!(endpoints.dst, "company-1");
    let page = db
        .commit_changes_page(
            changed_commit.as_str(),
            &ChangeFeedScope::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(page.block.changes.len(), 2);
    assert!(
        page.block
            .changes
            .iter()
            .all(|change| change.op == ChangeOpKind::Delete
                && change.before.is_some()
                && change.after.is_none())
    );

    db.apply_schema(
        r#"
node Person {
    name: String @key
    age: I32?
    _row_note: String?
}
node Company {
    name: String
}
edge WorksAt: Person -> Company {
    title: String?
}
"#,
    )
    .await
    .expect("legacy evolution restates historically legal underscore names");
    let ir: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(dir.path().join("_schema.ir.json")).unwrap())
            .unwrap();
    assert_eq!(
        ir["ir_version"].as_u64(),
        Some(u64::from(SCHEMA_IR_VERSION)),
        "ordinary evolution must re-emit the accepted vintage"
    );

    let historical = db
        .run_query_at(
            loaded_version,
            "query old_people() { match { $p: Person } return { $p.@id, $p.name } order { $p.@id asc } }",
            "old_people",
            &ParamMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(historical.num_rows(), 2);
    assert_eq!(
        collect_column_strings(historical.batches(), "p.@id"),
        ["Alice", "Bob"]
    );
    assert_eq!(
        collect_column_strings(historical.batches(), "p.name"),
        ["Alice", "Bob"]
    );

    let id_claim = db
        .apply_schema(
            r#"
node Person {
    name: String @key
    age: I32?
    _row_note: String?
    id: String?
}
node Company {
    name: String
}
edge WorksAt: Person -> Company {
    title: String?
}
"#,
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(
        id_claim.contains("collides with this graph's physical"),
        "unexpected error: {id_claim}"
    );

    let upgrade_claim = db
        .apply_schema(
            r#"
node Person {
    name: String @key
    age: I32?
    _row_note: String?
    __id: String?
}
node Company {
    name: String
}
edge WorksAt: Person -> Company {
    title: String?
}
"#,
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(
        upgrade_claim.contains("reserved for the system column upgrade"),
        "unexpected error: {upgrade_claim}"
    );

    drop(db);
    let mut reopened = Omnigraph::open(uri).await.unwrap();
    assert_eq!(
        reopened.catalog().system_columns.id,
        "id",
        "a reopened handle must resolve the spellings from the stored authority"
    );
    assert_eq!(reopened.catalog().system_columns.src, "src");
    let result = query_main(
        &mut reopened,
        "query people() {\n    match {\n        $p: Person\n    }\n    return { $p.name }\n}",
        "people",
        &ParamMap::new(),
    )
    .await
    .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[tokio::test]
async fn legacy_endpoint_constraints_keep_the_accepted_shape_hash() {
    for constraint in ["@unique(src, dst)", "@key(src, dst)"] {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let source = format!(
            "node Person {{ name: String @key }}\nedge Knows: Person -> Person {{\n since: I32?\n {constraint}\n @unique(dst, since)\n}}"
        );
        let db = Omnigraph::init_with_legacy_system_columns_for_tests(uri, &source)
            .await
            .unwrap();
        let old_state: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(dir.path().join("__schema_state.json")).unwrap(),
        )
        .unwrap();
        let old_ir: omnigraph_compiler::SchemaIR =
            serde_json::from_str(&fs::read_to_string(dir.path().join("_schema.ir.json")).unwrap())
                .unwrap();
        let old_shape = omnigraph_compiler::compile_schema_shape(
            &omnigraph_compiler::schema::parser::parse_persisted_schema_contract(&source).unwrap(),
        )
        .unwrap();
        assert_eq!(
            old_state["schema_shape_hash"],
            omnigraph_compiler::schema_shape_hash(&old_shape).unwrap()
        );
        assert_eq!(
            old_state["schema_shape_hash"],
            omnigraph_compiler::schema_shape_hash_from_ir(&old_ir).unwrap()
        );
        drop(db);
        let db = Omnigraph::open(uri).await.unwrap();
        load_jsonl(
            &db,
            r#"{"type":"Person","data":{"name":"alice"}}
{"type":"Person","data":{"name":"bob"}}
{"edge":"Knows","from":"alice","to":"bob","data":{"since":2020}}"#,
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
        let desired = source
            .replace("src", "@src")
            .replace("dst", "@dst")
            .replace("since: I32?", "since: I32?\n label: String?");
        assert!(db.plan_schema(&desired).await.unwrap().supported);
        db.apply_schema(&desired).await.unwrap();
        if constraint.starts_with("@unique") {
            let before = version_main(&db).await.unwrap();
            let head = snapshot_id(&db, "main").await.unwrap();
            let error = load_jsonl(&db,
                r#"{"edge":"Knows","id":"duplicate","from":"alice","to":"bob","data":{"since":2021}}"#,
                LoadMode::Append).await.unwrap_err();
            assert!(
                error.to_string().to_lowercase().contains("unique"),
                "{error}"
            );
            assert_eq!(version_main(&db).await.unwrap(), before);
            assert_eq!(snapshot_id(&db, "main").await.unwrap(), head);
            assert_eq!(count_rows(&db, "edge:Knows").await, 1);
        }
        for file in [
            "_schema.pg.staging",
            "_schema.ir.json.staging",
            "__schema_state.json.staging",
        ] {
            assert!(!dir.path().join(file).exists());
        }
        drop(db);
        let mut db = Omnigraph::open(uri).await.unwrap();
        let result = query_main(&mut db,
            "query endpoints() { match { $p: Person\n        $p $e:knows $f } return { min($e.@src), max($e.@dst) } }",
            "endpoints", &ParamMap::new()).await.unwrap();
        assert_eq!(
            collect_column_strings(result.batches(), "e.@src"),
            ["alice"]
        );
        assert_eq!(collect_column_strings(result.batches(), "e.@dst"), ["bob"]);
    }
}
