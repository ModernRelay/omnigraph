//! End-to-end coverage for a genuine legacy-vintage graph (schema IR version
//! 2, system columns physically spelled `id`/`src`/`dst`): the population
//! every pre-RFC-0040 deployment consists of. Everything else in the suite
//! exercises new-vintage graphs, so a hardcoded-current-spelling regression
//! on any runtime path would be invisible without this file.
#![cfg(feature = "failpoints")]

mod helpers;

use std::fs;

use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::{Literal, SCHEMA_IR_VERSION_LEGACY_COLUMNS};

use helpers::*;

const LEGACY_SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I32?
}
node Company {
    name: String @key
}
edge WorksAt: Person -> Company {
    title: String?
}
"#;

const LEGACY_DATA: &str = r#"{"type":"Person","data":{"name":"Alice","age":30}}
{"type":"Person","data":{"name":"Bob","age":25}}
{"type":"Company","data":{"name":"Acme"}}
{"edge":"WorksAt","from":"Alice","to":"Acme","data":{"title":"engineer"}}
{"edge":"WorksAt","from":"Bob","to":"Acme","data":{}}"#;

#[tokio::test]
async fn legacy_vintage_graph_works_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let mut db = Omnigraph::init_with_legacy_system_columns_for_tests(uri, LEGACY_SCHEMA)
        .await
        .unwrap();

    // The persisted schema authority records the legacy vintage.
    let ir: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(dir.path().join("_schema.ir.json")).unwrap())
            .unwrap();
    assert_eq!(
        ir["ir_version"].as_u64(),
        Some(u64::from(SCHEMA_IR_VERSION_LEGACY_COLUMNS))
    );

    // Physical tables carry the legacy spellings, and only those.
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
        assert!(dataset.schema().field("__id").is_none());
    }
    let works_at = snap.open_dataset("edge:WorksAt").await.unwrap();
    assert!(works_at.schema().field("src").is_some());
    assert!(works_at.schema().field("dst").is_some());
    assert!(works_at.schema().field("__src").is_none());

    load_jsonl(&db, LEGACY_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    // Reads resolve the legacy spellings through the catalog.
    let people = read_table(&db, "node:Person").await;
    let mut ids = collect_column_strings(&people, "id");
    ids.sort();
    assert_eq!(ids, ["Alice", "Bob"]);
    let edges = read_table(&db, "edge:WorksAt").await;
    assert_eq!(collect_column_strings(&edges, "src"), ["Alice", "Bob"]);
    assert_eq!(collect_column_strings(&edges, "dst"), ["Acme", "Acme"]);

    // Traversal joins on the legacy endpoint columns.
    let result = query_main(
        &mut db,
        "query coworkers() {\n    match {\n        $p: Person\n        $p worksat $c\n    }\n    return { $p.name, $c.name }\n}",
        "coworkers",
        &ParamMap::new(),
    )
    .await
    .unwrap();
    assert_eq!(result.num_rows(), 2);

    // Mutation by key and cascade delete run on the legacy spellings.
    mutate_main(
        &mut db,
        "query raise($name: String, $age: I32) {\n    update Person set { age: $age } where name = $name\n}",
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
        "query fire($name: String) {\n    delete Person where name = $name\n}",
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
    assert_eq!(collect_column_strings(&people, "id"), ["Alice"]);
    let edges = read_table(&db, "edge:WorksAt").await;
    assert_eq!(
        collect_column_strings(&edges, "src"),
        ["Alice"],
        "cascade delete must remove Bob's edge via the legacy src column"
    );

    // Ordinary schema evolution keeps the vintage, and historically legal
    // underscore names stay restatable; the freed and upgrade-target names
    // stay reserved.
    db.apply_schema(
        r#"
node Person {
    name: String @key
    age: I32?
    _row_note: String?
}
node Company {
    name: String @key
}
edge WorksAt: Person -> Company {
    title: String?
}
"#,
    )
    .await
    .unwrap();
    let ir: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(dir.path().join("_schema.ir.json")).unwrap())
            .unwrap();
    assert_eq!(
        ir["ir_version"].as_u64(),
        Some(u64::from(SCHEMA_IR_VERSION_LEGACY_COLUMNS)),
        "ordinary evolution must re-emit the accepted vintage"
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
    name: String @key
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
    name: String @key
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

    // A reopened handle resolves the spellings from the stored authority.
    drop(db);
    let mut reopened = Omnigraph::open(uri).await.unwrap();
    assert_eq!(reopened.catalog().system_columns.id, "id");
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
