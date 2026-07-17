mod helpers;

use std::collections::HashSet;

use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::Literal;

use helpers::*;

const EXPORT_MUTATIONS: &str = r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}

query add_friend($from: String, $to: String) {
    insert Knows { from: $from, to: $to }
}
"#;

const NOTE_SCHEMA: &str = r#"
node Note {
    text: String
}

edge References: Note -> Note
"#;

const NOTE_DATA: &str = r#"
{"type":"Note","data":{"id":"note-1","text":"Alpha"}}
{"type":"Note","data":{"id":"note-2","text":"Beta"}}
{"edge":"References","from":"note-1","to":"note-2","data":{"id":"edge-1"}}
"#;

const U64_KEY_SCHEMA: &str = r#"
node Counter {
    sequence: U64 @key
    label: String
}
"#;

const U64_KEY_MUTATION: &str = r#"
query insert_counter($sequence: U64, $label: String) {
    insert Counter { sequence: $sequence, label: $label }
}
"#;

const LEGACY_TEMPORAL_KEY_SCHEMA: &str = r#"
node CalendarDay {
    day: Date @key
}

node Instant {
    happened_at: DateTime @key
}

edge OccursOn: Instant -> CalendarDay
"#;

const LEGACY_TEMPORAL_KEY_DATA: &str = r#"
{"edge":"OccursOn","from":"2024-01-01T00:00:00Z","to":"2024-01-01","data":{"id":"legacy-edge"}}
{"type":"CalendarDay","data":{"id":"2024-01-01","day":19723}}
{"type":"Instant","data":{"id":"2024-01-01T00:00:00Z","happened_at":"2024-01-01T00:00:00Z"}}
"#;

const LEGACY_TYPED_REMAP_SCHEMA: &str = r#"
node Exact {
    value: I32 @key
}

node Rounded {
    value: F32 @key
}

node Padded {
    value: U32 @key
}

edge Converts: Exact -> Rounded
"#;

const LEGACY_TYPED_REMAP_DATA: &str = r#"
{"edge":"Converts","from":"16777217","to":"16777217","data":{"id":"conversion"}}
{"type":"Rounded","data":{"id":"16777217","value":16777217}}
{"type":"Padded","data":{"id":"00042","value":42}}
{"type":"Exact","data":{"id":"16777217","value":16777217}}
"#;

const COMPOSITE_KEY_SCHEMA: &str = r#"
node Membership {
    tenant: String
    slot: U32
    label: String
    @key(tenant, slot)
}

edge Related: Membership -> Membership
"#;

const COMPOSITE_KEY_MUTATION: &str = r#"
query put_membership($tenant: String, $slot: U32, $label: String) {
    insert Membership { tenant: $tenant, slot: $slot, label: $label }
}

query move_membership($tenant: String, $next_slot: U32) {
    update Membership set { slot: $next_slot } where tenant = $tenant
}
"#;

const RENAMED_COMPOSITE_KEY_SCHEMA: &str = r#"
node RenamedPair {
    aaaa: Date @rename_from("zeta")
    zzzz: U32 @rename_from("alpha")
    label: String
    @key(aaaa, zzzz)
}

edge Connects: RenamedPair -> RenamedPair
"#;

const NARROWING_SCHEMA: &str = r#"
node SignedBoundary {
    value: I32 @key
}

node UnsignedBoundary {
    value: U32 @key
}

node FloatBoundary {
    value: F32 @key
}
"#;

const NARROWING_MUTATIONS: &str = r#"
query put_signed($value: I32) {
    insert SignedBoundary { value: $value }
}

query put_unsigned($value: U32) {
    insert UnsignedBoundary { value: $value }
}

query put_float($value: F32) {
    insert FloatBoundary { value: $value }
}
"#;

fn collect_u64_key_rows(batches: &[RecordBatch]) -> Vec<(u64, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let keys = batch
            .column_by_name("sequence")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|row| (keys.value(row), ids.value(row).to_string())));
    }
    rows.sort_by_key(|(key, _)| *key);
    rows
}

async fn assert_exact_id_primary_key(db: &Omnigraph, table_key: &str) {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let dataset = snapshot.open(table_key).await.unwrap();
    let primary_key = dataset
        .schema()
        .unenforced_primary_key()
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        primary_key,
        ["id"],
        "later import must preserve exactly `id` as the Lance unenforced primary key for {table_key}"
    );
}

#[tokio::test]
async fn export_jsonl_round_trips_branch_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    db.branch_create_from(ReadTarget::branch("main"), "feature")
        .await
        .unwrap();
    db.mutate(
        "feature",
        EXPORT_MUTATIONS,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 29)]),
    )
    .await
    .unwrap();
    db.mutate(
        "feature",
        EXPORT_MUTATIONS,
        "add_friend",
        &params(&[("$from", "Eve"), ("$to", "Alice")]),
    )
    .await
    .unwrap();

    let source_identity_domain = db
        .catalog()
        .bound_schema_ir()
        .unwrap()
        .schema_identity_domain
        .as_str()
        .to_string();
    let source_commit_ids = db
        .list_commits(Some("main"))
        .await
        .unwrap()
        .into_iter()
        .chain(db.list_commits(Some("feature")).await.unwrap())
        .map(|commit| commit.graph_commit_id)
        .collect::<HashSet<_>>();

    let main_jsonl = db.export_jsonl("main", &[], &[]).await.unwrap();
    let feature_jsonl = db.export_jsonl("feature", &[], &[]).await.unwrap();

    let imported_main_dir = tempfile::tempdir().unwrap();
    let imported_feature_dir = tempfile::tempdir().unwrap();
    let mut imported_main =
        Omnigraph::init(imported_main_dir.path().to_str().unwrap(), TEST_SCHEMA)
            .await
            .unwrap();
    let mut imported_feature =
        Omnigraph::init(imported_feature_dir.path().to_str().unwrap(), TEST_SCHEMA)
            .await
            .unwrap();
    load_jsonl(&mut imported_main, &main_jsonl, LoadMode::Overwrite)
        .await
        .unwrap();
    load_jsonl(&mut imported_feature, &feature_jsonl, LoadMode::Overwrite)
        .await
        .unwrap();

    assert_eq!(count_rows(&db, "node:Person").await, 4);
    assert_eq!(count_rows_branch(&db, "feature", "node:Person").await, 5);
    assert_eq!(count_rows(&imported_main, "node:Person").await, 4);
    assert_eq!(count_rows(&imported_feature, "node:Person").await, 5);
    assert_eq!(count_rows(&imported_main, "edge:Knows").await, 3);
    assert_eq!(count_rows(&imported_feature, "edge:Knows").await, 4);

    // A branch export is a data snapshot, not a clone of graph identity,
    // branch refs, or commit ancestry. Each rebuild is therefore an
    // independent graph root even when both use the same schema source.
    assert_eq!(imported_main.branch_list().await.unwrap(), ["main"]);
    assert_eq!(imported_feature.branch_list().await.unwrap(), ["main"]);
    let imported_main_domain = imported_main
        .catalog()
        .bound_schema_ir()
        .unwrap()
        .schema_identity_domain
        .as_str()
        .to_string();
    let imported_feature_domain = imported_feature
        .catalog()
        .bound_schema_ir()
        .unwrap()
        .schema_identity_domain
        .as_str()
        .to_string();
    assert_ne!(imported_main_domain, source_identity_domain);
    assert_ne!(imported_feature_domain, source_identity_domain);
    assert_ne!(imported_main_domain, imported_feature_domain);

    for (name, rebuilt) in [
        ("main snapshot", &imported_main),
        ("feature snapshot", &imported_feature),
    ] {
        let commits = rebuilt.list_commits(Some("main")).await.unwrap();
        let rebuilt_ids = commits
            .iter()
            .map(|commit| commit.graph_commit_id.as_str())
            .collect::<HashSet<_>>();
        assert!(
            rebuilt_ids
                .iter()
                .all(|commit_id| !source_commit_ids.contains(*commit_id)),
            "{name} rebuild must not inherit source commit ids"
        );
        for commit in &commits {
            assert!(
                commit
                    .parent_commit_id
                    .as_deref()
                    .is_none_or(|parent| rebuilt_ids.contains(parent)),
                "{name} commit {} points outside its rebuilt history: {:?}",
                commit.graph_commit_id,
                commit.parent_commit_id
            );
            assert!(
                commit.merged_parent_commit_id.is_none(),
                "{name} rebuild must not import source merge ancestry"
            );
        }
    }
}

#[tokio::test]
async fn export_jsonl_round_trips_typed_u64_key_and_rejects_id_mismatch() {
    let source_dir = tempfile::tempdir().unwrap();
    let source = Omnigraph::init(source_dir.path().to_str().unwrap(), U64_KEY_SCHEMA)
        .await
        .unwrap();

    // Exercise the mutation path at its current signed-literal ceiling, then
    // exercise JSON import above that boundary. Both routes must derive the
    // same physical string id from the stored U64 value.
    source
        .mutate(
            "main",
            U64_KEY_MUTATION,
            "insert_counter",
            &mixed_params(&[("$label", "mutation")], &[("$sequence", i64::MAX)]),
        )
        .await
        .unwrap();
    load_jsonl(
        &source,
        &format!(
            "{{\"type\":\"Counter\",\"data\":{{\"sequence\":{},\"label\":\"loader\"}}}}",
            u64::MAX
        ),
        LoadMode::Append,
    )
    .await
    .unwrap();

    let expected = vec![
        (i64::MAX as u64, i64::MAX.to_string()),
        (u64::MAX, u64::MAX.to_string()),
    ];
    assert_eq!(
        collect_u64_key_rows(&read_table(&source, "node:Counter").await),
        expected
    );
    assert_exact_id_primary_key(&source, "node:Counter").await;

    let exported = source.export_jsonl("main", &[], &[]).await.unwrap();
    let mut exported_keys = Vec::new();
    let mut mismatched_rows = Vec::new();
    for line in exported.lines() {
        let mut row: serde_json::Value = serde_json::from_str(line).unwrap();
        let key = row["data"]["sequence"].as_u64().unwrap();
        let id = row["data"]["id"].as_str().unwrap();
        assert_eq!(id, key.to_string(), "exported id must match typed U64 key");
        exported_keys.push(key);
        if key == u64::MAX {
            row["data"]["id"] = serde_json::Value::String("wrong-id".to_string());
        }
        mismatched_rows.push(serde_json::to_string(&row).unwrap());
    }
    exported_keys.sort_unstable();
    assert_eq!(exported_keys, [i64::MAX as u64, u64::MAX]);

    let rebuilt_dir = tempfile::tempdir().unwrap();
    let rebuilt = Omnigraph::init(rebuilt_dir.path().to_str().unwrap(), U64_KEY_SCHEMA)
        .await
        .unwrap();
    let mismatch = load_jsonl(&rebuilt, &mismatched_rows.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap_err();
    assert!(
        mismatch.to_string().contains("does not match @key"),
        "explicit physical id mismatch must fail clearly: {mismatch}"
    );
    assert_eq!(count_rows(&rebuilt, "node:Counter").await, 0);

    load_jsonl(&rebuilt, &exported, LoadMode::Overwrite)
        .await
        .unwrap();
    assert_eq!(
        collect_u64_key_rows(&read_table(&rebuilt, "node:Counter").await),
        expected
    );
    assert_exact_id_primary_key(&rebuilt, "node:Counter").await;
}

#[tokio::test]
async fn legacy_temporal_key_ids_are_canonicalized_with_edge_remap_and_round_trip() {
    let source_dir = tempfile::tempdir().unwrap();
    let source = Omnigraph::init(
        source_dir.path().to_str().unwrap(),
        LEGACY_TEMPORAL_KEY_SCHEMA,
    )
    .await
    .unwrap();

    // This is the genuine old-export shape: the physical id retains the
    // mutation literal spelling, while the key property is the exported typed
    // Arrow value. The loader accepts typed equality, persists the canonical
    // id, and rewrites edges even though the edge appears before its nodes.
    load_jsonl(&source, LEGACY_TEMPORAL_KEY_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    assert_eq!(
        collect_column_strings(&read_table(&source, "node:CalendarDay").await, "id"),
        ["19723"]
    );
    assert_eq!(
        collect_column_strings(&read_table(&source, "node:Instant").await, "id"),
        ["1704067200000"]
    );

    let source_edges = read_table(&source, "edge:OccursOn").await;
    assert_eq!(
        collect_column_strings(&source_edges, "src"),
        ["1704067200000"]
    );
    assert_eq!(collect_column_strings(&source_edges, "dst"), ["19723"]);

    for (table_key, canonical_id, duplicate_row) in [
        (
            "node:CalendarDay",
            "19723",
            r#"{"type":"CalendarDay","data":{"day":19723}}"#,
        ),
        (
            "node:Instant",
            "1704067200000",
            r#"{"type":"Instant","data":{"happened_at":1704067200000}}"#,
        ),
    ] {
        let duplicate = load_jsonl(&source, duplicate_row, LoadMode::Append)
            .await
            .unwrap_err();
        assert!(
            matches!(
                duplicate,
                OmniError::KeyConflict {
                    table_key: ref conflicted_table,
                    key: Some(ref key)
                } if conflicted_table == table_key && key == canonical_id
            ),
            "post-rebuild append must conflict on canonical temporal id {canonical_id}: {duplicate}"
        );
        assert_eq!(count_rows(&source, table_key).await, 1);
    }

    let exported = source.export_jsonl("main", &[], &[]).await.unwrap();
    let rows = exported
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let day = rows
        .iter()
        .find(|row| row["type"] == "CalendarDay")
        .unwrap();
    assert_eq!(day["data"]["id"], "19723");
    assert_eq!(day["data"]["day"].as_i64(), Some(19_723));
    let instant = rows.iter().find(|row| row["type"] == "Instant").unwrap();
    assert_eq!(instant["data"]["id"], "1704067200000");
    assert_eq!(
        instant["data"]["happened_at"].as_i64(),
        Some(1_704_067_200_000),
        "Date64 export must be an exact JSON integer, not a display fallback"
    );

    let rejected_dir = tempfile::tempdir().unwrap();
    let rejected = Omnigraph::init(
        rejected_dir.path().to_str().unwrap(),
        LEGACY_TEMPORAL_KEY_SCHEMA,
    )
    .await
    .unwrap();
    let mismatch = load_jsonl(
        &rejected,
        r#"{"type":"CalendarDay","data":{"id":"2024-01-02","day":19723}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap_err();
    assert!(
        mismatch.to_string().contains("does not match @key"),
        "a different typed Date must still be rejected: {mismatch}"
    );
    assert_eq!(count_rows(&rejected, "node:CalendarDay").await, 0);

    let rebuilt_dir = tempfile::tempdir().unwrap();
    let rebuilt = Omnigraph::init(
        rebuilt_dir.path().to_str().unwrap(),
        LEGACY_TEMPORAL_KEY_SCHEMA,
    )
    .await
    .unwrap();
    load_jsonl(&rebuilt, &exported, LoadMode::Overwrite)
        .await
        .unwrap();
    assert_eq!(
        collect_column_strings(&read_table(&rebuilt, "node:CalendarDay").await, "id"),
        ["19723"]
    );
    assert_eq!(
        collect_column_strings(&read_table(&rebuilt, "node:Instant").await, "id"),
        ["1704067200000"]
    );
    let rebuilt_edges = read_table(&rebuilt, "edge:OccursOn").await;
    assert_eq!(
        collect_column_strings(&rebuilt_edges, "src"),
        ["1704067200000"]
    );
    assert_eq!(collect_column_strings(&rebuilt_edges, "dst"), ["19723"]);
    assert_exact_id_primary_key(&rebuilt, "node:CalendarDay").await;
    assert_exact_id_primary_key(&rebuilt, "node:Instant").await;
}

#[tokio::test]
async fn legacy_numeric_ids_are_canonicalized_and_edge_remap_is_endpoint_typed() {
    let dir = tempfile::tempdir().unwrap();
    let db = Omnigraph::init(dir.path().to_str().unwrap(), LEGACY_TYPED_REMAP_SCHEMA)
        .await
        .unwrap();

    load_jsonl(&db, LEGACY_TYPED_REMAP_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    assert_eq!(
        collect_column_strings(&read_table(&db, "node:Exact").await, "id"),
        ["16777217"]
    );
    assert_eq!(
        collect_column_strings(&read_table(&db, "node:Rounded").await, "id"),
        ["16777216"],
        "the F32 key must use the value actually stored after width conversion"
    );
    assert_eq!(
        collect_column_strings(&read_table(&db, "node:Padded").await, "id"),
        ["42"],
        "a leading-zero legacy numeric id must rebuild to the typed canonical id"
    );

    let edges = read_table(&db, "edge:Converts").await;
    assert_eq!(collect_column_strings(&edges, "src"), ["16777217"]);
    assert_eq!(collect_column_strings(&edges, "dst"), ["16777216"]);

    let duplicate = load_jsonl(
        &db,
        r#"{"type":"Rounded","data":{"value":16777217}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            duplicate,
            OmniError::KeyConflict {
                ref table_key,
                key: Some(ref key)
            } if table_key == "node:Rounded" && key == "16777216"
        ),
        "a later same-key append must conflict on the canonical id, not duplicate: {duplicate}"
    );
    assert_eq!(count_rows(&db, "node:Rounded").await, 1);

    let collision_dir = tempfile::tempdir().unwrap();
    let collision = Omnigraph::init(
        collision_dir.path().to_str().unwrap(),
        LEGACY_TYPED_REMAP_SCHEMA,
    )
    .await
    .unwrap();
    let collision_error = load_jsonl(
        &collision,
        r#"
{"type":"Padded","data":{"id":"00042","value":42}}
{"type":"Padded","data":{"id":"42","value":42}}
"#,
        LoadMode::Append,
    )
    .await
    .unwrap_err();
    assert!(
        collision_error.to_string().contains("duplicate")
            || collision_error.to_string().contains("@unique violation")
            || matches!(collision_error, OmniError::KeyConflict { .. }),
        "distinct legacy spellings that collapse to one id must fail closed: {collision_error}"
    );
    assert_eq!(count_rows(&collision, "node:Padded").await, 0);
}

#[tokio::test]
async fn composite_key_rebuild_uses_full_tuple_and_rejects_ambiguous_legacy_ids() {
    let dir = tempfile::tempdir().unwrap();
    let db = Omnigraph::init(dir.path().to_str().unwrap(), COMPOSITE_KEY_SCHEMA)
        .await
        .unwrap();
    // Accepted SchemaIR canonicalizes constraint columns, so the bound catalog
    // owns tuple order (`slot`, then `tenant`) on every write surface.
    let canonical = r#"["7","acme"]"#;
    let legacy = r#"
{"edge":"Related","from":"7","to":"7","data":{"id":"related"}}
{"type":"Membership","data":{"id":"7","tenant":"acme","slot":7,"label":"legacy"}}
"#;
    load_jsonl(&db, legacy, LoadMode::Overwrite).await.unwrap();

    assert_eq!(
        collect_column_strings(&read_table(&db, "node:Membership").await, "id"),
        [canonical]
    );
    let edges = read_table(&db, "edge:Related").await;
    assert_eq!(collect_column_strings(&edges, "src"), [canonical]);
    assert_eq!(collect_column_strings(&edges, "dst"), [canonical]);

    let duplicate = load_jsonl(
        &db,
        r#"{"type":"Membership","data":{"tenant":"acme","slot":7,"label":"append"}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap_err();
    assert!(matches!(duplicate, OmniError::KeyConflict { .. }));
    assert_eq!(count_rows(&db, "node:Membership").await, 1);

    db.mutate(
        "main",
        COMPOSITE_KEY_MUTATION,
        "put_membership",
        &mixed_params(
            &[("$tenant", "acme"), ("$label", "updated")],
            &[("$slot", 7)],
        ),
    )
    .await
    .unwrap();
    assert_eq!(count_rows(&db, "node:Membership").await, 1);
    let membership = read_table(&db, "node:Membership").await;
    assert_eq!(collect_column_strings(&membership, "id"), [canonical]);
    assert_eq!(collect_column_strings(&membership, "label"), ["updated"]);

    let key_update = db
        .mutate(
            "main",
            COMPOSITE_KEY_MUTATION,
            "move_membership",
            &mixed_params(&[("$tenant", "acme")], &[("$next_slot", 8)]),
        )
        .await
        .unwrap_err();
    assert!(
        key_update
            .to_string()
            .contains("cannot update @key property 'slot'"),
        "every composite component is immutable: {key_update}"
    );
    assert_eq!(count_rows(&db, "node:Membership").await, 1);
    assert_eq!(
        collect_column_strings(&read_table(&db, "node:Membership").await, "id"),
        [canonical]
    );

    let ambiguous_dir = tempfile::tempdir().unwrap();
    let ambiguous = Omnigraph::init(ambiguous_dir.path().to_str().unwrap(), COMPOSITE_KEY_SCHEMA)
        .await
        .unwrap();
    let ambiguity = load_jsonl(
        &ambiguous,
        r#"
{"type":"Membership","data":{"id":"7","tenant":"acme","slot":7,"label":"one"}}
{"type":"Membership","data":{"id":"7","tenant":"globex","slot":7,"label":"two"}}
"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap_err();
    assert!(
        ambiguity
            .to_string()
            .contains("ambiguous edge endpoint remap"),
        "one legacy first-component id must not choose between tuple ids: {ambiguity}"
    );
    assert_eq!(count_rows(&ambiguous, "node:Membership").await, 0);
}

#[tokio::test]
async fn composite_key_rebuild_accepts_mixed_pre_and_post_rename_scalar_ids() {
    let dir = tempfile::tempdir().unwrap();
    let db = Omnigraph::init(dir.path().to_str().unwrap(), RENAMED_COMPOSITE_KEY_SCHEMA)
        .await
        .unwrap();

    // In v5, physical identity used the first runtime @key component. Before
    // the lexical-crossing rename that was `alpha` (now `zzzz`, U32), while a
    // later row used `aaaa` (formerly `zeta`, Date). A genuine export can thus
    // contain both scalar-id generations under the current schema. Rebuild must
    // accept both by typed equality, persist full tuple ids, and rewrite edge
    // endpoints that still name those old physical ids.
    let legacy = r#"
{"edge":"Connects","from":"7","to":"2024-01-02","data":{"id":"mixed-generation"}}
{"type":"RenamedPair","data":{"id":"7","aaaa":19723,"zzzz":7,"label":"pre-rename"}}
{"type":"RenamedPair","data":{"id":"2024-01-02","aaaa":"2024-01-02","zzzz":8,"label":"post-rename"}}
"#;
    load_jsonl(&db, legacy, LoadMode::Overwrite).await.unwrap();

    let mut ids = collect_column_strings(&read_table(&db, "node:RenamedPair").await, "id");
    ids.sort();
    assert_eq!(ids, [r#"["19723","7"]"#, r#"["19724","8"]"#]);

    let edges = read_table(&db, "edge:Connects").await;
    assert_eq!(collect_column_strings(&edges, "src"), [r#"["19723","7"]"#]);
    assert_eq!(collect_column_strings(&edges, "dst"), [r#"["19724","8"]"#]);
}

#[tokio::test]
async fn numeric_narrowing_rejects_out_of_range_loader_and_mutation_values_pre_effect() {
    let load_dir = tempfile::tempdir().unwrap();
    let loader = Omnigraph::init(load_dir.path().to_str().unwrap(), NARROWING_SCHEMA)
        .await
        .unwrap();
    let boundaries = [
        serde_json::json!({"type":"SignedBoundary","data":{"value":i32::MAX}}),
        serde_json::json!({"type":"UnsignedBoundary","data":{"value":u32::MAX}}),
        serde_json::json!({"type":"FloatBoundary","data":{"value":f32::MAX as f64}}),
    ]
    .into_iter()
    .map(|row| serde_json::to_string(&row).unwrap())
    .collect::<Vec<_>>()
    .join("\n");
    load_jsonl(&loader, &boundaries, LoadMode::Append)
        .await
        .unwrap();

    for (table, bad_value) in [
        ("SignedBoundary", serde_json::json!(i32::MAX as i64 + 1)),
        ("UnsignedBoundary", serde_json::json!(u32::MAX as u64 + 1)),
        ("UnsignedBoundary", serde_json::json!(-1)),
        ("FloatBoundary", serde_json::json!(f64::MAX)),
    ] {
        let row = serde_json::json!({"type":table,"data":{"value":bad_value}}).to_string();
        let error = load_jsonl(&loader, &row, LoadMode::Append)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("range"),
            "{table} out-of-range load must fail clearly: {error}"
        );
        assert_eq!(count_rows(&loader, &format!("node:{table}")).await, 1);
    }

    let mutation_dir = tempfile::tempdir().unwrap();
    let mutation = Omnigraph::init(mutation_dir.path().to_str().unwrap(), NARROWING_SCHEMA)
        .await
        .unwrap();
    mutation
        .mutate(
            "main",
            NARROWING_MUTATIONS,
            "put_signed",
            &int_params(&[("$value", i32::MAX as i64)]),
        )
        .await
        .unwrap();
    for value in [i32::MAX as i64 + 1] {
        let error = mutation
            .mutate(
                "main",
                NARROWING_MUTATIONS,
                "put_signed",
                &int_params(&[("$value", value)]),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Int32 range"));
    }
    assert_eq!(count_rows(&mutation, "node:SignedBoundary").await, 1);

    mutation
        .mutate(
            "main",
            NARROWING_MUTATIONS,
            "put_unsigned",
            &int_params(&[("$value", u32::MAX as i64)]),
        )
        .await
        .unwrap();
    for value in [u32::MAX as i64 + 1, -1] {
        let error = mutation
            .mutate(
                "main",
                NARROWING_MUTATIONS,
                "put_unsigned",
                &int_params(&[("$value", value)]),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("UInt32 range"));
    }
    assert_eq!(count_rows(&mutation, "node:UnsignedBoundary").await, 1);

    let mut float_boundary = ParamMap::new();
    float_boundary.insert("value".to_string(), Literal::Float(f32::MAX as f64));
    mutation
        .mutate("main", NARROWING_MUTATIONS, "put_float", &float_boundary)
        .await
        .unwrap();
    let mut float_overflow = ParamMap::new();
    float_overflow.insert("value".to_string(), Literal::Float(f64::MAX));
    let error = mutation
        .mutate("main", NARROWING_MUTATIONS, "put_float", &float_overflow)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Float32 range"));
    assert_eq!(count_rows(&mutation, "node:FloatBoundary").await, 1);
}

#[tokio::test]
async fn export_jsonl_preserves_explicit_ids_for_non_key_graphs() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Omnigraph::init(dir.path().to_str().unwrap(), NOTE_SCHEMA)
        .await
        .unwrap();
    load_jsonl(&mut db, NOTE_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    let exported = db.export_jsonl("main", &[], &[]).await.unwrap();

    let imported_dir = tempfile::tempdir().unwrap();
    let mut imported = Omnigraph::init(imported_dir.path().to_str().unwrap(), NOTE_SCHEMA)
        .await
        .unwrap();
    load_jsonl(&mut imported, &exported, LoadMode::Overwrite)
        .await
        .unwrap();

    let node_batches = read_table(&imported, "node:Note").await;
    let node_ids = collect_column_strings(&node_batches, "id");
    assert_eq!(node_ids, vec!["note-1".to_string(), "note-2".to_string()]);

    let edge_batches = read_table(&imported, "edge:References").await;
    let edge_ids = collect_column_strings(&edge_batches, "id");
    assert_eq!(edge_ids, vec!["edge-1".to_string()]);

    let srcs = edge_batches[0]
        .column_by_name("src")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let dsts = edge_batches[0]
        .column_by_name("dst")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(srcs.value(0), "note-1");
    assert_eq!(dsts.value(0), "note-2");
}

// ─── Regression: export with blob columns ────────────────────────────────────

#[tokio::test]
async fn export_jsonl_with_blob_type() {
    // Regression: export on types with blob columns failed with
    // "Schema error: Can not append column _rowaddr on schema" because
    // Lance 4's take_blobs duplicated _rowaddr on the unsorted path.
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    const BLOB_SCHEMA: &str = r#"
node Document {
    title: String @key
    content: Blob?
}
"#;

    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();
    let data = concat!(
        "{\"type\": \"Document\", \"data\": {\"title\": \"readme\", \"content\": \"base64:SGVsbG8=\"}}\n",
        "{\"type\": \"Document\", \"data\": {\"title\": \"empty\"}}\n",
    );
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    // Export should succeed
    let exported = db.export_jsonl("main", &[], &[]).await.unwrap();
    assert!(
        exported.contains("readme"),
        "export should contain readme doc"
    );

    // Verify blob value is in the export
    assert!(
        exported.contains("base64:") || exported.contains("SGVsbG8"),
        "export should contain blob data as base64"
    );

    // Round-trip: re-import and verify blob data survives
    let imported_dir = tempfile::tempdir().unwrap();
    let imported_uri = imported_dir.path().to_str().unwrap();
    let mut imported = Omnigraph::init(imported_uri, BLOB_SCHEMA).await.unwrap();
    load_jsonl(&mut imported, &exported, LoadMode::Overwrite)
        .await
        .unwrap();

    let bytes = read_blob_bytes(&imported, "Document", "readme", "content").await;
    assert_eq!(&bytes[..], b"Hello");

    // A later import into the already-populated v6 table must retain both the
    // physical PK contract and blob-v2 fidelity.
    load_jsonl(
        &mut imported,
        r#"{"type":"Document","data":{"title":"later","content":"base64:AAECA/8="}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    assert_exact_id_primary_key(&imported, "node:Document").await;
    let later = read_blob_bytes(&imported, "Document", "later", "content").await;
    assert_eq!(&later[..], &[0, 1, 2, 3, 255]);
}
