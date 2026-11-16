//! Coverage for `build_indices_on_dataset_for_catalog`'s per-property index
//! dispatch: which scalar/vector indexes each `@index`/`@key` column gets
//! (docs/rfcs/2026-11-07-gq-3.md section 4, "Scalar indexes").
//!
//! Orderable scalars get a BTREE, so `SnapshotDataset::index_coverage` (the
//! helper the traversal chooser uses) reports `Indexed`. An enum or `Bool`
//! gets a BITMAP, a list a LABEL_LIST, and a free-text String its full-text
//! index, an NGRAM and a BTREE; each is checked by its index family.

use omnigraph::IndexCoverage;
use omnigraph::db::Omnigraph;
use omnigraph::loader::LoadMode;

use crate::helpers::*;

const SCHEMA: &str = r#"
node Item {
    slug: String @key
    status: enum(active, archived) @index
    published: DateTime @index
    rank: I32 @index
    title: String @index
    active: Bool @index
    tags: [String] @index
    note: String?
}
"#;

const DATA: &str = r#"{"type":"Item","data":{"slug":"a","status":"active","published":"2024-06-01T00:00:00Z","rank":1,"title":"alpha","active":true,"tags":["x"],"note":"n1"}}
{"type":"Item","data":{"slug":"b","status":"archived","published":"2023-01-01T00:00:00Z","rank":2,"title":"beta","active":false,"tags":[],"note":"n2"}}
{"type":"Item","data":{"slug":"c","status":"active","published":"2025-02-02T00:00:00Z","rank":3,"title":"gamma","active":true,"tags":["x","y"],"note":"n3"}}"#;

/// The index families on `column`, by the message name of each index's
/// details (`BTree`, `Bitmap`, `LabelList`, `NGram`, `Inverted`), sorted.
async fn families(ds: &omnigraph::db::SnapshotDataset, column: &str) -> Vec<String> {
    let field = ds.schema().field(column).unwrap().id;
    let mut families: Vec<String> = ds
        .load_indices()
        .await
        .unwrap()
        .iter()
        .filter(|index| index.fields == [field])
        .filter_map(|index| index.index_details.as_ref())
        .filter_map(|details| details.type_url.rsplit('.').next())
        .map(|message| message.trim_end_matches("IndexDetails").to_string())
        .collect();
    families.sort();
    families
}

// Orderable scalars (DateTime, numeric) get a BTREE from the index
// reconciler, so a `=`/range filter on them uses the index. An enum and a
// `Bool` get a BITMAP and no BTREE, a list a LABEL_LIST, and a free-text
// String its full-text index, an NGRAM and a BTREE. An un-annotated column
// has no scalar index and reports `Degraded`, the negative control that keeps
// this test from being vacuously green. A second `ensure_indices` run must be
// a no-op.
#[tokio::test]
async fn node_index_columns_get_the_index_kinds_their_type_serves() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = session(Omnigraph::init(uri, SCHEMA).await.unwrap());
    db.load_jsonl(DATA, LoadMode::Overwrite).await.unwrap();
    db.ensure_indices_as(None).await.unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open_dataset("node:Item").await.unwrap();

    for col in ["published", "rank"] {
        let cov = ds.index_coverage(col).await.unwrap();
        assert_eq!(
            cov,
            IndexCoverage::Indexed,
            "column '{col}' (DateTime/numeric @index) must get a BTREE, got {cov:?}"
        );
    }
    for (col, expected) in [
        ("published", &["BTree"][..]),
        ("rank", &["BTree"][..]),
        ("status", &["Bitmap"][..]),
        ("active", &["Bitmap"][..]),
        ("tags", &["LabelList"][..]),
        ("title", &["BTree", "Inverted", "NGram"][..]),
        ("note", &[][..]),
    ] {
        assert_eq!(families(&ds, col).await, expected, "the indexes of `{col}`");
    }
    assert!(ds.has_fts_index("title").await.unwrap());

    // No @index annotation -> no scalar index at all -> Degraded.
    let note_cov = ds.index_coverage("note").await.unwrap();
    assert!(
        matches!(note_cov, IndexCoverage::Degraded { .. }),
        "un-annotated column should have no scalar index, got {note_cov:?}"
    );

    // Idempotence: a second run finds every declared index present and
    // publishes nothing (dual indexes must not re-plan as missing forever).
    let version_before = ds.published_dataset_version();
    db.ensure_indices_as(None).await.unwrap();
    let snap2 = snapshot_main(&db).await.unwrap();
    let ds2 = snap2.open_dataset("node:Item").await.unwrap();
    assert_eq!(
        version_before,
        ds2.published_dataset_version(),
        "second ensure_indices must be a no-op on a fully-indexed graph"
    );
}
