//! Coverage for `build_indices_on_dataset_for_catalog`'s per-property index
//! dispatch: which scalar/vector index each `@index`/`@key` column gets.
//!
//! The observable signal is `SnapshotTable::index_coverage`, which
//! reports `Indexed` only when a BTREE covers the column (the same helper the
//! traversal chooser uses). Enums and orderable scalars must get a BTREE so
//! `=`/range/IN/IS NULL are index-accelerated; free-text Strings keep FTS
//! (which `key_column_index_coverage` does not count as a BTREE, by design).

mod helpers;

use omnigraph::IndexCoverage;
use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};

use helpers::*;

const SCHEMA: &str = r#"
node Item {
    slug: String @key
    status: enum(active, archived) @index
    published: DateTime @index
    rank: I32 @index
    title: String @index
    note: String?
}
"#;

const DATA: &str = r#"{"type":"Item","data":{"slug":"a","status":"active","published":"2024-06-01T00:00:00Z","rank":1,"title":"alpha","note":"n1"}}
{"type":"Item","data":{"slug":"b","status":"archived","published":"2023-01-01T00:00:00Z","rank":2,"title":"beta","note":"n2"}}
{"type":"Item","data":{"slug":"c","status":"active","published":"2025-02-02T00:00:00Z","rank":3,"title":"gamma","note":"n3"}}"#;

// Enums and orderable scalars (DateTime, numeric) get a BTREE from the index
// reconciler, so a `=`/range filter on them uses the index. Free-text
// String `@index` keeps FTS (no BTREE), and an un-annotated column has no
// scalar index — both report `Degraded`, which is the negative control that
// keeps this test from being vacuously green.
#[tokio::test]
async fn node_scalar_and_enum_index_columns_get_btree() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, SCHEMA).await.unwrap();
    load_jsonl(&mut db, DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open("node:Item").await.unwrap();

    for col in ["status", "published", "rank"] {
        let cov = ds.index_coverage(col).await.unwrap();
        assert_eq!(
            cov,
            IndexCoverage::Indexed,
            "column '{col}' (enum/DateTime/numeric @index) must get a BTREE, got {cov:?}"
        );
    }

    // Free-text String @index -> FTS, which is not a BTREE -> Degraded.
    let title_cov = ds.index_coverage("title").await.unwrap();
    assert!(
        matches!(title_cov, IndexCoverage::Degraded { .. }),
        "free-text String @index should keep FTS (no BTREE), got {title_cov:?}"
    );

    // No @index annotation -> no scalar index at all -> Degraded.
    let note_cov = ds.index_coverage("note").await.unwrap();
    assert!(
        matches!(note_cov, IndexCoverage::Degraded { .. }),
        "un-annotated column should have no scalar index, got {note_cov:?}"
    );
}
