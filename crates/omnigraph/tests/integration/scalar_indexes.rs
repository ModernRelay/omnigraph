//! Coverage for `build_indices_on_dataset_for_catalog`'s per-property index
//! dispatch: which scalar/vector index each `@index`/`@key` column gets.
//!
//! The observable signal is `SnapshotDataset::index_coverage`, which
//! reports `Indexed` only when a BTREE covers the column (the same helper the
//! traversal chooser uses). Enums and orderable scalars must get a BTREE so
//! `=`/range/IN/IS NULL are index-accelerated; free-text Strings keep FTS
//! only (a companion BTREE is deferred on the second-generation clone
//! index-read bug — see `lance_surface_guards`).

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
    note: String?
}
"#;

const DATA: &str = r#"{"type":"Item","data":{"slug":"a","status":"active","published":"2024-06-01T00:00:00Z","rank":1,"title":"alpha","note":"n1"}}
{"type":"Item","data":{"slug":"b","status":"archived","published":"2023-01-01T00:00:00Z","rank":2,"title":"beta","note":"n2"}}
{"type":"Item","data":{"slug":"c","status":"active","published":"2025-02-02T00:00:00Z","rank":3,"title":"gamma","note":"n3"}}"#;

// Enums and orderable scalars (DateTime, numeric) get a BTREE from the index
// reconciler, so a `=`/range filter on them uses the index. Free-text
// String `@index` keeps FTS only (its companion BTREE is deferred on the
// upstream clone-of-clone index-read bug), and an un-annotated column has no
// scalar index — both report `Degraded`, the negative control that keeps
// this test from being vacuously green. A second `ensure_indices` run must
// be a no-op.
#[tokio::test]
async fn node_scalar_and_enum_index_columns_get_btree() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = session(Omnigraph::init(uri, SCHEMA).await.unwrap());
    db.load_jsonl(DATA, LoadMode::Overwrite).await.unwrap();
    db.ensure_indices_as(None).await.unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open_dataset("node:Item").await.unwrap();

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
        "free-text String @index keeps FTS only while the companion BTREE is deferred, got {title_cov:?}"
    );
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

// (The companion-BTREE naming-collision regression — a column literally
// named `{other}_btree` colliding with another column's companion index name
// — lives on the `dual-btree-companion` branch with the deferred dispatch.)

// #1385: `ensure_indices` builds independent BTREE indexes concurrently, both
// within one table and across tables, with at most four in Lance's builder at
// once. The probe's rendezvous parks the first two builds until both have
// entered, so a serial build loop times out instead of passing on result
// equivalence alone.
const WIDE_SCHEMA: &str = r#"
node Item {
    slug: String @key
    status: enum(active, archived) @index
    published: DateTime @index
    rank: I32 @index
    weight: I64 @index
    born: Date @index
}
"#;

const WIDE_DATA: &str = r#"{"type":"Item","data":{"slug":"a","status":"active","published":"2024-06-01T00:00:00Z","rank":1,"weight":10,"born":"2020-01-01"}}
{"type":"Item","data":{"slug":"b","status":"archived","published":"2023-01-01T00:00:00Z","rank":2,"weight":20,"born":"2021-01-01"}}
{"type":"Item","data":{"slug":"c","status":"active","published":"2025-02-02T00:00:00Z","rank":3,"weight":30,"born":"2022-01-01"}}"#;

const TWO_TABLE_SCHEMA: &str = r#"
node Left {
    name: String
}
node Right {
    name: String
}
"#;

const TWO_TABLE_DATA: &str = r#"{"type":"Left","data":{"name":"l1"}}
{"type":"Left","data":{"name":"l2"}}
{"type":"Right","data":{"name":"r1"}}
{"type":"Right","data":{"name":"r2"}}"#;

async fn ensure_indices_under_rendezvous(db: &Omnigraph) -> (u64, u64) {
    use omnigraph::instrumentation::{IndexBuildProbes, with_index_build_probes};

    let probes = IndexBuildProbes::rendezvous(2);
    let observed = probes.clone();
    let build = with_index_build_probes(probes, Box::pin(db.ensure_indices_as(None)));
    tokio::time::timeout(std::time::Duration::from_secs(30), build)
        .await
        .expect("index build ran its BTREE builds one at a time")
        .unwrap();
    (observed.entered(), observed.peak_in_flight())
}

#[tokio::test]
async fn index_build_overlaps_btree_builds_of_one_table_issue_1385() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = session(Omnigraph::init(uri, WIDE_SCHEMA).await.unwrap());
    db.load_jsonl(WIDE_DATA, LoadMode::Overwrite).await.unwrap();

    let (entered, peak) = ensure_indices_under_rendezvous(&db).await;
    assert!(
        entered >= 6,
        "the table needs its id BTREE and five @index BTREEs, saw {entered} builds"
    );
    assert!(
        (2..=4).contains(&peak),
        "independent BTREE builds must overlap, at most four at once; peak was {peak}"
    );

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open_dataset("node:Item").await.unwrap();
    for col in ["status", "published", "rank", "weight", "born"] {
        assert_eq!(
            ds.index_coverage(col).await.unwrap(),
            IndexCoverage::Indexed,
            "column '{col}' must be covered by its BTREE after a concurrent build"
        );
    }
    let version_before = ds.published_dataset_version();
    db.ensure_indices_as(None).await.unwrap();
    let ds2 = snapshot_main(&db)
        .await
        .unwrap()
        .open_dataset("node:Item")
        .await
        .unwrap();
    assert_eq!(
        version_before,
        ds2.published_dataset_version(),
        "a second index build after a concurrent one must find nothing to do"
    );
}

#[tokio::test]
async fn index_build_overlaps_tables_issue_1385() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = session(Omnigraph::init(uri, TWO_TABLE_SCHEMA).await.unwrap());
    db.load_jsonl(TWO_TABLE_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    let (entered, peak) = ensure_indices_under_rendezvous(&db).await;
    assert_eq!(
        entered, 2,
        "each table needs exactly its id BTREE, so two builds"
    );
    assert_eq!(peak, 2, "the two tables' builds must overlap");

    let snap = snapshot_main(&db).await.unwrap();
    for table in ["node:Left", "node:Right"] {
        let ds = snap.open_dataset(table).await.unwrap();
        assert!(
            ds.has_btree_index("__id").await.unwrap(),
            "{table} must have its id BTREE"
        );
    }
}
