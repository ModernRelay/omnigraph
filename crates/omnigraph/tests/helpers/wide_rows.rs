//! Graphs whose rows reach an ordered scan's sort input wider than its
//! 37.5 MiB single-batch hard cap (`ORDERED_SCAN_MEMORY_BYTES / 4`).
//!
//! Two shapes trip the cap when complete rows are sorted:
//!
//! * [`init_wide_row_graph`]: one logical row genuinely wider than the cap.
//! * [`init_sliced_parent_graph`]: thousands of ordinary rows (a few KiB each)
//!   in one fragment. Lance's byte-targeted scan slices one decoded batch
//!   without copying and keeps re-slicing the tail, so it can hand the sort a
//!   one-row slice whose Arrow memory size counts the whole decoded parent
//!   buffer. No row is anywhere near the cap.
//!
//! Every id-ordered walk must therefore sort keys only and hydrate rows in
//! bounded chunks.

use omnigraph::db::Omnigraph;
use omnigraph::loader::LoadMode;

use super::Session;

pub const WIDE_ROW_SCHEMA: &str = r#"
node Doc {
    key: String @key
    payload: String?
}
"#;

pub const WIDE_ROW_SET_PAYLOAD: &str = "query set_payload($key: String, $payload: String) {\n    update Doc set { payload: $payload } where key = $key\n}";

/// Comfortably past the 150 MiB / 4 = 37.5 MiB SortExec input cap.
pub const WIDE_PAYLOAD_BYTES: usize = 40 * 1024 * 1024;

/// One `Doc` table whose `wide` row decodes past the 37.5 MiB ordered-scan
/// single-row hard cap, plus three small rows. Overwrite load deliberately
/// bypasses the keyed 32 MiB Arrow envelope (bulk-replacement contract), so a
/// wider-than-cap logical row is legitimate pre-existing table state.
pub async fn init_wide_row_graph(dir: &tempfile::TempDir, wide_payload_bytes: usize) -> Session {
    let uri = dir.path().to_str().unwrap();
    let main = super::session(Omnigraph::init(uri, WIDE_ROW_SCHEMA).await.unwrap());
    let mut rows = serde_json::json!({
        "type": "Doc",
        "data": { "key": "wide", "payload": "x".repeat(wide_payload_bytes) },
    })
    .to_string();
    for key in ["small-0", "small-1", "small-2"] {
        rows.push('\n');
        rows.push_str(
            &serde_json::json!({
                "type": "Doc",
                "data": { "key": key, "payload": "tiny" },
            })
            .to_string(),
        );
    }
    main.load("main", &rows, LoadMode::Overwrite).await.unwrap();
    main
}

/// Rows in the sliced-parent fixture: one full default scan batch.
pub const SLICED_PARENT_ROWS: usize = 8192;
/// Bytes per row: the fragment's decoded batch is ~46 MiB, past the cap,
/// while each row stays a few KiB.
pub const SLICED_PARENT_ROW_BYTES: usize = 5632;

/// One `Doc` fragment of [`SLICED_PARENT_ROWS`] ordinary rows, written by a
/// single overwrite load. Asserts the single-fragment precondition the shape
/// depends on.
pub async fn init_sliced_parent_graph(dir: &tempfile::TempDir) -> Session {
    let uri = dir.path().to_str().unwrap();
    let main = super::session(Omnigraph::init(uri, WIDE_ROW_SCHEMA).await.unwrap());
    let payload = "y".repeat(SLICED_PARENT_ROW_BYTES);
    let rows = (0..SLICED_PARENT_ROWS)
        .map(|row| {
            serde_json::json!({
                "type": "Doc",
                "data": { "key": format!("row-{row:05}"), "payload": payload },
            })
            .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n");
    main.load("main", &rows, LoadMode::Overwrite).await.unwrap();
    let docs = super::open_pinned_dataset_for_test(&main, "main", "node:Doc").await;
    assert_eq!(
        docs.fragments().len(),
        1,
        "precondition: every row lands in one fragment"
    );
    main
}
