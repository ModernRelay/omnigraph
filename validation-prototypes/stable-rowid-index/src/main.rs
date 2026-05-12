//! MR-927 Phase 1 — Validate stable-row-id behavior for built-in Lance scalar indices.
//!
//! Goal: produce empirical evidence for the MR-927 RFC about whether Lance 4.0
//! built-in scalar indices (BTree, Bitmap, LabelList) survive `compact_files`
//! on a stable-row-id dataset, and answer the side question raised during MR-925's
//! follow-up: does `Operation::Overwrite` preserve the manifest's
//! `uses_stable_row_ids` flag when `WriteParams::enable_stable_row_ids` is NOT
//! set in the rewrite call?
//!
//! Matrix:
//!
//!   For idx in {BTree, Bitmap, LabelList}:
//!     For stable in {true, false}:
//!       - Create dataset, enable_stable_row_ids=stable
//!       - Insert 1000 rows (keys 0..1000)
//!       - Create scalar index of type `idx` on the indexed column
//!       - Probe: scan with filter targeting one row, expect 1 hit
//!       - Append 500 more rows (keys 1000..1500)
//!       - Probe: same filter, expect 1 hit (also probe a new-row key)
//!       - compact_files
//!       - Probe: same filter, expect 1 hit (THIS is the survival check)
//!
//! Plus side experiment for `stage_overwrite` flag preservation.
//!
//! Output: a tabular report. Findings are appended to
//! `.context/experiments/stable-row-id-compaction.md`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::builder::{ListBuilder, StringBuilder, UInt64Builder};
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::optimize::{CompactionOptions, compact_files};
use lance::dataset::{InsertBuilder, WriteMode, WriteParams};
use lance_index::DatasetIndexExt;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_index::IndexType;
use tempfile::TempDir;

#[derive(Copy, Clone, Debug)]
enum Idx {
    BTree,
    Bitmap,
    LabelList,
}

impl Idx {
    fn name(&self) -> &'static str {
        match self {
            Idx::BTree => "BTree",
            Idx::Bitmap => "Bitmap",
            Idx::LabelList => "LabelList",
        }
    }
    fn lance_type(&self) -> IndexType {
        match self {
            Idx::BTree => IndexType::BTree,
            Idx::Bitmap => IndexType::Bitmap,
            Idx::LabelList => IndexType::LabelList,
        }
    }
    fn params(&self) -> ScalarIndexParams {
        match self {
            Idx::BTree => ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
            Idx::Bitmap => ScalarIndexParams::for_builtin(BuiltinIndexType::Bitmap),
            Idx::LabelList => ScalarIndexParams::for_builtin(BuiltinIndexType::LabelList),
        }
    }
}

/// Schema for BTree/Bitmap experiments: a UInt64 key + Utf8 payload.
fn primitive_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn build_primitive(n: u64, key_base: u64) -> RecordBatch {
    let schema = primitive_schema();
    let mut keys = UInt64Builder::with_capacity(n as usize);
    let mut payloads = StringBuilder::new();
    for i in 0..n {
        keys.append_value(key_base + i);
        payloads.append_value(format!("p_{:06}", key_base + i));
    }
    RecordBatch::try_new(
        schema,
        vec![Arc::new(keys.finish()), Arc::new(payloads.finish())],
    )
    .expect("build primitive batch")
}

/// Schema for LabelList: a List<Utf8> column + payload.
fn list_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "labels",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn build_list(n: u64, key_base: u64) -> RecordBatch {
    let schema = list_schema();
    let mut labels = ListBuilder::new(StringBuilder::new());
    let mut payloads = StringBuilder::new();
    for i in 0..n {
        let key = key_base + i;
        // Each row has one label "k<key>" so the filter `array_contains(labels, 'k500')`
        // matches exactly one row (the row whose key is 500).
        labels.values().append_value(format!("k{}", key));
        labels.append(true);
        payloads.append_value(format!("p_{:06}", key));
    }
    RecordBatch::try_new(
        schema,
        vec![Arc::new(labels.finish()), Arc::new(payloads.finish())],
    )
    .expect("build list batch")
}

async fn create_dataset(
    uri: &str,
    idx: Idx,
    stable: bool,
    n: u64,
    key_base: u64,
) -> Result<Dataset> {
    let (schema, batch) = match idx {
        Idx::LabelList => (list_schema(), build_list(n, key_base)),
        _ => (primitive_schema(), build_primitive(n, key_base)),
    };
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    // Use small max_rows_per_file so the initial seed lays down several
    // small fragments. That makes the subsequent `compact_files` actually
    // consolidate them instead of being a no-op.
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: stable,
        max_rows_per_file: 250,
        ..Default::default()
    };
    Dataset::write(reader, uri, Some(params))
        .await
        .context("create dataset")
}

async fn append_more(ds: &mut Dataset, idx: Idx, n: u64, key_base: u64) -> Result<()> {
    let (schema, batch) = match idx {
        Idx::LabelList => (list_schema(), build_list(n, key_base)),
        _ => (primitive_schema(), build_primitive(n, key_base)),
    };
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    // Same small-fragment policy on append so the dataset has multiple
    // fragments to actually consolidate during compaction.
    let params = WriteParams {
        mode: WriteMode::Append,
        max_rows_per_file: 250,
        ..Default::default()
    };
    ds.append(reader, Some(params))
        .await
        .context("append more")?;
    Ok(())
}

/// Issue a filter scan that *should* be served by the index. Returns the
/// number of rows returned AND the row IDs we got back (with stable row
/// IDs enabled, those IDs are logical and must survive compaction). We
/// don't enforce that the index was used (that would require introspection
/// of the physical plan); we only enforce that the answer is correct and
/// that the row IDs the index emits round-trip correctly.
async fn probe(ds: &Dataset, idx: Idx, key: u64) -> Result<ProbeResult> {
    let filter = match idx {
        Idx::LabelList => format!("array_has(labels, 'k{}')", key),
        _ => format!("key = {}", key),
    };
    let mut scanner = ds.scan();
    scanner.filter(&filter).context("set filter")?;
    scanner.with_row_id();
    let stream = scanner.try_into_stream().await.context("into stream")?;
    let batches: Vec<RecordBatch> = stream.try_collect().await.context("collect stream")?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    let mut row_ids: Vec<u64> = Vec::new();
    for b in &batches {
        let col = b
            .column_by_name("_rowid")
            .context("_rowid missing in probe output")?;
        let arr = col.as_primitive::<UInt64Type>();
        for i in 0..arr.len() {
            row_ids.push(arr.value(i));
        }
    }
    Ok(ProbeResult { count: total, row_ids })
}

#[derive(Debug, Clone)]
struct ProbeResult {
    count: usize,
    row_ids: Vec<u64>,
}

async fn run_case(idx: Idx, stable: bool) -> Result<CaseResult> {
    let tmp = TempDir::new().context("tempdir")?;
    let uri = tmp.path().join(format!("{}.lance", idx.name())).to_string_lossy().to_string();

    let mut ds = create_dataset(&uri, idx, stable, 1000, 0).await?;
    let v_initial = ds.manifest().version;
    let stable_after_create = ds.manifest().uses_stable_row_ids();

    // Create index
    ds.create_index(
        &[match idx {
            Idx::LabelList => "labels",
            _ => "key",
        }],
        idx.lance_type(),
        Some(format!("idx_{}", idx.name().to_lowercase())),
        &idx.params(),
        false,
    )
    .await
    .context("create_index")?;
    let v_post_index = ds.manifest().version;

    let pre_compact_existing = probe(&ds, idx, 500).await?;

    append_more(&mut ds, idx, 500, 1000).await?;
    let pre_compact_new = probe(&ds, idx, 1234).await?;

    let pre_fragments = ds.manifest().fragments.len();

    // Force consolidation: target_rows_per_fragment is normally 1M, our seed
    // is 1500 rows. Set it to something modest so compact_files will gather
    // all small fragments into one.
    let compaction_opts = CompactionOptions {
        target_rows_per_fragment: 10_000,
        ..Default::default()
    };
    let metrics = compact_files(&mut ds, compaction_opts, None)
        .await
        .context("compact_files")?;
    let post_fragments = ds.manifest().fragments.len();
    let v_post_compact = ds.manifest().version;
    let stable_after_compact = ds.manifest().uses_stable_row_ids();

    let post_compact_existing = probe(&ds, idx, 500).await?;
    let post_compact_new = probe(&ds, idx, 1234).await?;

    let stable_ids_500 = pre_compact_existing.row_ids == post_compact_existing.row_ids;
    let stable_ids_1234 = pre_compact_new.row_ids == post_compact_new.row_ids;

    Ok(CaseResult {
        idx,
        stable_requested: stable,
        stable_after_create,
        stable_after_compact,
        v_initial,
        v_post_index,
        v_post_compact,
        pre_fragments,
        post_fragments,
        fragments_removed: metrics.fragments_removed,
        fragments_added: metrics.fragments_added,
        pre_compact_existing,
        pre_compact_new,
        post_compact_existing,
        post_compact_new,
        stable_ids_500,
        stable_ids_1234,
    })
}

#[derive(Debug)]
#[allow(dead_code)] // Several fields populated for completeness, not all printed.
struct CaseResult {
    idx: Idx,
    stable_requested: bool,
    stable_after_create: bool,
    stable_after_compact: bool,
    v_initial: u64,
    v_post_index: u64,
    v_post_compact: u64,
    pre_fragments: usize,
    post_fragments: usize,
    fragments_removed: usize,
    fragments_added: usize,
    pre_compact_existing: ProbeResult,
    pre_compact_new: ProbeResult,
    post_compact_existing: ProbeResult,
    post_compact_new: ProbeResult,
    stable_ids_500: bool,
    stable_ids_1234: bool,
}

impl CaseResult {
    fn ok(&self) -> bool {
        let counts_ok = self.pre_compact_existing.count == 1
            && self.pre_compact_new.count == 1
            && self.post_compact_existing.count == 1
            && self.post_compact_new.count == 1;
        let flag_ok = self.stable_after_create == self.stable_requested;
        // When stable row IDs are enabled, the row IDs returned for the
        // same logical row must be identical pre- and post-compaction.
        // When disabled, they may differ — that's the substrate behavior
        // we are documenting, not asserting equality on.
        let row_ids_ok = if self.stable_requested {
            self.stable_ids_500 && self.stable_ids_1234
        } else {
            true // we don't assert in this branch
        };
        counts_ok && flag_ok && row_ids_ok
    }
}

/// Side experiment: does `Operation::Overwrite` (built via
/// `InsertBuilder::with_params(WriteParams { mode: Overwrite, .. })` without
/// `enable_stable_row_ids: true`) preserve the manifest's stable-row-ids flag?
/// Mirrors `table_store.rs:stage_overwrite` at line 956 in OmniGraph.
async fn run_overwrite_preservation() -> Result<OverwriteResult> {
    let tmp = TempDir::new()?;
    let uri = tmp.path().join("overwrite.lance").to_string_lossy().to_string();

    // 1. Create with stable=true
    let schema = primitive_schema();
    let reader = RecordBatchIterator::new(vec![Ok(build_primitive(100, 0))], schema.clone());
    let create_params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        ..Default::default()
    };
    let mut ds = Dataset::write(reader, &uri, Some(create_params)).await?;
    let stable_after_create = ds.manifest().uses_stable_row_ids();

    // 2. Build an uncommitted Overwrite transaction WITHOUT enable_stable_row_ids.
    let batch = build_primitive(50, 5000);
    let owb_params = WriteParams {
        mode: WriteMode::Overwrite,
        ..Default::default() // <-- enable_stable_row_ids defaults to false
    };
    let txn = InsertBuilder::new(Arc::new(ds.clone()))
        .with_params(&owb_params)
        .execute_uncommitted(vec![batch])
        .await?;

    // 3. Commit via CommitBuilder (mirrors `TableStore::commit_staged`).
    use lance::dataset::CommitBuilder;
    let new_ds = CommitBuilder::new(Arc::new(ds.clone())).execute(txn).await?;
    let stable_after_overwrite = new_ds.manifest().uses_stable_row_ids();

    // 4. Sanity: a third write via Dataset::write with mode=Overwrite (the
    //    non-staged path) for comparison.
    let reader = RecordBatchIterator::new(vec![Ok(build_primitive(50, 7000))], schema.clone());
    let direct_params = WriteParams {
        mode: WriteMode::Overwrite,
        ..Default::default()
    };
    let _ = Dataset::write(reader, &uri, Some(direct_params)).await?;
    ds = Dataset::open(&uri).await?;
    let stable_after_direct_overwrite = ds.manifest().uses_stable_row_ids();

    Ok(OverwriteResult {
        stable_after_create,
        stable_after_staged_overwrite: stable_after_overwrite,
        stable_after_direct_overwrite,
    })
}

#[derive(Debug)]
struct OverwriteResult {
    stable_after_create: bool,
    stable_after_staged_overwrite: bool,
    stable_after_direct_overwrite: bool,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let mut cases: Vec<CaseResult> = Vec::new();
    for idx in [Idx::BTree, Idx::Bitmap, Idx::LabelList] {
        for stable in [true, false] {
            print_progress(idx, stable);
            let case = run_case(idx, stable).await?;
            cases.push(case);
        }
    }

    print_matrix(&cases);

    println!();
    println!("=== On-disk index inspection (BTree, stable=true) ===");
    inspect_index_files(Idx::BTree, true).await?;

    println!();
    println!("=== Side experiment: stage_overwrite flag preservation ===");
    let r = run_overwrite_preservation().await?;
    print_overwrite(&r);

    let all_ok = cases.iter().all(|c| c.ok());
    println!();
    if all_ok {
        println!("ALL CASES OK — all post-compaction probes returned 1 row.");
    } else {
        println!("SOME CASES FAILED — see matrix above.");
    }
    Ok(())
}

fn print_progress(idx: Idx, stable: bool) {
    println!("running idx={} stable={} ...", idx.name(), stable);
}

fn print_matrix(cases: &[CaseResult]) {
    println!();
    println!("=== MR-927 Phase 1 matrix ===");
    println!(
        "{:<10} {:<8} {:<7} {:<7} {:<14} {:<14} {:<18} {:<22} {:<22} {:<6} {:<6} {}",
        "idx",
        "stable",
        "manif1",
        "manif2",
        "pre/post.cnt",
        "pre/post.cnt",
        "fragments",
        "row_id key=500",
        "row_id key=1234",
        "id500",
        "id1234",
        "ok",
    );
    for c in cases {
        let pre500 = c.pre_compact_existing.row_ids.first().copied();
        let post500 = c.post_compact_existing.row_ids.first().copied();
        let pre1234 = c.pre_compact_new.row_ids.first().copied();
        let post1234 = c.post_compact_new.row_ids.first().copied();
        let frag = format!(
            "{}->{}  (+{},-{})",
            c.pre_fragments, c.post_fragments, c.fragments_added, c.fragments_removed
        );
        let id500 = format!(
            "{}->{}",
            pre500.map_or("-".to_string(), |v| v.to_string()),
            post500.map_or("-".to_string(), |v| v.to_string()),
        );
        let id1234 = format!(
            "{}->{}",
            pre1234.map_or("-".to_string(), |v| v.to_string()),
            post1234.map_or("-".to_string(), |v| v.to_string()),
        );
        let cnt500 = format!(
            "{}->{}",
            c.pre_compact_existing.count, c.post_compact_existing.count
        );
        let cnt1234 = format!(
            "{}->{}",
            c.pre_compact_new.count, c.post_compact_new.count
        );
        println!(
            "{:<10} {:<8} {:<7} {:<7} {:<14} {:<14} {:<18} {:<22} {:<22} {:<6} {:<6} {}",
            c.idx.name(),
            c.stable_requested,
            c.stable_after_create,
            c.stable_after_compact,
            cnt500,
            cnt1234,
            frag,
            id500,
            id1234,
            c.stable_ids_500,
            c.stable_ids_1234,
            if c.ok() { "OK" } else { "FAIL" },
        );
    }
}

fn print_overwrite(r: &OverwriteResult) {
    println!("create  (enable_stable_row_ids: true)                              -> manifest.uses_stable_row_ids = {}", r.stable_after_create);
    println!("staged Overwrite (WriteParams without enable_stable_row_ids: true) -> manifest.uses_stable_row_ids = {}", r.stable_after_staged_overwrite);
    println!("direct Dataset::write Overwrite (same flag absent)                 -> manifest.uses_stable_row_ids = {}", r.stable_after_direct_overwrite);
}

/// Walk `<uri>/_indices/<uuid>/` and print the files Lance laid down for one
/// concrete case. The shapes are stable across BTree/Bitmap/LabelList
/// (each writes a small handful of index segment files); the bytes are
/// opaque, so we just enumerate names and sizes.
async fn inspect_index_files(idx: Idx, stable: bool) -> Result<()> {
    let tmp = TempDir::new()?;
    let uri = tmp
        .path()
        .join(format!("inspect_{}.lance", idx.name()))
        .to_string_lossy()
        .to_string();
    let mut ds = create_dataset(&uri, idx, stable, 1000, 0).await?;
    ds.create_index(
        &[match idx {
            Idx::LabelList => "labels",
            _ => "key",
        }],
        idx.lance_type(),
        Some(format!("idx_{}", idx.name().to_lowercase())),
        &idx.params(),
        false,
    )
    .await?;
    let indices_root = Path::new(&uri).join("_indices");
    if !indices_root.exists() {
        println!("no _indices/ on disk for {} (stable={})", idx.name(), stable);
        return Ok(());
    }
    println!("({}, stable={}) _indices/ tree:", idx.name(), stable);
    walk_dir(&indices_root, 1)?;
    Ok(())
}

fn walk_dir(dir: &Path, depth: usize) -> Result<()> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect();
    entries.sort();
    for p in entries {
        let name = p.file_name().unwrap().to_string_lossy().to_string();
        let pad = "  ".repeat(depth);
        if p.is_dir() {
            println!("{}{}/", pad, name);
            walk_dir(&p, depth + 1)?;
        } else {
            let size = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
            println!("{}{}  ({} bytes)", pad, name, size);
        }
    }
    Ok(())
}
