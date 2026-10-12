//! Instrument: what the row set of one `__manifest` version is made of as
//! commit history grows. Every step repeats `set_age` on the same person, so
//! the `table` rows stay fixed and any growth is commit history. At each
//! checkpoint the latest `__manifest` version is scanned raw and reported per
//! `object_type` and per stored leaf column: rows, Arrow bytes, values that are
//! not the filler, distinct values and their bytes; then the size of the
//! version's data files and of `__history`.
//! `#[ignore]`d, run explicitly; it reads the stored schema as it finds it.
#![recursion_limit = "512"]

mod helpers;

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchIterator, StructArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::dataset::WriteParams;
use lance_file::version::LanceFileVersion;

use helpers::cost::local_graph;
use helpers::{MUTATION_QUERIES, mixed_params};

const CHECKPOINTS: [u64; 5] = [1, 64, 256, 512, 1024];
const SAMPLE_BYTES: usize = 400;

fn leaves(prefix: &str, column: &ArrayRef, out: &mut Vec<(String, ArrayRef)>) {
    match column.as_any().downcast_ref::<StructArray>() {
        Some(record) => {
            for (field, child) in record.fields().iter().zip(record.columns()) {
                leaves(&format!("{prefix}.{}", field.name()), child, out);
            }
        }
        None => out.push((prefix.to_string(), Arc::clone(column))),
    }
}

struct LeafShape {
    sample: Option<String>,
    arrow_bytes: usize,
    filled: usize,
    distinct: usize,
    distinct_bytes: usize,
    value_bytes: usize,
    max_value_bytes: usize,
}

fn shape_of(column: &ArrayRef) -> LeafShape {
    let arrow_bytes = column.to_data().get_slice_memory_size().unwrap();
    let texts: Vec<Option<String>> = match column.data_type() {
        DataType::Utf8 => column
            .as_string::<i32>()
            .iter()
            .map(|value| value.map(str::to_owned))
            .collect(),
        DataType::LargeUtf8 => column
            .as_string::<i64>()
            .iter()
            .map(|value| value.map(str::to_owned))
            .collect(),
        other if other.primitive_width().is_some() => arrow_cast::cast(column, &DataType::Utf8)
            .unwrap()
            .as_string::<i32>()
            .iter()
            .map(|value| value.map(str::to_owned))
            .collect(),
        other => panic!("manifest leaf column of unexpected type {other}"),
    };
    let fixed = column.data_type().primitive_width();
    let filler = if fixed.is_some() { "0" } else { "" };
    let width = |text: &str| fixed.unwrap_or(text.len());
    let filled: Vec<&str> = texts
        .iter()
        .flatten()
        .map(String::as_str)
        .filter(|text| *text != filler)
        .collect();
    let distinct: HashSet<&str> = filled.iter().copied().collect();
    LeafShape {
        sample: filled
            .last()
            .map(|text| text.chars().take(SAMPLE_BYTES).collect()),
        arrow_bytes,
        filled: filled.len(),
        distinct: distinct.len(),
        distinct_bytes: distinct.iter().map(|text| width(text)).sum(),
        value_bytes: filled.iter().map(|text| width(text)).sum(),
        max_value_bytes: filled.iter().map(|text| width(text)).max().unwrap_or(0),
    }
}

/// The data file bytes of `columns` written alone as one Lance 2.2 dataset, the
/// encoding `__manifest` publishes with.
async fn lance_bytes(fields: Vec<Field>, columns: Vec<ArrayRef>) -> u64 {
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let params = WriteParams {
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    let reader = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
    lance::Dataset::write(reader, dir.path().to_str().unwrap(), Some(params))
        .await
        .unwrap();
    dir_bytes(&dir.path().join("data"))
}

fn dir_bytes(path: &Path) -> u64 {
    if !path.exists() {
        return 0;
    }
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                dir_bytes(&entry.path())
            } else {
                entry.metadata().unwrap().len()
            }
        })
        .sum()
}

async fn report(root: &Path, commits: u64) {
    let manifest_dir = root.join("__manifest");
    let dataset = helpers::open_dataset_head(manifest_dir.to_str().unwrap(), None).await;
    let batches: Vec<RecordBatch> = dataset
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let batch = arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
    let mut by_type = BTreeMap::<String, Vec<u32>>::new();
    let types = batch.column_by_name("object_type").unwrap();
    for (row, object_type) in types.as_string::<i32>().iter().enumerate() {
        by_type
            .entry(object_type.unwrap().to_string())
            .or_default()
            .push(u32::try_from(row).unwrap());
    }
    let mut columns = Vec::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        leaves(field.name(), column, &mut columns);
    }
    for (object_type, rows) in &by_type {
        let indices = UInt32Array::from(rows.clone());
        let mut type_bytes = 0;
        let mut type_lance_bytes = 0;
        let mut leaf_fields = Vec::new();
        let mut leaf_columns = Vec::new();
        for (name, column) in &columns {
            let taken = arrow_select::take::take(column, &indices, None).unwrap();
            let shape = shape_of(&taken);
            let field = Field::new(name.replace('.', "_"), taken.data_type().clone(), true);
            let lance = lance_bytes(vec![field.clone()], vec![Arc::clone(&taken)]).await;
            type_bytes += shape.arrow_bytes;
            type_lance_bytes += lance;
            leaf_fields.push(field);
            leaf_columns.push(taken);
            eprintln!(
                "MANIFEST_ROW_BYTES {}",
                serde_json::json!({
                    "record": "column",
                    "commits": commits,
                    "object_type": object_type,
                    "column": name,
                    "rows": rows.len(),
                    "arrow_bytes": shape.arrow_bytes,
                    "lance_bytes": lance,
                    "filled": shape.filled,
                    "value_bytes": shape.value_bytes,
                    "max_value_bytes": shape.max_value_bytes,
                    "distinct": shape.distinct,
                    "distinct_bytes": shape.distinct_bytes,
                    "sample": shape.sample,
                }),
            );
        }
        let stored_fields: Vec<Field> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        let stored_columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|column| arrow_select::take::take(column, &indices, None).unwrap())
            .collect();
        eprintln!(
            "MANIFEST_ROW_BYTES {}",
            serde_json::json!({
                "record": "object_type",
                "commits": commits,
                "object_type": object_type,
                "rows": rows.len(),
                "arrow_bytes": type_bytes,
                "lance_bytes_per_leaf": type_lance_bytes,
                "lance_bytes_leaves_together": lance_bytes(leaf_fields, leaf_columns).await,
                "lance_bytes_stored_shape": lance_bytes(stored_fields, stored_columns).await,
            }),
        );
    }
    let data_file_bytes: u64 = dataset
        .get_fragments()
        .iter()
        .flat_map(|fragment| fragment.metadata().files.clone())
        .map(|file| {
            std::fs::metadata(manifest_dir.join("data").join(&file.path))
                .unwrap()
                .len()
        })
        .sum();
    eprintln!(
        "MANIFEST_ROW_BYTES {}",
        serde_json::json!({
            "record": "version",
            "commits": commits,
            "manifest_version": dataset.version().version,
            "rows": batch.num_rows(),
            "fragments": dataset.get_fragments().len(),
            "data_file_bytes": data_file_bytes,
            "manifest_retained_bytes": dir_bytes(&manifest_dir),
            "history_retained_bytes": dir_bytes(&root.join("__history")),
        }),
    );
}

#[tokio::test]
#[ignore = "instrument: the row set of one __manifest version per object_type and column"]
async fn manifest_row_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let db = local_graph(&dir).await;
    report(dir.path(), 0).await;
    let mut written = 0;
    for checkpoint in CHECKPOINTS {
        while written < checkpoint {
            written += 1;
            let result = db
                .mutate(
                    "main",
                    MUTATION_QUERIES,
                    "set_age",
                    &mixed_params(
                        &[("$name", "Alice")],
                        &[("$age", 100 + i64::try_from(written).unwrap())],
                    ),
                )
                .await
                .unwrap();
            assert_eq!(result.affected_nodes, 1);
        }
        report(dir.path(), written).await;
    }
}
