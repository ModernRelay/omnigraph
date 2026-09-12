//! RFC 0040 Historical reads: the system roles of a pinned image are read
//! from the image itself, never from the current catalog's spelling.

use std::sync::Arc;

use crate::changes::row_compare::{RawRow, rows_equal_across_vintages, user_schema_fingerprint};
use arrow_array::{RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::{ColumnAlteration, WriteMode, WriteParams};
use lance::datatypes::LANCE_UNENFORCED_PRIMARY_KEY;
use omnigraph_compiler::{SYSTEM_COLUMNS_LEGACY, SYSTEM_COLUMNS_V3};

use super::system_columns_at_image;

const TABLE: &str = "edge:Knows";

async fn legacy_table(uri: &str) -> Dataset {
    let id_field = Field::new("id", DataType::Utf8, false).with_metadata(
        [(LANCE_UNENFORCED_PRIMARY_KEY.to_string(), "true".to_string())]
            .into_iter()
            .collect(),
    );
    let schema = Arc::new(Schema::new(vec![
        id_field,
        Field::new("src", DataType::Utf8, false),
        Field::new("dst", DataType::Utf8, false),
        Field::new("weight", DataType::UInt64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["e1"])),
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(StringArray::from(vec!["b"])),
            Arc::new(UInt64Array::from(vec![1_u64])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    // forbidden-api-allow: raw Lance fixture standing in for a graph table; no graph write.
    Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            mode: WriteMode::Create,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
}

fn rename(from: &str, to: &str) -> ColumnAlteration {
    ColumnAlteration::new(from.to_string()).rename(to.to_string())
}

/// The primary-key role marker survives the upgrade's rename, so each version
/// of one incarnation decodes as the vintage it spells: the pre-rename image
/// as legacy, the post-rename image as current.
#[tokio::test]
async fn system_roles_resolve_from_each_image_across_a_rename() {
    for table_key in ["edge:Knows", "node:Person"] {
        let is_edge = table_key.starts_with("edge:");
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("table");
        let mut dataset = legacy_table(uri.to_str().unwrap()).await;
        let mut renames = vec![rename("id", "__id")];
        if is_edge {
            renames.extend([rename("src", "__src"), rename("dst", "__dst")]);
        }
        dataset.alter_columns(&renames).await.unwrap();
        let pinned = dataset.checkout_version(1).await.unwrap();
        let from_columns = system_columns_at_image(pinned.schema(), table_key).unwrap();
        let to_columns = system_columns_at_image(dataset.schema(), table_key).unwrap();
        assert_eq!(from_columns, SYSTEM_COLUMNS_LEGACY);
        assert_eq!(to_columns, SYSTEM_COLUMNS_V3);
        assert_eq!(
            user_schema_fingerprint(&pinned, from_columns, is_edge),
            user_schema_fingerprint(&dataset, to_columns, is_edge),
        );
        let before = pinned
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_next()
            .await
            .unwrap()
            .unwrap();
        let after = dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_next()
            .await
            .unwrap()
            .unwrap();
        let left = RawRow::single(&pinned, &before, 0, from_columns.id).unwrap();
        let right = RawRow::single(&dataset, &after, 0, to_columns.id).unwrap();
        assert!(
            rows_equal_across_vintages(
                &pinned,
                &left,
                &dataset,
                &right,
                from_columns,
                to_columns,
                is_edge
            )
            .await
            .unwrap()
        );
        assert!(
            rows_equal_across_vintages(
                &dataset,
                &right,
                &pinned,
                &left,
                to_columns,
                from_columns,
                is_edge
            )
            .await
            .unwrap()
        );
        let mut changed_columns = after.columns().to_vec();
        changed_columns[3] = Arc::new(UInt64Array::from(vec![2_u64]));
        let changed = RecordBatch::try_new(after.schema(), changed_columns).unwrap();
        let changed = RawRow::single(&dataset, &changed, 0, to_columns.id).unwrap();
        assert!(
            !rows_equal_across_vintages(
                &pinned,
                &left,
                &dataset,
                &changed,
                from_columns,
                to_columns,
                is_edge
            )
            .await
            .unwrap()
        );
        assert!(
            !rows_equal_across_vintages(
                &dataset,
                &changed,
                &pinned,
                &left,
                to_columns,
                from_columns,
                is_edge
            )
            .await
            .unwrap()
        );
    }
}

/// A spelling outside the two vintages, or endpoints of the other vintage,
/// is refused rather than decoded under any catalog's spelling.
#[tokio::test]
async fn system_roles_refuse_a_spelling_outside_the_two_vintages() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("edge_knows");
    let uri = uri.to_str().unwrap();
    let mut dataset = legacy_table(uri).await;
    let mut nullable = dataset.schema().clone();
    nullable.fields[0].nullable = true;
    assert!(
        system_columns_at_image(&nullable, TABLE)
            .unwrap_err()
            .to_string()
            .contains("non-null Utf8")
    );
    let mut integer = dataset.schema().clone();
    integer.fields[0] = lance::datatypes::Field::try_from(
        &Field::new("id", DataType::Int64, false).with_metadata(
            [(LANCE_UNENFORCED_PRIMARY_KEY.to_string(), "true".to_string())]
                .into_iter()
                .collect(),
        ),
    )
    .unwrap();
    assert!(
        system_columns_at_image(&integer, TABLE)
            .unwrap_err()
            .to_string()
            .contains("non-null Utf8")
    );

    dataset
        .alter_columns(&[rename("src", "__src")])
        .await
        .unwrap();
    let err = system_columns_at_image(dataset.schema(), TABLE)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("lacks the 'src'/'dst' endpoints"),
        "got: {err}"
    );

    dataset
        .alter_columns(&[rename("id", "ident")])
        .await
        .unwrap();
    let err = system_columns_at_image(dataset.schema(), TABLE)
        .unwrap_err()
        .to_string();
    assert!(err.contains("spelled 'ident'"), "got: {err}");
}
