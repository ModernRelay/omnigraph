use arrow_array::cast::AsArray;
use arrow_array::temporal_conversions::{date32_to_datetime, date64_to_datetime};
use arrow_array::types::{Date32Type, Date64Type};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_json::writer::{JsonArray, JsonFormat, LineDelimited, WriterBuilder};
use arrow_schema::DataType;

use crate::error::{CompilerError, Result};

/// The rows as one JSON array: `[{…},{…}]`.
pub(crate) fn record_batches_to_json_bytes(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    render::<JsonArray>(batches)
}

/// The rows as JSON objects, one per line, each terminated by `\n`.
pub(crate) fn record_batches_to_json_lines(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    render::<LineDelimited>(batches)
}

fn render<F: JsonFormat>(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    if let Some(cell) = unformattable_date(batches) {
        return Err(CompilerError::Execution(cell.to_string()));
    }
    let mut writer = WriterBuilder::new().build::<_, F>(Vec::new());
    writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
    writer.finish()?;
    Ok(writer.into_inner())
}

/// Whether the JSON writer can format this `Date` day count (chrono's calendar range).
pub fn date32_renderable(days: i32) -> bool {
    date32_to_datetime(days).is_some()
}

/// Whether the JSON writer can format this `DateTime` millisecond count.
pub fn date64_renderable(millis: i64) -> bool {
    date64_to_datetime(millis).is_some()
}

/// The first `Date` or `DateTime` cell in `batches` the JSON writer cannot
/// format: its column, its row across all batches, and the stored count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnformattableDate {
    pub column: String,
    pub row: usize,
    pub kind: &'static str,
    pub value: i64,
}

impl std::fmt::Display for UnformattableDate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "column `{}` at rows[{}] holds a {} value the JSON writer cannot format: {}",
            self.column, self.row, self.kind, self.value
        )
    }
}

pub fn unformattable_date(batches: &[RecordBatch]) -> Option<UnformattableDate> {
    let mut first_row = 0;
    for batch in batches {
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            if !holds_date(field.data_type()) {
                continue;
            }
            if let Some(cell) = first_unformattable_cell(column, &|_| true) {
                return Some(UnformattableDate {
                    column: field.name().clone(),
                    row: first_row + cell.row,
                    kind: cell.kind,
                    value: cell.value,
                });
            }
        }
        first_row += batch.num_rows();
    }
    None
}

fn holds_date(data_type: &DataType) -> bool {
    match data_type {
        DataType::Date32 | DataType::Date64 => true,
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            holds_date(field.data_type())
        }
        DataType::Struct(fields) => fields.iter().any(|field| holds_date(field.data_type())),
        _ => false,
    }
}

struct UnformattableCell {
    row: usize,
    kind: &'static str,
    value: i64,
}

fn first_unformattable_cell(
    array: &ArrayRef,
    rendered: &dyn Fn(usize) -> bool,
) -> Option<UnformattableCell> {
    match array.data_type() {
        DataType::Date32 => array
            .as_primitive::<Date32Type>()
            .iter()
            .enumerate()
            .find_map(|(row, days)| {
                let days = days.filter(|days| rendered(row) && !date32_renderable(*days))?;
                Some(UnformattableCell {
                    row,
                    kind: "Date",
                    value: i64::from(days),
                })
            }),
        DataType::Date64 => array
            .as_primitive::<Date64Type>()
            .iter()
            .enumerate()
            .find_map(|(row, millis)| {
                let millis =
                    millis.filter(|millis| rendered(row) && !date64_renderable(*millis))?;
                Some(UnformattableCell {
                    row,
                    kind: "DateTime",
                    value: millis,
                })
            }),
        DataType::List(_) => first_unformattable_in_slots(array.as_list::<i32>().iter(), rendered),
        DataType::LargeList(_) => {
            first_unformattable_in_slots(array.as_list::<i64>().iter(), rendered)
        }
        DataType::FixedSizeList(_, _) => {
            first_unformattable_in_slots(array.as_fixed_size_list().iter(), rendered)
        }
        DataType::Struct(_) => {
            let parent = array.as_struct();
            parent.columns().iter().find_map(|child| {
                first_unformattable_cell(child, &|row| rendered(row) && parent.is_valid(row))
            })
        }
        _ => None,
    }
}

fn first_unformattable_in_slots(
    slots: impl Iterator<Item = Option<ArrayRef>>,
    rendered: &dyn Fn(usize) -> bool,
) -> Option<UnformattableCell> {
    slots.enumerate().find_map(|(row, slot)| {
        let slot = slot.filter(|_| rendered(row))?;
        let cell = first_unformattable_cell(&slot, &|_| true)?;
        Some(UnformattableCell { row, ..cell })
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{
        FixedSizeListBuilder, Float32Builder, Int64Builder, ListBuilder, StringBuilder,
    };
    use arrow_array::{
        ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int32Array,
        Int64Array, LargeStringArray, RecordBatch, StringArray, StructArray, UInt32Array,
        UInt64Array,
    };
    use arrow_schema::{DataType, Field, Fields, Schema};

    use super::record_batches_to_json_bytes;

    fn render(batches: &[RecordBatch]) -> String {
        String::from_utf8(record_batches_to_json_bytes(batches).expect("render")).expect("utf8")
    }

    fn single(name: &str, array: ArrayRef) -> RecordBatch {
        let field = Field::new(name, array.data_type().clone(), true);
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).expect("batch")
    }

    #[test]
    fn every_catalog_type_renders_to_the_documented_spelling() {
        let mut vectors = FixedSizeListBuilder::new(Float32Builder::new(), 3);
        for value in [0.25_f32, 0.5, 0.75] {
            vectors.values().append_value(value);
        }
        vectors.append(true);
        for _ in 0..3 {
            vectors.values().append_null();
        }
        vectors.append(false);
        for value in [1.0_f32, 2.0, 3.0] {
            vectors.values().append_value(value);
        }
        vectors.append(true);

        let mut lists = ListBuilder::new(Int64Builder::new());
        lists.values().append_value(1);
        lists.values().append_null();
        lists.values().append_value(3);
        lists.append(true);
        lists.append(false);
        lists.append(true);

        let mut names = ListBuilder::new(StringBuilder::new());
        names.values().append_value("x");
        names.append(true);
        names.append(false);
        names.append(true);

        let struct_fields = Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let structs = StructArray::new(
            struct_fields,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringArray::from(vec![None, None, Some("b")])),
            ],
            None,
        );

        let columns: Vec<(&str, ArrayRef)> = vec![
            (
                "s",
                Arc::new(StringArray::from(vec![Some("a\"b"), None, Some("only")])),
            ),
            (
                "b",
                Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            ),
            (
                "i32",
                Arc::new(Int32Array::from(vec![Some(-1), None, Some(i32::MAX)])),
            ),
            (
                "i64",
                Arc::new(Int64Array::from(vec![
                    Some(9_007_199_254_740_993),
                    None,
                    Some(i64::MIN),
                ])),
            ),
            (
                "u32",
                Arc::new(UInt32Array::from(vec![Some(u32::MAX), None, Some(0)])),
            ),
            (
                "u64",
                Arc::new(UInt64Array::from(vec![Some(u64::MAX), None, Some(7)])),
            ),
            (
                "f32",
                Arc::new(Float32Array::from(vec![
                    Some(0.99),
                    Some(f32::INFINITY),
                    Some(f32::NAN),
                ])),
            ),
            (
                "f32_shape",
                Arc::new(Float32Array::from(vec![
                    Some(1.0),
                    Some(-0.0),
                    Some(1.0e10),
                ])),
            ),
            (
                "f64",
                Arc::new(Float64Array::from(vec![
                    Some(2.5),
                    Some(f64::NEG_INFINITY),
                    Some(f64::INFINITY),
                ])),
            ),
            (
                "f64_shape",
                Arc::new(Float64Array::from(vec![
                    Some(1.0e10),
                    Some(1.0e-7),
                    Some(1.0e20),
                ])),
            ),
            (
                "f32_edge",
                Arc::new(Float32Array::from(vec![
                    Some(1.0e9),
                    Some(1.0e-5),
                    Some(1.0e-6),
                ])),
            ),
            (
                "f64_edge",
                Arc::new(Float64Array::from(vec![
                    Some(1.0e9),
                    Some(1.0e-5),
                    Some(1.0e-6),
                ])),
            ),
            (
                "date",
                Arc::new(Date32Array::from(vec![Some(19_723), None, Some(0)])),
            ),
            (
                "datetime",
                Arc::new(Date64Array::from(vec![
                    Some(1_704_110_096_789),
                    None,
                    Some(1_704_067_200_000),
                ])),
            ),
            ("vec", Arc::new(vectors.finish())),
            ("list", Arc::new(lists.finish())),
            ("names", Arc::new(names.finish())),
            (
                "large",
                Arc::new(LargeStringArray::from(vec![Some("L"), None, Some("")])),
            ),
            ("struct", Arc::new(structs)),
        ];
        let fields = columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns.into_iter().map(|(_, array)| array).collect(),
        )
        .expect("batch");

        assert_eq!(
            render(&[batch]),
            concat!(
                "[",
                r#"{"s":"a\"b","b":true,"i32":-1,"i64":9007199254740993,"u32":4294967295,"u64":18446744073709551615,"f32":0.99,"f32_shape":1.0,"f64":2.5,"f64_shape":1.0e10,"f32_edge":1000000000.0,"f64_edge":1000000000.0,"date":"2024-01-01","datetime":"2024-01-01T11:54:56.789","vec":[0.25,0.5,0.75],"list":[1,null,3],"names":["x"],"large":"L","struct":{"a":1}}"#,
                ",",
                r#"{"f32":null,"f32_shape":-0.0,"f64":null,"f64_shape":1.0e-7,"f32_edge":0.00001,"f64_edge":0.00001,"struct":{}}"#,
                ",",
                r#"{"s":"only","b":false,"i32":2147483647,"i64":-9223372036854775808,"u32":0,"u64":7,"f32":null,"f32_shape":1.0e10,"f64":null,"f64_shape":1.0e20,"f32_edge":1.0e-6,"f64_edge":1.0e-6,"date":"1970-01-01","datetime":"2024-01-01T00:00:00","vec":[1.0,2.0,3.0],"list":[],"names":[],"large":"","struct":{"a":3,"b":"b"}}"#,
                "]",
            )
        );
    }

    #[test]
    fn empty_result_renders_as_an_empty_array() {
        assert_eq!(render(&[]), "[]");
        let empty = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::UInt64,
            false,
        )])));
        assert_eq!(render(&[empty]), "[]");
    }

    #[test]
    fn batches_render_into_one_array() {
        let first = single("id", Arc::new(UInt64Array::from(vec![1_u64, 2])));
        let second = single("id", Arc::new(UInt64Array::from(vec![3_u64])));
        assert_eq!(render(&[first, second]), r#"[{"id":1},{"id":2},{"id":3}]"#);
    }

    fn render_err(batches: &[RecordBatch]) -> String {
        record_batches_to_json_bytes(batches)
            .expect_err("unformattable date")
            .to_string()
    }

    #[test]
    fn date_counts_outside_the_render_range_are_errors() {
        let batch = single(
            "d",
            Arc::new(Date32Array::from(vec![Some(19_723), Some(i32::MAX)])),
        );
        assert_eq!(
            render_err(&[batch]),
            "execution error: column `d` at rows[1] holds a Date value the JSON writer cannot format: 2147483647"
        );

        for millis in [i64::MAX, i64::MIN] {
            let batch = single("t", Arc::new(Date64Array::from(vec![Some(millis)])));
            assert_eq!(
                render_err(&[batch]),
                format!(
                    "execution error: column `t` at rows[0] holds a DateTime value the JSON writer cannot format: {millis}"
                )
            );
        }

        let mut days = ListBuilder::new(arrow_array::builder::Date32Builder::new());
        days.values().append_value(i32::MIN);
        days.append(true);
        let batch = single("days", Arc::new(days.finish()));
        assert!(
            render_err(&[batch]).contains("column `days` at rows[0] holds a Date value"),
            "list element"
        );
    }

    #[test]
    fn row_index_counts_across_batches_and_only_rendered_slots_are_checked() {
        let good = single("d", Arc::new(Date32Array::from(vec![Some(0), None])));
        let bad = single("d", Arc::new(Date32Array::from(vec![None, Some(i32::MIN)])));
        assert!(
            render_err(&[good, bad]).contains("at rows[3] holds a Date value"),
            "row index is the position in the whole result"
        );

        let mut days = ListBuilder::new(arrow_array::builder::Date32Builder::new());
        days.values().append_value(19_723);
        days.append(true);
        days.values().append_value(i32::MAX);
        days.append(true);
        let lists: ArrayRef = Arc::new(days.finish());
        let sliced = single("days", lists.slice(0, 1));
        assert_eq!(render(&[sliced]), r#"[{"days":["2024-01-01"]}]"#);
        let sliced = single("days", lists.slice(1, 1));
        assert!(render_err(&[sliced]).contains("at rows[0]"), "slice offset");

        let field = Field::new("d", DataType::Date32, true);
        let child: ArrayRef = Arc::new(Date32Array::from(vec![Some(i32::MAX), Some(19_723)]));
        let structs = StructArray::new(
            Fields::from(vec![field]),
            vec![child],
            Some(vec![false, true].into()),
        );
        let batch = single("s", Arc::new(structs));
        assert_eq!(render(&[batch]), r#"[{},{"s":{"d":"2024-01-01"}}]"#);
    }
}
