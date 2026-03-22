mod common;

use std::sync::Arc;

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use nanograph::QueryResult;

fn build_vector_query_result(rows: usize, dim: i32) -> QueryResult {
    let mut ids = Vec::with_capacity(rows);
    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dim);
    for row in 0..rows {
        ids.push(format!("doc_{:06}", row));
        for col in 0..dim {
            let value = ((row as f32) * 0.001) + ((col as f32) * 0.0001);
            builder.values().append_value(value);
        }
        builder.append(true);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("slug", DataType::Utf8, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(ids)), Arc::new(builder.finish())],
    )
    .expect("vector batch");
    QueryResult::new(schema, vec![batch])
}

fn bench_result_transport(c: &mut Criterion) {
    let rows = 256usize;
    let dim = 768i32;
    let result = build_vector_query_result(rows, dim);

    let mut group = c.benchmark_group("result_json_vs_arrow");
    group.bench_function(BenchmarkId::new("json", format!("{rows}x{dim}")), |b| {
        b.iter(|| {
            let payload = result.to_sdk_json();
            black_box(payload);
        });
    });
    group.bench_function(
        BenchmarkId::new("arrow_ipc", format!("{rows}x{dim}")),
        |b| {
            b.iter(|| {
                let payload = result.to_arrow_ipc().expect("arrow ipc");
                black_box(payload);
            });
        },
    );
    group.finish();
}

criterion_group! {
    name = benches;
    config = common::criterion_config();
    targets = bench_result_transport
}
criterion_main!(benches);
