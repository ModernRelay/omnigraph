// Legacy manual performance harness.
// Prefer the Criterion benchmark in `crates/nanograph/benches/result_transport.rs`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
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
    .expect("batch");
    QueryResult::new(schema, vec![batch])
}

fn average_duration(mut f: impl FnMut()) -> Duration {
    let iterations = std::env::var("NANOGRAPH_JSON_PERF_ITERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10);
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let total = start.elapsed();
    Duration::from_secs_f64(total.as_secs_f64() / iterations as f64)
}

#[test]
#[ignore = "performance harness; run manually with --ignored --nocapture"]
fn benchmark_vector_json_vs_arrow_transport() {
    let rows = std::env::var("NANOGRAPH_JSON_PERF_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(256);
    let dim = std::env::var("NANOGRAPH_JSON_PERF_DIM")
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(768);
    let result = build_vector_query_result(rows, dim);

    // Warm up allocator and serializer code paths.
    let _ = result.to_sdk_json();
    let _ = result.to_arrow_ipc().expect("arrow ipc warmup");

    let json_avg = average_duration(|| {
        let json = result.to_sdk_json();
        assert_eq!(json.as_array().map(|rows| rows.len()), Some(rows));
    });
    let arrow_avg = average_duration(|| {
        let buffer = result.to_arrow_ipc().expect("arrow ipc");
        assert!(!buffer.is_empty());
    });

    let json_ms = json_avg.as_secs_f64() * 1000.0;
    let arrow_ms = arrow_avg.as_secs_f64() * 1000.0;
    let speedup = if arrow_avg.is_zero() {
        f64::INFINITY
    } else {
        json_avg.as_secs_f64() / arrow_avg.as_secs_f64()
    };

    println!(
        "json_output_perf rows={} dim={} json_avg_ms={:.3} arrow_avg_ms={:.3} arrow_speedup={:.2}x",
        rows, dim, json_ms, arrow_ms, speedup
    );

    if std::env::var("NANOGRAPH_JSON_PERF_ENFORCE").as_deref() == Ok("1") {
        let max_json_ms = std::env::var("NANOGRAPH_JSON_PERF_MAX_JSON_MS")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(40.0);
        let min_arrow_speedup = std::env::var("NANOGRAPH_JSON_PERF_MIN_ARROW_SPEEDUP")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(1.5);
        assert!(
            json_ms <= max_json_ms,
            "json serialization latency {:.3}ms exceeds max {:.3}ms",
            json_ms,
            max_json_ms
        );
        assert!(
            speedup >= min_arrow_speedup,
            "Arrow speedup {:.2}x below minimum {:.2}x",
            speedup,
            min_arrow_speedup
        );
    }
}
