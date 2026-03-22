mod common;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use nanograph::params;
use nanograph::store::database::{Database, LoadMode};

const LOOKUP_QUERY_SOURCE: &str = r#"
query key_lookup($slug: String) {
    match { $p: Person { slug: $slug } }
    return { $p.name }
    limit 1
}

query email_lookup($email: String) {
    match {
        $p: Person
        $p.email = $email
    }
    return { $p.name }
    limit 1
}
"#;

fn bench_key_lookup(c: &mut Criterion) {
    let runtime = common::bench_runtime();
    let mut group = c.benchmark_group("key_lookup");

    for (label, rows) in common::lookup_scales() {
        let db = runtime
            .block_on(Database::open_in_memory(common::keyed_people_schema(false)))
            .expect("open in memory db");
        let data = common::build_people_rows(0, *rows);
        runtime
            .block_on(db.load_with_mode(&data, LoadMode::Overwrite))
            .expect("load lookup data");
        let prepared = common::prepare_named_query(&db, LOOKUP_QUERY_SOURCE, "key_lookup");
        let params = params! {
            "slug" => format!("user_{:06}", rows / 2),
        }
        .expect("lookup params");

        group.throughput(Throughput::Elements(*rows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(label), rows, |b, _| {
            b.iter(|| {
                let result = runtime
                    .block_on(prepared.execute(&params))
                    .expect("execute key lookup");
                black_box(result.num_rows());
            });
        });
    }

    group.finish();
}

fn bench_indexed_property_lookup(c: &mut Criterion) {
    let runtime = common::bench_runtime();
    let mut group = c.benchmark_group("indexed_property_lookup");

    for (label, rows) in common::lookup_scales() {
        let indexed_db = runtime
            .block_on(Database::open_in_memory(common::keyed_people_schema(true)))
            .expect("open indexed db");
        let unindexed_db = runtime
            .block_on(Database::open_in_memory(common::keyed_people_schema(false)))
            .expect("open unindexed db");
        let data = common::build_people_rows(0, *rows);
        runtime
            .block_on(indexed_db.load_with_mode(&data, LoadMode::Overwrite))
            .expect("load indexed data");
        runtime
            .block_on(unindexed_db.load_with_mode(&data, LoadMode::Overwrite))
            .expect("load unindexed data");

        let indexed = common::prepare_named_query(&indexed_db, LOOKUP_QUERY_SOURCE, "email_lookup");
        let unindexed =
            common::prepare_named_query(&unindexed_db, LOOKUP_QUERY_SOURCE, "email_lookup");
        let params = params! {
            "email" => format!("user_{:06}@example.com", rows / 2),
        }
        .expect("email params");

        group.throughput(Throughput::Elements(*rows as u64));
        group.bench_function(BenchmarkId::new("indexed", label), |b| {
            b.iter(|| {
                let result = runtime
                    .block_on(indexed.execute(&params))
                    .expect("execute indexed lookup");
                black_box(result.num_rows());
            });
        });
        group.bench_function(BenchmarkId::new("full_scan", label), |b| {
            b.iter(|| {
                let result = runtime
                    .block_on(unindexed.execute(&params))
                    .expect("execute full scan lookup");
                black_box(result.num_rows());
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = common::criterion_config();
    targets = bench_key_lookup, bench_indexed_property_lookup
}
criterion_main!(benches);
