mod common;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use nanograph::params;
use nanograph::store::database::{Database, LoadMode};

const TRAVERSAL_QUERY_SOURCE: &str = r#"
query one_hop($slug: String) {
    match {
        $p: Person { slug: $slug }
        $p knows $friend
    }
    return { $friend.slug }
}

query two_hop($slug: String) {
    match {
        $p: Person { slug: $slug }
        $p knows{2,2} $friend
    }
    return { $friend.slug }
}

query filtered($slug: String, $team: String) {
    match {
        $p: Person { slug: $slug }
        $p knows $friend
        $friend.team = $team
    }
    return { $friend.slug }
}
"#;

fn bench_traversal(c: &mut Criterion) {
    let runtime = common::bench_runtime();
    let (label, nodes, edges) = common::traversal_scale();
    let out_degree = (edges / nodes).max(1);
    let db = runtime
        .block_on(Database::open_in_memory(common::social_graph_schema()))
        .expect("open traversal db");
    let data = common::build_social_graph_data(nodes, out_degree);
    runtime
        .block_on(db.load_with_mode(&data, LoadMode::Overwrite))
        .expect("load traversal graph");

    let one_hop = common::prepare_named_query(&db, TRAVERSAL_QUERY_SOURCE, "one_hop");
    let two_hop = common::prepare_named_query(&db, TRAVERSAL_QUERY_SOURCE, "two_hop");
    let filtered = common::prepare_named_query(&db, TRAVERSAL_QUERY_SOURCE, "filtered");
    let base_params = params! {
        "slug" => format!("user_{:06}", nodes / 3),
    }
    .expect("one hop params");
    let filtered_params = params! {
        "slug" => format!("user_{:06}", nodes / 3),
        "team" => "team_03",
    }
    .expect("filtered params");

    let mut group = c.benchmark_group("traversal");
    group.throughput(Throughput::Elements(edges as u64));
    group.bench_function(BenchmarkId::new("one_hop", label), |b| {
        b.iter(|| {
            let result = runtime
                .block_on(one_hop.execute(&base_params))
                .expect("execute one-hop");
            black_box(result.num_rows());
        });
    });
    group.bench_function(BenchmarkId::new("two_hop", label), |b| {
        b.iter(|| {
            let result = runtime
                .block_on(two_hop.execute(&base_params))
                .expect("execute two-hop");
            black_box(result.num_rows());
        });
    });
    group.bench_function(BenchmarkId::new("filtered", label), |b| {
        b.iter(|| {
            let result = runtime
                .block_on(filtered.execute(&filtered_params))
                .expect("execute filtered traversal");
            black_box(result.num_rows());
        });
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = common::criterion_config();
    targets = bench_traversal
}
criterion_main!(benches);
