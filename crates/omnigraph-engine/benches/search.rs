mod common;

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use nanograph::params;

fn bench_search_queries(c: &mut Criterion) {
    let runtime = common::bench_runtime();
    let _mock = common::enable_mock_embeddings();
    let (starwars_db, starwars_queries) = common::load_example_db(&runtime, "starwars");
    let (revops_db, revops_queries) = common::load_example_db(&runtime, "revops");

    let keyword = common::prepare_named_query(&starwars_db, &starwars_queries, "keyword_search");
    let fuzzy = common::prepare_named_query(&starwars_db, &starwars_queries, "fuzzy_search");
    let semantic = common::prepare_named_query(&starwars_db, &starwars_queries, "semantic_search");
    let hybrid = common::prepare_named_query(&starwars_db, &starwars_queries, "hybrid_search");
    let revops_semantic =
        common::prepare_named_query(&revops_db, &revops_queries, "semantic_signals_for_client");

    let keyword_params = params! { "q" => "jedi order" }.expect("keyword params");
    let fuzzy_params = params! { "q" => "Luk Skywlkr" }.expect("fuzzy params");
    let semantic_params = params! { "q" => "desert hero becomes jedi" }.expect("semantic params");
    let hybrid_params = params! { "q" => "mentor and rebellion" }.expect("hybrid params");
    let revops_params = params! {
        "client" => "client-stripe",
        "q" => "migration blocker and champion"
    }
    .expect("revops params");

    let mut group = c.benchmark_group("search_example");
    group.bench_function(BenchmarkId::new("keyword_search", "starwars"), |b| {
        b.iter(|| {
            let result = runtime
                .block_on(keyword.execute(&keyword_params))
                .expect("execute keyword search");
            black_box(result.num_rows());
        });
    });
    group.bench_function(BenchmarkId::new("fuzzy_search", "starwars"), |b| {
        b.iter(|| {
            let result = runtime
                .block_on(fuzzy.execute(&fuzzy_params))
                .expect("execute fuzzy search");
            black_box(result.num_rows());
        });
    });
    group.bench_function(BenchmarkId::new("semantic_search", "starwars"), |b| {
        b.iter(|| {
            let result = runtime
                .block_on(semantic.execute(&semantic_params))
                .expect("execute semantic search");
            black_box(result.num_rows());
        });
    });
    group.bench_function(BenchmarkId::new("hybrid_search", "starwars"), |b| {
        b.iter(|| {
            let result = runtime
                .block_on(hybrid.execute(&hybrid_params))
                .expect("execute hybrid search");
            black_box(result.num_rows());
        });
    });
    group.bench_function(
        BenchmarkId::new("semantic_signals_for_client", "revops"),
        |b| {
            b.iter(|| {
                let result = runtime
                    .block_on(revops_semantic.execute(&revops_params))
                    .expect("execute revops semantic search");
                black_box(result.num_rows());
            });
        },
    );
    group.finish();
}

criterion_group! {
    name = benches;
    config = common::criterion_config();
    targets = bench_search_queries
}
criterion_main!(benches);
