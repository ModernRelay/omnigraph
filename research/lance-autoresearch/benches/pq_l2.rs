//! Criterion benchmark — runs the same kernels the agent edits, but with
//! statistical sampling. Use this for stable speed comparisons; the
//! `run_experiment` binary is the agent's per-trial harness.
//!
//! `cargo bench --bench pq_l2`

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

use lance_autoresearch::fixture::Fixture;
use lance_autoresearch::kernels::{TopKHeap, compute_distance_table_l2, probe_pq_l2_top_k};
use lance_autoresearch::{DIM, NUM_SUB_VECTORS};

fn bench_pq_l2(c: &mut Criterion) {
    let fix = Fixture::load_or_synthesize().expect("fixture");

    let q = &fix.query_vectors[..DIM];
    let table0 = compute_distance_table_l2(q, &fix.codebook);

    c.bench_function("compute_distance_table_l2", |b| {
        b.iter(|| {
            let t = compute_distance_table_l2(black_box(q), black_box(&fix.codebook));
            black_box(t);
        });
    });

    c.bench_function("probe_pq_l2_top_k", |b| {
        b.iter(|| {
            let mut heap = TopKHeap::new();
            probe_pq_l2_top_k(
                black_box(&table0),
                black_box(&fix.codes),
                black_box(fix.num_base),
                &mut heap,
            );
            black_box(heap);
        });
    });

    c.bench_function("end_to_end_one_query", |b| {
        b.iter(|| {
            let t = compute_distance_table_l2(black_box(q), black_box(&fix.codebook));
            let mut heap = TopKHeap::new();
            probe_pq_l2_top_k(&t, black_box(&fix.codes), black_box(fix.num_base), &mut heap);
            black_box(heap);
        });
    });

    // Reference: silence unused warning for NUM_SUB_VECTORS in case the bench is
    // ever stubbed out — keeps the constant import meaningful.
    let _ = NUM_SUB_VECTORS;
}

criterion_group!(benches, bench_pq_l2);
criterion_main!(benches);
