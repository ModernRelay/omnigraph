//! Criterion benchmark — same kernels the agent edits, with statistical sampling.
//! Use this for stable speed comparisons; `run_experiment` is the per-trial harness.
//!
//! `cargo bench --bench pq_l2`

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

use lance_autoresearch::inputs::{SHAPES, SPEED_TOP_K, speed_workloads};
use lance_autoresearch::kernels::PqKernel;

fn bench_pq_l2(c: &mut Criterion) {
    let workloads = speed_workloads(0xBE3C_C0DE_F1AC_BABE);

    for wl in &workloads {
        let kernel = PqKernel::new(wl.shape, &wl.codebook);
        let q = &wl.queries[..wl.shape.dim];
        let table0 = kernel.distance_table(q);

        let label_shape = format!(
            "{}x{}x{}",
            wl.shape.dim, wl.shape.num_sub_vectors, wl.shape.num_centroids
        );
        let label_dist = format!("{:?}", wl.distribution).to_lowercase();
        let id = format!("{label_shape}/{label_dist}");

        c.bench_function(&format!("distance_table/{id}"), |b| {
            b.iter(|| {
                let t = kernel.distance_table(black_box(q));
                black_box(t);
            });
        });
        c.bench_function(&format!("probe_top_k/{id}"), |b| {
            b.iter(|| {
                let r = kernel.probe_top_k(
                    black_box(&table0),
                    black_box(&wl.codes),
                    black_box(wl.num_vectors),
                    black_box(SPEED_TOP_K),
                );
                black_box(r);
            });
        });
    }

    let _ = SHAPES;
}

criterion_group!(benches, bench_pq_l2);
criterion_main!(benches);
