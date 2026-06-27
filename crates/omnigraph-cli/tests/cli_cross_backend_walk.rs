//! Cross-backend generative walk (PR-E): the SAME seeded DST op sequence driven
//! against the embedded `Omnigraph` SDK AND the `omnigraph` CLI subprocess must
//! agree, per step, on the black-box state (Person/Doc slug sets + traversable
//! edge count).
//!
//! Honest scope: the CLI wraps the SAME engine, so this is NOT a second engine
//! implementation — it verifies the CLI transport layer (arg parsing, output
//! serialization, `--store` addressing, `--as` actor resolution) faithfully
//! reflects the embedded engine across a realistic generative sequence. The
//! white-box invariant battery stays embedded-only (it needs the real handle);
//! this is the black-box half the CLI arm can satisfy. Complements the
//! single-op `parity_matrix.rs` (CLI-local vs CLI-HTTP) and the fixed-sequence
//! `cli_dst_parity.rs` smoke with a generated, model-driven op stream.
//!
//! Lockstep determinism: both walks use the same seed and an identical reference
//! `Model`, and both wrap the same engine, so the op stream matches step-for-
//! step. Any divergence in op-kind, success, or resulting state fails the assert
//! immediately — before the two RNGs/models could desync.

use std::collections::BTreeSet;
use std::path::PathBuf;

use omnigraph_dst::op::OpKind;
use omnigraph_dst::{step, Backend, BackendError, Cli, Embedded, Model, Rng};

/// All slug values for a node type, normalized to an order-independent set. The
/// slug key is matched by suffix so the same parse works for the embedded
/// `to_rust_json` (`"x.slug"`) and the CLI `--json` shape.
async fn slugs<B: Backend>(b: &B, ty: &str) -> BTreeSet<String> {
    let q = format!("query q() {{ match {{ $x: {ty} }} return {{ $x.slug }} }}");
    let rows = b.query("main", &q).await.expect("slug query");
    rows.iter()
        .filter_map(|r| {
            r.as_object()?
                .iter()
                .find(|(k, _)| k.ends_with("slug"))
                .and_then(|(_, v)| v.as_str())
                .map(|s| s.to_string())
        })
        .collect()
}

/// The traversable `Knows` edge count (`$a knows $b`) — the black-box edge oracle.
async fn edge_count<B: Backend>(b: &B) -> usize {
    let rows = b
        .query(
            "main",
            "query q() { match { $a: Person $a knows $b } return { $a.slug, $b.slug } }",
        )
        .await
        .expect("edge query");
    rows.len()
}

#[tokio::test]
async fn embedded_and_cli_agree_on_seeded_walk() {
    let bin = PathBuf::from(env!("CARGO_BIN_EXE_omnigraph"));

    for seed in 0..2u64 {
        let emb_dir = tempfile::tempdir().unwrap();
        let cli_dir = tempfile::tempdir().unwrap();
        let emb = Embedded::open_clean(emb_dir.path().to_str().unwrap()).await;
        let cli = Cli::new(bin.clone(), cli_dir.path().to_str().unwrap().to_string());
        cli.init().await.expect("cli init");

        let mut emb_rng = Rng::new(seed);
        let mut cli_rng = Rng::new(seed);
        let mut emb_model = Model::new();
        let mut cli_model = Model::new();

        for i in 0..18 {
            let (ek, eres) = step(&emb, &mut emb_rng, &mut emb_model).await;
            let (ck, cres) = step(&cli, &mut cli_rng, &mut cli_model).await;

            assert_eq!(ek, ck, "seed={seed} step={i}: op-kind diverged");
            // Repair's success FLAG legitimately differs in ONE known case:
            // embedded `repair(force=false)` returns Ok and leaves suspicious/
            // unverifiable drift in place (e.g. the drift RC-1 strands), whereas
            // the CLI `repair --confirm` (no --force) EXITS NON-ZERO refusing to
            // publish it. Allow-list exactly that (emb Ok + CLI "refused
            // suspicious" error); a no-op on logical data either way, so the STATE
            // assertions below still match. Any OTHER divergence — including a
            // broken repair invocation, or any non-Repair op — must still fail.
            let known_repair_divergence = ek == OpKind::Repair
                && eres.is_ok()
                && cres.as_ref().err().is_some_and(|e| {
                    let m = e.message();
                    m.contains("repair refused") || m.contains("suspicious")
                });
            if !known_repair_divergence {
                assert_eq!(
                    eres.is_ok(),
                    cres.is_ok(),
                    "seed={seed} step={i} op[{ek:?}]: success diverged (emb={:?} cli={:?})",
                    eres.as_ref().err().map(BackendError::message),
                    cres.as_ref().err().map(BackendError::message),
                );
            }

            assert_eq!(
                slugs(&emb, "Person").await,
                slugs(&cli, "Person").await,
                "seed={seed} step={i} op[{ek:?}]: Person slug set diverged"
            );
            assert_eq!(
                slugs(&emb, "Doc").await,
                slugs(&cli, "Doc").await,
                "seed={seed} step={i} op[{ek:?}]: Doc slug set diverged"
            );
            assert_eq!(
                edge_count(&emb).await,
                edge_count(&cli).await,
                "seed={seed} step={i} op[{ek:?}]: traversable edge count diverged"
            );
        }
    }
}
