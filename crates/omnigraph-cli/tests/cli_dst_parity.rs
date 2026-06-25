//! CLI cross-backend smoke (DST PR-D follow-up).
//!
//! A DST-flavored op SEQUENCE — init → two `load --merge` (multi-fragment) →
//! insert → edge-free delete → query — run against the EMBEDDED `Omnigraph` SDK
//! and the `omnigraph` CLI SUBPROCESS on twin local graphs must agree on the
//! final person-slug set.
//!
//! `parity_matrix.rs` already covers SINGLE-verb CLI-local-vs-HTTP-server
//! parity; this adds the missing pieces: the **embedded-SDK arm** and a
//! **multi-op sequence**. The CLI wraps the same engine, so this verifies the
//! CLI transport layer (arg parsing, `--json` serialization, `--store`
//! addressing, `--as` actor resolution) faithfully reflects the embedded engine
//! across a realistic sequence — it is NOT a second engine implementation.
//!
//! Oracle is format-tolerant (embedded `to_rust_json` and CLI `--json` differ in
//! wrapping): compare the SET of `smoke-*` slug strings recursively extracted
//! from each arm's output.

mod support;

use std::collections::BTreeSet;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;

const SCHEMA: &str = "node Person { slug: String @key  name: String }\nedge Knows: Person -> Person\n";
const ACTOR: &str = "smoke-actor";
const FINAL_Q: &str = "query q() { match { $p: Person } return { $p.slug } }";
const INSERT: &str = "query i() { insert Person { slug: \"smoke-new\", name: \"n\" } }";
// smoke-8 has NO incident edges → a clean single-statement delete (no cascade,
// so it can't trip the RC-1 multi-statement-delete drift) — both arms succeed.
const DELETE: &str = "query d() { delete Person where slug = \"smoke-8\" }";

fn person(slug: &str) -> String {
    format!("{{\"type\":\"Person\",\"data\":{{\"slug\":\"{slug}\",\"name\":\"n\"}}}}\n")
}
fn knows(a: &str, b: &str) -> String {
    format!("{{\"edge\":\"Knows\",\"from\":\"{a}\",\"to\":\"{b}\",\"data\":{{}}}}\n")
}
fn batch1() -> String {
    (0..5).map(|i| person(&format!("smoke-{i}"))).collect()
}
fn batch2() -> String {
    let mut s: String = (5..10).map(|i| person(&format!("smoke-{i}"))).collect();
    s.push_str(&knows("smoke-0", "smoke-1"));
    s.push_str(&knows("smoke-1", "smoke-2"));
    s
}

/// Recursively collect every string value beginning with `smoke-`.
fn smoke_slugs(v: &serde_json::Value) -> BTreeSet<String> {
    fn walk(v: &serde_json::Value, out: &mut BTreeSet<String>) {
        match v {
            serde_json::Value::String(s) if s.starts_with("smoke-") => {
                out.insert(s.clone());
            }
            serde_json::Value::Array(a) => a.iter().for_each(|x| walk(x, out)),
            serde_json::Value::Object(m) => m.values().for_each(|x| walk(x, out)),
            _ => {}
        }
    }
    let mut out = BTreeSet::new();
    walk(v, &mut out);
    out
}

/// Drive the sequence via the embedded engine; return the final slug set.
async fn embedded_arm(uri: &str) -> BTreeSet<String> {
    let db = Omnigraph::init(uri, SCHEMA).await.unwrap();
    load_jsonl(&db, &batch1(), LoadMode::Merge).await.unwrap();
    load_jsonl(&db, &batch2(), LoadMode::Merge).await.unwrap();
    db.mutate("main", INSERT, "i", &ParamMap::new()).await.unwrap();
    db.mutate("main", DELETE, "d", &ParamMap::new()).await.unwrap();
    let res = db
        .query(ReadTarget::branch("main"), FINAL_Q, "q", &ParamMap::new())
        .await
        .unwrap();
    smoke_slugs(&res.to_rust_json())
}

/// Drive the SAME sequence via the `omnigraph` CLI subprocess (sync — assert_cmd).
fn cli_arm(uri: &str, schema_path: &str, b1: &str, b2: &str) -> BTreeSet<String> {
    support::cli()
        .args(["init", "--schema", schema_path, uri])
        .assert()
        .success();
    support::cli()
        .args(["load", "--mode", "merge", "--data", b1, uri])
        .assert()
        .success();
    support::cli()
        .args(["load", "--mode", "merge", "--data", b2, uri])
        .assert()
        .success();
    support::cli()
        .args(["mutate", "-e", INSERT, "--store", uri, "--as", ACTOR])
        .assert()
        .success();
    support::cli()
        .args(["mutate", "-e", DELETE, "--store", uri, "--as", ACTOR])
        .assert()
        .success();
    let out = support::cli()
        .args(["query", "-e", FINAL_Q, "--json", "--store", uri, "--as", ACTOR])
        .output()
        .expect("cli query spawn");
    assert!(out.status.success(), "cli query failed: {out:?}");
    let v: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("cli --json query output");
    smoke_slugs(&v)
}

#[tokio::test(flavor = "multi_thread")]
async fn embedded_and_cli_agree_on_dst_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let schema_path = dir.path().join("smoke.pg");
    std::fs::write(&schema_path, SCHEMA).unwrap();
    let b1 = dir.path().join("b1.jsonl");
    std::fs::write(&b1, batch1()).unwrap();
    let b2 = dir.path().join("b2.jsonl");
    std::fs::write(&b2, batch2()).unwrap();
    let emb_uri = dir.path().join("emb.omni");
    let cli_uri = dir.path().join("cli.omni");

    // Embedded arm (in-process async).
    let emb = embedded_arm(emb_uri.to_str().unwrap()).await;

    // CLI arm (subprocess; assert_cmd is sync → run on a blocking thread).
    let (cli_uri_s, schema_s, b1_s, b2_s) = (
        cli_uri.to_str().unwrap().to_string(),
        schema_path.to_str().unwrap().to_string(),
        b1.to_str().unwrap().to_string(),
        b2.to_str().unwrap().to_string(),
    );
    let cli = tokio::task::spawn_blocking(move || cli_arm(&cli_uri_s, &schema_s, &b1_s, &b2_s))
        .await
        .unwrap();

    assert_eq!(
        emb, cli,
        "embedded and CLI diverged on the final person-slug set"
    );

    // Sanity: the sequence produced the expected set (0..9 + new − 8).
    let expected: BTreeSet<String> = (0..10)
        .filter(|i| *i != 8)
        .map(|i| format!("smoke-{i}"))
        .chain(["smoke-new".to_string()])
        .collect();
    assert_eq!(emb, expected, "unexpected final state: {emb:?}");
}
