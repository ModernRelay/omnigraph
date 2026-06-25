//! D3 × D2: the read-shape battery run against deliberately-built table
//! morphologies. The oracle is primarily NO-CRASH — every query shape must
//! execute without an engine error or panic across each latent layout (single
//! fragment, ≥2 fragments, deletion vectors, compacted, on-branch) — plus exact
//! count checks where the answer is morphology-invariant. This is the D2×D3
//! cell sweep: a planner/execution regression that only bites a specific
//! fragment layout shows up here, where the generic walk's main-branch oracles
//! would miss it.
//!
//! Scope: the non-vector/non-FTS shapes (scan · @key · indexed · non-indexed ·
//! range · order+limit · count · numeric aggregates · 1-hop · var-hop ·
//! negation · zero-match). Vector (`nearest`) and FTS (`bm25`/`search`) shapes
//! are deferred — they need vector data and the inverted index whose builder
//! OOB-panics at depth (see `regression`-adjacent finding in dst.rs).

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;

/// Richer than the walk schema: `age: I64` unlocks range + numeric aggregates.
pub const SCHEMA: &str = r#"
node Person {
    slug: String @key
    name: String
    age: I64
}
node Doc {
    slug: String @key
    source: enum(whatsapp, email, linkedin, slack, telegram) @index
    body: String
}
edge Knows: Person -> Person @card(0..1)
"#;

fn person(slug: &str, age: i64) -> String {
    format!("{{\"type\":\"Person\",\"data\":{{\"slug\":\"{slug}\",\"name\":\"n\",\"age\":{age}}}}}\n")
}
fn doc(slug: &str, source: &str) -> String {
    format!("{{\"type\":\"Doc\",\"data\":{{\"slug\":\"{slug}\",\"source\":\"{source}\",\"body\":\"needle filler\"}}}}\n")
}
fn knows(from: &str, to: &str) -> String {
    format!("{{\"edge\":\"Knows\",\"from\":\"{from}\",\"to\":\"{to}\",\"data\":{{}}}}\n")
}

#[derive(Clone, Copy, Debug)]
pub enum Morph {
    /// One load batch → one fragment.
    Single,
    /// Each row group in its own `Merge` batch → ≥2 fragments.
    MultiFragment,
    /// Multi-fragment, then a delete → deletion vectors over live rows.
    WithDeletions,
    /// Multi-fragment, then `optimize` → compacted + reindexed.
    Optimized,
}

impl Morph {
    pub const ALL: [Morph; 4] = [
        Morph::Single,
        Morph::MultiFragment,
        Morph::WithDeletions,
        Morph::Optimized,
    ];
}

/// The base population (before morphology-specific shaping): 10 persons p0..p9
/// with ages 0..9, a `knows` chain p0→p1→…→p8, and 10 docs cycling 4 of the 5
/// enum sources (never `telegram`, so zero-match is deterministic).
fn base_rows() -> (Vec<String>, Vec<String>) {
    let sources = ["whatsapp", "email", "linkedin", "slack"];
    let mut persons = Vec::new();
    let mut docs = Vec::new();
    for i in 0..10 {
        persons.push(person(&format!("p{i}"), i as i64));
        docs.push(doc(&format!("d{i}"), sources[i % 4]));
    }
    (persons, docs)
}

/// Build the requested morphology on a fresh graph; returns the live Person
/// count after shaping (the only count the read battery asserts exactly).
pub async fn build(db: &Omnigraph, morph: Morph) -> usize {
    let (persons, docs) = base_rows();
    match morph {
        Morph::Single => {
            let mut all = persons.concat();
            all.push_str(&docs.concat());
            for i in 0..9 {
                all.push_str(&knows(&format!("p{i}"), &format!("p{}", i + 1)));
            }
            load_jsonl(db, &all, LoadMode::Merge).await.unwrap();
            10
        }
        Morph::MultiFragment | Morph::Optimized => {
            // Each person + its doc in its own batch → 10 fragments.
            for i in 0..10 {
                load_jsonl(db, &format!("{}{}", persons[i], docs[i]), LoadMode::Merge)
                    .await
                    .unwrap();
            }
            for i in 0..9 {
                load_jsonl(db, &knows(&format!("p{i}"), &format!("p{}", i + 1)), LoadMode::Merge)
                    .await
                    .unwrap();
            }
            if matches!(morph, Morph::Optimized) {
                db.optimize().await.unwrap();
            }
            10
        }
        Morph::WithDeletions => {
            for i in 0..10 {
                load_jsonl(db, &format!("{}{}", persons[i], docs[i]), LoadMode::Merge)
                    .await
                    .unwrap();
            }
            for i in 0..9 {
                load_jsonl(db, &knows(&format!("p{i}"), &format!("p{}", i + 1)), LoadMode::Merge)
                    .await
                    .unwrap();
            }
            // Delete p0..p2 → deletion vectors + cascaded edges.
            for i in 0..3 {
                db.mutate(
                    "main",
                    &format!("query d() {{ delete Person where slug = \"p{i}\" }}"),
                    "d",
                    &ParamMap::new(),
                )
                .await
                .unwrap();
            }
            7
        }
    }
}

/// Every read shape as `(name, query)`. Each must execute without an engine
/// error against every morphology.
pub fn shapes() -> Vec<(&'static str, &'static str)> {
    vec![
        ("full-scan", "query q() { match { $p: Person } return { $p.slug } }"),
        ("key-filter", "query q() { match { $p: Person { slug: \"p5\" } } return { $p.slug } }"),
        ("indexed-filter", "query q() { match { $d: Doc { source: \"whatsapp\" } } return { $d.slug } }"),
        ("nonindexed-filter", "query q() { match { $p: Person $p.age >= 5 } return { $p.slug } }"),
        ("range", "query q() { match { $p: Person $p.age >= 3 $p.age <= 6 } return { $p.slug } }"),
        ("order-limit", "query q() { match { $p: Person } return { $p.slug, $p.age } order { $p.age desc } limit 3 }"),
        ("count", "query q() { match { $p: Person } return { count($p) as n } }"),
        ("aggregate", "query q() { match { $p: Person } return { sum($p.age) as s, avg($p.age) as a, min($p.age) as mn, max($p.age) as mx } }"),
        ("traversal-1hop", "query q() { match { $a: Person $a knows $b } return { $a.slug, $b.slug } }"),
        ("var-hop", "query q() { match { $a: Person { slug: \"p3\" } $a knows{1,3} $b } return { $b.slug } }"),
        ("negation", "query q() { match { $p: Person not { $p knows $_ } } return { $p.slug } }"),
        ("zero-match", "query q() { match { $d: Doc { source: \"telegram\" } } return { $d.slug } }"),
    ]
}

/// Run the whole battery against `branch`; returns `(shape, Result<rows,error>)`.
pub async fn run(db: &Omnigraph, branch: &str) -> Vec<(&'static str, Result<usize, String>)> {
    let mut out = Vec::new();
    for (name, q) in shapes() {
        let r = db
            .query(ReadTarget::branch(branch), q, "q", &ParamMap::new())
            .await
            .map(|res| res.num_rows())
            .map_err(|e| e.to_string());
        out.push((name, r));
    }
    out
}
