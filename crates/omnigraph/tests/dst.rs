//! Deterministic-simulation / morphological-matrix test harness (iss-784 / epc-783).
//!
//! Cut 1 (this file): the white-box invariant battery built on PUBLIC APIs, the
//! three confirmed-bug characterization regressions, and a seeded op-loop that
//! runs the invariants after every operation. Phase 2 (StorageAdapter fault
//! injection + proptest-state-machine shrinking) and the structural
//! row-id-overlap invariant land in follow-ups; this is the compiling spine.
//!
//! Run: `cargo test -p omnigraph-engine --test dst`
//!
//! The three regressions ASSERT the *current buggy behavior* on the edge build
//! (Lance 7.0.0) — they are characterization guards that will visibly break
//! (forcing review) when each bug is fixed:
//!   - RC-X  → Lance #7230 (fixed in Lance v8.0.0): scalar-BTREE from_sorted_iter
//!   - RC-1  → stale-view manifest CAS on delete op-class combos
//!   - dup-@key → MR-714: uniqueness not enforced across concurrent commits

use std::sync::Arc;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;

// One schema exercising both bug surfaces: Person+Knows(@card) for the
// delete-cascade / stale-view path, and Doc.source (low-cardinality enum
// @index → scalar BTREE) for the index-corruption path.
const SCHEMA: &str = r#"
node Person {
    slug: String @key
    name: String
}
node Doc {
    slug: String @key
    source: enum(whatsapp, email, linkedin, slack, telegram) @index
    body: String
}
edge Knows: Person -> Person @card(0..1)
"#;

// ─── inline deterministic RNG (xorshift64*, no `rand` dep — same as examples) ───
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E3779B97F4A7C15)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

async fn init(uri: &str) -> Omnigraph {
    Omnigraph::init(uri, SCHEMA).await.expect("init")
}

fn person(slug: &str) -> String {
    format!("{{\"type\":\"Person\",\"data\":{{\"slug\":\"{slug}\",\"name\":\"n\"}}}}\n")
}
fn doc(slug: &str, source: &str) -> String {
    format!("{{\"type\":\"Doc\",\"data\":{{\"slug\":\"{slug}\",\"source\":\"{source}\",\"body\":\"needle filler\"}}}}\n")
}
fn knows(from: &str, to: &str) -> String {
    format!("{{\"edge\":\"Knows\",\"from\":\"{from}\",\"to\":\"{to}\",\"data\":{{}}}}\n")
}

async fn count(db: &Omnigraph, ty: &str) -> Result<usize, String> {
    let q = format!("query q() {{ match {{ $x: {ty} }} return {{ $x.slug }} }}");
    db.query(ReadTarget::branch("main"), &q, "q", &ParamMap::new())
        .await
        .map(|r| r.num_rows())
        .map_err(|e| e.to_string())
}

// ── WHITE-BOX INVARIANT #1: Lance HEAD == manifest table version, per table ──
// Catches RC-1's drift precondition. Uses only public Snapshot + Lance Dataset.
async fn invariant_head_eq_manifest(db: &Omnigraph) -> Result<(), String> {
    let snap = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .map_err(|e| format!("snapshot_of: {e}"))?;
    for entry in snap.entries() {
        let ds = snap
            .open(&entry.table_key)
            .await
            .map_err(|e| format!("open {}: {e}", entry.table_key))?;
        let lance_head = ds.version().version;
        if lance_head != entry.table_version {
            return Err(format!(
                "HEAD!=manifest on {}: lance_head={} manifest_pin={}",
                entry.table_key, lance_head, entry.table_version
            ));
        }
    }
    Ok(())
}

fn is_stale_view(e: &str) -> bool {
    let l = e.to_lowercase();
    l.contains("stale view") || (l.contains("expected") && l.contains("current"))
}

// ── WHITE-BOX INVARIANT #2: Lance Dataset::validate() per table ──
// General structural corruption: fragment-id uniqueness/ordering, equal frag
// lengths, index metadata. (Does NOT inspect scalar-BTREE page content — that's
// the index-probe below.)
async fn invariant_dataset_validate(db: &Omnigraph) -> Result<(), String> {
    let snap = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .map_err(|e| e.to_string())?;
    for entry in snap.entries() {
        let ds = snap.open(&entry.table_key).await.map_err(|e| e.to_string())?;
        ds.validate()
            .await
            .map_err(|e| format!("validate {}: {e}", entry.table_key))?;
    }
    Ok(())
}

// ── WHITE-BOX INVARIANT #3: scalar-index probe (catches RC-X at creation) ──
// Force-loads the Doc.source BTREE flat pages by filtering on each enum value;
// a duplicate-row-address page (Lance #7230) trips `from_sorted_iter` here,
// deterministically, before a random read happens to hit it.
async fn invariant_index_probe(db: &Omnigraph) -> Result<(), String> {
    for src in ["whatsapp", "email", "linkedin", "slack", "telegram"] {
        let q = format!(
            "query w() {{ match {{ $d: Doc {{ source: \"{src}\" }} }} return {{ $d.slug }} }}"
        );
        db.query(ReadTarget::branch("main"), &q, "w", &ParamMap::new())
            .await
            .map_err(|e| format!("index-probe source={src}: {e}"))?;
    }
    Ok(())
}

/// Known open bugs we allow-list so the generative walk explores PAST them and
/// surfaces NOVEL violations (which fail the run). Returns the bug name if the
/// error/violation string matches a known signature, else None (= novel).
fn known_bug(e: &str) -> Option<&'static str> {
    let l = e.to_lowercase();
    if l.contains("from_sorted") || l.contains("non-sorted") {
        Some("RC-X/#7230 scalar-BTREE")
    } else if l.contains("stale view") || (l.contains("expected") && l.contains("current")) {
        Some("RC-1 stale-view")
    } else {
        None
    }
}

// ═══════════════════════════════════ regressions ═══════════════════════════

/// RC-1 — multi-statement `delete Person + delete Knows` deterministically
/// fails with a spurious stale-view manifest-CAS error (node-delete cascade
/// bumps edge:Knows under the explicit edge-delete's pinned version).
#[tokio::test]
async fn regression_rc1_stale_view_on_delete_combo() {
    let dir = tempfile::tempdir().unwrap();
    let db = init(dir.path().to_str().unwrap()).await;
    // ring of 50 persons each Knows the next (valid for @card(0..1))
    let mut seed = String::new();
    for i in 0..50 {
        seed.push_str(&person(&format!("p{i}")));
    }
    for i in 0..50 {
        seed.push_str(&knows(&format!("p{i}"), &format!("p{}", (i + 1) % 50)));
    }
    load_jsonl(&db, &seed, LoadMode::Merge).await.unwrap();

    let q = "query m() { delete Person where slug = \"p30\" delete Knows where from = \"p30\" }";
    let res = db.mutate("main", q, "m", &ParamMap::new()).await;

    // CHARACTERIZATION: current edge build returns a spurious stale-view error.
    // Flip this assertion (expect Ok) when the op-class-aware fix lands.
    match res {
        Err(e) if is_stale_view(&e.to_string()) => { /* bug reproduced */ }
        Err(e) => panic!("RC-1: expected stale-view, got other error: {e}"),
        Ok(_) => panic!("RC-1 appears FIXED — flip this regression to expect Ok"),
    }
}

/// RC-X — Lance #7230: a scalar BTREE over a low-cardinality column corrupts
/// (duplicate row addresses) under UPDATE racing optimize; a subsequent
/// indexed-filter read crashes with `from_sorted_iter ... non-sorted input`.
#[tokio::test]
async fn regression_rc_x_btree_from_sorted_iter() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(init(dir.path().to_str().unwrap()).await);
    // Docs with a dominant source value, across a few fragments (kept small —
    // the corruption is fragment-layout-sensitive, not volume-sensitive).
    const FRAGS: usize = 3;
    const PER: usize = 600;
    for frag in 0..FRAGS {
        let mut data = String::new();
        for i in 0..PER {
            let s = if i % 12 == 0 { "email" } else { "whatsapp" };
            data.push_str(&doc(&format!("d{frag}-{i}"), s));
        }
        load_jsonl(&db, &data, LoadMode::Merge).await.unwrap();
    }
    let _ = db.optimize().await; // build the BTREE

    // UPDATE bodies (moves whole rows → scalar-index remap) racing optimize.
    let upd = {
        let db = db.clone();
        tokio::spawn(async move {
            for round in 0..3 {
                for frag in 0..FRAGS {
                    for i in (0..PER).step_by(40) {
                        let q = format!(
                            "query u() {{ update Doc set {{ body: \"r{round} needle\" }} where slug = \"d{frag}-{i}\" }}"
                        );
                        for _ in 0..4 {
                            if db.mutate("main", &q, "u", &ParamMap::new()).await.is_ok() {
                                break;
                            }
                        }
                    }
                }
            }
        })
    };
    let opt = {
        let db = db.clone();
        tokio::spawn(async move {
            for _ in 0..4 {
                let _ = db.optimize().await;
            }
        })
    };
    let _ = upd.await;
    let _ = opt.await;

    // Indexed-filter read on the dominant value.
    let r = db
        .query(
            ReadTarget::branch("main"),
            "query w() { match { $d: Doc { source: \"whatsapp\" } } return { $d.slug } }",
            "w",
            &ParamMap::new(),
        )
        .await;

    // CHARACTERIZATION: on Lance 7.0.0 this read errors with from_sorted_iter.
    // Flip to expect Ok after upgrading to Lance v8.0.0 (PR #7235).
    match r {
        Err(e) if {
            let l = e.to_string().to_lowercase();
            l.contains("from_sorted") || l.contains("non-sorted")
        } => { /* bug reproduced */ }
        Err(e) => panic!("RC-X: expected from_sorted_iter, got: {e}"),
        Ok(res) => panic!(
            "RC-X appears FIXED ({} rows) — flip this regression to expect Ok (Lance >= 8.0.0)",
            res.num_rows()
        ),
    }
}

/// dup-@key (MR-714) — concurrent same-key merge-upserts produce duplicate
/// rows; uniqueness is not enforced across concurrent commits.
#[tokio::test]
async fn regression_dup_key_under_concurrency() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(init(dir.path().to_str().unwrap()).await);
    let workers = 4usize;
    let keys = 500usize;
    let mut handles = Vec::new();
    for _ in 0..workers {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            let mut data = String::new();
            for k in 0..keys {
                data.push_str(&person(&format!("hot-{k}")));
            }
            for _ in 0..12 {
                if load_jsonl(&db, &data, LoadMode::Merge).await.is_ok() {
                    break;
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let total = count(&db, "Person").await.unwrap();
    // CHARACTERIZATION: current build yields total > keys (duplicate @key).
    // Flip to assert total == keys when cross-commit uniqueness is enforced.
    assert!(
        total > keys,
        "dup-@key appears FIXED: total={total} == distinct keys={keys}; flip this regression"
    );
}

// ═══════════════════════════ seeded generative op-loop ═════════════════════
// Runs random ops; after EACH, asserts the white-box invariant battery holds.
// RC-1 (stale-view) is classified as retryable so the walk explores past it.

async fn run_op(db: &Omnigraph, rng: &mut Rng, next_id: &mut usize) -> Result<(), String> {
    match rng.below(6) {
        0 => {
            // insert persons
            let mut data = String::new();
            for _ in 0..(1 + rng.below(20)) {
                data.push_str(&person(&format!("g{}", *next_id)));
                *next_id += 1;
            }
            load_jsonl(db, &data, LoadMode::Merge).await.map(|_| ()).map_err(|e| e.to_string())
        }
        1 => {
            // insert docs (indexed scalar)
            let mut data = String::new();
            for _ in 0..(1 + rng.below(20)) {
                let s = if rng.below(100) < 85 { "whatsapp" } else { "email" };
                data.push_str(&doc(&format!("g{}", *next_id), s));
                *next_id += 1;
            }
            load_jsonl(db, &data, LoadMode::Merge).await.map(|_| ()).map_err(|e| e.to_string())
        }
        2 => db.optimize().await.map(|_| ()).map_err(|e| e.to_string()),
        3 => {
            // delete a person (may not exist — fine)
            let id = rng.below((*next_id).max(1));
            let q = format!("query d() {{ delete Person where slug = \"g{id}\" }}");
            db.mutate("main", &q, "d", &ParamMap::new()).await.map(|_| ()).map_err(|e| e.to_string())
        }
        4 => {
            // UPDATE a doc body — moves the whole row → scalar-index remap (the
            // morphology that, combined with optimize, mints RC-X corruption).
            let id = rng.below((*next_id).max(1));
            let q = format!("query u() {{ update Doc set {{ body: \"u{id} needle\" }} where slug = \"g{id}\" }}");
            db.mutate("main", &q, "u", &ParamMap::new()).await.map(|_| ()).map_err(|e| e.to_string())
        }
        _ => {
            // read (indexed filter)
            db.query(
                ReadTarget::branch("main"),
                "query w() { match { $d: Doc { source: \"whatsapp\" } } return { $d.slug } }",
                "w",
                &ParamMap::new(),
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
        }
    }
}

#[tokio::test]
async fn seeded_op_loop_invariants_hold() {
    let mut reproduced: Vec<String> = Vec::new();
    for seed in 0..2u64 {
        let dir = tempfile::tempdir().unwrap();
        let db = init(dir.path().to_str().unwrap()).await;
        let mut rng = Rng::new(seed);
        let mut next_id = 0usize;

        'walk: for step in 0..15 {
            // Op error: a KNOWN bug stops this seed (explored to the bug); a
            // NOVEL error fails the run.
            if let Err(e) = run_op(&db, &mut rng, &mut next_id).await {
                match known_bug(&e) {
                    Some(bug) => {
                        reproduced.push(format!("seed={seed} step={step} op -> {bug}"));
                        break 'walk;
                    }
                    None => panic!("seed={seed} step={step}: NOVEL op error: {e}"),
                }
            }
            // WHITE-BOX invariant battery after every op.
            let checks = [
                ("head==manifest", invariant_head_eq_manifest(&db).await),
                ("dataset.validate", invariant_dataset_validate(&db).await),
                ("index-probe", invariant_index_probe(&db).await),
            ];
            for (name, res) in checks {
                if let Err(v) = res {
                    match known_bug(&v) {
                        // Known corruption is terminal for this seed (e.g. a
                        // corrupt BTREE page does not self-heal) — record & move on.
                        Some(bug) => {
                            reproduced.push(format!("seed={seed} step={step} [{name}] -> {bug}"));
                            break 'walk;
                        }
                        None => panic!(
                            "seed={seed} step={step}: NOVEL invariant violation [{name}]: {v}"
                        ),
                    }
                }
            }
        }
    }
    // The generative walk is EXPECTED to (sometimes) reproduce known bugs on the
    // current build; it must NEVER surface a NOVEL invariant violation (those
    // panic above). This is the regression guard for new corruption classes.
    eprintln!(
        "[dst] generative walk: {} known-bug instance(s) reproduced, 0 novel violations",
        reproduced.len()
    );
    for r in &reproduced {
        eprintln!("  - {r}");
    }
}
