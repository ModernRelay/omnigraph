//! D1: the operation alphabet, as data. `step` picks an op, executes it against
//! the real engine, and updates the reference `Model` on success. Returns the
//! `OpKind` (for coverage) and the raw `OmniError` (for structured
//! classification — never stringified here).

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;

use crate::model::Model;

/// One schema exercising both bug surfaces: Person+Knows(@card) for the
/// delete-cascade / stale-view path, and Doc.source (low-cardinality enum
/// @index → scalar BTREE) for the index-corruption path.
pub const SCHEMA: &str = r#"
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

/// Inline deterministic RNG (xorshift64*, no `rand` dep).
pub struct Rng(u64);
impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

pub fn person(slug: &str) -> String {
    format!("{{\"type\":\"Person\",\"data\":{{\"slug\":\"{slug}\",\"name\":\"n\"}}}}\n")
}
pub fn doc(slug: &str, source: &str) -> String {
    format!("{{\"type\":\"Doc\",\"data\":{{\"slug\":\"{slug}\",\"source\":\"{source}\",\"body\":\"needle filler\"}}}}\n")
}
pub fn knows(from: &str, to: &str) -> String {
    format!("{{\"edge\":\"Knows\",\"from\":\"{from}\",\"to\":\"{to}\",\"data\":{{}}}}\n")
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum OpKind {
    InsertPerson,
    InsertDoc,
    Optimize,
    DeletePerson,
    UpdateDoc,
    Read,
}

impl OpKind {
    pub const ALL: [OpKind; 6] = [
        OpKind::InsertPerson,
        OpKind::InsertDoc,
        OpKind::Optimize,
        OpKind::DeletePerson,
        OpKind::UpdateDoc,
        OpKind::Read,
    ];
}

/// Pick and run one op. The model is updated only on success.
pub async fn step(db: &Omnigraph, rng: &mut Rng, model: &mut Model) -> (OpKind, Result<(), OmniError>) {
    match rng.below(6) {
        0 => {
            let mut ids = Vec::new();
            let mut data = String::new();
            for _ in 0..(1 + rng.below(10)) {
                let id = model.fresh_id();
                ids.push(id);
                data.push_str(&person(&format!("g{id}")));
            }
            let res = match load_jsonl(db, &data, LoadMode::Merge).await {
                Ok(_) => {
                    for id in ids {
                        model.add_person(id);
                    }
                    Ok(())
                }
                Err(e) => Err(e),
            };
            (OpKind::InsertPerson, res)
        }
        1 => {
            let mut ids = Vec::new();
            let mut data = String::new();
            for _ in 0..(1 + rng.below(10)) {
                let id = model.fresh_id();
                ids.push(id);
                let s = if rng.below(100) < 85 { "whatsapp" } else { "email" };
                data.push_str(&doc(&format!("g{id}"), s));
            }
            let res = match load_jsonl(db, &data, LoadMode::Merge).await {
                Ok(_) => {
                    // `doc()` writes body "needle filler" — track it for content==model.
                    for id in ids {
                        model.add_doc(id, "needle filler".to_string());
                    }
                    Ok(())
                }
                Err(e) => Err(e),
            };
            (OpKind::InsertDoc, res)
        }
        2 => (OpKind::Optimize, db.optimize().await.map(|_| ())),
        3 => {
            let id = rng.below(model.id_high());
            let q = format!("query d() {{ delete Person where slug = \"g{id}\" }}");
            let res = match db.mutate("main", &q, "d", &ParamMap::new()).await {
                Ok(_) => {
                    model.del_person(id);
                    Ok(())
                }
                Err(e) => Err(e),
            };
            (OpKind::DeletePerson, res)
        }
        4 => {
            // UPDATE moves the whole row → scalar-index remap (RC-X morphology).
            let id = rng.below(model.id_high());
            let body = format!("u{id} needle");
            let q = format!("query u() {{ update Doc set {{ body: \"{body}\" }} where slug = \"g{id}\" }}");
            let res = match db.mutate("main", &q, "u", &ParamMap::new()).await {
                Ok(_) => {
                    // Only mutates the model for a Doc it believes exists, so a
                    // no-op update (0 rows matched) can't desync content==model.
                    model.update_doc_body(id, body);
                    Ok(())
                }
                Err(e) => Err(e),
            };
            (OpKind::UpdateDoc, res)
        }
        _ => (
            OpKind::Read,
            db.query(
                ReadTarget::branch("main"),
                "query w() { match { $d: Doc { source: \"whatsapp\" } } return { $d.slug } }",
                "w",
                &ParamMap::new(),
            )
            .await
            .map(|_| ()),
        ),
    }
}
