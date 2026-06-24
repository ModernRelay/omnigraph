//! Reference model (D4 oracle source of truth) + the count==model invariant.
//!
//! Tracks the SET of live Person/Doc keys the harness believes should exist.
//! Updated only AFTER an op succeeds, so a failed op (e.g. RC-1 stale-view, or
//! a FaultAdapter-injected CAS loss) leaves the model consistent with reality.
//! A divergence means a lost write (count<model) or a duplicate key
//! (count>model) — both real bugs.

use std::collections::{HashMap, HashSet};

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph_compiler::ir::ParamMap;

use crate::invariants::Finding;

#[derive(Default)]
pub struct Model {
    persons: HashSet<usize>,
    docs: HashSet<usize>,
    /// Expected `body` value per live Doc id — the source of truth for the
    /// content==model oracle (a lost UPDATE shows up here even when counts match).
    doc_body: HashMap<usize, String>,
    next: usize,
}

impl Model {
    pub fn new() -> Self {
        Self::default()
    }
    /// A never-before-used id (so generated inserts never collide).
    pub fn fresh_id(&mut self) -> usize {
        let i = self.next;
        self.next += 1;
        i
    }
    /// Upper bound for picking an existing-ish id for delete/update targets.
    pub fn id_high(&self) -> usize {
        self.next.max(1)
    }
    pub fn add_person(&mut self, id: usize) {
        self.persons.insert(id);
    }
    pub fn add_doc(&mut self, id: usize, body: String) {
        self.docs.insert(id);
        self.doc_body.insert(id, body);
    }
    /// Record an UPDATE's new body — only for a Doc the model believes exists,
    /// so a no-op update (0 rows matched) doesn't desync the model.
    pub fn update_doc_body(&mut self, id: usize, body: String) {
        if self.docs.contains(&id) {
            self.doc_body.insert(id, body);
        }
    }
    pub fn del_person(&mut self, id: usize) {
        self.persons.remove(&id);
    }
    pub fn persons(&self) -> usize {
        self.persons.len()
    }
    pub fn docs(&self) -> usize {
        self.docs.len()
    }
    pub fn doc_bodies(&self) -> &HashMap<usize, String> {
        &self.doc_body
    }
}

async fn count(db: &Omnigraph, ty: &str) -> Result<usize, Finding> {
    let q = format!("query q() {{ match {{ $x: {ty} }} return {{ $x.slug }} }}");
    db.query(ReadTarget::branch("main"), &q, "q", &ParamMap::new())
        .await
        .map(|r| r.num_rows())
        .map_err(Finding::Engine)
}

/// count==model: live row counts must equal the model. A divergence is a
/// structural (Logical) finding — lost-write (count<model — e.g. a swallowed
/// CAS conflict) or duplicate-key (count>model — MR-714).
pub async fn check_counts(db: &Omnigraph, model: &Model) -> Result<(), Finding> {
    let p = count(db, "Person").await?;
    if p != model.persons() {
        return Err(Finding::Logical(format!(
            "count Person={p} != model={} (lost-write or dup-@key)",
            model.persons()
        )));
    }
    let d = count(db, "Doc").await?;
    if d != model.docs() {
        return Err(Finding::Logical(format!(
            "count Doc={d} != model={}",
            model.docs()
        )));
    }
    Ok(())
}

/// content==model: every live Doc's `body` must equal the model's expected
/// value, and no `@key` may appear twice. A count check passes through a
/// lost UPDATE (the row is still there, just stale) or a silent dup-`@key`
/// (a value-level duplicate the row count would only catch if it changed the
/// total) — this is the oracle that catches both. Slugs are `g{id}`.
pub async fn check_content(db: &Omnigraph, model: &Model) -> Result<(), Finding> {
    let res = db
        .query(
            ReadTarget::branch("main"),
            "query c() { match { $d: Doc } return { $d.slug, $d.body } }",
            "c",
            &ParamMap::new(),
        )
        .await
        .map_err(Finding::Engine)?;
    let json = res.to_rust_json();
    let rows = json
        .as_array()
        .ok_or_else(|| Finding::Logical("Doc content query did not return an array".into()))?;

    let mut seen: HashMap<usize, String> = HashMap::new();
    for row in rows {
        let slug = row["d.slug"]
            .as_str()
            .ok_or_else(|| Finding::Logical(format!("missing d.slug in {row}")))?;
        let body = row["d.body"]
            .as_str()
            .ok_or_else(|| Finding::Logical(format!("missing d.body in {row}")))?;
        let id: usize = slug
            .strip_prefix('g')
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| Finding::Logical(format!("unexpected Doc slug {slug}")))?;
        if let Some(prev) = seen.insert(id, body.to_string()) {
            return Err(Finding::Logical(format!(
                "duplicate Doc @key g{id} (dup-@key): {prev:?} and {body:?}"
            )));
        }
    }

    for (id, expected) in model.doc_bodies() {
        match seen.get(id) {
            None => {
                return Err(Finding::Logical(format!(
                    "Doc g{id} missing from engine (model body {expected:?})"
                )));
            }
            Some(actual) if actual != expected => {
                return Err(Finding::Logical(format!(
                    "Doc g{id} body mismatch: engine {actual:?} != model {expected:?} (lost update)"
                )));
            }
            _ => {}
        }
    }
    Ok(())
}
