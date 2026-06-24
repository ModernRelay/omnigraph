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
    /// Live `Knows` edges as `from -> to` (the map enforces the schema's
    /// `@card(0..1)`: a `from` has at most one outgoing edge). Every endpoint is
    /// a live Person by construction — `del_person` cascades both directions, so
    /// the model never holds an orphan and the engine producing one is a finding.
    knows: HashMap<usize, usize>,
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
        // Node delete cascades to incident edges, BOTH directions — mirror it so
        // the model never references a deleted Person (this is the RC-1 surface).
        self.knows.remove(&id);
        self.knows.retain(|_, &mut to| to != id);
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

    // ── edges ──
    /// Live person ids (for picking an edge endpoint).
    pub fn persons_vec(&self) -> Vec<usize> {
        self.persons.iter().copied().collect()
    }
    /// Persons with no outgoing `Knows` — the only legal `from` for a new edge
    /// under `@card(0..1)`, so every generated InsertKnows is a valid op.
    pub fn free_froms(&self) -> Vec<usize> {
        self.persons
            .iter()
            .copied()
            .filter(|p| !self.knows.contains_key(p))
            .collect()
    }
    /// Persons that currently have an outgoing edge (legal DeleteKnows targets).
    pub fn knows_froms(&self) -> Vec<usize> {
        self.knows.keys().copied().collect()
    }
    pub fn add_edge(&mut self, from: usize, to: usize) {
        self.knows.insert(from, to);
    }
    pub fn del_edge(&mut self, from: usize) {
        self.knows.remove(&from);
    }
    pub fn edges(&self) -> usize {
        self.knows.len()
    }
    /// Debug-only: the live edges as (from,to) pairs.
    pub fn knows_pairs(&self) -> Vec<(usize, usize)> {
        self.knows.iter().map(|(&f, &t)| (f, t)).collect()
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

/// edges==model (referential integrity): two complementary counts must both
/// equal the model's live-edge count. The RAW `edge:Knows` row count (read
/// white-box via the snapshot, so it sees orphans too) catches a lost
/// node-delete cascade that strands an edge pointing at a deleted Person; the
/// TRAVERSAL count (`$a knows $b`, which only matches live endpoints) catches a
/// lost edge write. Raw > traversal means an orphan exists.
pub async fn check_edges(db: &Omnigraph, model: &Model) -> Result<(), Finding> {
    let snap = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .map_err(Finding::Engine)?;
    let mut raw: usize = 0;
    for entry in snap.entries() {
        if entry.table_key == "edge:Knows" {
            let ds = snap.open(&entry.table_key).await.map_err(Finding::Engine)?;
            raw = ds
                .count_rows(None)
                .await
                .map_err(|e| Finding::Logical(format!("edge:Knows count_rows: {e}")))?;
        }
    }
    if raw != model.edges() {
        return Err(Finding::Logical(format!(
            "raw edge:Knows rows={raw} != model={} (orphan edge / lost cascade / dup edge)",
            model.edges()
        )));
    }
    let res = db
        .query(
            ReadTarget::branch("main"),
            "query e() { match { $a: Person $a knows $b } return { $a.slug, $b.slug } }",
            "e",
            &ParamMap::new(),
        )
        .await
        .map_err(Finding::Engine)?;
    if res.num_rows() != model.edges() {
        return Err(Finding::Logical(format!(
            "traversable Knows edges={} != model={} (orphan endpoint / lost edge)",
            res.num_rows(),
            model.edges()
        )));
    }
    Ok(())
}
