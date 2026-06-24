//! Reference model (D4 oracle source of truth) + the count==model invariant.
//!
//! Tracks the SET of live Person/Doc keys the harness believes should exist.
//! Updated only AFTER an op succeeds, so a failed op (e.g. RC-1 stale-view, or
//! a FaultAdapter-injected CAS loss) leaves the model consistent with reality.
//! A divergence means a lost write (count<model) or a duplicate key
//! (count>model) — both real bugs.

use std::collections::HashSet;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph_compiler::ir::ParamMap;

#[derive(Default)]
pub struct Model {
    persons: HashSet<usize>,
    docs: HashSet<usize>,
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
    pub fn add_doc(&mut self, id: usize) {
        self.docs.insert(id);
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
}

async fn count(db: &Omnigraph, ty: &str) -> Result<usize, String> {
    let q = format!("query q() {{ match {{ $x: {ty} }} return {{ $x.slug }} }}");
    db.query(ReadTarget::branch("main"), &q, "q", &ParamMap::new())
        .await
        .map(|r| r.num_rows())
        .map_err(|e| e.to_string())
}

/// count==model: live row counts must equal the model. Catches lost-write
/// (count<model — e.g. a swallowed CAS conflict) and duplicate-key
/// (count>model — MR-714 under concurrency).
pub async fn check_counts(db: &Omnigraph, model: &Model) -> Result<(), String> {
    let p = count(db, "Person").await?;
    if p != model.persons() {
        return Err(format!(
            "count Person={p} != model={} (lost-write or dup-@key)",
            model.persons()
        ));
    }
    let d = count(db, "Doc").await?;
    if d != model.docs() {
        return Err(format!("count Doc={d} != model={}", model.docs()));
    }
    Ok(())
}
