//! `Cohort<B>`: N INDEPENDENT writers over one store — the multi-coordinator
//! surface single-handle harness code cannot reach.
//!
//! The existing concurrent walk shares one `Arc<Omnigraph>`, so all actors hit
//! ONE coordinator, one `known_state`, and one in-process write queue. That can
//! never produce the diverging-warm-state race: handle B's warm publish state
//! going stale exactly when handle A commits. A `Cohort` opens N INDEPENDENT
//! handles — the embedded tier (`open_embedded`) opens N separate `Omnigraph`
//! handles, each with its own `ManifestCoordinator` + `known_state`; the CLI
//! tier is N subprocess configs in separate address spaces (no shared `Session`
//! or write queue). Generic over the `Backend` trait, so the in-process and
//! cross-process tiers are ONE abstraction over `Embedded` vs `Cli` rather than
//! two hand-rolled files. Phase 3 / Phase 5 / U2 / the epoch fence each reuse
//! this with one new schedule or one convergence-battery line.

use crate::backend::{Backend, Embedded};

/// A set of independent writers sharing one store URI. Generic over `Backend`
/// so the same driver runs the embedded (separate coordinators) and CLI
/// (separate processes) tiers.
pub struct Cohort<B: Backend> {
    writers: Vec<B>,
    store_uri: String,
}

impl<B: Backend> Cohort<B> {
    /// All writers, in open order (writer 0 is the one that initialized the
    /// graph in the embedded tier).
    pub fn writers(&self) -> &[B] {
        &self.writers
    }
    /// The `i`th independent writer.
    pub fn writer(&self, i: usize) -> &B {
        &self.writers[i]
    }
    pub fn len(&self) -> usize {
        self.writers.len()
    }
    pub fn is_empty(&self) -> bool {
        self.writers.is_empty()
    }
    pub fn store_uri(&self) -> &str {
        &self.store_uri
    }
}

impl Cohort<Embedded> {
    /// Open `n` INDEPENDENT coordinators on one store. Writer 0 initializes the
    /// graph (with the harness `SCHEMA`); writers 1..n reopen it. Each handle
    /// holds its own `ManifestCoordinator` + `known_state`, so a commit by one
    /// makes the others' warm state stale — exactly the surface a single-handle
    /// test cannot reach. `uri` must point at a fresh (uninitialized) store.
    pub async fn open_embedded(uri: &str, n: usize) -> Self {
        assert!(n >= 1, "a cohort needs at least one writer");
        let mut writers = Vec::with_capacity(n);
        writers.push(Embedded::open_clean(uri).await); // writer 0 inits the graph
        for _ in 1..n {
            writers.push(Embedded::reopen(uri).await); // independent coordinator
        }
        Cohort {
            writers,
            store_uri: uri.to_string(),
        }
    }
}
