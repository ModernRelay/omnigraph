//! D5 (embedded context) — graph construction. A `Backend` trait abstraction
//! (CLI / long-lived server) is the Phase-5 extension point; today there is one
//! in-process embedded backend, exposed as two open functions.

use omnigraph::db::Omnigraph;

use crate::op::SCHEMA;

/// A clean graph (no fault injection).
pub async fn open_clean(uri: &str) -> Omnigraph {
    Omnigraph::init(uri, SCHEMA).await.expect("init")
}

/// A graph whose storage injects seeded manifest-layer faults (CAS-lost). The
/// graph + schema are created cleanly first, then reopened through the
/// `FaultAdapter` so only the op workload runs under faults.
pub async fn open_faulted(uri: &str, seed: u64, cas_pct: u8) -> Omnigraph {
    Omnigraph::init(uri, SCHEMA).await.expect("init");
    let base = omnigraph::storage::storage_for_uri(uri).expect("storage_for_uri");
    let faulted = crate::fault::FaultAdapter::new(base, seed, cas_pct);
    Omnigraph::open_with_storage(uri, faulted)
        .await
        .expect("open_with_storage")
}
