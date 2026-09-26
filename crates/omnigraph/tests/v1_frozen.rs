//! Engine v1 is frozen (RFC 0068 milestone 3, phase 4): the read executor
//! and the two listed tests are pinned below. The query door extraction is
//! included in the executor pin; a defect seen on v1
//! is fixed on v2 (`src/engine/`), which the session setting `engine = v2`
//! selects. This test pins the bytes so the freeze is a CI fact, not a
//! convention; editing a frozen file means updating its hash here in the
//! same PR, under a reviewer's eyes.
//!
//! Unpinned owners include traversal_adaptive.rs, traversal_indexed.rs,
//! rrf_prefilter_gate.rs, and exec/query_doors.rs. The shared layer (`TableStore`, `Snapshot`,
//! the catalog, the graph index) and dependency bumps (Lance, DataFusion)
//! can change v1's behaviour without touching its bytes; the `engine-v2`
//! corpus matrix on every PR is the behaviour check for both routes.

use std::path::Path;

use sha2::{Digest, Sha256};

/// The baseline predates query-door extraction and correlated AntiJoin support.
/// The search fixture also has failpoints-gated four-partition IVF setup;
/// its query assertions are unchanged.
const BASELINE: &str = "8281807b";

/// (path under the crate, SHA-256 of the file's bytes).
const FROZEN: &[(&str, &str)] = &[
    (
        "src/exec/query.rs",
        "eafe1395d8bfb870cf00892538de69fad90ed826f715876dff49bd942de6e07b",
    ),
    (
        "src/exec/projection.rs",
        "0b645a65c338d28d4fcbe5408e0852368d57f12660c6ff8c463a2b062b6c4413",
    ),
    (
        "tests/traversal.rs",
        "de71294f17b74072264b4ebc5a0a3341f9d3144b5116aab7ac503911e7da62e0",
    ),
    (
        "tests/search.rs",
        "6f3589753ad30b29cf612c766d90550457ab3299bf23a066d0d7c50ad97c8ff4",
    ),
];

#[test]
fn v1_files_match_the_reviewed_frozen_bytes() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for (path, expected) in FROZEN {
        let bytes = std::fs::read(crate_root.join(path))
            .unwrap_or_else(|error| panic!("frozen file `{path}` is unreadable: {error}"));
        let actual = format!("{:x}", Sha256::digest(&bytes));
        assert_eq!(
            &actual, expected,
            "`{path}` is frozen relative to upstream `{BASELINE}` (including the query door extraction); a v1 edit needs the hash \
             in tests/v1_frozen.rs updated in the same PR and a reviewer's eyes. A defect seen \
             on v1 is fixed on v2 (`src/engine/`), selected by `set engine = v2;`."
        );
    }
}
