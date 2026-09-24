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

/// The upstream commit whose bytes the frozen files hold. The two executors differ
/// from it by the query-door extraction and by the correlated-block change (the
/// `SubqueryPredicate` on `IROp::AntiJoin`); the search fixture adds RFC 0067's IVF setup.
const BASELINE: &str = "8281807b";

/// (path under the crate, SHA-256 of the file's bytes).
const FROZEN: &[(&str, &str)] = &[
    (
        "src/exec/query.rs",
        "aaf9412eceff11b54abdcdedbf5148429c85066ae723ba17d2f1c75f595fc55e",
    ),
    (
        "src/exec/projection.rs",
        "1e545e10397e1727708c6ba9880225370c6c8e87007832350a4f3a4c780760d7",
    ),
    (
        "tests/traversal.rs",
        "de71294f17b74072264b4ebc5a0a3341f9d3144b5116aab7ac503911e7da62e0",
    ),
    (
        "tests/search.rs",
        "1a8be0d56aef638d8e614ea1721a0bebb13e0d7c357c15cc293c404c80fc0fc9",
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
