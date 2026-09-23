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

/// The upstream commit whose bytes the frozen files hold; `exec/query.rs`
/// differs from it by one hunk, the door block moved to `exec/query_doors.rs`
/// behind a `#[path]` line. The search fixture includes RFC 0067's explicit
/// four-partition IVF setup because Optimize now preserves partition counts;
/// its query assertions and the frozen executors are unchanged.
const BASELINE: &str = "8281807b";

/// (path under the crate, SHA-256 of the file's bytes).
const FROZEN: &[(&str, &str)] = &[
    (
        "src/exec/query.rs",
        "7597fc366ef041cabbcd244a68f156b6fbe5fcc62e185b3551da35b4faac92bf",
    ),
    (
        "src/exec/projection.rs",
        "738c1f6cef6d867116500e4dc685576af9bbf70fa76cd3c78bc8b2198b8cb4e1",
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
