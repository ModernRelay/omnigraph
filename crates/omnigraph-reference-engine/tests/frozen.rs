//! Engine v1 is frozen: every file under this crate's `src/` is pinned by
//! the SHA-256 of its bytes below, and a file missing from the list fails
//! too. v1 is the reference GQT's `--- expect same as v1` compares engine v2
//! against, so a defect seen on v1 is fixed on v2 (`crates/omnigraph/src/engine/`).
//! Editing a frozen file, or adding one, means updating this list in the
//! same PR, under a reviewer's eyes.
//!
//! The layers below v1 (`omnigraph-catalog`'s `Snapshot`, `omnigraph-core`,
//! the compiler) and dependency bumps (Lance, DataFusion) can change v1's
//! behaviour without touching its bytes; the `expect same as v1` steps are
//! the behaviour check.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// The change the hashes below were taken at: PR #795 moved v1 out of the engine
/// into this crate (`projection.rs` byte-identical to the engine's former pin;
/// `gate.rs` re-pinned for its refusal advice to the `expect same as v1` caller).
const BASELINE: &str = "PR #795 move";

/// (path under `src/`, SHA-256 of the file's bytes).
const FROZEN: &[(&str, &str)] = &[
    (
        "gate.rs",
        "c88ab5d0534db9e209ab7733330a6b444d5e256b46944167849b5d7a5d73eec0",
    ),
    (
        "graph_index.rs",
        "f9012d8bec7adac98e148e632585d3632506397c2faa8007c085f28ff6036d7f",
    ),
    (
        "instrumentation.rs",
        "8d54fc849b40ce31b9eb67cd158edaf5d5ea066b27e99a4d135d16291cf506b1",
    ),
    (
        "lib.rs",
        "853e5cdccffcd45926a7fd46f61ada5a496e10abad6a1e8fba784f783b232f8e",
    ),
    (
        "loader.rs",
        "da83849238af9211bb67c850b3d6aa1010964cb49391949e2c88851971b9656f",
    ),
    (
        "projection.rs",
        "0b645a65c338d28d4fcbe5408e0852368d57f12660c6ff8c463a2b062b6c4413",
    ),
    (
        "query.rs",
        "51ffdefd26d34d7034213ab325a3e890e660091772808d0cdd037832e0fd76bb",
    ),
    (
        "table_store.rs",
        "569223b084dc997618f7ee47550e29bb9f45281182dfa397fc663d7d672d799d",
    ),
];

fn files_under(dir: &Path, root: &Path, out: &mut BTreeSet<String>) {
    let entries = std::fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("`{}` is unreadable: {error}", dir.display()));
    for entry in entries {
        let path: PathBuf = entry.expect("directory entry").path();
        if path.is_dir() {
            files_under(&path, root, out);
        } else {
            let relative = path
                .strip_prefix(root)
                .expect("invariant: the walk stays under src/")
                .to_string_lossy()
                .replace('\\', "/");
            out.insert(relative);
        }
    }
}

#[test]
fn every_source_file_matches_the_reviewed_frozen_bytes() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut on_disk = BTreeSet::new();
    files_under(&src, &src, &mut on_disk);
    let listed: BTreeSet<String> = FROZEN.iter().map(|(path, _)| path.to_string()).collect();
    let unlisted: Vec<&String> = on_disk.difference(&listed).collect();
    let missing: Vec<&String> = listed.difference(&on_disk).collect();
    assert!(
        unlisted.is_empty() && missing.is_empty(),
        "the frozen list and src/ disagree: unlisted {unlisted:?}, missing {missing:?}; \
         a v1 file change needs this list in tests/frozen.rs updated in the same PR"
    );
    for (path, expected) in FROZEN {
        let bytes = std::fs::read(src.join(path))
            .unwrap_or_else(|error| panic!("frozen file `src/{path}` is unreadable: {error}"));
        let actual = format!("{:x}", Sha256::digest(&bytes));
        assert_eq!(
            &actual, expected,
            "`src/{path}` is frozen at `{BASELINE}`; a v1 edit needs its hash in tests/frozen.rs \
             updated in the same PR and a reviewer's eyes. A defect seen on v1 is fixed on v2 \
             (`crates/omnigraph/src/engine/`)."
        );
    }
}
