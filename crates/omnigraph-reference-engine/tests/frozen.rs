//! Engine v1 is frozen: every file under this crate's `src/` and the compiler's
//! `ir/untyped.rs` are pinned by the SHA-256 of their bytes below. A source file
//! missing from the list fails too. v1 is the reference GQT's
//! `--- expect same as v1` compares engine v2 against, so a defect seen on v1
//! is fixed on v2 (`crates/omnigraph/src/engine/`).
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

/// The reference executor over its frozen read IR. New selectors and synthetic
/// edge type reads are refused; named-edge execution is the reference behavior.
const BASELINE: &str = "frozen read IR with typed-edge adapters";

const FROZEN_IR_SHA256: &str = "0e1a73646d4fd910f10f9d0944a3e20cf74cefb570e49469621c2b163ed5891a";

/// (path under `src/`, SHA-256 of the file's bytes).
const FROZEN: &[(&str, &str)] = &[
    (
        "gate.rs",
        "b0e75a0ce4e60596b496305e01c52889cdad14335ab95c807025d399570466d6",
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
        "eb6a7c1b76a8ae51b6336779b81a715c369e9872f5ce08d5cb8ccca000b18851",
    ),
    (
        "loader.rs",
        "da83849238af9211bb67c850b3d6aa1010964cb49391949e2c88851971b9656f",
    ),
    (
        "projection.rs",
        "be11d4ce201b212c0e8e2963a5ad5f0e3b8cd874e033eefef24874f02d6a4631",
    ),
    (
        "query.rs",
        "97ffde90e498a9b1568567cd1cc3dc2d284e9111360ce85abacb1a0b2341c1a1",
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
    let ir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../omnigraph-compiler/src/ir/untyped.rs");
    let bytes = std::fs::read(&ir)
        .unwrap_or_else(|error| panic!("frozen IR `{}` is unreadable: {error}", ir.display()));
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        FROZEN_IR_SHA256,
        "`ir/untyped.rs` is frozen at `{BASELINE}`; an edit needs its hash in \
         tests/frozen.rs updated in the same PR and a reviewer's eyes"
    );
}
