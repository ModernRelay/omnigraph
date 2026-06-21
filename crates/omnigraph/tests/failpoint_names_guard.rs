//! Guard: failpoint names must come from the compile-checked `names` catalog
//! (`omnigraph::failpoints::names` / `omnigraph_cluster::failpoints::names`),
//! never bare string literals.
//!
//! The `names` consts give compile-time typo protection only if every call
//! site uses them. A bare `maybe_fail("typo.literal")` still compiles (the
//! arg is `&str`), so a typo there would silently never fire. This
//! source-walk closes that gap by construction — the same defense-in-depth
//! shape as `forbidden_apis.rs`. Add a new failpoint by adding its const to
//! the catalog first; this guard then forces every call site to reference it.

use std::path::{Path, PathBuf};

/// Call-site prefixes whose first argument must be a `names::` constant. A
/// literal first argument makes the prefix immediately precede a `"`.
const LITERAL_CALL_PATTERNS: &[&str] = &[
    "maybe_fail(\"",
    "ScopedFailPoint::new(\"",
    "ScopedFailPoint::with_callback(\"",
    "park_first(\"",
];

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Production call sites live under each crate's `src`; test call sites live
/// in the two failpoint integration binaries. This guard file is deliberately
/// not in the set (it names the patterns as literals itself).
fn files_to_scan() -> Vec<PathBuf> {
    let engine = manifest_dir();
    let cluster = engine.join("../omnigraph-cluster");
    let mut out = Vec::new();
    collect_rs(&engine.join("src"), &mut out);
    collect_rs(&cluster.join("src"), &mut out);
    out.push(engine.join("tests/failpoints.rs"));
    out.push(cluster.join("tests/failpoints.rs"));
    out
}

fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn failpoint_names_use_the_compile_checked_catalog() {
    let mut violations = Vec::new();
    for file in files_to_scan() {
        let Ok(contents) = std::fs::read_to_string(&file) else {
            continue;
        };
        for (idx, line) in contents.lines().enumerate() {
            for pattern in LITERAL_CALL_PATTERNS {
                if line.contains(pattern) {
                    violations.push(format!("{}:{}: {}", file.display(), idx + 1, line.trim()));
                }
            }
        }
    }
    assert!(
        violations.is_empty(),
        "failpoint names must reference the compile-checked \
         `omnigraph::failpoints::names::*` (or `omnigraph_cluster::failpoints::names::*`) \
         constants, not string literals — a literal typo would silently never fire:\n{}",
        violations.join("\n")
    );
}
