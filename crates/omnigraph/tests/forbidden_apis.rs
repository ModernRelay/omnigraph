//! MR-793 Phase 3 — forbidden-API guard test.
//!
//! Engine code (`exec/`, `db/omnigraph/`, `loader/`, `changes/`) MUST NOT
//! call Lance's inline-commit data-write APIs directly. The
//! `Storage` trait (`crate::storage_layer::TableStorage`) is the canonical
//! surface; staged primitives (`stage_append`, `stage_merge_insert`,
//! `stage_overwrite`, `stage_create_btree_index`,
//! `stage_create_inverted_index`) plus `commit_staged` are the only
//! way to advance Lance HEAD.
//!
//! The trait is sealed (only `TableStore` impls it), so by-construction
//! the trait surface forbids ad-hoc Lance calls. This test is **defense
//! in depth** — it catches the case where engine code reaches around
//! the trait by importing `lance::dataset::*` types directly.
//!
//! ## How it works
//!
//! Walks `crates/omnigraph/src/{exec,db/omnigraph,loader,changes}/**/*.rs`,
//! greps each line for forbidden symbols. Lines whose preceding line
//! contains the sentinel comment `// forbidden-api-allow: <reason>` are
//! exempt — reviewers see the sentinel in diff and can ask "is this
//! exemption justified?"
//!
//! ## What's deliberately out of scope (allow-listed by directory)
//!
//! - `crates/omnigraph/src/table_store.rs` — IS the storage layer.
//!   The forbidden Lance APIs live here legitimately.
//! - `crates/omnigraph/src/db/manifest/**` — uses `CommitBuilder` for
//!   the cross-table manifest commit. Documented exception.
//! - `crates/omnigraph/src/storage_layer.rs` — IS the trait module.
//!
//! ## Initial state (MR-793 Phase 3)
//!
//! At the time this test was written, MR-793 has migrated three writers
//! (ensure_indices, branch_merge, schema_apply rewrites) onto staged
//! primitives. Other engine call sites (the bulk loader, exec/mutation,
//! exec/query, etc.) still use the legacy inherent `TableStore` methods
//! — they're not visible at the trait boundary, but they DO call lance
//! types. The allow-list below reflects this transitional state. Phase 9
//! tightens the allow-list as call sites migrate.

use std::path::{Path, PathBuf};

const FORBIDDEN_PATTERNS: &[&str] = &[
    "MergeInsertBuilder",
    "InsertBuilder::",
    "DeleteBuilder",
    "CommitBuilder::new",
    ".create_index_builder(",
    ".create_index_segment_builder(",
];

/// Files exempt from the guard. These are the legitimate storage-layer
/// implementations that USE the forbidden APIs to provide the staged
/// primitives.
const ALLOW_LIST_FILES: &[&str] = &[
    "table_store.rs",      // The storage layer itself.
    "storage_layer.rs",    // The trait module.
];

/// Directories exempt from the guard. Files under these paths may use
/// the forbidden APIs.
const ALLOW_LIST_DIRS: &[&str] = &[
    "db/manifest",  // Manifest publisher uses CommitBuilder for cross-table commits.
    "db/manifest/", // Belt + suspenders for the directory match.
];

const SENTINEL: &str = "// forbidden-api-allow:";

fn engine_src_root() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir).join("src")
}

fn is_allow_listed(path: &Path) -> bool {
    let path_str = path.to_string_lossy();
    if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
        if ALLOW_LIST_FILES.iter().any(|f| *f == name) {
            return true;
        }
    }
    ALLOW_LIST_DIRS.iter().any(|d| path_str.contains(d))
}

fn walk_rust_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk_into(root, &mut out);
    out
}

fn walk_into(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_into(&path, out);
        } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

#[test]
fn engine_code_does_not_call_forbidden_lance_apis() {
    let src = engine_src_root();
    let mut violations = Vec::new();

    for file in walk_rust_files(&src) {
        if is_allow_listed(&file) {
            continue;
        }
        let contents = match std::fs::read_to_string(&file) {
            Ok(c) => c,
            Err(_) => continue,
        };
        let lines: Vec<&str> = contents.lines().collect();
        for (idx, line) in lines.iter().enumerate() {
            let trimmed = line.trim_start();
            // Skip comment-only lines — references to forbidden API
            // names in doc-comments, design notes, or residual-marker
            // comments are documentation, not code use. The trait
            // surface (sealed + trait-only) is the actual enforcement;
            // this test only catches code use.
            if trimmed.starts_with("//")
                || trimmed.starts_with("/*")
                || trimmed.starts_with("*")
            {
                continue;
            }
            // Allow lines marked with the sentinel on the SAME line or
            // the immediately preceding line.
            if line.contains(SENTINEL) {
                continue;
            }
            if idx > 0 && lines[idx - 1].contains(SENTINEL) {
                continue;
            }
            for pattern in FORBIDDEN_PATTERNS {
                if line.contains(pattern) {
                    let rel = file
                        .strip_prefix(&src)
                        .unwrap_or(&file)
                        .display()
                        .to_string();
                    violations.push(format!(
                        "{}:{}: forbidden pattern `{}` — {}",
                        rel,
                        idx + 1,
                        pattern,
                        line.trim()
                    ));
                }
            }
        }
    }

    if !violations.is_empty() {
        panic!(
            "MR-793 forbidden-API guard found {} violation(s) in engine code. \
             Engine code MUST route through the `TableStorage` trait (or its \
             inherent counterparts on `TableStore`) instead of calling Lance's \
             inline-commit APIs directly. If a use is genuinely justified, add \
             the comment `// forbidden-api-allow: <reason>` on the same line or \
             the line above.\n\nViolations:\n  {}",
            violations.len(),
            violations.join("\n  ")
        );
    }
}
