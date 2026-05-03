//! Forbidden-API guard test.
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
//! ## Transitional allow-list
//!
//! The migration of writers onto staged primitives is incremental.
//! Several writers (ensure_indices, branch_merge, schema_apply rewrites)
//! already route through the staged primitives; others (bulk loader,
//! exec/mutation, exec/query) still use the legacy inherent
//! `TableStore` methods — they're not visible at the trait boundary, but
//! they DO call lance types. The file-level allow-list below reflects
//! this transitional state and tightens as call sites migrate.

use std::path::{Path, PathBuf};

const FORBIDDEN_PATTERNS: &[&str] = &[
    // Builder types — direct construction is the side door around the
    // staged-write surface.
    "MergeInsertBuilder",
    "InsertBuilder::",
    "DeleteBuilder",
    "CommitBuilder::new",
    ".create_index_builder(",
    ".create_index_segment_builder(",
    // Associated-function forms of inline-commit Lance APIs. These would
    // only appear in source if the file imports `lance::Dataset` and
    // calls the static fn — exactly the misuse we want to catch. These
    // patterns deliberately exclude `.append(` / `.delete(` / `.write(`
    // because those would over-match (`.delete_branch(`, `Vec::append`,
    // arrow-array `.append(`, etc.).
    "Dataset::write",
    "Dataset::append",
    "Dataset::delete",
    "Dataset::merge_insert",
    "Dataset::add_columns",
    "Dataset::update_columns",
    "Dataset::drop_columns",
    "Dataset::truncate_table",
    "Dataset::restore",
    // Lance-specific method names that don't clash with our `TableStore`
    // wrappers (we use `merge_insert_batch{,es}`, `add_columns_to_*`,
    // etc. — never the bare Lance names). Engine code that writes
    // `ds.merge_insert(...)` against a `Dataset` value is reaching
    // around the trait surface.
    ".merge_insert(",
    ".add_columns(",
    ".update_columns(",
    ".drop_columns(",
    ".truncate_table(",
    // `.restore(` is Lance-specific (no other library in this workspace
    // exposes a `.restore(` method); safe to ban without false-positive
    // risk. Used to revert a Lance dataset to a prior version — never
    // an operation engine code should perform directly.
    ".restore(",
    // NOT included: `.append(`, `.delete(`, `.write(`. Each over-matches
    // legitimate non-Lance uses (`Vec::append`, `String::append`, arrow
    // array `BuilderArray::append`, `ObjectStore::delete`, etc.).
    // Engine code calling `ds.append(reader, params)` against an
    // imported `lance::Dataset` is the residual bypass route this guard
    // does NOT catch — but the trait surface itself is the primary
    // enforcement (sealed + only-callable-via-trait once Phase 1b
    // call-site conversion completes), so this gap is bounded.
];

/// Files exempt from the guard. These are the legitimate storage-layer
/// or manifest-layer implementations that USE the forbidden APIs to
/// provide the staged primitives or to maintain the system tables
/// (commit graph, manifest).
const ALLOW_LIST_FILES: &[&str] = &[
    "table_store.rs",        // The storage layer itself.
    "storage_layer.rs",      // The trait module.
    "commit_graph.rs",       // Maintains `_graph_commits.lance` system table.
    "graph_coordinator.rs",  // Drives the manifest publisher / branch coordinator.
    "recovery_audit.rs",     // Maintains `_graph_commit_recoveries.lance` (recovery audit trail).
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
            "Forbidden-API guard found {} violation(s) in engine code. \
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
