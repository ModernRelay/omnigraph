//! Embeds the SUT identity at build time: the git commit (plus a dirty flag)
//! and the cargo `OPT_LEVEL`. A run record's `source_commit` must describe
//! the binary, and the binary can outlive the tree state it was built from —
//! run-time `git rev-parse` describes the tree at run time, so it serves only
//! as a fallback, labeled "unverified" (see `source_commit` in `main.rs`).
//!
//! Rerun window: the rerun-if-changed lines below REPLACE cargo's default
//! rerun-on-any-source-change, so an unstaged edit to a tracked file (the
//! standard dev loop: edit, build, run) re-links the binary with the OLD
//! embedded commit and dirty flag — neither `.git/HEAD` nor the index moved.
//! `source_commit` closes the window at run time: it re-runs the same two git
//! commands and appends "-stale-build" when they disagree with the embedded
//! values. The watched paths are resolved via `git rev-parse --git-path`: in
//! a git worktree `.git` is a FILE pointing elsewhere, so the literal
//! `../../.git/HEAD` would not exist and cargo would rerun this script on
//! every build; the literals remain only as the no-git fallback.

use std::process::Command;

fn main() {
    // Re-run when HEAD moves or the working tree's staged state changes.
    // `--git-path` yields the real location (worktree-safe), relative to the
    // package directory this script runs in — the same base cargo resolves
    // rerun-if-changed paths against.
    let head =
        git(&["rev-parse", "--git-path", "HEAD"]).unwrap_or_else(|| "../../.git/HEAD".to_string());
    let index = git(&["rev-parse", "--git-path", "index"])
        .unwrap_or_else(|| "../../.git/index".to_string());
    println!("cargo:rerun-if-changed={head}");
    println!("cargo:rerun-if-changed={index}");
    if let Some(commit) = git(&["rev-parse", "HEAD"]) {
        println!("cargo:rustc-env=OMNIGRAPH_BENCH_GIT_COMMIT={commit}");
        // `-uno`: tracked modifications only — an untracked `.idea/` must not
        // mark every dev build dirty. An unreadable status is treated as
        // dirty: the honest direction. `source_commit` in `main.rs` runs the
        // same flags so the run-time staleness check compares like with like.
        let dirty = git(&["status", "--porcelain", "-uno"]).is_none_or(|s| !s.is_empty());
        println!(
            "cargo:rustc-env=OMNIGRAPH_BENCH_GIT_DIRTY={}",
            if dirty { "1" } else { "0" }
        );
    }
    if let Ok(opt_level) = std::env::var("OPT_LEVEL") {
        println!("cargo:rustc-env=OMNIGRAPH_BENCH_OPT_LEVEL={opt_level}");
    }
}

/// Trimmed stdout of a git command run in the package directory, on success.
fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8(out.stdout)
        .ok()
        .map(|s| s.trim().to_string())
}
