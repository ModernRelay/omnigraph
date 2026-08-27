const BUILD_SCRIPT: &str = include_str!("../build.rs");

#[test]
fn build_provenance_uses_the_versioned_source_fact_names() {
    assert!(BUILD_SCRIPT.contains("OMNIGRAPH_BENCH_SOURCE_GIT_COMMIT"));
    assert!(BUILD_SCRIPT.contains("OMNIGRAPH_BENCH_SOURCE_WORKTREE_DIRTY"));
    assert!(!BUILD_SCRIPT.contains("OMNIGRAPH_BENCH_SOURCE_COMMIT"));
    assert!(!BUILD_SCRIPT.contains("OMNIGRAPH_BENCH_SOURCE_DIRTY"));
    assert!(BUILD_SCRIPT.contains("OMNIGRAPH_BENCH_DECLARED_ENGINE_FEATURES"));
}

#[test]
fn build_provenance_keeps_directory_and_narrow_git_watches() {
    // Directory watches are intentional: unlike a one-time file inventory,
    // their metadata changes when a new untracked source file is created.
    assert!(BUILD_SCRIPT.contains("repository.join(\"crates\")"));
    assert!(BUILD_SCRIPT.contains("repository.join(\"benchmarks\")"));
    assert!(BUILD_SCRIPT.contains("git_dir.join(\"HEAD\")"));
    assert!(BUILD_SCRIPT.contains("git_dir.join(\"index\")"));
    assert!(BUILD_SCRIPT.contains("common_dir.join(\"packed-refs\")"));
    assert!(!BUILD_SCRIPT.contains("repository.join(\".git\").display()"));
    assert!(BUILD_SCRIPT.contains("[\"ls-files\", \"--others\", \"--exclude-standard\", \"-z\"]"));
    assert!(BUILD_SCRIPT.contains("[\"ls-files\", \"-v\", \"-z\", \"--\"]"));
    assert!(!BUILD_SCRIPT.contains("--error-unmatch"));
}

#[test]
fn build_provenance_parsing_and_subprocesses_fail_closed() {
    assert!(BUILD_SCRIPT.contains("toml::from_str::<toml::Value>(source)"));
    assert!(!BUILD_SCRIPT.contains("split_once('=')"));
    assert!(BUILD_SCRIPT.contains("receiver.recv_timeout(PROBE_REAP_DEADLINE)"));
    assert!(!BUILD_SCRIPT.contains("reader.join()"));
    assert!(BUILD_SCRIPT.contains("cargo:rerun-if-env-changed=RUSTC"));
    assert!(BUILD_SCRIPT.contains("cargo:rerun-if-env-changed=CARGO_ENCODED_RUSTFLAGS"));
    assert!(BUILD_SCRIPT.contains("const GIT_EXECUTABLE: &str = \"/usr/bin/git\""));
    assert!(BUILD_SCRIPT.contains(".arg(\"--git-dir\")"));
    assert!(BUILD_SCRIPT.contains(".arg(\"--work-tree\")"));
    assert!(BUILD_SCRIPT.contains("core.fsmonitor=false"));
    assert!(BUILD_SCRIPT.contains("GIT_NO_REPLACE_OBJECTS"));
    assert!(BUILD_SCRIPT.contains("canonical_index_inventory(&index)"));
    assert!(BUILD_SCRIPT.contains("[\"hash-object\", \"--no-filters\", \"--\"]"));
    assert!(BUILD_SCRIPT.contains("raw_source_matches_index(repository)"));
    assert!(BUILD_SCRIPT.contains("read_declared_engine_features"));
}
