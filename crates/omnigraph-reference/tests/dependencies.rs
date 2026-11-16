//! The interpreter is an oracle only while it shares nothing with what it
//! checks: it reads the compiler's core IR and a JSONL export, never the
//! planner, the engine or Lance. `libm` is the math library `bm25_v1`'s
//! numeric profile names (the GQ 3 RFC, section 4), so the
//! oracle computes the IDF with the same `log1p` the RFC fixes.

use std::path::Path;

#[test]
fn the_interpreter_depends_on_the_compiler_alone() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    let deps: Vec<&str> = manifest
        .split("[dependencies]")
        .nth(1)
        .unwrap()
        .split("\n[")
        .next()
        .unwrap()
        .lines()
        .filter_map(|l| l.split_once('=').map(|(name, _)| name.trim()))
        .filter(|name| !name.is_empty())
        .collect();
    assert_eq!(deps, ["libm", "omnigraph-compiler", "serde_json"]);
    let source = std::fs::read_to_string(root.join("src/lib.rs")).unwrap();
    for forbidden in [
        "omnigraph_planner",
        "omnigraph_engine",
        "omnigraph::",
        "lance",
        "datafusion",
    ] {
        assert!(
            !source.contains(forbidden),
            "the interpreter names `{forbidden}`"
        );
    }
}

/// The GQT runner is the interpreter's one caller: no other crate of the
/// workspace, the engine and the served runner included, depends on it.
#[test]
fn only_the_gqt_runner_depends_on_the_interpreter() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut dependents = Vec::new();
    for entry in std::fs::read_dir(crates).unwrap() {
        let dir = entry.unwrap().path();
        let Ok(manifest) = std::fs::read_to_string(dir.join("Cargo.toml")) else {
            continue;
        };
        let name = dir.file_name().unwrap().to_string_lossy().to_string();
        if name != "omnigraph-reference" && manifest.contains("omnigraph-reference") {
            dependents.push(name);
        }
    }
    dependents.sort();
    assert_eq!(dependents, ["omnigraph-gqt"]);
}
