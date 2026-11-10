//! The interpreter is an oracle only while it shares nothing with what it
//! checks: it reads the compiler's core IR and a JSONL export, never the
//! planner, the engine or Lance.

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
    assert_eq!(deps, ["omnigraph-compiler", "serde_json"]);
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
