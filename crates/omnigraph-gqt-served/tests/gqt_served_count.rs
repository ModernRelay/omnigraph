//! The served conformance set is visible and never empty: every corpus case
//! is counted by what `gqt_served_conformance` does with it (served, skipped
//! for lack of the `omnigraph-server` target, refused to parse), the totals
//! are printed, and an empty served set fails, so a lost opt-in sweep or a
//! filter that stops matching cannot leave the `served` CI cell green on
//! skips alone.

use std::path::Path;

use omnigraph_gqt_core::Execution;

#[test]
fn corpus_declares_served_cases() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../omnigraph-gqt/cases");
    let (paths, refusals) = omnigraph_gqt::list_cases(&root);
    assert!(
        refusals.is_empty(),
        "corpus discovery refused:\n{}",
        refusals.join("\n")
    );
    let mut served = 0usize;
    let mut skipped = 0usize;
    let mut unparsed = Vec::new();
    for path in &paths {
        let text = std::fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        match omnigraph_gqt_core::parse_case(&omnigraph_gqt::stem_of(path), &text) {
            Ok(case)
                if case
                    .runner
                    .environments
                    .iter()
                    .any(|env| matches!(env.execution, Execution::Server { .. })) =>
            {
                served += 1;
            }
            Ok(_) => skipped += 1,
            Err(error) => unparsed.push(format!("{}: {error}", path.display())),
        }
    }
    println!(
        "served conformance set: {served} served, {skipped} skipped (no omnigraph-server environment), {} refused to parse, {} cases",
        unparsed.len(),
        paths.len()
    );
    assert!(
        unparsed.is_empty(),
        "cases refused to parse:\n{}",
        unparsed.join("\n")
    );
    assert!(
        served > 0,
        "no corpus case declares the omnigraph-server target; the served conformance would run nothing"
    );
}
