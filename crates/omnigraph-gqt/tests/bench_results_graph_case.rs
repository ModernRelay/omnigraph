//! The results-graph case runs omnigraph-bench's schema and stored queries as
//! their files spell them, so neither can drift from the corpus.

use std::path::{Path, PathBuf};

fn bench_projection() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../omnigraph-bench/projection")
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

fn sections<'a>(case: &'a str, header: &str) -> Vec<&'a str> {
    case.split("\n--- ")
        .filter_map(|part| part.strip_prefix(header))
        .filter(|body| body.starts_with('\n'))
        .map(str::trim)
        .collect()
}

#[test]
fn bench_results_graph_case_pins_the_schema_file_and_every_stored_query() {
    let case =
        read(&Path::new(env!("CARGO_MANIFEST_DIR")).join("cases/bench_results_graph_queries.gqt"));
    let schema = read(&bench_projection().join("schema.pg"));
    assert_eq!(sections(&case, "schema"), vec![schema.trim()]);

    let queries = sections(&case, "query");
    let mut stored = 0;
    for entry in std::fs::read_dir(bench_projection().join("queries")).unwrap() {
        let path = entry.unwrap().path();
        let text = read(&path);
        assert!(
            queries.contains(&text.trim()),
            "{}: the case does not run the stored query as written",
            path.display()
        );
        stored += 1;
    }
    assert_eq!(stored, 10, "one stored query file per documented name");
}
