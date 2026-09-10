//! One libtest test per `cases/*.gqt`, registered at run time by
//! `datatest-stable` (`harness = false` in `Cargo.toml`), so
//! `cargo test -p omnigraph-gqt <substr>` runs the matching cases,
//! `-- --list` names them all, and `--test-threads` sets the concurrency.
//! The runner, format, and self-tests live in `src/lib.rs`; the corpus
//! layout check (no foreign entries, never empty) is the `corpus_layout`
//! unit test there.
// The `Send`/`Sync` walk of the spawned case future (engine query futures
// inside) overflows the default recursion limit; `src/lib.rs` raises it for
// the same reason.
#![recursion_limit = "512"]

use std::path::Path;

fn case(path: &Path) -> datatest_stable::Result<()> {
    if let Some(reason) = omnigraph_gqt::traversal_override_refusal(
        std::env::var_os("OMNIGRAPH_TRAVERSAL_MODE").as_deref(),
    ) {
        return Err(reason.into());
    }
    let outcome = omnigraph_gqt::run_corpus_case(
        path,
        Path::new(env!("CARGO_BIN_EXE_omnigraph-gqt")),
        omnigraph_gqt::bless_from_env(),
    );
    println!(
        "{} {} {:.2}s",
        if outcome.result.is_ok() { "ok" } else { "FAIL" },
        outcome.stem,
        outcome.elapsed.as_secs_f64()
    );
    outcome.result.map_err(Into::into)
}

datatest_stable::harness! {
    { test = case, root = "cases", pattern = r"^[^./][^/]*\.gqt$" },
}
