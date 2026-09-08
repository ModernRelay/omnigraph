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
use std::sync::OnceLock;

use omnigraph_gqt::{
    bless_from_env, case_budget_from_env, run_case_bounded, traversal_override_refusal,
};
use tokio::runtime::Runtime;

/// Worker stack for the case runtime: the engine's query futures overflow
/// the 2 MiB default (CI sets `RUST_MIN_STACK` to this same value for the
/// engine test jobs; pinning it here keeps this target's cases independent
/// of it; the crate's unit tests run on libtest's own threads).
const WORKER_STACK_BYTES: usize = 16 * 1024 * 1024;

/// One multi-thread runtime shared by every case; libtest calls `case`
/// from its own worker threads, each call spawns its case onto the runtime
/// and blocks on the join handle.
fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_stack_size(WORKER_STACK_BYTES)
            .build()
            .expect("tokio runtime")
    })
}

fn case(path: &Path) -> datatest_stable::Result<()> {
    if let Some(reason) =
        traversal_override_refusal(std::env::var_os("OMNIGRAPH_TRAVERSAL_MODE").as_deref())
    {
        return Err(reason.into());
    }
    let rt = runtime();
    let task = rt.spawn(run_case_bounded(
        path.to_path_buf(),
        case_budget_from_env(),
        bless_from_env(),
    ));
    // `run_case_bounded` catches the case's own panics; the task around it
    // holds nothing that can panic.
    let outcome = rt.block_on(task).expect("a case task never panics");
    let secs = outcome.elapsed.as_secs_f64();
    match outcome.result {
        Ok(()) => {
            println!("ok {} {secs:.2}s", outcome.stem);
            Ok(())
        }
        Err(detail) => {
            // The detail (row diff, refusal, panic text, budget overrun) goes
            // to stdout under the FAIL line: the harness renders a returned
            // error Debug-escaped on one line, which hides a multi-line diff.
            println!(
                "FAIL {} {secs:.2}s\n  {}",
                outcome.stem,
                detail.replace('\n', "\n  ")
            );
            Err(format!("{}: see its FAIL block above", outcome.stem).into())
        }
    }
}

datatest_stable::harness! {
    { test = case, root = "cases", pattern = r"^[^./][^/]*\.gqt$" },
}
