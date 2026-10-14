//! The served GQT conformance: one libtest test per corpus case
//! (`omnigraph-gqt/cases/**/*.gqt`), registered by `datatest-stable` as
//! `gq_logic_tests.rs` does. A case that declares the `omnigraph-server`
//! target runs twice, in-process on a fresh engine and through the served
//! executor against an in-process `omnigraph-server` booted on a graph
//! initialised with the case's schema; the in-process verdict must be green
//! and the served verdict must equal it, so a failure on both sides is a
//! test failure, never an agreement. A case without the target is skipped
//! with a line of its own (`gqt_served_count.rs` totals them and refuses an
//! empty served set): `cargo test -p omnigraph-server --test
//! gqt_served_conformance -- --nocapture` prints one line per case.
//!
//! The server boots with the same `engine` default the corpus seeds from
//! `OMNIGRAPH_GQ_ENGINE`, unauthenticated, with no policy, so a verdict
//! difference is a route or rendering difference, never an engine one.
#![recursion_limit = "512"]

use std::path::Path;

use omnigraph::db::Omnigraph;
use omnigraph::settings::SessionSettings;
use omnigraph_gqt::ServerTarget;
use omnigraph_gqt_core::{Case, Execution};
use omnigraph_server::{AppState, ProcessDefaults, build_app};

fn case(path: &Path) -> datatest_stable::Result<()> {
    if let Some(reason) = omnigraph_gqt::settings_override_refusal(std::env::var_os) {
        return Err(reason.into());
    }
    let stem = omnigraph_gqt::stem_of(path);
    let text = std::fs::read_to_string(path)?;
    let case = omnigraph_gqt_core::parse_case(&stem, &text).map_err(|e| format!("refused: {e}"))?;
    if !case
        .runner
        .environments
        .iter()
        .any(|env| matches!(env.execution, Execution::Server { .. }))
    {
        println!("skip {stem} (no omnigraph-server environment declared)");
        return Ok(());
    }
    let started = std::time::Instant::now();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let served = runtime.block_on(served_verdict(&case));
    let in_process = runtime.block_on(omnigraph_gqt::execute_in_process(&case, path));
    let green = served.is_ok() && in_process.is_ok();
    println!(
        "{} {stem} {:.2}s served={} in-process={}",
        if green { "ok" } else { "FAIL" },
        started.elapsed().as_secs_f64(),
        verdict_word(&served),
        verdict_word(&in_process)
    );
    if let Err(error) = &in_process {
        return Err(format!(
            "in-process verdict failed; the conformance compares the served verdict against a green case\nin-process: failed: {error}\nserved: {}",
            verdict_text(&served)
        )
        .into());
    }
    if let Err(error) = &served {
        return Err(format!(
            "served and in-process verdicts differ\nserved: failed: {error}\nin-process: ok"
        )
        .into());
    }
    Ok(())
}

fn verdict_word(verdict: &Result<(), String>) -> &'static str {
    match verdict {
        Ok(()) => "ok",
        Err(_) => "failed",
    }
}

fn verdict_text(verdict: &Result<(), String>) -> String {
    match verdict {
        Ok(()) => "ok".into(),
        Err(error) => format!("failed: {error}"),
    }
}

/// Boots one server on a graph carrying the case's schema and runs the
/// case through the served executor; the seed goes through `/load`.
async fn served_verdict(case: &Case) -> Result<(), String> {
    let fixture = case
        .fixture
        .as_ref()
        .ok_or("conformance needs a case with schema and seed")?;
    let engine = omnigraph_gqt::engine_from_env()?;
    let dir = tempfile::tempdir().map_err(|e| format!("tempdir failed: {e}"))?;
    let uri = dir
        .path()
        .to_str()
        .ok_or_else(|| "temp path is not utf-8".to_string())?
        .to_string();
    drop(
        Omnigraph::init(&uri, &fixture.schema)
            .await
            .map_err(|e| format!("init failed: {e}"))?,
    );
    let settings = SessionSettings::default()
        .with("engine", engine.as_str())
        .map_err(|e| format!("engine default: {e}"))?;
    let state = AppState::open(&uri)
        .await
        .map_err(|e| format!("server boot failed: {e}"))?
        .with_process_defaults(ProcessDefaults {
            settings,
            ..ProcessDefaults::default()
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("bind failed: {e}"))?;
    let addr = listener
        .local_addr()
        .map_err(|e| format!("local address: {e}"))?;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, build_app(state))
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let target = ServerTarget {
        url: format!("http://{addr}"),
        graph: "default".into(),
        token: None,
    };
    let verdict = omnigraph_gqt::execute_served(case, &target).await;
    let _ = stop.send(());
    let _ = server.await;
    verdict
}

datatest_stable::harness! {
    { test = case, root = "../omnigraph-gqt/cases", pattern = r"^(?:[^./][^/]*/)*[^./][^/]*\.gqt$" },
}
