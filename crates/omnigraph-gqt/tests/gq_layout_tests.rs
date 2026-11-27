//! Law 6b: physical layout never changes answers. One libtest test per
//! corpus case (`cases/**/*.gqt`), registered by `datatest-stable` as
//! `gq_logic_tests.rs` does: a case that declares the direct engine on the
//! local filesystem is re-run with its seed stored in each physical layout
//! (`omnigraph_gqt::Layout`): `a` no index, `b` every declared index built,
//! `c` every index built and then half the keyed rows written again into an
//! uncovered tail, `d` several fragments with deleted and re-inserted rows,
//! then indexed. Every layout must keep the case's own expected rows,
//! shapes and refusals, and its reference comparison; a step's `--- expect
//! plan` is not held, since it names the case's own access path.
//!
//! `OMNIGRAPH_GQT_LAYOUTS` selects the layouts, `a,b` when unset (the
//! gate); the nightly job sets `a,b,c,d`. A case whose rows depend on its
//! own layout opts out with `# layout: skipped <letters>: <reason>`, which
//! is printed with its result. A case without the direct engine prints a
//! skip line.
#![recursion_limit = "512"]

use std::path::Path;

use omnigraph_gqt::Layout;
use omnigraph_gqt_core::Execution;
use omnigraph_gqt_core::runner_config::Storage;

fn layouts() -> Result<Vec<Layout>, String> {
    let selected = std::env::var("OMNIGRAPH_GQT_LAYOUTS").unwrap_or_else(|_| "a,b".into());
    selected
        .split(',')
        .map(|letter| {
            let mut chars = letter.trim().chars();
            match (chars.next(), chars.next()) {
                (Some(c), None) => Layout::of_letter(c),
                _ => None,
            }
            .ok_or_else(|| format!("OMNIGRAPH_GQT_LAYOUTS names `{letter}`, not one of a, b, c, d"))
        })
        .collect()
}

fn case(path: &Path) -> datatest_stable::Result<()> {
    if let Some(reason) = omnigraph_gqt::settings_override_refusal(std::env::var_os) {
        return Err(reason.into());
    }
    let stem = omnigraph_gqt::stem_of(path);
    let text = std::fs::read_to_string(path)?;
    let mut case =
        omnigraph_gqt_core::parse_case(&stem, &text).map_err(|e| format!("refused: {e}"))?;
    // A `load csv` step reads its `--- csv` sections from files, as the
    // corpus run writes them.
    let csv_dir = tempfile::tempdir()?;
    case.materialize_csv_fixtures(csv_dir.path())?;
    let direct = case.runner.environments.iter().any(|env| {
        matches!(
            env.execution,
            Execution::Engine {
                storage: Storage::LocalFilesystem
            }
        )
    });
    if !direct || case.fixture.is_none() {
        println!("skip {stem} (no direct-engine environment with a fixture)");
        return Ok(());
    }
    let skips = case.layout_skips.clone();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let mut failures = Vec::new();
    for layout in layouts()? {
        let started = std::time::Instant::now();
        if let Some((letters, reason)) = &skips
            && letters.contains(&layout.letter())
        {
            println!("skip {stem} layout {}: {reason}", layout.letter());
            continue;
        }
        let verdict = runtime.block_on(omnigraph_gqt::execute_with_layout(&case, path, layout));
        println!(
            "{} {stem} layout {} {:.2}s",
            if verdict.is_ok() { "ok" } else { "FAIL" },
            layout.letter(),
            started.elapsed().as_secs_f64()
        );
        if let Err(error) = verdict {
            failures.push(format!("layout {}: {error}", layout.letter()));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("\n").into())
    }
}

datatest_stable::harness! {
    { test = case, root = "cases", pattern = r"^(?:[^./][^/]*/)*[^./][^/]*\.gqt$" },
}
