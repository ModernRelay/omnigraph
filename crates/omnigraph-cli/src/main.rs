#![recursion_limit = "256"]

use std::io::Write;

use color_eyre::eyre::Result;
use tokio::runtime::Runtime;

fn main() -> Result<std::process::ExitCode> {
    let runtime = Runtime::new()?;
    // A refusal explains its fix without environment or source-location footers.
    color_eyre::config::HookBuilder::default()
        .display_env_section(false)
        .display_location_section(false)
        .install()?;
    let code = match runtime.block_on(omnigraph_cli::app::run_from(std::env::args_os())) {
        Ok(completion) => completion.code(),
        Err(error) => {
            // Reported as returning it from `main` would, but with the exit
            // its outcome chose rather than 1, which says nothing changed.
            let _ = writeln!(std::io::stderr(), "Error: {error:?}");
            omnigraph_cli::app::exit_code(&error)
        }
    };
    finish_runtime(runtime, code);
    Ok(std::process::ExitCode::from(code as u8))
}

fn finish_runtime(runtime: Runtime, exit_code: i32) {
    if exit_code == 0 {
        // Preserve normal shutdown after success, including blocking I/O drain.
        drop(runtime);
    } else {
        // This refusal or timeout has already been reported. A timed-out DNS
        // lookup may still occupy the blocking pool; waiting for it here would
        // extend the caller's deadline indefinitely. Only the process boundary
        // skips that wait, as process::exit did before application extraction.
        runtime.shutdown_background();
    }
}

#[cfg(test)]
mod tests {
    use super::finish_runtime;
    use std::sync::mpsc;
    use std::time::Duration;
    use tokio::runtime::Runtime;

    #[test]
    fn reported_failure_does_not_wait_for_a_blocking_task() {
        // A timed-out DNS lookup can still be running in Tokio's blocking pool.
        // Hold that work until after the process boundary returns: channels
        // prove ordering; the deadline only prevents a regression hanging CI.
        for code in [1, 2, 4, 5, 75] {
            let (release, released) = mpsc::channel();
            let (settled, settlement) = mpsc::channel();
            let (finished, completion) = mpsc::channel();
            let thread = std::thread::spawn(move || {
                let runtime = Runtime::new().unwrap();
                runtime.block_on(async move {
                    let (started, start) = tokio::sync::oneshot::channel();
                    tokio::task::spawn_blocking(move || {
                        started.send(()).unwrap();
                        let _ = released.recv();
                        settled.send(()).unwrap();
                    });
                    start.await.unwrap();
                });
                finish_runtime(runtime, code);
                finished.send(code).unwrap();
            });
            let result = completion.recv_timeout(Duration::from_secs(5));
            // Always release and join before asserting, including on failure.
            release.send(()).unwrap();
            settlement.recv_timeout(Duration::from_secs(5)).unwrap();
            thread.join().unwrap();
            assert_eq!(result.unwrap(), code);
        }
    }
}
