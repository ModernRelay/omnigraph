//! Shared seeded execution for caller-owned scenarios.

use std::future::Future;

use futures::FutureExt;

use crate::harness::{UNIVERSE_STACK_BYTES, clear_process_slots};
use crate::rand::SplitMix64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UniverseProcess {
    Shared,
    Isolated,
}

/// Configuration creates resources only after seeded execution is installed.
/// Setup must guard partial resources; teardown borrows the complete set.
/// Resources remain owned until all runtime tasks have been dropped.
pub trait UniverseEnvironment: Sync {
    type Resources;
    fn seed(&self) -> u64;
    fn process(&self) -> UniverseProcess;
    fn setup(&self) -> impl Future<Output = Result<Self::Resources, String>>;
    fn teardown(&self, resources: &mut Self::Resources)
    -> impl Future<Output = Result<(), String>>;
}

/// A scenario owns its operations, checks and output; the universe owns execution.
pub trait UniverseScenario<R>: Sync {
    type Output: Send;

    fn run<'a>(
        &'a self,
        resources: &'a mut R,
        workload_seed: u64,
    ) -> impl Future<Output = Self::Output> + 'a;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UniversePhase {
    Runtime,
    Setup,
    Scenario,
}

/// Teardown evidence is retained even when the scenario fails or panics.
pub struct UniverseRun<T> {
    pub phase: UniversePhase,
    pub result: std::thread::Result<Result<T, String>>,
    pub cleanup: std::thread::Result<Result<(), String>>,
}

impl<T: std::fmt::Debug> std::fmt::Debug for UniverseRun<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UniverseRun")
            .field("phase", &self.phase)
            .field("result", &self.result.as_ref().map_err(|_| "panic"))
            .field("cleanup", &self.cleanup.as_ref().map_err(|_| "panic"))
            .finish()
    }
}

#[derive(Debug)]
struct InstalledEnvironment(UniverseProcess);

impl Drop for InstalledEnvironment {
    fn drop(&mut self) {
        omnigraph::dst_clock::uninstall_logical_clock();
        omnigraph::dst_ids::uninstall_seeded_ulids();
        omnigraph::dst_gate::uninstall_gate_hook();
        clear_process_slots();
        if self.0 == UniverseProcess::Isolated {
            crate::entropy::disarm();
        }
    }
}

#[derive(Debug)]
struct ProcessSlots;

impl Drop for ProcessSlots {
    fn drop(&mut self) {
        clear_process_slots();
    }
}

/// Run one complete scenario in a fresh seeded runtime.
///
/// The caller owns process isolation, pool configuration, failpoint registration
/// and the wall-clock deadline. Panics retain their original payloads.
/// A hard-killed worker requires caller-owned containment, not Rust teardown.
pub fn run_universe<E: UniverseEnvironment, S: UniverseScenario<E::Resources>>(
    environment: &E,
    scenario: &S,
) -> UniverseRun<S::Output> {
    let mut seeds = SplitMix64(environment.seed());
    let runtime_seed = seeds.next_u64();
    let ulid_seed = seeds.next_u64();
    let workload_seed = seeds.next_u64();
    let entropy_seed = seeds.next_u64();
    let process = environment.process();

    clear_process_slots();
    let _slots = ProcessSlots;
    if process == UniverseProcess::Shared {
        crate::entropy::arm(entropy_seed);
    }

    std::thread::scope(|scope| {
        let thread = std::thread::Builder::new()
            .name("dst-universe".into())
            .stack_size(UNIVERSE_STACK_BYTES)
            .spawn_scoped(scope, move || {
                let _environment = InstalledEnvironment(process);
                if process == UniverseProcess::Isolated {
                    crate::entropy::arm(entropy_seed);
                }
                rand::rng().reseed().expect("reseed DST thread entropy");
                let mut resources = None;
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .start_paused(true)
                    .rng_seed(tokio::runtime::RngSeed::from_bytes(
                        &runtime_seed.to_le_bytes(),
                    ))
                    .build_local(Default::default())
                    .expect("seeded current-thread runtime");

                let run = runtime.block_on(Box::pin(async {
                    omnigraph::dst_ids::install_seeded_ulids(ulid_seed);
                    omnigraph::dst_clock::install_logical_clock();
                    let setup = std::panic::AssertUnwindSafe(async { environment.setup().await })
                        .catch_unwind()
                        .await;
                    let owned_resources = match setup {
                        Ok(Ok(resources)) => resources,
                        Ok(Err(error)) => {
                            return UniverseRun {
                                phase: UniversePhase::Setup,
                                result: Ok(Err(error)),
                                cleanup: Ok(Ok(())),
                            };
                        }
                        Err(panic) => {
                            return UniverseRun {
                                phase: UniversePhase::Setup,
                                result: Err(panic),
                                cleanup: Ok(Ok(())),
                            };
                        }
                    };
                    let resources = resources.insert(owned_resources);
                    let result = std::panic::AssertUnwindSafe(async {
                        scenario.run(resources, workload_seed).await
                    })
                    .catch_unwind()
                    .await
                    .map(Ok);
                    let cleanup = std::panic::AssertUnwindSafe(async {
                        environment.teardown(resources).await
                    })
                    .catch_unwind()
                    .await;
                    UniverseRun {
                        phase: UniversePhase::Scenario,
                        result,
                        cleanup,
                    }
                }));
                drop(runtime);
                drop(resources);
                run
            });
        match thread {
            Ok(thread) => thread.join().unwrap_or_else(|panic| UniverseRun {
                phase: UniversePhase::Runtime,
                result: Err(panic),
                cleanup: Ok(Err(
                    "runtime exited before resource cleanup could be verified".into(),
                )),
            }),
            Err(error) => UniverseRun {
                phase: UniversePhase::Runtime,
                result: Ok(Err(format!("spawn universe thread: {error}"))),
                cleanup: Ok(Ok(())),
            },
        }
    })
}

#[cfg(test)]
mod tests;
