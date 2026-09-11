use std::cell::Cell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Event {
    Setup,
    Scenario,
    Teardown,
    ResourceDropped,
    TaskDropped,
}

#[derive(Debug)]
struct DropProbe {
    events: Arc<Mutex<Vec<Event>>>,
    event: Event,
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.events.lock().unwrap().push(self.event);
    }
}

#[derive(Debug)]
struct Resources {
    value: Rc<Cell<u64>>,
    started: tokio::time::Instant,
    lifetime: DropProbe,
}

#[derive(Debug, Default)]
struct Environment {
    events: Arc<Mutex<Vec<Event>>>,
    setup_error: bool,
    teardown_error: bool,
}

impl UniverseEnvironment for Environment {
    type Resources = Resources;

    fn seed(&self) -> u64 {
        42
    }

    fn process(&self) -> UniverseProcess {
        UniverseProcess::Shared
    }

    async fn setup(&self) -> Result<Resources, String> {
        self.events.lock().unwrap().push(Event::Setup);
        let resources = Resources {
            value: Rc::new(Cell::new(0)),
            started: tokio::time::Instant::now(),
            lifetime: DropProbe {
                events: self.events.clone(),
                event: Event::ResourceDropped,
            },
        };
        tokio::time::sleep(Duration::from_millis(7)).await;
        if self.setup_error {
            return Err("setup failed".into());
        }
        Ok(resources)
    }

    async fn teardown(&self, resources: &mut Resources) -> Result<(), String> {
        self.events.lock().unwrap().push(Event::Teardown);
        assert_eq!(resources.value.get(), 1);
        tokio::time::sleep(Duration::from_millis(5)).await;
        if self.teardown_error {
            Err("teardown failed".into())
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Pass,
    Error,
    Panic,
    SpawnLocal,
}

#[derive(Debug, PartialEq, Eq)]
struct Observation {
    workload_seed: u64,
    setup_elapsed: Duration,
    scenario_elapsed: Duration,
}

#[derive(Debug, PartialEq, Eq)]
struct ScenarioPanic(u64);

impl UniverseScenario<Resources> for Outcome {
    type Output = Result<Observation, String>;

    async fn run(&self, resources: &mut Resources, workload_seed: u64) -> Self::Output {
        resources
            .lifetime
            .events
            .lock()
            .unwrap()
            .push(Event::Scenario);
        resources.value.set(resources.value.get() + 1);
        let setup_elapsed = resources.started.elapsed();
        tokio::time::sleep(Duration::from_millis(11)).await;
        match self {
            Self::Pass => {}
            Self::Error => return Err("scenario failed".into()),
            Self::Panic => std::panic::panic_any(ScenarioPanic(71)),
            Self::SpawnLocal => {
                let probe = DropProbe {
                    events: resources.lifetime.events.clone(),
                    event: Event::TaskDropped,
                };
                let value = resources.value.clone();
                let (started, ready) = tokio::sync::oneshot::channel();
                tokio::task::spawn_local(async move {
                    let _probe = probe;
                    let _value = value;
                    started.send(()).unwrap();
                    std::future::pending::<()>().await;
                });
                ready.await.unwrap();
            }
        }
        Ok(Observation {
            workload_seed,
            setup_elapsed,
            scenario_elapsed: resources.started.elapsed() - setup_elapsed,
        })
    }
}

#[test]
#[serial_test::serial]
fn partial_setup_failure_drops_resources_without_running_scenario() {
    let environment = Environment {
        setup_error: true,
        ..Default::default()
    };
    let run = run_universe(&environment, &Outcome::Pass);
    assert_eq!(run.phase, UniversePhase::Setup);
    assert_eq!(run.result.unwrap().unwrap_err(), "setup failed");
    run.cleanup.unwrap().unwrap();
    assert_eq!(
        *environment.events.lock().unwrap(),
        [Event::Setup, Event::ResourceDropped]
    );
}

#[test]
#[serial_test::serial]
fn non_send_resources_survive_until_async_teardown() {
    let environment = Environment::default();
    let run = run_universe(&environment, &Outcome::Pass);
    assert_eq!(run.phase, UniversePhase::Scenario);
    run.result.unwrap().unwrap().unwrap();
    run.cleanup.unwrap().unwrap();
    assert_eq!(
        *environment.events.lock().unwrap(),
        [
            Event::Setup,
            Event::Scenario,
            Event::Teardown,
            Event::ResourceDropped,
        ]
    );
}

#[test]
#[serial_test::serial]
fn setup_and_scenario_share_virtual_time_and_preserve_seed_derivation() {
    let environment = Environment::default();
    let first = run_universe(&environment, &Outcome::Pass);
    first.cleanup.unwrap().unwrap();
    let first = first.result.unwrap().unwrap().unwrap();
    let second = run_universe(&environment, &Outcome::Pass);
    second.cleanup.unwrap().unwrap();
    assert_eq!(first, second.result.unwrap().unwrap().unwrap());
    assert_eq!(
        first,
        Observation {
            workload_seed: 5_139_283_748_462_763_858,
            setup_elapsed: Duration::from_millis(7),
            scenario_elapsed: Duration::from_millis(11),
        }
    );
}

#[test]
#[serial_test::serial]
fn scenario_panic_preserves_payload_and_still_tears_down() {
    let environment = Environment::default();
    let run = run_universe(&environment, &Outcome::Panic);
    assert_eq!(run.phase, UniversePhase::Scenario);
    let panic = run.result.unwrap_err();
    assert_eq!(
        panic.downcast_ref::<ScenarioPanic>(),
        Some(&ScenarioPanic(71))
    );
    run.cleanup.unwrap().unwrap();
    assert_eq!(
        *environment.events.lock().unwrap(),
        [
            Event::Setup,
            Event::Scenario,
            Event::Teardown,
            Event::ResourceDropped,
        ]
    );
}

#[test]
#[serial_test::serial]
fn scenario_and_teardown_failures_are_both_retained() {
    let environment = Environment {
        teardown_error: true,
        ..Default::default()
    };
    let run = run_universe(&environment, &Outcome::Error);
    assert_eq!(run.phase, UniversePhase::Scenario);
    assert_eq!(run.result.unwrap().unwrap().unwrap_err(), "scenario failed");
    assert_eq!(run.cleanup.unwrap().unwrap_err(), "teardown failed");
    assert_eq!(
        environment.events.lock().unwrap().last(),
        Some(&Event::ResourceDropped)
    );
}

#[test]
#[serial_test::serial]
fn pending_local_tasks_drop_before_universe_returns() {
    let environment = Environment::default();
    let run = run_universe(&environment, &Outcome::SpawnLocal);
    run.result.unwrap().unwrap().unwrap();
    run.cleanup.unwrap().unwrap();
    assert_eq!(
        *environment.events.lock().unwrap(),
        [
            Event::Setup,
            Event::Scenario,
            Event::Teardown,
            Event::TaskDropped,
            Event::ResourceDropped,
        ]
    );
}
