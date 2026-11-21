use std::fmt::Display;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::BoxFuture;
use omnigraph::Session;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph::storage::StorageAdapter;
use omnigraph_compiler::{ParamMap, QueryResult};
use serde_json::Value;

use crate::concurrent::{ConcurrentStep, Pass, permutation};
use crate::runner_config::SeamDirective;
use crate::served::ServerTarget;
use crate::{Case, Item, QueryStep, Step, StepFail};

/// The most workers one label of a block declares on a host with a scripted
/// clock (the plain host, the DST runner); a measuring host sets its own.
pub const SCRIPTED_HOST_WORKERS: usize = 8;

/// Refuses a case whose block declares more workers under one label than a
/// scripted host runs.
pub fn admit_workers(case: &Case) -> Result<(), String> {
    admit_workers_within(case, "a scripted host", |_| SCRIPTED_HOST_WORKERS)
}

/// Refuses a case one of whose blocks declares more workers under one label
/// than `bound` gives that block on `host`.
pub fn admit_workers_within(
    case: &Case,
    host: &str,
    bound: impl Fn(&ConcurrentStep) -> usize,
) -> Result<(), String> {
    for item in &case.items {
        if let Item::Step(Step::Concurrent(block)) = item {
            let most = bound(block);
            if block.max_workers() > most {
                return Err(format!(
                    "unsupported_environment: {host} runs at most {most} workers under one label; step {} declares {}",
                    block.ordinal,
                    block.max_workers()
                ));
            }
        }
    }
    Ok(())
}

/// The admission of a host without the DST runner's gate and without the
/// reference engine: no seam, no scripted block, no `same as v1` expect, and
/// no block over the worker bound `bound` gives it on `host`.
pub fn admit_ungated_case(
    case: &Case,
    host: &str,
    bound: impl Fn(&ConcurrentStep) -> usize,
) -> Result<(), String> {
    if case.needs_dst() {
        return Err(
            "unsupported_environment: this executor cannot run seams or scripted concurrent blocks"
                .into(),
        );
    }
    admit_workers_within(case, host, bound)?;
    if case.items.iter().any(|item| {
        let steps = match item {
            Item::Step(step) => std::slice::from_ref(step),
            Item::Loop { steps, .. } => steps,
        };
        steps
            .iter()
            .any(|step| matches!(step, Step::Query(query) if query.same_as_v1))
    }) {
        return Err(
            "unsupported_environment: this executor cannot compare the reference engine".into(),
        );
    }
    Ok(())
}

/// Runner-owned observation and execution facilities around ordinary GQT steps.
pub trait ExecutionHost: Sync {
    type StepGuard: Send;

    fn admit_case(&self, case: &Case) -> Result<(), String> {
        admit_ungated_case(case, "a scripted host", |_| SCRIPTED_HOST_WORKERS)
    }

    fn arm_seams(&self, seams: &[SeamDirective], step: &Step) -> Result<Self::StepGuard, String>;
    fn finish_seams(&self, guard: Self::StepGuard) -> Result<(), String>;
    fn observe(&self, _value: impl FnOnce() -> String) {}
    fn record(&self, _kind: &str, _value: impl FnOnce() -> Value) {}
    fn begin_operation(&self, _value: impl FnOnce() -> Value) {}
    fn observe_query<E: Display>(&self, _result: &Result<QueryResult, E>, _ordered: bool) {}
    fn observe_fault(&self, _error: &OmniError) {}
    fn lifetime_counts(&self) -> Option<[u64; 2]> {
        None
    }
    fn active(&self) -> bool {
        false
    }
    fn observe_snapshot(&self) -> bool {
        false
    }
    fn measure_step_begin(&self, _ordinal: u64, _line: Option<u64>, _kind: &'static str) {}
    fn measure_step_end(&self, _ordinal: u64) {}
    /// Called immediately before polling the engine operation, after preparing its arguments.
    fn operation_started(&self, _ordinal: usize) -> Result<(), String> {
        Ok(())
    }
    /// Called after the engine operation resolves, before validating its result.
    fn operation_finished(&self, _ordinal: usize) -> Result<(), String> {
        Ok(())
    }

    /// Reopens the current store; hosts may replace generation-scoped storage decorators here.
    fn reopen<'a>(
        &'a self,
        uri: &'a str,
        storage: Option<Arc<dyn StorageAdapter>>,
    ) -> BoxFuture<'a, Result<Omnigraph, OmniError>> {
        async move {
            match storage {
                Some(storage) => Omnigraph::open_with_storage(uri, storage).await,
                None => Omnigraph::open(uri).await,
            }
        }
        .boxed()
    }

    fn reference_query<'a>(
        &'a self,
        _session: &'a Session,
        _step: &'a QueryStep,
        _target: ReadTarget,
        _params: &'a ParamMap,
    ) -> BoxFuture<'a, Result<QueryResult, String>> {
        futures::future::ready(Err(
            "unsupported_environment: reference comparison requires the GQT runner".into(),
        ))
        .boxed()
    }

    /// A block as [`run_block_once`] runs it.
    fn concurrent_step<'a>(
        &'a self,
        session: &'a Session,
        _case: &'a Case,
        step: &'a ConcurrentStep,
    ) -> BoxFuture<'a, Result<(), StepFail>>
    where
        Self: Sized,
    {
        run_block_once(self, session, step)
    }

    /// The measured block of a workload against a server. Only a host that
    /// measures runs one, each session over a client of its own.
    fn served_block<'a>(
        &'a self,
        _target: &'a ServerTarget,
        _case: &'a Case,
        step: &'a ConcurrentStep,
    ) -> BoxFuture<'a, Result<(), StepFail>> {
        futures::future::ready(Err(StepFail::new(
            format!("step {} (concurrent)", step.ordinal),
            "unsupported_environment: a measured block against a server requires a measuring host"
                .into(),
        )))
        .boxed()
    }
}

/// A block on a host without the DST runner's gate. A free-running block runs
/// its sessions once each, one after another in declaration order, on the
/// case session; a block with an `order:` line is refused.
pub fn run_block_once<'a>(
    host: &'a impl ExecutionHost,
    session: &'a Session,
    step: &'a ConcurrentStep,
) -> BoxFuture<'a, Result<(), StepFail>> {
    if step.is_free_running() {
        let order = permutation(step.sessions.len(), Pass(0));
        return async move { crate::run_free_running_block(host, session, step, &order).await }
            .boxed();
    }
    futures::future::ready(Err(StepFail::new(
        format!("step {} (concurrent)", step.ordinal),
        "unsupported_environment: a concurrent block with an `order:` line requires the DST runner"
            .into(),
    )))
    .boxed()
}

/// Ordinary execution without runner observations or test-only capabilities.
#[derive(Clone, Copy, Debug, Default)]
pub struct PlainHost;

impl ExecutionHost for PlainHost {
    type StepGuard = ();

    fn arm_seams(&self, seams: &[SeamDirective], _step: &Step) -> Result<(), String> {
        if seams.is_empty() {
            Ok(())
        } else {
            Err("unsupported_environment: seams require the DST runner".into())
        }
    }

    fn finish_seams(&self, _guard: ()) -> Result<(), String> {
        Ok(())
    }
}
