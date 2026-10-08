use std::fmt::Display;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::BoxFuture;
use omnigraph::Session;
use omnigraph::db::Omnigraph;
use omnigraph::error::OmniError;
use omnigraph::storage::StorageAdapter;
use omnigraph_compiler::{ParamMap, QueryResult};
use serde_json::Value;

use crate::concurrent::ConcurrentStep;
use crate::runner_config::SeamDirective;
use crate::{Case, Item, QueryStep, Step, StepFail};

/// Runner-owned observation and execution facilities around ordinary GQT steps.
pub trait ExecutionHost: Sync {
    type StepGuard: Send;

    fn admit_case(&self, case: &Case) -> Result<(), String> {
        if case.needs_dst() {
            return Err(
                "unsupported_environment: this executor cannot run seams or concurrent blocks"
                    .into(),
            );
        }
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
        _params: &'a ParamMap,
    ) -> BoxFuture<'a, Result<QueryResult, String>> {
        futures::future::ready(Err(
            "unsupported_environment: reference comparison requires the GQT runner".into(),
        ))
        .boxed()
    }

    fn concurrent_step<'a>(
        &'a self,
        _session: &'a Session,
        _case: &'a Case,
        step: &'a ConcurrentStep,
    ) -> BoxFuture<'a, Result<(), StepFail>> {
        futures::future::ready(Err(StepFail::new(
            format!("step {} (concurrent)", step.ordinal),
            "unsupported_environment: concurrent blocks require the DST runner".into(),
        )))
        .boxed()
    }
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
