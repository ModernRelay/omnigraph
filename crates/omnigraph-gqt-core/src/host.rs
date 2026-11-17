use std::fmt::Display;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::BoxFuture;
use omnigraph::Session;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph::storage::StorageAdapter;
use omnigraph_compiler::ir::core::CoreRead;
use omnigraph_compiler::{ParamMap, QueryResult};
use serde_json::Value;

use crate::concurrent::ConcurrentStep;
use crate::runner_config::SeamDirective;
use crate::{Case, ReferenceRefusal, Step, StepFail};

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

    /// Whether this host holds a step's `--- expect plan` against the plan
    /// it ran. A physical-layout variant (`gq_layout_tests.rs`) does not: a
    /// plan claim names the access path of the case's own layout, while the
    /// rows, shapes and refusals hold under every layout.
    fn checks_plans(&self) -> bool {
        true
    }

    /// Whether this host compares every read step with the core reference
    /// interpreter: the GQT runner on the direct engine does; DST, served
    /// and measuring executors run the case without it.
    fn compares_reference(&self) -> bool {
        false
    }

    /// The core reference interpreter's outcome for `read` over `target`
    /// of the case session: its rows, or its refusal. `Err` is a failure to
    /// reach the interpreter at all (the export, the host).
    fn reference_rows<'a>(
        &'a self,
        _session: &'a Session,
        _read: &'a CoreRead,
        _target: &'a ReadTarget,
        _params: &'a ParamMap,
    ) -> BoxFuture<'a, Result<Result<Vec<Value>, ReferenceRefusal>, String>> {
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
