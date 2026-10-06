//! Immutable serving bindings and logical request lifetimes.
//!
//! A deployment closes its affected epochs, drains admitted request owners,
//! and activates validated bindings on the same engines. Logical counters do
//! not prove native-I/O settlement or permit engine disposal or lock release.

use std::collections::HashMap;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use omnigraph::db::{Omnigraph, SchemaContractDigest};
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::ApiError;
use crate::identity::GraphKey;
use crate::operations::OperationRuntime;
use crate::registry::{BlockedGraph, GraphHandle, GraphRegistry};

/// A non-reusable epoch within one registered graph's lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServingEpoch(u64);

impl ServingEpoch {
    pub(crate) const INITIAL: Self = Self(1);
    #[cfg(test)]
    pub(crate) const MAX: Self = Self(u64::MAX);

    pub(crate) fn successor(self) -> Result<Self, ServingTransitionError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(ServingTransitionError::EpochExhausted)
    }
}

/// Immutable bindings for one epoch. Changed bindings retain the engine owner;
/// a view is not an engine snapshot or a native settlement proof.
pub struct ServingView {
    handle: Arc<GraphHandle>,
    epoch: ServingEpoch,
    schema_contract: SchemaContractDigest,
    requests: Arc<EpochRequests>,
}

impl ServingView {
    pub(crate) fn new(handle: Arc<GraphHandle>, epoch: ServingEpoch) -> Self {
        let schema_contract = handle.engine.schema_contract_digest();
        Self {
            handle,
            epoch,
            schema_contract,
            requests: Arc::new(EpochRequests::default()),
        }
    }

    pub(crate) fn successor(&self, epoch: ServingEpoch) -> Self {
        Self {
            handle: Arc::clone(&self.handle),
            epoch,
            schema_contract: self.schema_contract.clone(),
            requests: Arc::new(EpochRequests::default()),
        }
    }

    /// Unchanged admission can reopen before a drain completes. Old request
    /// descendants still own this engine, so the next transition must include
    /// their count instead of starting an unrelated empty epoch counter.
    pub(crate) fn successor_retaining_requests(&self, epoch: ServingEpoch) -> Self {
        Self {
            handle: Arc::clone(&self.handle),
            epoch,
            schema_contract: self.schema_contract.clone(),
            requests: Arc::clone(&self.requests),
        }
    }

    pub fn epoch(&self) -> ServingEpoch {
        self.epoch
    }

    pub fn schema_contract(&self) -> &SchemaContractDigest {
        &self.schema_contract
    }

    /// Introspection of the retained bindings; this does not admit a request.
    pub fn handle(&self) -> &Arc<GraphHandle> {
        &self.handle
    }

    pub(crate) fn contract_is_current(&self) -> bool {
        self.schema_contract == self.handle.engine.schema_contract_digest()
    }

    pub(crate) fn request_count(&self) -> usize {
        self.requests.active.load(Ordering::Acquire)
    }

    /// Only the registry calls this, under the close/capture boundary.
    pub(crate) fn capture(self: &Arc<Self>) -> Result<GraphRequest, ApiError> {
        self.requests
            .active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .map_err(|_| ApiError::too_many_requests("graph request ownership exhausted"))?;
        Ok(GraphRequest(Arc::new(RequestLifetime {
            view: Arc::clone(self),
            // Fields drop in declaration order: bindings go before returning
            // the last logical ownership count.
            _release: RequestRelease(Arc::clone(&self.requests)),
        })))
    }

    pub(crate) async fn wait_requests(
        &self,
        operations: &OperationRuntime,
        deadline: Instant,
    ) -> Result<(), ServingTransitionError> {
        loop {
            let changed = self.requests.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if operations.snapshot().closed {
                return Err(ServingTransitionError::ProcessClosed);
            }
            if Instant::now() >= deadline {
                return Err(ServingTransitionError::DeadlineElapsed);
            }
            if self.request_count() == 0 {
                return Ok(());
            }
            tokio::select! {
                biased;
                () = operations.wait_closed() => {
                    return Err(ServingTransitionError::ProcessClosed);
                }
                result = tokio::time::timeout_at(deadline, changed) => {
                    result.map_err(|_| ServingTransitionError::DeadlineElapsed)?;
                }
            }
        }
    }
}

impl Deref for ServingView {
    type Target = GraphHandle;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl fmt::Debug for ServingView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServingView")
            .field("key", &self.handle.key)
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct EpochRequests {
    active: AtomicUsize,
    changed: Notify,
}

struct RequestLifetime {
    view: Arc<ServingView>,
    _release: RequestRelease,
}

struct RequestRelease(Arc<EpochRequests>);

impl Drop for RequestRelease {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}

/// One atomically captured view and admitted root. Clones are descendants of
/// the original root and may finish after its epoch closes.
#[derive(Clone)]
pub struct GraphRequest(Arc<RequestLifetime>);

impl GraphRequest {
    pub fn epoch(&self) -> ServingEpoch {
        self.0.view.epoch()
    }

    pub fn schema_contract(&self) -> &SchemaContractDigest {
        self.0.view.schema_contract()
    }
}

impl Deref for GraphRequest {
    type Target = GraphHandle;

    fn deref(&self) -> &Self::Target {
        &self.0.view
    }
}

impl fmt::Debug for GraphRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphRequest")
            .field("view", &self.0.view)
            .finish_non_exhaustive()
    }
}

/// Refusals at the serving transition boundary. No variant grants native
/// settlement, engine disposal, or automatic lock release.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ServingTransitionError {
    #[error("server operation admission is closed")]
    ProcessClosed,
    #[error("another serving transition already owns the candidate slot")]
    Busy,
    #[error("graph is not registered")]
    Gone,
    #[error("graph is not ready for a serving transition")]
    Unavailable,
    #[error("serving transition no longer owns its exact predecessor")]
    StaleAttempt,
    #[error("serving transition deadline elapsed")]
    DeadlineElapsed,
    #[error("admitted graph requests still retain the predecessor epoch")]
    RequestsActive,
    #[error("the engine schema contract differs from its serving bindings")]
    SchemaChanged,
    #[error("serving epoch identity is exhausted")]
    EpochExhausted,
    #[error("activation bindings do not match the reserved graph set")]
    InvalidBindings,
    #[error("stored queries do not match the achieved schema: {0}")]
    InvalidQueries(String),
    #[error("new graph cannot be registered: {0}")]
    InvalidGraph(String),
}

impl From<ApiError> for ServingTransitionError {
    fn from(_: ApiError) -> Self {
        // The only conversion site is OperationRuntime::while_open refusing
        // before its closure. Registry transition errors are already typed.
        Self::ProcessClosed
    }
}

/// One bounded deployment candidate retains its affected predecessor views.
/// Arc identity fences stale tickets without a second persistent identity.
pub(crate) struct TransitionRecord {
    pub(crate) predecessors: Vec<Arc<ServingView>>,
    pub(crate) unavailable: Vec<Arc<BlockedGraph>>,
    pub(crate) recovering: std::collections::HashSet<GraphKey>,
    pub(crate) deadline: Instant,
}

/// Reserved deployment candidate. Dropping before closure releases only this
/// exact reservation; no graph or native effects have begun.
pub struct PreparedTransition {
    registry: Arc<GraphRegistry>,
    record: Arc<TransitionRecord>,
    operations: OperationRuntime,
}

impl PreparedTransition {
    pub(crate) fn new(
        registry: Arc<GraphRegistry>,
        record: Arc<TransitionRecord>,
        operations: OperationRuntime,
    ) -> Self {
        Self {
            registry,
            record,
            operations,
        }
    }

    pub fn close(self) -> Result<GraphTransition, ServingTransitionError> {
        self.operations
            .while_open(|| self.registry.close_prepared(&self.record))?;
        Ok(GraphTransition {
            registry: Arc::clone(&self.registry),
            record: Arc::clone(&self.record),
            operations: self.operations.clone(),
        })
    }
}

impl Drop for PreparedTransition {
    fn drop(&mut self) {
        self.registry.discard_prepared(&self.record);
    }
}

/// Closed predecessors. Dropping this ticket retires its scheduling record;
/// the registry retains closed views and their resources until process shutdown.
/// Successful validated activation opens fresh epochs under the original deadline.
pub struct GraphTransition {
    registry: Arc<GraphRegistry>,
    record: Arc<TransitionRecord>,
    operations: OperationRuntime,
}

impl GraphTransition {
    pub async fn wait_requests(&self) -> Result<(), ServingTransitionError> {
        for predecessor in &self.record.predecessors {
            predecessor
                .wait_requests(&self.operations, self.record.deadline)
                .await?;
        }
        self.operations
            .while_open(|| self.registry.check_drained(&self.record))
    }

    /// The deployment controller uses the same engines after all affected
    /// request owners drain. This does not authorize another writer process.
    pub(crate) fn engines(
        &self,
    ) -> Result<HashMap<GraphKey, Arc<Omnigraph>>, ServingTransitionError> {
        self.operations.while_open(|| {
            self.registry.check_drained(&self.record)?;
            if self.record.predecessors.iter().any(|view| {
                !self.record.recovering.contains(&view.key) && !view.contract_is_current()
            }) {
                return Err(ServingTransitionError::SchemaChanged);
            }
            Ok(self
                .record
                .predecessors
                .iter()
                .map(|view| (view.key.clone(), Arc::clone(&view.engine)))
                .collect())
        })
    }

    /// Publish graph lifecycle and authorization changes in one registry snapshot.
    pub(crate) fn activate_deployment(
        self,
        handles: Vec<(Arc<GraphHandle>, SchemaContractDigest)>,
        unavailable: Vec<Arc<BlockedGraph>>,
        deleted: Vec<(GraphKey, SchemaContractDigest)>,
        server_policy: Option<Arc<crate::PolicyEngine>>,
        deployment: Option<crate::deployment::ActiveDeployment>,
    ) -> Result<HashMap<GraphKey, ServingEpoch>, ServingTransitionError> {
        let views = self
            .registry
            .validate_deployment_activation(&self.record, handles)?;
        self.operations.while_open(|| {
            self.registry.activate_deployment(
                &self.record,
                views,
                unavailable,
                deleted,
                server_policy,
                deployment,
            )
        })
    }

    /// The controller attests no deployment effect began. Reopen only these
    /// exact unchanged predecessors even if drain timed out. Old descendants
    /// remain counted by subsequent transitions. Shutdown and stale tickets
    /// still refuse; this never releases root admission or disposes an engine.
    pub(crate) fn abort_before_effects(
        self,
    ) -> Result<HashMap<GraphKey, ServingEpoch>, ServingTransitionError> {
        self.operations
            .while_open(|| self.registry.abort_before_effects(&self.record))
    }

    /// Resume all original bindings after a proven pre-effect refusal. Any
    /// changed engine contract refuses the entire batch.
    pub(crate) fn resume_same_views(
        self,
    ) -> Result<HashMap<GraphKey, ServingEpoch>, ServingTransitionError> {
        self.operations
            .while_open(|| self.registry.resume_same_views(&self.record))
    }

    /// Install precisely the original bindings in a fresh epoch. This is not
    /// native disposal or lock-release authority.
    pub fn resume_same_view(self) -> Result<ServingEpoch, ServingTransitionError> {
        self.operations
            .while_open(|| self.registry.resume_same_view(&self.record))
    }
}

impl Drop for GraphTransition {
    fn drop(&mut self) {
        self.registry.discard_transition(&self.record);
    }
}
