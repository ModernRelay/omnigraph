//! Immutable serving bindings and logical request lifetimes.
//!
//! A closed epoch can resume only its unchanged view in a fresh epoch. These
//! logical counters prove neither native-I/O settlement nor engine reuse.

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use omnigraph::db::SchemaContractDigest;
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::ApiError;
use crate::operations::OperationRuntime;
use crate::registry::{GraphHandle, GraphRegistry};

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

/// Immutable bindings for one epoch. An engine remains shared across unchanged
/// resumption; a view is not an engine snapshot or a native settlement proof.
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

/// Actual refusal cases of unchanged-view transitions. No variant grants
/// schema publication, native settlement or automatic lock release.
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
}

impl From<ApiError> for ServingTransitionError {
    fn from(_: ApiError) -> Self {
        // The only conversion site is OperationRuntime::while_open refusing
        // before its closure. Registry transition errors are already typed.
        Self::ProcessClosed
    }
}

/// The one bounded candidate contains only retained predecessor bindings.
/// Arc identity fences stale tickets without a second persistent identity.
pub(crate) struct TransitionRecord {
    pub(crate) predecessor: Arc<ServingView>,
    pub(crate) deadline: Instant,
}

/// Reserved same-view candidate. Dropping before closure releases only this
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

/// Closed predecessor. Dropping this ticket cannot reopen its epoch or free
/// its candidate slot. The registry retains its exact view until resumption or
/// process shutdown. The original absolute deadline never moves.
pub struct GraphTransition {
    registry: Arc<GraphRegistry>,
    record: Arc<TransitionRecord>,
    operations: OperationRuntime,
}

impl GraphTransition {
    pub async fn wait_requests(&self) -> Result<(), ServingTransitionError> {
        self.record
            .predecessor
            .wait_requests(&self.operations, self.record.deadline)
            .await
    }

    /// Install precisely the original bindings in a fresh epoch. This is not
    /// schema/query replacement, native disposal, or lock-release authority.
    pub fn resume_same_view(self) -> Result<ServingEpoch, ServingTransitionError> {
        self.operations
            .while_open(|| self.registry.resume_same_view(&self.record))
    }
}
