//! Configured graph identity and the single serving-admission authority.
//!
//! Snapshot readers retain one immutable view. Request capture and transitions
//! use the same synchronous registry boundary under process admission.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use omnigraph::db::{Omnigraph, SchemaContractDigest};
use omnigraph::storage::normalize_root_uri;
use tokio::time::Instant;

use crate::ApiError;
use crate::identity::GraphKey;
use crate::operations::OperationRuntime;
use crate::policy::PolicyEngine;
use crate::queries::QueryRegistry;
use crate::serving::{
    GraphRequest, PreparedTransition, ServingEpoch, ServingTransitionError, ServingView,
    TransitionRecord,
};

/// Fixed engine and effective bindings for one registered graph. Requests use
/// a [`GraphRequest`] to retain these together with their admitted epoch.
pub struct GraphHandle {
    /// Registry key. In Cluster mode `key.tenant_id` is always `None`.
    pub key: GraphKey,
    /// The URI the engine was opened from (`s3://...`, `az://...`, or local path).
    /// Stable for the engine's lifetime; surfaced in responses like
    /// `BranchCreateOutput.uri`.
    pub uri: String,
    /// Engine. Reads/writes go directly through `&self` methods on
    /// `Omnigraph` (no `RwLock` — MR-686 preserved).
    pub engine: Arc<Omnigraph>,
    /// Per-graph Cedar policy. `None` means "no policy gate on engine-layer
    /// `_as` writers"; the HTTP-layer `require_bearer_auth` middleware still
    /// runs regardless.
    pub policy: Option<Arc<PolicyEngine>>,
    /// Per-graph stored-query registry, loaded and validated at
    /// startup. `None` means the operator declared no stored queries for
    /// this graph — `POST /queries/{name}` then 404s. Mirrors the
    /// optional `policy` shape.
    pub queries: Option<Arc<QueryRegistry>>,
}

pub use crate::api::GraphStartupFailure as StartupFailure;

/// A configured graph whose startup failed. Invalid policy is not equivalent
/// to absence of a policy when authorizing status disclosure.
pub struct BlockedGraph {
    pub key: GraphKey,
    pub uri: String,
    pub policy: Option<Arc<PolicyEngine>>,
    pub failure: StartupFailure,
}

/// Captured startup identity and disclosure policy, before an engine exists.
/// Its Arc identity also fences the sole startup completion for this entry.
pub struct LoadingGraph {
    pub key: GraphKey,
    pub uri: String,
    pub policy: Option<Arc<PolicyEngine>>,
}

#[derive(Clone)]
pub enum GraphEntry {
    Loading(Arc<LoadingGraph>),
    Ready(Arc<ServingView>),
    Transitioning(Arc<ServingView>),
    Blocked(Arc<BlockedGraph>),
}

impl GraphEntry {
    /// Startup input; the view grants no request admission on its own.
    pub fn ready(handle: Arc<GraphHandle>) -> Self {
        Self::Ready(Arc::new(ServingView::new(handle, ServingEpoch::INITIAL)))
    }

    pub fn key(&self) -> &GraphKey {
        match self {
            Self::Loading(graph) => &graph.key,
            Self::Ready(view) | Self::Transitioning(view) => &view.key,
            Self::Blocked(graph) => &graph.key,
        }
    }

    pub fn uri(&self) -> &str {
        match self {
            Self::Loading(graph) => &graph.uri,
            Self::Ready(view) | Self::Transitioning(view) => &view.uri,
            Self::Blocked(graph) => &graph.uri,
        }
    }

    pub fn policy(&self) -> Option<&Arc<PolicyEngine>> {
        match self {
            Self::Loading(graph) => graph.policy.as_ref(),
            Self::Ready(view) | Self::Transitioning(view) => view.policy.as_ref(),
            Self::Blocked(graph) => graph.policy.as_ref(),
        }
    }

    fn requires_policy_auth(&self) -> bool {
        self.policy().is_some()
            || matches!(self, Self::Blocked(graph) if graph.failure == StartupFailure::InvalidPolicy)
    }
}

/// One immutable inventory/availability snapshot. Its policy cache is derived
/// on construction and remains valid while an epoch is transitioning.
pub struct RegistrySnapshot {
    pub graphs: HashMap<GraphKey, GraphEntry>,
    pub any_per_graph_policy: bool,
}

impl RegistrySnapshot {
    pub fn new(graphs: HashMap<GraphKey, GraphEntry>) -> Self {
        let any_per_graph_policy = graphs.values().any(GraphEntry::requires_policy_auth);
        Self {
            graphs,
            any_per_graph_policy,
        }
    }
}

impl Default for RegistrySnapshot {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}

/// Introspection only. Ready views do not admit requests or allow a caller to
/// resume a closed epoch.
pub enum RegistryLookup {
    Loading(Arc<LoadingGraph>),
    Ready(Arc<ServingView>),
    Transitioning(Arc<ServingView>),
    Blocked(Arc<BlockedGraph>),
    Gone,
}

/// Coherent request capture, including the effective bindings required to
/// authorize disclosure of an unavailable graph.
pub(crate) enum RegistryCapture {
    Loading(Arc<LoadingGraph>),
    Ready(GraphRequest),
    Transitioning(Arc<ServingView>),
    Blocked(Arc<BlockedGraph>),
    Gone,
}

#[derive(Debug, thiserror::Error)]
pub enum InsertError {
    #[error("graph '{0}' is already registered")]
    DuplicateKey(GraphKey),
    #[error("URI '{0}' is already registered as another graph")]
    DuplicateUri(String),
    #[error("URI '{uri}' is invalid: {message}")]
    InvalidUri { uri: String, message: String },
    #[error("graph '{0}' has a live serving transition and cannot initialize a new registry")]
    Transitioning(GraphKey),
}

struct RegistryState {
    snapshot: Arc<RegistrySnapshot>,
    // One active scheduling record per process registry. This record adds no
    // engine instance or request capacity: the graph entry retains the predecessor
    // after closure, including when the record expires or is abandoned.
    candidate: Option<TransitionCandidate>,
}

struct TransitionCandidate {
    record: Arc<TransitionRecord>,
    closed: bool,
}

pub struct GraphRegistry {
    state: Mutex<RegistryState>,
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl GraphRegistry {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(RegistryState {
                snapshot: Arc::new(RegistrySnapshot::default()),
                candidate: None,
            }),
        }
    }

    pub fn from_handles(handles: Vec<Arc<GraphHandle>>) -> Result<Self, InsertError> {
        Self::from_entries(handles.into_iter().map(GraphEntry::ready).collect())
    }

    pub fn from_entries(entries: Vec<GraphEntry>) -> Result<Self, InsertError> {
        let mut graphs = HashMap::with_capacity(entries.len());
        let mut seen_uris = HashMap::with_capacity(entries.len());
        for entry in entries {
            let entry = canonicalize_entry_uri(entry)?;
            if graphs.contains_key(entry.key()) {
                return Err(InsertError::DuplicateKey(entry.key().clone()));
            }
            if seen_uris.contains_key(entry.uri()) {
                return Err(InsertError::DuplicateUri(entry.uri().to_string()));
            }
            seen_uris.insert(entry.uri().to_string(), entry.key().clone());
            graphs.insert(entry.key().clone(), entry);
        }
        Ok(Self {
            state: Mutex::new(RegistryState {
                snapshot: Arc::new(RegistrySnapshot::new(graphs)),
                candidate: None,
            }),
        })
    }

    pub fn snapshot_ref(&self) -> Arc<RegistrySnapshot> {
        Arc::clone(&locked(&self.state).snapshot)
    }

    pub fn get(&self, key: &GraphKey) -> RegistryLookup {
        match self.snapshot_ref().graphs.get(key) {
            Some(GraphEntry::Loading(graph)) => RegistryLookup::Loading(Arc::clone(graph)),
            Some(GraphEntry::Ready(view)) => RegistryLookup::Ready(Arc::clone(view)),
            Some(GraphEntry::Transitioning(view)) => {
                RegistryLookup::Transitioning(Arc::clone(view))
            }
            Some(GraphEntry::Blocked(graph)) => RegistryLookup::Blocked(Arc::clone(graph)),
            None => RegistryLookup::Gone,
        }
    }

    pub fn list(&self) -> Vec<Arc<ServingView>> {
        self.snapshot_ref()
            .graphs
            .values()
            .filter_map(|entry| match entry {
                GraphEntry::Ready(view) => Some(Arc::clone(view)),
                GraphEntry::Loading(_) | GraphEntry::Transitioning(_) | GraphEntry::Blocked(_) => {
                    None
                }
            })
            .collect()
    }

    pub fn entries(&self) -> Vec<GraphEntry> {
        self.snapshot_ref().graphs.values().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.snapshot_ref().graphs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Process admission is always acquired before the registry lock. Only
    /// AppState's HTTP/MCP boundary passes its actual process runtime here.
    pub(crate) fn capture(
        &self,
        operations: &OperationRuntime,
        key: &GraphKey,
    ) -> Result<RegistryCapture, ApiError> {
        operations.while_open(|| {
            let state = locked(&self.state);
            Ok(match state.snapshot.graphs.get(key) {
                Some(GraphEntry::Loading(graph)) => RegistryCapture::Loading(Arc::clone(graph)),
                Some(GraphEntry::Ready(view)) => RegistryCapture::Ready(view.capture()?),
                Some(GraphEntry::Transitioning(view)) => {
                    RegistryCapture::Transitioning(Arc::clone(view))
                }
                Some(GraphEntry::Blocked(graph)) => RegistryCapture::Blocked(Arc::clone(graph)),
                None => RegistryCapture::Gone,
            })
        })
    }

    /// Install one startup result, or the complete strict-startup batch. The
    /// process boundary fences shutdown; the captured entry fences stale work.
    pub(crate) fn complete_startup(
        &self,
        operations: &OperationRuntime,
        results: Vec<(Arc<LoadingGraph>, GraphEntry)>,
    ) -> Result<(), ApiError> {
        operations.while_open(|| {
            let mut state = locked(&self.state);
            for (pending, result) in &results {
                if !matches!(state.snapshot.graphs.get(&pending.key),
                    Some(GraphEntry::Loading(current)) if Arc::ptr_eq(current, pending))
                    || result.key() != &pending.key
                    || result.uri() != pending.uri
                    || !matches!(result, GraphEntry::Ready(_) | GraphEntry::Blocked(_))
                {
                    return Err(ApiError::internal("stale graph startup completion"));
                }
            }
            let mut graphs = state.snapshot.graphs.clone();
            for (pending, result) in results {
                graphs.insert(pending.key.clone(), result);
            }
            state.snapshot = Arc::new(RegistrySnapshot::new(graphs));
            Ok(())
        })
    }

    /// Reserve the sole candidate before closing healthy graph admission.
    /// There is no query parsing, storage refresh, or engine effect here.
    pub(crate) fn prepare_same_view(
        self: &Arc<Self>,
        operations: &OperationRuntime,
        key: &GraphKey,
        deadline: Instant,
    ) -> Result<PreparedTransition, ServingTransitionError> {
        self.prepare_transition(operations, std::slice::from_ref(key), deadline)
    }

    /// Reserve all existing graphs affected by one durable deployment. An
    /// empty set is valid for creation-only deployment; it still owns the one
    /// candidate so concurrent activation cannot race inventory publication.
    pub(crate) fn prepare_transition(
        self: &Arc<Self>,
        operations: &OperationRuntime,
        keys: &[GraphKey],
        deadline: Instant,
    ) -> Result<PreparedTransition, ServingTransitionError> {
        operations.while_open(|| {
            let mut state = locked(&self.state);
            if Instant::now() >= deadline {
                return Err(ServingTransitionError::DeadlineElapsed);
            }
            // Expiry retires bookkeeping only. Closed graphs and every
            // admitted descendant remain retained by their registry entries.
            if state
                .candidate
                .as_ref()
                .is_some_and(|candidate| Instant::now() >= candidate.record.deadline)
            {
                state.candidate = None;
            }
            if state.candidate.is_some() {
                return Err(ServingTransitionError::Busy);
            }
            let mut seen = std::collections::HashSet::with_capacity(keys.len());
            let mut predecessors = Vec::with_capacity(keys.len());
            for key in keys {
                if !seen.insert(key) {
                    return Err(ServingTransitionError::InvalidBindings);
                }
                let predecessor = match state.snapshot.graphs.get(key) {
                    Some(GraphEntry::Ready(view)) => Arc::clone(view),
                    Some(
                        GraphEntry::Loading(_)
                        | GraphEntry::Transitioning(_)
                        | GraphEntry::Blocked(_),
                    ) => {
                        return Err(ServingTransitionError::Unavailable);
                    }
                    None => return Err(ServingTransitionError::Gone),
                };
                if !predecessor.contract_is_current() {
                    return Err(ServingTransitionError::SchemaChanged);
                }
                predecessor.epoch().successor()?;
                predecessors.push(predecessor);
            }
            let record = Arc::new(TransitionRecord {
                predecessors,
                deadline,
            });
            if Instant::now() >= deadline {
                return Err(ServingTransitionError::DeadlineElapsed);
            }
            state.candidate = Some(TransitionCandidate {
                record: Arc::clone(&record),
                closed: false,
            });
            Ok(PreparedTransition::new(
                Arc::clone(self),
                record,
                operations.clone(),
            ))
        })
    }

    pub(crate) fn close_prepared(
        &self,
        record: &Arc<TransitionRecord>,
    ) -> Result<(), ServingTransitionError> {
        let mut state = locked(&self.state);
        validate_candidate(&state, record)?;
        if state.candidate.as_ref().unwrap().closed {
            return Err(ServingTransitionError::StaleAttempt);
        }
        for predecessor in &record.predecessors {
            if !matches!(state.snapshot.graphs.get(&predecessor.key),
                Some(GraphEntry::Ready(view)) if Arc::ptr_eq(view, predecessor))
            {
                return Err(ServingTransitionError::StaleAttempt);
            }
            if !predecessor.contract_is_current() {
                return Err(ServingTransitionError::SchemaChanged);
            }
        }
        let mut graphs = state.snapshot.graphs.clone();
        for predecessor in &record.predecessors {
            graphs.insert(
                predecessor.key.clone(),
                GraphEntry::Transitioning(Arc::clone(predecessor)),
            );
        }
        let snapshot = Arc::new(RegistrySnapshot::new(graphs));
        if Instant::now() >= record.deadline {
            return Err(ServingTransitionError::DeadlineElapsed);
        }
        state.snapshot = snapshot;
        state.candidate.as_mut().unwrap().closed = true;
        Ok(())
    }

    pub(crate) fn discard_prepared(&self, record: &Arc<TransitionRecord>) {
        let mut state = locked(&self.state);
        if state
            .candidate
            .as_ref()
            .is_some_and(|candidate| Arc::ptr_eq(&candidate.record, record) && !candidate.closed)
        {
            state.candidate = None;
        }
    }

    /// Retire scheduling capacity without reopening or disposing closed views.
    /// This supplies no native settlement evidence or lock-release authority.
    pub(crate) fn discard_transition(&self, record: &Arc<TransitionRecord>) {
        let mut state = locked(&self.state);
        if state
            .candidate
            .as_ref()
            .is_some_and(|candidate| Arc::ptr_eq(&candidate.record, record) && candidate.closed)
        {
            state.candidate = None;
        }
    }

    pub(crate) fn check_drained(
        &self,
        record: &Arc<TransitionRecord>,
    ) -> Result<(), ServingTransitionError> {
        validate_closed(&locked(&self.state), record)
    }

    /// Reopen unchanged admission after the controller proves that no effect
    /// began. Deadline and drainage are deliberately not preconditions here:
    /// every older descendant remains in the successor's shared counter.
    pub(crate) fn abort_before_effects(
        &self,
        record: &Arc<TransitionRecord>,
    ) -> Result<HashMap<GraphKey, ServingEpoch>, ServingTransitionError> {
        let mut state = locked(&self.state);
        if !state
            .candidate
            .as_ref()
            .is_some_and(|candidate| candidate.closed && Arc::ptr_eq(&candidate.record, record))
        {
            return Err(ServingTransitionError::StaleAttempt);
        }
        let mut graphs = state.snapshot.graphs.clone();
        let mut epochs = HashMap::with_capacity(record.predecessors.len());
        for predecessor in &record.predecessors {
            if !matches!(graphs.get(&predecessor.key), Some(GraphEntry::Transitioning(view)) if Arc::ptr_eq(view, predecessor))
            {
                return Err(ServingTransitionError::StaleAttempt);
            }
            if !predecessor.contract_is_current() {
                return Err(ServingTransitionError::SchemaChanged);
            }
            let epoch = predecessor.epoch().successor()?;
            graphs.insert(
                predecessor.key.clone(),
                GraphEntry::Ready(Arc::new(predecessor.successor_retaining_requests(epoch))),
            );
            epochs.insert(predecessor.key.clone(), epoch);
        }
        let snapshot = Arc::new(RegistrySnapshot::new(graphs));
        if record
            .predecessors
            .iter()
            .any(|view| !view.contract_is_current())
        {
            return Err(ServingTransitionError::SchemaChanged);
        }
        state.snapshot = snapshot;
        state.candidate = None;
        Ok(epochs)
    }

    pub(crate) fn resume_same_view(
        &self,
        record: &Arc<TransitionRecord>,
    ) -> Result<ServingEpoch, ServingTransitionError> {
        if record.predecessors.len() != 1 {
            return Err(ServingTransitionError::InvalidBindings);
        }
        Ok(self.resume_same_views(record)?[&record.predecessors[0].key])
    }

    pub(crate) fn resume_same_views(
        &self,
        record: &Arc<TransitionRecord>,
    ) -> Result<HashMap<GraphKey, ServingEpoch>, ServingTransitionError> {
        let views = record
            .predecessors
            .iter()
            .map(|view| Ok(Arc::new(view.successor(view.epoch().successor()?))))
            .collect::<Result<Vec<_>, ServingTransitionError>>()?;
        self.activate(record, views)
    }

    /// Compile the complete replacement set before entering the short final
    /// publication boundary. The exact engine contract is checked both before
    /// compilation and again by activate, so no query registry crosses schemas.
    pub(crate) fn validate_activation(
        &self,
        record: &Arc<TransitionRecord>,
        mut bindings: HashMap<GraphKey, (SchemaContractDigest, QueryRegistry)>,
        additions: Vec<(Arc<GraphHandle>, SchemaContractDigest)>,
    ) -> Result<Vec<Arc<ServingView>>, ServingTransitionError> {
        self.check_drained(record)?;
        if bindings.len() != record.predecessors.len() {
            return Err(ServingTransitionError::InvalidBindings);
        }
        let mut views = Vec::with_capacity(bindings.len() + additions.len());
        for predecessor in &record.predecessors {
            let (contract, queries) = bindings
                .remove(&predecessor.key)
                .ok_or(ServingTransitionError::InvalidBindings)?;
            if contract != predecessor.engine.schema_contract_digest() {
                return Err(ServingTransitionError::SchemaChanged);
            }
            crate::validate_registry_against_catalog(
                &queries,
                &predecessor.engine.catalog(),
                predecessor.key.graph_id.as_str(),
            )
            .map_err(|error| ServingTransitionError::InvalidQueries(error.to_string()))?;
            let handle = Arc::new(GraphHandle {
                key: predecessor.key.clone(),
                uri: predecessor.uri.clone(),
                engine: Arc::clone(&predecessor.engine),
                policy: predecessor.policy.clone(),
                queries: (!queries.is_empty()).then(|| Arc::new(queries)),
            });
            let view = Arc::new(ServingView::new(handle, predecessor.epoch().successor()?));
            if view.schema_contract() != &contract {
                return Err(ServingTransitionError::SchemaChanged);
            }
            views.push(view);
        }
        for (handle, contract) in additions {
            let (_, handle) = canonicalize_handle_uri(handle)
                .map_err(|error| ServingTransitionError::InvalidGraph(error.to_string()))?;
            if contract != handle.engine.schema_contract_digest() {
                return Err(ServingTransitionError::SchemaChanged);
            }
            if let Some(queries) = &handle.queries {
                crate::validate_registry_against_catalog(
                    queries,
                    &handle.engine.catalog(),
                    handle.key.graph_id.as_str(),
                )
                .map_err(|error| ServingTransitionError::InvalidQueries(error.to_string()))?;
            }
            let view = Arc::new(ServingView::new(handle, ServingEpoch::INITIAL));
            if view.schema_contract() != &contract {
                return Err(ServingTransitionError::SchemaChanged);
            }
            views.push(view);
        }
        Ok(views)
    }

    pub(crate) fn activate(
        &self,
        record: &Arc<TransitionRecord>,
        views: Vec<Arc<ServingView>>,
    ) -> Result<HashMap<GraphKey, ServingEpoch>, ServingTransitionError> {
        let mut state = locked(&self.state);
        validate_closed(&state, record)?;
        let mut graphs = state.snapshot.graphs.clone();
        let mut epochs = HashMap::with_capacity(views.len());
        for view in &views {
            if epochs.insert(view.key.clone(), view.epoch()).is_some() {
                return Err(ServingTransitionError::InvalidBindings);
            }
            if let Some(predecessor) = record.predecessors.iter().find(|old| old.key == view.key) {
                if !Arc::ptr_eq(&predecessor.engine, &view.engine)
                    || predecessor.uri != view.uri
                    || view.epoch() != predecessor.epoch().successor()?
                {
                    return Err(ServingTransitionError::InvalidBindings);
                }
            } else if graphs.contains_key(&view.key) {
                return Err(ServingTransitionError::InvalidGraph(
                    InsertError::DuplicateKey(view.key.clone()).to_string(),
                ));
            }
            if graphs
                .values()
                .any(|entry| entry.key() != &view.key && entry.uri() == view.uri)
            {
                return Err(ServingTransitionError::InvalidGraph(
                    InsertError::DuplicateUri(view.uri.clone()).to_string(),
                ));
            }
            graphs.insert(view.key.clone(), GraphEntry::Ready(Arc::clone(view)));
        }
        if record
            .predecessors
            .iter()
            .any(|view| !epochs.contains_key(&view.key))
        {
            return Err(ServingTransitionError::InvalidBindings);
        }
        let snapshot = Arc::new(RegistrySnapshot::new(graphs));
        if views.iter().any(|view| !view.contract_is_current()) {
            return Err(ServingTransitionError::SchemaChanged);
        }
        if Instant::now() >= record.deadline {
            return Err(ServingTransitionError::DeadlineElapsed);
        }
        state.snapshot = snapshot;
        state.candidate = None;
        Ok(epochs)
    }

    /// Test fixture insertion. Production additions use the deployment activation boundary.
    #[cfg(test)]
    pub async fn insert(&self, handle: Arc<GraphHandle>) -> Result<(), InsertError> {
        let (canonical_uri, handle) = canonicalize_handle_uri(handle)?;
        let mut state = locked(&self.state);
        if state.snapshot.graphs.contains_key(&handle.key) {
            return Err(InsertError::DuplicateKey(handle.key.clone()));
        }
        if state
            .snapshot
            .graphs
            .values()
            .any(|entry| entry.uri() == canonical_uri)
        {
            return Err(InsertError::DuplicateUri(canonical_uri));
        }
        let mut graphs = state.snapshot.graphs.clone();
        graphs.insert(handle.key.clone(), GraphEntry::ready(handle));
        state.snapshot = Arc::new(RegistrySnapshot::new(graphs));
        Ok(())
    }
}

fn validate_candidate(
    state: &RegistryState,
    record: &Arc<TransitionRecord>,
) -> Result<(), ServingTransitionError> {
    if !state
        .candidate
        .as_ref()
        .is_some_and(|candidate| Arc::ptr_eq(&candidate.record, record))
    {
        return Err(ServingTransitionError::StaleAttempt);
    }
    if Instant::now() >= record.deadline {
        return Err(ServingTransitionError::DeadlineElapsed);
    }
    Ok(())
}

fn validate_closed(
    state: &RegistryState,
    record: &Arc<TransitionRecord>,
) -> Result<(), ServingTransitionError> {
    validate_candidate(state, record)?;
    if !state.candidate.as_ref().unwrap().closed {
        return Err(ServingTransitionError::StaleAttempt);
    }
    for predecessor in &record.predecessors {
        if !matches!(state.snapshot.graphs.get(&predecessor.key),
            Some(GraphEntry::Transitioning(view)) if Arc::ptr_eq(view, predecessor))
        {
            return Err(ServingTransitionError::StaleAttempt);
        }
        if predecessor.request_count() != 0 {
            return Err(ServingTransitionError::RequestsActive);
        }
    }
    Ok(())
}

fn canonicalize_entry_uri(entry: GraphEntry) -> Result<GraphEntry, InsertError> {
    match entry {
        GraphEntry::Loading(graph) => {
            let uri = normalize_root_uri(&graph.uri).map_err(|err| InsertError::InvalidUri {
                uri: graph.uri.clone(),
                message: err.to_string(),
            })?;
            if uri == graph.uri {
                Ok(GraphEntry::Loading(graph))
            } else {
                Ok(GraphEntry::Loading(Arc::new(LoadingGraph {
                    key: graph.key.clone(),
                    uri,
                    policy: graph.policy.clone(),
                })))
            }
        }
        GraphEntry::Ready(view) => {
            let (_, handle) = canonicalize_handle_uri(Arc::clone(view.handle()))?;
            if Arc::ptr_eq(&handle, view.handle()) {
                Ok(GraphEntry::Ready(view))
            } else {
                Ok(GraphEntry::ready(handle))
            }
        }
        GraphEntry::Transitioning(view) => Err(InsertError::Transitioning(view.key.clone())),
        GraphEntry::Blocked(graph) => {
            let uri = normalize_root_uri(&graph.uri).map_err(|err| InsertError::InvalidUri {
                uri: graph.uri.clone(),
                message: err.to_string(),
            })?;
            if uri == graph.uri {
                Ok(GraphEntry::Blocked(graph))
            } else {
                Ok(GraphEntry::Blocked(Arc::new(BlockedGraph {
                    key: graph.key.clone(),
                    uri,
                    policy: graph.policy.clone(),
                    failure: graph.failure,
                })))
            }
        }
    }
}

fn canonicalize_handle_uri(
    handle: Arc<GraphHandle>,
) -> Result<(String, Arc<GraphHandle>), InsertError> {
    let canonical_uri = normalize_root_uri(&handle.uri).map_err(|err| InsertError::InvalidUri {
        uri: handle.uri.clone(),
        message: err.to_string(),
    })?;
    if canonical_uri == handle.uri {
        return Ok((canonical_uri, handle));
    }
    let canonical_handle = Arc::new(GraphHandle {
        key: handle.key.clone(),
        uri: canonical_uri.clone(),
        engine: Arc::clone(&handle.engine),
        policy: handle.policy.clone(),
        queries: handle.queries.clone(),
    });
    Ok((canonical_uri, canonical_handle))
}

impl Default for GraphRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::TempDir;

    use super::*;
    use crate::graph_id::GraphId;

    const TEST_SCHEMA: &str = "node Person { name: String @key }\n";

    async fn build_handle(graph_id: &str, dir: &Path) -> Arc<GraphHandle> {
        let graph_uri = dir.join(graph_id).to_str().unwrap().to_string();
        let engine = Omnigraph::init(&graph_uri, TEST_SCHEMA)
            .await
            .expect("init engine for registry test");
        Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from(graph_id).unwrap()),
            uri: graph_uri,
            engine: Arc::new(engine),
            policy: None,
            queries: None,
        })
    }

    #[tokio::test]
    async fn new_registry_is_empty() {
        let registry = GraphRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert!(registry.list().is_empty());
    }

    #[tokio::test]
    async fn insert_then_get_returns_ready() {
        let dir = TempDir::new().unwrap();
        let registry = GraphRegistry::new();
        let handle = build_handle("alpha", dir.path()).await;
        registry.insert(Arc::clone(&handle)).await.unwrap();

        match registry.get(&handle.key) {
            RegistryLookup::Ready(found) => {
                assert!(Arc::ptr_eq(found.handle(), &handle));
            }
            RegistryLookup::Gone
            | RegistryLookup::Loading(_)
            | RegistryLookup::Blocked(_)
            | RegistryLookup::Transitioning(_) => panic!("expected Ready"),
        }
    }

    #[tokio::test]
    async fn get_nonexistent_returns_gone() {
        let registry = GraphRegistry::new();
        let key = GraphKey::cluster(GraphId::try_from("ghost").unwrap());
        match registry.get(&key) {
            RegistryLookup::Gone => {}
            RegistryLookup::Ready(_)
            | RegistryLookup::Loading(_)
            | RegistryLookup::Blocked(_)
            | RegistryLookup::Transitioning(_) => panic!("expected Gone"),
        }
    }

    #[tokio::test]
    async fn insert_duplicate_key_returns_error() {
        let dir = TempDir::new().unwrap();
        let registry = GraphRegistry::new();
        let h1 = build_handle("alpha", dir.path()).await;
        // Same key, different URI sub-path (build_handle uses graph_id as subdir).
        let dir2 = TempDir::new().unwrap();
        let h2 = build_handle("alpha", dir2.path()).await;
        registry.insert(h1).await.unwrap();

        match registry.insert(h2).await {
            Err(InsertError::DuplicateKey(_)) => {}
            other => panic!("expected DuplicateKey, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn insert_duplicate_uri_returns_error() {
        let dir = TempDir::new().unwrap();
        // Two handles with the same URI but different keys.
        let shared_uri = dir.path().join("shared").to_str().unwrap().to_string();
        let engine = Omnigraph::init(&shared_uri, TEST_SCHEMA).await.unwrap();
        let engine = Arc::new(engine);
        let h1 = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("alpha").unwrap()),
            uri: shared_uri.clone(),
            engine: Arc::clone(&engine),
            policy: None,
            queries: None,
        });
        let h2 = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("beta").unwrap()),
            uri: shared_uri,
            engine,
            policy: None,
            queries: None,
        });

        let registry = GraphRegistry::new();
        registry.insert(h1).await.unwrap();
        match registry.insert(h2).await {
            Err(InsertError::DuplicateUri(_)) => {}
            other => panic!("expected DuplicateUri, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn list_returns_all_inserted_handles() {
        let dir = TempDir::new().unwrap();
        let registry = GraphRegistry::new();
        for name in ["alpha", "beta", "gamma"] {
            let h = build_handle(name, dir.path()).await;
            registry.insert(h).await.unwrap();
        }
        assert_eq!(registry.len(), 3);
        let mut ids: Vec<_> = registry
            .list()
            .into_iter()
            .map(|h| h.key.graph_id.as_str().to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["alpha", "beta", "gamma"]);
    }

    #[tokio::test]
    async fn from_handles_bulk_init_succeeds() {
        let dir = TempDir::new().unwrap();
        let handles = vec![
            build_handle("alpha", dir.path()).await,
            build_handle("beta", dir.path()).await,
        ];
        let registry = GraphRegistry::from_handles(handles).unwrap();
        assert_eq!(registry.len(), 2);
        assert!(!registry.snapshot_ref().any_per_graph_policy);

        // Availability never removes configured identity. A refused policy
        // must still require authentication even though no engine was opened.
        let mut entries = registry.entries();
        let blocked = Arc::new(BlockedGraph {
            key: GraphKey::cluster(GraphId::try_from("blocked").unwrap()),
            uri: dir.path().join("blocked").to_string_lossy().into_owned(),
            policy: None,
            failure: StartupFailure::InvalidPolicy,
        });
        entries.push(GraphEntry::Blocked(Arc::clone(&blocked)));
        let registry = GraphRegistry::from_entries(entries).unwrap();
        assert_eq!(registry.len(), 3);
        assert_eq!(registry.entries().len(), 3);
        assert_eq!(registry.list().len(), 2);
        assert!(registry.snapshot_ref().any_per_graph_policy);
        match registry.get(&blocked.key) {
            RegistryLookup::Blocked(found) => {
                assert_eq!(found.failure, StartupFailure::InvalidPolicy);
                assert!(found.policy.is_none());
                assert_eq!(found.uri, normalize_root_uri(&blocked.uri).unwrap());
            }
            RegistryLookup::Ready(_)
            | RegistryLookup::Loading(_)
            | RegistryLookup::Gone
            | RegistryLookup::Transitioning(_) => {
                panic!("expected Blocked")
            }
        }

        let mut duplicate_key = registry.entries();
        duplicate_key.push(GraphEntry::Blocked(Arc::clone(&blocked)));
        assert!(matches!(
            GraphRegistry::from_entries(duplicate_key),
            Err(InsertError::DuplicateKey(_)),
        ));
        let mut duplicate_uri = registry.entries();
        duplicate_uri.push(GraphEntry::Blocked(Arc::new(BlockedGraph {
            key: GraphKey::cluster(GraphId::try_from("other").unwrap()),
            uri: blocked.uri.clone(),
            policy: None,
            failure: StartupFailure::OpenFailed,
        })));
        assert!(matches!(
            GraphRegistry::from_entries(duplicate_uri),
            Err(InsertError::DuplicateUri(_)),
        ));

        // A strict startup installs the whole captured batch or none. A stale
        // completion and process shutdown cannot admit either loading graph.
        let handles = registry
            .list()
            .into_iter()
            .map(|view| Arc::clone(view.handle()))
            .collect::<Vec<_>>();
        let loading = handles
            .iter()
            .map(|handle| {
                Arc::new(LoadingGraph {
                    key: handle.key.clone(),
                    uri: handle.uri.clone(),
                    policy: handle.policy.clone(),
                })
            })
            .collect::<Vec<_>>();
        let pending_registry =
            GraphRegistry::from_entries(loading.iter().cloned().map(GraphEntry::Loading).collect())
                .unwrap();
        let operations = OperationRuntime::new();
        assert!(matches!(
            pending_registry
                .capture(&operations, &handles[0].key)
                .unwrap(),
            RegistryCapture::Loading(_)
        ));
        assert!(pending_registry.list().is_empty());
        let foreign = Arc::new(LoadingGraph {
            key: loading[1].key.clone(),
            uri: loading[1].uri.clone(),
            policy: None,
        });
        assert!(
            pending_registry
                .complete_startup(
                    &operations,
                    vec![
                        (
                            Arc::clone(&loading[0]),
                            GraphEntry::ready(Arc::clone(&handles[0]))
                        ),
                        (foreign, GraphEntry::ready(Arc::clone(&handles[1]))),
                    ]
                )
                .is_err()
        );
        assert!(
            pending_registry.list().is_empty(),
            "partial strict install is forbidden"
        );
        let results = || {
            loading
                .iter()
                .cloned()
                .zip(handles.iter().cloned().map(GraphEntry::ready))
                .collect()
        };
        pending_registry
            .complete_startup(&operations, results())
            .unwrap();
        assert_eq!(pending_registry.list().len(), 2);
        assert!(
            pending_registry
                .complete_startup(&operations, results())
                .is_err()
        );
        let stopped_registry =
            GraphRegistry::from_entries(loading.iter().cloned().map(GraphEntry::Loading).collect())
                .unwrap();
        operations.close();
        assert!(
            stopped_registry
                .complete_startup(&operations, results())
                .is_err()
        );
        assert!(stopped_registry.list().is_empty());
    }

    #[tokio::test]
    async fn from_handles_rejects_duplicate_keys() {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();
        let h1 = build_handle("alpha", dir1.path()).await;
        let h2 = build_handle("alpha", dir2.path()).await;
        let err = match GraphRegistry::from_handles(vec![h1, h2]) {
            Ok(_) => panic!("expected DuplicateKey, got Ok"),
            Err(err) => err,
        };
        assert!(
            matches!(err, InsertError::DuplicateKey(_)),
            "expected DuplicateKey, got {err}",
        );
    }

    #[tokio::test]
    async fn from_handles_rejects_duplicate_uris() {
        let dir = TempDir::new().unwrap();
        let shared_uri = dir.path().join("shared").to_str().unwrap().to_string();
        let engine = Arc::new(Omnigraph::init(&shared_uri, TEST_SCHEMA).await.unwrap());
        let h1 = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("alpha").unwrap()),
            uri: shared_uri.clone(),
            engine: Arc::clone(&engine),
            policy: None,
            queries: None,
        });
        let h2 = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("beta").unwrap()),
            uri: shared_uri,
            engine,
            policy: None,
            queries: None,
        });
        let err = match GraphRegistry::from_handles(vec![h1, h2]) {
            Ok(_) => panic!("expected DuplicateUri, got Ok"),
            Err(err) => err,
        };
        assert!(
            matches!(err, InsertError::DuplicateUri(_)),
            "expected DuplicateUri, got {err}",
        );
    }

    /// Race test modeled on `actor_admission_race_does_not_exceed_cap`
    /// at `tests/server.rs:3596+`. Spawn N concurrent inserts with the
    /// same `GraphKey` (each constructing its own `GraphHandle` against
    /// its own tempdir). Exactly one must succeed; the others must
    /// return `DuplicateKey`. No `unwrap` panic: the registry mutex +
    /// in-mutex re-check is the linearization point.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_insert_same_key_exactly_one_succeeds() {
        const N: usize = 8;

        let registry = Arc::new(GraphRegistry::new());
        // Pre-create N handles (each in its own tempdir; same key).
        let mut handles = Vec::with_capacity(N);
        let mut dirs = Vec::with_capacity(N);
        for _ in 0..N {
            let d = TempDir::new().unwrap();
            handles.push(build_handle("contested", d.path()).await);
            dirs.push(d);
        }

        let barrier = Arc::new(tokio::sync::Barrier::new(N));
        let mut tasks = Vec::with_capacity(N);
        for handle in handles {
            let registry = Arc::clone(&registry);
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                registry.insert(handle).await
            }));
        }

        let mut ok_count = 0usize;
        let mut dup_count = 0usize;
        for t in tasks {
            match t.await.unwrap() {
                Ok(()) => ok_count += 1,
                Err(InsertError::DuplicateKey(_)) => dup_count += 1,
                Err(other) => panic!("unexpected error: {other:?}"),
            }
        }
        assert_eq!(ok_count, 1, "exactly one insert must succeed");
        assert_eq!(dup_count, N - 1, "the rest must return DuplicateKey");
        assert_eq!(registry.len(), 1);

        // Drop the dirs at the end (preserves engines until tasks finish).
        drop(dirs);
    }

    /// Concurrent inserts with **distinct** keys all succeed.
    /// Linearizability over the mutex still serializes them.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_insert_distinct_keys_all_succeed() {
        const N: usize = 8;

        let registry = Arc::new(GraphRegistry::new());
        // Pre-create N handles with distinct ids, each in its own tempdir.
        let mut handles = Vec::with_capacity(N);
        let mut dirs = Vec::with_capacity(N);
        for i in 0..N {
            let d = TempDir::new().unwrap();
            handles.push(build_handle(&format!("graph-{i}"), d.path()).await);
            dirs.push(d);
        }

        let barrier = Arc::new(tokio::sync::Barrier::new(N));
        let mut tasks = Vec::with_capacity(N);
        for handle in handles {
            let registry = Arc::clone(&registry);
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                registry.insert(handle).await
            }));
        }
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        assert_eq!(registry.len(), N);
        drop(dirs);
    }

    /// Concurrent reads during a write must always see a consistent
    /// snapshot (no torn state). The read either sees
    /// the old snapshot or the new one — never both, never neither.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_reads_during_inserts_see_consistent_snapshots() {
        let dir = TempDir::new().unwrap();
        let registry = Arc::new(GraphRegistry::new());

        // Spawn a writer that inserts graph-0..graph-9 sequentially.
        const N_WRITES: usize = 10;
        let writer_registry = Arc::clone(&registry);
        let writer_dir = dir.path().to_path_buf();
        let writer = tokio::spawn(async move {
            for i in 0..N_WRITES {
                let h = build_handle(&format!("graph-{i}"), &writer_dir).await;
                writer_registry.insert(h).await.unwrap();
            }
        });

        // Reader loop: repeatedly snapshot the registry until the writer
        // finishes. Every snapshot's len must be in [0, N_WRITES], and
        // for every key g in the snapshot, get(g) must return Ready.
        let reader_registry = Arc::clone(&registry);
        let reader = tokio::spawn(async move {
            for _ in 0..200 {
                let snap = reader_registry.list();
                assert!(snap.len() <= N_WRITES);
                for handle in &snap {
                    match reader_registry.get(&handle.key) {
                        RegistryLookup::Ready(found) => {
                            assert!(Arc::ptr_eq(&found, handle));
                        }
                        RegistryLookup::Gone
                        | RegistryLookup::Loading(_)
                        | RegistryLookup::Blocked(_)
                        | RegistryLookup::Transitioning(_) => panic!(
                            "snapshot listed ready key {} but get() did not return Ready",
                            handle.key.graph_id
                        ),
                    }
                }
                tokio::task::yield_now().await;
            }
        });

        writer.await.unwrap();
        reader.await.unwrap();
        assert_eq!(registry.len(), N_WRITES);
    }

    fn captured(
        registry: &GraphRegistry,
        operations: &OperationRuntime,
        key: &GraphKey,
    ) -> GraphRequest {
        match registry.capture(operations, key).unwrap() {
            RegistryCapture::Ready(request) => request,
            _ => panic!("expected admitted ready graph"),
        }
    }

    fn transition_deadline() -> Instant {
        Instant::now() + std::time::Duration::from_secs(10)
    }

    #[tokio::test]
    async fn same_view_transition_retains_descendants_and_allocates_a_fresh_epoch() {
        let dir = TempDir::new().unwrap();
        let alpha = build_handle("alpha", dir.path()).await;
        let beta = build_handle("beta", dir.path()).await;
        let registry = Arc::new(
            GraphRegistry::from_handles(vec![Arc::clone(&alpha), Arc::clone(&beta)]).unwrap(),
        );
        let operations = OperationRuntime::new();
        let request = captured(&registry, &operations, &alpha.key);
        let descendant = request.clone();
        let old_epoch = request.epoch();
        let old_snapshot = registry.snapshot_ref();
        let prepared = registry
            .prepare_same_view(&operations, &alpha.key, transition_deadline())
            .unwrap();
        // Reserving the one candidate does not close healthy admission.
        drop(captured(&registry, &operations, &alpha.key));
        assert!(matches!(
            registry.prepare_same_view(&operations, &beta.key, transition_deadline()),
            Err(ServingTransitionError::Busy)
        ));
        let transition = prepared.close().unwrap();
        assert!(matches!(
            registry.capture(&operations, &alpha.key).unwrap(),
            RegistryCapture::Transitioning(_)
        ));
        assert!(matches!(
            registry.get(&alpha.key),
            RegistryLookup::Transitioning(_)
        ));
        assert_eq!(registry.entries().len(), 2);
        assert_eq!(registry.list().len(), 1);
        drop(captured(&registry, &operations, &beta.key));
        {
            let waiting = transition.wait_requests();
            tokio::pin!(waiting);
            assert!(futures::poll!(waiting.as_mut()).is_pending());
            drop(request);
            assert!(futures::poll!(waiting.as_mut()).is_pending());
            drop(descendant);
            waiting.await.unwrap();
        }
        let fresh_epoch = transition.resume_same_view().unwrap();
        assert_ne!(fresh_epoch, old_epoch);
        let request = captured(&registry, &operations, &alpha.key);
        assert_eq!(request.epoch(), fresh_epoch);
        assert!(Arc::ptr_eq(&request.engine, &alpha.engine));
        assert_eq!(
            request.schema_contract(),
            &alpha.engine.schema_contract_digest()
        );
        // Retaining an introspection snapshot cannot revive its old epoch.
        match old_snapshot.graphs.get(&alpha.key).unwrap() {
            GraphEntry::Ready(view) => assert_eq!(view.epoch(), old_epoch),
            _ => panic!("the retained predecessor snapshot changed"),
        }
    }

    fn activation_queries(property: &str) -> QueryRegistry {
        QueryRegistry::from_specs(vec![crate::queries::RegistrySpec {
            name: "people".to_owned(),
            source: format!(
                "query people() {{ match {{ $p: Person }} return {{ $p.{property} }} }}"
            ),
            expose: true,
            tool_name: None,
        }])
        .unwrap()
    }

    #[tokio::test]
    async fn batch_activation_drains_all_affected_graphs_and_keeps_the_same_engines() {
        let dir = TempDir::new().unwrap();
        let alpha = build_handle("alpha", dir.path()).await;
        let beta = build_handle("beta", dir.path()).await;
        let peer = build_handle("peer", dir.path()).await;
        let registry = Arc::new(
            GraphRegistry::from_handles(vec![
                Arc::clone(&alpha),
                Arc::clone(&beta),
                Arc::clone(&peer),
            ])
            .unwrap(),
        );
        let operations = OperationRuntime::new();
        let before = registry.snapshot_ref();
        let alpha_request = captured(&registry, &operations, &alpha.key);
        let descendant = alpha_request.clone();
        let beta_request = captured(&registry, &operations, &beta.key);
        let prepared = registry
            .prepare_transition(
                &operations,
                &[alpha.key.clone(), beta.key.clone()],
                transition_deadline(),
            )
            .unwrap();
        let transition = prepared.close().unwrap();
        assert!(matches!(
            transition.engines(),
            Err(ServingTransitionError::RequestsActive)
        ));
        for key in [&alpha.key, &beta.key] {
            assert!(matches!(
                registry.capture(&operations, key).unwrap(),
                RegistryCapture::Transitioning(_)
            ));
        }
        let peer_epoch = captured(&registry, &operations, &peer.key).epoch();
        {
            let waiting = transition.wait_requests();
            tokio::pin!(waiting);
            assert!(futures::poll!(waiting.as_mut()).is_pending());
            drop(alpha_request);
            drop(beta_request);
            assert!(futures::poll!(waiting.as_mut()).is_pending());
            drop(descendant);
            waiting.await.unwrap();
        }
        let engines = transition.engines().unwrap();
        for handle in [&alpha, &beta] {
            assert!(Arc::ptr_eq(&engines[&handle.key], &handle.engine));
            engines[&handle.key]
                .apply_schema("node Person { name: String @key nickname: String? }\n")
                .await
                .unwrap();
        }
        let added = build_handle("added", dir.path()).await;
        let bindings = [&alpha, &beta]
            .into_iter()
            .map(|handle| {
                (
                    handle.key.clone(),
                    (
                        handle.engine.schema_contract_digest(),
                        activation_queries("nickname"),
                    ),
                )
            })
            .collect();
        let epochs = transition
            .activate(
                bindings,
                vec![(Arc::clone(&added), added.engine.schema_contract_digest())],
            )
            .unwrap();
        assert_eq!(epochs.len(), 3);
        for handle in [&alpha, &beta] {
            let request = captured(&registry, &operations, &handle.key);
            assert_eq!(request.epoch(), epochs[&handle.key]);
            assert!(Arc::ptr_eq(&request.engine, &handle.engine));
            assert_eq!(
                request.schema_contract(),
                &handle.engine.schema_contract_digest()
            );
            assert!(
                request
                    .queries
                    .as_ref()
                    .unwrap()
                    .lookup("people")
                    .unwrap()
                    .source
                    .contains("nickname")
            );
            match before.graphs.get(&handle.key).unwrap() {
                GraphEntry::Ready(old) => {
                    assert_ne!(old.epoch(), request.epoch());
                    assert!(old.queries.is_none());
                    assert_ne!(old.schema_contract(), request.schema_contract());
                }
                _ => panic!("predecessor snapshot changed"),
            }
        }
        assert!(Arc::ptr_eq(
            &captured(&registry, &operations, &added.key).engine,
            &added.engine
        ));
        assert_eq!(
            captured(&registry, &operations, &peer.key).epoch(),
            peer_epoch
        );
    }

    #[tokio::test]
    async fn batch_activation_rejects_incomplete_invalid_or_duplicate_bindings_atomically() {
        let dir = TempDir::new().unwrap();
        let alpha = build_handle("alpha", dir.path()).await;
        let beta = build_handle("beta", dir.path()).await;
        for refusal in ["missing", "queries", "contract", "duplicate"] {
            let registry = Arc::new(
                GraphRegistry::from_handles(vec![Arc::clone(&alpha), Arc::clone(&beta)]).unwrap(),
            );
            let operations = OperationRuntime::new();
            let transition = registry
                .prepare_transition(
                    &operations,
                    &[alpha.key.clone(), beta.key.clone()],
                    transition_deadline(),
                )
                .unwrap()
                .close()
                .unwrap();
            transition.wait_requests().await.unwrap();
            let mut bindings: HashMap<_, _> = [&alpha, &beta]
                .into_iter()
                .map(|handle| {
                    (
                        handle.key.clone(),
                        (
                            handle.engine.schema_contract_digest(),
                            activation_queries("name"),
                        ),
                    )
                })
                .collect();
            let additions = match refusal {
                "missing" => {
                    bindings.remove(&beta.key);
                    Vec::new()
                }
                "queries" => {
                    bindings.get_mut(&beta.key).unwrap().1 = activation_queries("nonexistent");
                    Vec::new()
                }
                "contract" => {
                    beta.engine
                        .apply_schema(&format!("// changed\n{TEST_SCHEMA}"))
                        .await
                        .unwrap();
                    Vec::new()
                }
                "duplicate" => vec![(Arc::clone(&alpha), alpha.engine.schema_contract_digest())],
                _ => unreachable!(),
            };
            let error = transition.activate(bindings, additions).unwrap_err();
            match refusal {
                "queries" => assert!(matches!(error, ServingTransitionError::InvalidQueries(_))),
                "contract" => assert_eq!(error, ServingTransitionError::SchemaChanged),
                _ => assert_eq!(error, ServingTransitionError::InvalidBindings),
            }
            assert!(registry.list().is_empty());
            assert_eq!(registry.len(), 2);
            for key in [&alpha.key, &beta.key] {
                assert!(matches!(
                    registry.get(key),
                    RegistryLookup::Transitioning(_)
                ));
            }
        }
    }

    #[tokio::test]
    async fn create_only_activation_and_unchanged_batch_resumption_share_the_candidate_boundary() {
        let dir = TempDir::new().unwrap();
        let alpha = build_handle("alpha", dir.path()).await;
        let beta = build_handle("beta", dir.path()).await;
        let registry = Arc::new(GraphRegistry::from_handles(vec![Arc::clone(&alpha)]).unwrap());
        let operations = OperationRuntime::new();
        let transition = registry
            .prepare_transition(&operations, &[], transition_deadline())
            .unwrap()
            .close()
            .unwrap();
        assert!(matches!(
            registry.prepare_same_view(&operations, &alpha.key, transition_deadline()),
            Err(ServingTransitionError::Busy)
        ));
        transition.wait_requests().await.unwrap();
        assert!(transition.engines().unwrap().is_empty());
        assert_eq!(
            transition
                .activate(
                    HashMap::new(),
                    vec![(Arc::clone(&beta), beta.engine.schema_contract_digest())]
                )
                .unwrap()
                .len(),
            1
        );
        let prior_alpha = captured(&registry, &operations, &alpha.key).epoch();
        let prior_beta = captured(&registry, &operations, &beta.key).epoch();
        let transition = registry
            .prepare_transition(
                &operations,
                &[alpha.key.clone(), beta.key.clone()],
                transition_deadline(),
            )
            .unwrap()
            .close()
            .unwrap();
        transition.wait_requests().await.unwrap();
        let resumed = transition.resume_same_views().unwrap();
        assert_ne!(resumed[&alpha.key], prior_alpha);
        assert_ne!(resumed[&beta.key], prior_beta);
        assert_eq!(registry.list().len(), 2);

        let transition = registry
            .prepare_transition(&operations, &[], transition_deadline())
            .unwrap()
            .close()
            .unwrap();
        assert!(matches!(
            transition.activate(
                HashMap::new(),
                vec![(Arc::clone(&alpha), alpha.engine.schema_contract_digest())]
            ),
            Err(ServingTransitionError::InvalidGraph(_))
        ));
        assert_eq!(registry.list().len(), 2);
    }

    #[tokio::test]
    async fn activation_final_boundary_rechecks_contract_and_process_closure() {
        let dir = TempDir::new().unwrap();
        let alpha = build_handle("alpha", dir.path()).await;
        for refusal in ["contract", "shutdown"] {
            let registry = Arc::new(GraphRegistry::from_handles(vec![Arc::clone(&alpha)]).unwrap());
            let operations = OperationRuntime::new();
            let transition = registry
                .prepare_transition(
                    &operations,
                    std::slice::from_ref(&alpha.key),
                    transition_deadline(),
                )
                .unwrap()
                .close()
                .unwrap();
            transition.wait_requests().await.unwrap();
            let record = Arc::clone(&locked(&registry.state).candidate.as_ref().unwrap().record);
            let replacements = registry
                .validate_activation(
                    &record,
                    HashMap::from([(
                        alpha.key.clone(),
                        (
                            alpha.engine.schema_contract_digest(),
                            activation_queries("name"),
                        ),
                    )]),
                    vec![],
                )
                .unwrap();
            let expected = if refusal == "contract" {
                alpha
                    .engine
                    .apply_schema(&format!("// final-boundary\n{TEST_SCHEMA}"))
                    .await
                    .unwrap();
                ServingTransitionError::SchemaChanged
            } else {
                operations.close();
                ServingTransitionError::ProcessClosed
            };
            assert_eq!(
                operations.while_open(|| registry.activate(&record, replacements)),
                Err(expected)
            );
            assert!(matches!(
                registry.get(&alpha.key),
                RegistryLookup::Transitioning(_)
            ));
            drop(transition);
        }
    }

    #[tokio::test]
    async fn prepared_drop_releases_scheduling_capacity_and_closed_drop_stays_closed() {
        let dir = TempDir::new().unwrap();
        let handle = build_handle("alpha", dir.path()).await;
        let beta = build_handle("beta", dir.path()).await;
        let registry = Arc::new(
            GraphRegistry::from_handles(vec![Arc::clone(&handle), Arc::clone(&beta)]).unwrap(),
        );
        let operations = OperationRuntime::new();
        let prepared = registry
            .prepare_same_view(&operations, &handle.key, transition_deadline())
            .unwrap();
        drop(prepared);
        let transition = registry
            .prepare_same_view(&operations, &handle.key, transition_deadline())
            .unwrap()
            .close()
            .unwrap();
        drop(transition);
        assert!(matches!(
            registry.capture(&operations, &handle.key).unwrap(),
            RegistryCapture::Transitioning(_)
        ));
        assert!(matches!(
            registry.prepare_same_view(&operations, &handle.key, transition_deadline()),
            Err(ServingTransitionError::Unavailable)
        ));
        let other = registry
            .prepare_same_view(&operations, &beta.key, transition_deadline())
            .unwrap()
            .close()
            .unwrap();
        other.wait_requests().await.unwrap();
        other.resume_same_view().unwrap();
        assert!(matches!(
            GraphRegistry::from_entries(registry.entries()),
            Err(InsertError::Transitioning(_))
        ));
    }

    #[tokio::test]
    async fn process_closure_revokes_the_original_transition_runtime() {
        let dir = TempDir::new().unwrap();
        let handle = build_handle("alpha", dir.path()).await;
        for close_before_graph in [true, false] {
            let registry =
                Arc::new(GraphRegistry::from_handles(vec![Arc::clone(&handle)]).unwrap());
            let operations = OperationRuntime::new();
            let request = captured(&registry, &operations, &handle.key);
            let prepared = registry
                .prepare_same_view(&operations, &handle.key, transition_deadline())
                .unwrap();
            if close_before_graph {
                operations.close();
                assert!(matches!(
                    prepared.close(),
                    Err(ServingTransitionError::ProcessClosed)
                ));
            } else {
                let transition = prepared.close().unwrap();
                {
                    let waiting = transition.wait_requests();
                    tokio::pin!(waiting);
                    assert!(futures::poll!(waiting.as_mut()).is_pending());
                    operations.close();
                    assert_eq!(waiting.await, Err(ServingTransitionError::ProcessClosed));
                }
                drop(request);
                assert_eq!(
                    transition.resume_same_view(),
                    Err(ServingTransitionError::ProcessClosed)
                );
                assert!(matches!(
                    registry.get(&handle.key),
                    RegistryLookup::Transitioning(_)
                ));
                continue;
            }
            assert!(registry.capture(&operations, &handle.key).is_err());
            drop(request);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn capture_racing_close_either_owns_predecessor_or_refuses() {
        let dir = TempDir::new().unwrap();
        let handle = build_handle("alpha", dir.path()).await;
        let registry = Arc::new(GraphRegistry::from_handles(vec![Arc::clone(&handle)]).unwrap());
        let operations = OperationRuntime::new();
        let prepared = registry
            .prepare_same_view(&operations, &handle.key, transition_deadline())
            .unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let capture = {
            let registry = Arc::clone(&registry);
            let operations = operations.clone();
            let key = handle.key.clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                registry.capture(&operations, &key).unwrap()
            })
        };
        barrier.wait().await;
        let transition = prepared.close().unwrap();
        match capture.await.unwrap() {
            RegistryCapture::Ready(request) => {
                let waiting = transition.wait_requests();
                tokio::pin!(waiting);
                assert!(futures::poll!(waiting.as_mut()).is_pending());
                drop(request);
                waiting.await.unwrap();
            }
            RegistryCapture::Transitioning(_) => transition.wait_requests().await.unwrap(),
            _ => panic!("close/capture changed graph identity or startup state"),
        }
        transition.resume_same_view().unwrap();
    }

    #[tokio::test]
    async fn final_resume_rechecks_elapsed_deadline_without_a_timer_callback() {
        let dir = TempDir::new().unwrap();
        let alpha = build_handle("alpha", dir.path()).await;
        let beta = build_handle("beta", dir.path()).await;
        let registry = Arc::new(
            GraphRegistry::from_handles(vec![Arc::clone(&alpha), Arc::clone(&beta)]).unwrap(),
        );
        let operations = OperationRuntime::new();
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let transition = registry
            .prepare_same_view(&operations, &alpha.key, deadline)
            .unwrap()
            .close()
            .unwrap();
        transition.wait_requests().await.unwrap();
        // Deliberately block this current-thread runtime: no timer callback
        // or background expiry task can make the final predicate true for us.
        std::thread::sleep(
            deadline.saturating_duration_since(Instant::now())
                + std::time::Duration::from_millis(1),
        );
        assert_eq!(
            transition.resume_same_view(),
            Err(ServingTransitionError::DeadlineElapsed)
        );
        assert!(matches!(
            registry.get(&alpha.key),
            RegistryLookup::Transitioning(_)
        ));
        let next = registry
            .prepare_same_view(&operations, &beta.key, transition_deadline())
            .unwrap()
            .close()
            .unwrap();
        next.wait_requests().await.unwrap();
        next.resume_same_view().unwrap();
        drop(captured(&registry, &operations, &beta.key));

        let gamma = build_handle("gamma", dir.path()).await;
        let registry = Arc::new(
            GraphRegistry::from_handles(vec![Arc::clone(&beta), Arc::clone(&gamma)]).unwrap(),
        );
        let retained = captured(&registry, &operations, &beta.key);
        let deadline = Instant::now() + std::time::Duration::from_millis(20);
        let expired = registry
            .prepare_same_view(&operations, &beta.key, deadline)
            .unwrap()
            .close()
            .unwrap();
        tokio::time::sleep_until(deadline).await;
        let next = registry
            .prepare_same_view(&operations, &gamma.key, transition_deadline())
            .unwrap();
        assert_eq!(
            expired.resume_same_view(),
            Err(ServingTransitionError::StaleAttempt)
        );
        // The obsolete ticket must not discard the new candidate, and neither
        // expiry nor its disposal releases the old graph or its descendants.
        assert!(
            matches!(registry.get(&beta.key), RegistryLookup::Transitioning(view)
            if view.request_count() == 1 && Arc::ptr_eq(&view.handle().engine, &beta.engine))
        );
        let next = next.close().unwrap();
        next.wait_requests().await.unwrap();
        next.resume_same_view().unwrap();
        drop(retained);
    }

    #[tokio::test]
    async fn pre_effect_abort_reopens_admission_and_retains_prior_descendants() {
        let dir = TempDir::new().unwrap();
        let handle = build_handle("alpha", dir.path()).await;
        let registry = Arc::new(GraphRegistry::from_handles(vec![Arc::clone(&handle)]).unwrap());
        let operations = OperationRuntime::new();
        let parked = captured(&registry, &operations, &handle.key);
        let descendant = parked.clone();
        let original_epoch = parked.epoch();
        let deadline = Instant::now() + std::time::Duration::from_millis(20);
        let expired = registry
            .prepare_same_view(&operations, &handle.key, deadline)
            .unwrap()
            .close()
            .unwrap();
        assert_eq!(
            expired.wait_requests().await,
            Err(ServingTransitionError::DeadlineElapsed)
        );
        let epochs = expired.abort_before_effects().unwrap();
        assert_eq!(epochs[&handle.key], original_epoch.successor().unwrap());
        let fresh = captured(&registry, &operations, &handle.key);
        assert_ne!(fresh.epoch(), original_epoch);
        assert!(Arc::ptr_eq(&fresh.engine, &handle.engine));
        let next = registry
            .prepare_same_view(&operations, &handle.key, transition_deadline())
            .unwrap()
            .close()
            .unwrap();
        {
            let waiting = next.wait_requests();
            tokio::pin!(waiting);
            drop(fresh);
            drop(parked);
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), waiting.as_mut())
                    .await
                    .is_err(),
                "the later transition must still await the old epoch's descendant"
            );
            drop(descendant);
            waiting.await.unwrap();
        }
        next.resume_same_view().unwrap();
        let closed = registry
            .prepare_same_view(&operations, &handle.key, transition_deadline())
            .unwrap()
            .close()
            .unwrap();
        operations.close();
        assert_eq!(
            closed.abort_before_effects(),
            Err(ServingTransitionError::ProcessClosed)
        );
        assert!(matches!(
            registry.get(&handle.key),
            RegistryLookup::Transitioning(_)
        ));
    }

    #[tokio::test]
    async fn schema_drift_refuses_at_prepare_close_and_final_resume() {
        let dir = TempDir::new().unwrap();
        let alpha = build_handle("alpha", dir.path()).await;
        let beta = build_handle("beta", dir.path()).await;
        for stage in ["prepare", "close", "resume", "abort"] {
            let registry = Arc::new(
                GraphRegistry::from_handles(vec![Arc::clone(&alpha), Arc::clone(&beta)]).unwrap(),
            );
            let operations = OperationRuntime::new();
            let prepared = (stage != "prepare").then(|| {
                registry
                    .prepare_same_view(&operations, &alpha.key, transition_deadline())
                    .unwrap()
            });
            let (prepared, transition) = if matches!(stage, "resume" | "abort") {
                (None, Some(prepared.unwrap().close().unwrap()))
            } else {
                (prepared, None)
            };
            // Deliberately bypass request admission to model foreign handle
            // drift. Even a source-only change invalidates the exact binding.
            alpha
                .engine
                .apply_schema(&format!("// foreign {stage} revision\n{TEST_SCHEMA}"))
                .await
                .unwrap();
            match stage {
                "prepare" => assert!(matches!(
                    registry.prepare_same_view(&operations, &alpha.key, transition_deadline()),
                    Err(ServingTransitionError::SchemaChanged)
                )),
                "close" => assert!(matches!(
                    prepared.unwrap().close(),
                    Err(ServingTransitionError::SchemaChanged)
                )),
                "abort" => assert_eq!(
                    transition.unwrap().abort_before_effects(),
                    Err(ServingTransitionError::SchemaChanged)
                ),
                "resume" => {
                    let transition = transition.unwrap();
                    transition.wait_requests().await.unwrap();
                    assert_eq!(
                        transition.resume_same_view(),
                        Err(ServingTransitionError::SchemaChanged)
                    );
                }
                _ => unreachable!(),
            }
            if matches!(stage, "resume" | "abort") {
                assert!(matches!(
                    registry.get(&alpha.key),
                    RegistryLookup::Transitioning(_)
                ));
                drop(
                    registry
                        .prepare_same_view(&operations, &beta.key, transition_deadline())
                        .unwrap(),
                );
            } else {
                assert!(matches!(registry.get(&alpha.key), RegistryLookup::Ready(_)));
                // Failure before closure did not take the sole candidate slot.
                drop(
                    registry
                        .prepare_same_view(&operations, &beta.key, transition_deadline())
                        .unwrap(),
                );
            }
        }
    }

    #[tokio::test]
    async fn stale_preparation_and_epoch_exhaustion_cannot_revoke_current_authority() {
        let dir = TempDir::new().unwrap();
        let alpha = build_handle("alpha", dir.path()).await;
        let beta = build_handle("beta", dir.path()).await;
        let registry = Arc::new(
            GraphRegistry::from_handles(vec![Arc::clone(&alpha), Arc::clone(&beta)]).unwrap(),
        );
        let operations = OperationRuntime::new();
        let first = registry
            .prepare_same_view(&operations, &alpha.key, transition_deadline())
            .unwrap();
        let stale = Arc::clone(&locked(&registry.state).candidate.as_ref().unwrap().record);
        drop(first);
        let current = registry
            .prepare_same_view(&operations, &beta.key, transition_deadline())
            .unwrap();
        registry.discard_prepared(&stale);
        assert!(matches!(
            registry.prepare_same_view(&operations, &alpha.key, transition_deadline()),
            Err(ServingTransitionError::Busy)
        ));
        assert_eq!(
            operations.while_open(|| registry.close_prepared(&stale)),
            Err(ServingTransitionError::StaleAttempt)
        );
        assert!(matches!(registry.get(&alpha.key), RegistryLookup::Ready(_)));
        let transition = current.close().unwrap();
        assert_eq!(
            operations.while_open(|| registry.resume_same_view(&stale)),
            Err(ServingTransitionError::StaleAttempt)
        );
        assert_eq!(
            operations.while_open(|| registry.abort_before_effects(&stale)),
            Err(ServingTransitionError::StaleAttempt)
        );
        transition.wait_requests().await.unwrap();
        transition.resume_same_view().unwrap();

        // The exhausted identity is a boundary-value fixture, not a publicly
        // constructible epoch or a test-only activation capability.
        {
            let mut state = locked(&registry.state);
            let mut graphs = state.snapshot.graphs.clone();
            graphs.insert(
                alpha.key.clone(),
                GraphEntry::Ready(Arc::new(ServingView::new(
                    Arc::clone(&alpha),
                    ServingEpoch::MAX,
                ))),
            );
            state.snapshot = Arc::new(RegistrySnapshot::new(graphs));
        }
        assert!(matches!(
            registry.prepare_same_view(&operations, &alpha.key, transition_deadline()),
            Err(ServingTransitionError::EpochExhausted)
        ));
        drop(captured(&registry, &operations, &alpha.key));
        drop(
            registry
                .prepare_same_view(&operations, &beta.key, transition_deadline())
                .unwrap(),
        );
    }
}
