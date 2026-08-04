#![allow(dead_code)] // Hidden F4 engine seam; production transport activation is intentionally deferred.

//! Hidden RFC-026 F4 NDJSON request driver.
//!
//! This is deliberately transport-neutral and crate-private. It composes the
//! bounded framing/result primitives with the existing B2 authority and
//! durability corridor; it does not define an HTTP, CLI, SDK, or OpenAPI
//! surface.

use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use futures::{Stream, StreamExt};
use lance_index::mem_wal::ShardId;
use omnigraph_compiler::catalog::Catalog;

use crate::db::manifest::TableIdentity;
use crate::db::manifest::stream_token::{
    AdmissionRequest, PayloadDigest, PayloadDigestInput, StreamRowOrigin, StreamTokenAuthorityRow,
    StreamTokenDisposition, StreamWriteEnvelope, TrustedContributorId, TrustedStreamRowMetadata,
    build_trusted_stream_metadata_array,
};
use crate::db::manifest::token_store::validate_stream_token_plan_bounds;
use crate::error::{ManifestConflictDetails, OmniError, Result};
use crate::table_store::mem_wal::{
    B1_MAX_GENERATION_ARROW_BYTES, B1_MAX_GENERATION_ROWS, B2PreprocessingPermit,
    CallerOrdinalRange, b1_logical_batch_bytes,
};

use super::Omnigraph;
use super::stream_enrollment::PrepareStreamIngestOutcome;
use super::stream_graph_ingest::{ParsedGraphStreamJsonRow, parse_graph_stream_json_row};
use super::stream_ingest::{
    NormalizedStreamJsonRow, STREAM_B2_CLASSIFICATION_WINDOW_ROWS, StreamB2AckUnknown,
    StreamB2BindingProof, StreamB2BoundaryDisposition, StreamB2PrefixOutcome,
    StreamGraphIngestWitness, StreamTokenAdmissionAck, normalize_stream_json_parts_with_catalog,
    normalize_stream_json_row_with_catalog,
};
use super::stream_request::{
    NdjsonFrame, NdjsonFramer, OrderedResultBuffer, STREAM_REQUEST_MAX_RESULT_STATUSES,
    StreamLineBinding, StreamLineDetail, StreamLineOutcome, StreamLineStatus, StreamOccurrence,
    StreamRequestBlocker, StreamRequestPermit, apply_tail_precedence,
};

/// A single buffered result is the conservative hidden-profile default. It
/// keeps result bytes covered by the fixed request transport reservation even
/// when one logical id or validation message approaches the 32-MiB line cap.
const STREAM_RESULT_CHANNEL_CAPACITY: usize = 1;
const STREAM_SIZING_ATTEMPT_ID: &str = "00000000-0000-4000-8000-000000000001";
const GRAPH_STREAM_SIZING_UUID: &str = "00000000-0000-4000-8000-000000000002";
const STREAM_TAIL_SIZING_RETRY_DELAY: Duration = Duration::from_millis(1);

pub(super) type StreamChunkSource = Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send + 'static>>;

struct TransportOwnership {
    _permit: StreamRequestPermit,
}

pub(super) struct StreamRequestHandle {
    receiver: tokio::sync::mpsc::Receiver<StreamLineOutcome>,
    task: Option<tokio::task::JoinHandle<()>>,
    _transport: Arc<TransportOwnership>,
}

impl StreamRequestHandle {
    pub(super) async fn recv(&mut self) -> Result<Option<StreamLineOutcome>> {
        let Some(outcome) = self.receiver.recv().await else {
            self.wait_for_task().await?;
            return Ok(None);
        };
        Ok(Some(outcome))
    }

    async fn wait_for_task(&mut self) -> Result<()> {
        let Some(task) = self.task.as_mut() else {
            return Ok(());
        };
        // Await in place so cancelling this future retains the join handle.
        // A later recv/cancel can therefore still distinguish task failure
        // from clean channel EOF.
        let outcome = task.await;
        self.task = None;
        outcome
            .map_err(|error| OmniError::Lance(format!("stream request task failed: {error}")))?;
        Ok(())
    }

    #[cfg(feature = "failpoints")]
    async fn cancel_and_wait(mut self) -> Result<()> {
        self.receiver.close();
        self.wait_for_task().await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GraphLineRoute {
    kind: &'static str,
    type_name: String,
    logical_id: Option<String>,
    write_id: String,
}

struct GraphStreamLineOutcome {
    route: Option<GraphLineRoute>,
    outcome: StreamLineOutcome,
}

/// Hidden graph-native request handle. The existing line outcome retains all
/// private evidence needed for stop-tail safety, while this wrapper attaches
/// only caller-logical row identity for the redacted graph projection.
struct GraphStreamRequestHandle {
    inner: StreamRequestHandle,
    routes: Arc<Mutex<BTreeMap<u64, GraphLineRoute>>>,
}

impl GraphStreamRequestHandle {
    async fn recv(&mut self) -> Result<Option<GraphStreamLineOutcome>> {
        let Some(outcome) = self.inner.recv().await? else {
            return Ok(None);
        };
        let route = self
            .routes
            .lock()
            .map_err(|_| OmniError::manifest_internal("graph stream route registry was poisoned"))?
            .remove(&outcome.ordinal);
        Ok(Some(GraphStreamLineOutcome { route, outcome }))
    }
}

struct GraphLaneContext {
    table_key: String,
    stream_incarnation_id: String,
    sizing: IntrinsicSizingContext,
}

struct PendingRow {
    ordinal: u64,
    envelope: StreamWriteEnvelope,
    logical_id: Arc<str>,
    /// Owned only while the run is accumulating. Submission moves every
    /// one-row batch into concatenation so the request does not retain both
    /// the original row buffers and the dense submitted batch.
    batch: Option<RecordBatch>,
}

#[derive(Clone)]
struct IntrinsicSizingContext {
    identity: TableIdentity,
    accepted_schema_hash: String,
    enrollment_id: String,
}

impl PendingRow {
    fn occurrence(&self) -> StreamOccurrence {
        StreamOccurrence {
            stream_incarnation_id: self.envelope.stream_incarnation_id.clone(),
            logical_id: self.logical_id.to_string(),
            write_id: self.envelope.write_id.clone(),
        }
    }
}

struct AccumulatingRun {
    rows: Vec<PendingRow>,
    logical_bytes: u64,
    keys: BTreeSet<Arc<str>>,
    preprocessing: Option<B2PreprocessingPermit>,
    expected_graph_witness: Option<StreamGraphIngestWitness>,
}

impl AccumulatingRun {
    fn new(
        preprocessing: B2PreprocessingPermit,
        expected_graph_witness: Option<StreamGraphIngestWitness>,
    ) -> Self {
        Self {
            rows: Vec::new(),
            logical_bytes: 0,
            keys: BTreeSet::new(),
            preprocessing: Some(preprocessing),
            expected_graph_witness,
        }
    }

    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn must_split_before(&self, logical_id: &str, row_bytes: u64) -> bool {
        if self.rows.is_empty() {
            return false;
        }
        if self.keys.contains(logical_id) {
            return true;
        }
        let rows = self.rows.len().saturating_add(1);
        let bytes = self.logical_bytes.checked_add(row_bytes);
        rows > usize::try_from(B1_MAX_GENERATION_ROWS).unwrap_or(usize::MAX)
            || bytes.is_none_or(|bytes| bytes > B1_MAX_GENERATION_ARROW_BYTES)
    }

    fn push(&mut self, row: PendingRow, row_bytes: u64) -> Result<()> {
        if !self.keys.insert(row.logical_id.clone()) {
            return Err(OmniError::manifest_internal(
                "stream request planner admitted a repeated key into one physical run",
            ));
        }
        self.logical_bytes = self
            .logical_bytes
            .checked_add(row_bytes)
            .ok_or_else(|| OmniError::manifest_internal("stream run Arrow-byte overflow"))?;
        self.rows.push(row);
        Ok(())
    }

    fn is_full(&self) -> bool {
        self.rows.len() >= usize::try_from(B1_MAX_GENERATION_ROWS).unwrap_or(usize::MAX)
            || self.logical_bytes >= B1_MAX_GENERATION_ARROW_BYTES
    }
}

struct SubmittedRun {
    batch: RecordBatch,
    rows: Vec<PendingRow>,
    cursor: usize,
    preprocessing: Option<B2PreprocessingPermit>,
    expected_graph_witness: Option<StreamGraphIngestWitness>,
}

impl SubmittedRun {
    fn from_accumulating(mut run: AccumulatingRun) -> Result<Self> {
        if run.rows.is_empty() {
            return Err(OmniError::manifest_internal(
                "cannot submit an empty stream request run",
            ));
        }
        let batches = run
            .rows
            .iter_mut()
            .map(|row| {
                row.batch.take().ok_or_else(|| {
                    OmniError::manifest_internal(
                        "accumulating stream row lost its one-row Arrow batch",
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let schema = batches[0].schema();
        let batch = arrow_select::concat::concat_batches(&schema, &batches)
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        Ok(Self {
            batch,
            rows: run.rows,
            cursor: 0,
            preprocessing: run.preprocessing,
            expected_graph_witness: run.expected_graph_witness,
        })
    }

    fn window_end(&self) -> usize {
        self.cursor
            .saturating_add(STREAM_B2_CLASSIFICATION_WINDOW_ROWS)
            .min(self.rows.len())
    }
}

impl Omnigraph {
    /// Authenticate and start one hidden, transport-neutral NDJSON request.
    ///
    /// Policy, root/per-actor transport admission, checked serving-runtime
    /// authority, and an exact OPEN-lane capture all complete before `body` is
    /// polled. The returned receiver is bounded; dropping it stops future body
    /// polling while the root-owned task still settles any already-invoked B2
    /// call.
    pub(super) async fn stream_ingest_ndjson_as(
        self: &Arc<Self>,
        table_key: &str,
        actor_id: &str,
        body: StreamChunkSource,
    ) -> Result<StreamRequestHandle> {
        self.enforce(
            omnigraph_policy::PolicyAction::StreamIngest,
            &omnigraph_policy::ResourceScope::Graph,
            Some(actor_id),
        )?;
        let contributor_id = TrustedContributorId::new(actor_id.to_string())
            .map_err(|error| OmniError::manifest(error.to_string()))?;
        let permit = self.stream_requests.try_acquire(actor_id)?;

        let profile_guard = self.write_queue().acquire_stream_profile_shared().await;
        self.ensure_streaming_ingest_runtime_authorized().await?;
        let capture = self
            .capture_stream_authority(table_key, "stream NDJSON request preflight")
            .await?;
        Self::ensure_stream_table_admission_supported(&capture.txn.catalog, table_key)?;
        let catalog = Arc::new(super::public_catalog_view(&capture.txn.catalog)?);
        let sizing = IntrinsicSizingContext {
            identity: capture.entry.identity,
            accepted_schema_hash: capture.txn.authority.schema_ir_hash.clone(),
            enrollment_id: capture.binding.enrollment_id.clone(),
        };
        drop(profile_guard);

        let ownership = Arc::new(TransportOwnership { _permit: permit });
        let task_ownership = Arc::clone(&ownership);
        let (sender, receiver) = tokio::sync::mpsc::channel(STREAM_RESULT_CHANNEL_CAPACITY);
        let db = Arc::clone(self);
        let table_key = table_key.to_string();
        let task = tokio::spawn(async move {
            let _transport = task_ownership;
            drive_request(db, table_key, contributor_id, catalog, sizing, body, sender).await;
        });
        Ok(StreamRequestHandle {
            receiver,
            task: Some(task),
            _transport: ownership,
        })
    }

    /// Start one hidden graph-native mixed node/edge request.
    ///
    /// The graph is the caller-visible resource. Logical declarations are
    /// resolved against one pinned accepted catalog, and compatible private
    /// Lance lanes are enrolled lazily inside the request. No table key,
    /// dataset identity, or stream incarnation crosses this boundary.
    /// Rows retain firehose order rather than forming an atomic graph
    /// transaction: an edge is checked against the graph made visible by
    /// earlier settled lanes, and this slice does not buffer a later endpoint
    /// node as a forward reference.
    async fn stream_ingest_graph_ndjson_as(
        self: &Arc<Self>,
        actor_id: &str,
        body: StreamChunkSource,
    ) -> Result<GraphStreamRequestHandle> {
        self.enforce(
            omnigraph_policy::PolicyAction::StreamIngest,
            &omnigraph_policy::ResourceScope::Graph,
            Some(actor_id),
        )?;
        let contributor_id = TrustedContributorId::new(actor_id.to_string())
            .map_err(|error| OmniError::manifest(error.to_string()))?;
        let permit = self.stream_requests.try_acquire(actor_id)?;

        {
            let initial_profile_guard = self.write_queue().acquire_stream_profile_shared().await;
            self.ensure_streaming_ingest_runtime_authorized().await?;
            drop(initial_profile_guard);
        }
        // Settle the existing coordinator's cold startup cut before this
        // request can make an edge lane resident. Otherwise the sole resident
        // slot could make a previously acknowledged endpoint node lose the
        // node-before-edge round and turn a valid edge into a dead letter.
        self.settle_initial_stream_lanes_for_graph_ingest().await?;
        // The root→profile lock order above deliberately opens a transition
        // window. Reacquire profile authority and revalidate it before the
        // request pins its graph witness/catalog or polls the body.
        let profile_guard = self.write_queue().acquire_stream_profile_shared().await;
        self.ensure_streaming_ingest_runtime_authorized().await?;
        let txn = self.open_write_txn(None).await?;
        let expected_graph_witness = StreamGraphIngestWitness::capture(&txn)?;
        let catalog = Arc::new(super::public_catalog_view(&txn.catalog)?);
        drop(profile_guard);

        let ownership = Arc::new(TransportOwnership { _permit: permit });
        let task_ownership = Arc::clone(&ownership);
        let routes = Arc::new(Mutex::new(BTreeMap::new()));
        let task_routes = Arc::clone(&routes);
        let (sender, receiver) = tokio::sync::mpsc::channel(STREAM_RESULT_CHANNEL_CAPACITY);
        let db = Arc::clone(self);
        let actor_id = actor_id.to_string();
        let task = tokio::spawn(async move {
            let _transport = task_ownership;
            drive_graph_request(
                db,
                actor_id,
                contributor_id,
                catalog,
                expected_graph_witness,
                body,
                sender,
                task_routes,
            )
            .await;
        });
        Ok(GraphStreamRequestHandle {
            inner: StreamRequestHandle {
                receiver,
                task: Some(task),
                _transport: ownership,
            },
            routes,
        })
    }

    /// Feature-gated graph-wire proof seam. It exercises mixed declaration
    /// routing and real private lane durability while returning only the
    /// redacted graph result vocabulary.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_stream_ingest_graph_ndjson_as_for_test(
        self: &Arc<Self>,
        chunks: Vec<Vec<u8>>,
        actor_id: &str,
    ) -> Result<Vec<String>> {
        let body = Box::pin(futures::stream::iter(chunks.into_iter().map(Ok)));
        let mut handle = self.stream_ingest_graph_ndjson_as(actor_id, body).await?;
        let mut results = Vec::new();
        while let Some(outcome) = handle.recv().await? {
            results.push(graph_stream_line_outcome_json(&outcome).to_string());
        }
        Ok(results)
    }

    /// Feature-gated wire-only proof seam. JSON strings keep all private
    /// protocol/result types out of the SDK while allowing integration tests
    /// to pin caller order and token safety.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_stream_ingest_ndjson_as_for_test(
        self: &Arc<Self>,
        table_key: &str,
        chunks: Vec<Vec<u8>>,
        actor_id: &str,
    ) -> Result<Vec<String>> {
        let body = Box::pin(futures::stream::iter(chunks.into_iter().map(Ok)));
        let mut handle = self
            .stream_ingest_ndjson_as(table_key, actor_id, body)
            .await?;
        let mut results = Vec::new();
        while let Some(outcome) = handle.recv().await? {
            results.push(stream_line_outcome_json(&outcome).to_string());
        }
        Ok(results)
    }

    /// Feature-gated cancellation proof seam. It closes the result consumer
    /// and joins the request task so tests can prove disconnect does not cross
    /// a not-yet-invoked physical append boundary.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_stream_ingest_ndjson_cancel_for_test(
        self: &Arc<Self>,
        table_key: &str,
        chunks: Vec<Vec<u8>>,
        actor_id: &str,
    ) -> Result<()> {
        let body = Box::pin(futures::stream::iter(chunks.into_iter().map(Ok)));
        self.stream_ingest_ndjson_as(table_key, actor_id, body)
            .await?
            .cancel_and_wait()
            .await?;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive_graph_request(
    db: Arc<Omnigraph>,
    actor_id: String,
    contributor_id: TrustedContributorId,
    catalog: Arc<Catalog>,
    expected_graph_witness: StreamGraphIngestWitness,
    mut body: StreamChunkSource,
    sender: tokio::sync::mpsc::Sender<StreamLineOutcome>,
    routes: Arc<Mutex<BTreeMap<u64, GraphLineRoute>>>,
) {
    let mut framer = NdjsonFramer::new();
    let mut lane: Option<GraphLaneContext> = None;
    let mut run: Option<AccumulatingRun> = None;
    let mut blocker: Option<StreamRequestBlocker> = None;
    let mut results = OrderedResultBuffer::new(0);
    let mut next_input_ordinal = 0_u64;

    loop {
        tokio::select! {
            biased;
            _ = sender.closed() => return,
            next = body.next() => {
                match next {
                    Some(Ok(chunk)) => {
                        let mut frames = match framer.push_chunk(chunk) {
                            Ok(frames) => frames,
                            Err(error) => {
                                if !flush_graph_run(
                                    &db,
                                    &contributor_id,
                                    lane.as_ref(),
                                    &mut run,
                                    &mut blocker,
                                    &mut results,
                                    &sender,
                                ).await {
                                    return;
                                }
                                let local = StreamLineOutcome::new(
                                    next_input_ordinal,
                                    StreamLineDetail::Invalid {
                                        message: error.to_string(),
                                    },
                                );
                                let outcome = apply_tail_precedence(local, blocker.as_ref());
                                let _ = emit_ordered(outcome, &mut results, &sender).await;
                                return;
                            }
                        };
                        for frame in &mut frames {
                            if sender.is_closed() {
                                return;
                            }
                            next_input_ordinal = frame.ordinal().saturating_add(1);
                            if !process_graph_frame(
                                &db,
                                &actor_id,
                                &contributor_id,
                                &catalog,
                                &expected_graph_witness,
                                frame,
                                &mut lane,
                                &mut run,
                                &mut blocker,
                                &mut results,
                                &sender,
                                &routes,
                            ).await {
                                return;
                            }
                        }
                    }
                    Some(Err(error)) => {
                        if !flush_graph_run(
                            &db,
                            &contributor_id,
                            lane.as_ref(),
                            &mut run,
                            &mut blocker,
                            &mut results,
                            &sender,
                        ).await {
                            return;
                        }
                        let local = StreamLineOutcome::new(
                            next_input_ordinal,
                            StreamLineDetail::Invalid {
                                message: format!("stream body failed: {error}"),
                            },
                        );
                        let outcome = apply_tail_precedence(local, blocker.as_ref());
                        let _ = emit_ordered(outcome, &mut results, &sender).await;
                        return;
                    }
                    None => break,
                }
            }
        }
    }

    if sender.is_closed() {
        return;
    }
    for frame in framer.finish() {
        if sender.is_closed() {
            return;
        }
        if !process_graph_frame(
            &db,
            &actor_id,
            &contributor_id,
            &catalog,
            &expected_graph_witness,
            frame,
            &mut lane,
            &mut run,
            &mut blocker,
            &mut results,
            &sender,
            &routes,
        )
        .await
        {
            return;
        }
    }
    let _ = flush_graph_run(
        &db,
        &contributor_id,
        lane.as_ref(),
        &mut run,
        &mut blocker,
        &mut results,
        &sender,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn process_graph_frame(
    db: &Arc<Omnigraph>,
    actor_id: &str,
    contributor_id: &TrustedContributorId,
    catalog: &Catalog,
    expected_graph_witness: &StreamGraphIngestWitness,
    frame: NdjsonFrame,
    lane: &mut Option<GraphLaneContext>,
    run: &mut Option<AccumulatingRun>,
    blocker: &mut Option<StreamRequestBlocker>,
    results: &mut OrderedResultBuffer,
    sender: &tokio::sync::mpsc::Sender<StreamLineOutcome>,
    routes: &Arc<Mutex<BTreeMap<u64, GraphLineRoute>>>,
) -> bool {
    match frame {
        NdjsonFrame::InputTooLarge {
            ordinal,
            limit,
            actual,
        } => {
            if !flush_graph_run(
                db,
                contributor_id,
                lane.as_ref(),
                run,
                blocker,
                results,
                sender,
            )
            .await
            {
                return false;
            }
            let local = StreamLineOutcome::new(
                ordinal,
                StreamLineDetail::StreamInputTooLarge {
                    limit: u64::try_from(limit).unwrap_or(u64::MAX),
                    actual: u64::try_from(actual).unwrap_or(u64::MAX),
                },
            );
            emit_ordered(
                apply_tail_precedence(local, blocker.as_ref()),
                results,
                sender,
            )
            .await
        }
        NdjsonFrame::Line { ordinal, bytes } => {
            // An accumulating run already owns the parsing envelope. A fresh
            // reservation is needed only for an empty request, a stopped
            // tail, or a declaration transition that must retain this row
            // while the previous lane settles.
            let mut next_preprocessing = if blocker.is_some() {
                match reserve_tail_sizing_preprocessing(db, sender).await {
                    Some(Ok(preprocessing)) => Some(preprocessing),
                    Some(Err(_)) => {
                        return emit_ordered(
                            apply_tail_precedence(uninvoked_local(ordinal), blocker.as_ref()),
                            results,
                            sender,
                        )
                        .await;
                    }
                    None => return false,
                }
            } else if run.is_none() {
                match db.stream_workers.reserve_b2_preprocessing() {
                    Ok(preprocessing) => Some(preprocessing),
                    Err(error) => {
                        let outcome = b2_error_outcome(ordinal, error);
                        *blocker = outcome.as_blocker();
                        return emit_ordered(outcome, results, sender).await;
                    }
                }
            } else {
                None
            };

            let parsed = match parse_graph_stream_json_row(&bytes) {
                Ok(parsed) => parsed,
                Err(error) => {
                    drop(next_preprocessing.take());
                    if !flush_graph_run(
                        db,
                        contributor_id,
                        lane.as_ref(),
                        run,
                        blocker,
                        results,
                        sender,
                    )
                    .await
                    {
                        return false;
                    }
                    return emit_ordered(
                        apply_tail_precedence(
                            adapter_error_outcome(ordinal, error),
                            blocker.as_ref(),
                        ),
                        results,
                        sender,
                    )
                    .await;
                }
            };
            if !record_graph_route(routes, ordinal, graph_route(&parsed)) {
                return false;
            }
            let (table_key, canonical_type_name) = match resolve_graph_declaration(catalog, &parsed)
            {
                Ok(resolved) => resolved,
                Err(error) => {
                    drop(parsed);
                    drop(next_preprocessing.take());
                    if !flush_graph_run(
                        db,
                        contributor_id,
                        lane.as_ref(),
                        run,
                        blocker,
                        results,
                        sender,
                    )
                    .await
                    {
                        return false;
                    }
                    return emit_ordered(
                        apply_tail_precedence(
                            adapter_error_outcome(ordinal, error),
                            blocker.as_ref(),
                        ),
                        results,
                        sender,
                    )
                    .await;
                }
            };
            let canonical_route = GraphLineRoute {
                kind: parsed.kind(),
                type_name: canonical_type_name,
                logical_id: parsed.logical_id().map(ToOwned::to_owned),
                write_id: parsed.write_id().to_string(),
            };
            if !record_graph_route(routes, ordinal, canonical_route.clone()) {
                return false;
            }

            // Complete schema normalization and exact intrinsic sizing before
            // lazy enrollment. The placeholder UUIDs are fixed-width private
            // values; replacing the incarnation below cannot change the size
            // proof.
            let mut normalized = match normalize_graph_row_before_enrollment(
                db,
                catalog,
                expected_graph_witness,
                contributor_id,
                &table_key,
                ordinal,
                parsed,
            )
            .await
            {
                Ok(normalized) => normalized,
                Err(error) => {
                    drop(next_preprocessing.take());
                    if !flush_graph_run(
                        db,
                        contributor_id,
                        lane.as_ref(),
                        run,
                        blocker,
                        results,
                        sender,
                    )
                    .await
                    {
                        return false;
                    }
                    return emit_ordered(
                        apply_tail_precedence(
                            adapter_error_outcome(ordinal, error),
                            blocker.as_ref(),
                        ),
                        results,
                        sender,
                    )
                    .await;
                }
            };
            if !record_graph_route(
                routes,
                ordinal,
                GraphLineRoute {
                    kind: canonical_route.kind,
                    type_name: canonical_route.type_name.clone(),
                    logical_id: Some(normalized.logical_id.clone()),
                    write_id: canonical_route.write_id.clone(),
                },
            ) {
                return false;
            }
            if blocker.is_some() {
                drop(normalized);
                drop(next_preprocessing.take());
                return emit_ordered(
                    apply_tail_precedence(uninvoked_local(ordinal), blocker.as_ref()),
                    results,
                    sender,
                )
                .await;
            }
            let declaration_changed = lane
                .as_ref()
                .is_some_and(|active| active.table_key != table_key);
            if declaration_changed {
                if next_preprocessing.is_none() {
                    match db.stream_workers.reserve_b2_preprocessing() {
                        Ok(preprocessing) => next_preprocessing = Some(preprocessing),
                        Err(error) => {
                            // Without a second envelope the normalized row may
                            // not remain resident while the prior lane flushes.
                            drop(normalized);
                            if !flush_graph_run(
                                db,
                                contributor_id,
                                lane.as_ref(),
                                run,
                                blocker,
                                results,
                                sender,
                            )
                            .await
                            {
                                return false;
                            }
                            *lane = None;
                            if blocker.is_some() {
                                return emit_ordered(
                                    apply_tail_precedence(
                                        uninvoked_local(ordinal),
                                        blocker.as_ref(),
                                    ),
                                    results,
                                    sender,
                                )
                                .await;
                            }
                            let outcome = b2_error_outcome(ordinal, error);
                            *blocker = outcome.as_blocker();
                            return emit_ordered(outcome, results, sender).await;
                        }
                    }
                }
                if !flush_graph_run(
                    db,
                    contributor_id,
                    lane.as_ref(),
                    run,
                    blocker,
                    results,
                    sender,
                )
                .await
                {
                    return false;
                }
                if blocker.is_some() {
                    drop(normalized);
                    drop(next_preprocessing.take());
                    return emit_ordered(
                        apply_tail_precedence(uninvoked_local(ordinal), blocker.as_ref()),
                        results,
                        sender,
                    )
                    .await;
                }
                let prior_lane = lane
                    .as_ref()
                    .expect("a declaration transition has one active private lane");
                if let Err(error) = db
                    .fold_resident_stream_lane_for_handoff(
                        prior_lane.sizing.identity,
                        &prior_lane.table_key,
                    )
                    .await
                {
                    drop(normalized);
                    drop(next_preprocessing.take());
                    let outcome = b2_error_outcome(ordinal, error);
                    *blocker = outcome.as_blocker();
                    return emit_ordered(outcome, results, sender).await;
                }
                *lane = None;
            }

            if lane.is_none() {
                match ensure_graph_lane(db, actor_id, &table_key, expected_graph_witness).await {
                    Ok(active) => *lane = Some(active),
                    Err(error) => {
                        drop(normalized);
                        drop(next_preprocessing.take());
                        let outcome = b2_error_outcome(ordinal, error);
                        *blocker = outcome.as_blocker();
                        return emit_ordered(outcome, results, sender).await;
                    }
                }
            }
            let active = lane.as_ref().expect("graph lane was installed");
            normalized.envelope.stream_incarnation_id = active.stream_incarnation_id.clone();
            if run.is_none() {
                let Some(preprocessing) = next_preprocessing.take() else {
                    drop(normalized);
                    let outcome = StreamLineOutcome::new(
                        ordinal,
                        StreamLineDetail::Invalid {
                            message: "graph stream row lost preprocessing ownership".to_string(),
                        },
                    );
                    return emit_ordered(outcome, results, sender).await;
                };
                *run = Some(AccumulatingRun::new(
                    preprocessing,
                    Some(expected_graph_witness.clone()),
                ));
            }
            debug_assert!(next_preprocessing.is_none());
            process_normalized_row(
                db,
                &active.table_key,
                contributor_id,
                &active.sizing,
                Some(expected_graph_witness),
                ordinal,
                normalized,
                true,
                run,
                blocker,
                results,
                sender,
            )
            .await
        }
    }
}

fn graph_route(parsed: &ParsedGraphStreamJsonRow) -> GraphLineRoute {
    GraphLineRoute {
        kind: parsed.kind(),
        type_name: parsed.type_name().to_string(),
        logical_id: parsed.logical_id().map(ToOwned::to_owned),
        write_id: parsed.write_id().to_string(),
    }
}

fn record_graph_route(
    routes: &Arc<Mutex<BTreeMap<u64, GraphLineRoute>>>,
    ordinal: u64,
    route: GraphLineRoute,
) -> bool {
    let Ok(mut routes) = routes.lock() else {
        return false;
    };
    routes.insert(ordinal, route);
    true
}

fn resolve_graph_declaration(
    catalog: &Catalog,
    parsed: &ParsedGraphStreamJsonRow,
) -> Result<(String, String)> {
    let (table_key, canonical_type_name) = match parsed.kind() {
        "node" => {
            let node = catalog.node_types.get(parsed.type_name()).ok_or_else(|| {
                OmniError::manifest(format!(
                    "unknown graph node declaration '{}'",
                    parsed.type_name()
                ))
            })?;
            (format!("node:{}", node.name), node.name.clone())
        }
        "edge" => {
            let edge = catalog
                .lookup_edge_by_name(parsed.type_name())
                .ok_or_else(|| {
                    OmniError::manifest(format!(
                        "unknown graph edge declaration '{}'",
                        parsed.type_name()
                    ))
                })?;
            (format!("edge:{}", edge.name), edge.name.clone())
        }
        _ => {
            return Err(OmniError::manifest_internal(
                "graph stream parser returned an unknown logical kind",
            ));
        }
    };
    Omnigraph::ensure_stream_table_admission_supported(catalog, &table_key)?;
    Ok((table_key, canonical_type_name))
}

async fn normalize_graph_row_before_enrollment(
    db: &Arc<Omnigraph>,
    catalog: &Catalog,
    expected_graph_witness: &StreamGraphIngestWitness,
    contributor_id: &TrustedContributorId,
    table_key: &str,
    ordinal: u64,
    parsed: ParsedGraphStreamJsonRow,
) -> Result<NormalizedStreamJsonRow> {
    let (write_id, predecessor_token, body) = parsed.into_normalizer_parts();
    let envelope = StreamWriteEnvelope {
        stream_incarnation_id: GRAPH_STREAM_SIZING_UUID.to_string(),
        write_id,
        predecessor_token,
    };
    let normalized = normalize_stream_json_parts_with_catalog(table_key, envelope, body, catalog)?;
    // Pin every single-row bound before enrollment. The later real
    // incarnation/enrollment IDs have the same canonical UUID width.
    let _ = b1_logical_batch_bytes(&normalized.batch)
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    let sizing = IntrinsicSizingContext {
        identity: graph_catalog_table_identity(catalog, table_key)?,
        accepted_schema_hash: expected_graph_witness.schema_ir_hash().to_string(),
        enrollment_id: GRAPH_STREAM_SIZING_UUID.to_string(),
    };
    validate_intrinsic_single_row_size(
        db,
        table_key,
        contributor_id,
        &sizing,
        ordinal,
        &normalized,
    )
    .await?;
    Ok(normalized)
}

async fn ensure_graph_lane(
    db: &Arc<Omnigraph>,
    actor_id: &str,
    table_key: &str,
    expected_graph_witness: &StreamGraphIngestWitness,
) -> Result<GraphLaneContext> {
    let current = db.open_write_txn(None).await?;
    expected_graph_witness.validate(&current, "graph stream lazy enrollment")?;

    let enrollment_request_id = ShardId::new_v4().to_string();
    let first = db
        .prepare_stream_ingest_pre_authorized(table_key, &enrollment_request_id, None, actor_id)
        .await?;
    let completed = match first {
        PrepareStreamIngestOutcome::WitnessRequired(witness) => {
            db.prepare_stream_ingest_pre_authorized(
                table_key,
                &enrollment_request_id,
                Some(&witness),
                actor_id,
            )
            .await?
        }
        completed => completed,
    };
    if matches!(completed, PrepareStreamIngestOutcome::WitnessRequired(_)) {
        return Err(OmniError::manifest_read_set_changed(
            format!("graph_stream_lazy_enrollment:{table_key}"),
            Some("echoed eligibility witness".to_string()),
            Some("stream eligibility moved before enrollment".to_string()),
        ));
    }

    let capture = db
        .capture_stream_authority(table_key, "graph stream lazy enrollment result")
        .await?;
    expected_graph_witness.validate(&capture.txn, "graph stream lane capture")?;
    Omnigraph::ensure_stream_table_admission_supported(&capture.txn.catalog, table_key)?;
    Ok(GraphLaneContext {
        table_key: table_key.to_string(),
        stream_incarnation_id: capture
            .lifecycle
            .enrollment_receipt
            .stream_incarnation_id
            .clone(),
        sizing: IntrinsicSizingContext {
            identity: capture.entry.identity,
            accepted_schema_hash: capture.txn.authority.schema_ir_hash.clone(),
            enrollment_id: capture.binding.enrollment_id.clone(),
        },
    })
}

fn graph_catalog_table_identity(catalog: &Catalog, table_key: &str) -> Result<TableIdentity> {
    let (stable, incarnation) = if let Some(name) = table_key.strip_prefix("node:") {
        (
            catalog.node_type_id(name),
            catalog.node_table_incarnation_id(name),
        )
    } else if let Some(name) = table_key.strip_prefix("edge:") {
        (
            catalog.edge_type_id(name),
            catalog.edge_table_incarnation_id(name),
        )
    } else {
        return Err(OmniError::manifest_internal(
            "invalid resolved graph stream table key",
        ));
    };
    let stable = stable.ok_or_else(|| {
        OmniError::manifest_internal("resolved graph stream declaration has no stable identity")
    })?;
    let incarnation = incarnation.ok_or_else(|| {
        OmniError::manifest_internal("resolved graph stream declaration has no table incarnation")
    })?;
    TableIdentity::new(stable.get(), incarnation.get())
}

#[allow(clippy::too_many_arguments)]
async fn flush_graph_run(
    db: &Arc<Omnigraph>,
    contributor_id: &TrustedContributorId,
    lane: Option<&GraphLaneContext>,
    run: &mut Option<AccumulatingRun>,
    blocker: &mut Option<StreamRequestBlocker>,
    results: &mut OrderedResultBuffer,
    sender: &tokio::sync::mpsc::Sender<StreamLineOutcome>,
) -> bool {
    if run.is_none() {
        return true;
    }
    let Some(lane) = lane else {
        let outcome = StreamLineOutcome::new(
            0,
            StreamLineDetail::Invalid {
                message: "graph stream run lost its private lane".to_string(),
            },
        );
        return emit_ordered(outcome, results, sender).await;
    };
    flush_run(
        db,
        &lane.table_key,
        contributor_id,
        run,
        blocker,
        results,
        sender,
    )
    .await
}

async fn drive_request(
    db: Arc<Omnigraph>,
    table_key: String,
    contributor_id: TrustedContributorId,
    catalog: Arc<Catalog>,
    sizing: IntrinsicSizingContext,
    mut body: StreamChunkSource,
    sender: tokio::sync::mpsc::Sender<StreamLineOutcome>,
) {
    let mut framer = NdjsonFramer::new();
    let mut run: Option<AccumulatingRun> = None;
    let mut blocker: Option<StreamRequestBlocker> = None;
    let mut results = OrderedResultBuffer::new(0);
    let mut next_input_ordinal = 0_u64;

    loop {
        tokio::select! {
            biased;
            _ = sender.closed() => return,
            next = body.next() => {
                match next {
                    Some(Ok(chunk)) => {
                        let mut frames = match framer.push_chunk(chunk) {
                            Ok(frames) => frames,
                            Err(error) => {
                                if !flush_run(
                                    &db,
                                    &table_key,
                                    &contributor_id,
                                    &mut run,
                                    &mut blocker,
                                    &mut results,
                                    &sender,
                                ).await {
                                    return;
                                }
                                let local = StreamLineOutcome::new(
                                    next_input_ordinal,
                                    StreamLineDetail::Invalid {
                                        message: error.to_string(),
                                    },
                                );
                                let outcome = apply_tail_precedence(local, blocker.as_ref());
                                let _ = emit_ordered(outcome, &mut results, &sender).await;
                                return;
                            }
                        };
                        for frame in &mut frames {
                            if sender.is_closed() {
                                return;
                            }
                            next_input_ordinal = frame.ordinal().saturating_add(1);
                            if !process_frame(
                                &db,
                                &table_key,
                                &contributor_id,
                                &catalog,
                                &sizing,
                                None,
                                frame,
                                &mut run,
                                &mut blocker,
                                &mut results,
                                &sender,
                            )
                            .await
                            {
                                return;
                            }
                        }
                    }
                    Some(Err(error)) => {
                        if !flush_run(
                            &db,
                            &table_key,
                            &contributor_id,
                            &mut run,
                            &mut blocker,
                            &mut results,
                            &sender,
                        ).await {
                            return;
                        }
                        let local = StreamLineOutcome::new(
                            next_input_ordinal,
                            StreamLineDetail::Invalid {
                                message: format!("stream body failed: {error}"),
                            },
                        );
                        let outcome = apply_tail_precedence(local, blocker.as_ref());
                        let _ = emit_ordered(outcome, &mut results, &sender).await;
                        return;
                    }
                    None => break,
                }
            }
        }
    }

    if sender.is_closed() {
        return;
    }
    for frame in framer.finish() {
        if sender.is_closed() {
            return;
        }
        if !process_frame(
            &db,
            &table_key,
            &contributor_id,
            &catalog,
            &sizing,
            None,
            frame,
            &mut run,
            &mut blocker,
            &mut results,
            &sender,
        )
        .await
        {
            return;
        }
    }
    let _ = flush_run(
        &db,
        &table_key,
        &contributor_id,
        &mut run,
        &mut blocker,
        &mut results,
        &sender,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn process_frame(
    db: &Arc<Omnigraph>,
    table_key: &str,
    contributor_id: &TrustedContributorId,
    catalog: &Catalog,
    sizing: &IntrinsicSizingContext,
    expected_graph_witness: Option<&StreamGraphIngestWitness>,
    frame: NdjsonFrame,
    run: &mut Option<AccumulatingRun>,
    blocker: &mut Option<StreamRequestBlocker>,
    results: &mut OrderedResultBuffer,
    sender: &tokio::sync::mpsc::Sender<StreamLineOutcome>,
) -> bool {
    match frame {
        NdjsonFrame::InputTooLarge {
            ordinal,
            limit,
            actual,
        } => {
            if !flush_run(db, table_key, contributor_id, run, blocker, results, sender).await {
                return false;
            }
            let local = StreamLineOutcome::new(
                ordinal,
                StreamLineDetail::StreamInputTooLarge {
                    limit: u64::try_from(limit).unwrap_or(u64::MAX),
                    actual: u64::try_from(actual).unwrap_or(u64::MAX),
                },
            );
            emit_ordered(
                apply_tail_precedence(local, blocker.as_ref()),
                results,
                sender,
            )
            .await
        }
        NdjsonFrame::Line { ordinal, bytes } => {
            if blocker.is_some() {
                let preprocessing = match reserve_tail_sizing_preprocessing(db, sender).await {
                    Some(Ok(preprocessing)) => preprocessing,
                    Some(Err(_)) => {
                        return emit_ordered(
                            apply_tail_precedence(uninvoked_local(ordinal), blocker.as_ref()),
                            results,
                            sender,
                        )
                        .await;
                    }
                    None => return false,
                };
                if sender.is_closed() {
                    return false;
                }
                let local = match normalize_stream_json_row_with_catalog(table_key, &bytes, catalog)
                {
                    Ok(normalized) => match tokio::select! {
                        biased;
                        _ = sender.closed() => return false,
                        result = validate_intrinsic_single_row_size(
                            db,
                            table_key,
                            contributor_id,
                            sizing,
                            ordinal,
                            &normalized,
                        ) => result,
                    } {
                        Err(error) if intrinsic_input_too_large(&error).is_some() => {
                            let (limit, actual) = intrinsic_input_too_large(&error)
                                .expect("guard proved an intrinsic input limit");
                            StreamLineOutcome::new(
                                ordinal,
                                StreamLineDetail::StreamInputTooLarge { limit, actual },
                            )
                        }
                        Ok(()) | Err(_) => uninvoked_local(ordinal),
                    },
                    Err(error) => adapter_error_outcome(ordinal, error),
                };
                drop(preprocessing);
                return emit_ordered(
                    apply_tail_precedence(local, blocker.as_ref()),
                    results,
                    sender,
                )
                .await;
            }

            if run.is_none() {
                match db.stream_workers.reserve_b2_preprocessing() {
                    Ok(preprocessing) => {
                        *run = Some(AccumulatingRun::new(
                            preprocessing,
                            expected_graph_witness.cloned(),
                        ))
                    }
                    Err(error) => {
                        let outcome = b2_error_outcome(ordinal, error);
                        *blocker = outcome.as_blocker();
                        return emit_ordered(outcome, results, sender).await;
                    }
                }
            }

            let normalized =
                match normalize_stream_json_row_with_catalog(table_key, &bytes, catalog) {
                    Ok(normalized) => normalized,
                    Err(error) => {
                        if !flush_run(db, table_key, contributor_id, run, blocker, results, sender)
                            .await
                        {
                            return false;
                        }
                        let local = adapter_error_outcome(ordinal, error);
                        return emit_ordered(
                            apply_tail_precedence(local, blocker.as_ref()),
                            results,
                            sender,
                        )
                        .await;
                    }
                };
            process_normalized_row(
                db,
                table_key,
                contributor_id,
                sizing,
                expected_graph_witness,
                ordinal,
                normalized,
                false,
                run,
                blocker,
                results,
                sender,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_normalized_row(
    db: &Arc<Omnigraph>,
    table_key: &str,
    contributor_id: &TrustedContributorId,
    sizing: &IntrinsicSizingContext,
    expected_graph_witness: Option<&StreamGraphIngestWitness>,
    ordinal: u64,
    normalized: NormalizedStreamJsonRow,
    intrinsic_already_validated: bool,
    run: &mut Option<AccumulatingRun>,
    blocker: &mut Option<StreamRequestBlocker>,
    results: &mut OrderedResultBuffer,
    sender: &tokio::sync::mpsc::Sender<StreamLineOutcome>,
) -> bool {
    let row_bytes = match b1_logical_batch_bytes(&normalized.batch) {
        Ok(bytes) => bytes,
        Err(error) => {
            if !flush_run(db, table_key, contributor_id, run, blocker, results, sender).await {
                return false;
            }
            let local = StreamLineOutcome::new(
                ordinal,
                StreamLineDetail::Invalid {
                    message: error.to_string(),
                },
            );
            return emit_ordered(
                apply_tail_precedence(local, blocker.as_ref()),
                results,
                sender,
            )
            .await;
        }
    };
    if !intrinsic_already_validated
        && let Err(error) = validate_intrinsic_single_row_size(
            db,
            table_key,
            contributor_id,
            sizing,
            ordinal,
            &normalized,
        )
        .await
    {
        if !flush_run(db, table_key, contributor_id, run, blocker, results, sender).await {
            return false;
        }
        let local = adapter_error_outcome(ordinal, error);
        return emit_ordered(
            apply_tail_precedence(local, blocker.as_ref()),
            results,
            sender,
        )
        .await;
    }

    if run
        .as_ref()
        .is_some_and(|run| run.must_split_before(&normalized.logical_id, row_bytes))
        && !flush_run(db, table_key, contributor_id, run, blocker, results, sender).await
    {
        return false;
    }
    if blocker.is_some() {
        return emit_ordered(
            apply_tail_precedence(uninvoked_local(ordinal), blocker.as_ref()),
            results,
            sender,
        )
        .await;
    }
    if run.is_none() {
        match db.stream_workers.reserve_b2_preprocessing() {
            Ok(preprocessing) => {
                *run = Some(AccumulatingRun::new(
                    preprocessing,
                    expected_graph_witness.cloned(),
                ))
            }
            Err(error) => {
                let outcome = b2_error_outcome(ordinal, error);
                *blocker = outcome.as_blocker();
                return emit_ordered(outcome, results, sender).await;
            }
        }
    }
    let pending = PendingRow {
        ordinal,
        envelope: normalized.envelope,
        logical_id: Arc::<str>::from(normalized.logical_id),
        batch: Some(normalized.batch),
    };
    if let Err(error) = run
        .as_mut()
        .expect("stream run was installed")
        .push(pending, row_bytes)
    {
        let outcome = StreamLineOutcome::new(
            ordinal,
            StreamLineDetail::Invalid {
                message: error.to_string(),
            },
        );
        return emit_ordered(outcome, results, sender).await;
    }
    if run.as_ref().is_some_and(AccumulatingRun::is_full) {
        return flush_run(db, table_key, contributor_id, run, blocker, results, sender).await;
    }
    true
}

/// Temporarily enter the same root preprocessing corridor used by B2 before
/// performing blocked-tail canonical/token sizing.
///
/// The registry exposes effect-free try-reservation because ordinary admission
/// reports pressure per line. A stopped tail cannot report that later pressure
/// in place of adapter-local `invalid` / `stream_input_too_large`, so it waits
/// for the transient root reservation while racing output cancellation.
async fn reserve_tail_sizing_preprocessing(
    db: &Arc<Omnigraph>,
    sender: &tokio::sync::mpsc::Sender<StreamLineOutcome>,
) -> Option<Result<B2PreprocessingPermit>> {
    loop {
        if sender.is_closed() {
            return None;
        }
        match db.stream_workers.reserve_b2_preprocessing() {
            Ok(preprocessing) => {
                if sender.is_closed() {
                    return None;
                }
                return Some(Ok(preprocessing));
            }
            Err(OmniError::ResourceLimitExceeded { ref resource, .. })
                if matches!(
                    resource.as_str(),
                    "stream_b2_preprocessing_bytes" | "stream_inflight_calls"
                ) =>
            {
                tokio::select! {
                    biased;
                    _ = sender.closed() => return None,
                    _ = tokio::time::sleep(STREAM_TAIL_SIZING_RETRY_DELAY) => {}
                }
            }
            Err(error) => return Some(Err(error)),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn flush_run(
    db: &Arc<Omnigraph>,
    table_key: &str,
    contributor_id: &TrustedContributorId,
    run: &mut Option<AccumulatingRun>,
    blocker: &mut Option<StreamRequestBlocker>,
    results: &mut OrderedResultBuffer,
    sender: &tokio::sync::mpsc::Sender<StreamLineOutcome>,
) -> bool {
    let Some(accumulating) = run.take() else {
        return true;
    };
    if accumulating.is_empty() {
        return true;
    }
    let first_run_ordinal = accumulating.rows[0].ordinal;
    let mut submitted = match SubmittedRun::from_accumulating(accumulating) {
        Ok(submitted) => submitted,
        Err(error) => {
            return emit_ordered(
                StreamLineOutcome::new(
                    first_run_ordinal,
                    StreamLineDetail::Invalid {
                        message: error.to_string(),
                    },
                ),
                results,
                sender,
            )
            .await;
        }
    };

    while submitted.cursor < submitted.rows.len() {
        if sender.is_closed() {
            return false;
        }
        if let Some(active) = blocker.as_ref() {
            while submitted.cursor < submitted.rows.len() {
                let ordinal = submitted.rows[submitted.cursor].ordinal;
                let outcome = apply_tail_precedence(uninvoked_local(ordinal), Some(active));
                submitted.cursor += 1;
                if !emit_ordered(outcome, results, sender).await {
                    return false;
                }
            }
            return true;
        }

        if let Err(error) = db.heal_pending_recovery_sidecars_for_write(&[None]).await {
            let ordinal = submitted.rows[submitted.cursor].ordinal;
            let outcome = b2_error_outcome(ordinal, error);
            *blocker = outcome.as_blocker();
            if !emit_ordered(outcome, results, sender).await {
                return false;
            }
            continue;
        }
        if sender.is_closed() {
            return false;
        }

        let end = submitted.window_end();
        let first_ordinal = submitted.rows[submitted.cursor].ordinal;
        let last_ordinal = submitted.rows[end - 1].ordinal;
        let caller_ordinals = match CallerOrdinalRange::new(first_ordinal, last_ordinal) {
            Ok(range) => range,
            Err(error) => {
                let outcome = StreamLineOutcome::new(
                    first_ordinal,
                    StreamLineDetail::Invalid {
                        message: error.to_string(),
                    },
                );
                if !emit_ordered(outcome, results, sender).await {
                    return false;
                }
                submitted.cursor += 1;
                continue;
            }
        };
        let preprocessing = match submitted.preprocessing.take() {
            Some(preprocessing) => preprocessing,
            None => match db.stream_workers.reserve_b2_preprocessing() {
                Ok(preprocessing) => preprocessing,
                Err(error) => {
                    let outcome = b2_error_outcome(first_ordinal, error);
                    *blocker = outcome.as_blocker();
                    if !emit_ordered(outcome, results, sender).await {
                        return false;
                    }
                    continue;
                }
            },
        };
        let write_queue = db.write_queue();
        let driver_round_guard = tokio::select! {
            biased;
            _ = sender.closed() => return false,
            guard = db.acquire_stream_fold_producer_guard() => guard,
        };
        let profile_guard = tokio::select! {
            biased;
            _ = sender.closed() => return false,
            guard = write_queue.acquire_stream_profile_shared() => guard,
        };
        if let Err(error) = db.ensure_streaming_ingest_runtime_authorized().await {
            drop(profile_guard);
            let outcome = b2_error_outcome(first_ordinal, error);
            *blocker = outcome.as_blocker();
            if !emit_ordered(outcome, results, sender).await {
                return false;
            }
            continue;
        }
        if sender.is_closed() {
            return false;
        }
        let envelopes = submitted.rows[submitted.cursor..end]
            .iter()
            .map(|row| row.envelope.clone())
            .collect::<Vec<_>>();
        // Cancellation and the physical call share one biased handoff. If the
        // receiver is already gone, the B2 future is never polled. If B2 has
        // crossed its detached invocation boundary, dropping this waiter only
        // abandons the response: the root-owned worker still settles watcher,
        // fence, and retirement ownership.
        let outcome = tokio::select! {
            biased;
            _ = sender.closed() => return false,
            outcome = db.stream_put_phase_b2_distinct_prefix_under_profile_guard(
                table_key,
                submitted
                    .batch
                    .slice(submitted.cursor, end - submitted.cursor),
                caller_ordinals,
                envelopes,
                contributor_id.clone(),
                Some(preprocessing),
                driver_round_guard,
                profile_guard,
                submitted.expected_graph_witness.as_ref(),
            ) => outcome,
        };
        match outcome {
            Ok(StreamB2PrefixOutcome::Admitted {
                binding,
                acknowledgements,
                ..
            }) => {
                let count = acknowledgements.len();
                if count == 0 || count > end - submitted.cursor {
                    let outcome = StreamLineOutcome::new(
                        first_ordinal,
                        StreamLineDetail::Invalid {
                            message: "B2 returned an invalid admitted-prefix length".to_string(),
                        },
                    );
                    let _ = emit_ordered(outcome, results, sender).await;
                    return false;
                }
                for (offset, ack) in acknowledgements.into_iter().enumerate() {
                    let row = &submitted.rows[submitted.cursor + offset];
                    if !emit_ordered(
                        durable_outcome(row, ack, line_binding(&binding)),
                        results,
                        sender,
                    )
                    .await
                    {
                        return false;
                    }
                }
                submitted.cursor += count;
            }
            Ok(StreamB2PrefixOutcome::Boundary {
                caller_ordinal,
                binding,
                disposition,
            }) => {
                let Some(row) = submitted.rows.get(submitted.cursor) else {
                    return false;
                };
                if row.ordinal != caller_ordinal {
                    let outcome = StreamLineOutcome::new(
                        row.ordinal,
                        StreamLineDetail::Invalid {
                            message: "B2 boundary ordinal did not match request cursor".to_string(),
                        },
                    );
                    if !emit_ordered(outcome, results, sender).await {
                        return false;
                    }
                } else {
                    let outcome = boundary_outcome(row, disposition, line_binding(&binding));
                    if let Some(stopping) = outcome.as_blocker() {
                        *blocker = Some(stopping);
                    }
                    if !emit_ordered(outcome, results, sender).await {
                        return false;
                    }
                }
                submitted.cursor += 1;
            }
            Ok(StreamB2PrefixOutcome::AckUnknown(unknown)) => {
                let count = unknown.rows.len();
                let outcomes = match ack_unknown_outcomes(&submitted, &unknown) {
                    Ok(outcomes) => outcomes,
                    Err(error) => {
                        let outcome = StreamLineOutcome::new(
                            first_ordinal,
                            StreamLineDetail::Invalid {
                                message: error.to_string(),
                            },
                        );
                        if !emit_ordered(outcome, results, sender).await {
                            return false;
                        }
                        return false;
                    }
                };
                if count == 0 || outcomes.len() != count || count > end - submitted.cursor {
                    let outcome = StreamLineOutcome::new(
                        first_ordinal,
                        StreamLineDetail::Invalid {
                            message: "B2 returned an invalid ambiguous-prefix length".to_string(),
                        },
                    );
                    let _ = emit_ordered(outcome, results, sender).await;
                    return false;
                }
                if let Some(first) = outcomes.first().and_then(StreamLineOutcome::as_blocker) {
                    *blocker = Some(first);
                }
                for outcome in outcomes {
                    if !emit_ordered(outcome, results, sender).await {
                        return false;
                    }
                }
                submitted.cursor += count;
            }
            Ok(StreamB2PrefixOutcome::Refused {
                caller_ordinal,
                binding,
                error,
            }) => {
                let outcome = if caller_ordinal == first_ordinal {
                    b2_error_outcome(first_ordinal, error).with_binding(line_binding(&binding))
                } else {
                    StreamLineOutcome::new(
                        first_ordinal,
                        StreamLineDetail::Invalid {
                            message: "B2 refusal ordinal did not match request cursor".to_string(),
                        },
                    )
                    .with_binding(line_binding(&binding))
                };
                if let Some(stopping) = outcome.as_blocker() {
                    *blocker = Some(stopping);
                }
                if !emit_ordered(outcome, results, sender).await {
                    return false;
                }
                submitted.cursor += 1;
            }
            Err(error) => {
                let outcome = b2_error_outcome(first_ordinal, error);
                if let Some(stopping) = outcome.as_blocker() {
                    *blocker = Some(stopping);
                }
                if !emit_ordered(outcome, results, sender).await {
                    return false;
                }
                submitted.cursor += 1;
            }
        }
    }
    true
}

fn line_binding(binding: &StreamB2BindingProof) -> StreamLineBinding {
    StreamLineBinding {
        enrollment_id: binding.enrollment_id.clone(),
        shard_id: binding.shard_id.clone(),
        writer_epoch: binding.writer_epoch,
    }
}

fn durable_outcome(
    row: &PendingRow,
    ack: StreamTokenAdmissionAck,
    binding: StreamLineBinding,
) -> StreamLineOutcome {
    if ack.disposition != StreamTokenDisposition::Present
        || ack.terminal_correction.is_some()
        || ack.terminal_dead_letter.is_some()
    {
        return StreamLineOutcome::new(
            row.ordinal,
            StreamLineDetail::Invalid {
                message: "new stream admission returned terminal token evidence".to_string(),
            },
        )
        .with_binding(binding);
    }
    let admission_attempt_id = match &ack.origin {
        StreamRowOrigin::Admission {
            admission_attempt_id,
            ..
        } => admission_attempt_id.clone(),
        StreamRowOrigin::Correction { .. } => {
            return StreamLineOutcome::new(
                row.ordinal,
                StreamLineDetail::Invalid {
                    message: "stream admission returned a correction origin".to_string(),
                },
            )
            .with_binding(binding);
        }
    };
    StreamLineOutcome::new(
        row.ordinal,
        StreamLineDetail::Durable {
            occurrence: row.occurrence(),
            confirmed_stream_token: ack.stream_token.to_string(),
            admission_attempt_id,
        },
    )
    .with_current_evidence(binding, ack.origin)
}

fn boundary_outcome(
    row: &PendingRow,
    disposition: StreamB2BoundaryDisposition,
    binding: StreamLineBinding,
) -> StreamLineOutcome {
    match disposition {
        StreamB2BoundaryDisposition::AlreadyDurable(ack) => match ack.disposition {
            StreamTokenDisposition::Present => {
                if ack.terminal_correction.is_some() || ack.terminal_dead_letter.is_some() {
                    return StreamLineOutcome::new(
                        row.ordinal,
                        StreamLineDetail::Invalid {
                            message: "PRESENT token carried terminal correction evidence"
                                .to_string(),
                        },
                    )
                    .with_binding(binding);
                }
                StreamLineOutcome::new(
                    row.ordinal,
                    StreamLineDetail::AlreadyDurable {
                        occurrence: row.occurrence(),
                        confirmed_stream_token: ack.stream_token.to_string(),
                    },
                )
                .with_current_evidence(binding, ack.origin)
            }
            StreamTokenDisposition::Withdrawn => {
                if ack.terminal_dead_letter.is_some() {
                    return StreamLineOutcome::new(
                        row.ordinal,
                        StreamLineDetail::Invalid {
                            message: "WITHDRAWN token carried dead-letter evidence".to_string(),
                        },
                    )
                    .with_binding(binding);
                }
                let Some(terminal_correction) = ack.terminal_correction else {
                    return StreamLineOutcome::new(
                        row.ordinal,
                        StreamLineDetail::Invalid {
                            message: "WITHDRAWN token omitted terminal correction evidence"
                                .to_string(),
                        },
                    )
                    .with_binding(binding);
                };
                StreamLineOutcome::new(
                    row.ordinal,
                    StreamLineDetail::Withdrawn {
                        occurrence: row.occurrence(),
                        confirmed_stream_token: ack.stream_token.to_string(),
                        message: "the current occurrence is withdrawn".to_string(),
                    },
                )
                .with_withdrawn_evidence(binding, ack.origin, terminal_correction)
            }
            StreamTokenDisposition::DeadLettered => {
                if ack.terminal_correction.is_some() {
                    return StreamLineOutcome::new(
                        row.ordinal,
                        StreamLineDetail::Invalid {
                            message: "DEAD_LETTERED token carried withdrawal evidence".to_string(),
                        },
                    )
                    .with_binding(binding);
                }
                let Some(terminal_dead_letter) = ack.terminal_dead_letter else {
                    return StreamLineOutcome::new(
                        row.ordinal,
                        StreamLineDetail::Invalid {
                            message: "DEAD_LETTERED token omitted terminal evidence".to_string(),
                        },
                    )
                    .with_binding(binding);
                };
                let operation_id = terminal_dead_letter.object.fold_operation_id.clone();
                StreamLineOutcome::new(
                    row.ordinal,
                    StreamLineDetail::DeadLettered {
                        occurrence: row.occurrence(),
                        current_token: ack.stream_token.to_string(),
                        operation_id,
                        message: "the current occurrence was diverted by graph validation"
                            .to_string(),
                    },
                )
                .with_dead_letter_evidence(
                    binding,
                    ack.origin,
                    terminal_dead_letter,
                )
            }
        },
        StreamB2BoundaryDisposition::BindingChanged {
            current_stream_incarnation_id,
            ..
        } => StreamLineOutcome::new(
            row.ordinal,
            StreamLineDetail::StreamBindingChanged {
                current_stream_incarnation_id: Some(current_stream_incarnation_id),
                logical_id: Some(row.logical_id.to_string()),
                message: "stream binding changed".to_string(),
            },
        )
        .with_binding(binding),
        StreamB2BoundaryDisposition::SequenceConflict {
            logical_id,
            current_token,
            ..
        } => StreamLineOutcome::new(
            row.ordinal,
            StreamLineDetail::StreamSequenceConflict {
                current_stream_incarnation_id: Some(row.envelope.stream_incarnation_id.clone()),
                logical_id,
                current_token,
                message: "stream predecessor is not current".to_string(),
            },
        )
        .with_binding(binding),
        StreamB2BoundaryDisposition::IdempotencyConflict {
            logical_id,
            current_token,
            ..
        } => StreamLineOutcome::new(
            row.ordinal,
            StreamLineDetail::StreamIdempotencyConflict {
                current_stream_incarnation_id: Some(row.envelope.stream_incarnation_id.clone()),
                logical_id,
                current_token,
                message: "stream occurrence was reused with different content or actor".to_string(),
            },
        )
        .with_binding(binding),
    }
}

fn ack_unknown_outcomes(
    submitted: &SubmittedRun,
    unknown: &StreamB2AckUnknown,
) -> Result<Vec<StreamLineOutcome>> {
    unknown
        .rows
        .iter()
        .map(|ambiguous| {
            let row = submitted
                .rows
                .iter()
                .find(|row| row.ordinal == ambiguous.caller_ordinal)
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "AckUnknown ordinal {} is outside the submitted request run",
                        ambiguous.caller_ordinal
                    ))
                })?;
            Ok(StreamLineOutcome::new(
                row.ordinal,
                StreamLineDetail::AckUnknown {
                    occurrence: row.occurrence(),
                    unconfirmed_candidate_token: ambiguous.unconfirmed_candidate_token.to_string(),
                    admission_attempt_id: ambiguous.admission_attempt_id.clone(),
                    message: unknown.reason.clone(),
                },
            )
            .with_binding(line_binding(&unknown.binding)))
        })
        .collect()
}

/// Prove only limits which are intrinsic to this normalized occurrence.
///
/// The inherited request blocker still owns admission. This helper therefore
/// performs no token lookup, queue acquisition, recovery, or put. It builds the
/// exact fresh-occurrence stored row and token/recovery row under the accepted
/// preflight identity. If that single-row shape cannot fit an empty generation
/// or empty token plan, folding/retrying cannot make the input admissible and
/// `stream_input_too_large` must outrank the blocker.
async fn validate_intrinsic_single_row_size(
    db: &Omnigraph,
    table_key: &str,
    contributor_id: &TrustedContributorId,
    sizing: &IntrinsicSizingContext,
    ordinal: u64,
    normalized: &NormalizedStreamJsonRow,
) -> Result<()> {
    let batch = db
        .storage()
        .prepare_keyed_write_batch(table_key, normalized.batch.clone())
        .await?;
    let canonical_payload = super::canonical_stream_payload_v1(&batch, 0)?;
    let payload_digest = PayloadDigest::derive(&PayloadDigestInput {
        identity: sizing.identity,
        accepted_schema_hash: &sizing.accepted_schema_hash,
        canonical_payload: &canonical_payload,
    })
    .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    let request = AdmissionRequest {
        identity: sizing.identity,
        logical_id: normalized.logical_id.clone(),
        envelope: normalized.envelope.clone(),
        contributor_id: contributor_id.clone(),
        payload_digest,
    };
    request
        .validate()
        .map_err(|error| OmniError::manifest(error.to_string()))?;
    let candidate = request
        .candidate_token()
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    let metadata = TrustedStreamRowMetadata::new_admission(
        &request,
        candidate,
        request.envelope.predecessor_token,
        1,
        STREAM_SIZING_ATTEMPT_ID.to_string(),
        ordinal,
    )
    .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    let authority = StreamTokenAuthorityRow::from_present_metadata(
        sizing.identity,
        normalized.logical_id.clone(),
        sizing.enrollment_id.clone(),
        &metadata,
    )
    .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    validate_stream_token_plan_bounds(std::slice::from_ref(&authority))?;

    let hidden = build_trusted_stream_metadata_array(&[Some(metadata)])
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    let source_schema = batch.schema();
    let mut fields = source_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(crate::db::manifest::stream_token::trusted_stream_metadata_field());
    let schema = Arc::new(ArrowSchema::new_with_metadata(
        fields,
        source_schema.metadata().clone(),
    ));
    let mut columns = batch.columns().to_vec();
    columns.push(hidden);
    let stored = RecordBatch::try_new(schema, columns)
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    let stored_bytes =
        b1_logical_batch_bytes(&stored).map_err(|error| OmniError::Lance(error.to_string()))?;
    if stored_bytes > B1_MAX_GENERATION_ARROW_BYTES {
        return Err(OmniError::resource_limit(
            "stream_input_arrow_bytes",
            B1_MAX_GENERATION_ARROW_BYTES,
            stored_bytes,
        ));
    }
    Ok(())
}

fn intrinsic_input_too_large(error: &OmniError) -> Option<(u64, u64)> {
    match error {
        OmniError::ResourceLimitExceeded {
            resource,
            limit,
            actual,
        } if matches!(
            resource.as_str(),
            "stream raw JSON bytes"
                | "stream_json_structural_slots"
                | "stream_input_arrow_bytes"
                | "stream_canonical_payload_bytes"
                | "stream_token_projection_arrow_bytes"
                | "stream_token_recovery_json_bytes"
        ) =>
        {
            Some((*limit, *actual))
        }
        _ => None,
    }
}

fn adapter_error_outcome(ordinal: u64, error: OmniError) -> StreamLineOutcome {
    match intrinsic_input_too_large(&error) {
        Some((limit, actual)) => StreamLineOutcome::new(
            ordinal,
            StreamLineDetail::StreamInputTooLarge { limit, actual },
        ),
        None => StreamLineOutcome::new(
            ordinal,
            StreamLineDetail::Invalid {
                message: error.to_string(),
            },
        ),
    }
}

fn b2_error_outcome(ordinal: u64, error: OmniError) -> StreamLineOutcome {
    if let Some((limit, actual)) = intrinsic_input_too_large(&error) {
        return StreamLineOutcome::new(
            ordinal,
            StreamLineDetail::StreamInputTooLarge { limit, actual },
        );
    }
    let detail = match error {
        OmniError::FoldRequired { .. } => StreamLineDetail::StreamFoldRequired {
            message: error.to_string(),
        },
        OmniError::ResourceLimitExceeded { .. } => StreamLineDetail::StreamBackpressure {
            message: error.to_string(),
        },
        OmniError::RecoveryRequired {
            operation_id,
            reason,
        } => StreamLineDetail::RecoveryRequired {
            operation_id,
            message: reason,
        },
        OmniError::StreamBindingChanged {
            current_stream_incarnation_id,
            ..
        } => StreamLineDetail::StreamBindingChanged {
            current_stream_incarnation_id: Some(current_stream_incarnation_id),
            logical_id: None,
            message: "stream binding changed".to_string(),
        },
        OmniError::StreamLifecycleChanged { .. } => StreamLineDetail::StreamLifecycleChanged {
            current_stream_incarnation_id: None,
            message: error.to_string(),
        },
        OmniError::StreamingAuthorityMismatch { .. }
        | OmniError::StreamingRequiresClusterRuntime { .. }
        | OmniError::StreamingContentOperationUnsupported { .. } => {
            StreamLineDetail::StreamAuthorityChanged {
                current_stream_incarnation_id: None,
                message: error.to_string(),
            }
        }
        OmniError::StreamSequenceConflict {
            logical_id,
            current_token,
            ..
        } => StreamLineDetail::StreamSequenceConflict {
            current_stream_incarnation_id: None,
            logical_id,
            current_token,
            message: "stream predecessor is not current".to_string(),
        },
        OmniError::StreamIdempotencyConflict {
            logical_id,
            current_token,
            ..
        } => StreamLineDetail::StreamIdempotencyConflict {
            current_stream_incarnation_id: None,
            logical_id,
            current_token,
            message: "stream occurrence was reused with different content or actor".to_string(),
        },
        OmniError::Manifest(manifest) => match manifest.details {
            Some(ManifestConflictDetails::StreamLifecycleConflict { lifecycle, .. }) => {
                StreamLineDetail::StreamResumeRequired {
                    current_stream_incarnation_id: None,
                    message: format!("stream lifecycle is {lifecycle}"),
                }
            }
            Some(ManifestConflictDetails::ReadSetChanged { .. }) => {
                StreamLineDetail::StreamAuthorityChanged {
                    current_stream_incarnation_id: None,
                    message: manifest.message,
                }
            }
            _ => StreamLineDetail::StreamAuthorityChanged {
                current_stream_incarnation_id: None,
                message: manifest.message,
            },
        },
        other => StreamLineDetail::StreamBackpressure {
            message: other.to_string(),
        },
    };
    StreamLineOutcome::new(ordinal, detail)
}

fn uninvoked_local(ordinal: u64) -> StreamLineOutcome {
    StreamLineOutcome::new(
        ordinal,
        StreamLineDetail::StreamBackpressure {
            message: "line was not physically invoked".to_string(),
        },
    )
}

async fn emit_ordered(
    outcome: StreamLineOutcome,
    results: &mut OrderedResultBuffer,
    sender: &tokio::sync::mpsc::Sender<StreamLineOutcome>,
) -> bool {
    if results.insert(outcome).is_err() {
        return false;
    }
    while let Some(ready) = results.pop_ready() {
        if sender.send(ready).await.is_err() {
            return false;
        }
    }
    true
}

#[cfg(feature = "failpoints")]
fn graph_stream_line_outcome_json(outcome: &GraphStreamLineOutcome) -> serde_json::Value {
    let line = &outcome.outcome;
    let status = graph_status_name(line.status());
    let blocking = line.blocking.as_ref();
    let mut value = serde_json::json!({
        "ordinal": line.ordinal,
        "status": status,
        "scope": graph_status_scope(line.status()),
    });
    let object = value
        .as_object_mut()
        .expect("graph stream result starts as an object");
    if let Some(route) = outcome.route.as_ref() {
        object.insert("kind".to_string(), route.kind.into());
        object.insert("type".to_string(), route.type_name.clone().into());
    }
    if let Some(logical_id) = outcome
        .route
        .as_ref()
        .and_then(|route| route.logical_id.as_deref())
        .or_else(|| line.logical_id())
    {
        object.insert("id".to_string(), logical_id.into());
    }
    if let Some(write_id) = outcome
        .route
        .as_ref()
        .map(|route| route.write_id.as_str())
        .or_else(|| line.write_id())
    {
        object.insert("write_id".to_string(), write_id.into());
    }
    if let Some(token) = line.confirmed_stream_token() {
        object.insert("stream_token".to_string(), token.into());
    }
    if let Some(token) = line.unconfirmed_candidate_token() {
        object.insert("unconfirmed_candidate_token".to_string(), token.into());
    }
    if let Some(token) = line.current_token() {
        object.insert("current_token".to_string(), token.into());
    }
    if let Some(message) = graph_safe_message(line.status()) {
        object.insert("message".to_string(), message.into());
    }
    if let StreamLineDetail::StreamInputTooLarge { limit, actual } = &line.detail {
        object.insert("limit".to_string(), (*limit).into());
        object.insert("actual".to_string(), (*actual).into());
    }
    if let Some(blocking) = blocking {
        object.insert("blocking_ordinal".to_string(), blocking.ordinal.into());
        object.insert(
            "blocking_status".to_string(),
            graph_status_name(blocking.status).into(),
        );
    }
    value
}

#[cfg(feature = "failpoints")]
fn graph_status_name(status: StreamLineStatus) -> &'static str {
    match status {
        StreamLineStatus::StreamBindingChanged
        | StreamLineStatus::StreamLifecycleChanged
        | StreamLineStatus::StreamAuthorityChanged
        | StreamLineStatus::StreamResumeRequired => "stream_authority_changed",
        other => status_name(other),
    }
}

#[cfg(feature = "failpoints")]
fn graph_status_scope(status: StreamLineStatus) -> &'static str {
    match status {
        StreamLineStatus::AckUnknown
        | StreamLineStatus::StreamBindingChanged
        | StreamLineStatus::StreamLifecycleChanged
        | StreamLineStatus::StreamAuthorityChanged
        | StreamLineStatus::StreamResumeRequired
        | StreamLineStatus::StreamFoldRequired
        | StreamLineStatus::StreamBackpressure
        | StreamLineStatus::RecoveryRequired
        | StreamLineStatus::StreamRetryRequired => "graph",
        StreamLineStatus::Durable
        | StreamLineStatus::AlreadyDurable
        | StreamLineStatus::Withdrawn
        | StreamLineStatus::DeadLettered
        | StreamLineStatus::Invalid
        | StreamLineStatus::StreamInputTooLarge
        | StreamLineStatus::StreamSequenceConflict
        | StreamLineStatus::StreamIdempotencyConflict => "row",
    }
}

#[cfg(feature = "failpoints")]
fn graph_safe_message(status: StreamLineStatus) -> Option<&'static str> {
    match status {
        StreamLineStatus::Durable
        | StreamLineStatus::AlreadyDurable
        | StreamLineStatus::StreamInputTooLarge => None,
        StreamLineStatus::AckUnknown => {
            Some("durability acknowledgement is unknown; retry the same logical write")
        }
        StreamLineStatus::Withdrawn => Some("the current logical occurrence is withdrawn"),
        StreamLineStatus::DeadLettered => {
            Some("the current logical occurrence was diverted by graph validation")
        }
        StreamLineStatus::Invalid => Some("row is invalid for the accepted graph schema"),
        StreamLineStatus::StreamBindingChanged
        | StreamLineStatus::StreamLifecycleChanged
        | StreamLineStatus::StreamAuthorityChanged
        | StreamLineStatus::StreamResumeRequired => {
            Some("graph stream authority changed; retry the request")
        }
        StreamLineStatus::StreamSequenceConflict => {
            Some("the predecessor token is not current for this logical row")
        }
        StreamLineStatus::StreamIdempotencyConflict => {
            Some("the write id was reused with different logical content")
        }
        StreamLineStatus::StreamFoldRequired => {
            Some("the graph stream must fold before this row can be retried")
        }
        StreamLineStatus::StreamBackpressure => {
            Some("graph stream capacity is temporarily unavailable")
        }
        StreamLineStatus::RecoveryRequired => {
            Some("graph recovery must complete before ingest can continue")
        }
        StreamLineStatus::StreamRetryRequired => {
            Some("this row was not invoked; retry it after the graph blocker clears")
        }
    }
}

#[cfg(feature = "failpoints")]
fn stream_line_outcome_json(outcome: &StreamLineOutcome) -> serde_json::Value {
    let blocking = outcome.blocking.as_ref();
    let binding = outcome.binding();
    let blocking_binding = blocking.and_then(|blocking| blocking.binding.as_ref());
    let origin = outcome
        .origin()
        .map(|origin| serde_json::to_value(origin).expect("stream origin is serializable"));
    let terminal_correction = outcome.terminal_correction().map(|correction| {
        serde_json::to_value(correction).expect("terminal stream correction is serializable")
    });
    let terminal_dead_letter = outcome.terminal_dead_letter().map(|terminal| {
        serde_json::to_value(terminal).expect("terminal dead-letter evidence is serializable")
    });
    let (limit, actual) = match &outcome.detail {
        StreamLineDetail::StreamInputTooLarge { limit, actual } => (Some(*limit), Some(*actual)),
        _ => (None, None),
    };
    serde_json::json!({
        "ordinal": outcome.ordinal,
        "status": status_name(outcome.status()),
        "stream_token": outcome.confirmed_stream_token(),
        "unconfirmed_candidate_token": outcome.unconfirmed_candidate_token(),
        "current_token": outcome.current_token(),
        "current_stream_incarnation_id": outcome.current_stream_incarnation_id(),
        "logical_id": outcome.logical_id(),
        "write_id": outcome.write_id(),
        "admission_attempt_id": outcome.admission_attempt_id(),
        "origin": origin,
        "enrollment_id": binding.map(|binding| binding.enrollment_id.as_str()),
        "shard_id": binding.map(|binding| binding.shard_id.as_str()),
        "writer_epoch": binding.map(|binding| binding.writer_epoch),
        "terminal_correction": terminal_correction,
        "terminal_dead_letter": terminal_dead_letter,
        "operation_id": outcome.operation_id(),
        "message": outcome.message(),
        "limit": limit,
        "actual": actual,
        "blocking_ordinal": blocking.map(|blocking| blocking.ordinal),
        "blocking_status": blocking.map(|blocking| status_name(blocking.status)),
        "blocking_admission_attempt_id": blocking
            .and_then(|blocking| blocking.admission_attempt_id.as_deref()),
        "blocking_enrollment_id": blocking_binding
            .map(|binding| binding.enrollment_id.as_str()),
        "blocking_shard_id": blocking_binding.map(|binding| binding.shard_id.as_str()),
        "blocking_writer_epoch": blocking_binding.map(|binding| binding.writer_epoch),
    })
}

fn status_name(status: StreamLineStatus) -> &'static str {
    match status {
        StreamLineStatus::Durable => "durable",
        StreamLineStatus::AckUnknown => "ack_unknown",
        StreamLineStatus::AlreadyDurable => "already_durable",
        StreamLineStatus::Withdrawn => "withdrawn",
        StreamLineStatus::DeadLettered => "dead_lettered",
        StreamLineStatus::Invalid => "invalid",
        StreamLineStatus::StreamInputTooLarge => "stream_input_too_large",
        StreamLineStatus::StreamBindingChanged => "stream_binding_changed",
        StreamLineStatus::StreamLifecycleChanged => "stream_lifecycle_changed",
        StreamLineStatus::StreamAuthorityChanged => "stream_authority_changed",
        StreamLineStatus::StreamSequenceConflict => "stream_sequence_conflict",
        StreamLineStatus::StreamIdempotencyConflict => "stream_idempotency_conflict",
        StreamLineStatus::StreamResumeRequired => "stream_resume_required",
        StreamLineStatus::StreamFoldRequired => "stream_fold_required",
        StreamLineStatus::StreamBackpressure => "stream_backpressure",
        StreamLineStatus::RecoveryRequired => "recovery_required",
        StreamLineStatus::StreamRetryRequired => "stream_retry_required",
    }
}

const _: () = assert!(STREAM_REQUEST_MAX_RESULT_STATUSES >= STREAM_RESULT_CHANNEL_CAPACITY);

#[cfg(all(test, feature = "failpoints"))]
mod tests {
    use super::*;

    fn test_request_handle(
        receiver: tokio::sync::mpsc::Receiver<StreamLineOutcome>,
        task: tokio::task::JoinHandle<()>,
    ) -> StreamRequestHandle {
        let registry = super::super::stream_request::StreamRequestRegistry::for_root(&format!(
            "memory://stream-request-task/{}",
            ulid::Ulid::new()
        ));
        let permit = registry
            .try_acquire("agent:request-task")
            .expect("test request permit");
        let ownership = Arc::new(TransportOwnership { _permit: permit });
        StreamRequestHandle {
            receiver,
            task: Some(task),
            _transport: ownership,
        }
    }

    #[tokio::test]
    async fn recv_drains_buffered_outcomes_then_propagates_request_task_panics() {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(async move {
            sender
                .send(StreamLineOutcome::new(
                    0,
                    StreamLineDetail::Invalid {
                        message: "buffered before panic".to_string(),
                    },
                ))
                .await
                .expect("test receiver remains open");
            panic!("intentional request-task panic");
        });
        let mut handle = test_request_handle(receiver, task);

        let outcome = handle
            .recv()
            .await
            .expect("the buffered outcome must precede task settlement")
            .expect("one buffered outcome");
        assert_eq!(outcome.ordinal, 0);
        let error = handle
            .recv()
            .await
            .expect_err("task panic must not become clean EOF");
        assert!(
            error.to_string().contains("stream request task failed"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn recv_reports_clean_fused_eof_after_request_task_completion() {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(async move {
            drop(sender);
        });
        let mut handle = test_request_handle(receiver, task);

        assert!(
            handle
                .recv()
                .await
                .expect("clean request completion")
                .is_none()
        );
        assert!(
            handle
                .recv()
                .await
                .expect("clean EOF remains fused")
                .is_none()
        );
    }

    #[tokio::test]
    async fn cancelling_terminal_recv_retains_request_task_join_ownership() {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let (closed_sender, closed_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            drop(sender);
            closed_sender.send(()).expect("signal closed channel");
            let _ = release_receiver.await;
        });
        let mut handle = test_request_handle(receiver, task);
        closed_receiver.await.expect("request sender closed");

        let mut first_recv = Box::pin(handle.recv());
        assert!(matches!(
            futures::poll!(first_recv.as_mut()),
            std::task::Poll::Pending
        ));
        drop(first_recv);

        let mut second_recv = Box::pin(handle.recv());
        assert!(
            matches!(
                futures::poll!(second_recv.as_mut()),
                std::task::Poll::Pending
            ),
            "a cancelled terminal receive must not detach the request task"
        );
        release_sender.send(()).expect("release request task");
        assert!(
            second_recv
                .await
                .expect("retained task joins cleanly")
                .is_none()
        );
    }

    #[tokio::test]
    async fn cancellation_seam_propagates_request_task_panics() {
        let (_sender, receiver) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(async {
            panic!("intentional request-task panic");
        });
        let handle = test_request_handle(receiver, task);

        let error = handle
            .cancel_and_wait()
            .await
            .expect_err("the cancellation proof must not swallow a task panic");
        assert!(
            error.to_string().contains("stream request task failed"),
            "{error:?}"
        );
    }
}
