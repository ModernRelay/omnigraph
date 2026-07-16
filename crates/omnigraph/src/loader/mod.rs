use std::collections::HashMap;

use std::io::{BufRead, BufReader, Cursor};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
    builder::{
        ArrayBuilder, BooleanBuilder, Date32Builder, Date64Builder, FixedSizeListBuilder,
        Float32Builder, Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
        UInt32Builder, UInt64Builder,
    },
};
use arrow_schema::DataType;
use base64::Engine;
use lance::blob::BlobArrayBuilder;
use omnigraph_compiler::catalog::NodeType;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::db::Omnigraph;
use crate::error::{OmniError, Result};
use crate::exec::staging::{MutationStaging, PendingMode};
use crate::storage_layer::KEYED_WRITE_MAX_BYTES;

/// Result of a load operation.
#[derive(Debug, Clone, Default)]
pub struct LoadResult {
    /// Branch the load landed on (`"main"` when no branch was given).
    pub branch: String,
    /// Base branch a fork was requested from (the `base` parameter of
    /// `load_as`), recorded verbatim even when the target branch already
    /// existed and no fork happened.
    pub base_branch: Option<String>,
    /// True when this load created `branch` by forking it from `base_branch`.
    pub branch_created: bool,
    pub nodes_loaded: HashMap<String, usize>,
    pub edges_loaded: HashMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestTableResult {
    pub table_key: String,
    pub rows_loaded: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestResult {
    pub branch: String,
    pub base_branch: String,
    pub branch_created: bool,
    pub mode: LoadMode,
    pub tables: Vec<IngestTableResult>,
}

/// Load mode for data ingestion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadMode {
    /// Overwrite existing data.
    Overwrite,
    /// Append to existing data.
    Append,
    /// Merge by `id` key (upsert).
    Merge,
}

/// Convenience: load JSONL data onto the database handle's *active branch*
/// (`main` when unbound). Equivalent to `db.load(active_branch, data, mode)`;
/// use `Omnigraph::load`/`load_as` directly when targeting an explicit branch
/// or when fork-from-base semantics are needed.
pub async fn load_jsonl(db: &Omnigraph, data: &str, mode: LoadMode) -> Result<LoadResult> {
    let current_branch = db.active_branch().await;
    let branch = current_branch.as_deref().unwrap_or("main");
    db.load(branch, data, mode).await
}

/// Convenience: like [`load_jsonl`] but reading from a file path.
pub async fn load_jsonl_file(db: &Omnigraph, path: &str, mode: LoadMode) -> Result<LoadResult> {
    let current_branch = db.active_branch().await;
    let branch = current_branch.as_deref().unwrap_or("main");
    db.load_file(branch, path, mode).await
}

impl Omnigraph {
    #[deprecated(
        note = "use `load_as` with an explicit `base` instead; the ingest family will be removed in a future release"
    )]
    pub async fn ingest(
        &self,
        branch: &str,
        from: Option<&str>,
        data: &str,
        mode: LoadMode,
    ) -> Result<IngestResult> {
        #[allow(deprecated)]
        self.ingest_as(branch, from, data, mode, None).await
    }

    /// Deprecated shim over the unified `load_as`. Preserves the historical
    /// ingest contract exactly: `from: None` means fork from `main`, and the
    /// base branch is recorded in the result even when the target branch
    /// already existed (no fork happened).
    #[deprecated(
        note = "use `load_as` with an explicit `base` instead; the ingest family will be removed in a future release"
    )]
    pub async fn ingest_as(
        &self,
        branch: &str,
        from: Option<&str>,
        data: &str,
        mode: LoadMode,
        actor_id: Option<&str>,
    ) -> Result<IngestResult> {
        let result = self
            .load_as(branch, Some(from.unwrap_or("main")), data, mode, actor_id)
            .await?;
        Ok(IngestResult {
            branch: result.branch.clone(),
            base_branch: result
                .base_branch
                .clone()
                .unwrap_or_else(|| "main".to_string()),
            branch_created: result.branch_created,
            mode,
            tables: result.to_ingest_tables(),
        })
    }

    #[deprecated(
        note = "use `load_file_as` with an explicit `base` instead; the ingest family will be removed in a future release"
    )]
    pub async fn ingest_file(
        &self,
        branch: &str,
        from: Option<&str>,
        path: &str,
        mode: LoadMode,
    ) -> Result<IngestResult> {
        #[allow(deprecated)]
        self.ingest_file_as(branch, from, path, mode, None).await
    }

    #[deprecated(
        note = "use `load_file_as` with an explicit `base` instead; the ingest family will be removed in a future release"
    )]
    pub async fn ingest_file_as(
        &self,
        branch: &str,
        from: Option<&str>,
        path: &str,
        mode: LoadMode,
        actor_id: Option<&str>,
    ) -> Result<IngestResult> {
        let data = std::fs::read_to_string(path).map_err(OmniError::Io)?;
        #[allow(deprecated)]
        self.ingest_as(branch, from, &data, mode, actor_id).await
    }

    pub async fn load(&self, branch: &str, data: &str, mode: LoadMode) -> Result<LoadResult> {
        self.load_as(branch, None, data, mode, None).await
    }

    /// Load JSONL data onto `branch`.
    ///
    /// `base` selects the branch-creation behavior: with `Some(base)`, a
    /// missing target branch is forked from `base` first (the former
    /// `ingest` semantics); with `None`, the target branch must already
    /// exist — staging fails on an unknown branch when it resolves the
    /// manifest snapshot, so a typo'd branch name can never create one.
    pub async fn load_as(
        &self,
        branch: &str,
        base: Option<&str>,
        data: &str,
        mode: LoadMode,
        actor_id: Option<&str>,
    ) -> Result<LoadResult> {
        // Engine-layer policy gate (MR-722 fan-out / PR #3). Scope is
        // `Branch(branch)` to match the HTTP-layer Change convention.
        // When a fork happens below, `branch_create_from_as` additionally
        // checks `BranchCreate` — both authorities are genuinely needed
        // for "load into a fresh branch", so the layered check is
        // correct, not redundant.
        self.enforce(
            omnigraph_policy::PolicyAction::Change,
            &omnigraph_policy::ResourceScope::Branch(branch.to_string()),
            actor_id,
        )?;
        // Reject internal `__run__*` / system-prefixed branches at the
        // public write boundary. Direct-publish paths assert this
        // explicitly so a caller can't write to legacy or system
        // staging branches by passing the prefix verbatim.
        crate::db::ensure_public_branch_ref(branch, "load")?;
        // Branch convention: `None` represents `main`. Re-normalizing to
        // `Some("main")` here would route the publisher commit through a
        // separate coordinator (the cross-branch path in
        // `commit_prepared_updates_on_branch_with_expected`) and leave
        // `self.coordinator` with a stale manifest snapshot.
        let requested = Self::normalize_branch_name(branch)?;
        let base_branch = match base {
            Some(base) => {
                Some(Self::normalize_branch_name(base)?.unwrap_or_else(|| "main".to_string()))
            }
            None => None,
        };
        // Schema/catalog authority is captured once via the `WriteTxn` (plus its
        // cheap trailing identity-marker fence); the only second full validation
        // is the required pre-effect recheck under gates. Per-table resolution
        // performs no additional contract reads.
        //
        // Stage A precedes both an implicit target-branch fork and data staging.
        // The target branch and an explicit base are read/write authority for the
        // operation, so an unresolved intent on either closes the barrier. The
        // helper folds `Some("main")` to main's canonical `None` identity.
        let mut recovery_branches = vec![requested.as_deref()];
        if base.is_some() {
            // `base_branch` retains `Some("main")` for the result DTO; the
            // barrier helper canonicalizes it to main's `None` identity.
            recovery_branches.push(base_branch.as_deref());
        }
        self.heal_pending_recovery_sidecars_for_write(&recovery_branches)
            .await?;
        // Fork-if-missing only when a base branch was explicitly given.
        // `requested == None` is `main`, which always exists.
        let mut branch_created = false;
        if let (Some(target), Some(base_name)) = (requested.as_deref(), base_branch.as_deref()) {
            let exists = self.branch_list().await?.iter().any(|name| name == target);
            if !exists {
                // Thread the actor through to the implicit BranchCreate so
                // policy decisions match what an explicit `branch_create_from_as`
                // call would see. Calling the no-actor variant here would
                // bypass BranchCreate enforcement when policy is installed —
                // the footgun guard catches that case too, but threading is
                // the correct fix.
                self.branch_create_from_as(
                    crate::db::ReadTarget::branch(base_name),
                    target,
                    actor_id,
                )
                .await?;
                branch_created = true;
            }
        }
        // Direct-to-target writes: no Run state machine, no `__run__` staging
        // branch. Cross-table OCC is enforced by the publisher's
        // `expected_table_versions` CAS inside the load attempt.
        let mut result = self
            .load_direct_on_branch(requested.as_deref(), data, mode, actor_id)
            .await?;
        result.branch = requested.unwrap_or_else(|| "main".to_string());
        result.base_branch = base_branch;
        result.branch_created = branch_created;
        Ok(result)
    }

    pub async fn load_file(&self, branch: &str, path: &str, mode: LoadMode) -> Result<LoadResult> {
        self.load_file_as(branch, None, path, mode, None).await
    }

    /// Read a file into memory and delegate to `load_as`. Used by the
    /// CLI's `omnigraph load` so file-path-based writes flow through
    /// the same engine-layer policy gate as in-memory `load_as` calls.
    pub async fn load_file_as(
        &self,
        branch: &str,
        base: Option<&str>,
        path: &str,
        mode: LoadMode,
        actor_id: Option<&str>,
    ) -> Result<LoadResult> {
        let data = std::fs::read_to_string(path).map_err(OmniError::Io)?;
        self.load_as(branch, base, &data, mode, actor_id).await
    }

    async fn load_direct_on_branch(
        &self,
        branch: Option<&str>,
        data: &str,
        mode: LoadMode,
        actor_id: Option<&str>,
    ) -> Result<LoadResult> {
        load_jsonl_data(self, branch, data, mode, actor_id).await
    }
}

impl LoadMode {
    pub fn as_str(self) -> &'static str {
        match self {
            LoadMode::Overwrite => "overwrite",
            LoadMode::Append => "append",
            LoadMode::Merge => "merge",
        }
    }
}

impl LoadResult {
    pub fn to_ingest_tables(&self) -> Vec<IngestTableResult> {
        let mut tables = self
            .nodes_loaded
            .iter()
            .map(|(type_name, rows_loaded)| IngestTableResult {
                table_key: format!("node:{type_name}"),
                rows_loaded: *rows_loaded,
            })
            .chain(
                self.edges_loaded
                    .iter()
                    .map(|(edge_name, rows_loaded)| IngestTableResult {
                        table_key: format!("edge:{edge_name}"),
                        rows_loaded: *rows_loaded,
                    }),
            )
            .collect::<Vec<_>>();
        tables.sort_by(|a, b| a.table_key.cmp(&b.table_key));
        tables
    }
}

async fn load_jsonl_data(
    db: &Omnigraph,
    branch: Option<&str>,
    data: &str,
    mode: LoadMode,
    actor_id: Option<&str>,
) -> Result<LoadResult> {
    const MAX_PRE_EFFECT_REPREPARES: usize = 32;

    // Every public load entry point already owns a stable `&str` payload
    // (`load_file_as` reads its file once). Replay that slice directly on a
    // pre-effect retry; copying it into a second raw byte buffer would double
    // peak input memory before the parser's per-type materialization.
    let retryable = matches!(mode, LoadMode::Append | LoadMode::Merge);
    for attempt in 0..=MAX_PRE_EFFECT_REPREPARES {
        let replay = BufReader::new(Cursor::new(data.as_bytes()));
        match load_jsonl_reader_once(db, branch, replay, mode, actor_id).await {
            Err(err)
                if retryable
                    && err.is_read_set_changed()
                    && attempt < MAX_PRE_EFFECT_REPREPARES =>
            {
                tracing::debug!(
                    attempt = attempt + 1,
                    branch = branch.unwrap_or("main"),
                    "prepared load authority changed before effects; repreparing"
                );
                db.refresh().await?;
            }
            result => return result,
        }
    }
    unreachable!("bounded load retry loop always returns")
}

async fn load_jsonl_reader_once<R: BufRead>(
    db: &Omnigraph,
    branch: Option<&str>,
    reader: R,
    mode: LoadMode,
    actor_id: Option<&str>,
) -> Result<LoadResult> {
    // Capture the manifest/schema authority before interpreting any input. The
    // catalog rides the WriteTxn and was built from the exact accepted IR named
    // by its schema token; a long-lived handle's global catalog may legitimately
    // lag a schema apply completed through another handle.
    let txn = db.open_write_txn(branch).await?;
    let catalog = Arc::clone(&txn.catalog);
    let snapshot = txn.base.clone();

    // Phase 1: Parse all lines, spool into per-type collections
    let mut node_rows: HashMap<String, Vec<JsonValue>> = HashMap::new();
    let mut edge_rows: HashMap<String, Vec<(String, String, JsonValue)>> = HashMap::new();
    let mut keyed_input_budget: HashMap<String, (usize, u64)> = HashMap::new();
    let bounded_keyed_input = matches!(mode, LoadMode::Append | LoadMode::Merge);

    // Parse a stream of JSON values. Accepts both compact JSONL (one object
    // per line) and pretty-printed JSON where a single object spans multiple
    // lines — serde's streaming deserializer treats any whitespace (including
    // newlines) between top-level values as a separator.
    for (idx, parsed) in serde_json::Deserializer::from_reader(reader)
        .into_iter::<JsonValue>()
        .enumerate()
    {
        let record_num = idx + 1;
        let mut value: JsonValue = parsed.map_err(|e| {
            OmniError::manifest(format!("invalid JSON at record {}: {}", record_num, e))
        })?;

        if let Some(type_name) = value
            .get("type")
            .and_then(|v| v.as_str())
            .map(str::to_string)
        {
            if !catalog.node_types.contains_key(&type_name) {
                return Err(OmniError::manifest(format!(
                    "record {}: unknown node type '{}'",
                    record_num, type_name
                )));
            }
            let data = value
                .get_mut("data")
                .map(JsonValue::take)
                .unwrap_or(JsonValue::Object(serde_json::Map::new()));
            if bounded_keyed_input {
                account_keyed_json_row(
                    &format!("node:{type_name}"),
                    &data,
                    0,
                    &mut keyed_input_budget,
                )?;
            }
            node_rows.entry(type_name).or_default().push(data);
        } else if let Some(edge_name) = value
            .get("edge")
            .and_then(|v| v.as_str())
            .map(str::to_string)
        {
            if catalog.lookup_edge_by_name(&edge_name).is_none() {
                return Err(OmniError::manifest(format!(
                    "record {}: unknown edge type '{}'",
                    record_num, edge_name
                )));
            }
            let from = value
                .get("from")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    OmniError::manifest(format!("record {}: edge missing 'from'", record_num))
                })?
                .to_string();
            let to = value
                .get("to")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    OmniError::manifest(format!("record {}: edge missing 'to'", record_num))
                })?
                .to_string();
            let data = value
                .get_mut("data")
                .map(JsonValue::take)
                .unwrap_or(JsonValue::Object(serde_json::Map::new()));
            let canonical = catalog
                .lookup_edge_by_name(&edge_name)
                .unwrap()
                .name
                .clone();
            if bounded_keyed_input {
                account_keyed_json_row(
                    &format!("edge:{canonical}"),
                    &data,
                    from.len().saturating_add(to.len()),
                    &mut keyed_input_budget,
                )?;
            }
            edge_rows
                .entry(canonical)
                .or_default()
                .push((from, to, data));
        } else {
            return Err(OmniError::manifest(format!(
                "record {}: expected 'type' or 'edge' field",
                record_num
            )));
        }
    }

    // Phase 2: Build per-type RecordBatches and accumulate into the
    // staging pipeline. Batches go into an in-memory accumulator and a
    // single `stage_*` + `commit_staged` per touched table runs at
    // end-of-load — a mid-load failure (RI / cardinality violation) leaves
    // Lance HEAD untouched. `LoadMode::Overwrite` uses Lance's staged
    // `Overwrite` transaction rather than the former truncate-then-append
    // inline path.

    let mut result = LoadResult::default();
    // The branch-wide WriteTxn captured above is threaded through every table
    // open and the manifest publish, so the parsed batches, validation catalog,
    // base snapshot, native branch identity, exact graph head, and schema
    // identity form one immutable authority unit.
    let mut staging = MutationStaging::default();
    let pending_mode = match mode {
        LoadMode::Merge => PendingMode::Upsert,
        // Append mode is a strict exact-id insert. Every physical graph table
        // has `id` as its Lance PK in format v6, including edges and node types
        // without a user-declared @key; no keyed table has an Append side door.
        // Merge mode applies last-write-wins source dedupe before fenced upsert.
        LoadMode::Append => PendingMode::StrictInsert,
        LoadMode::Overwrite => PendingMode::Overwrite,
    };
    // Map LoadMode to the early table-version policy. Append/Merge may stage
    // reclaimable files before the effect gates, then revalidate the complete
    // branch token and fully reprepare on a bounded pre-effect conflict.
    // Overwrite keeps the strict early check because it replaces the image; its
    // later branch-wide mismatch surfaces `ReadSetChanged` without replay.
    let load_op_kind = match mode {
        LoadMode::Append => crate::db::MutationOpKind::Insert,
        LoadMode::Merge => crate::db::MutationOpKind::Merge,
        LoadMode::Overwrite => crate::db::MutationOpKind::SchemaRewrite,
    };

    // Phase 2a: build and validate every node batch up front. Cheap and
    // synchronous — surfaces validation errors before any S3 traffic.
    let mut node_id_remap = TypedNodeIdRemap::default();
    let mut prepared_nodes: Vec<(String, String, RecordBatch, usize)> =
        Vec::with_capacity(node_rows.len());
    for (type_name, rows) in &node_rows {
        let node_type = &catalog.node_types[type_name];
        let batch = build_node_batch(node_type, rows, &mut node_id_remap)?;
        // Validation (value/enum/unique) runs end-of-load via the evaluator.
        let loaded_count = batch.num_rows();
        let table_key = format!("node:{}", type_name);
        let _entry = snapshot
            .entry(&table_key)
            .ok_or_else(|| OmniError::manifest(format!("no manifest entry for {}", table_key)))?;
        prepared_nodes.push((type_name.clone(), table_key, batch, loaded_count));
    }

    // Phase 2b: accumulate every node type in memory. Fragment writes are
    // delayed until after all validation succeeds.
    for (type_name, table_key, batch, loaded_count) in prepared_nodes {
        // The loader only needs the captured expected version (the publisher's
        // CAS fence) for `ensure_path` — it discards the handle. With a
        // non-strict load op (Merge/Append) and a `WriteTxn`, collapse #1 skips
        // the dataset open and returns the pinned base version directly.
        let opened = db
            .open_for_mutation_on_branch(branch, &table_key, load_op_kind, Some(&txn))
            .await?;
        staging.ensure_path(
            &table_key,
            opened.identity,
            opened.full_path,
            opened.table_branch,
            opened.deferred_fork,
            opened.expected_version,
            load_op_kind,
        )?;
        let schema = batch.schema();
        staging.append_batch(&table_key, schema, pending_mode, batch)?;
        result.nodes_loaded.insert(type_name, loaded_count);
    }

    // Phase 2d: build edge batches. Edge referential integrity (and the rest)
    // runs end-of-load via the unified evaluator, below.
    let mut prepared_edges: Vec<(String, String, RecordBatch, usize)> =
        Vec::with_capacity(edge_rows.len());
    for (edge_name, rows) in &edge_rows {
        let edge_type = &catalog.edge_types[edge_name];
        let batch = build_edge_batch(edge_type, rows, &node_id_remap)?;
        // Validation (enum/unique, edge-RI, @card) runs end-of-load via the evaluator.
        let loaded_count = batch.num_rows();
        let table_key = format!("edge:{}", edge_name);
        let _entry = snapshot
            .entry(&table_key)
            .ok_or_else(|| OmniError::manifest(format!("no manifest entry for {}", table_key)))?;
        prepared_edges.push((edge_name.clone(), table_key, batch, loaded_count));
    }

    // Phase 2e: accumulate every edge type. Same dispatch as Phase 2b.
    for (edge_name, table_key, batch, loaded_count) in prepared_edges {
        // Same as the node phase: only the captured expected version is used;
        // collapse #1 skips the open for a non-strict load op under a `WriteTxn`.
        let opened = db
            .open_for_mutation_on_branch(branch, &table_key, load_op_kind, Some(&txn))
            .await?;
        staging.ensure_path(
            &table_key,
            opened.identity,
            opened.full_path,
            opened.table_branch,
            opened.deferred_fork,
            opened.expected_version,
            load_op_kind,
        )?;
        let schema = batch.schema();
        staging.append_batch(&table_key, schema, pending_mode, batch)?;
        result.edges_loaded.insert(edge_name, loaded_count);
    }

    // Phase 3: end-of-load validation — one unified evaluator pass over the
    // accumulated staging (value/enum, uniqueness incl. cross-version, edge-RI,
    // cardinality) against the pinned pre-load base. `Overwrite` validates each
    // touched table as its whole new image (that table's committed view empty),
    // but is PER-TABLE — a table absent from the batch keeps `base`, so an
    // edges-only overwrite still resolves RI against committed nodes;
    // `Append`/`Merge` keep `base` everywhere. This shares the evaluator with the
    // mutation + merge paths, so the surfaces cannot drift.
    let mut changeset = staging.to_changeset();
    // Overwrite replaces each touched table; a committed row absent from the new
    // batch is REMOVED but is not in `to_changeset` (which only records the new
    // batch). Express those removals as `deleted_ids` so edge-RI (path-b) and
    // cardinality recompute against them — e.g. overwriting `node:Person` to drop
    // Bob while a retained `edge:Knows(Alice->Bob)` would otherwise publish an
    // orphan. (Per-table, like the rest of Overwrite handling.)
    if mode == LoadMode::Overwrite {
        let keys: Vec<String> = changeset.keys().cloned().collect();
        for table_key in keys {
            let removed = crate::validate::overwrite_removed_ids(
                &snapshot,
                &table_key,
                changeset.get(&table_key).expect("key from this changeset"),
            )
            .await?;
            if !removed.is_empty() {
                changeset
                    .get_mut(&table_key)
                    .expect("key from this changeset")
                    .deleted_ids = removed;
            }
        }
    }
    let committed = crate::validate::CommittedState::load(&snapshot, mode, &changeset);
    crate::validate::validate_changeset(&changeset, &committed, &catalog).await?;

    // Phase 4: Atomic manifest commit with publisher-level OCC.
    let staged = staging
        .stage_all_with_concurrency(db, branch, load_write_concurrency())
        .await?;
    crate::failpoints::maybe_fail(crate::failpoints::names::MUTATION_POST_STAGE_PRE_EFFECT_GATE)?;
    let lineage_intent = db.new_lineage_intent_for_branch(branch, actor_id).await?;
    // `_queue_guards` holds the root-shared schema → branch → sorted-table
    // gates across manifest publication. This closes same-process
    // interleaving across the v3 sidecar/effect lifetime. The exact publisher
    // token and durable sidecar remain persistent correctness authorities, but
    // these local gates do not expand the documented single-writer-process
    // recovery boundary.
    let crate::exec::staging::CommittedMutation {
        updates,
        expected_versions,
        sidecar_handle,
        guards: _queue_guards,
    } = staged
        .commit_all(
            db,
            branch,
            crate::db::manifest::SidecarKind::Load,
            actor_id,
            &txn,
            &lineage_intent,
        )
        .await?;
    // Same confirmed-effects → publisher boundary as mutations: table HEADs
    // have advanced and the v3 sidecar contains their exact transaction
    // identities, but the graph manifest has not published the result. Reuse
    // the mutation failpoint name so one failpoint pins the shared boundary.
    crate::failpoints::maybe_fail(crate::failpoints::names::MUTATION_POST_FINALIZE_PRE_PUBLISHER)?;
    let publish_result = db
        .commit_updates_on_branch_with_expected(
            branch,
            &updates,
            &expected_versions,
            actor_id,
            &txn,
            lineage_intent,
        )
        .await;
    if let Err(err) = publish_result {
        // Empty loads can still publish lineage but have no table effect and
        // therefore no recovery sidecar. Preserve that publish error instead
        // of manufacturing an "unknown" recovery operation.
        return match sidecar_handle.as_ref() {
            Some(handle) => Err(OmniError::recovery_required(
                handle.operation_id.clone(),
                err.to_string(),
            )),
            None => Err(err),
        };
    }
    // The v3 recovery sidecar protects every independently durable table effect
    // through the one manifest visibility point. Phase C succeeded — clean up
    // best-effort: failing the user here would error out a write that already
    // landed durably; a leftover fixed outcome is idempotently finalized later.
    if let Some(handle) = sidecar_handle {
        if let Err(err) = crate::db::manifest::delete_sidecar(&handle, db.storage_adapter()).await {
            tracing::warn!(
                error = %err,
                operation_id = handle.operation_id.as_str(),
                "recovery sidecar cleanup failed; the next open's recovery sweep will resolve it"
            );
        }
    }

    Ok(result)
}

/// Account a keyed JSON record before retaining it in the per-table parse
/// spool. This is a conservative lower bound on the Arrow payload (string and
/// decoded blob bytes, scalar widths, and list offsets); the exact accumulated
/// Arrow check in `MutationStaging::append_batch` remains the final authority.
/// The early counter prevents an unbounded JSON spool and catches base64 by its
/// decoded size before the decoder allocates a second copy.
fn account_keyed_json_row(
    table_key: &str,
    data: &JsonValue,
    structural_string_bytes: usize,
    budgets: &mut HashMap<String, (usize, u64)>,
) -> Result<()> {
    let entry = budgets.entry(table_key.to_string()).or_insert((0, 0));
    entry.0 = entry
        .0
        .checked_add(1)
        .ok_or_else(|| OmniError::manifest_internal("keyed parsed row count overflow"))?;
    if entry.0 > crate::storage_layer::KEYED_WRITE_MAX_ROWS {
        return Err(OmniError::resource_limit(
            format!("keyed rows for {table_key}"),
            crate::storage_layer::KEYED_WRITE_MAX_ROWS as u64,
            entry.0 as u64,
        ));
    }
    let row_bytes = estimate_json_arrow_bytes(data)?
        .checked_add(
            u64::try_from(structural_string_bytes)
                .map_err(|_| OmniError::manifest_internal("keyed string bytes exceed u64"))?,
        )
        .ok_or_else(|| OmniError::manifest_internal("keyed parsed row bytes overflow"))?;
    entry.1 = entry
        .1
        .checked_add(row_bytes)
        .ok_or_else(|| OmniError::manifest_internal("keyed parsed byte count overflow"))?;
    if entry.1 > KEYED_WRITE_MAX_BYTES {
        return Err(OmniError::resource_limit(
            format!("keyed parsed value bytes for {table_key}"),
            KEYED_WRITE_MAX_BYTES,
            entry.1,
        ));
    }
    Ok(())
}

fn estimate_json_arrow_bytes(value: &JsonValue) -> Result<u64> {
    match value {
        JsonValue::Null => Ok(0),
        JsonValue::Bool(_) => Ok(1),
        // Four bytes avoids rejecting valid Float32/Int32 input early. Wider
        // physical scalars are charged exactly by the later Arrow batch check.
        JsonValue::Number(_) => Ok(4),
        JsonValue::String(value) => {
            let bytes = match value.strip_prefix("base64:") {
                Some(encoded) => base64::decoded_len_estimate(encoded.len()).saturating_sub(
                    encoded
                        .as_bytes()
                        .iter()
                        .rev()
                        .take_while(|&&byte| byte == b'=')
                        .count(),
                ),
                None => value.len(),
            };
            u64::try_from(bytes)
                .map_err(|_| OmniError::manifest_internal("JSON string bytes exceed u64"))
        }
        JsonValue::Array(values) => {
            let offsets = u64::try_from(values.len())
                .map_err(|_| OmniError::manifest_internal("JSON array length exceeds u64"))?
                .checked_add(1)
                .and_then(|count| count.checked_mul(4))
                .ok_or_else(|| OmniError::manifest_internal("JSON array offset bytes overflow"))?;
            values.iter().try_fold(offsets, |bytes, value| {
                bytes
                    .checked_add(estimate_json_arrow_bytes(value)?)
                    .ok_or_else(|| OmniError::manifest_internal("JSON array bytes overflow"))
            })
        }
        // Property names are schema, not per-row Arrow payload. Count values
        // only so the early lower bound does not reject an otherwise-valid
        // wide schema; exact field buffers are charged after batch building.
        JsonValue::Object(values) => values.values().try_fold(0_u64, |bytes, value| {
            bytes
                .checked_add(estimate_json_arrow_bytes(value)?)
                .ok_or_else(|| OmniError::manifest_internal("JSON object bytes overflow"))
        }),
    }
}

/// Legacy exports may carry a physical node id whose spelling predates the
/// current typed canonical renderer. Edges in the same import still name that
/// old id, so rebuilding canonical node ids also requires an endpoint rewrite.
/// Endpoint identity is type-scoped: two concrete node types may legitimately
/// reuse the same old id string and map it differently.
#[derive(Default)]
struct TypedNodeIdRemap {
    by_node_type: HashMap<String, HashMap<String, String>>,
}

impl TypedNodeIdRemap {
    fn record(&mut self, node_type: &str, old_id: &str, canonical_id: &str) -> Result<()> {
        let by_old_id = self.by_node_type.entry(node_type.to_string()).or_default();
        if let Some(existing) = by_old_id.get(old_id) {
            if existing != canonical_id {
                return Err(OmniError::manifest(format!(
                    "node {node_type} explicit id '{old_id}' maps to both canonical @key ids \
                     '{existing}' and '{canonical_id}'; refusing ambiguous edge endpoint remap"
                )));
            }
            return Ok(());
        }
        by_old_id.insert(old_id.to_string(), canonical_id.to_string());
        Ok(())
    }

    fn endpoint<'a>(&'a self, node_type: &str, old_id: &str) -> Option<&'a str> {
        self.by_node_type
            .get(node_type)
            .and_then(|by_old_id| by_old_id.get(old_id))
            .map(String::as_str)
    }
}

fn build_node_batch(
    node_type: &NodeType,
    rows: &[JsonValue],
    node_id_remap: &mut TypedNodeIdRemap,
) -> Result<RecordBatch> {
    let schema = node_type.arrow_schema.clone();
    let row_refs = rows.iter().collect::<Vec<_>>();
    preflight_blob_decode_budget(
        &format!("node:{}", node_type.name),
        node_type.blob_properties.iter().map(String::as_str),
        &row_refs,
    )?;

    // Materialize the typed property columns before deriving physical ids. A
    // scalar @key's identity is the canonical rendering of the value that will
    // actually be stored (including width conversion for F32/I32 and the full
    // U64 range), not an independent rendering of its input JSON token.
    let mut property_columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len() - 1);
    for field in schema.fields().iter().skip(1) {
        if node_type.blob_properties.contains(field.name()) {
            let col = build_blob_column(field.name(), field.is_nullable(), rows)?;
            property_columns.push(col);
        } else {
            let col =
                build_column_from_json(field.name(), field.data_type(), field.is_nullable(), rows)?;
            property_columns.push(col);
        }
    }

    let key_columns = node_type
        .key
        .as_ref()
        .map(|key_properties| {
            key_properties
                .iter()
                .map(|key_prop| {
                    let schema_index = schema.index_of(key_prop).map_err(|_| {
                        OmniError::manifest_internal(format!(
                            "@key property '{}' is missing from node {} Arrow schema",
                            key_prop, node_type.name
                        ))
                    })?;
                    let property_index = schema_index.checked_sub(1).ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "@key property '{}' aliases reserved physical id",
                            key_prop
                        ))
                    })?;
                    property_columns
                        .get(property_index)
                        .cloned()
                        .ok_or_else(|| {
                            OmniError::manifest_internal(format!(
                                "@key property '{}' has invalid schema position {}",
                                key_prop, schema_index
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;

    // Build id column: exact explicit id, canonical typed @key value, or a
    // generated ULID. Export always emits the physical id as a JSON string;
    // when present, a non-string explicit id is malformed instead of being
    // silently ignored and replaced.
    let ids: Vec<String> = rows
        .iter()
        .enumerate()
        .map(|(row_index, row)| {
            let explicit_id = match row.get("id") {
                None => None,
                Some(JsonValue::String(id)) => Some(id.as_str()),
                Some(value) => {
                    return Err(OmniError::manifest(format!(
                        "node {} explicit id must be a string, got {}",
                        node_type.name, value
                    )));
                }
            };
            if let (Some(key_properties), Some(key_columns)) =
                (node_type.key.as_ref(), key_columns.as_ref())
            {
                let key_description = match key_properties.as_slice() {
                    [key] => format!("@key property '{key}'"),
                    _ => format!("@key properties ({})", key_properties.join(", ")),
                };
                let key_value = canonical_node_id(key_columns, row_index)?.ok_or_else(|| {
                    OmniError::manifest(format!(
                        "node {} missing {key_description}",
                        node_type.name
                    ))
                })?;
                if let Some(explicit_id) = explicit_id {
                    if !explicit_id_matches_node_key(
                        key_columns,
                        row_index,
                        explicit_id,
                        &key_value,
                    )? {
                        return Err(OmniError::manifest(format!(
                            "node {} has explicit id '{}' that does not match {key_description} canonical value '{}'",
                            node_type.name, explicit_id, key_value
                        )));
                    }
                    node_id_remap.record(&node_type.name, explicit_id, &key_value)?;
                }
                Ok(key_value)
            } else if let Some(explicit_id) = explicit_id {
                Ok(explicit_id.to_string())
            } else {
                Ok(generate_id())
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    columns.push(Arc::new(StringArray::from(ids)));
    columns.extend(property_columns);

    RecordBatch::try_new(schema, columns).map_err(|e| OmniError::Lance(e.to_string()))
}

fn build_edge_batch(
    edge_type: &omnigraph_compiler::catalog::EdgeType,
    rows: &[(String, String, JsonValue)],
    node_id_remap: &TypedNodeIdRemap,
) -> Result<RecordBatch> {
    let schema = edge_type.arrow_schema.clone();
    let row_refs = rows.iter().map(|(_, _, data)| data).collect::<Vec<_>>();
    preflight_blob_decode_budget(
        &format!("edge:{}", edge_type.name),
        edge_type.blob_properties.iter().map(String::as_str),
        &row_refs,
    )?;

    let ids: Vec<String> = rows
        .iter()
        .map(|(_, _, data)| {
            data.get("id")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .unwrap_or_else(generate_id)
        })
        .collect();
    let srcs: Vec<String> = rows
        .iter()
        .map(|(from, _, _)| {
            node_id_remap
                .endpoint(&edge_type.from_type, from)
                .unwrap_or(from)
                .to_string()
        })
        .collect();
    let dsts: Vec<String> = rows
        .iter()
        .map(|(_, to, _)| {
            node_id_remap
                .endpoint(&edge_type.to_type, to)
                .unwrap_or(to)
                .to_string()
        })
        .collect();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    columns.push(Arc::new(StringArray::from(ids)));
    columns.push(Arc::new(StringArray::from(srcs)));
    columns.push(Arc::new(StringArray::from(dsts)));

    // Build edge property columns (skip id, src, dst at indices 0-2)
    let data_values: Vec<JsonValue> = rows.iter().map(|(_, _, data)| data.clone()).collect();
    for field in schema.fields().iter().skip(3) {
        if edge_type.blob_properties.contains(field.name()) {
            let col = build_blob_column(field.name(), field.is_nullable(), &data_values)?;
            columns.push(col);
        } else {
            let col = build_column_from_json(
                field.name(),
                field.data_type(),
                field.is_nullable(),
                &data_values,
            )?;
            columns.push(col);
        }
    }

    RecordBatch::try_new(schema, columns).map_err(|e| OmniError::Lance(e.to_string()))
}

/// Refuse an oversized aggregate base64 payload before any blob bytes are
/// decoded. The later Arrow-sized staging check remains authoritative for all
/// columns and allocator overhead; this guard prevents encoded input from
/// briefly allocating an over-limit decoded copy first.
fn preflight_blob_decode_budget<'a>(
    table_key: &str,
    blob_properties: impl Iterator<Item = &'a str>,
    rows: &[&JsonValue],
) -> Result<()> {
    let mut decoded_bytes = 0_u64;
    for property in blob_properties {
        for row in rows {
            let Some(encoded) = row
                .get(property)
                .and_then(JsonValue::as_str)
                .and_then(|value| value.strip_prefix("base64:"))
            else {
                continue;
            };
            let estimate = base64::decoded_len_estimate(encoded.len()).saturating_sub(
                encoded
                    .as_bytes()
                    .iter()
                    .rev()
                    .take_while(|&&byte| byte == b'=')
                    .count(),
            ) as u64;
            decoded_bytes = decoded_bytes.checked_add(estimate).ok_or_else(|| {
                OmniError::manifest_internal("decoded blob input byte count overflow")
            })?;
            if decoded_bytes > KEYED_WRITE_MAX_BYTES {
                return Err(OmniError::resource_limit(
                    format!("decoded blob input bytes for {table_key}"),
                    KEYED_WRITE_MAX_BYTES,
                    decoded_bytes,
                ));
            }
        }
    }
    Ok(())
}

/// Append a blob value (URI or base64 bytes) to a BlobArrayBuilder.
pub(crate) fn append_blob_value(builder: &mut BlobArrayBuilder, value: &str) -> Result<()> {
    if let Some(encoded) = value.strip_prefix("base64:") {
        let decoded_estimate = base64::decoded_len_estimate(encoded.len()).saturating_sub(
            encoded
                .as_bytes()
                .iter()
                .rev()
                .take_while(|&&b| b == b'=')
                .count(),
        );
        if decoded_estimate as u64 > KEYED_WRITE_MAX_BYTES {
            return Err(OmniError::resource_limit(
                "decoded blob input bytes",
                KEYED_WRITE_MAX_BYTES,
                decoded_estimate as u64,
            ));
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|e| OmniError::manifest(format!("invalid base64 blob data: {}", e)))?;
        builder
            .push_bytes(bytes)
            .map_err(|e| OmniError::Lance(e.to_string()))
    } else {
        // Treat as URI (file://, s3://, gs://, or any other scheme)
        builder
            .push_uri(value)
            .map_err(|e| OmniError::Lance(e.to_string()))
    }
}

/// Build a blob column from JSON values using Lance BlobArrayBuilder.
fn build_blob_column(name: &str, nullable: bool, rows: &[JsonValue]) -> Result<ArrayRef> {
    let mut builder = BlobArrayBuilder::new(rows.len());
    for row in rows {
        match row.get(name) {
            Some(JsonValue::String(s)) => {
                append_blob_value(&mut builder, s)?;
            }
            Some(JsonValue::Null) | None if nullable => {
                builder
                    .push_null()
                    .map_err(|e| OmniError::Lance(e.to_string()))?;
            }
            Some(JsonValue::Null) | None => {
                return Err(OmniError::manifest(format!(
                    "non-nullable blob property '{}' has null values",
                    name
                )));
            }
            _ => {
                return Err(OmniError::manifest(format!(
                    "blob property '{}' must be a URI string or base64: prefixed data",
                    name
                )));
            }
        }
    }
    builder
        .finish()
        .map_err(|e| OmniError::Lance(e.to_string()))
}

fn build_column_from_json(
    name: &str,
    data_type: &DataType,
    nullable: bool,
    rows: &[JsonValue],
) -> Result<ArrayRef> {
    let array: ArrayRef = match data_type {
        DataType::Utf8 => {
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|row| {
                    row.get(name)
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                })
                .collect();
            Arc::new(StringArray::from(values))
        }
        DataType::Int32 => {
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                let value = row.get(name).unwrap_or(&JsonValue::Null);
                let converted = if let Some(value) = value.as_i64() {
                    Some(i32::try_from(value).map_err(|_| {
                        OmniError::manifest(format!(
                            "property '{name}' value {value} exceeds Int32 range"
                        ))
                    })?)
                } else if let Some(value) = value.as_u64() {
                    Some(i32::try_from(value).map_err(|_| {
                        OmniError::manifest(format!(
                            "property '{name}' value {value} exceeds Int32 range"
                        ))
                    })?)
                } else {
                    None
                };
                values.push(converted);
            }
            Arc::new(Int32Array::from(values))
        }
        DataType::Int64 => {
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                let value = row.get(name).unwrap_or(&JsonValue::Null);
                let converted = if let Some(value) = value.as_i64() {
                    Some(value)
                } else if let Some(value) = value.as_u64() {
                    Some(i64::try_from(value).map_err(|_| {
                        OmniError::manifest(format!(
                            "property '{name}' value {value} exceeds Int64 range"
                        ))
                    })?)
                } else {
                    None
                };
                values.push(converted);
            }
            Arc::new(Int64Array::from(values))
        }
        DataType::UInt32 => {
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                let value = row.get(name).unwrap_or(&JsonValue::Null);
                let converted = if let Some(value) = value.as_u64() {
                    Some(u32::try_from(value).map_err(|_| {
                        OmniError::manifest(format!(
                            "property '{name}' value {value} exceeds UInt32 range"
                        ))
                    })?)
                } else if let Some(value) = value.as_i64() {
                    Some(u32::try_from(value).map_err(|_| {
                        OmniError::manifest(format!(
                            "property '{name}' value {value} exceeds UInt32 range"
                        ))
                    })?)
                } else {
                    None
                };
                values.push(converted);
            }
            Arc::new(UInt32Array::from(values))
        }
        DataType::UInt64 => {
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                let value = row.get(name).unwrap_or(&JsonValue::Null);
                let converted = if let Some(value) = value.as_u64() {
                    Some(value)
                } else if let Some(value) = value.as_i64() {
                    Some(u64::try_from(value).map_err(|_| {
                        OmniError::manifest(format!(
                            "property '{name}' value {value} exceeds UInt64 range"
                        ))
                    })?)
                } else {
                    None
                };
                values.push(converted);
            }
            Arc::new(UInt64Array::from(values))
        }
        DataType::Float32 => {
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                let value = row.get(name).unwrap_or(&JsonValue::Null);
                values.push(
                    value
                        .as_f64()
                        .map(|value| checked_json_f32(value, &format!("property '{name}'")))
                        .transpose()?,
                );
            }
            Arc::new(Float32Array::from(values))
        }
        DataType::Float64 => {
            let values: Vec<Option<f64>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_f64()))
                .collect();
            Arc::new(Float64Array::from(values))
        }
        DataType::Boolean => {
            let values: Vec<Option<bool>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_bool()))
                .collect();
            Arc::new(BooleanArray::from(values))
        }
        DataType::Date32 => {
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                values.push(parse_date32_json_value(
                    row.get(name).unwrap_or(&JsonValue::Null),
                )?);
            }
            Arc::new(Date32Array::from(values))
        }
        DataType::Date64 => {
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                values.push(parse_date64_json_value(
                    row.get(name).unwrap_or(&JsonValue::Null),
                )?);
            }
            Arc::new(Date64Array::from(values))
        }
        DataType::List(field) => {
            let mut builder = ListBuilder::with_capacity(
                make_list_value_builder(field.data_type(), rows.len())?,
                rows.len(),
            )
            .with_field(field.clone());
            for row in rows {
                let value = row.get(name).unwrap_or(&JsonValue::Null);
                if value.is_null() {
                    builder.append(false);
                    continue;
                }
                let items = value.as_array().ok_or_else(|| {
                    OmniError::manifest(format!(
                        "list property '{}' expects a JSON array, got {}",
                        name, value
                    ))
                })?;
                for item in items {
                    append_json_list_item(builder.values(), field.data_type(), item)?;
                }
                builder.append(true);
            }
            Arc::new(builder.finish())
        }
        DataType::FixedSizeList(child_field, dim) => {
            // Vector type: parse JSON array of floats into FixedSizeList<Float32>
            let dim = *dim;
            let mut builder = FixedSizeListBuilder::with_capacity(
                Float32Builder::with_capacity(rows.len() * dim as usize),
                dim,
                rows.len(),
            )
            .with_field(child_field.clone());
            for row in rows {
                if let Some(arr) = row.get(name).and_then(|v| v.as_array()) {
                    if arr.len() != dim as usize {
                        return Err(OmniError::manifest(format!(
                            "vector property '{}' expects {} dimensions, got {}",
                            name,
                            dim,
                            arr.len()
                        )));
                    }
                    for val in arr {
                        // Parity with the mutation path: non-numeric elements
                        // (null — what json! emits for a non-finite float —
                        // strings, bools) are rejected loudly, never coerced
                        // to 0.0, which would silently corrupt the vector's
                        // direction while passing every dimension check.
                        let Some(v) = val.as_f64() else {
                            return Err(OmniError::manifest(format!(
                                "vector property '{}' elements must be numeric, got {}",
                                name, val
                            )));
                        };
                        builder
                            .values()
                            .append_value(checked_json_f32(v, "vector element")?);
                    }
                    builder.append(true);
                } else if nullable {
                    for _ in 0..dim as usize {
                        builder.values().append_null();
                    }
                    builder.append(false);
                } else {
                    return Err(OmniError::manifest(format!(
                        "non-nullable vector property '{}' has null values",
                        name
                    )));
                }
            }
            Arc::new(builder.finish())
        }
        _ => {
            // Unsupported type: fill with nulls
            let values: Vec<Option<&str>> = vec![None; rows.len()];
            Arc::new(StringArray::from(values))
        }
    };

    if !nullable && array.null_count() > 0 {
        return Err(OmniError::manifest(format!(
            "non-nullable property '{}' has null or invalid values",
            name
        )));
    }

    Ok(array)
}

fn make_list_value_builder(data_type: &DataType, capacity: usize) -> Result<Box<dyn ArrayBuilder>> {
    Ok(match data_type {
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, capacity * 8)),
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::UInt32 => Box::new(UInt32Builder::with_capacity(capacity)),
        DataType::UInt64 => Box::new(UInt64Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Date32 => Box::new(Date32Builder::with_capacity(capacity)),
        DataType::Date64 => Box::new(Date64Builder::with_capacity(capacity)),
        other => {
            return Err(OmniError::manifest(format!(
                "unsupported list element data type {:?}",
                other
            )));
        }
    })
}

fn checked_json_f32(value: f64, context: &str) -> Result<f32> {
    if !value.is_finite() {
        return Err(OmniError::manifest(format!(
            "{context} value {value} must be finite for Float32"
        )));
    }
    // Judge range after IEEE round-to-nearest conversion. A shortest decimal
    // spelling that round-trips through JSON may land just outside the exact
    // f64 value of `f32::MAX` while still converting back to finite
    // `f32::MAX`; rejecting it would make export/import asymmetric.
    let narrowed = value as f32;
    if !narrowed.is_finite() {
        return Err(OmniError::manifest(format!(
            "{context} value {value} exceeds Float32 range"
        )));
    }
    Ok(narrowed)
}

fn append_json_list_item(
    builder: &mut Box<dyn ArrayBuilder>,
    data_type: &DataType,
    value: &JsonValue,
) -> Result<()> {
    match data_type {
        DataType::Utf8 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .ok_or_else(|| OmniError::manifest("list Utf8 builder downcast failed"))?;
            if let Some(value) = value.as_str() {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        DataType::Boolean => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .ok_or_else(|| OmniError::manifest("list Boolean builder downcast failed"))?;
            if let Some(value) = value.as_bool() {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        DataType::Int32 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .ok_or_else(|| OmniError::manifest("list Int32 builder downcast failed"))?;
            if let Some(value) = value.as_i64() {
                let value = i32::try_from(value).map_err(|_| {
                    OmniError::manifest(format!("list value {} exceeds Int32 range", value))
                })?;
                builder.append_value(value);
            } else if let Some(value) = value.as_u64() {
                let value = i32::try_from(value).map_err(|_| {
                    OmniError::manifest(format!("list value {} exceeds Int32 range", value))
                })?;
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        DataType::Int64 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .ok_or_else(|| OmniError::manifest("list Int64 builder downcast failed"))?;
            if let Some(value) = value.as_i64() {
                builder.append_value(value);
            } else if let Some(value) = value.as_u64() {
                let value = i64::try_from(value).map_err(|_| {
                    OmniError::manifest(format!("list value {} exceeds Int64 range", value))
                })?;
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        DataType::UInt32 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<UInt32Builder>()
                .ok_or_else(|| OmniError::manifest("list UInt32 builder downcast failed"))?;
            if let Some(value) = value.as_u64() {
                let value = u32::try_from(value).map_err(|_| {
                    OmniError::manifest(format!("list value {} exceeds UInt32 range", value))
                })?;
                builder.append_value(value);
            } else if let Some(value) = value.as_i64() {
                let value = u32::try_from(value).map_err(|_| {
                    OmniError::manifest(format!("list value {} exceeds UInt32 range", value))
                })?;
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        DataType::UInt64 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<UInt64Builder>()
                .ok_or_else(|| OmniError::manifest("list UInt64 builder downcast failed"))?;
            if let Some(value) = value.as_u64() {
                builder.append_value(value);
            } else if let Some(value) = value.as_i64() {
                let value = u64::try_from(value).map_err(|_| {
                    OmniError::manifest(format!("list value {} exceeds UInt64 range", value))
                })?;
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        DataType::Float32 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .ok_or_else(|| OmniError::manifest("list Float32 builder downcast failed"))?;
            if let Some(value) = value.as_f64() {
                builder.append_value(checked_json_f32(value, "list value")?);
            } else {
                builder.append_null();
            }
        }
        DataType::Float64 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .ok_or_else(|| OmniError::manifest("list Float64 builder downcast failed"))?;
            if let Some(value) = value.as_f64() {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        DataType::Date32 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .ok_or_else(|| OmniError::manifest("list Date32 builder downcast failed"))?;
            if let Some(value) = parse_date32_json_value(value)? {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        DataType::Date64 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Date64Builder>()
                .ok_or_else(|| OmniError::manifest("list Date64 builder downcast failed"))?;
            if let Some(value) = parse_date64_json_value(value)? {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        other => {
            return Err(OmniError::manifest(format!(
                "unsupported list element data type {:?}",
                other
            )));
        }
    }

    Ok(())
}

fn parse_date32_json_value(value: &JsonValue) -> Result<Option<i32>> {
    if value.is_null() {
        return Ok(None);
    }
    if let Some(days) = value.as_i64() {
        let days = i32::try_from(days)
            .map_err(|_| OmniError::manifest(format!("Date value out of range: {}", days)))?;
        return Ok(Some(days));
    }
    if let Some(days) = value.as_u64() {
        let days = i32::try_from(days)
            .map_err(|_| OmniError::manifest(format!("Date value out of range: {}", days)))?;
        return Ok(Some(days));
    }
    if let Some(value) = value.as_str() {
        return Ok(Some(parse_date32_literal(value)?));
    }
    Ok(None)
}

fn parse_date64_json_value(value: &JsonValue) -> Result<Option<i64>> {
    if value.is_null() {
        return Ok(None);
    }
    if let Some(ms) = value.as_i64() {
        return Ok(Some(ms));
    }
    if let Some(ms) = value.as_u64() {
        let ms = i64::try_from(ms)
            .map_err(|_| OmniError::manifest(format!("DateTime value out of range: {}", ms)))?;
        return Ok(Some(ms));
    }
    if let Some(value) = value.as_str() {
        return Ok(Some(parse_date64_literal(value)?));
    }
    Ok(None)
}

/// Write a batch to a Lance dataset, returning (new_version, total_row_count).
/// How many per-type Lance writes to run concurrently during a load.
///
/// Each write is an independent S3 manifest + fragment write against a
/// different table. Ops within a single table must still be serial (Lance
/// OCC on the manifest), but cross-table writes have no shared state.
///
/// 8 is a conservative default — enough to overlap S3 round-trip latency
/// across the typical 10-30 table schemas without flooding the runtime.
/// Override via `OMNIGRAPH_LOAD_CONCURRENCY` for benchmarking.
const DEFAULT_LOAD_WRITE_CONCURRENCY: usize = 8;

fn load_write_concurrency() -> usize {
    std::env::var("OMNIGRAPH_LOAD_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_LOAD_WRITE_CONCURRENCY)
}

fn generate_id() -> String {
    ulid::Ulid::new().to_string()
}

pub(crate) fn parse_date32_literal(value: &str) -> Result<i32> {
    let raw: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some(value)]));
    let casted = arrow_cast::cast::cast(raw.as_ref(), &DataType::Date32)
        .map_err(|e| OmniError::manifest(format!("invalid Date literal '{}': {}", value, e)))?;
    let out = casted
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| OmniError::manifest("Date32 cast produced unexpected array"))?;
    if out.is_null(0) {
        return Err(OmniError::manifest(format!(
            "invalid Date literal '{}'",
            value
        )));
    }
    Ok(out.value(0))
}

pub(crate) fn parse_date64_literal(value: &str) -> Result<i64> {
    let raw: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some(value)]));
    let casted = arrow_cast::cast::cast(raw.as_ref(), &DataType::Date64)
        .map_err(|e| OmniError::manifest(format!("invalid DateTime literal '{}': {}", value, e)))?;
    let out = casted
        .as_any()
        .downcast_ref::<Date64Array>()
        .ok_or_else(|| OmniError::manifest("Date64 cast produced unexpected array"))?;
    if out.is_null(0) {
        return Err(OmniError::manifest(format!(
            "invalid DateTime literal '{}'",
            value
        )));
    }
    Ok(out.value(0))
}

// ─── Value constraint validation ─────────────────────────────────────────────

pub(crate) fn validate_value_constraints(
    batch: &RecordBatch,
    node_type: &omnigraph_compiler::catalog::NodeType,
) -> Result<()> {
    use arrow_array::Array;

    // Range constraints
    for rc in &node_type.range_constraints {
        let Some(col) = batch.column_by_name(&rc.property) else {
            continue;
        };
        for row in 0..batch.num_rows() {
            if col.is_null(row) {
                continue;
            }
            let value = extract_numeric_value(col, row);
            if let Some(val) = value {
                if val.is_nan() {
                    return Err(OmniError::manifest(format!(
                        "@range violation on {}.{}: value is NaN",
                        node_type.name, rc.property
                    )));
                }
                if let Some(ref min) = rc.min {
                    let min_f = literal_value_to_f64(min);
                    if val < min_f {
                        return Err(OmniError::manifest(format!(
                            "@range violation on {}.{}: value {} < min {}",
                            node_type.name, rc.property, val, min_f
                        )));
                    }
                }
                if let Some(ref max) = rc.max {
                    let max_f = literal_value_to_f64(max);
                    if val > max_f {
                        return Err(OmniError::manifest(format!(
                            "@range violation on {}.{}: value {} > max {}",
                            node_type.name, rc.property, val, max_f
                        )));
                    }
                }
            }
        }
    }

    // Check constraints (regex)
    for cc in &node_type.check_constraints {
        let re = regex::Regex::new(&cc.pattern).map_err(|e| {
            OmniError::manifest(format!(
                "@check on {}.{} has invalid regex '{}': {}",
                node_type.name, cc.property, cc.pattern, e
            ))
        })?;
        let Some(col) = batch.column_by_name(&cc.property) else {
            continue;
        };
        let str_col = col.as_any().downcast_ref::<StringArray>();
        if let Some(str_col) = str_col {
            for row in 0..str_col.len() {
                if str_col.is_null(row) {
                    continue;
                }
                let val = str_col.value(row);
                if !re.is_match(val) {
                    return Err(OmniError::manifest(format!(
                        "@check violation on {}.{}: value '{}' does not match pattern '{}'",
                        node_type.name, cc.property, val, cc.pattern
                    )));
                }
            }
        }
    }

    Ok(())
}

/// Validate that every enum-typed property in `properties` only contains values
/// from its declared enum value set. Operates on a single `RecordBatch` so it
/// can be called from any write path that already holds a batch.
///
/// Scalar string enums are checked directly. List-of-enum properties are
/// checked element-by-element across the underlying string values.
pub(crate) fn validate_enum_constraints(
    batch: &RecordBatch,
    properties: &HashMap<String, omnigraph_compiler::types::PropType>,
    type_name: &str,
) -> Result<()> {
    use arrow_array::{Array, ListArray};

    for (prop_name, prop_type) in properties {
        let Some(allowed) = prop_type.enum_values.as_ref() else {
            continue;
        };
        let Some(col) = batch.column_by_name(prop_name) else {
            continue;
        };
        if prop_type.list {
            let Some(list_col) = col.as_any().downcast_ref::<ListArray>() else {
                continue;
            };
            for row in 0..list_col.len() {
                if list_col.is_null(row) {
                    continue;
                }
                let item_arr = list_col.value(row);
                let Some(str_arr) = item_arr.as_any().downcast_ref::<StringArray>() else {
                    continue;
                };
                for i in 0..str_arr.len() {
                    if str_arr.is_null(i) {
                        continue;
                    }
                    let val = str_arr.value(i);
                    if !allowed.iter().any(|a| a.as_str() == val) {
                        return Err(OmniError::manifest(format!(
                            "invalid enum value '{}' for {}.{} (expected: {})",
                            val,
                            type_name,
                            prop_name,
                            allowed.join(", ")
                        )));
                    }
                }
            }
        } else if let Some(str_col) = col.as_any().downcast_ref::<StringArray>() {
            for row in 0..str_col.len() {
                if str_col.is_null(row) {
                    continue;
                }
                let val = str_col.value(row);
                if !allowed.iter().any(|a| a.as_str() == val) {
                    return Err(OmniError::manifest(format!(
                        "invalid enum value '{}' for {}.{} (expected: {})",
                        val,
                        type_name,
                        prop_name,
                        allowed.join(", ")
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Build the composite uniqueness key for `row` over a constraint group's
/// already-resolved columns (in declaration order).
///
/// The key is the *tuple* of per-column scalar strings (`Vec<String>`), keyed
/// directly in the dedup map — there is no separator, so no data value can
/// forge a collision (an earlier version joined on `U+001F`, which a value
/// containing that control char could still defeat).
///
/// - `Ok(None)` if any column is null: the row is exempt (a partial tuple
///   can't violate uniqueness under SQL null semantics).
/// - `Ok(Some(tuple))` otherwise.
/// - `Err(..)` propagated from [`canonical_scalar_key`] on an un-keyable value.
///
/// Shared by every write surface through the unified validation evaluator
/// (`crate::validate::evaluate_unique`, used by the loader, mutation, and
/// branch-merge paths) so they derive identical keys and cannot drift on
/// separator or scalar conversion.
pub(crate) fn composite_unique_key(
    group_columns: &[ArrayRef],
    row: usize,
) -> Result<Option<Vec<String>>> {
    let mut parts = Vec::with_capacity(group_columns.len());
    for column in group_columns {
        match canonical_scalar_key(column, row)? {
            Some(value) => parts.push(value),
            None => return Ok(None),
        }
    }
    Ok(Some(parts))
}

/// Derive the exact physical node id for a typed `@key` tuple.
///
/// A one-column key retains the historical scalar spelling. Composite keys use
/// a JSON array of the per-column canonical strings: JSON escaping makes the
/// encoding deterministic and unambiguous without inventing a delimiter that
/// user data could forge.
pub(crate) fn canonical_node_id(key_columns: &[ArrayRef], row: usize) -> Result<Option<String>> {
    let Some(parts) = composite_unique_key(key_columns, row)? else {
        return Ok(None);
    };
    match parts.as_slice() {
        [] => Err(OmniError::manifest_internal(
            "cannot derive a physical node id from an empty @key tuple",
        )),
        [scalar] => Ok(Some(scalar.clone())),
        _ => serde_json::to_string(&parts)
            .map(Some)
            .map_err(|error| OmniError::manifest_internal(format!("encode @key tuple: {error}"))),
    }
}

/// Render a constraint's column tuple for error messages: a single item as
/// `col`, a composite as `(a, b)`. Used for both the column list and the
/// offending value tuple, which share the same shape.
pub(crate) fn format_tuple(items: &[String]) -> String {
    match items {
        [single] => single.clone(),
        _ => format!("({})", items.join(", ")),
    }
}

/// Reduce one typed Arrow scalar at (`array`, `row`) to its canonical key
/// string. This one renderer owns both physical node-id derivation for `@key`
/// writes and logical uniqueness tuples, so input surfaces cannot disagree
/// about width conversion, dates, booleans, or unsigned values.
///
/// - `Ok(None)` for a null value: nulls are exempt from uniqueness (standard
///   SQL semantics over nullable columns).
/// - `Ok(Some(s))` for every scalar type a `@unique` / `@key` column can hold.
///   Strings are covered in all three physical Arrow encodings (`Utf8`,
///   `LargeUtf8`, `Utf8View`), so a legal string column is always keyable
///   regardless of how Lance materializes it on read-back.
/// - `Err(..)` for a non-null value whose Arrow type can't be reduced to a key
///   (a list, blob, or vector column). This fails loudly rather than silently
///   exempting the row, and because every legal scalar encoding is handled
///   above, the error fires only for a genuinely un-keyable column type — never
///   for a legal value that merely arrived in an unenumerated encoding.
pub(crate) fn canonical_scalar_key(array: &ArrayRef, row: usize) -> Result<Option<String>> {
    use arrow_array::{Array, LargeStringArray, StringViewArray};
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(a.value(row).to_string()));
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Some(a.value(row).to_string()));
    }
    if let Some(a) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(Some(a.value(row).to_string()));
    }
    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(Some(a.value(row).to_string()));
    }
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(Some(a.value(row).to_string()));
    }
    if let Some(a) = array.as_any().downcast_ref::<UInt32Array>() {
        return Ok(Some(a.value(row).to_string()));
    }
    if let Some(a) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(Some(a.value(row).to_string()));
    }
    if let Some(a) = array.as_any().downcast_ref::<Float32Array>() {
        let value = a.value(row);
        if !value.is_finite() {
            return Err(OmniError::manifest(format!(
                "scalar key: non-finite Float32 value {value} cannot identify a row"
            )));
        }
        return Ok(Some(if value == 0.0 {
            "0".to_string()
        } else {
            value.to_string()
        }));
    }
    if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
        let value = a.value(row);
        if !value.is_finite() {
            return Err(OmniError::manifest(format!(
                "scalar key: non-finite Float64 value {value} cannot identify a row"
            )));
        }
        return Ok(Some(if value == 0.0 {
            "0".to_string()
        } else {
            value.to_string()
        }));
    }
    if let Some(a) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok(Some(a.value(row).to_string()));
    }
    if let Some(a) = array.as_any().downcast_ref::<Date32Array>() {
        return Ok(Some(a.value(row).to_string()));
    }
    if let Some(a) = array.as_any().downcast_ref::<Date64Array>() {
        return Ok(Some(a.value(row).to_string()));
    }
    Err(OmniError::manifest(format!(
        "scalar key: unsupported column type {:?} for @unique/@key enforcement",
        array.data_type()
    )))
}

/// Compare an explicit physical id from an export with the typed key scalar it
/// accompanies. Comparison happens in the declared Arrow type, not by string,
/// because old writers derived some ids from literal text while export emitted
/// the stored value (notably Date/DateTime and rounded F32).
fn explicit_id_matches_scalar_key(array: &ArrayRef, row: usize, explicit: &str) -> Result<bool> {
    use arrow_array::{Array, LargeStringArray, StringViewArray};
    if array.is_null(row) {
        return Ok(false);
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(a.value(row) == explicit);
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(a.value(row) == explicit);
    }
    if let Some(a) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(a.value(row) == explicit);
    }
    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(explicit.parse::<i32>().ok() == Some(a.value(row)));
    }
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(explicit.parse::<i64>().ok() == Some(a.value(row)));
    }
    if let Some(a) = array.as_any().downcast_ref::<UInt32Array>() {
        return Ok(explicit.parse::<u32>().ok() == Some(a.value(row)));
    }
    if let Some(a) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(explicit.parse::<u64>().ok() == Some(a.value(row)));
    }
    if let Some(a) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(explicit
            .parse::<f32>()
            .is_ok_and(|value| value == a.value(row)));
    }
    if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(explicit
            .parse::<f64>()
            .is_ok_and(|value| value == a.value(row)));
    }
    if let Some(a) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok(explicit
            .parse::<bool>()
            .is_ok_and(|value| value == a.value(row)));
    }
    if let Some(a) = array.as_any().downcast_ref::<Date32Array>() {
        let parsed = explicit
            .parse::<i32>()
            .ok()
            .or_else(|| parse_date32_literal(explicit).ok());
        return Ok(parsed == Some(a.value(row)));
    }
    if let Some(a) = array.as_any().downcast_ref::<Date64Array>() {
        let parsed = explicit
            .parse::<i64>()
            .ok()
            .or_else(|| parse_date64_literal(explicit).ok());
        return Ok(parsed == Some(a.value(row)));
    }
    Err(OmniError::manifest(format!(
        "scalar key: unsupported column type {:?} for explicit id comparison",
        array.data_type()
    )))
}

/// Accept either the current exact tuple id or a legacy id derived from one
/// typed key component. Before composite keys were made physical, writers used
/// whichever component was first in that version's runtime catalog. Supported
/// property renames could change that lexical order, so one old export may
/// legitimately contain scalar ids from different tuple positions. The caller
/// always persists `canonical_id` and records an endpoint remap; accepting an
/// old spelling never weakens the invariant that physical `id` equals the
/// complete typed key. If the same old id names different tuples, the typed
/// remap rejects the import as ambiguous before any effect.
fn explicit_id_matches_node_key(
    key_columns: &[ArrayRef],
    row: usize,
    explicit: &str,
    canonical_id: &str,
) -> Result<bool> {
    if explicit == canonical_id {
        return Ok(true);
    }
    if key_columns.is_empty() {
        return Err(OmniError::manifest_internal(
            "cannot compare an explicit id to an empty @key tuple",
        ));
    }
    for column in key_columns {
        if explicit_id_matches_scalar_key(column, row, explicit)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn extract_numeric_value(col: &ArrayRef, row: usize) -> Option<f64> {
    use arrow_array::{
        Array, Float32Array, Float64Array, Int32Array, Int64Array, UInt32Array, UInt64Array,
    };
    if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = col.as_any().downcast_ref::<Float32Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return Some(a.value(row));
    }
    None
}

fn literal_value_to_f64(v: &omnigraph_compiler::catalog::LiteralValue) -> f64 {
    use omnigraph_compiler::catalog::LiteralValue;
    match v {
        LiteralValue::Integer(n) => *n as f64,
        LiteralValue::Float(f) => *f,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Omnigraph;
    use arrow_array::Array;
    use futures::TryStreamExt;
    use std::collections::HashMap;

    const TEST_SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I32?
}
node Company {
    name: String @key
}
edge Knows: Person -> Person {
    since: Date?
}
edge WorksAt: Person -> Company
"#;

    const TEST_DATA: &str = r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}
{"type": "Person", "data": {"name": "Bob", "age": 25}}
{"type": "Company", "data": {"name": "Acme"}}
{"edge": "Knows", "from": "Alice", "to": "Bob"}
{"edge": "WorksAt", "from": "Alice", "to": "Acme"}
"#;

    #[tokio::test]
    async fn test_load_creates_data() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        let result = load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();

        assert_eq!(result.nodes_loaded["Person"], 2);
        assert_eq!(result.nodes_loaded["Company"], 1);
        assert_eq!(result.edges_loaded["Knows"], 1);
        assert_eq!(result.edges_loaded["WorksAt"], 1);
    }

    #[tokio::test]
    async fn test_load_data_readable_via_lance() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
        load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();

        // Read back via snapshot
        let snap = db.snapshot().await;
        let person_ds = snap.open("node:Person").await.unwrap();

        assert_eq!(person_ds.count_rows(None).await.unwrap(), 2);

        // Verify data
        let batches: Vec<RecordBatch> = person_ds
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let batch = &batches[0];
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // @key=name, so ids should be "Alice" and "Bob"
        let id_values: Vec<&str> = (0..ids.len()).map(|i| ids.value(i)).collect();
        assert!(id_values.contains(&"Alice"));
        assert!(id_values.contains(&"Bob"));
    }

    #[tokio::test]
    async fn test_load_edges_reference_node_keys() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
        load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();

        let snap = db.snapshot().await;
        let knows_ds = snap.open("edge:Knows").await.unwrap();

        let batches: Vec<RecordBatch> = knows_ds
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let batch = &batches[0];
        let srcs = batch
            .column_by_name("src")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dsts = batch
            .column_by_name("dst")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(srcs.value(0), "Alice");
        assert_eq!(dsts.value(0), "Bob");
    }

    #[tokio::test]
    async fn test_load_manifest_version_advances() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
        let v1 = db.version().await;

        load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();

        assert!(db.version().await > v1);
    }

    #[tokio::test]
    async fn test_load_append_adds_rows() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        let batch1 = r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}"#;
        let batch2 = r#"{"type": "Person", "data": {"name": "Bob", "age": 25}}"#;

        load_jsonl(&mut db, batch1, LoadMode::Overwrite)
            .await
            .unwrap();
        load_jsonl(&mut db, batch2, LoadMode::Append).await.unwrap();

        let snap = db.snapshot().await;
        let person_ds = snap.open("node:Person").await.unwrap();
        assert_eq!(person_ds.count_rows(None).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_load_unknown_type_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        let bad = r#"{"type": "FakeType", "data": {"name": "x"}}"#;
        let result = load_jsonl(&mut db, bad, LoadMode::Overwrite).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn test_ingest_creates_branch_and_reports_tables() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        let result = db
            .ingest("feature", Some("main"), TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();

        assert_eq!(result.branch, "feature");
        assert_eq!(result.base_branch, "main");
        assert!(result.branch_created);
        assert_eq!(result.mode, LoadMode::Overwrite);
        assert_eq!(
            result.tables,
            vec![
                IngestTableResult {
                    table_key: "edge:Knows".to_string(),
                    rows_loaded: 1
                },
                IngestTableResult {
                    table_key: "edge:WorksAt".to_string(),
                    rows_loaded: 1
                },
                IngestTableResult {
                    table_key: "node:Company".to_string(),
                    rows_loaded: 1
                },
                IngestTableResult {
                    table_key: "node:Person".to_string(),
                    rows_loaded: 2
                },
            ]
        );
        assert!(
            db.branch_list()
                .await
                .unwrap()
                .contains(&"feature".to_string())
        );
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn test_ingest_existing_branch_ignores_from_and_merges_data() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
        load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();
        db.branch_create_from(crate::db::ReadTarget::branch("main"), "feature")
            .await
            .unwrap();

        let result = db
            .ingest(
                "feature",
                Some("missing-base"),
                r#"{"type":"Person","data":{"name":"Bob","age":26}}
{"type":"Person","data":{"name":"Eve","age":31}}"#,
                LoadMode::Merge,
            )
            .await
            .unwrap();

        assert_eq!(result.branch, "feature");
        assert_eq!(result.base_branch, "missing-base");
        assert!(!result.branch_created);
        assert_eq!(result.mode, LoadMode::Merge);
        assert_eq!(
            result.tables,
            vec![IngestTableResult {
                table_key: "node:Person".to_string(),
                rows_loaded: 2
            }]
        );

        let snap = db
            .snapshot_of(crate::db::ReadTarget::branch("feature"))
            .await
            .unwrap();
        let person_ds = snap.open("node:Person").await.unwrap();
        assert_eq!(person_ds.count_rows(None).await.unwrap(), 3);

        let batches: Vec<RecordBatch> = person_ds
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut ages_by_id = HashMap::new();
        for batch in &batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let ages = batch
                .column_by_name("age")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for idx in 0..ids.len() {
                ages_by_id.insert(ids.value(idx).to_string(), ages.value(idx));
            }
        }

        assert_eq!(ages_by_id.get("Bob"), Some(&26));
        assert_eq!(ages_by_id.get("Eve"), Some(&31));
        assert_eq!(ages_by_id.get("Alice"), Some(&30));
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn test_ingest_as_stamps_actor_on_branch_head_commit() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        db.ingest_as(
            "feature",
            Some("main"),
            TEST_DATA,
            LoadMode::Overwrite,
            Some("act-andrew"),
        )
        .await
        .unwrap();

        let head = db
            .list_commits(Some("feature"))
            .await
            .unwrap()
            .into_iter()
            .last()
            .unwrap();
        assert_eq!(head.actor_id.as_deref(), Some("act-andrew"));
    }

    #[tokio::test]
    async fn test_load_as_with_base_forks_missing_branch_and_stamps_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        let result = db
            .load_as("feature", Some("main"), TEST_DATA, LoadMode::Merge, None)
            .await
            .unwrap();

        assert_eq!(result.branch, "feature");
        assert_eq!(result.base_branch.as_deref(), Some("main"));
        assert!(result.branch_created);
        assert!(
            db.branch_list()
                .await
                .unwrap()
                .contains(&"feature".to_string())
        );

        // Re-loading onto the now-existing branch records the base but
        // performs no fork.
        let again = db
            .load_as(
                "feature",
                Some("main"),
                r#"{"type":"Person","data":{"name":"Bob","age":26}}"#,
                LoadMode::Merge,
                None,
            )
            .await
            .unwrap();
        assert!(!again.branch_created);
        assert_eq!(again.base_branch.as_deref(), Some("main"));
    }

    #[tokio::test]
    async fn test_load_as_without_base_errors_on_missing_branch() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        let result = db
            .load_as("nonexistent", None, TEST_DATA, LoadMode::Merge, None)
            .await;
        assert!(
            result.is_err(),
            "load without base must not create branches"
        );
        assert!(
            !db.branch_list()
                .await
                .unwrap()
                .contains(&"nonexistent".to_string()),
            "failed load must not leave a branch behind"
        );

        // Loads to main carry the default branch metadata.
        let main_load = db
            .load("main", TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();
        assert_eq!(main_load.branch, "main");
        assert_eq!(main_load.base_branch, None);
        assert!(!main_load.branch_created);
    }

    #[test]
    fn test_range_constraint_rejects_nan() {
        use arrow_array::{Float64Array, RecordBatch, StringArray};
        use omnigraph_compiler::catalog::{LiteralValue, NodeType, RangeConstraint};
        use std::sync::Arc;

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("score", arrow_schema::DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["bad"])),
                Arc::new(Float64Array::from(vec![f64::NAN])),
            ],
        )
        .unwrap();

        let node_type = NodeType {
            name: "Test".to_string(),
            implements: vec![],
            properties: Default::default(),
            key: None,
            unique_constraints: vec![],
            indices: vec![],
            range_constraints: vec![RangeConstraint {
                property: "score".to_string(),
                min: Some(LiteralValue::Float(0.0)),
                max: Some(LiteralValue::Float(1.0)),
            }],
            check_constraints: vec![],
            embed_sources: Default::default(),
            blob_properties: Default::default(),
            arrow_schema: schema,
        };

        let result = validate_value_constraints(&batch, &node_type);
        assert!(result.is_err(), "expected NaN to be rejected");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("NaN"), "error should mention NaN: {}", err);
    }

    #[test]
    fn composite_unique_key_builds_tuple_and_exempts_null() {
        let a: ArrayRef = Arc::new(StringArray::from(vec![Some("x|y"), Some("x"), None]));
        let b: ArrayRef = Arc::new(StringArray::from(vec![Some("z"), Some("y|z"), Some("q")]));
        let cols = [a, b];

        // Tuple key, so `("x|y", "z")` and `("x", "y|z")` stay distinct —
        // a separator-joined key (the old `|` join) would collapse both to
        // `x|y|z`.
        assert_eq!(
            composite_unique_key(&cols, 0).unwrap(),
            Some(vec!["x|y".to_string(), "z".to_string()])
        );
        assert_eq!(
            composite_unique_key(&cols, 1).unwrap(),
            Some(vec!["x".to_string(), "y|z".to_string()])
        );
        assert_ne!(
            composite_unique_key(&cols, 0).unwrap(),
            composite_unique_key(&cols, 1).unwrap()
        );

        // Any null column → the whole row is exempt (SQL null semantics).
        assert_eq!(composite_unique_key(&cols, 2).unwrap(), None);
    }

    #[test]
    fn unique_key_scalar_errors_loudly_on_unkeyable_type() {
        use arrow_array::LargeBinaryArray;
        // A binary/blob column can't be reduced to a uniqueness key. Before the
        // hardening this returned `None`, so a `@unique` on such a column was
        // silently un-enforced; now it errors instead of weakening the
        // constraint in silence.
        let blob: ArrayRef = Arc::new(LargeBinaryArray::from(vec![Some(&b"abc"[..])]));
        let err = canonical_scalar_key(&blob, 0).unwrap_err();
        assert!(
            err.to_string().contains("unsupported column type"),
            "un-keyable type must fail loudly (got: {err})"
        );
    }

    #[test]
    fn unique_key_scalar_handles_all_string_encodings() {
        use arrow_array::{LargeStringArray, StringViewArray};
        // A legal string column is keyable in every physical Arrow encoding
        // Lance might hand back (Utf8 / LargeUtf8 / Utf8View). None of these may
        // fall through to the loud `Err` path — that branch is reserved for
        // genuinely un-keyable column types, not a legal value in an
        // unenumerated encoding.
        let utf8: ArrayRef = Arc::new(StringArray::from(vec![Some("v")]));
        let large: ArrayRef = Arc::new(LargeStringArray::from(vec![Some("v")]));
        let view: ArrayRef = Arc::new(StringViewArray::from(vec![Some("v")]));
        for array in [&utf8, &large, &view] {
            assert_eq!(
                canonical_scalar_key(array, 0).unwrap(),
                Some("v".to_string()),
                "string array {:?} must render, not error",
                array.data_type()
            );
        }
    }

    #[test]
    fn canonical_scalar_key_handles_every_supported_non_string_key_type() {
        let cases: Vec<(ArrayRef, &str)> = vec![
            (Arc::new(BooleanArray::from(vec![true])), "true"),
            (Arc::new(Int32Array::from(vec![-32])), "-32"),
            (
                Arc::new(Int64Array::from(vec![i64::MIN])),
                "-9223372036854775808",
            ),
            (Arc::new(UInt32Array::from(vec![u32::MAX])), "4294967295"),
            (
                Arc::new(UInt64Array::from(vec![u64::MAX])),
                "18446744073709551615",
            ),
            (Arc::new(Float32Array::from(vec![1.25_f32])), "1.25"),
            (Arc::new(Float64Array::from(vec![-2.5_f64])), "-2.5"),
            (Arc::new(Float32Array::from(vec![-0.0_f32])), "0"),
            (Arc::new(Float64Array::from(vec![-0.0_f64])), "0"),
            (Arc::new(Date32Array::from(vec![19_723])), "19723"),
            (
                Arc::new(Date64Array::from(vec![1_704_067_200_000])),
                "1704067200000",
            ),
        ];

        for (array, expected) in cases {
            assert_eq!(
                canonical_scalar_key(&array, 0).unwrap().as_deref(),
                Some(expected),
                "scalar array {:?} must use its exact stored representation",
                array.data_type()
            );
        }

        for non_finite in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let array: ArrayRef = Arc::new(Float32Array::from(vec![non_finite]));
            assert!(canonical_scalar_key(&array, 0).is_err());
        }
        for non_finite in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let array: ArrayRef = Arc::new(Float64Array::from(vec![non_finite]));
            assert!(canonical_scalar_key(&array, 0).is_err());
        }
    }

    #[test]
    fn explicit_id_comparison_is_typed_for_legacy_spellings() {
        let equivalent: Vec<(ArrayRef, &str)> = vec![
            (Arc::new(BooleanArray::from(vec![true])), "true"),
            (
                Arc::new(Float32Array::from(vec![1.234_567_9_f32])),
                "1.23456789",
            ),
            (Arc::new(Date32Array::from(vec![19_723])), "2024-01-01"),
            (
                Arc::new(Date64Array::from(vec![1_704_067_200_000])),
                "2024-01-01T00:00:00Z",
            ),
            (
                Arc::new(UInt64Array::from(vec![u64::MAX])),
                "18446744073709551615",
            ),
        ];

        for (array, explicit) in equivalent {
            assert!(
                explicit_id_matches_scalar_key(&array, 0, explicit).unwrap(),
                "legacy id {explicit:?} must equal typed {:?} value",
                array.data_type()
            );
        }
        let day: ArrayRef = Arc::new(Date32Array::from(vec![19_723]));
        assert!(!explicit_id_matches_scalar_key(&day, 0, "2024-01-02").unwrap());
    }

    #[test]
    fn composite_node_id_is_deterministic_and_unambiguous() {
        let scalar: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec!["a,b"]))];
        assert_eq!(
            canonical_node_id(&scalar, 0).unwrap().as_deref(),
            Some("a,b"),
            "one-column keys retain their historical scalar id"
        );

        let composite: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["a,b"])),
            Arc::new(UInt32Array::from(vec![7])),
            Arc::new(StringArray::from(vec!["quote\"and\\slash"])),
        ];
        assert_eq!(
            canonical_node_id(&composite, 0).unwrap().as_deref(),
            Some(r#"["a,b","7","quote\"and\\slash"]"#)
        );
    }

    #[test]
    fn typed_node_id_remap_disambiguates_types_and_rejects_ambiguity() {
        let mut remap = TypedNodeIdRemap::default();
        remap.record("Exact", "16777217", "16777217").unwrap();
        remap.record("Rounded", "16777217", "16777216").unwrap();
        assert_eq!(remap.endpoint("Exact", "16777217"), Some("16777217"));
        assert_eq!(remap.endpoint("Rounded", "16777217"), Some("16777216"));

        let err = remap.record("Rounded", "16777217", "16777218").unwrap_err();
        assert!(
            err.to_string().contains("ambiguous edge endpoint remap"),
            "same typed old id must not choose a canonical endpoint silently: {err}"
        );
    }

    #[test]
    fn checked_json_f32_accepts_boundary_and_rejects_overflow_and_nonfinite() {
        assert_eq!(checked_json_f32(f32::MAX as f64, "test").unwrap(), f32::MAX);
        assert_eq!(
            checked_json_f32(f32::MAX as f64 * (1.0 + f64::EPSILON), "test").unwrap(),
            f32::MAX
        );
        assert!(checked_json_f32(f32::MAX as f64 * (1.0 + f32::EPSILON as f64), "test").is_err());
        assert!(checked_json_f32(f64::MAX, "test").is_err());
        assert!(checked_json_f32(f64::INFINITY, "test").is_err());
        assert!(checked_json_f32(f64::NAN, "test").is_err());
    }
}
