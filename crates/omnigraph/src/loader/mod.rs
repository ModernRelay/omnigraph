use std::collections::{HashMap, HashSet};

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
        // Schema-contract validation is captured ONCE per write via the
        // `WriteTxn` opened in `load_jsonl_reader` (after branch resolution).
        // The redundant `ensure_schema_state_valid` that used to run here is
        // subsumed by `open_write_txn`'s `resolved_branch_target` call.
        // Converge any pending recovery sidecar (a previously failed
        // writer's Phase B → Phase C residual) before staging anything:
        // without this, sidecar-covered drift wedges every load on the
        // commit-time drift guard until a process restart — `repair`
        // refuses while a sidecar is pending. One `list_dir` when no
        // sidecars exist (the steady state).
        self.heal_pending_recovery_sidecars().await?;
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
        // `expected_table_versions` CAS inside `load_jsonl_reader`.
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
        let reader = BufReader::new(Cursor::new(data.as_bytes()));
        load_jsonl_reader(self, branch, reader, mode, actor_id).await
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

async fn load_jsonl_reader<R: BufRead>(
    db: &Omnigraph,
    branch: Option<&str>,
    reader: R,
    mode: LoadMode,
    actor_id: Option<&str>,
) -> Result<LoadResult> {
    let catalog = db.catalog().clone();

    // Phase 1: Parse all lines, spool into per-type collections
    let mut node_rows: HashMap<String, Vec<JsonValue>> = HashMap::new();
    let mut edge_rows: HashMap<String, Vec<(String, String, JsonValue)>> = HashMap::new();

    // Parse a stream of JSON values. Accepts both compact JSONL (one object
    // per line) and pretty-printed JSON where a single object spans multiple
    // lines — serde's streaming deserializer treats any whitespace (including
    // newlines) between top-level values as a separator.
    for (idx, parsed) in serde_json::Deserializer::from_reader(reader)
        .into_iter::<JsonValue>()
        .enumerate()
    {
        let record_num = idx + 1;
        let value: JsonValue = parsed.map_err(|e| {
            OmniError::manifest(format!("invalid JSON at record {}: {}", record_num, e))
        })?;

        if let Some(type_name) = value.get("type").and_then(|v| v.as_str()) {
            if !catalog.node_types.contains_key(type_name) {
                return Err(OmniError::manifest(format!(
                    "record {}: unknown node type '{}'",
                    record_num, type_name
                )));
            }
            let data = value
                .get("data")
                .cloned()
                .unwrap_or(JsonValue::Object(serde_json::Map::new()));
            node_rows
                .entry(type_name.to_string())
                .or_default()
                .push(data);
        } else if let Some(edge_name) = value.get("edge").and_then(|v| v.as_str()) {
            if catalog.lookup_edge_by_name(edge_name).is_none() {
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
                .get("data")
                .cloned()
                .unwrap_or(JsonValue::Object(serde_json::Map::new()));
            let canonical = catalog.lookup_edge_by_name(edge_name).unwrap().name.clone();
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
    // Capture-once write transaction (RFC-013 step 3b). `open_write_txn`
    // validates the schema contract ONCE and pins the base snapshot. Threaded
    // as `Some(&txn)` through the per-table opens and the manifest publish so
    // each resolve point reuses the pinned base instead of re-validating the
    // contract. The branch already exists here (fork-if-missing ran in
    // `load_as` before this), so this captures the post-fork snapshot. The
    // load's own base read (`db.snapshot_for_branch` previously) is the same
    // per-branch snapshot, so reuse `txn.base` for it — dropping a validation.
    let txn = db.open_write_txn(branch).await?;
    let snapshot = txn.base.clone();
    let mut staging = MutationStaging::default();
    let pending_mode = match mode {
        LoadMode::Merge => PendingMode::Merge,
        // Append-mode loads accumulate as Append. Edge tables (no @key)
        // and no-key node tables stay safe on the stage_append path. The
        // Merge mode applies dedupe-by-id; Append assumes unique inputs.
        LoadMode::Append => PendingMode::Append,
        LoadMode::Overwrite => PendingMode::Overwrite,
    };
    // Map LoadMode to MutationOpKind for the version-check policy.
    // Append/Merge skip the strict pre-stage check (concurrency-safe
    // under the per-(table, branch) queue + publisher CAS); Overwrite
    // uses the strict check because it truncates and replaces the
    // dataset — concurrent advances change what "replace" means.
    let load_op_kind = match mode {
        LoadMode::Append => crate::db::MutationOpKind::Insert,
        LoadMode::Merge => crate::db::MutationOpKind::Merge,
        LoadMode::Overwrite => crate::db::MutationOpKind::SchemaRewrite,
    };

    // Up-front fork-queue acquisition. The first write to a table on a
    // non-main branch forks it (create_branch), which advances Lance state
    // before the manifest publish; the reclaim of any manifest-unreferenced
    // leftover (`reclaim_orphaned_fork_and_refork`) must not race a concurrent
    // in-process fork. So when this load will fork at least one touched table,
    // acquire the per-(table, branch) write queues for ALL touched tables up
    // front (one sorted `acquire_many`, keyed uniformly by the target branch
    // so it covers what `commit_all` recomputes) and hold them through the
    // publish. Main-branch loads never fork; branch loads where every touched
    // table is already forked skip this and let `commit_all` acquire at commit.
    let fork_queue_guards: Option<(
        Vec<(String, Option<String>)>,
        Vec<tokio::sync::OwnedMutexGuard<()>>,
    )> = if let Some(active) = branch {
        let touched: Vec<(String, Option<String>)> = node_rows
            .keys()
            .map(|t| (format!("node:{t}"), Some(active.to_string())))
            .chain(
                edge_rows
                    .keys()
                    .map(|e| (format!("edge:{e}"), Some(active.to_string()))),
            )
            .collect();
        let needs_fork = touched.iter().any(|(table_key, _)| {
            snapshot
                .entry(table_key)
                .map(|e| e.table_branch.as_deref() != Some(active))
                .unwrap_or(false)
        });
        if needs_fork {
            let guards = db.write_queue().acquire_many(&touched).await;
            Some((touched, guards))
        } else {
            None
        }
    } else {
        None
    };

    // Phase 2a: build and validate every node batch up front. Cheap and
    // synchronous — surfaces validation errors before any S3 traffic.
    let mut prepared_nodes: Vec<(String, String, RecordBatch, usize)> =
        Vec::with_capacity(node_rows.len());
    for (type_name, rows) in &node_rows {
        let node_type = &catalog.node_types[type_name];
        let batch = build_node_batch(node_type, rows)?;
        validate_value_constraints(&batch, node_type)?;
        validate_enum_constraints(&batch, &node_type.properties, type_name)?;
        let unique_groups = unique_constraint_groups_for_node(node_type);
        if !unique_groups.is_empty() {
            enforce_unique_constraints_intra_batch(&batch, type_name, &unique_groups)?;
        }
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
            opened.full_path,
            opened.table_branch,
            opened.expected_version,
            load_op_kind,
        );
        let schema = batch.schema();
        staging.append_batch(&table_key, schema, pending_mode, batch)?;
        result.nodes_loaded.insert(type_name, loaded_count);
    }

    // Phase 2c: Validate edge referential integrity — every src/dst must
    // reference an existing node ID in the appropriate type. For
    // Append/Merge the lookup unions snapshot-committed IDs with the
    // in-memory pending batches. For Overwrite, a touched node table's
    // pending batch is the replacement image, so committed rows are not
    // included for that table.
    for (edge_name, rows) in &edge_rows {
        let edge_type = &catalog.edge_types[edge_name];
        let from_ids =
            collect_node_ids_with_pending(db, branch, &edge_type.from_type, &staging).await?;
        let to_ids =
            collect_node_ids_with_pending(db, branch, &edge_type.to_type, &staging).await?;

        for (i, (src, dst, _)) in rows.iter().enumerate() {
            if !from_ids.contains(src.as_str()) {
                return Err(OmniError::manifest(format!(
                    "edge {} row {}: src '{}' not found in {}",
                    edge_name,
                    i + 1,
                    src,
                    edge_type.from_type
                )));
            }
            if !to_ids.contains(dst.as_str()) {
                return Err(OmniError::manifest(format!(
                    "edge {} row {}: dst '{}' not found in {}",
                    edge_name,
                    i + 1,
                    dst,
                    edge_type.to_type
                )));
            }
        }
    }

    // Phase 2d: build edge batches.
    let mut prepared_edges: Vec<(String, String, RecordBatch, usize)> =
        Vec::with_capacity(edge_rows.len());
    for (edge_name, rows) in &edge_rows {
        let edge_type = &catalog.edge_types[edge_name];
        let batch = build_edge_batch(edge_type, rows)?;
        validate_enum_constraints(&batch, &edge_type.properties, edge_name)?;
        let unique_groups = unique_constraint_groups_for_edge(edge_type);
        if !unique_groups.is_empty() {
            enforce_unique_constraints_intra_batch(&batch, edge_name, &unique_groups)?;
        }
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
            opened.full_path,
            opened.table_branch,
            opened.expected_version,
            load_op_kind,
        );
        let schema = batch.schema();
        staging.append_batch(&table_key, schema, pending_mode, batch)?;
        result.edges_loaded.insert(edge_name, loaded_count);
    }

    // Phase 3: Validate edge cardinality constraints (before commit —
    // invalid data must not be committed). The helper scans committed
    // edges via Lance + iterates pending edges in-memory; for Overwrite it
    // treats the pending edge batches as the replacement table image.
    for (edge_name, _) in &edge_rows {
        let edge_type = &catalog.edge_types[edge_name];
        let table_key = format!("edge:{}", edge_name);
        validate_edge_cardinality_with_pending_loader(
            db, branch, edge_type, &table_key, &staging, mode,
        )
        .await?;
    }

    // Phase 4: Atomic manifest commit with publisher-level OCC.
    let staged = staging
        .stage_all_with_concurrency(db, branch, load_write_concurrency())
        .await?;
    // `_queue_guards` holds per-(table_key, branch) write queues
    // across the manifest publish below — see exec/mutation.rs for
    // the rationale (interleaving prevention).
    let crate::exec::staging::CommittedMutation {
        updates,
        expected_versions,
        sidecar_handle,
        guards: _queue_guards,
        committed_handles,
    } = staged
        .commit_all(
            db,
            branch,
            crate::db::manifest::SidecarKind::Load,
            actor_id,
            fork_queue_guards,
            Some(&txn),
        )
        .await?;
    // Same finalize → publisher residual as mutations: per-table
    // staged commits have advanced Lance HEAD, but the manifest
    // publish has not run yet. Reuse the mutation failpoint name so
    // one failpoint pins the shared `MutationStaging` boundary.
    crate::failpoints::maybe_fail(crate::failpoints::names::MUTATION_POST_FINALIZE_PRE_PUBLISHER)?;
    db.commit_updates_on_branch_with_expected(
        branch,
        &updates,
        &expected_versions,
        actor_id,
        Some(&txn),
        committed_handles,
    )
    .await?;
    // The recovery sidecar protects the per-table commit_staged →
    // manifest publish window. Phase C succeeded — clean up
    // best-effort: failing the user here would error out a write
    // that already landed durably.
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

fn build_node_batch(node_type: &NodeType, rows: &[JsonValue]) -> Result<RecordBatch> {
    let schema = node_type.arrow_schema.clone();

    // Build id column: explicit id, @key value, or generated ULID.
    let ids: Vec<String> = rows
        .iter()
        .map(|row| {
            let explicit_id = row.get("id").and_then(|v| v.as_str()).map(str::to_string);
            if let Some(key_prop) = node_type.key_property() {
                let key_value = row
                    .get(key_prop)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| {
                        OmniError::manifest(format!(
                            "node {} missing @key property '{}'",
                            node_type.name, key_prop
                        ))
                    })?;
                if let Some(explicit_id) = explicit_id {
                    if explicit_id != key_value {
                        return Err(OmniError::manifest(format!(
                            "node {} has explicit id '{}' that does not match @key property '{}' value '{}'",
                            node_type.name, explicit_id, key_prop, key_value
                        )));
                    }
                }
                Ok(key_value)
            } else if let Some(explicit_id) = explicit_id {
                Ok(explicit_id)
            } else {
                Ok(generate_id())
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    columns.push(Arc::new(StringArray::from(ids)));

    // Build property columns (skip "id" field at index 0)
    for field in schema.fields().iter().skip(1) {
        if node_type.blob_properties.contains(field.name()) {
            let col = build_blob_column(field.name(), field.is_nullable(), rows)?;
            columns.push(col);
        } else {
            let col =
                build_column_from_json(field.name(), field.data_type(), field.is_nullable(), rows)?;
            columns.push(col);
        }
    }

    RecordBatch::try_new(schema, columns).map_err(|e| OmniError::Lance(e.to_string()))
}

fn build_edge_batch(
    edge_type: &omnigraph_compiler::catalog::EdgeType,
    rows: &[(String, String, JsonValue)],
) -> Result<RecordBatch> {
    let schema = edge_type.arrow_schema.clone();

    let ids: Vec<String> = rows
        .iter()
        .map(|(_, _, data)| {
            data.get("id")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .unwrap_or_else(generate_id)
        })
        .collect();
    let srcs: Vec<&str> = rows.iter().map(|(from, _, _)| from.as_str()).collect();
    let dsts: Vec<&str> = rows.iter().map(|(_, to, _)| to.as_str()).collect();

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

/// Append a blob value (URI or base64 bytes) to a BlobArrayBuilder.
pub(crate) fn append_blob_value(builder: &mut BlobArrayBuilder, value: &str) -> Result<()> {
    if let Some(encoded) = value.strip_prefix("base64:") {
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
            let values: Vec<Option<i32>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_i64()).map(|v| v as i32))
                .collect();
            Arc::new(Int32Array::from(values))
        }
        DataType::Int64 => {
            let values: Vec<Option<i64>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_i64()))
                .collect();
            Arc::new(Int64Array::from(values))
        }
        DataType::UInt32 => {
            let values: Vec<Option<u32>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_u64()).map(|v| v as u32))
                .collect();
            Arc::new(UInt32Array::from(values))
        }
        DataType::UInt64 => {
            let values: Vec<Option<u64>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_u64()))
                .collect();
            Arc::new(UInt64Array::from(values))
        }
        DataType::Float32 => {
            let values: Vec<Option<f32>> = rows
                .iter()
                .map(|row| row.get(name).and_then(|v| v.as_f64()).map(|v| v as f32))
                .collect();
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
                        builder
                            .values()
                            .append_value(val.as_f64().unwrap_or(0.0) as f32);
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
                builder.append_value(value as f32);
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

/// Detect duplicate values within a single `RecordBatch` for any of the
/// `unique_constraints` groups. Each group is a list of one or more columns
/// that together form a uniqueness key: a violation occurs when two rows share
/// the same tuple of values across *all* columns in a group, so a composite
/// `@unique(a, b)` only conflicts when both `a` and `b` match. Returns an
/// error on the first duplicate found.
///
/// Rows where any column in a group is null are exempt (standard SQL semantics
/// for uniqueness over nullable columns), as is any group whose columns are
/// not all present in the batch (e.g. a partial-schema load).
///
/// Note: this only catches duplicates *within* the batch. Cross-batch
/// uniqueness against already-committed rows is not enforced here — that
/// requires a dataset scan and is tracked separately.
pub(crate) fn enforce_unique_constraints_intra_batch(
    batch: &RecordBatch,
    type_name: &str,
    unique_constraints: &[Vec<String>],
) -> Result<()> {
    for columns in unique_constraints {
        // Resolve the group's columns once. A group whose columns aren't all
        // present in this batch is skipped (e.g. a partial-schema load).
        let Some(group_columns) = columns
            .iter()
            .map(|name| {
                batch
                    .schema()
                    .index_of(name)
                    .ok()
                    .map(|i| batch.column(i).clone())
            })
            .collect::<Option<Vec<ArrayRef>>>()
        else {
            continue;
        };
        let mut seen: HashMap<Vec<String>, usize> = HashMap::new();
        for row in 0..batch.num_rows() {
            let Some(key) = composite_unique_key(&group_columns, row)? else {
                continue;
            };
            if let Some(prev_row) = seen.insert(key.clone(), row) {
                return Err(OmniError::manifest(format!(
                    "@unique violation on {}.{}: value '{}' appears in rows {} and {}",
                    type_name,
                    format_tuple(columns),
                    format_tuple(&key),
                    prev_row,
                    row
                )));
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
/// - `Err(..)` propagated from [`unique_key_scalar`] on an un-keyable value.
///
/// Shared by the intake path (`enforce_unique_constraints_intra_batch`) and the
/// branch-merge path (`exec/merge.rs::update_unique_constraints`) so the two
/// derive identical keys and cannot drift on separator or scalar conversion.
pub(crate) fn composite_unique_key(
    group_columns: &[ArrayRef],
    row: usize,
) -> Result<Option<Vec<String>>> {
    let mut parts = Vec::with_capacity(group_columns.len());
    for column in group_columns {
        match unique_key_scalar(column, row)? {
            Some(value) => parts.push(value),
            None => return Ok(None),
        }
    }
    Ok(Some(parts))
}

/// Render a constraint's column tuple for error messages: a single item as
/// `col`, a composite as `(a, b)`. Used for both the column list and the
/// offending value tuple, which share the same shape.
fn format_tuple(items: &[String]) -> String {
    match items {
        [single] => single.clone(),
        _ => format!("({})", items.join(", ")),
    }
}

/// Reduce a single Arrow scalar at (`array`, `row`) to its uniqueness-key
/// string.
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
fn unique_key_scalar(array: &ArrayRef, row: usize) -> Result<Option<String>> {
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
        return Ok(Some(a.value(row).to_string()));
    }
    if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(Some(a.value(row).to_string()));
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
        "uniqueness key: unsupported column type {:?} for @unique/@key enforcement",
        array.data_type()
    )))
}

/// Build the list of uniqueness constraint groups to enforce on a node type.
/// Each group is the column tuple of one constraint. Includes every
/// `@unique(...)` constraint (from `NodeType.unique_constraints`) and the
/// `@key` (which implies uniqueness over its column tuple). Grouping is
/// preserved so a composite `@unique(a, b)` is enforced as a composite key
/// rather than degraded into independent single-field checks.
pub(crate) fn unique_constraint_groups_for_node(
    node_type: &omnigraph_compiler::catalog::NodeType,
) -> Vec<Vec<String>> {
    let mut groups: Vec<Vec<String>> = node_type.unique_constraints.clone();
    if let Some(key) = &node_type.key
        && !groups.contains(key)
    {
        groups.push(key.clone());
    }
    groups
}

/// Same as [`unique_constraint_groups_for_node`] but for an edge type (edges
/// have no `@key`).
pub(crate) fn unique_constraint_groups_for_edge(
    edge_type: &omnigraph_compiler::catalog::EdgeType,
) -> Vec<Vec<String>> {
    edge_type.unique_constraints.clone()
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

// ─── Edge cardinality validation ─────────────────────────────────────────────

pub(crate) async fn validate_edge_cardinality(
    db: &crate::db::Omnigraph,
    branch: Option<&str>,
    edge_name: &str,
    written_version: u64,
    written_branch: Option<&str>,
) -> Result<()> {
    use arrow_array::Array;
    let catalog = db.catalog();
    let edge_type = &catalog.edge_types[edge_name];
    if edge_type.cardinality.is_default() {
        return Ok(());
    }

    // Open edge sub-table at the just-written version, not the snapshot's
    // (the snapshot still pins to the pre-write version).
    let snapshot = db.snapshot_for_branch(branch).await?;
    let table_key = format!("edge:{}", edge_name);
    let entry = snapshot
        .entry(&table_key)
        .ok_or_else(|| OmniError::manifest(format!("no manifest entry for {}", table_key)))?;
    let ds = db
        .open_dataset_at_state(
            &entry.table_path,
            written_branch.or(entry.table_branch.as_deref()),
            written_version,
        )
        .await?;

    // Scan src column, count per source
    let batches = db.storage().scan(&ds, Some(&["src"]), None, None).await?;

    let mut counts: HashMap<String, u32> = HashMap::new();
    for batch in &batches {
        let srcs = batch
            .column_by_name("src")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..srcs.len() {
            *counts.entry(srcs.value(i).to_string()).or_insert(0) += 1;
        }
    }

    let card = &edge_type.cardinality;
    for (src, count) in &counts {
        if let Some(max) = card.max {
            if *count > max {
                return Err(OmniError::manifest(format!(
                    "@card violation on edge {}: source '{}' has {} edges (max {})",
                    edge_name, src, count, max
                )));
            }
        }
        if *count < card.min {
            return Err(OmniError::manifest(format!(
                "@card violation on edge {}: source '{}' has {} edges (min {})",
                edge_name, src, count, card.min
            )));
        }
    }

    Ok(())
}

/// Validate edge `@card` cardinality with in-memory pending edges visible.
///
/// Loader-level analog to `exec::mutation::validate_edge_cardinality_with_pending`:
/// opens the committed dataset at the pre-load snapshot version, then
/// delegates to the shared `count_src_per_edge` + `enforce_cardinality_bounds`
/// helpers in `exec::staging`. Used by Append/Merge loads (the Overwrite
/// path uses `validate_edge_cardinality` which opens the just-written
/// Lance version).
///
/// `mode` controls dedup behavior. `LoadMode::Merge` passes `Some("id")`
/// so committed edges that the load is *updating* (same edge id,
/// possibly changed `src`) are not double-counted. `LoadMode::Append`
/// passes `None` because each line generates a fresh ULID id that
/// never collides with committed.
async fn validate_edge_cardinality_with_pending_loader(
    db: &Omnigraph,
    branch: Option<&str>,
    edge_type: &omnigraph_compiler::catalog::EdgeType,
    table_key: &str,
    staging: &MutationStaging,
    mode: LoadMode,
) -> Result<()> {
    if edge_type.cardinality.is_default() {
        return Ok(());
    }
    let snapshot = db.snapshot_for_branch(branch).await?;
    let Some(entry) = snapshot.entry(table_key) else {
        // No manifest entry — table doesn't exist yet. Pending-only is
        // fine; the helper handles empty committed scans.
        return Ok(());
    };
    let ds = db
        .open_dataset_at_state(
            &entry.table_path,
            entry.table_branch.as_deref(),
            entry.table_version,
        )
        .await?;
    let dedupe_key = match mode {
        LoadMode::Merge => Some("id"),
        LoadMode::Append | LoadMode::Overwrite => None,
    };
    let counts =
        crate::exec::staging::count_src_per_edge(db, &ds, table_key, staging, dedupe_key).await?;
    crate::exec::staging::enforce_cardinality_bounds(edge_type, &counts)
}

/// Collect all valid node IDs for a given type, with in-memory pending
/// node inserts visible. Used by the staged loader's Phase 2c
/// referential-integrity validation.
///
/// Union of:
/// - IDs from the staged loader's pending batches (in-memory; just-staged
///   inserts of this type)
/// - IDs from the committed sub-table at the pre-load snapshot version
///
/// For `LoadMode::Overwrite`, if the node table is touched then the pending
/// batches are the replacement image. In that case committed IDs are not
/// included, so edge RI is validated against exactly what the overwrite will
/// publish.
async fn collect_node_ids_with_pending(
    db: &Omnigraph,
    branch: Option<&str>,
    type_name: &str,
    staging: &MutationStaging,
) -> Result<HashSet<String>> {
    let mut ids = HashSet::new();
    let table_key = format!("node:{}", type_name);

    // From staging.pending: walk the in-memory accumulator's id column.
    for batch in staging.pending_batches(&table_key) {
        if let Some(col) = batch.column_by_name("id") {
            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    if arr.is_valid(i) {
                        ids.insert(arr.value(i).to_string());
                    }
                }
            }
        }
    }

    if staging.pending_mode(&table_key) == Some(PendingMode::Overwrite) {
        return Ok(ids);
    }

    // From the committed Lance sub-table at the pre-load snapshot version.
    let snapshot = db.snapshot_for_branch(branch).await?;
    let Some(entry) = snapshot.entry(&table_key) else {
        return Ok(ids);
    };
    let ds = db
        .open_dataset_at_state(
            &entry.table_path,
            entry.table_branch.as_deref(),
            entry.table_version,
        )
        .await?;

    let batches = db.storage().scan(&ds, Some(&["id"]), None, None).await?;

    for batch in &batches {
        let id_col = batch
            .column_by_name("id")
            .ok_or_else(|| OmniError::Lance("missing 'id' column".into()))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| OmniError::Lance("'id' column is not Utf8".into()))?;
        for i in 0..batch.num_rows() {
            // Defensive: `id` is the @key column on every node type and
            // is non-nullable by schema, but a committed-row corruption
            // (or future schema change) could surface a NULL. Skip
            // rather than insert "" — pending-side does the same.
            if id_col.is_valid(i) {
                ids.insert(id_col.value(i).to_string());
            }
        }
    }

    Ok(ids)
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
        assert!(result.is_err(), "load without base must not create branches");
        assert!(
            !db.branch_list()
                .await
                .unwrap()
                .contains(&"nonexistent".to_string()),
            "failed load must not leave a branch behind"
        );

        // Loads to main carry the default branch metadata.
        let main_load = db.load("main", TEST_DATA, LoadMode::Overwrite).await.unwrap();
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
        let err = unique_key_scalar(&blob, 0).unwrap_err();
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
                unique_key_scalar(array, 0).unwrap(),
                Some("v".to_string()),
                "string array {:?} must render, not error",
                array.data_type()
            );
        }
    }
}
