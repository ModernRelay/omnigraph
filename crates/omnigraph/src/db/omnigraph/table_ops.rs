use super::*;

pub(super) async fn graph_index(db: &Omnigraph) -> Result<Arc<crate::graph_index::GraphIndex>> {
    db.ensure_schema_state_valid().await?;
    let resolved = db
        .coordinator
        .resolve_target(&ReadTarget::Branch(
            db.coordinator
                .current_branch()
                .unwrap_or("main")
                .to_string(),
        ))
        .await?;
    db.runtime_cache.graph_index(&resolved, &db.catalog).await
}

pub(super) async fn graph_index_for_resolved(
    db: &Omnigraph,
    resolved: &ResolvedTarget,
) -> Result<Arc<crate::graph_index::GraphIndex>> {
    db.runtime_cache.graph_index(resolved, &db.catalog).await
}

pub(super) async fn ensure_indices(db: &mut Omnigraph) -> Result<()> {
    let current_branch = db.coordinator.current_branch().map(str::to_string);
    ensure_indices_for_branch(db, current_branch.as_deref()).await
}

pub(super) async fn ensure_indices_on(db: &mut Omnigraph, branch: &str) -> Result<()> {
    let branch = normalize_branch_name(branch)?;
    ensure_indices_for_branch(db, branch.as_deref()).await
}

pub(super) async fn ensure_indices_for_branch(
    db: &mut Omnigraph,
    branch: Option<&str>,
) -> Result<()> {
    db.ensure_schema_state_valid().await?;
    db.ensure_schema_apply_idle("ensure_indices").await?;
    let resolved = db.resolved_branch_target(branch).await?;
    let snapshot = resolved.snapshot;
    let mut updates = Vec::new();
    let active_branch = resolved.branch;

    // Recovery sidecar: protect the per-table commit_staged loop in
    // build_indices_on_dataset (one commit per index built). Only pins
    // tables that ACTUALLY need index work — the classifier
    // loose-matches for SidecarKind::EnsureIndices (the actual N
    // depends on which indices are missing), but if a table needs zero
    // commits and gets pinned, the all-or-nothing decision rule
    // classifies it as `NoMovement` and rolls back legitimately-
    // committed work on sibling tables. Steady-state runs (everything
    // already indexed) skip the sidecar entirely.
    let mut recovery_pins: Vec<crate::db::manifest::SidecarTablePin> = Vec::new();
    for type_name in db.catalog.node_types.keys() {
        let table_key = format!("node:{}", type_name);
        let Some(entry) = snapshot.entry(&table_key) else {
            continue;
        };
        // Match the processing loop's branch filter: when running on a
        // feature branch, main-branch tables (table_branch = None) are
        // skipped (`None => continue` at ~line 118). Pinning them here
        // would force NoMovement on recovery and trigger an all-or-
        // nothing rollback of legitimately-committed work on the
        // feature-branch tables.
        if active_branch.is_some() && entry.table_branch.is_none() {
            continue;
        }
        let full_path = format!("{}/{}", db.root_uri, entry.table_path);
        if needs_index_work_node(
            db,
            type_name,
            &table_key,
            &full_path,
            entry.table_branch.as_deref(),
        )
        .await?
        {
            recovery_pins.push(crate::db::manifest::SidecarTablePin {
                table_key,
                table_path: full_path,
                expected_version: entry.table_version,
                post_commit_pin: entry.table_version + 1,
            });
        }
    }
    for edge_name in db.catalog.edge_types.keys() {
        let table_key = format!("edge:{}", edge_name);
        let Some(entry) = snapshot.entry(&table_key) else {
            continue;
        };
        if active_branch.is_some() && entry.table_branch.is_none() {
            continue;
        }
        let full_path = format!("{}/{}", db.root_uri, entry.table_path);
        if needs_index_work_edge(db, &table_key, &full_path, entry.table_branch.as_deref())
            .await?
        {
            recovery_pins.push(crate::db::manifest::SidecarTablePin {
                table_key,
                table_path: full_path,
                expected_version: entry.table_version,
                post_commit_pin: entry.table_version + 1,
            });
        }
    }
    let recovery_handle = if recovery_pins.is_empty() {
        None
    } else {
        let sidecar = crate::db::manifest::new_sidecar(
            crate::db::manifest::SidecarKind::EnsureIndices,
            active_branch.clone(),
            db.audit_actor_id.clone(),
            recovery_pins,
        );
        Some(
            crate::db::manifest::write_sidecar(db.root_uri(), db.storage_adapter(), &sidecar)
                .await?,
        )
    };

    for type_name in db.catalog.node_types.keys() {
        let table_key = format!("node:{}", type_name);
        let Some(entry) = snapshot.entry(&table_key) else {
            continue;
        };
        let full_path = format!("{}/{}", db.root_uri, entry.table_path);
        let (mut ds, resolved_branch) = match active_branch.as_deref() {
            Some(active_branch) => match entry.table_branch.as_deref() {
                None => continue,
                _ => {
                    open_owned_dataset_for_branch_write(
                        db,
                        &table_key,
                        &full_path,
                        entry.table_branch.as_deref(),
                        entry.table_version,
                        active_branch,
                    )
                    .await?
                }
            },
            None => (
                db.table_store
                    .open_dataset_head_for_write(&table_key, &full_path, None)
                    .await?,
                None,
            ),
        };
        let row_count = db.table_store.count_rows(&ds, None).await.unwrap_or(0);
        if row_count > 0 {
            build_indices_on_dataset(db, &table_key, &mut ds).await?;
        }

        let state = db.table_store.table_state(&full_path, &ds).await?;
        if state.version != entry.table_version
            || resolved_branch.as_deref() != entry.table_branch.as_deref()
        {
            updates.push(crate::db::SubTableUpdate {
                table_key,
                table_version: state.version,
                table_branch: resolved_branch,
                row_count: state.row_count,
                version_metadata: state.version_metadata,
            });
        }
    }

    for edge_name in db.catalog.edge_types.keys() {
        let table_key = format!("edge:{}", edge_name);
        let Some(entry) = snapshot.entry(&table_key) else {
            continue;
        };
        let full_path = format!("{}/{}", db.root_uri, entry.table_path);
        let (mut ds, resolved_branch) = match active_branch.as_deref() {
            Some(active_branch) => match entry.table_branch.as_deref() {
                None => continue,
                _ => {
                    open_owned_dataset_for_branch_write(
                        db,
                        &table_key,
                        &full_path,
                        entry.table_branch.as_deref(),
                        entry.table_version,
                        active_branch,
                    )
                    .await?
                }
            },
            None => (
                db.table_store
                    .open_dataset_head_for_write(&table_key, &full_path, None)
                    .await?,
                None,
            ),
        };
        let row_count = db.table_store.count_rows(&ds, None).await.unwrap_or(0);
        if row_count > 0 {
            build_indices_on_dataset(db, &table_key, &mut ds).await?;
        }

        let state = db.table_store.table_state(&full_path, &ds).await?;
        if state.version != entry.table_version
            || resolved_branch.as_deref() != entry.table_branch.as_deref()
        {
            updates.push(crate::db::SubTableUpdate {
                table_key,
                table_version: state.version,
                table_branch: resolved_branch,
                row_count: state.row_count,
                version_metadata: state.version_metadata,
            });
        }
    }

    // Failpoint: pin the per-writer Phase B → Phase C residual for
    // ensure_indices. Lance HEAD has advanced on every touched table
    // (one commit_staged per index built) but the manifest publish below
    // hasn't run. Used by
    // `tests/failpoints.rs::ensure_indices_phase_b_failure_recovered_on_next_open`.
    crate::failpoints::maybe_fail("ensure_indices.post_phase_b_pre_manifest_commit")?;

    if !updates.is_empty() {
        commit_prepared_updates_on_branch(db, branch, &updates).await?;
    }

    // Recovery sidecar lifecycle: delete after the manifest publish (or
    // no-op when there were no updates — the sidecar covered the
    // per-table commit window regardless). Best-effort cleanup; failing
    // the user here would error a call that already succeeded.
    if let Some(handle) = recovery_handle {
        if let Err(err) =
            crate::db::manifest::delete_sidecar(&handle, db.storage_adapter()).await
        {
            tracing::warn!(
                error = %err,
                operation_id = handle.operation_id.as_str(),
                "recovery sidecar cleanup failed; the next open's recovery sweep will resolve it"
            );
        }
    }

    Ok(())
}

/// Returns true if the node table is missing at least one declared
/// scalar/vector index that `build_indices_on_dataset_for_catalog` would
/// build AND has at least one row (the ensure_indices loop has
/// `if row_count > 0 { build_indices(...) }`, so empty tables produce
/// zero commits and must NOT be pinned in the sidecar — pinning them
/// would force `NoMovement` classification on recovery and trigger the
/// all-or-nothing rollback of sibling tables' legitimate index work).
///
/// Per the actual `build_indices_on_dataset_for_catalog` implementation
/// (this file, ~line 419-491), nodes get BTree (id) + per-prop FTS
/// (@search String) + per-prop Vector indices; edges get BTree only
/// (id, src, dst). The two helpers mirror that asymmetry — see the
/// `needs_index_work_edge` doc comment.
async fn needs_index_work_node(
    db: &Omnigraph,
    type_name: &str,
    table_key: &str,
    full_path: &str,
    table_branch: Option<&str>,
) -> Result<bool> {
    let ds = db
        .table_store
        .open_dataset_head_for_write(table_key, full_path, table_branch)
        .await?;
    // Empty tables are skipped by the ensure_indices loop, so they must
    // not be pinned in the sidecar — pinning a table that produces zero
    // commits classifies as NoMovement on recovery and forces all-or-
    // nothing rollback of sibling tables' legitimate index work.
    // Errors from count_rows are propagated: silently treating them as
    // "0 rows" risks skipping a table that is actually about to be
    // modified.
    if db.table_store.count_rows(&ds, None).await? == 0 {
        return Ok(false);
    }
    if !db.table_store.has_btree_index(&ds, "id").await? {
        return Ok(true);
    }
    let Some(node_type) = db.catalog.node_types.get(type_name) else {
        return Ok(false);
    };
    for index_cols in &node_type.indices {
        if index_cols.len() != 1 {
            continue;
        }
        let prop_name = &index_cols[0];
        let Some(prop_type) = node_type.properties.get(prop_name) else {
            continue;
        };
        if matches!(prop_type.scalar, ScalarType::String) && !prop_type.list {
            if !db.table_store.has_fts_index(&ds, prop_name).await? {
                return Ok(true);
            }
        } else if matches!(prop_type.scalar, ScalarType::Vector(_)) && !prop_type.list {
            if !db.table_store.has_vector_index(&ds, prop_name).await? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Companion to `needs_index_work_node` for edge tables.
///
/// **Intentional asymmetry with the node helper**: edges only need
/// BTree indices (id, src, dst) per `build_indices_on_dataset_for_catalog`
/// at the edge branch (this file, lines 474-485). FTS / vector indices
/// on edge properties are not built today; if they ever are, this
/// helper plus the build function must be updated together.
///
/// Empty edge tables are skipped by the ensure_indices loop the same
/// way node tables are; see `needs_index_work_node`.
async fn needs_index_work_edge(
    db: &Omnigraph,
    table_key: &str,
    full_path: &str,
    table_branch: Option<&str>,
) -> Result<bool> {
    let ds = db
        .table_store
        .open_dataset_head_for_write(table_key, full_path, table_branch)
        .await?;
    if db.table_store.count_rows(&ds, None).await? == 0 {
        return Ok(false);
    }
    Ok(!db.table_store.has_btree_index(&ds, "id").await?
        || !db.table_store.has_btree_index(&ds, "src").await?
        || !db.table_store.has_btree_index(&ds, "dst").await?)
}

pub(super) async fn open_for_mutation(
    db: &Omnigraph,
    table_key: &str,
) -> Result<(Dataset, String, Option<String>)> {
    let current_branch = db.coordinator.current_branch().map(str::to_string);
    open_for_mutation_on_branch(db, current_branch.as_deref(), table_key).await
}

pub(super) async fn open_for_mutation_on_branch(
    db: &Omnigraph,
    branch: Option<&str>,
    table_key: &str,
) -> Result<(Dataset, String, Option<String>)> {
    db.ensure_schema_apply_not_locked("write").await?;
    let resolved = db.resolved_branch_target(branch).await?;
    let entry = resolved
        .snapshot
        .entry(table_key)
        .ok_or_else(|| OmniError::manifest(format!("no manifest entry for {}", table_key)))?;
    let full_path = format!("{}/{}", db.root_uri, entry.table_path);
    match resolved.branch.as_deref() {
        None => {
            let ds = db
                .table_store
                .open_dataset_head_for_write(table_key, &full_path, None)
                .await?;
            db.table_store
                .ensure_expected_version(&ds, table_key, entry.table_version)?;
            Ok((ds, full_path, None))
        }
        Some(active_branch) => {
            let (ds, table_branch) = open_owned_dataset_for_branch_write(
                db,
                table_key,
                &full_path,
                entry.table_branch.as_deref(),
                entry.table_version,
                active_branch,
            )
            .await?;
            Ok((ds, full_path, table_branch))
        }
    }
}

pub(super) async fn open_owned_dataset_for_branch_write(
    db: &Omnigraph,
    table_key: &str,
    full_path: &str,
    entry_branch: Option<&str>,
    entry_version: u64,
    active_branch: &str,
) -> Result<(Dataset, Option<String>)> {
    match entry_branch {
        Some(branch) if branch == active_branch => {
            let ds = db
                .table_store
                .open_dataset_head_for_write(table_key, full_path, Some(active_branch))
                .await?;
            db.table_store
                .ensure_expected_version(&ds, table_key, entry_version)?;
            Ok((ds, Some(active_branch.to_string())))
        }
        source_branch => {
            fork_dataset_from_entry_state(
                db,
                table_key,
                full_path,
                source_branch,
                entry_version,
                active_branch,
            )
            .await?;
            let ds = db
                .table_store
                .open_dataset_head_for_write(table_key, full_path, Some(active_branch))
                .await?;
            db.table_store
                .ensure_expected_version(&ds, table_key, entry_version)?;
            Ok((ds, Some(active_branch.to_string())))
        }
    }
}

pub(super) async fn fork_dataset_from_entry_state(
    db: &Omnigraph,
    table_key: &str,
    full_path: &str,
    source_branch: Option<&str>,
    source_version: u64,
    active_branch: &str,
) -> Result<Dataset> {
    db.table_store
        .fork_branch_from_state(
            full_path,
            source_branch,
            table_key,
            source_version,
            active_branch,
        )
        .await
}

pub(super) async fn reopen_for_mutation(
    db: &Omnigraph,
    table_key: &str,
    full_path: &str,
    table_branch: Option<&str>,
    expected_version: u64,
) -> Result<Dataset> {
    db.ensure_schema_apply_not_locked("write").await?;
    db.table_store
        .reopen_for_mutation(full_path, table_branch, table_key, expected_version)
        .await
}

pub(super) async fn open_dataset_at_state(
    db: &Omnigraph,
    table_path: &str,
    table_branch: Option<&str>,
    table_version: u64,
) -> Result<Dataset> {
    db.table_store
        .open_dataset_at_state(table_path, table_branch, table_version)
        .await
}

pub(super) async fn build_indices_on_dataset(
    db: &Omnigraph,
    table_key: &str,
    ds: &mut Dataset,
) -> Result<()> {
    build_indices_on_dataset_for_catalog(db, &db.catalog, table_key, ds).await
}

pub(super) async fn build_indices_on_dataset_for_catalog(
    db: &Omnigraph,
    catalog: &Catalog,
    table_key: &str,
    ds: &mut Dataset,
) -> Result<()> {
    if let Some(type_name) = table_key.strip_prefix("node:") {
        if !db.table_store.has_btree_index(ds, "id").await? {
            stage_and_commit_btree(db, table_key, ds, &["id"]).await?;
        }

        if let Some(node_type) = catalog.node_types.get(type_name) {
            // Stage scalar indices first (BTree, Inverted), then call
            // `create_vector_index` inline. The inline-commit on a vector
            // index advances HEAD, which would invalidate any uncommitted
            // scalar index transactions if we stacked them. Today the
            // per-stage shape commits each scalar index immediately so
            // the order constraint is implicit, but if we ever batch
            // scalar stages we must ensure they all land before the
            // vector inline-commit.
            for index_cols in &node_type.indices {
                if index_cols.len() != 1 {
                    continue;
                }
                let prop_name = &index_cols[0];
                if let Some(prop_type) = node_type.properties.get(prop_name) {
                    if matches!(prop_type.scalar, ScalarType::String) && !prop_type.list {
                        if !db.table_store.has_fts_index(ds, prop_name).await? {
                            stage_and_commit_inverted(db, table_key, ds, prop_name.as_str())
                                .await?;
                        }
                    } else if matches!(prop_type.scalar, ScalarType::Vector(_)) && !prop_type.list {
                        if !db.table_store.has_vector_index(ds, prop_name).await? {
                            // Inline-commit residual: lance-4.0.0 does not
                            // expose `build_index_metadata_from_segments` as
                            // `pub`, so vector indices cannot be staged from
                            // outside the lance crate. Document at the call
                            // site; companion ticket to lance-format/lance#6658.
                            db.table_store
                                .create_vector_index(ds, prop_name.as_str())
                                .await
                                .map_err(|e| {
                                    OmniError::Lance(format!(
                                        "create Vector index on {}({}): {}",
                                        table_key, prop_name, e
                                    ))
                                })?;
                        }
                    }
                }
            }
        }
        return Ok(());
    }

    if table_key.starts_with("edge:") {
        if !db.table_store.has_btree_index(ds, "id").await? {
            stage_and_commit_btree(db, table_key, ds, &["id"]).await?;
        }
        if !db.table_store.has_btree_index(ds, "src").await? {
            stage_and_commit_btree(db, table_key, ds, &["src"]).await?;
        }
        if !db.table_store.has_btree_index(ds, "dst").await? {
            stage_and_commit_btree(db, table_key, ds, &["dst"]).await?;
        }
        return Ok(());
    }

    Err(OmniError::manifest(format!(
        "invalid table key '{}'",
        table_key
    )))
}

/// Stage a BTREE index transaction and commit it, advancing the in-memory
/// `*ds` to the new HEAD. The staged primitive + immediate `commit_staged`
/// pair replaced the earlier inline-commit `create_btree_index(ds)` call.
/// Per-call behavior is unchanged (HEAD advances once per index), but
/// the bytes-on-disk and HEAD-advance are now decoupled at the
/// `TableStore` API surface — a caller that needs end-of-batch atomicity
/// can stage many transactions and commit them in one pass (the eventual
/// index reconciler relies on this).
async fn stage_and_commit_btree(
    db: &Omnigraph,
    table_key: &str,
    ds: &mut Dataset,
    columns: &[&str],
) -> Result<()> {
    let staged = db
        .table_store
        .stage_create_btree_index(ds, columns)
        .await
        .map_err(|e| {
            OmniError::Lance(format!(
                "stage_create_btree_index on {}({:?}): {}",
                table_key, columns, e
            ))
        })?;
    // Failpoint between stage and commit. Used by `tests/failpoints.rs`
    // to demonstrate that a Phase A failure in the staged-index path
    // leaves no Lance-HEAD drift on the touched table.
    crate::failpoints::maybe_fail("ensure_indices.post_stage_pre_commit_btree")?;
    let new_ds = db
        .table_store
        .commit_staged(Arc::new(ds.clone()), staged.transaction)
        .await
        .map_err(|e| {
            OmniError::Lance(format!(
                "commit BTree index on {}({:?}): {}",
                table_key, columns, e
            ))
        })?;
    *ds = new_ds;
    Ok(())
}

/// Stage an INVERTED (FTS) index transaction and commit it. See
/// `stage_and_commit_btree` for the rationale.
async fn stage_and_commit_inverted(
    db: &Omnigraph,
    table_key: &str,
    ds: &mut Dataset,
    column: &str,
) -> Result<()> {
    let staged = db
        .table_store
        .stage_create_inverted_index(ds, column)
        .await
        .map_err(|e| {
            OmniError::Lance(format!(
                "stage_create_inverted_index on {}({}): {}",
                table_key, column, e
            ))
        })?;
    let new_ds = db
        .table_store
        .commit_staged(Arc::new(ds.clone()), staged.transaction)
        .await
        .map_err(|e| {
            OmniError::Lance(format!(
                "commit Inverted index on {}({}): {}",
                table_key, column, e
            ))
        })?;
    *ds = new_ds;
    Ok(())
}

async fn prepare_updates_for_commit(
    db: &Omnigraph,
    branch: Option<&str>,
    updates: &[crate::db::SubTableUpdate],
) -> Result<Vec<crate::db::SubTableUpdate>> {
    if updates.is_empty() {
        return Ok(Vec::new());
    }

    let snapshot = db.snapshot_for_branch(branch).await?;
    let mut prepared = Vec::with_capacity(updates.len());

    for update in updates {
        let Some(entry) = snapshot.entry(&update.table_key) else {
            return Err(OmniError::manifest(format!(
                "no manifest entry for {}",
                update.table_key
            )));
        };

        let mut prepared_update = update.clone();
        if prepared_update.row_count > 0 {
            let full_path = format!("{}/{}", db.root_uri, entry.table_path);
            let mut ds = reopen_for_mutation(
                db,
                &prepared_update.table_key,
                &full_path,
                prepared_update.table_branch.as_deref(),
                prepared_update.table_version,
            )
            .await?;
            build_indices_on_dataset(db, &prepared_update.table_key, &mut ds).await?;
            let state = db.table_store.table_state(&full_path, &ds).await?;
            prepared_update.table_version = state.version;
            prepared_update.row_count = state.row_count;
            prepared_update.version_metadata = state.version_metadata;
        }

        prepared.push(prepared_update);
    }

    Ok(prepared)
}

async fn commit_prepared_updates(
    db: &mut Omnigraph,
    updates: &[crate::db::SubTableUpdate],
) -> Result<u64> {
    let actor_id = db.current_audit_actor().map(str::to_string);
    let PublishedSnapshot {
        manifest_version,
        _snapshot_id: _,
    } = db
        .coordinator
        .commit_updates_with_actor(updates, actor_id.as_deref())
        .await?;
    Ok(manifest_version)
}

async fn commit_prepared_updates_with_expected(
    db: &mut Omnigraph,
    updates: &[crate::db::SubTableUpdate],
    expected_table_versions: &std::collections::HashMap<String, u64>,
) -> Result<u64> {
    let actor_id = db.current_audit_actor().map(str::to_string);
    let PublishedSnapshot {
        manifest_version,
        _snapshot_id: _,
    } = db
        .coordinator
        .commit_updates_with_actor_with_expected(
            updates,
            expected_table_versions,
            actor_id.as_deref(),
        )
        .await?;
    Ok(manifest_version)
}

pub(super) async fn commit_prepared_updates_on_branch(
    db: &mut Omnigraph,
    branch: Option<&str>,
    updates: &[crate::db::SubTableUpdate],
) -> Result<u64> {
    let current_branch = db.coordinator.current_branch().map(str::to_string);
    let requested_branch = branch.map(str::to_string);
    if requested_branch == current_branch {
        return commit_prepared_updates(db, updates).await;
    }

    let mut coordinator = match requested_branch.as_deref() {
        Some(branch) => {
            GraphCoordinator::open_branch(db.uri(), branch, Arc::clone(&db.storage)).await?
        }
        None => GraphCoordinator::open(db.uri(), Arc::clone(&db.storage)).await?,
    };
    let actor_id = db.current_audit_actor().map(str::to_string);
    let PublishedSnapshot {
        manifest_version,
        _snapshot_id: _,
    } = coordinator
        .commit_updates_with_actor(updates, actor_id.as_deref())
        .await?;
    Ok(manifest_version)
}

pub(super) async fn commit_prepared_updates_on_branch_with_expected(
    db: &mut Omnigraph,
    branch: Option<&str>,
    updates: &[crate::db::SubTableUpdate],
    expected_table_versions: &std::collections::HashMap<String, u64>,
) -> Result<u64> {
    let current_branch = db.coordinator.current_branch().map(str::to_string);
    let requested_branch = branch.map(str::to_string);
    if requested_branch == current_branch {
        return commit_prepared_updates_with_expected(db, updates, expected_table_versions).await;
    }

    let mut coordinator = match requested_branch.as_deref() {
        Some(branch) => {
            GraphCoordinator::open_branch(db.uri(), branch, Arc::clone(&db.storage)).await?
        }
        None => GraphCoordinator::open(db.uri(), Arc::clone(&db.storage)).await?,
    };
    let actor_id = db.current_audit_actor().map(str::to_string);
    let PublishedSnapshot {
        manifest_version,
        _snapshot_id: _,
    } = coordinator
        .commit_updates_with_actor_with_expected(
            updates,
            expected_table_versions,
            actor_id.as_deref(),
        )
        .await?;
    Ok(manifest_version)
}

// Used only by in-tree tests (`#[cfg(test)]`); the runtime path now uses
// `commit_updates_on_branch_with_expected` exclusively.
#[cfg(test)]
pub(super) async fn commit_updates(
    db: &mut Omnigraph,
    updates: &[crate::db::SubTableUpdate],
) -> Result<u64> {
    db.ensure_schema_apply_not_locked("write commit").await?;
    let current_branch = db.coordinator.current_branch().map(str::to_string);
    let prepared = prepare_updates_for_commit(db, current_branch.as_deref(), updates).await?;
    commit_prepared_updates(db, &prepared).await
}

pub(super) async fn commit_manifest_updates(
    db: &mut Omnigraph,
    updates: &[crate::db::SubTableUpdate],
) -> Result<u64> {
    db.coordinator.commit_manifest_updates(updates).await
}

pub(super) async fn record_merge_commit(
    db: &mut Omnigraph,
    manifest_version: u64,
    parent_commit_id: &str,
    merged_parent_commit_id: &str,
) -> Result<String> {
    let actor_id = db.current_audit_actor().map(str::to_string);
    db.coordinator
        .record_merge_commit(
            manifest_version,
            parent_commit_id,
            merged_parent_commit_id,
            actor_id.as_deref(),
        )
        .await
        .map(|snapshot_id| snapshot_id.as_str().to_string())
}

/// Commit updates with a publisher-level OCC fence. The
/// `expected_table_versions` map asserts the manifest's pre-write per-table
/// versions; mismatches surface as `ManifestConflictDetails::ExpectedVersionMismatch`.
pub(super) async fn commit_updates_on_branch_with_expected(
    db: &mut Omnigraph,
    branch: Option<&str>,
    updates: &[crate::db::SubTableUpdate],
    expected_table_versions: &std::collections::HashMap<String, u64>,
) -> Result<u64> {
    db.ensure_schema_apply_not_locked("write commit").await?;
    let prepared = prepare_updates_for_commit(db, branch, updates).await?;
    commit_prepared_updates_on_branch_with_expected(db, branch, &prepared, expected_table_versions)
        .await
}

pub(super) async fn ensure_commit_graph_initialized(db: &mut Omnigraph) -> Result<()> {
    db.coordinator.ensure_commit_graph_initialized().await
}

pub(super) async fn invalidate_graph_index(db: &Omnigraph) {
    db.runtime_cache.invalidate_all().await;
}
