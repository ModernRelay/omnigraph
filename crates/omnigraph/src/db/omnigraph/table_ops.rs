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

    if !updates.is_empty() {
        commit_prepared_updates_on_branch(db, branch, &updates).await?;
    }

    Ok(())
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
            db.table_store
                .create_btree_index(ds, &["id"])
                .await
                .map_err(|e| {
                    OmniError::Lance(format!("create BTree index on {}(id): {}", table_key, e))
                })?;
        }

        if let Some(node_type) = catalog.node_types.get(type_name) {
            for index_cols in &node_type.indices {
                if index_cols.len() != 1 {
                    continue;
                }
                let prop_name = &index_cols[0];
                if let Some(prop_type) = node_type.properties.get(prop_name) {
                    if matches!(prop_type.scalar, ScalarType::String) && !prop_type.list {
                        if !db.table_store.has_fts_index(ds, prop_name).await? {
                            db.table_store
                                .create_inverted_index(ds, prop_name.as_str())
                                .await
                                .map_err(|e| {
                                    OmniError::Lance(format!(
                                        "create Inverted index on {}({}): {}",
                                        table_key, prop_name, e
                                    ))
                                })?;
                        }
                    } else if matches!(prop_type.scalar, ScalarType::Vector(_)) && !prop_type.list {
                        if !db.table_store.has_vector_index(ds, prop_name).await? {
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
            db.table_store
                .create_btree_index(ds, &["id"])
                .await
                .map_err(|e| {
                    OmniError::Lance(format!("create BTree index on {}(id): {}", table_key, e))
                })?;
        }
        if !db.table_store.has_btree_index(ds, "src").await? {
            db.table_store
                .create_btree_index(ds, &["src"])
                .await
                .map_err(|e| {
                    OmniError::Lance(format!("create BTree index on {}(src): {}", table_key, e))
                })?;
        }
        if !db.table_store.has_btree_index(ds, "dst").await? {
            db.table_store
                .create_btree_index(ds, &["dst"])
                .await
                .map_err(|e| {
                    OmniError::Lance(format!("create BTree index on {}(dst): {}", table_key, e))
                })?;
        }
        return Ok(());
    }

    Err(OmniError::manifest(format!(
        "invalid table key '{}'",
        table_key
    )))
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
