use super::*;

pub(super) async fn plan_schema(
    db: &Omnigraph,
    desired_schema_source: &str,
) -> Result<SchemaMigrationPlan> {
    db.ensure_schema_state_valid().await?;
    let accepted_ir = read_accepted_schema_ir(db.uri(), Arc::clone(&db.storage)).await?;
    let desired_ir = read_schema_ir_from_source(desired_schema_source)?;
    plan_schema_migration(&accepted_ir, &desired_ir)
        .map_err(|err| OmniError::manifest(err.to_string()))
}

pub(super) async fn apply_schema(
    db: &mut Omnigraph,
    desired_schema_source: &str,
) -> Result<SchemaApplyResult> {
    acquire_schema_apply_lock(db).await?;
    let result = apply_schema_with_lock(db, desired_schema_source).await;
    let release_result = release_schema_apply_lock(db).await;
    match (result, release_result) {
        (Ok(result), Ok(())) => Ok(result),
        (Ok(_), Err(err)) => Err(err),
        (Err(err), Ok(())) => Err(err),
        (Err(err), Err(_)) => Err(err),
    }
}

pub(super) async fn apply_schema_with_lock(
    db: &mut Omnigraph,
    desired_schema_source: &str,
) -> Result<SchemaApplyResult> {
    db.ensure_schema_state_valid().await?;
    let branches = db.coordinator.all_branches().await?;
    // Skip `main` and internal system branches. The schema-apply lock branch
    // is excluded because it is the cluster-wide schema-apply serializer.
    // `__run__*` branches are no longer created; the filter remains as
    // defense-in-depth for legacy repos with leftover staging branches.
    // A future production sweep will let this guard go.
    let blocking_branches = branches
        .into_iter()
        .filter(|branch| branch != "main" && !is_internal_system_branch(branch))
        .collect::<Vec<_>>();
    if !blocking_branches.is_empty() {
        return Err(OmniError::manifest_conflict(format!(
            "schema apply requires a repo with only main; found non-main branches: {}",
            blocking_branches.join(", ")
        )));
    }

    let accepted_ir = read_accepted_schema_ir(db.uri(), Arc::clone(&db.storage)).await?;
    let desired_ir = read_schema_ir_from_source(desired_schema_source)?;
    let plan = plan_schema_migration(&accepted_ir, &desired_ir)
        .map_err(|err| OmniError::manifest(err.to_string()))?;
    if !plan.supported {
        let reason = plan
            .steps
            .iter()
            .find_map(|step| match step {
                SchemaMigrationStep::UnsupportedChange { reason, .. } => Some(reason.as_str()),
                _ => None,
            })
            .unwrap_or("unsupported schema migration plan");
        return Err(OmniError::manifest(reason.to_string()));
    }
    if plan.steps.is_empty() {
        return Ok(SchemaApplyResult {
            supported: true,
            applied: false,
            manifest_version: db.version(),
            steps: plan.steps,
        });
    }

    let mut desired_catalog = build_catalog_from_ir(&desired_ir)?;
    fixup_blob_schemas(&mut desired_catalog);

    let snapshot = db.snapshot();
    let base_manifest_version = snapshot.version();
    let mut added_tables = BTreeSet::new();
    let mut renamed_tables = HashMap::new();
    let mut rewritten_tables = BTreeSet::new();
    let mut indexed_tables = BTreeSet::new();
    let mut property_renames = HashMap::<String, HashMap<String, String>>::new();
    let mut changed_edge_tables = false;

    for step in &plan.steps {
        match step {
            SchemaMigrationStep::AddType { type_kind, name } => {
                let table_key = schema_table_key(*type_kind, name);
                if table_key.starts_with("edge:") {
                    changed_edge_tables = true;
                }
                added_tables.insert(table_key);
            }
            SchemaMigrationStep::RenameType {
                type_kind,
                from,
                to,
            } => {
                let source_key = schema_table_key(*type_kind, from);
                let target_key = schema_table_key(*type_kind, to);
                if source_key.starts_with("edge:") {
                    changed_edge_tables = true;
                }
                renamed_tables.insert(target_key, source_key);
            }
            SchemaMigrationStep::AddProperty {
                type_kind,
                type_name,
                ..
            } => {
                let table_key = schema_table_key(*type_kind, type_name);
                if table_key.starts_with("edge:") {
                    changed_edge_tables = true;
                }
                rewritten_tables.insert(table_key);
            }
            SchemaMigrationStep::RenameProperty {
                type_kind,
                type_name,
                from,
                to,
            } => {
                let table_key = schema_table_key(*type_kind, type_name);
                if table_key.starts_with("edge:") {
                    changed_edge_tables = true;
                }
                rewritten_tables.insert(table_key.clone());
                property_renames
                    .entry(table_key)
                    .or_default()
                    .insert(to.clone(), from.clone());
            }
            SchemaMigrationStep::AddConstraint {
                type_kind,
                type_name,
                ..
            } => {
                indexed_tables.insert(schema_table_key(*type_kind, type_name));
            }
            SchemaMigrationStep::UpdateTypeMetadata { .. }
            | SchemaMigrationStep::UpdatePropertyMetadata { .. } => {}
            SchemaMigrationStep::UnsupportedChange { reason, .. } => {
                return Err(OmniError::manifest(reason.clone()));
            }
        }
    }

    let mut table_registrations = HashMap::<String, String>::new();
    let mut table_updates = HashMap::<String, crate::db::SubTableUpdate>::new();
    let mut table_tombstones = HashMap::<String, u64>::new();

    for table_key in &added_tables {
        let table_path = table_path_for_table_key(table_key)?;
        let dataset_uri = db.table_store.dataset_uri(&table_path);
        let schema = schema_for_table_key(&desired_catalog, table_key)?;
        let mut ds = TableStore::create_empty_dataset(&dataset_uri, &schema).await?;
        db.build_indices_on_dataset_for_catalog(&desired_catalog, table_key, &mut ds)
            .await?;
        let state = db.table_store.table_state(&dataset_uri, &ds).await?;
        table_registrations.insert(table_key.clone(), table_path);
        table_updates.insert(
            table_key.clone(),
            crate::db::SubTableUpdate {
                table_key: table_key.clone(),
                table_version: state.version,
                table_branch: None,
                row_count: state.row_count,
                version_metadata: state.version_metadata,
            },
        );
    }

    for (target_table_key, source_table_key) in &renamed_tables {
        let source_entry = snapshot.entry(source_table_key).ok_or_else(|| {
            OmniError::manifest(format!(
                "missing source table '{}' for schema rename",
                source_table_key
            ))
        })?;
        ensure_snapshot_entry_head_matches(db, source_entry).await?;
        let source_ds = snapshot.open(source_table_key).await?;
        let batch = batch_for_schema_apply_rewrite(
            db,
            &source_ds,
            source_table_key,
            &db.catalog,
            target_table_key,
            &desired_catalog,
            property_renames.get(target_table_key),
        )
        .await?;
        let table_path = table_path_for_table_key(target_table_key)?;
        let dataset_uri = db.table_store.dataset_uri(&table_path);
        let mut target_ds = TableStore::write_dataset(&dataset_uri, batch).await?;
        db.build_indices_on_dataset_for_catalog(&desired_catalog, target_table_key, &mut target_ds)
            .await?;
        let state = db.table_store.table_state(&dataset_uri, &target_ds).await?;
        table_registrations.insert(target_table_key.clone(), table_path);
        table_updates.insert(
            target_table_key.clone(),
            crate::db::SubTableUpdate {
                table_key: target_table_key.clone(),
                table_version: state.version,
                table_branch: None,
                row_count: state.row_count,
                version_metadata: state.version_metadata,
            },
        );
        table_tombstones.insert(
            source_table_key.clone(),
            source_entry.table_version.saturating_add(1),
        );
    }

    for table_key in &rewritten_tables {
        if added_tables.contains(table_key) || renamed_tables.contains_key(table_key) {
            continue;
        }
        let entry = snapshot.entry(table_key).ok_or_else(|| {
            OmniError::manifest(format!(
                "missing source table '{}' for schema apply",
                table_key
            ))
        })?;
        ensure_snapshot_entry_head_matches(db, entry).await?;
        let source_ds = snapshot.open(table_key).await?;
        let batch = batch_for_schema_apply_rewrite(
            db,
            &source_ds,
            table_key,
            &db.catalog,
            table_key,
            &desired_catalog,
            property_renames.get(table_key),
        )
        .await?;
        let dataset_uri = db.table_store.dataset_uri(&entry.table_path);
        let mut target_ds = TableStore::overwrite_dataset(&dataset_uri, batch).await?;
        db.build_indices_on_dataset_for_catalog(&desired_catalog, table_key, &mut target_ds)
            .await?;
        let state = db.table_store.table_state(&dataset_uri, &target_ds).await?;
        table_updates.insert(
            table_key.clone(),
            crate::db::SubTableUpdate {
                table_key: table_key.clone(),
                table_version: state.version,
                table_branch: None,
                row_count: state.row_count,
                version_metadata: state.version_metadata,
            },
        );
    }

    for table_key in &indexed_tables {
        if added_tables.contains(table_key)
            || renamed_tables.contains_key(table_key)
            || rewritten_tables.contains(table_key)
        {
            continue;
        }
        let entry = snapshot.entry(table_key).ok_or_else(|| {
            OmniError::manifest(format!(
                "missing table '{}' for schema index apply",
                table_key
            ))
        })?;
        ensure_snapshot_entry_head_matches(db, entry).await?;
        let dataset_uri = db.table_store.dataset_uri(&entry.table_path);
        let mut ds = db
            .table_store
            .open_dataset_head_for_write(table_key, &dataset_uri, entry.table_branch.as_deref())
            .await?;
        db.table_store
            .ensure_expected_version(&ds, table_key, entry.table_version)?;
        db.build_indices_on_dataset_for_catalog(&desired_catalog, table_key, &mut ds)
            .await?;
        let state = db.table_store.table_state(&dataset_uri, &ds).await?;
        table_updates.insert(
            table_key.clone(),
            crate::db::SubTableUpdate {
                table_key: table_key.clone(),
                table_version: state.version,
                table_branch: None,
                row_count: state.row_count,
                version_metadata: state.version_metadata,
            },
        );
    }

    let mut manifest_changes = Vec::new();
    for (table_key, table_path) in table_registrations {
        manifest_changes.push(ManifestChange::RegisterTable(TableRegistration {
            table_key,
            table_path,
        }));
    }
    for update in table_updates.into_values() {
        manifest_changes.push(ManifestChange::Update(update));
    }
    for (table_key, tombstone_version) in table_tombstones {
        manifest_changes.push(ManifestChange::Tombstone(TableTombstone {
            table_key,
            tombstone_version,
        }));
    }

    db.refresh().await?;
    if db.version() != base_manifest_version {
        return Err(OmniError::manifest_conflict(format!(
            "schema apply lost its write lease: main advanced from v{} to v{} while schema apply was in progress",
            base_manifest_version,
            db.version()
        )));
    }

    // Atomic schema apply.
    //
    // Write the new schema source + IR contract to staging filenames first,
    // then commit the manifest, then rename staging → final. A crash
    // between these stages is recoverable on next open via
    // `recover_schema_state_files`:
    //   - crash before commit  → manifest unchanged; staging deleted on open
    //   - crash after commit   → manifest advanced; staging renamed on open
    let staging_pg_uri = schema_source_staging_uri(&db.root_uri);
    db.storage
        .write_text(&staging_pg_uri, desired_schema_source)
        .await?;
    write_schema_contract_staging(&db.root_uri, db.storage.as_ref(), &desired_ir).await?;

    crate::failpoints::maybe_fail("schema_apply.after_staging_write")?;

    let actor_id = db.current_audit_actor().map(str::to_string);
    let PublishedSnapshot {
        manifest_version,
        _snapshot_id: _,
    } = db
        .coordinator
        .commit_changes_with_actor(&manifest_changes, actor_id.as_deref())
        .await?;

    crate::failpoints::maybe_fail("schema_apply.after_manifest_commit")?;

    db.storage
        .rename_text(&staging_pg_uri, &schema_source_uri(&db.root_uri))
        .await?;
    db.storage
        .rename_text(
            &schema_ir_staging_uri(&db.root_uri),
            &schema_ir_uri(&db.root_uri),
        )
        .await?;
    db.storage
        .rename_text(
            &schema_state_staging_uri(&db.root_uri),
            &schema_state_uri(&db.root_uri),
        )
        .await?;

    db.catalog = desired_catalog;
    db.schema_source = desired_schema_source.to_string();
    db.coordinator.refresh().await?;
    db.runtime_cache.invalidate_all().await;
    if changed_edge_tables {
        db.invalidate_graph_index().await;
    }

    Ok(SchemaApplyResult {
        supported: true,
        applied: true,
        manifest_version,
        steps: plan.steps,
    })
}

pub(super) async fn ensure_schema_apply_idle(db: &mut Omnigraph, operation: &str) -> Result<()> {
    db.refresh().await?;
    ensure_schema_apply_not_locked(db, operation).await
}

pub(super) async fn acquire_schema_apply_lock(db: &mut Omnigraph) -> Result<()> {
    db.ensure_schema_state_valid().await?;
    db.refresh().await?;
    let branches = db.coordinator.all_branches().await?;
    if branches
        .iter()
        .any(|branch| is_schema_apply_lock_branch(branch))
    {
        return Err(OmniError::manifest_conflict(
            "schema apply is already in progress".to_string(),
        ));
    }

    db.coordinator
        .branch_create(SCHEMA_APPLY_LOCK_BRANCH)
        .await?;
    db.refresh().await?;

    let blocking_branches = db
        .coordinator
        .all_branches()
        .await?
        .into_iter()
        .filter(|branch| branch != "main" && !is_internal_system_branch(branch))
        .collect::<Vec<_>>();
    if !blocking_branches.is_empty() {
        let _ = release_schema_apply_lock(db).await;
        return Err(OmniError::manifest_conflict(format!(
            "schema apply requires a repo with only main; found non-main branches: {}",
            blocking_branches.join(", ")
        )));
    }

    Ok(())
}

pub(super) async fn release_schema_apply_lock(db: &mut Omnigraph) -> Result<()> {
    db.coordinator
        .branch_delete(SCHEMA_APPLY_LOCK_BRANCH)
        .await?;
    db.refresh().await
}

pub(super) async fn ensure_schema_apply_not_locked(db: &Omnigraph, operation: &str) -> Result<()> {
    if db
        .coordinator
        .all_branches()
        .await?
        .iter()
        .any(|branch| is_schema_apply_lock_branch(branch))
    {
        return Err(OmniError::manifest_conflict(format!(
            "{} is unavailable while schema apply is in progress",
            operation
        )));
    }
    Ok(())
}

pub(super) async fn ensure_snapshot_entry_head_matches(
    db: &Omnigraph,
    entry: &SubTableEntry,
) -> Result<()> {
    let dataset_uri = db.table_store.dataset_uri(&entry.table_path);
    let ds = db
        .table_store
        .open_dataset_head_for_write(
            &entry.table_key,
            &dataset_uri,
            entry.table_branch.as_deref(),
        )
        .await?;
    db.table_store
        .ensure_expected_version(&ds, &entry.table_key, entry.table_version)
}

pub(super) async fn batch_for_schema_apply_rewrite(
    db: &Omnigraph,
    source_ds: &Dataset,
    source_table_key: &str,
    source_catalog: &Catalog,
    target_table_key: &str,
    target_catalog: &Catalog,
    property_renames: Option<&HashMap<String, String>>,
) -> Result<RecordBatch> {
    let target_schema = schema_for_table_key(target_catalog, target_table_key)?;
    let source_blob_properties = blob_properties_for_table_key(source_catalog, source_table_key)?;
    let target_blob_properties = blob_properties_for_table_key(target_catalog, target_table_key)?;
    let needs_row_ids = !source_blob_properties.is_empty() || !target_blob_properties.is_empty();
    let batches = if needs_row_ids {
        db.table_store()
            .scan_with(source_ds, None, None, None, true, |_| Ok(()))
            .await?
    } else {
        db.table_store().scan_batches(source_ds).await?
    };
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(target_schema));
    }
    let source_schema = batches[0].schema();
    let batch = concat_or_empty_batches(source_schema, batches)?;

    let row_ids = if needs_row_ids {
        Some(
            batch
                .column_by_name("_rowid")
                .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| {
                    OmniError::Lance(format!(
                        "expected _rowid column when rewriting '{}'",
                        source_table_key
                    ))
                })?
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
        )
    } else {
        None
    };

    let mut columns = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields() {
        let source_name = property_renames
            .and_then(|renames| renames.get(field.name()))
            .map(String::as_str)
            .unwrap_or_else(|| field.name().as_str());
        if let Some(column) = batch.column_by_name(source_name) {
            if target_blob_properties.contains(field.name())
                && source_blob_properties.contains(source_name)
            {
                let descriptions =
                    column
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| {
                            OmniError::Lance(format!(
                                "expected blob descriptions for '{}.{}'",
                                source_table_key, source_name
                            ))
                        })?;
                let rebuilt = rebuild_blob_column(
                    db,
                    source_ds,
                    source_name,
                    descriptions,
                    row_ids.as_deref().unwrap_or(&[]),
                )
                .await?;
                columns.push(rebuilt);
            } else {
                columns.push(column.clone());
            }
        } else {
            columns.push(new_null_array(field.data_type(), batch.num_rows()));
        }
    }

    RecordBatch::try_new(target_schema, columns).map_err(|e| OmniError::Lance(e.to_string()))
}

async fn rebuild_blob_column(
    _db: &Omnigraph,
    source_ds: &Dataset,
    column_name: &str,
    descriptions: &StructArray,
    row_ids: &[u64],
) -> Result<Arc<dyn Array>> {
    let mut builder = BlobArrayBuilder::new(row_ids.len());
    let mut non_null_row_ids = Vec::new();
    let mut row_has_blob = Vec::with_capacity(row_ids.len());

    for row in 0..row_ids.len() {
        let is_null = blob_description_is_null(descriptions, row)?;
        row_has_blob.push(!is_null);
        if !is_null {
            non_null_row_ids.push(row_ids[row]);
        }
    }

    let blob_files = if non_null_row_ids.is_empty() {
        Vec::new()
    } else {
        Arc::new(source_ds.clone())
            .take_blobs(&non_null_row_ids, column_name)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?
    };

    let mut files = blob_files.into_iter();
    for has_blob in row_has_blob {
        if !has_blob {
            builder
                .push_null()
                .map_err(|e| OmniError::Lance(e.to_string()))?;
            continue;
        }

        let blob = files.next().ok_or_else(|| {
            OmniError::Lance(format!(
                "blob rewrite for '{}' lost alignment with source rows",
                column_name
            ))
        })?;
        if let Some(uri) = blob.uri() {
            builder
                .push_uri(uri)
                .map_err(|e| OmniError::Lance(e.to_string()))?;
        } else {
            builder
                .push_bytes(
                    blob.read()
                        .await
                        .map_err(|e| OmniError::Lance(e.to_string()))?,
                )
                .map_err(|e| OmniError::Lance(e.to_string()))?;
        }
    }

    if files.next().is_some() {
        return Err(OmniError::Lance(format!(
            "blob rewrite for '{}' produced extra source blobs",
            column_name
        )));
    }

    builder
        .finish()
        .map_err(|e| OmniError::Lance(e.to_string()))
}
