use super::*;

pub(super) async fn graph_index(db: &Omnigraph) -> Result<Arc<crate::graph_index::GraphIndex>> {
    let (resolved, catalog) = db.capture_current_read_view().await?;
    // Whole-graph entry point: cover every edge type. Query execution scopes to
    // the edges it actually traverses (see `referenced_edge_types`).
    let edge_types: std::collections::HashMap<String, (String, String)> = catalog
        .edge_types
        .iter()
        .map(|(name, et)| (name.clone(), (et.from_type.clone(), et.to_type.clone())))
        .collect();
    db.runtime_cache.graph_index(&resolved, &edge_types).await
}

pub(super) async fn graph_index_for_resolved(
    db: &Omnigraph,
    resolved: &ResolvedTarget,
    edge_types: &std::collections::HashMap<String, (String, String)>,
) -> Result<Arc<crate::graph_index::GraphIndex>> {
    db.runtime_cache.graph_index(resolved, edge_types).await
}

pub(super) async fn ensure_indices(db: &Omnigraph) -> Result<Vec<PendingIndex>> {
    let current_branch = db
        .coordinator
        .read()
        .await
        .current_branch()
        .map(str::to_string);
    ensure_indices_for_branch(db, current_branch.as_deref()).await
}

pub(super) async fn ensure_indices_on(db: &Omnigraph, branch: &str) -> Result<Vec<PendingIndex>> {
    let branch = normalize_branch_name(branch)?;
    ensure_indices_for_branch(db, branch.as_deref()).await
}

#[cfg(feature = "failpoints")]
pub(super) async fn failpoint_publish_table_head_without_index_rebuild_for_test(
    db: &mut Omnigraph,
    branch: &str,
    table_key: &str,
    table_branch: Option<&str>,
) -> Result<u64> {
    let branch = normalize_branch_name(branch)?;
    let snapshot = db.snapshot_for_branch(branch.as_deref()).await?;
    let entry = snapshot
        .entry(table_key)
        .ok_or_else(|| OmniError::manifest(format!("no manifest entry for {}", table_key)))?;
    let full_path = format!("{}/{}", db.root_uri, entry.table_path);
    let ds = db
        .storage()
        .open_dataset_head(&full_path, table_branch)
        .await?;
    let state = db.storage().table_state(&full_path, &ds).await?;
    let update = crate::db::SubTableUpdate {
        identity: entry.identity,
        table_key: table_key.to_string(),
        table_version: state.version,
        table_branch: table_branch.map(str::to_string),
        row_count: state.row_count,
        version_metadata: state.version_metadata,
    };
    let mut expected = crate::db::manifest::ExpectedTableVersions::new();
    expected.insert(
        entry.identity,
        crate::db::manifest::TableVersionExpectation {
            table_key: table_key.to_string(),
            table_version: entry.table_version,
        },
    );
    commit_prepared_updates_on_branch_with_expected(
        db,
        branch.as_deref(),
        &[update],
        &expected,
        None,
    )
    .await
}

pub(super) async fn ensure_indices_for_branch(
    db: &Omnigraph,
    branch: Option<&str>,
) -> Result<Vec<PendingIndex>> {
    // RFC-022 entry recovery barrier: recovery may advance the manifest, so
    // resolve or refuse every relevant intent before capturing the index
    // plan's base.
    // The final under-gate relist below remains necessary to close the race
    // between this entry barrier and the first table-HEAD effect.
    db.heal_pending_recovery_sidecars_for_write(&[branch])
        .await?;
    db.ensure_schema_apply_idle("ensure_indices").await?;
    let txn = db.open_write_txn(branch).await?;
    let snapshot = txn.base.clone();
    let mut pending_by_table = HashMap::<String, Vec<PendingIndex>>::new();
    let active_branch = txn.branch.clone();
    let catalog = Arc::clone(&txn.catalog);

    let mut recovery_pins: Vec<crate::db::manifest::SidecarTablePin> = Vec::new();
    let mut work_by_table = HashMap::<String, PlannedIndexWork>::new();
    let mut existing_targets =
        std::collections::HashMap::<String, crate::storage_layer::SnapshotHandle>::new();
    let mut existing_staged =
        std::collections::HashMap::<String, crate::storage_layer::StagedHandle>::new();
    let mut first_touch_sources =
        std::collections::HashMap::<String, crate::storage_layer::SnapshotHandle>::new();
    let mut planned_transactions = std::collections::HashMap::<
        crate::db::manifest::TableIdentity,
        crate::table_store::StagedTransactionIdentity,
    >::new();
    let mut first_touch_source_versions =
        std::collections::HashMap::<crate::db::manifest::TableIdentity, u64>::new();

    // Plan and build uncommitted artifacts before taking writer gates. Existing
    // target refs have a stable branch-local `_indices` root, so their complete
    // BTREE/FTS/vector batch can be staged here and abandoned safely if final
    // authority revalidation loses. A first-touch named ref does not have that
    // root yet; its artifacts must be staged after sidecar -> ref creation.
    for type_name in catalog.node_types.keys() {
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
        let first_touch =
            active_branch.is_some() && entry.table_branch.as_deref() != active_branch.as_deref();
        let ds = if first_touch {
            // The inherited owner's HEAD may advance independently after this
            // graph branch was cut. Plan from the exact inherited snapshot, not
            // from that owner's current HEAD.
            db.storage().open_snapshot_at_entry(entry).await?
        } else {
            db.storage()
                .open_dataset_head(&full_path, active_branch.as_deref())
                .await?
        };
        let work = plan_index_work_node(db, &catalog, type_name, &table_key, &ds).await?;
        if !work.pending.is_empty() {
            pending_by_table.insert(table_key.clone(), work.pending.clone());
        }
        if work.needs_commit() {
            recovery_pins.push(crate::db::manifest::SidecarTablePin {
                identity: entry.identity,
                table_key: table_key.clone(),
                table_path: full_path,
                expected_version: entry.table_version,
                post_commit_pin: entry.table_version + 1,
                confirmed_version: None,
                table_branch: active_branch.clone(),
            });
            if !first_touch {
                let staged = db
                    .storage()
                    .stage_create_indices(&ds, &work.specs)
                    .await
                    .map_err(|error| {
                        OmniError::Lance(format!(
                            "stage index batch on {} ({:?}): {}",
                            table_key, work.specs, error
                        ))
                    })?;
                crate::failpoints::maybe_fail(
                    crate::failpoints::names::ENSURE_INDICES_POST_STAGE_PRE_COMMIT_BTREE,
                )?;
                planned_transactions.insert(entry.identity, staged.transaction_identity());
                existing_staged.insert(table_key.clone(), staged);
                existing_targets.insert(table_key.clone(), ds);
            } else {
                planned_transactions.insert(
                    entry.identity,
                    pre_minted_index_transaction(entry.table_version),
                );
                first_touch_source_versions.insert(entry.identity, entry.table_version);
                first_touch_sources.insert(table_key.clone(), ds);
            }
            work_by_table.insert(table_key, work);
        }
    }
    for edge_name in catalog.edge_types.keys() {
        let table_key = format!("edge:{}", edge_name);
        let Some(entry) = snapshot.entry(&table_key) else {
            continue;
        };
        if active_branch.is_some() && entry.table_branch.is_none() {
            continue;
        }
        let full_path = format!("{}/{}", db.root_uri, entry.table_path);
        let first_touch =
            active_branch.is_some() && entry.table_branch.as_deref() != active_branch.as_deref();
        let ds = if first_touch {
            db.storage().open_snapshot_at_entry(entry).await?
        } else {
            db.storage()
                .open_dataset_head(&full_path, active_branch.as_deref())
                .await?
        };
        let work = plan_index_work_edge_on_dataset(db, &ds).await?;
        if work.needs_commit() {
            recovery_pins.push(crate::db::manifest::SidecarTablePin {
                identity: entry.identity,
                table_key: table_key.clone(),
                table_path: full_path,
                expected_version: entry.table_version,
                post_commit_pin: entry.table_version + 1,
                confirmed_version: None,
                table_branch: active_branch.clone(),
            });
            if !first_touch {
                let staged = db
                    .storage()
                    .stage_create_indices(&ds, &work.specs)
                    .await
                    .map_err(|error| {
                        OmniError::Lance(format!(
                            "stage index batch on {} ({:?}): {}",
                            table_key, work.specs, error
                        ))
                    })?;
                crate::failpoints::maybe_fail(
                    crate::failpoints::names::ENSURE_INDICES_POST_STAGE_PRE_COMMIT_BTREE,
                )?;
                planned_transactions.insert(entry.identity, staged.transaction_identity());
                existing_staged.insert(table_key.clone(), staged);
                existing_targets.insert(table_key.clone(), ds);
            } else {
                planned_transactions.insert(
                    entry.identity,
                    pre_minted_index_transaction(entry.table_version),
                );
                first_touch_source_versions.insert(entry.identity, entry.table_version);
                first_touch_sources.insert(table_key.clone(), ds);
            }
            work_by_table.insert(table_key, work);
        }
    }

    let queue_keys: Vec<(String, Option<String>)> = recovery_pins
        .iter()
        .map(|pin| (pin.table_key.clone(), pin.table_branch.clone()))
        .collect();
    let _schema_guard = db
        .write_queue()
        .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
        .await;
    let _branch_guard = db
        .write_queue()
        .acquire_branch(active_branch.as_deref())
        .await;
    let _queue_guards = db.write_queue().acquire_many(&queue_keys).await;

    db.ensure_no_pending_recovery_sidecars_under_gates(
        &[active_branch.as_deref()],
        "ensure_indices",
    )
    .await?;
    let live_snapshot = db.revalidate_write_txn(&txn).await?;

    for pin in &recovery_pins {
        let prepared_entry = snapshot.entry(&pin.table_key).ok_or_else(|| {
            OmniError::manifest_conflict(format!(
                "table '{}' disappeared from the prepared index plan",
                pin.table_key,
            ))
        })?;
        let live_entry = live_snapshot.entry(&pin.table_key).ok_or_else(|| {
            OmniError::manifest_read_set_changed(
                format!("table_head:{}", pin.table_key),
                Some(pin.expected_version.to_string()),
                None,
            )
        })?;
        if live_entry.table_version != pin.expected_version
            || live_entry.identity != pin.identity
            || live_entry.table_path != prepared_entry.table_path
            || live_entry.table_branch != prepared_entry.table_branch
        {
            return Err(OmniError::manifest_read_set_changed(
                format!("table_head:{}", pin.table_key),
                Some(format!(
                    "{}:{}:{}",
                    prepared_entry.table_path,
                    prepared_entry.table_branch.as_deref().unwrap_or("main"),
                    pin.expected_version,
                )),
                Some(format!(
                    "{}:{}:{}",
                    live_entry.table_path,
                    live_entry.table_branch.as_deref().unwrap_or("main"),
                    live_entry.table_version,
                )),
            ));
        }

        if let Some(ds) = existing_targets.get(&pin.table_key) {
            db.ensure_existing_effect_baseline(
                &pin.table_key,
                pin.table_branch.as_deref(),
                pin.expected_version,
                ds,
            )
            .await?;
        } else if let Some(source) = first_touch_sources.get(&pin.table_key) {
            let target_branch = active_branch.as_deref().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "first-touch index target '{}' has no active named branch",
                    pin.table_key,
                ))
            })?;
            let branches = source
                .dataset()
                .list_branches()
                .await
                .map_err(|error| OmniError::Lance(error.to_string()))?;
            if branches.contains_key(target_branch) {
                return Err(OmniError::manifest_conflict(format!(
                    "index target ref '{}:{}' already exists while the graph manifest still \
                     inherits the table from another branch; refusing to claim unowned \
                     physical state — inspect and remove the orphaned ref before retrying",
                    pin.table_key, target_branch,
                )));
            }
        }
    }

    if recovery_pins.is_empty() {
        // Preserve the no-work failpoint contract without manufacturing durable
        // recovery state or graph lineage.
        crate::failpoints::maybe_fail(
            crate::failpoints::names::ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT,
        )?;
    } else {
        let expected_versions = recovery_pins
            .iter()
            .map(|pin| {
                (
                    pin.identity,
                    crate::db::manifest::TableVersionExpectation {
                        table_key: pin.table_key.clone(),
                        table_version: pin.expected_version,
                    },
                )
            })
            .collect::<crate::db::manifest::ExpectedTableVersions>();
        let lineage = db
            .new_lineage_intent_for_branch(active_branch.as_deref(), None)
            .await?;
        let authority = crate::db::manifest::RecoveryAuthorityToken {
            branch_identifier: txn.authority.branch_identifier.clone(),
            graph_head: txn.authority.graph_head.clone(),
            schema_identity_domain: txn.authority.schema_identity_domain.clone(),
            schema_ir_hash: txn.authority.schema_ir_hash.clone(),
            schema_identity_version: txn.authority.schema_identity_version,
        };
        let recovery_lineage = crate::db::manifest::RecoveryLineageIntent {
            graph_commit_id: lineage.graph_commit_id.clone(),
            branch: lineage.branch.clone(),
            actor_id: lineage.actor_id.clone(),
            merged_parent_commit_id: lineage.merged_parent_commit_id.clone(),
            created_at: lineage.created_at,
        };
        let mut sidecar = crate::db::manifest::new_ensure_indices_sidecar_v9(
            active_branch.clone(),
            None,
            recovery_pins.clone(),
            authority,
            recovery_lineage,
            planned_transactions.clone(),
            first_touch_source_versions.clone(),
        )?;
        let recovery_handle =
            crate::db::manifest::write_sidecar(db.root_uri(), db.storage_adapter(), &sidecar)
                .await?;
        let recovery_operation_id = recovery_handle.operation_id.clone();

        let post_arm_result = async {
            if !first_touch_sources.is_empty() {
                crate::failpoints::maybe_fail(
                    crate::failpoints::names::ENSURE_INDICES_POST_SIDECAR_PRE_FORK,
                )?;
            }

            let mut updates = Vec::with_capacity(recovery_pins.len());
            let mut committed_transactions = HashMap::new();
            let mut confirmed_ref_identifiers = HashMap::new();
            for pin in &recovery_pins {
                let table_key = pin.table_key.clone();
                let entry = snapshot.entry(&table_key).ok_or_else(|| {
                    OmniError::manifest(format!("no manifest entry for {}", table_key))
                })?;
                let full_path = pin.table_path.clone();
                let first_touch = first_touch_source_versions.contains_key(&pin.identity);
                let (ds, resolved_branch) = match active_branch.as_deref() {
                    Some(active_branch) => {
                        if let Some(ds) = existing_targets.remove(&table_key) {
                            (ds, Some(active_branch.to_string()))
                        } else {
                            first_touch_sources.remove(&table_key).ok_or_else(|| {
                                OmniError::manifest_internal(format!(
                                    "missing first-touch source for index table '{}'",
                                    table_key
                                ))
                            })?;
                            open_owned_dataset_for_branch_write(
                                db,
                                &table_key,
                                pin.identity,
                                &full_path,
                                entry.table_branch.as_deref(),
                                entry.table_version,
                                active_branch,
                                crate::db::MutationOpKind::SchemaRewrite,
                                true,
                            )
                            .await?
                        }
                    }
                    None => (
                        existing_targets.remove(&table_key).ok_or_else(|| {
                            OmniError::manifest_internal(format!(
                                "missing verified existing target for main table '{}'",
                                table_key,
                            ))
                        })?,
                        None,
                    ),
                };

                let mut staged = if let Some(staged) = existing_staged.remove(&table_key) {
                    staged
                } else {
                    let work = work_by_table.get(&table_key).ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "missing exact index work for first-touch table '{}'",
                            table_key
                        ))
                    })?;
                    let staged = db
                        .storage()
                        .stage_create_indices(&ds, &work.specs)
                        .await
                        .map_err(|error| {
                            OmniError::Lance(format!(
                                "stage first-touch index batch on {} ({:?}): {}",
                                table_key, work.specs, error
                            ))
                        })?;
                    crate::failpoints::maybe_fail(
                        crate::failpoints::names::ENSURE_INDICES_POST_STAGE_PRE_COMMIT_BTREE,
                    )?;
                    staged
                };
                let planned = planned_transactions.get(&pin.identity).ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "missing planned index transaction for '{}'",
                        table_key
                    ))
                })?;
                staged.bind_transaction_identity(planned)?;
                let outcome = db.storage().commit_staged_exact(ds, staged).await?;
                if !outcome.is_exact() {
                    return Err(OmniError::manifest_read_set_changed(
                        format!("ensure_indices_lance_transaction:{table_key}"),
                        Some(format!("{:?}", outcome.planned_transaction())),
                        Some(format!("{:?}", outcome.committed_transaction())),
                    ));
                }
                committed_transactions
                    .insert(pin.identity, outcome.committed_transaction().clone());
                let ds = outcome.into_snapshot();
                if first_touch {
                    confirmed_ref_identifiers
                        .insert(pin.identity, db.storage().branch_identifier(&ds).await?);
                }
                let state = db.storage().table_state(&full_path, &ds).await?;
                updates.push(crate::db::SubTableUpdate {
                    identity: pin.identity,
                    table_key,
                    table_version: state.version,
                    table_branch: resolved_branch,
                    row_count: state.row_count,
                    version_metadata: state.version_metadata,
                });
                crate::failpoints::maybe_fail(
                    crate::failpoints::names::ENSURE_INDICES_POST_TABLE_EFFECT,
                )?;
            }

            crate::failpoints::maybe_fail(
                crate::failpoints::names::ENSURE_INDICES_POST_EFFECTS_PRE_CONFIRM,
            )?;
            crate::db::manifest::confirm_ensure_indices_sidecar_v9(
                db.root_uri(),
                db.storage_adapter(),
                &mut sidecar,
                &updates,
                &committed_transactions,
                &confirmed_ref_identifiers,
            )
            .await?;
            crate::failpoints::maybe_fail(
                crate::failpoints::names::ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT,
            )?;
            commit_updates_on_branch_with_expected(
                db,
                active_branch.as_deref(),
                &updates,
                &expected_versions,
                None,
                &txn,
                lineage,
            )
            .await?;
            Ok::<(), OmniError>(())
        }
        .await;

        if let Err(error) = post_arm_result {
            return Err(OmniError::recovery_required(
                recovery_operation_id,
                error.to_string(),
            ));
        }

        if let Err(err) =
            crate::db::manifest::delete_sidecar(&recovery_handle, db.storage_adapter()).await
        {
            tracing::warn!(
                error = %err,
                operation_id = recovery_handle.operation_id.as_str(),
                "recovery sidecar cleanup failed; the next open's recovery sweep will resolve it"
            );
        }
    }

    // Preserve the historical, observable catalog order even though planning
    // and physical effects now happen in separate phases.
    let mut pending = Vec::new();
    for type_name in catalog.node_types.keys() {
        if let Some(mut table_pending) = pending_by_table.remove(&format!("node:{type_name}")) {
            pending.append(&mut table_pending);
        }
    }
    Ok(pending)
}

fn pre_minted_index_transaction(
    read_version: u64,
) -> crate::table_store::StagedTransactionIdentity {
    crate::table_store::StagedTransactionIdentity {
        read_version,
        uuid: format!("omnigraph-index-{}", ulid::Ulid::new()),
    }
}

/// The single scalar/vector index a node property receives from a one-column
/// `@index`/`@key` declaration, or `None` when the property type is not
/// indexable here (a list column or `Blob`).
///
/// Shared by `build_indices_on_dataset_for_catalog` (which builds the index)
/// and `index_work_status_on_dataset_for_catalog` (which decides recovery-
/// sidecar pinning) so the two cannot drift: an enum or orderable scalar the
/// builder gives a BTREE must also be reported as "needs work" until that
/// BTREE exists, or the HEAD-advancing build would run without sidecar cover.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum NodePropIndexKind {
    Btree,
    Fts,
    Vector,
}

fn node_prop_index_kind(prop_type: &PropType) -> Option<NodePropIndexKind> {
    if prop_type.list {
        return None;
    }
    // Enums are physically `String` but filtered by equality, so they take a
    // scalar BTREE, not an FTS inverted index (Lance never consults an inverted
    // index for `=`/range). Free-text Strings keep FTS for
    // `search()`/`match_text`/`bm25`.
    let is_enum = prop_type.enum_values.is_some();
    match prop_type.scalar {
        ScalarType::String if !is_enum => Some(NodePropIndexKind::Fts),
        ScalarType::Vector(_) => Some(NodePropIndexKind::Vector),
        ScalarType::String
        | ScalarType::DateTime
        | ScalarType::Date
        | ScalarType::I32
        | ScalarType::I64
        | ScalarType::U32
        | ScalarType::U64
        | ScalarType::F32
        | ScalarType::F64
        | ScalarType::Bool => Some(NodePropIndexKind::Btree),
        ScalarType::Blob => None,
    }
}

/// Whether a vector column currently has at least one non-null vector — the
/// minimum for Lance IVF k-means to train (the `ivf_flat(1)` index we build
/// needs >=1 vector). Used identically by index-work status planning (so an
/// untrainable column is not pinned for recovery — avoiding a zero-commit pin
/// that would roll back a sibling's index work) and by the vector build arm (so
/// vector staging is only attempted when it can succeed, keeping genuine
/// build errors fatal instead of swallowed as pending). If index params
/// become size-aware (dev-graph iss-687), this threshold moves with them.
async fn vector_column_trainable(
    db: &Omnigraph,
    ds: &SnapshotHandle,
    column: &str,
) -> Result<bool> {
    Ok(db
        .storage()
        .count_rows(ds, Some(format!("{column} IS NOT NULL")))
        .await?
        > 0)
}

/// Describes the index work visible from one already-selected dataset snapshot.
/// `needs_commit` excludes pending-only work (today an untrainable vector
/// column), which is load-bearing for multi-table recovery: a table that cannot
/// advance HEAD must not become a zero-movement pin beside productive siblings.
pub(super) struct IndexWorkStatus {
    pub(super) needs_commit: bool,
    pub(super) pending: Vec<PendingIndex>,
}

/// Returns the declared scalar/vector index work that
/// `build_indices_on_dataset_for_catalog` would build, plus pending-only work,
/// for one operation-local accepted catalog and one selected dataset snapshot.
/// The table must have at least one row (the ensure_indices loop has
/// `if row_count > 0 { build_indices(...) }`, so empty tables produce
/// zero commits and must NOT be pinned in the sidecar — pinning them
/// would force `NoMovement` classification on recovery and trigger the
/// all-or-nothing rollback of sibling tables' legitimate index work).
///
/// Per `build_indices_on_dataset_for_catalog`, nodes get BTree (id) plus, for
/// each one-column `@index`/`@key` property, the index `node_prop_index_kind`
/// assigns: a scalar BTREE for enums and orderable scalars
/// (DateTime/Date/numeric/Bool), FTS for free-text Strings, or a Vector index.
/// Edges get BTree only (id, src, dst). This helper and the builder share
/// `node_prop_index_kind` so they cannot drift — see its doc comment.
#[derive(Default)]
struct PlannedIndexWork {
    specs: Vec<crate::storage_layer::IndexBuildSpec>,
    pending: Vec<PendingIndex>,
}

impl PlannedIndexWork {
    fn needs_commit(&self) -> bool {
        !self.specs.is_empty()
    }

    /// A property can reach the catalog's index-intent list through more than
    /// one annotation (for example `@key` plus an explicit `@index`). The old
    /// per-index commit loop observed the first committed index before visiting
    /// the duplicate intent; a one-snapshot batch must deduplicate explicitly.
    fn push_spec(&mut self, spec: crate::storage_layer::IndexBuildSpec) {
        if !self.specs.contains(&spec) {
            self.specs.push(spec);
        }
    }
}

/// Classify index work against one already-selected dataset snapshot and one
/// operation-local accepted catalog. Keeping the opener outside this helper is
/// load-bearing for named-branch first touch: an inherited graph entry must be
/// checked at its exact pinned version, not at the inherited owner's newer HEAD.
async fn plan_index_work_node(
    db: &Omnigraph,
    catalog: &Catalog,
    type_name: &str,
    table_key: &str,
    ds: &SnapshotHandle,
) -> Result<PlannedIndexWork> {
    if db.storage().count_rows(ds, None).await? == 0 {
        return Ok(PlannedIndexWork::default());
    }

    let mut work = PlannedIndexWork::default();
    if !db.storage().has_btree_index(ds, "id").await? {
        work.push_spec(crate::storage_layer::IndexBuildSpec::BTree {
            column: "id".to_string(),
        });
    }
    let Some(node_type) = catalog.node_types.get(type_name) else {
        return Ok(work);
    };
    for index_cols in &node_type.indices {
        if index_cols.len() != 1 {
            continue;
        }
        let prop_name = &index_cols[0];
        let Some(prop_type) = node_type.properties.get(prop_name) else {
            continue;
        };
        match node_prop_index_kind(prop_type) {
            Some(NodePropIndexKind::Fts) => {
                if !db.storage().has_fts_index(ds, prop_name).await? {
                    work.push_spec(crate::storage_layer::IndexBuildSpec::FullText {
                        column: prop_name.clone(),
                    });
                }
            }
            Some(NodePropIndexKind::Vector) => {
                if !db.storage().has_vector_index(ds, prop_name).await? {
                    if vector_column_trainable(db, ds, prop_name).await? {
                        work.push_spec(crate::storage_layer::IndexBuildSpec::Vector {
                            column: prop_name.clone(),
                        });
                    } else {
                        work.pending.push(PendingIndex {
                            table_key: table_key.to_string(),
                            column: prop_name.clone(),
                            reason: "column has no non-null vectors to train on yet".to_string(),
                        });
                    }
                }
            }
            Some(NodePropIndexKind::Btree) => {
                if !db.storage().has_btree_index(ds, prop_name).await? {
                    work.push_spec(crate::storage_layer::IndexBuildSpec::BTree {
                        column: prop_name.clone(),
                    });
                }
            }
            None => {}
        }
    }
    Ok(work)
}

pub(super) async fn index_work_status_on_dataset_for_catalog(
    db: &Omnigraph,
    catalog: &Catalog,
    table_key: &str,
    ds: &SnapshotHandle,
) -> Result<IndexWorkStatus> {
    let work = if let Some(type_name) = table_key.strip_prefix("node:") {
        plan_index_work_node(db, catalog, type_name, table_key, ds).await?
    } else if table_key.starts_with("edge:") {
        // Intentional asymmetry: edges only receive the id/src/dst BTREEs.
        plan_index_work_edge_on_dataset(db, ds).await?
    } else {
        return Err(OmniError::manifest(format!(
            "invalid table key '{}'",
            table_key
        )));
    };
    Ok(IndexWorkStatus {
        needs_commit: work.needs_commit(),
        pending: work.pending,
    })
}

async fn plan_index_work_edge_on_dataset(
    db: &Omnigraph,
    ds: &SnapshotHandle,
) -> Result<PlannedIndexWork> {
    if db.storage().count_rows(&ds, None).await? == 0 {
        return Ok(PlannedIndexWork::default());
    }
    let mut work = PlannedIndexWork::default();
    for column in ["id", "src", "dst"] {
        if !db.storage().has_btree_index(ds, column).await? {
            work.push_spec(crate::storage_layer::IndexBuildSpec::BTree {
                column: column.to_string(),
            });
        }
    }
    Ok(work)
}

/// Result of opening a sub-table for mutation. `handle` is `None` only when a
/// non-strict (Insert/Merge) op on the WriteTxn's own branch skipped the
/// accumulation open (RFC-013 step 3b collapse #1) — there the caller needs just
/// `expected_version`. It is ALWAYS `Some` for strict ops, the fork path, and
/// every no-`txn` caller (branch merge), which use [`Self::require_handle`].
#[derive(Debug)]
pub(crate) struct OpenedForMutation {
    /// Immutable logical table lifetime captured from the same manifest entry
    /// as `expected_version`. Writers carry this through recovery and publish;
    /// the mutable alias is never used as OCC identity.
    pub(crate) identity: crate::db::manifest::TableIdentity,
    /// The opened dataset, or `None` on the non-strict-txn open-skip path.
    pub(crate) handle: Option<SnapshotHandle>,
    /// The publisher's CAS fence: the opened handle's version, or — when the open
    /// was skipped — the pinned base entry's version (equal absent uncovered drift).
    pub(crate) expected_version: u64,
    pub(crate) full_path: String,
    pub(crate) table_branch: Option<String>,
    /// RFC-022 first-touch named-branch writes stage against the inherited
    /// source snapshot and defer the durable Lance ref creation until after
    /// their v9 recovery intent (`protocol_v3` payload) is armed in
    /// `StagedMutation::commit_all`.
    pub(crate) deferred_fork: Option<DeferredTableFork>,
}

#[derive(Debug, Clone)]
pub(crate) struct DeferredTableFork {
    pub(crate) source_entry: crate::db::SubTableEntry,
    pub(crate) target_branch: String,
}

impl OpenedForMutation {
    /// Destructure for a caller that REQUIRES the handle (strict ops, the fork
    /// path, every no-`txn` caller). The `None` skip fires solely on the
    /// non-strict `txn` path, which these callers are not — so a panic here means
    /// a future change broke that contract, named by `ctx`.
    pub(crate) fn require_handle(self, ctx: &str) -> (SnapshotHandle, String, Option<String>) {
        let handle = self.handle.unwrap_or_else(|| {
            panic!("{ctx}: open_for_mutation returned no handle on a path that requires one")
        });
        (handle, self.full_path, self.table_branch)
    }
}

pub(super) async fn open_for_mutation(
    db: &Omnigraph,
    table_key: &str,
    op_kind: crate::db::MutationOpKind,
) -> Result<OpenedForMutation> {
    let current_branch = db
        .coordinator
        .read()
        .await
        .current_branch()
        .map(str::to_string);
    // `open_for_mutation` is the no-txn entry (branch merge). Passing `None`
    // keeps the exact pre-WriteTxn code path (a fresh `resolved_branch_target`
    // that re-validates the schema). With `txn = None` the non-strict early-skip
    // in `open_for_mutation_on_branch` never fires, so this always returns a
    // `Some(handle)` for its callers.
    open_for_mutation_on_branch(db, current_branch.as_deref(), table_key, op_kind, None).await
}

/// Open a sub-table for mutation. The `op_kind` selects the strict-vs-relaxed
/// pre-stage version-check policy — see [`crate::db::MutationOpKind`] for the
/// rationale per kind. Insert / Merge skip the strict
/// `ensure_expected_version` check (Lance's natural conflict resolver +
/// per-(table, branch) queue + publisher CAS handle drift); Update / Delete /
/// SchemaRewrite keep it (read-modify-write SI).
pub(super) async fn open_for_mutation_on_branch(
    db: &Omnigraph,
    branch: Option<&str>,
    table_key: &str,
    op_kind: crate::db::MutationOpKind,
    txn: Option<&crate::db::WriteTxn>,
) -> Result<OpenedForMutation> {
    db.ensure_schema_apply_not_locked("write").await?;
    // Source the resolved (snapshot, branch). With a `WriteTxn` the contract was
    // validated once at capture, so use the pinned base + resolved branch instead
    // of `resolved_branch_target` (which re-runs `ensure_schema_state_valid`). The
    // base is the same fresh per-branch manifest read the no-txn path would have
    // resolved — only the redundant schema re-validation is dropped. Without a txn
    // this is byte-identical to the prior `resolved_branch_target` call.
    let (snapshot, resolved_branch) = match txn {
        Some(txn) => (txn.base.clone(), txn.branch.clone()),
        None => {
            let resolved = db.resolved_branch_target(branch).await?;
            (resolved.snapshot, resolved.branch)
        }
    };
    let entry = snapshot
        .entry(table_key)
        .ok_or_else(|| OmniError::manifest(format!("no manifest entry for {}", table_key)))?;
    let full_path = format!("{}/{}", db.root_uri, entry.table_path);

    // Collapse #1 (RFC-013 step 3b): a non-strict op (Insert/Merge) on the txn's
    // own branch needs no dataset open for ACCUMULATION — the only thing the
    // caller reads from this handle on the non-strict path is `.version()` (the
    // publisher's CAS fence), which is exactly the pinned base version. The base
    // already validated the schema contract once, and the staging reopen
    // (`reopen_for_mutation`) plus the publisher CAS in `commit_all` are the real
    // drift guards. So skip `open_dataset_head` entirely and source the
    // expected version from the pinned entry.
    //
    // Gated on `txn.is_some()`: without a txn (branch merge's `open_for_mutation`)
    // every arm below is byte-identical to before. STRICT ops (Update/Delete/
    // SchemaRewrite) always open live HEAD + run `ensure_expected_version`
    // (read-modify-write SI), and any write that must FORK (the table isn't yet on
    // the resolved branch) opens too (the fork is a real Lance state advance the
    // manifest snapshot can't substitute for).
    if txn.is_some() && !op_kind.strict_pre_stage_version_check() {
        match resolved_branch.as_deref() {
            // Non-strict, table already on the active branch → no open, no fork.
            Some(active_branch) if entry.table_branch.as_deref() == Some(active_branch) => {
                return Ok(OpenedForMutation {
                    identity: entry.identity,
                    handle: None,
                    expected_version: entry.table_version,
                    full_path,
                    table_branch: Some(active_branch.to_string()),
                    deferred_fork: None,
                });
            }
            // Main branch, non-strict → no open. (Main never forks.)
            None => {
                return Ok(OpenedForMutation {
                    identity: entry.identity,
                    handle: None,
                    expected_version: entry.table_version,
                    full_path,
                    table_branch: None,
                    deferred_fork: None,
                });
            }
            // Non-strict but the table isn't on the active branch yet — falls
            // through to fork below.
            Some(_) => {}
        }
    }

    match resolved_branch.as_deref() {
        None => {
            let ds = db.storage().open_dataset_head(&full_path, None).await?;
            if op_kind.strict_pre_stage_version_check() {
                if txn.is_some() && ds.version() != entry.table_version {
                    return Err(OmniError::manifest_read_set_changed(
                        format!("table_head:{table_key}"),
                        Some(entry.table_version.to_string()),
                        Some(ds.version().to_string()),
                    ));
                }
                if txn.is_none() {
                    db.storage()
                        .ensure_expected_version(&ds, table_key, entry.table_version)?;
                }
            }
            let version = ds.version();
            Ok(OpenedForMutation {
                identity: entry.identity,
                handle: Some(ds),
                expected_version: version,
                full_path,
                table_branch: None,
                deferred_fork: None,
            })
        }
        Some(active_branch) => {
            // RFC-022-enrolled mutation/load adapters must arm durable intent
            // before creating a per-table Lance branch ref. Read and stage from
            // the inherited source entry now; `commit_all` creates the target
            // ref after its v9 sidecar is durable, then commits this transaction
            // onto the new ref. Legacy writers retain the eager fork path below.
            if txn.is_some() && entry.table_branch.as_deref() != Some(active_branch) {
                let ds = db.storage().open_snapshot_at_entry(entry).await?;
                return Ok(OpenedForMutation {
                    identity: entry.identity,
                    handle: Some(ds),
                    expected_version: entry.table_version,
                    full_path,
                    table_branch: Some(active_branch.to_string()),
                    deferred_fork: Some(DeferredTableFork {
                        source_entry: entry.clone(),
                        target_branch: active_branch.to_string(),
                    }),
                });
            }
            let (ds, table_branch) = open_owned_dataset_for_branch_write(
                db,
                table_key,
                entry.identity,
                &full_path,
                entry.table_branch.as_deref(),
                entry.table_version,
                active_branch,
                op_kind,
                txn.is_some(),
            )
            .await?;
            let version = ds.version();
            Ok(OpenedForMutation {
                identity: entry.identity,
                handle: Some(ds),
                expected_version: version,
                full_path,
                table_branch,
                deferred_fork: None,
            })
        }
    }
}

pub(super) async fn open_owned_dataset_for_branch_write(
    db: &Omnigraph,
    table_key: &str,
    identity: crate::db::manifest::TableIdentity,
    full_path: &str,
    entry_branch: Option<&str>,
    entry_version: u64,
    active_branch: &str,
    op_kind: crate::db::MutationOpKind,
    occ_enrolled: bool,
) -> Result<(SnapshotHandle, Option<String>)> {
    match entry_branch {
        Some(branch) if branch == active_branch => {
            let ds = db
                .storage()
                .open_dataset_head(full_path, Some(active_branch))
                .await?;
            if op_kind.strict_pre_stage_version_check() {
                if occ_enrolled && ds.version() != entry_version {
                    return Err(OmniError::manifest_read_set_changed(
                        format!("table_head:{table_key}"),
                        Some(entry_version.to_string()),
                        Some(ds.version().to_string()),
                    ));
                }
                if !occ_enrolled {
                    db.storage()
                        .ensure_expected_version(&ds, table_key, entry_version)?;
                }
            }
            Ok((ds, Some(active_branch.to_string())))
        }
        source_branch => {
            crate::failpoints::maybe_fail(crate::failpoints::names::FORK_BEFORE_CLASSIFY)?;
            // Authority check before forking: re-read the live manifest. If this
            // table is already forked on active_branch, a concurrent first-write
            // won the race and our snapshot is stale — that is a retryable
            // conflict, not an orphan. (A zombie fork is never in the manifest,
            // so this only fires for a live concurrent fork.)
            let live = db.snapshot_for_branch(Some(active_branch)).await?;
            if let Some(entry) = live.entry(table_key) {
                if entry.table_branch.as_deref() == Some(active_branch) {
                    return if occ_enrolled {
                        Err(OmniError::manifest_read_set_changed(
                            format!("table_head:{table_key}"),
                            Some(entry_version.to_string()),
                            Some(entry.table_version.to_string()),
                        ))
                    } else {
                        Err(OmniError::manifest_expected_version_mismatch(
                            table_key,
                            entry_version,
                            entry.table_version,
                        ))
                    };
                }
            }
            // The fork advances Lance state before the manifest publish. The
            // caller holds the per-(table, active_branch) write queue from
            // before this fork through the publish, so a leftover ref is a
            // manifest-unreferenced fork (interrupted prior fork, or
            // delete+recreate), not a live in-process fork. The wrapper
            // self-heals it (reclaim + re-fork); see
            // `Omnigraph::fork_dataset_from_entry_state`.
            db.fork_dataset_from_entry_state(
                table_key,
                identity,
                full_path,
                source_branch,
                entry_version,
                active_branch,
            )
            .await?;
            let ds = db
                .storage()
                .open_dataset_head(full_path, Some(active_branch))
                .await?;
            if op_kind.strict_pre_stage_version_check() {
                db.storage()
                    .ensure_expected_version(&ds, table_key, entry_version)?;
            }
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
) -> Result<crate::storage_layer::ForkOutcome<SnapshotHandle>> {
    db.storage()
        .fork_branch_from_state(
            full_path,
            source_branch,
            table_key,
            source_version,
            active_branch,
        )
        .await
}

/// Classification of a Lance branch ref `B` on table `T` against FRESH manifest
/// authority — the single decision both fork-ref reclaim sites share: the
/// write-path reclaim ([`reclaim_orphaned_fork_and_refork`]) and the cleanup
/// reconciler (`optimize::reconcile_orphaned_branches`). Having one classifier
/// keeps the two destructive sites from drifting (the bug history: each was
/// hardened separately and the other lagged).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ForkRefStatus {
    /// The manifest places `T` on `B` — a legitimate fork. Never destroy.
    Legitimate,
    /// The manifest does not reference this fork (`T` not on `B`, or `B` absent
    /// from the manifest entirely). Reclaimable.
    Orphan,
    /// Fresh authority could not be established (a transient read failure on a
    /// live branch). Ambiguous — do not destroy; the caller retries / converges.
    Indeterminate,
}

/// Classify a fork ref from FRESH manifest authority (bypasses the coordinator
/// cache). MUST be called with the per-`(table, branch)` write queue held, so
/// the classification is stable against in-process writers for the caller's
/// critical section. Both reclaim sites map the result to their own action
/// (write path: reclaim vs retryable; cleanup: delete vs skip), but the
/// destroy-only-on-`Orphan` rule is enforced here, once.
pub(crate) async fn classify_fork_ref(
    db: &Omnigraph,
    _table_key: &str,
    identity: crate::db::manifest::TableIdentity,
    branch: &str,
    excluding_operation_id: Option<&str>,
) -> ForkRefStatus {
    // Deferred mutation/load forks are created only after their v9 sidecar is
    // durable. Until the manifest publish places this table on `branch`, that
    // sidecar is the only durable ownership record for the ref. Treat a
    // matching pending intent as indeterminate rather than an orphan so neither
    // destructive caller can steal a live writer's fork. The writer that owns
    // the intent may exclude itself while reclaiming a genuinely stale ref it
    // collided with; every other sidecar remains a hard stop. A list failure is
    // likewise indeterminate -- cleanup must never turn missing authority into
    // permission to delete.
    let sidecars =
        match crate::db::manifest::list_sidecars(db.root_uri(), db.storage_adapter()).await {
            Ok(sidecars) => sidecars,
            Err(_) => return ForkRefStatus::Indeterminate,
        };
    if sidecars.iter().any(|sidecar| {
        Some(sidecar.operation_id.as_str()) != excluding_operation_id
            && sidecar.branch.as_deref() == Some(branch)
            && sidecar
                .tables
                .iter()
                .any(|pin| pin.identity == identity && pin.table_branch.as_deref() == Some(branch))
    }) {
        return ForkRefStatus::Indeterminate;
    }

    // `classify.fresh_read` failpoint: simulate a transient failure of the
    // fresh-authority read (no-op without the `failpoints` feature). Lets a
    // test exercise the Indeterminate path — a read failure on a live branch
    // must classify as Indeterminate (skip), never Orphan (destroy).
    let fresh = match crate::failpoints::maybe_fail(crate::failpoints::names::CLASSIFY_FRESH_READ) {
        Ok(()) => db.fresh_snapshot_for_branch(Some(branch)).await,
        Err(injected) => Err(injected),
    };
    match fresh {
        Ok(snap) => {
            let placed = snap
                .entries()
                .find(|entry| entry.identity == identity)
                .map(|e| e.table_branch.as_deref() == Some(branch))
                .unwrap_or(false);
            if placed {
                ForkRefStatus::Legitimate
            } else {
                // Branch resolves but the manifest does not place this table on
                // it — a manifest-unreferenced fork.
                ForkRefStatus::Orphan
            }
        }
        // Branch did not resolve. `all_branches` lists `_refs/branches/` live, so
        // absent there = genuinely no such manifest branch (origin-1 orphan);
        // present (or a list error) = transient read — never destroy on that.
        Err(_) => match db.coordinator.read().await.all_branches().await {
            Ok(fresh) if !fresh.iter().any(|b| b == branch) => ForkRefStatus::Orphan,
            _ => ForkRefStatus::Indeterminate,
        },
    }
}

/// Reclaim a manifest-unreferenced fork and re-fork in its place.
///
/// Reached when `fork_branch_from_state` reports `RefAlreadyExists`. This is a
/// destructive op (it force-deletes a Lance branch ref), so it owns its own
/// safety precondition rather than trusting the caller's: it re-derives, via
/// [`classify_fork_ref`], that the manifest does not place this table on
/// `active_branch`. The caller's earlier proof may have come from the
/// coordinator's *cached* branch snapshot (`resolved_branch_target` returns
/// the cache when the handle is bound to `active_branch` — an embedded handle
/// on the branch, or `branch_merge`'s target swap); trusting it could
/// force-delete a fork a concurrent writer just legitimately published. Only
/// once fresh authority confirms the ref is unreferenced does it drop the ref
/// (idempotent `force_delete_branch`) and re-fork, exactly once.
///
/// If fresh authority shows the table IS on `active_branch` (a legitimate
/// concurrent fork), or a second collision occurs after reclaim (a foreign-
/// process writer recreated the ref — the documented one-winner-CAS gap), it
/// surfaces a retryable conflict; on retry the winner's fork is visible and
/// the no-fork path runs.
pub(super) async fn reclaim_orphaned_fork_and_refork(
    db: &Omnigraph,
    table_key: &str,
    identity: crate::db::manifest::TableIdentity,
    full_path: &str,
    source_branch: Option<&str>,
    source_version: u64,
    active_branch: &str,
    current_operation_id: Option<&str>,
) -> Result<SnapshotHandle> {
    // The immutable identity and physical target are one authority fact. Keep
    // them coupled here as a final destructive-operation guard: an old
    // incarnation must never authorize deleting a ref on a replacement table's
    // path, even if both lifetimes reused the same public alias.
    let canonical_path = crate::db::manifest::table_path_for_identity(table_key, identity)?;
    let canonical_full_path = db.storage().dataset_uri(&canonical_path);
    if full_path != canonical_full_path {
        return Err(OmniError::manifest_read_set_changed(
            format!("fork_target_path:{identity}"),
            Some(canonical_full_path),
            Some(full_path.to_string()),
        ));
    }

    // A v9 mutation/load sidecar (`protocol_v3` payload) is written before its deferred fork. A
    // manifest-unreferenced ref claimed by another pending operation is live,
    // not an orphan: never force-delete it. Excluding our own operation lets a
    // writer reclaim a genuinely stale pre-existing ref after its own intent is
    // durable. A sidecar-list failure is indeterminate and therefore loud.
    let sidecars = crate::db::manifest::list_sidecars(db.root_uri(), db.storage_adapter()).await?;
    if let Some(owner) = sidecars.iter().find(|sidecar| {
        Some(sidecar.operation_id.as_str()) != current_operation_id
            && sidecar.branch.as_deref() == Some(active_branch)
            && sidecar.tables.iter().any(|pin| {
                pin.identity == identity && pin.table_branch.as_deref() == Some(active_branch)
            })
    }) {
        return Err(OmniError::manifest_read_set_changed(
            format!("fork_intent:{active_branch}:{table_key}"),
            None,
            Some(owner.operation_id.clone()),
        ));
    }

    // Self-validate against FRESH authority before destroying anything. Only an
    // Orphan is reclaimable; a Legitimate status (a concurrent writer published
    // a real fork despite the caller's possibly-cached proof) or an
    // Indeterminate one (transient read) surfaces a retryable conflict rather
    // than stranding the manifest at a version the recreated ref won't have.
    match classify_fork_ref(db, table_key, identity, active_branch, current_operation_id).await {
        ForkRefStatus::Orphan => {}
        ForkRefStatus::Legitimate => {
            let actual = db
                .fresh_snapshot_for_branch(Some(active_branch))
                .await
                .ok()
                .and_then(|s| {
                    s.entries()
                        .find(|entry| entry.identity == identity)
                        .map(|entry| entry.table_version)
                })
                .unwrap_or(source_version);
            if current_operation_id.is_some() {
                return Err(OmniError::manifest_read_set_changed(
                    format!("table_head:{table_key}"),
                    Some(source_version.to_string()),
                    Some(actual.to_string()),
                ));
            }
            return Err(OmniError::manifest_expected_version_mismatch(
                table_key,
                source_version,
                actual,
            ));
        }
        ForkRefStatus::Indeterminate => {
            return Err(OmniError::manifest_conflict(format!(
                "could not verify whether branch '{active_branch}' still owns an orphaned \
                 fork for table '{table_key}' because fresh manifest authority was \
                 unavailable; refresh and retry"
            )));
        }
    }

    crate::failpoints::maybe_fail(crate::failpoints::names::FORK_BEFORE_RECLAIM)?;
    db.storage()
        .force_delete_branch(full_path, active_branch)
        .await
        .map_err(|e| {
            // Lance refuses to delete a branch with dependent child branches
            // even under force (RefConflict). Unreachable for a leaf first-write
            // fork (the cleanup reconciler also drops children before parents),
            // but surface it actionably if it ever happens. We match loosely on
            // "referenc" rather than the exact prose, which is not a Lance API
            // contract; a typed RefConflict variant through `force_delete_branch`
            // is the durable follow-up.
            if e.to_string().contains("referenc") {
                OmniError::manifest_conflict(format!(
                    "branch '{active_branch}' cannot reclaim the leftover fork for \
                     table '{table_key}' because it has dependent child branches; \
                     delete the child branches (or run `omnigraph cleanup`) first"
                ))
            } else {
                e
            }
        })?;

    match fork_dataset_from_entry_state(
        db,
        table_key,
        full_path,
        source_branch,
        source_version,
        active_branch,
    )
    .await?
    {
        crate::storage_layer::ForkOutcome::Created(ds) => Ok(ds),
        crate::storage_layer::ForkOutcome::RefAlreadyExists => {
            let live = db.fresh_snapshot_for_branch(Some(active_branch)).await?;
            let actual = live
                .entries()
                .find(|entry| entry.identity == identity)
                .map(|entry| entry.table_version)
                .unwrap_or(source_version);
            Err(OmniError::manifest_expected_version_mismatch(
                table_key,
                source_version,
                actual,
            ))
        }
    }
}

pub(super) async fn reopen_for_mutation(
    db: &Omnigraph,
    table_key: &str,
    full_path: &str,
    table_branch: Option<&str>,
    expected_version: u64,
    op_kind: crate::db::MutationOpKind,
) -> Result<SnapshotHandle> {
    db.ensure_schema_apply_not_locked("write").await?;
    if op_kind.strict_pre_stage_version_check() {
        db.storage()
            .reopen_for_mutation(full_path, table_branch, table_key, expected_version)
            .await
    } else {
        // Insert / Merge: skip the strict version check. Open at HEAD —
        // Lance's natural conflict resolver at commit_staged time
        // (rebase append, dedupe merge_insert) handles concurrent
        // writers correctly; the publisher CAS in
        // `MutationStaging::commit_all` (refreshed under the
        // per-(table, branch) queue via `snapshot_for_branch`) catches
        // genuine cross-process drift as 409. See
        // [`crate::db::MutationOpKind`] for the policy rationale.
        let _ = expected_version;
        db.storage()
            .open_dataset_head(full_path, table_branch)
            .await
    }
}

/// A declared index the builder could not materialize on this pass. Today the
/// only such case is a vector (IVF) column with no trainable vectors yet
/// (KMeans needs >=1 vector), e.g. the load-before-embed window. Reported, not
/// fatal: a later `ensure_indices`/`optimize` retries once the column is
/// buildable, and reads stay correct via brute-force meanwhile. Surfacing
/// pending index *status* rather than failing the operation is the database
/// norm (Postgres `indisvalid`, LanceDB `list_indices`).
#[derive(Debug, Clone)]
pub struct PendingIndex {
    pub table_key: String,
    pub column: String,
    pub reason: String,
}

pub(super) async fn build_indices_on_dataset(
    db: &Omnigraph,
    table_key: &str,
    ds: &mut SnapshotHandle,
) -> Result<Vec<PendingIndex>> {
    let catalog = db.catalog();
    build_indices_on_dataset_for_catalog(db, &catalog, table_key, ds).await
}

pub(super) async fn build_indices_on_dataset_for_catalog(
    db: &Omnigraph,
    catalog: &Catalog,
    table_key: &str,
    ds: &mut SnapshotHandle,
) -> Result<Vec<PendingIndex>> {
    let work = if let Some(type_name) = table_key.strip_prefix("node:") {
        plan_index_work_node(db, catalog, type_name, table_key, ds).await?
    } else if table_key.starts_with("edge:") {
        plan_index_work_edge_on_dataset(db, ds).await?
    } else {
        return Err(OmniError::manifest(format!(
            "invalid table key '{}'",
            table_key
        )));
    };

    if work.specs.is_empty() {
        return Ok(work.pending);
    }

    let staged = db
        .storage()
        .stage_create_indices(ds, &work.specs)
        .await
        .map_err(|error| {
            OmniError::Lance(format!(
                "stage index batch on {} ({:?}): {}",
                table_key, work.specs, error
            ))
        })?;
    // Retain the established test seam at the now-batched stage/commit
    // boundary. EnsureIndices itself stages existing targets before its gates;
    // legacy callers of this shared helper still exercise the same no-HEAD-
    // movement guarantee.
    crate::failpoints::maybe_fail(
        crate::failpoints::names::ENSURE_INDICES_POST_STAGE_PRE_COMMIT_BTREE,
    )?;
    let new_ds = db
        .storage()
        .commit_staged(ds.clone(), staged)
        .await
        .map_err(|error| {
            OmniError::Lance(format!(
                "commit index batch on {} ({:?}): {}",
                table_key, work.specs, error
            ))
        })?;
    *ds = new_ds;
    Ok(work.pending)
}

async fn prepare_updates_for_commit(
    db: &Omnigraph,
    branch: Option<&str>,
    updates: &[crate::db::SubTableUpdate],
    txn: Option<&crate::db::WriteTxn>,
) -> Result<Vec<crate::db::SubTableUpdate>> {
    if updates.is_empty() {
        return Ok(Vec::new());
    }

    // RFC-022 mutation/load adapter: the physical effect envelope must be
    // closed before its recovery sidecar is armed. Building missing indexes
    // here can add one extra staged CreateIndex commit per table, which is not
    // the exact logical-data transaction promised by that sidecar. Indexes are derived
    // state: enrolled mutation/load writes publish the exact data-table result
    // and leave declared-index materialization to the existing
    // ensure_indices/optimize reconciler. Legacy test/merge callers retain the
    // shared rebuild tail below.
    if txn.is_some() {
        return Ok(updates.to_vec());
    }

    // Enrolled mutation/load returned above: derived-index work is deliberately
    // outside their exact physical effect envelope. Legacy callers retain the
    // historical reopen/build path.
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
            // Strict version check is correct here: this runs INSIDE
            // the publisher commit path, after `commit_staged` already
            // advanced Lance HEAD to `prepared_update.table_version`.
            // The check is a defense-in-depth assertion that the
            // dataset state matches what we just committed; not the
            // pre-stage race the op-kind policy targets.
            let mut ds = reopen_for_mutation(
                db,
                &prepared_update.table_key,
                &full_path,
                prepared_update.table_branch.as_deref(),
                prepared_update.table_version,
                crate::db::MutationOpKind::SchemaRewrite,
            )
            .await?;
            // Any column not yet buildable (e.g. a vector column whose rows
            // have null embeddings) is deferred and logged inside
            // build_indices; a later ensure_indices/optimize materializes it.
            // Legacy merge/test callers must not fail on it; enrolled
            // mutation/load callers returned before this block.
            let _pending =
                build_indices_on_dataset(db, &prepared_update.table_key, &mut ds).await?;
            let state = db.storage().table_state(&full_path, &ds).await?;
            prepared_update.table_version = state.version;
            prepared_update.row_count = state.row_count;
            prepared_update.version_metadata = state.version_metadata;
        }

        prepared.push(prepared_update);
    }

    Ok(prepared)
}

#[cfg(test)]
async fn commit_prepared_updates(
    db: &Omnigraph,
    updates: &[crate::db::SubTableUpdate],
    actor_id: Option<&str>,
) -> Result<u64> {
    let PublishedSnapshot {
        manifest_version,
        _snapshot_id: _,
    } = db
        .coordinator
        .write()
        .await
        .commit_updates_with_actor(updates, actor_id)
        .await?;
    Ok(manifest_version)
}

#[cfg(feature = "failpoints")]
async fn commit_prepared_updates_with_expected(
    db: &Omnigraph,
    updates: &[crate::db::SubTableUpdate],
    expected_table_versions: &crate::db::manifest::ExpectedTableVersions,
    actor_id: Option<&str>,
) -> Result<u64> {
    let PublishedSnapshot {
        manifest_version,
        _snapshot_id: _,
    } = db
        .coordinator
        .write()
        .await
        .commit_updates_with_actor_with_expected(updates, expected_table_versions, actor_id)
        .await?;
    Ok(manifest_version)
}

#[cfg(feature = "failpoints")]
pub(super) async fn commit_prepared_updates_on_branch_with_expected(
    db: &Omnigraph,
    branch: Option<&str>,
    updates: &[crate::db::SubTableUpdate],
    expected_table_versions: &crate::db::manifest::ExpectedTableVersions,
    actor_id: Option<&str>,
) -> Result<u64> {
    let current_branch = db
        .coordinator
        .read()
        .await
        .current_branch()
        .map(str::to_string);
    let requested_branch = branch.map(str::to_string);
    if requested_branch == current_branch {
        return commit_prepared_updates_with_expected(
            db,
            updates,
            expected_table_versions,
            actor_id,
        )
        .await;
    }

    let mut coordinator = match requested_branch.as_deref() {
        Some(branch) => {
            GraphCoordinator::open_branch_with_session(
                db.uri(),
                branch,
                Arc::clone(&db.storage),
                &db.control_session(),
            )
            .await?
        }
        None => {
            GraphCoordinator::open_with_session(
                db.uri(),
                Arc::clone(&db.storage),
                &db.control_session(),
            )
            .await?
        }
    };
    let PublishedSnapshot {
        manifest_version,
        _snapshot_id: _,
    } = coordinator
        .commit_updates_with_actor_with_expected(updates, expected_table_versions, actor_id)
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
    let current_branch = db
        .coordinator
        .read()
        .await
        .current_branch()
        .map(str::to_string);
    let prepared = prepare_updates_for_commit(db, current_branch.as_deref(), updates, None).await?;
    commit_prepared_updates(db, &prepared, None).await
}

/// Commit updates with a publisher-level OCC fence. The
/// `expected_table_versions` map asserts the manifest's pre-write per-table
/// versions; mismatches surface as `ManifestConflictDetails::ExpectedVersionMismatch`.
pub(super) async fn commit_updates_on_branch_with_expected(
    db: &Omnigraph,
    branch: Option<&str>,
    updates: &[crate::db::SubTableUpdate],
    expected_table_versions: &crate::db::manifest::ExpectedTableVersions,
    actor_id: Option<&str>,
    txn: &crate::db::WriteTxn,
    lineage_intent: crate::db::manifest::LineageIntent,
) -> Result<u64> {
    db.ensure_schema_apply_not_locked("write commit").await?;
    let prepared = prepare_updates_for_commit(db, branch, updates, Some(txn)).await?;

    debug_assert_eq!(lineage_intent.actor_id.as_deref(), actor_id);
    let changes = prepared
        .iter()
        .cloned()
        .map(ManifestChange::Update)
        .collect::<Vec<_>>();
    let expectation = crate::db::manifest::GraphHeadExpectation::new(
        branch,
        txn.authority.branch_identifier.clone(),
        txn.authority.graph_head.clone(),
    );
    let precondition = crate::db::manifest::PublishPrecondition::ExactGraphHead(expectation);

    let current_branch = db
        .coordinator
        .read()
        .await
        .current_branch()
        .map(str::to_string);
    let requested_branch = branch.map(str::to_string);
    let published = if requested_branch == current_branch {
        db.coordinator
            .write()
            .await
            .commit_changes_with_intent_and_expected(
                &changes,
                expected_table_versions,
                lineage_intent,
                &precondition,
            )
            .await?
    } else {
        let mut coordinator = match requested_branch.as_deref() {
            Some(branch) => {
                GraphCoordinator::open_branch_with_session(
                    db.uri(),
                    branch,
                    Arc::clone(&db.storage),
                    &db.control_session(),
                )
                .await?
            }
            None => {
                GraphCoordinator::open_with_session(
                    db.uri(),
                    Arc::clone(&db.storage),
                    &db.control_session(),
                )
                .await?
            }
        };
        coordinator
            .commit_changes_with_intent_and_expected(
                &changes,
                expected_table_versions,
                lineage_intent,
                &precondition,
            )
            .await?
    };
    Ok(published.manifest_version)
}

pub(super) async fn invalidate_graph_index(db: &Omnigraph) {
    db.runtime_cache.invalidate_all().await;
}

#[cfg(test)]
mod classify_fork_ref_tests {
    //! Direct coverage of [`classify_fork_ref`] — the single fresh-authority
    //! decision both fork-ref reclaim sites (write-path reclaim + cleanup
    //! reconciler) route through. Pins each deterministic status so reverting
    //! the fresh-authority logic at either site fails here. (The `Indeterminate`
    //! arm needs an injected transient read and is covered under the
    //! `failpoints` suite.)
    use super::*;
    use crate::db::Omnigraph;
    use crate::loader::LoadMode;

    const SCHEMA: &str = "node Person { name: String @key }\nnode Company { name: String @key }\n";

    /// On-disk dataset path for a node table, taken from the manifest entry
    /// (the same path the engine uses) so the test forges against the real ref.
    async fn node_path(db: &Omnigraph, branch: &str, table_key: &str) -> String {
        let snap = db.snapshot_for_branch(Some(branch)).await.unwrap();
        let entry = snap.entry(table_key).unwrap();
        format!("{}/{}", db.root_uri, entry.table_path)
    }

    #[tokio::test]
    async fn classify_distinguishes_legitimate_unreferenced_and_ghost() {
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
            .await
            .unwrap();
        db.branch_create("feature").await.unwrap();

        // Legitimate: a real write forks Company onto `feature`, and the
        // manifest places Company on `feature`.
        db.load_as(
            "feature",
            None,
            r#"{"type":"Company","data":{"name":"Acme"}}"#,
            LoadMode::Merge,
            None,
        )
        .await
        .unwrap();
        let feature_snapshot = db.snapshot_for_branch(Some("feature")).await.unwrap();
        let company_identity = feature_snapshot.entry("node:Company").unwrap().identity;
        let person_identity = feature_snapshot.entry("node:Person").unwrap().identity;
        assert_eq!(
            classify_fork_ref(&db, "node:Company", company_identity, "feature", None,).await,
            ForkRefStatus::Legitimate,
            "a manifest-placed fork must classify as Legitimate (never destroyed)"
        );

        // Orphan (manifest-unreferenced): forge a `feature` ref on Person, which
        // the manifest's `feature` snapshot still places on main.
        let person = node_path(&db, "feature", "node:Person").await;
        {
            // forbidden-api-allow: test synthesizes a branch ref directly on the Lance dataset.
            let mut ds = lance::Dataset::open(&person).await.unwrap();
            let v = ds.version().version;
            ds.create_branch("feature", v, None).await.unwrap();
        }
        assert_eq!(
            classify_fork_ref(&db, "node:Person", person_identity, "feature", None).await,
            ForkRefStatus::Orphan,
            "a ref the manifest does not place on the branch must classify as Orphan"
        );

        // Orphan (ghost): a ref for a branch the manifest does not have at all.
        {
            // forbidden-api-allow: test synthesizes a branch ref directly on the Lance dataset.
            let mut ds = lance::Dataset::open(&person).await.unwrap();
            let v = ds.version().version;
            ds.create_branch("ghost", v, None).await.unwrap();
        }
        assert_eq!(
            classify_fork_ref(&db, "node:Person", person_identity, "ghost", None).await,
            ForkRefStatus::Orphan,
            "a ref for a branch absent from the manifest must classify as Orphan"
        );
    }

    #[tokio::test]
    async fn classify_does_not_adopt_a_reused_alias_across_incarnations() {
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
            .await
            .unwrap();
        let old_identity = db.snapshot().await.entry("node:Person").unwrap().identity;

        db.apply_schema("node Company { name: String @key }\n")
            .await
            .unwrap();
        db.apply_schema(SCHEMA).await.unwrap();
        let new_entry = db.snapshot().await.entry("node:Person").unwrap().clone();
        let new_identity = new_entry.identity;
        assert_ne!(old_identity, new_identity);

        db.branch_create("feature").await.unwrap();
        db.load_as(
            "feature",
            None,
            r#"{"type":"Person","data":{"name":"Ada"}}"#,
            LoadMode::Merge,
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            classify_fork_ref(&db, "node:Person", new_identity, "feature", None).await,
            ForkRefStatus::Legitimate
        );
        assert_eq!(
            classify_fork_ref(&db, "node:Person", old_identity, "feature", None).await,
            ForkRefStatus::Orphan,
            "a live placement under the reused alias belongs only to the new incarnation"
        );

        let full_path = db.storage().dataset_uri(&new_entry.table_path);
        let before = db
            .storage()
            .open_dataset_head(&full_path, Some("feature"))
            .await
            .unwrap();
        let before_identifier = db.storage().branch_identifier(&before).await.unwrap();
        reclaim_orphaned_fork_and_refork(
            &db,
            "node:Person",
            old_identity,
            &full_path,
            None,
            new_entry.table_version,
            "feature",
            None,
        )
        .await
        .expect_err("a stale identity must not authorize deletion on the replacement path");
        let after = db
            .storage()
            .open_dataset_head(&full_path, Some("feature"))
            .await
            .unwrap();
        let after_identifier = db.storage().branch_identifier(&after).await.unwrap();
        assert_eq!(before_identifier, after_identifier);
    }
}
