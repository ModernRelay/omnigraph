use super::*;
use crate::error::missing_graph_type_at_snapshot;
use crate::seams::{decide_seam, fail};
use lance::index::DatasetIndexExt;

pub(super) async fn graph_index(db: &Omnigraph) -> Result<Arc<crate::graph_index::GraphIndex>> {
    let (resolved, catalog) = db.capture_current_read_view().await?;
    // Whole-graph entry point: cover every edge type. Query execution scopes to
    // the edges it actually traverses (see `referenced_edge_types`).
    let edge_types: std::collections::HashMap<String, (String, String)> = catalog
        .edge_types
        .iter()
        .map(|(name, et)| (name.clone(), (et.from_type.clone(), et.to_type.clone())))
        .collect();
    db.runtime_cache
        .graph_index(
            &resolved,
            &edge_types,
            db.storage_adapter(),
            catalog.system_columns,
        )
        .await
}

pub(super) async fn graph_index_for_resolved(
    db: &Omnigraph,
    resolved: &ResolvedTarget,
    edge_types: &std::collections::HashMap<String, (String, String)>,
    system_columns: SystemColumns,
) -> Result<Arc<crate::graph_index::GraphIndex>> {
    db.runtime_cache
        .graph_index(resolved, edge_types, db.storage_adapter(), system_columns)
        .await
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

/// The indexes replaced by one successfully published full-text rebuild.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullTextIndexRebuildResult {
    /// The selected logical graph branch, including `main`.
    pub branch: String,
    /// The publication made by this operation, or `None` when no index needs rebuilding.
    pub graph_commit_id: Option<String>,
    pub rebuilt_indexes: Vec<RebuiltFullTextIndex>,
}

/// A full-text property rebuilt from the selected branch's current rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebuiltFullTextIndex {
    pub type_key: String,
    pub property: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum IndexMaintenanceMode {
    Ensure,
    RebuildFullText,
}

struct IndexMaintenanceOutcome {
    pending: Vec<PendingIndex>,
    graph_commit_id: Option<String>,
    rebuilt_indexes: Vec<RebuiltFullTextIndex>,
}

pub(super) async fn rebuild_full_text_indices_on_as(
    db: &Omnigraph,
    branch: &str,
    actor: Option<&str>,
) -> Result<FullTextIndexRebuildResult> {
    let branch = normalize_branch_name(branch)?;
    let public_branch = branch.as_deref().unwrap_or("main");
    ensure_public_branch_ref(public_branch, "rebuild_full_text_indices")?;
    db.enforce(
        omnigraph_policy::PolicyAction::Change,
        &omnigraph_policy::ResourceScope::Branch(public_branch.to_string()),
        actor,
    )?;
    let outcome = maintain_indices_for_branch(
        db,
        branch.as_deref(),
        IndexMaintenanceMode::RebuildFullText,
        actor,
    )
    .await?;
    Ok(FullTextIndexRebuildResult {
        branch: public_branch.to_string(),
        graph_commit_id: outcome.graph_commit_id,
        rebuilt_indexes: outcome.rebuilt_indexes,
    })
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
        .dataset(table_key)
        .ok_or_else(|| OmniError::manifest(missing_graph_type_at_snapshot(table_key)))?;
    // Tests name the fork logically; resolve it to the live native ref unless
    // the caller already addresses an exact incarnation.
    let table_branch = match table_branch {
        Some(name)
            if name != "main"
                && crate::branch_names::split_native_branch_name(name)
                    .1
                    .is_none() =>
        {
            let owner = db.native_branch_for(name).await?;
            Some(
                entry
                    .native_dataset_branch
                    .as_deref()
                    .filter(|fork| entry.version_metadata.is_table_fork_of(fork, &owner))
                    .unwrap_or(&owner)
                    .to_string(),
            )
        }
        other => other.map(str::to_string),
    };
    let table_fork_owner = if table_branch == entry.native_dataset_branch {
        entry
            .version_metadata
            .table_fork_owner()
            .map(str::to_string)
    } else {
        // A first-touch fork carries no intent record (RFC 0067); ownership
        // of a fork the entry does not name yet is proven by ref equality.
        None
    };
    let full_path = format!("{}/{}", db.root_uri, entry.dataset_path);
    let ds = db
        .storage()
        .open_dataset_head(&full_path, table_branch.as_deref())
        .await?;
    let state = db.storage().table_state(&full_path, &ds).await?;
    let update = crate::db::DatasetUpdate {
        identity: entry.identity,
        type_key: table_key.to_string(),
        published_dataset_version: state.version,
        native_dataset_branch: table_branch.clone(),
        entity_count: state.row_count,
        version_metadata: state
            .version_metadata
            .with_table_fork_owner(table_fork_owner.as_deref()),
    };
    let mut expected = crate::db::manifest::ExpectedTableVersions::new();
    expected.insert(
        entry.identity,
        crate::db::manifest::TableVersionExpectation {
            table_key: table_key.to_string(),
            table_version: entry.published_dataset_version,
            native_ref: crate::db::manifest::NativeRefPin::Exact(
                entry.native_dataset_branch.clone(),
            ),
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
    Ok(
        maintain_indices_for_branch(db, branch, IndexMaintenanceMode::Ensure, None)
            .await?
            .pending,
    )
}

decide_seam! {
    pub static ENSURE_INDICES_POST_TABLE_EFFECT = ("ensure_indices.post_table_effect", Unreachable, [Fail]);
}

decide_seam! {
    /// Every first-touch fork exists, before the first detached index commit.
    /// An unreferenced fork is reclaimable garbage that cleanup classifies.
    pub static ENSURE_INDICES_POST_FORK_PRE_COMMIT = ("ensure_indices.post_fork_pre_commit", Unreachable, [Fail]);
}

decide_seam! {
    /// Every detached index commit exists, before the manifest publishes the
    /// pins: nothing is visible and no linear HEAD moved.
    pub static ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT = ("ensure_indices.post_phase_b_pre_manifest_commit", Unreachable, [Fail]);
}

decide_seam! {
    /// The pins are published, before the writer promotes them.
    pub static ENSURE_INDICES_POST_PUBLISH_PRE_PROMOTION = ("ensure_indices.post_publish_pre_promotion", Unreachable, [Fail]);
}

decide_seam! {
    pub static ENSURE_INDICES_POST_STAGE_PRE_COMMIT_BTREE = ("ensure_indices.post_stage_pre_commit_btree", Unreachable, [Fail]);
}

/// One table the index writer commits to.
struct IndexTarget {
    identity: crate::db::manifest::TableIdentity,
    table_key: String,
    full_path: String,
    expected_version: u64,
    /// The native ref the pin was read from.
    read_ref: Option<String>,
    /// The native ref the effect lands on: the read ref, or the fork a first
    /// touch creates once the lineage intent names it.
    table_branch: Option<String>,
    first_touch: bool,
}

async fn maintain_indices_for_branch(
    db: &Omnigraph,
    branch: Option<&str>,
    mode: IndexMaintenanceMode,
    actor: Option<&str>,
) -> Result<IndexMaintenanceOutcome> {
    // Install this handle's pending schema contract, if any, before capturing
    // the index plan's base. The under-gate revalidation below closes the race
    // between this capture and the first table effect.
    db.settle_pending_schema_install().await?;
    db.ensure_schema_apply_idle("ensure_indices").await?;
    let txn = db.open_write_txn(branch).await?;
    let snapshot = txn.base.clone();
    let mut pending_by_table = HashMap::<String, Vec<PendingIndex>>::new();
    let active_branch = txn.branch.clone();
    // Physical fork opens and first-touch targets name the branch's forks by
    // native ref; `active_branch` stays logical for gates and lineage.
    let native_active: Option<String> = match active_branch.as_deref() {
        None => None,
        Some(branch) => Some(match snapshot.native_branch() {
            Some(native) => native.to_string(),
            None => db.native_branch_for(branch).await?,
        }),
    };
    let catalog = Arc::clone(&txn.catalog);

    let mut targets: Vec<IndexTarget> = Vec::new();
    let mut work_by_table = HashMap::<String, PlannedIndexWork>::new();
    let mut existing_targets =
        std::collections::HashMap::<String, crate::storage_layer::SnapshotHandle>::new();
    let mut existing_staged =
        std::collections::HashMap::<String, crate::storage_layer::StagedHandle>::new();

    // Plan and build uncommitted artifacts before taking writer gates. An
    // existing target is opened at its pin, so the complete BTREE/FTS/vector
    // batch stages on the exact version the pin names and is abandoned
    // safely if final authority revalidation loses. A first-touch fork does
    // not exist yet; its artifacts are staged once the fork is created under
    // the gates.
    let mut __dst_nk1: Vec<_> = catalog.node_types.keys().collect();
    __dst_nk1.sort();
    for type_name in __dst_nk1 {
        let table_key = format!("node:{}", type_name);
        let Some(entry) = snapshot.dataset(&table_key) else {
            continue;
        };
        // Ordinary ensure preserves lazy main inheritance. Explicit rebuilding
        // must fork inherited tables when their full-text indexes need work.
        if mode == IndexMaintenanceMode::Ensure
            && active_branch.is_some()
            && entry.native_dataset_branch.is_none()
        {
            continue;
        }
        let full_path = format!("{}/{}", db.root_uri, entry.dataset_path);
        let first_touch = native_active.as_deref().is_some_and(|owner| {
            !entry
                .native_dataset_branch
                .as_deref()
                .is_some_and(|fork| entry.version_metadata.is_table_fork_of(fork, owner))
        });
        let ds = if first_touch {
            // The inherited owner's HEAD may advance independently after this
            // graph branch was cut. Plan from the exact inherited snapshot, not
            // from that owner's current HEAD; a fork needs a linear source
            // version, so a pending inherited pin is promoted first.
            db.promote_inherited_pin(&table_key, &full_path, entry)
                .await?;
            db.storage().open_snapshot_at_entry(entry).await?
        } else {
            db.open_pinned_for_write(&table_key, &full_path, entry)
                .await?
        };
        let work = match mode {
            IndexMaintenanceMode::Ensure => {
                plan_index_work_node(db, &catalog, type_name, &table_key, &ds).await?
            }
            IndexMaintenanceMode::RebuildFullText => {
                let node = &catalog.node_types[type_name];
                plan_full_text_rebuild(&table_key, &node.properties, &node.indices, &ds).await?
            }
        };
        if !work.pending.is_empty() {
            pending_by_table.insert(table_key.clone(), work.pending.clone());
        }
        if work.needs_commit() {
            targets.push(IndexTarget {
                identity: entry.identity,
                table_key: table_key.clone(),
                full_path,
                expected_version: entry.published_dataset_version,
                read_ref: entry.native_dataset_branch.clone(),
                table_branch: entry.native_dataset_branch.clone(),
                first_touch,
            });
            if !first_touch {
                existing_targets.insert(table_key.clone(), ds);
            }
            work_by_table.insert(table_key, work);
        }
    }
    let mut __dst_ek1: Vec<_> = catalog.edge_types.keys().collect();
    __dst_ek1.sort();
    for edge_name in __dst_ek1 {
        let table_key = format!("edge:{}", edge_name);
        let Some(entry) = snapshot.dataset(&table_key) else {
            continue;
        };
        if mode == IndexMaintenanceMode::Ensure
            && active_branch.is_some()
            && entry.native_dataset_branch.is_none()
        {
            continue;
        }
        let full_path = format!("{}/{}", db.root_uri, entry.dataset_path);
        let first_touch = native_active.as_deref().is_some_and(|owner| {
            !entry
                .native_dataset_branch
                .as_deref()
                .is_some_and(|fork| entry.version_metadata.is_table_fork_of(fork, owner))
        });
        let ds = if first_touch {
            db.promote_inherited_pin(&table_key, &full_path, entry)
                .await?;
            db.storage().open_snapshot_at_entry(entry).await?
        } else {
            db.open_pinned_for_write(&table_key, &full_path, entry)
                .await?
        };
        let work = match mode {
            IndexMaintenanceMode::Ensure => {
                plan_index_work_edge_on_dataset(db, &ds, catalog.system_columns).await?
            }
            IndexMaintenanceMode::RebuildFullText => {
                let edge = &catalog.edge_types[edge_name];
                // Ensure only builds edge id/src/dst BTREEs; edge declarations
                // must not introduce FTS during migration. Existing physical
                // edge FTS inventory is still replaced for SDK reads.
                plan_full_text_rebuild(&table_key, &edge.properties, &[], &ds).await?
            }
        };
        if work.needs_commit() {
            targets.push(IndexTarget {
                identity: entry.identity,
                table_key: table_key.clone(),
                full_path,
                expected_version: entry.published_dataset_version,
                read_ref: entry.native_dataset_branch.clone(),
                table_branch: entry.native_dataset_branch.clone(),
                first_touch,
            });
            if !first_touch {
                existing_targets.insert(table_key.clone(), ds);
            }
            work_by_table.insert(table_key, work);
        }
    }

    // Validate the entire inventory before building any artifacts. In rebuild
    // mode an unsupported physical FTS index must refuse the whole operation,
    // not leave a subset looking migrated. First-touch artifacts still wait
    // for the fork.
    for target in &targets {
        if let Some(ds) = existing_targets.get(&target.table_key) {
            let work = &work_by_table[&target.table_key];
            let staged = db
                .storage()
                .stage_create_indices(ds, &work.specs)
                .await
                .map_err(|error| {
                    error.with_context(format!(
                        "stage index batch on {} ({:?})",
                        target.table_key, work.specs
                    ))
                })?;
            fail(&ENSURE_INDICES_POST_STAGE_PRE_COMMIT_BTREE)?;
            existing_staged.insert(target.table_key.clone(), staged);
        }
    }

    let queue_keys: Vec<(String, Option<String>)> = targets
        .iter()
        .map(|target| (target.table_key.clone(), active_branch.clone()))
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

    let live_snapshot = db.revalidate_write_txn(&txn).await?;
    for target in &targets {
        let prepared_entry = snapshot.dataset(&target.table_key).ok_or_else(|| {
            OmniError::manifest_conflict(format!(
                "{} disappeared from the prepared index plan",
                crate::error::dataset_subject(&target.table_key),
            ))
        })?;
        let prepared_binding = format!(
            "{}:{}:{}",
            prepared_entry.dataset_path,
            prepared_entry
                .native_dataset_branch
                .as_deref()
                .unwrap_or("main"),
            target.expected_version,
        );
        let live_entry = live_snapshot.dataset(&target.table_key).ok_or_else(|| {
            OmniError::manifest_read_set_changed(
                format!("dataset_binding:{}", target.identity),
                Some(prepared_binding.clone()),
                None,
            )
        })?;
        if live_entry.published_dataset_version != target.expected_version
            || live_entry.identity != target.identity
            || live_entry.dataset_path != prepared_entry.dataset_path
            || live_entry.native_dataset_branch != prepared_entry.native_dataset_branch
        {
            return Err(OmniError::manifest_read_set_changed(
                format!("dataset_binding:{}", target.identity),
                Some(prepared_binding),
                Some(format!(
                    "{}:{}:{}",
                    live_entry.dataset_path,
                    live_entry
                        .native_dataset_branch
                        .as_deref()
                        .unwrap_or("main"),
                    live_entry.published_dataset_version,
                )),
            ));
        }
    }

    let graph_commit_id = if targets.is_empty() {
        // Preserve the no-work failpoint contract without manufacturing graph
        // lineage.
        fail(&ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT)?;
        None
    } else {
        let expected_versions = targets
            .iter()
            .map(|target| {
                (
                    target.identity,
                    crate::db::manifest::TableVersionExpectation {
                        table_key: target.table_key.clone(),
                        table_version: target.expected_version,
                        native_ref: crate::db::manifest::NativeRefPin::Exact(
                            target.read_ref.clone(),
                        ),
                    },
                )
            })
            .collect::<crate::db::manifest::ExpectedTableVersions>();
        let lineage = db
            .new_lineage_intent_for_branch(active_branch.as_deref(), actor)
            .await?;
        for target in &mut targets {
            if target.first_touch {
                let owner = native_active.as_deref().ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "first-touch index target '{}' has no native branch ref",
                        target.table_key,
                    ))
                })?;
                target.table_branch = Some(crate::branch_names::table_fork_name(
                    owner,
                    txn.base.graph_manifest_version(),
                    &lineage.graph_commit_id,
                ));
            }
        }

        // RFC 0067: first-touch forks are created now, with no intent record.
        // An unreferenced fork is reclaimable garbage that cleanup classifies,
        // never a graph-visible effect. Their index batches stage on the fork.
        let mut forked =
            std::collections::HashMap::<String, crate::storage_layer::SnapshotHandle>::new();
        for target in &targets {
            if !target.first_touch {
                continue;
            }
            let entry = snapshot.dataset(&target.table_key).ok_or_else(|| {
                OmniError::manifest(missing_graph_type_at_snapshot(&target.table_key))
            })?;
            let fork = target.table_branch.as_deref().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "first-touch index target '{}' has no fork name",
                    target.table_key,
                ))
            })?;
            let ds = db
                .fork_dataset_from_entry_state(
                    &target.table_key,
                    target.identity,
                    &target.full_path,
                    entry.native_dataset_branch.as_deref(),
                    entry.published_dataset_version,
                    fork,
                )
                .await?;
            let work = &work_by_table[&target.table_key];
            let staged = db
                .storage()
                .stage_create_indices(&ds, &work.specs)
                .await
                .map_err(|error| {
                    error.with_context(format!(
                        "stage first-touch index batch on {} ({:?})",
                        target.table_key, work.specs
                    ))
                })?;
            fail(&ENSURE_INDICES_POST_STAGE_PRE_COMMIT_BTREE)?;
            existing_staged.insert(target.table_key.clone(), staged);
            forked.insert(target.table_key.clone(), ds);
        }
        if !forked.is_empty() {
            fail(&ENSURE_INDICES_POST_FORK_PRE_COMMIT)?;
        }

        // RFC 0067: every index batch is committed as a detached version of
        // its pinned base. Nothing moves a linear HEAD and nothing is visible
        // until the manifest publishes the pin `(expected + 1, staged, uuid)`;
        // the writer promotes the pins after that.
        let mut updates = Vec::with_capacity(targets.len());
        let mut promotions = Vec::with_capacity(targets.len());
        for target in &targets {
            let ds = existing_targets
                .remove(&target.table_key)
                .or_else(|| forked.remove(&target.table_key))
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "missing index target for table '{}'",
                        target.table_key
                    ))
                })?;
            let staged = existing_staged.remove(&target.table_key).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "missing staged index batch for table '{}'",
                    target.table_key
                ))
            })?;
            let entry = snapshot.dataset(&target.table_key).ok_or_else(|| {
                OmniError::manifest(missing_graph_type_at_snapshot(&target.table_key))
            })?;
            let base = ds.clone();
            let (detached, identity) = db.storage().commit_staged_detached(ds, staged).await?;
            let state = db
                .storage()
                .table_state(&target.full_path, &detached)
                .await?;
            let published_dataset_version = target.expected_version + 1;
            let version_metadata = state
                .version_metadata
                .with_table_fork_owner(native_active.as_deref())
                .with_staged(state.version, identity.uuid.clone());
            promotions.push(crate::db::HeldPromotion {
                table_key: target.table_key.clone(),
                dataset_path: entry.dataset_path.clone(),
                full_path: target.full_path.clone(),
                table_branch: target.table_branch.clone(),
                base,
                chain: Vec::new(),
                detached,
                target: published_dataset_version,
                uuid: identity.uuid,
                e_tag: version_metadata.e_tag().map(str::to_string),
            });
            updates.push(crate::db::DatasetUpdate {
                identity: target.identity,
                type_key: target.table_key.clone(),
                published_dataset_version,
                native_dataset_branch: target.table_branch.clone(),
                entity_count: state.row_count,
                version_metadata,
            });
            fail(&ENSURE_INDICES_POST_TABLE_EFFECT)?;
        }

        fail(&ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT)?;
        let published = commit_updates_on_branch_with_expected(
            db,
            active_branch.as_deref(),
            &updates,
            &expected_versions,
            actor,
            &txn,
            lineage,
        )
        .await?;
        match fail(&ENSURE_INDICES_POST_PUBLISH_PRE_PROMOTION) {
            Ok(()) => db.promote_held_all(promotions).await,
            Err(error) => {
                tracing::warn!(error = %error, "index promotion interrupted; the next writer promotes")
            }
        }
        Some(published.graph_commit_id)
    };

    // Preserve the historical, observable catalog order even though planning
    // and physical effects now happen in separate phases.
    let mut pending = Vec::new();
    let mut __dst_nk2: Vec<_> = catalog.node_types.keys().collect();
    __dst_nk2.sort();
    for type_name in __dst_nk2 {
        if let Some(mut table_pending) = pending_by_table.remove(&format!("node:{type_name}")) {
            pending.append(&mut table_pending);
        }
    }
    let mut rebuilt_indexes = Vec::new();
    if mode == IndexMaintenanceMode::RebuildFullText && graph_commit_id.is_some() {
        for target in &targets {
            for spec in &work_by_table[&target.table_key].specs {
                if let crate::storage_layer::IndexBuildSpec::FullText { column } = spec {
                    rebuilt_indexes.push(RebuiltFullTextIndex {
                        type_key: target.table_key.clone(),
                        property: column.clone(),
                    });
                }
            }
        }
        rebuilt_indexes.sort_by(|a, b| (&a.type_key, &a.property).cmp(&(&b.type_key, &b.property)));
    }
    Ok(IndexMaintenanceOutcome {
        pending,
        graph_commit_id,
        rebuilt_indexes,
    })
}

/// The single scalar/vector index a node property receives from a one-column
/// `@index`/`@key` declaration, or `None` when the property type is not
/// indexable here (a list column or `Blob`).
///
/// Shared by `build_indices_on_dataset_for_catalog` (which builds the index)
/// and `index_work_status_on_dataset_for_catalog` (which decides whether a
/// table has index work to commit) so the two cannot drift: an enum or
/// orderable scalar the builder gives a BTREE must also be reported as "needs
/// work" until that BTREE exists.
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
/// untrainable column is reported as pending instead of planned as a commit
/// that stages nothing) and by the vector build arm (so
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
/// column): a table with nothing to stage must not become a publication target
/// beside productive siblings.
pub(super) struct IndexWorkStatus {
    pub(super) needs_commit: bool,
    pub(super) pending: Vec<PendingIndex>,
}

/// Returns the declared scalar/vector index work that
/// `build_indices_on_dataset_for_catalog` would build, plus pending-only work,
/// for one operation-local accepted catalog and one selected dataset snapshot.
/// The table must have at least one row (the ensure_indices loop has
/// `if row_count > 0 { build_indices(...) }`, so empty tables produce
/// zero commits and must NOT become publication targets).
///
/// Per `build_indices_on_dataset_for_catalog`, nodes get BTree (id) plus, for
/// each one-column `@index`/`@key` property, the index `node_prop_index_kind`
/// assigns: a scalar BTREE for enums and orderable scalars
/// (DateTime/Date/numeric/Bool), FTS for free-text Strings, or a Vector
/// index. Edges get BTree only (id, src, dst). This helper and the builder
/// share `node_prop_index_kind` so they cannot drift — see its doc comment.
#[derive(Default)]
pub(super) struct PlannedIndexWork {
    pub(super) specs: Vec<crate::storage_layer::IndexBuildSpec>,
    pub(super) pending: Vec<PendingIndex>,
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

/// Full rebuilds use schema declarations plus the actual physical inventory.
/// An externally named FTS index on a supported property is still replaced;
/// names and the table writer version are not compatibility evidence.
async fn plan_full_text_rebuild(
    table_key: &str,
    properties: &HashMap<String, PropType>,
    declarations: &[Vec<String>],
    ds: &SnapshotHandle,
) -> Result<PlannedIndexWork> {
    let mut columns = BTreeSet::new();
    for declaration in declarations {
        if let [column] = declaration.as_slice()
            && properties.get(column).is_some_and(|property| {
                node_prop_index_kind(property) == Some(NodePropIndexKind::Fts)
            })
        {
            columns.insert(column.clone());
        }
    }

    let dataset = ds.dataset();
    let store = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    let raw_indexes = lance_table::io::manifest::read_manifest_indexes(
        &store,
        dataset.manifest_location(),
        dataset.manifest(),
    )
    .await
    .map_err(OmniError::storage)?;
    let indexes = dataset.load_indices().await.map_err(OmniError::storage)?;
    let supported_ids: HashSet<_> = indexes.iter().map(|index| index.uuid).collect();
    // load_indices filters unsupported versions and may infer legacy details.
    // Neither operation proves that the complete physical inventory can be
    // rebuilt. Validate the unfiltered manifest before accepting that view.
    for index in &raw_indexes {
        if index.index_details.is_none() {
            // Supported engine-created v6 indexes (Lance 9/10) carry kind
            // metadata. Older/external unknown-kind artifacts are outside this
            // automatic rebuild: guessing from a text declaration could drop a
            // BTREE companion instead of replacing full-text postings.
            return Err(OmniError::manifest(format!(
                "cannot rebuild full-text indexes on {}: index '{}' has no index-kind \
                 metadata, outside the supported automatic upgrade; see \
                 docs/user/operations/upgrade.md#unsupported-index-inventory for a controlled \
                 export/import rebuild; no indexes were published",
                dataset_subject(table_key),
                index.name,
            )));
        }
        if !supported_ids.contains(&index.uuid) {
            return Err(OmniError::manifest(format!(
                "cannot rebuild full-text indexes on {}: index '{}' has an unsupported \
                 physical format; no indexes were published",
                dataset_subject(table_key),
                index.name,
            )));
        }
    }
    for index in indexes.iter() {
        if !TableStore::is_full_text_index(index) {
            continue;
        }
        let field = index
            .keyed_field()
            .filter(|_| index.covering_fields.is_empty())
            .and_then(|id| dataset.schema().fields.iter().find(|field| field.id == id));
        let Some(field) = field.filter(|field| {
            properties.get(&field.name).is_some_and(|property| {
                node_prop_index_kind(property) == Some(NodePropIndexKind::Fts)
            })
        }) else {
            return Err(OmniError::manifest(format!(
                "cannot rebuild full-text index '{}' on {}: only single, non-list, \
                 free-text String properties in the selected graph schema are supported; \
                 no indexes were published",
                index.name,
                dataset_subject(table_key),
            )));
        };
        columns.insert(field.name.clone());
    }

    // Empty tables are deliberately not skipped: either Lance publishes an
    // actual empty full build, or its error refuses the operation. Returning
    // success while retaining an old incompatible index is not a rebuild.
    Ok(PlannedIndexWork {
        specs: columns
            .into_iter()
            .map(|column| crate::storage_layer::IndexBuildSpec::FullText { column })
            .collect(),
        pending: Vec::new(),
    })
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
    if !db
        .storage()
        .has_btree_index(ds, catalog.system_columns.id)
        .await?
    {
        work.push_spec(crate::storage_layer::IndexBuildSpec::BTree {
            column: catalog.system_columns.id.to_string(),
            name: None,
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
                            type_key: table_key.to_string(),
                            property: prop_name.clone(),
                            reason: "property has no non-null vectors to train on yet".to_string(),
                        });
                    }
                }
            }
            Some(NodePropIndexKind::Btree)
                if !db.storage().has_btree_index(ds, prop_name).await? =>
            {
                work.push_spec(crate::storage_layer::IndexBuildSpec::BTree {
                    column: prop_name.clone(),
                    name: None,
                });
            }
            Some(NodePropIndexKind::Btree) | None => {}
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
        plan_index_work_edge_on_dataset(db, ds, catalog.system_columns).await?
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
    system_columns: SystemColumns,
) -> Result<PlannedIndexWork> {
    if db.storage().count_rows(ds, None).await? == 0 {
        return Ok(PlannedIndexWork::default());
    }
    let mut work = PlannedIndexWork::default();
    for column in [system_columns.id, system_columns.src, system_columns.dst] {
        if !db.storage().has_btree_index(ds, column).await? {
            work.push_spec(crate::storage_layer::IndexBuildSpec::BTree {
                column: column.to_string(),
                name: None,
            });
        }
    }
    Ok(work)
}

/// Result of opening a sub-table for mutation. `handle` is `None` only when a
/// non-strict (Insert/Merge) op on the WriteTxn's own branch skipped the
/// accumulation open (RFC-013 step 3b collapse #1) — there the caller needs just
/// `expected_version`. It is always `Some` for strict operations, the fork
/// path, and the test-only non-transactional opener.
#[derive(Debug)]
pub(crate) struct OpenedForMutation {
    /// Immutable logical table lifetime captured from the same manifest entry
    /// as `expected_version`. Writers carry this through staging and publish;
    /// the mutable alias is never used as OCC identity.
    pub(crate) identity: crate::db::manifest::TableIdentity,
    /// The opened dataset, or `None` on the non-strict-txn open-skip path.
    pub(crate) handle: Option<SnapshotHandle>,
    /// The publisher's CAS fence: the opened handle's version, or — when the open
    /// was skipped — the pinned base entry's version (equal absent uncovered drift).
    pub(crate) expected_version: u64,
    pub(crate) full_path: String,
    pub(crate) table_branch: Option<String>,
    /// The ref the pin was read on, see `NativeRefPin`.
    pub(crate) pinned_native_ref: Option<String>,
    /// The manifest registration the pin was read from (RFC 0067): the
    /// staging reopen resolves it, promoting a pending predecessor first.
    pub(crate) entry: crate::db::DatasetEntry,
    /// RFC-022 first-touch named-branch writes stage against the inherited
    /// source snapshot and defer the durable Lance ref creation until
    /// `StagedMutation::commit_all` holds the gates.
    pub(crate) deferred_fork: Option<DeferredTableFork>,
}

#[derive(Debug, Clone)]
pub(crate) struct DeferredTableFork {
    pub(crate) source_entry: crate::db::DatasetEntry,
    pub(crate) target_branch: String,
}

decide_seam! {
    pub static FORK_BEFORE_CLASSIFY = ("fork.before_classify", AnyWrite, [Fail]);
}

#[cfg(test)]
impl OpenedForMutation {
    /// Raw-write fixtures use the no-transaction path, which must open a handle.
    pub(crate) fn require_handle(self, ctx: &str) -> (SnapshotHandle, String, Option<String>) {
        let handle = self.handle.unwrap_or_else(|| {
            panic!("{ctx}: open_for_mutation returned no handle on a path that requires one")
        });
        (handle, self.full_path, self.table_branch)
    }
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
        .dataset(table_key)
        .ok_or_else(|| OmniError::manifest(missing_graph_type_at_snapshot(table_key)))?;
    let full_path = format!("{}/{}", db.root_uri, entry.dataset_path);
    let native_active = match resolved_branch.as_deref() {
        Some(branch) => Some(match snapshot.native_branch() {
            Some(native) => native.to_string(),
            None => db.native_branch_for(branch).await?,
        }),
        None => None,
    };

    // Collapse #1 (RFC-013 step 3b): a non-strict op (Insert/Merge) on the txn's
    // own branch needs no dataset open for ACCUMULATION — the only thing the
    // caller reads from this handle on the non-strict path is `.version()` (the
    // publisher's CAS fence), which is exactly the pinned base version. The base
    // already validated the schema contract once, and the staging reopen
    // (`reopen_for_mutation`) plus the publisher CAS in `commit_all` are the real
    // drift guards. So skip `open_dataset_head` entirely and source the
    // expected version from the pinned entry.
    //
    // Gated on `txn.is_some()`: callers without an accumulation transaction
    // always open a handle. STRICT ops (Update/Delete/
    // SchemaRewrite) always open live HEAD + run `ensure_expected_version`
    // (read-modify-write SI), and any write that must FORK (the table isn't yet on
    // the resolved branch) opens too (the fork is a real Lance state advance the
    // manifest snapshot can't substitute for).
    if txn.is_some() && !op_kind.strict_pre_stage_version_check() {
        match resolved_branch.as_deref() {
            // Non-strict, table already on the active branch → no open, no fork.
            Some(_)
                if entry
                    .native_dataset_branch
                    .as_deref()
                    .zip(native_active.as_deref())
                    .is_some_and(|(fork, owner)| {
                        entry.version_metadata.is_table_fork_of(fork, owner)
                    }) =>
            {
                return Ok(OpenedForMutation {
                    identity: entry.identity,
                    entry: entry.clone(),
                    handle: None,
                    expected_version: entry.published_dataset_version,
                    full_path,
                    table_branch: entry.native_dataset_branch.clone(),
                    pinned_native_ref: entry.native_dataset_branch.clone(),
                    deferred_fork: None,
                });
            }
            // Main branch, non-strict → no open. (Main never forks.)
            None => {
                return Ok(OpenedForMutation {
                    identity: entry.identity,
                    entry: entry.clone(),
                    handle: None,
                    expected_version: entry.published_dataset_version,
                    full_path,
                    table_branch: None,
                    pinned_native_ref: entry.native_dataset_branch.clone(),
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
            // RFC 0067: the pinned image, never HEAD. The manifest CAS at
            // publication is the read-set fence for every op kind.
            let ds = db
                .open_pinned_for_write(table_key, &full_path, entry)
                .await?;
            Ok(OpenedForMutation {
                identity: entry.identity,
                entry: entry.clone(),
                handle: Some(ds),
                expected_version: entry.published_dataset_version,
                full_path,
                table_branch: None,
                pinned_native_ref: entry.native_dataset_branch.clone(),
                deferred_fork: None,
            })
        }
        Some(active_branch) => {
            // RFC-022-enrolled mutation/load adapters create a per-table Lance
            // branch ref only under the final gates. Read and stage from the
            // inherited source entry now; `commit_all` creates the target ref,
            // then commits this transaction detached on the new ref. Legacy
            // writers retain the eager fork path below.
            let native_active = native_active.as_deref().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "branch '{active_branch}' resolved without a native ref"
                ))
            })?;
            if txn.is_some()
                && !entry.native_dataset_branch.as_deref().is_some_and(|fork| {
                    entry.version_metadata.is_table_fork_of(fork, native_active)
                })
            {
                let ds = db.storage().open_snapshot_at_entry(entry).await?;
                return Ok(OpenedForMutation {
                    identity: entry.identity,
                    entry: entry.clone(),
                    handle: Some(ds),
                    expected_version: entry.published_dataset_version,
                    full_path,
                    table_branch: Some(native_active.to_string()),
                    pinned_native_ref: entry.native_dataset_branch.clone(),
                    deferred_fork: Some(DeferredTableFork {
                        source_entry: entry.clone(),
                        target_branch: native_active.to_string(),
                    }),
                });
            }
            let (ds, table_branch) = open_owned_dataset_for_branch_write(
                db,
                table_key,
                entry,
                &full_path,
                active_branch,
                native_active,
                op_kind,
                snapshot.graph_manifest_version(),
            )
            .await?;
            Ok(OpenedForMutation {
                identity: entry.identity,
                entry: entry.clone(),
                handle: Some(ds),
                expected_version: entry.published_dataset_version,
                full_path,
                table_branch,
                pinned_native_ref: entry.native_dataset_branch.clone(),
                deferred_fork: None,
            })
        }
    }
}

pub(super) async fn open_owned_dataset_for_branch_write(
    db: &Omnigraph,
    table_key: &str,
    entry: &crate::db::DatasetEntry,
    full_path: &str,
    active_branch: &str,
    native_active: &str,
    op_kind: crate::db::MutationOpKind,
    base_manifest_version: u64,
) -> Result<(SnapshotHandle, Option<String>)> {
    let identity = entry.identity;
    let entry_version = entry.published_dataset_version;
    // `active_branch` is the logical branch (manifest reads, gates, lineage);
    // `native_active` is its native ref (Lance opens, forks, entry names).
    match entry.native_dataset_branch.as_deref() {
        Some(branch)
            if entry
                .version_metadata
                .is_table_fork_of(branch, native_active) =>
        {
            // RFC 0067: the pinned image on the branch's own ref.
            let ds = db
                .open_pinned_for_write(table_key, full_path, entry)
                .await?;
            Ok((ds, Some(branch.to_string())))
        }
        source_branch => {
            fail(&FORK_BEFORE_CLASSIFY)?;
            let live = db.snapshot_for_branch(Some(active_branch)).await?;
            let current = live.dataset(table_key).ok_or_else(|| {
                OmniError::manifest_read_set_changed(
                    format!("dataset_binding:{identity}"),
                    Some(format!("{identity}:{entry_version}:{source_branch:?}")),
                    None,
                )
            })?;
            if current.identity != identity
                || current.published_dataset_version != entry_version
                || current.native_dataset_branch.as_deref() != source_branch
                || live.native_branch() != Some(native_active)
            {
                return Err(OmniError::manifest_read_set_changed(
                    format!("dataset_binding:{identity}"),
                    Some(format!("{identity}:{entry_version}:{source_branch:?}")),
                    Some(format!(
                        "{}:{}:{:?}",
                        current.identity,
                        current.published_dataset_version,
                        current.native_dataset_branch,
                    )),
                ));
            }
            let target = crate::branch_names::table_fork_name(
                native_active,
                base_manifest_version,
                &crate::dst_ids::new_ulid().to_string(),
            );
            // A pending pin on the inherited source is promoted first so the
            // fork has a linear version to start from (RFC 0067).
            db.promote_inherited_pin(table_key, full_path, entry)
                .await?;
            let ds = db
                .fork_dataset_from_entry_state(
                    table_key,
                    identity,
                    full_path,
                    source_branch,
                    entry_version,
                    &target,
                )
                .await?;
            if op_kind.strict_pre_stage_version_check() {
                db.storage()
                    .ensure_expected_version(&ds, table_key, entry_version)?;
            }
            Ok((ds, Some(target)))
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
) -> Result<SnapshotHandle> {
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

/// Classify a table fork against fresh graph publication authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
pub(crate) enum ForkRefStatus {
    /// The manifest places `T` on `B` — a legitimate fork. Never destroy.
    Legitimate,
    /// Another graph branch still pins this ref after its owner detached.
    /// Re-forking would overwrite the borrower's immutable history.
    Borrowed,
    /// The manifest does not reference this fork (`T` not on `B`, or `B` absent
    /// from the manifest entirely). Reclaimable.
    Orphan,
    /// Fresh authority could not be established (a transient read failure on a
    /// live branch). Ambiguous — do not destroy; the caller retries / converges.
    Indeterminate,
}

#[cfg(test)]
async fn classify_fork_ref(
    db: &Omnigraph,
    _table_key: &str,
    identity: crate::db::manifest::TableIdentity,
    branch: &str,
) -> ForkRefStatus {
    let references =
        match crate::db::manifest::ManifestCoordinator::native_fork_references_under_control_gates(
            db.root_uri(),
            &db.control_session(),
        )
        .await
        {
            Ok(references) => references,
            Err(_) => return ForkRefStatus::Indeterminate,
        };
    classify_fork_ref_with_references(identity, branch, &references)
}

/// Use a graph-wide proof captured under the same held schema control gate.
#[cfg(test)]
pub(crate) fn classify_fork_ref_with_references(
    identity: crate::db::manifest::TableIdentity,
    branch: &str,
    references: &crate::db::manifest::NativeForkReferences,
) -> ForkRefStatus {
    if fail(&crate::seams::catalog::CLASSIFY_FRESH_READ).is_err() {
        return ForkRefStatus::Indeterminate;
    }
    if references.owner_contains(identity, branch) {
        ForkRefStatus::Legitimate
    } else if references.contains(identity, branch) {
        ForkRefStatus::Borrowed
    } else {
        ForkRefStatus::Orphan
    }
}

/// Index work deferred on this pass. A vector property without trainable
/// vectors can be retried once populated; an existing full-text index with
/// incomplete coverage requires explicit rebuilding. `reason` names the remedy.
/// Reads retain their unindexed fallback; this status alone is not a write.
#[derive(Debug, Clone)]
pub struct PendingIndex {
    pub type_key: String,
    pub property: String,
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

/// The index batch a table needs at `ds`: every declared index it lacks.
pub(super) async fn plan_index_work_on_dataset_for_catalog(
    db: &Omnigraph,
    catalog: &Catalog,
    table_key: &str,
    ds: &SnapshotHandle,
) -> Result<PlannedIndexWork> {
    if let Some(type_name) = table_key.strip_prefix("node:") {
        plan_index_work_node(db, catalog, type_name, table_key, ds).await
    } else if table_key.starts_with("edge:") {
        plan_index_work_edge_on_dataset(db, ds, catalog.system_columns).await
    } else {
        Err(OmniError::manifest(format!(
            "invalid table key '{}'",
            table_key
        )))
    }
}

pub(super) async fn build_indices_on_dataset_for_catalog(
    db: &Omnigraph,
    catalog: &Catalog,
    table_key: &str,
    ds: &mut SnapshotHandle,
) -> Result<Vec<PendingIndex>> {
    let work = plan_index_work_on_dataset_for_catalog(db, catalog, table_key, ds).await?;
    if work.specs.is_empty() {
        return Ok(work.pending);
    }

    let staged = db
        .storage()
        .stage_create_indices(ds, &work.specs)
        .await
        .map_err(|error| {
            error.with_context(format!(
                "stage index batch on {table_key} ({:?})",
                work.specs
            ))
        })?;
    // Retain the established test seam at the now-batched stage/commit
    // boundary. EnsureIndices itself stages existing targets before its gates;
    // legacy callers of this shared helper still exercise the same no-HEAD-
    // movement guarantee.
    fail(&ENSURE_INDICES_POST_STAGE_PRE_COMMIT_BTREE)?;
    let new_ds = db
        .storage()
        .commit_staged(ds.clone(), staged)
        .await
        .map_err(|error| {
            error.with_context(format!(
                "commit index batch on {table_key} ({:?})",
                work.specs
            ))
        })?;
    *ds = new_ds;
    Ok(work.pending)
}

async fn prepare_updates_for_commit(
    db: &Omnigraph,
    branch: Option<&str>,
    updates: &[crate::db::DatasetUpdate],
    txn: Option<&crate::db::WriteTxn>,
) -> Result<Vec<crate::db::DatasetUpdate>> {
    if updates.is_empty() {
        return Ok(Vec::new());
    }

    // RFC-022 mutation/load adapter: the published pin names exactly one
    // detached logical-data transaction per table. Building missing indexes
    // here would add an extra CreateIndex commit that pin does not name.
    // Indexes are derived state: enrolled mutation/load writes publish the
    // exact data-table result and leave declared-index materialization to the
    // existing ensure_indices/optimize reconciler. Legacy test/merge callers
    // retain the shared rebuild tail below.
    if txn.is_some() {
        return Ok(updates.to_vec());
    }

    // Enrolled mutation/load returned above: derived-index work is deliberately
    // outside their exact physical effect envelope. Legacy callers retain the
    // historical reopen/build path.
    let snapshot = db.snapshot_for_branch(branch).await?;
    let mut prepared = Vec::with_capacity(updates.len());

    for update in updates {
        let Some(entry) = snapshot.dataset(&update.type_key) else {
            return Err(OmniError::manifest(missing_graph_type_at_snapshot(
                &update.type_key,
            )));
        };

        let mut prepared_update = update.clone();
        if prepared_update.entity_count > 0 {
            let full_path = format!("{}/{}", db.root_uri, entry.dataset_path);
            // A legacy caller committed linearly on HEAD, so HEAD is the
            // version its update names; the strict check is a defense-in-depth
            // assertion that the dataset state matches what it just committed.
            let mut ds = db
                .storage()
                .open_dataset_head(&full_path, prepared_update.native_dataset_branch.as_deref())
                .await?;
            db.storage().ensure_expected_version(
                &ds,
                &prepared_update.type_key,
                prepared_update.published_dataset_version,
            )?;
            // Any column not yet buildable (e.g. a vector column whose rows
            // have null embeddings) is deferred and logged inside
            // build_indices; a later ensure_indices/optimize materializes it.
            // Legacy merge/test callers must not fail on it; enrolled
            // mutation/load callers returned before this block.
            let _pending = build_indices_on_dataset(db, &prepared_update.type_key, &mut ds).await?;
            let state = db.storage().table_state(&full_path, &ds).await?;
            prepared_update.published_dataset_version = state.version;
            prepared_update.entity_count = state.row_count;
            prepared_update.version_metadata = state
                .version_metadata
                .with_table_fork_owner(prepared_update.version_metadata.table_fork_owner());
        }

        prepared.push(prepared_update);
    }

    Ok(prepared)
}

#[cfg(test)]
async fn commit_prepared_updates(
    db: &Omnigraph,
    updates: &[crate::db::DatasetUpdate],
    actor_id: Option<&str>,
) -> Result<u64> {
    let PublishedSnapshot {
        graph_manifest_version: manifest_version,
        ..
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
    updates: &[crate::db::DatasetUpdate],
    expected_table_versions: &crate::db::manifest::ExpectedTableVersions,
    actor_id: Option<&str>,
) -> Result<u64> {
    let PublishedSnapshot {
        graph_manifest_version: manifest_version,
        ..
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
    updates: &[crate::db::DatasetUpdate],
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
        graph_manifest_version: manifest_version,
        ..
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
    updates: &[crate::db::DatasetUpdate],
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
/// versions; mismatches surface as `ManifestConflictDetails::PublishedDatasetVersionMismatch`.
pub(super) async fn commit_updates_on_branch_with_expected(
    db: &Omnigraph,
    branch: Option<&str>,
    updates: &[crate::db::DatasetUpdate],
    expected_table_versions: &crate::db::manifest::ExpectedTableVersions,
    actor_id: Option<&str>,
    txn: &crate::db::WriteTxn,
    lineage_intent: crate::db::manifest::LineageIntent,
) -> Result<crate::db::GraphCommit> {
    db.ensure_schema_apply_not_locked("write commit").await?;
    if branch != txn.branch.as_deref() {
        return Err(OmniError::manifest_internal(
            "publication branch differs from its captured write transaction",
        ));
    }
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

    // Choose and publish through one coordinator lock. A same-name cached
    // coordinator can still hold another incarnation or incomplete lineage,
    // so only an exact captured view is reused without a fresh open.
    let mut active = db.coordinator.write().await;
    let published = if branch == active.current_branch() {
        let captured_view_matches = active.branch_identifier().await?
            == txn.authority.branch_identifier
            && active.exact_graph_head() == txn.authority.graph_head
            && active.version() == txn.base.graph_manifest_version();
        if !captured_view_matches {
            // Keep the handle's binding while replacing only stale state.
            // The publisher still reads fresh authority and applies its CAS.
            *active = db.open_coordinator_for_branch(branch).await?;
        }
        active
            .commit_changes_with_intent_and_expected(
                &changes,
                expected_table_versions,
                lineage_intent,
                &precondition,
            )
            .await?
    } else {
        // An operation on a non-bound branch owns its coordinator locally;
        // it must never temporarily change this handle's query/write binding.
        drop(active);
        let cache_key = branch.unwrap_or("main");
        let captured = {
            let mut cache = db.merge_authority_cache.lock().await;
            let matches = if let Some((key, coordinator)) = cache.as_ref() {
                key == cache_key
                    && coordinator.current_branch() == branch
                    && coordinator.branch_identifier().await? == txn.authority.branch_identifier
                    && coordinator.exact_graph_head() == txn.authority.graph_head
                    && coordinator.version() == txn.base.graph_manifest_version()
            } else {
                false
            };
            if matches {
                cache.take().map(|(_, coordinator)| coordinator)
            } else {
                None
            }
        };
        let mut coordinator = match captured {
            Some(coordinator) => coordinator,
            None => db.open_coordinator_for_branch(branch).await?,
        };
        // The cache supplies only the already captured starting view. The
        // publisher still reads fresh state and enforces ExactGraphHead on
        // every CAS attempt. On error, drop a taken cache view; the next
        // capture must resolve any ambiguous durable publication.
        let published = coordinator
            .commit_changes_with_intent_and_expected(
                &changes,
                expected_table_versions,
                lineage_intent,
                &precondition,
            )
            .await?;
        *db.merge_authority_cache.lock().await = Some((cache_key.to_string(), coordinator));
        published
    };
    Ok(published.commit)
}

pub(super) async fn invalidate_graph_index(db: &Omnigraph) {
    db.runtime_cache.invalidate_all().await;
}

#[cfg(test)]
mod classify_fork_ref_tests {
    //! Physical fork classification and stable table-identity guards.
    use super::*;
    use crate::db::Omnigraph;
    use crate::loader::LoadMode;

    const SCHEMA: &str = "node Person { name: String @key }\nnode Company { name: String @key }\n";

    /// On-disk dataset path for a node table, taken from the manifest entry
    /// (the same path the engine uses) so the test forges against the real ref.
    async fn node_path(db: &Omnigraph, branch: &str, table_key: &str) -> String {
        let snap = db.snapshot_for_branch(Some(branch)).await.unwrap();
        let entry = snap.dataset(table_key).unwrap();
        format!("{}/{}", db.root_uri, entry.dataset_path)
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
        let feature_native = db.native_branch_for("feature").await.unwrap();
        let company_identity = feature_snapshot.dataset("node:Company").unwrap().identity;
        let person_identity = feature_snapshot.dataset("node:Person").unwrap().identity;
        assert_eq!(
            classify_fork_ref(
                &db,
                "node:Company",
                company_identity,
                feature_snapshot
                    .dataset("node:Company")
                    .unwrap()
                    .native_dataset_branch
                    .as_deref()
                    .unwrap(),
            )
            .await,
            ForkRefStatus::Legitimate,
            "a manifest-placed fork must classify as Legitimate (never destroyed)"
        );

        // Orphan (manifest-unreferenced): forge the live native `feature` ref
        // on Person, which the manifest's `feature` snapshot still places on
        // main.
        let person = node_path(&db, "feature", "node:Person").await;
        {
            // forbidden-api-allow: test synthesizes a branch ref directly on the Lance dataset.
            let mut ds = lance::Dataset::open(&person).await.unwrap();
            let v = ds.version().version;
            ds.create_branch(&feature_native, v, None).await.unwrap();
        }
        assert_eq!(
            classify_fork_ref(&db, "node:Person", person_identity, &feature_native).await,
            ForkRefStatus::Orphan,
            "a ref the manifest does not place on the branch must classify as Orphan"
        );

        // Orphan (dead incarnation): a fork under the same logical name but a
        // different incarnation suffix belongs to a deleted predecessor, even
        // though the logical branch is live.
        let dead_native = crate::branch_names::native_branch_name(
            "feature",
            &crate::branch_names::mint_incarnation(),
        );
        {
            // forbidden-api-allow: test synthesizes a branch ref directly on the Lance dataset.
            let mut ds = lance::Dataset::open(&person).await.unwrap();
            let v = ds.version().version;
            ds.create_branch(&dead_native, v, None).await.unwrap();
        }
        assert_eq!(
            classify_fork_ref(&db, "node:Company", company_identity, &dead_native).await,
            ForkRefStatus::Orphan,
            "a fork of a dead incarnation must classify as Orphan while the logical name is live"
        );

        // Orphan (ghost): a ref for a branch the manifest does not have at all.
        {
            // forbidden-api-allow: test synthesizes a branch ref directly on the Lance dataset.
            let mut ds = lance::Dataset::open(&person).await.unwrap();
            let v = ds.version().version;
            ds.create_branch("ghost", v, None).await.unwrap();
        }
        assert_eq!(
            classify_fork_ref(&db, "node:Person", person_identity, "ghost").await,
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
        let old_identity = db.snapshot().await.dataset("node:Person").unwrap().identity;

        db.apply_schema("node Company { name: String @key }\n")
            .await
            .unwrap();
        db.apply_schema(SCHEMA).await.unwrap();
        let new_entry = db.snapshot().await.dataset("node:Person").unwrap().clone();
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

        let feature_snapshot = db.snapshot_for_branch(Some("feature")).await.unwrap();
        let feature_native = feature_snapshot
            .dataset("node:Person")
            .unwrap()
            .native_dataset_branch
            .clone()
            .unwrap();
        assert_eq!(
            classify_fork_ref(&db, "node:Person", new_identity, &feature_native).await,
            ForkRefStatus::Legitimate
        );
        assert_eq!(
            classify_fork_ref(&db, "node:Person", old_identity, &feature_native).await,
            ForkRefStatus::Orphan,
            "a live placement under the reused alias belongs only to the new incarnation"
        );

        let full_path = db.storage().dataset_uri(&new_entry.dataset_path);
        let before = db
            .storage()
            .open_dataset_head(&full_path, Some(&feature_native))
            .await
            .unwrap();
        let before_identifier = db.storage().branch_identifier(&before).await.unwrap();
        db.fork_dataset_from_entry_state(
            "node:Person",
            old_identity,
            &full_path,
            None,
            new_entry.published_dataset_version,
            &feature_native,
        )
        .await
        .expect_err("a stale identity must not authorize effects on the replacement path");
        let after = db
            .storage()
            .open_dataset_head(&full_path, Some(&feature_native))
            .await
            .unwrap();
        let after_identifier = db.storage().branch_identifier(&after).await.unwrap();
        assert_eq!(before_identifier, after_identifier);
    }
}
