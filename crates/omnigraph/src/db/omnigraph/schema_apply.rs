use super::*;
use crate::seams::{decide_seam, fail};

mod prepared;
mod settlement;
pub use prepared::{PreparedSchemaApply, SchemaApplyReconciliation, SchemaContractDigest};
pub(super) use prepared::{prepare_schema_apply, reconcile_schema_apply};
pub use settlement::{PreparedSchemaSettlement, SchemaApplySettlement, SchemaNonPublicationProof};
pub(super) use settlement::{prepare_schema_settlement, settle_prepared_schema};

fn resolve_desired_schema_ir(
    accepted_ir: &SchemaIR,
    desired_schema_source: &str,
) -> Result<SchemaIR> {
    let desired_shape = crate::db::omnigraph::read_schema_shape_for_vintage(
        desired_schema_source,
        accepted_ir.system_columns(),
    )?;
    let resolution = omnigraph_compiler::resolve_schema_ir(accepted_ir, &desired_shape)
        .map_err(|error| OmniError::manifest(error.to_string()))?;
    let source_hash = omnigraph_compiler::schema_shape_hash(&desired_shape)
        .map_err(|error| OmniError::manifest(error.to_string()))?;
    let resolved_hash = omnigraph_compiler::schema_shape_hash_from_ir(&resolution.schema_ir)
        .map_err(|error| OmniError::manifest(error.to_string()))?;
    if source_hash != resolved_hash {
        return Err(OmniError::manifest(
            "desired schema source does not match its resolved schema; refusing before schema apply effects",
        ));
    }
    for diagnostic in &resolution.diagnostics {
        tracing::warn!(
            target: "omnigraph::schema::identity",
            kind = ?diagnostic.kind,
            entity = %diagnostic.entity,
            hint = %diagnostic.hint,
            "schema identity rename hint was ignored after an exact-name match"
        );
    }
    Ok(resolution.schema_ir)
}

fn table_identity_for_schema_key(
    schema_ir: &SchemaIR,
    table_key: &str,
) -> Result<crate::db::manifest::TableIdentity> {
    let (type_id, incarnation_id) = if let Some(type_name) = table_key.strip_prefix("node:") {
        let node = schema_ir
            .nodes
            .iter()
            .find(|node| node.name == type_name)
            .ok_or_else(|| {
                OmniError::manifest(format!(
                    "schema IR has no node identity for table alias '{table_key}'"
                ))
            })?;
        (node.type_id.get(), node.table_incarnation_id.get())
    } else if let Some(type_name) = table_key.strip_prefix("edge:") {
        let edge = schema_ir
            .edges
            .iter()
            .find(|edge| edge.name == type_name)
            .ok_or_else(|| {
                OmniError::manifest(format!(
                    "schema IR has no edge identity for table alias '{table_key}'"
                ))
            })?;
        (edge.type_id.get(), edge.table_incarnation_id.get())
    } else {
        return Err(OmniError::manifest(format!(
            "invalid schema table key '{table_key}'"
        )));
    };
    crate::db::manifest::TableIdentity::new(type_id, incarnation_id)
}

/// Plan a migration to `desired_schema_source`. A drop step reclaims nothing
/// at apply; see `SchemaMigrationStep::DropType` and `DropProperty`.
pub(super) async fn plan_schema(
    db: &Omnigraph,
    desired_schema_source: &str,
) -> Result<SchemaMigrationPlan> {
    let accepted_ir = accepted_ir_for_planning(db).await?;
    let desired_ir = resolve_desired_schema_ir(&accepted_ir, desired_schema_source)?;
    plan_schema_migration(&accepted_ir, &desired_ir)
        .map_err(|err| OmniError::manifest(err.to_string()))
}

pub(super) fn plan_schema_at_contract(
    db: &Omnigraph,
    desired_schema_source: &str,
    expected: &SchemaContractDigest,
) -> Result<SchemaMigrationPlan> {
    let view = db.schema_view.load();
    if &view.contract_digest() != expected {
        return Err(OmniError::manifest_conflict(
            "handle schema differs from the observed deployment contract",
        ));
    }
    let accepted_ir = view.catalog.bound_schema_ir().ok_or_else(|| {
        OmniError::manifest_internal("accepted catalog carries no bound SchemaIR")
    })?;
    let desired_ir = resolve_desired_schema_ir(accepted_ir, desired_schema_source)?;
    plan_schema_migration(accepted_ir, &desired_ir)
        .map_err(|error| OmniError::manifest(error.to_string()))
}

/// The accepted IR a plan is made against: the contract of the live view the
/// handle resolves now (probe, refresh when the manifest moved), not the
/// handle's warm ArcSwap catalog.
async fn accepted_ir_for_planning(db: &Omnigraph) -> Result<SchemaIR> {
    let (_, catalog) = db.capture_current_read_view().await?;
    catalog
        .bound_schema_ir()
        .cloned()
        .ok_or_else(|| OmniError::manifest_internal("accepted catalog carries no bound SchemaIR"))
}

struct PlannedSchemaApply {
    plan: SchemaMigrationPlan,
    desired_ir: SchemaIR,
    desired_catalog: Catalog,
}

async fn plan_schema_for_apply(
    db: &Omnigraph,
    desired_schema_source: &str,
) -> Result<PlannedSchemaApply> {
    let accepted_ir = accepted_ir_for_planning(db).await?;
    plan_schema_for_apply_from_accepted(db, desired_schema_source, &accepted_ir).await
}

decide_seam! {
    pub static SCHEMA_APPLY_AFTER_MANIFEST_COMMIT = ("schema_apply.after_manifest_commit", Unreachable, [Fail]);
}

decide_seam! {
    /// After each SchemaApply table effect commits (a detached schema-evolution
    /// commit, one of at most two per table, or a new-table create), before
    /// the next table effect or the publication.
    pub static SCHEMA_APPLY_POST_TABLE_COMMIT = ("schema_apply.post_table_commit", Unreachable, [Fail]);
}

decide_seam! {
    /// Under the schema, branch and table gates, before the first table effect.
    pub static SCHEMA_APPLY_POST_LOCK_PRE_EFFECT = ("schema_apply.post_lock_pre_effect", Unreachable, [Fail]);
}

async fn plan_schema_for_apply_from_accepted(
    db: &Omnigraph,
    desired_schema_source: &str,
    accepted_ir: &SchemaIR,
) -> Result<PlannedSchemaApply> {
    let branches = db.coordinator.read().await.all_branches().await?;
    let blocking_branches = branches
        .into_iter()
        .filter(|branch| branch != "main")
        .collect::<Vec<_>>();
    if !blocking_branches.is_empty() {
        return Err(OmniError::manifest_conflict(format!(
            "schema apply requires a graph with only main; found non-main branches: {}",
            blocking_branches.join(", ")
        )));
    }

    let desired_ir = resolve_desired_schema_ir(accepted_ir, desired_schema_source)?;
    let plan = plan_schema_migration(accepted_ir, &desired_ir)
        .map_err(|err| OmniError::manifest(err.to_string()))?;
    if !plan.supported {
        let message = plan
            .steps
            .iter()
            .find_map(|step| step.unsupported_error_message())
            .unwrap_or_else(|| "unsupported schema migration plan".to_string());
        return Err(OmniError::manifest(message));
    }

    let mut desired_catalog = build_catalog_from_ir(&desired_ir)?;
    fixup_physical_schemas(&mut desired_catalog)?;
    Ok(PlannedSchemaApply {
        plan,
        desired_ir,
        desired_catalog,
    })
}

pub(super) async fn preview_schema_apply(
    db: &Omnigraph,
    desired_schema_source: &str,
) -> Result<SchemaApplyPreview> {
    let planned = plan_schema_for_apply(db, desired_schema_source).await?;
    Ok(SchemaApplyPreview {
        plan: planned.plan,
        catalog: planned.desired_catalog,
    })
}

pub(super) async fn apply_schema<F>(
    db: &Omnigraph,
    desired_schema_source: &str,
    actor: Option<&str>,
    validate_catalog: F,
) -> Result<SchemaApplyResult>
where
    F: FnOnce(&Catalog) -> Result<()>,
{
    // Engine-layer policy gate (MR-722 chassis core).
    //
    // Fires BEFORE acquiring the schema-apply lock or doing any other
    // work. When no PolicyChecker is installed this is a no-op and
    // the apply path behaves exactly as it did before MR-722. When
    // a PolicyChecker IS installed and the actor is None, this is a
    // hard error — see Omnigraph::enforce's docstring for the
    // forget-the-actor-footgun reasoning.
    //
    // Scope is TargetBranch("main") to match the HTTP-layer convention
    // for SchemaApply: branch=None, target_branch=Some("main"). Cedar
    // policies in the wild use `target_branch_scope: protected` to
    // gate schema applies, so the engine-layer call has to set the
    // target_branch shape that activates that predicate. Wrong scope
    // here = silent policy mismatch with HTTP. See
    // `omnigraph_policy::ResourceScope::to_branch_pair` for the mapping.
    db.enforce(
        omnigraph_policy::PolicyAction::SchemaApply,
        &omnigraph_policy::ResourceScope::TargetBranch("main".to_string()),
        actor,
    )?;

    let _export_exclusion = db.reserve_export_destructive_control()?;

    let _schema_gate = db.write_queue().acquire_schema_exclusive().await;
    apply_schema_with_lock(db, desired_schema_source, actor, validate_catalog, None).await
}

pub(super) async fn apply_prepared_schema(
    db: &Omnigraph,
    prepared: &PreparedSchemaApply,
    actor: Option<&str>,
) -> Result<SchemaApplyResult> {
    prepared::authorize(db, actor)?;
    prepared.validate_envelope(db, actor)?;
    let _export_exclusion = db.reserve_export_destructive_control()?;
    let _schema_gate = db.write_queue().acquire_schema_exclusive().await;
    apply_schema_with_lock(
        db,
        &prepared.desired_source,
        actor,
        |_| Ok(()),
        Some(prepared),
    )
    .await
}

pub(super) async fn apply_schema_with_lock<F>(
    db: &Omnigraph,
    desired_schema_source: &str,
    actor: Option<&str>,
    validate_catalog: F,
    prepared: Option<&PreparedSchemaApply>,
) -> Result<SchemaApplyResult>
where
    F: FnOnce(&Catalog) -> Result<()>,
{
    prepared::authorize(db, actor)?;
    let captured = prepared::capture(db, desired_schema_source, prepared)
        .await
        .map_err(OmniError::before_effect)?;
    let issued;
    let prepared = if let Some(prepared) = prepared {
        prepared.validate_capture(db, actor, &captured)?;
        prepared
    } else {
        issued = captured.issue(db, desired_schema_source, actor)?;
        &issued
    };
    if let Some(commit_id) = prepared.graph_commit_id()
        && db
            .coordinator
            .read()
            .await
            .captured_commit(commit_id)?
            .is_some()
    {
        return Err(OmniError::manifest_conflict(
            "schema publication identity already exists",
        ));
    }
    let prepared::CapturedSchemaApply {
        planned,
        accepted_catalog,
        accepted_identity,
        snapshot,
        branch_identifier: base_branch_identifier,
        graph_head: base_graph_head,
        ..
    } = captured;
    let accepted_ir = accepted_catalog.bound_schema_ir().cloned().ok_or_else(|| {
        OmniError::manifest_internal("accepted catalog carries no bound SchemaIR")
    })?;
    validate_catalog(&planned.desired_catalog)?;
    let PlannedSchemaApply {
        plan,
        desired_ir,
        desired_catalog,
    } = planned;
    if prepared.is_noop() {
        db.store_schema_view(
            desired_catalog,
            desired_schema_source.to_string(),
            &desired_ir,
        )?;
        return Ok(SchemaApplyResult {
            supported: true,
            applied: false,
            graph_manifest_version: snapshot.graph_manifest_version(),
            steps: plan.steps,
            commit: None,
            contract: prepared.desired_contract().clone(),
        });
    }
    let lineage_intent = prepared
        .lineage
        .clone()
        .expect("effectful intent has lineage");
    let mut added_tables = BTreeSet::new();
    // Resolve every rename before classifying dependent property steps. The
    // planner currently emits RenameType first, but correctness must not depend
    // on step ordering: a same-apply rename + property drop still routes the
    // schema evolution to the source table captured under its old alias.
    let renamed_tables = plan
        .steps
        .iter()
        .filter_map(|step| match step {
            SchemaMigrationStep::RenameType {
                type_kind,
                from,
                to,
            } if !matches!(type_kind, SchemaTypeKind::Interface) => Some((
                schema_table_key(*type_kind, to),
                schema_table_key(*type_kind, from),
            )),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    // Existing tables whose columns change: added, renamed or dropped
    // properties. Each evolves by metadata-only Lance commits that keep every
    // data file; no row is read or rewritten.
    let mut evolved_tables = BTreeSet::new();
    let mut dropped_tables = BTreeSet::new();
    let mut property_renames = HashMap::<String, HashMap<String, String>>::new();
    let mut changed_edge_tables = false;

    for step in &plan.steps {
        match step {
            SchemaMigrationStep::AddType { type_kind, name } => {
                if matches!(type_kind, SchemaTypeKind::Interface) {
                    continue;
                }
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
                if matches!(type_kind, SchemaTypeKind::Interface) {
                    continue;
                }
                let source_key = schema_table_key(*type_kind, from);
                let target_key = schema_table_key(*type_kind, to);
                if source_key.starts_with("edge:") {
                    changed_edge_tables = true;
                }
                debug_assert_eq!(renamed_tables.get(&target_key), Some(&source_key));
            }
            SchemaMigrationStep::AddProperty {
                type_kind,
                type_name,
                ..
            } => {
                if matches!(type_kind, SchemaTypeKind::Interface) {
                    continue;
                }
                let table_key = schema_table_key(*type_kind, type_name);
                if table_key.starts_with("edge:") {
                    changed_edge_tables = true;
                }
                evolved_tables.insert(table_key);
            }
            SchemaMigrationStep::RenameProperty {
                type_kind,
                type_name,
                from,
                to,
            } => {
                if matches!(type_kind, SchemaTypeKind::Interface) {
                    continue;
                }
                let table_key = schema_table_key(*type_kind, type_name);
                if table_key.starts_with("edge:") {
                    changed_edge_tables = true;
                }
                evolved_tables.insert(table_key.clone());
                property_renames
                    .entry(table_key)
                    .or_default()
                    .insert(to.clone(), from.clone());
            }
            // AddConstraint is only ever an `@index` addition (every other
            // added constraint plans as UnsupportedChange). It records intent
            // in the desired catalog/IR; the physical index is built off the
            // critical path by ensure_indices/optimize (iss-848), so the apply
            // does no table work for it — a pure metadata change like the two
            // metadata steps below.
            // ExtendEnum is a pure widening (planner-verified superset): every
            // committed row is valid under the wider set, so no table data is
            // touched — the accepted catalog update alone makes the unified
            // validator accept the new variants on all three write surfaces.
            SchemaMigrationStep::AddConstraint { .. }
            | SchemaMigrationStep::ExtendEnum { .. }
            | SchemaMigrationStep::UpdateTypeMetadata { .. }
            | SchemaMigrationStep::UpdatePropertyMetadata { .. } => {}
            SchemaMigrationStep::DropProperty {
                type_kind,
                type_name,
                ..
            } => {
                if matches!(type_kind, SchemaTypeKind::Interface) {
                    continue;
                }
                // A property drop removes the column from the table's
                // schema by a metadata-only commit: its values stay in the
                // existing data files, which older versions still read.
                // Nothing is reclaimed at apply; see
                // `SchemaMigrationStep::DropProperty`.
                let table_key = schema_table_key(*type_kind, type_name);
                if table_key.starts_with("edge:") {
                    changed_edge_tables = true;
                }
                evolved_tables.insert(table_key);
            }
            SchemaMigrationStep::DropType { type_kind, name } => {
                if matches!(type_kind, SchemaTypeKind::Interface) {
                    continue;
                }
                // A type drop tombstones the table's entry in
                // the current __manifest version (no per-table write).
                // Nothing is reclaimed after the publish; see
                // `SchemaMigrationStep::DropType`.
                let table_key = schema_table_key(*type_kind, name);
                if table_key.starts_with("edge:") {
                    changed_edge_tables = true;
                }
                dropped_tables.insert(table_key);
            }
            step @ SchemaMigrationStep::UnsupportedChange { .. } => {
                return Err(OmniError::manifest(
                    step.unsupported_error_message()
                        .unwrap_or_else(|| "unsupported schema migration step".to_string()),
                ));
            }
        }
    }

    let mut table_registrations =
        BTreeMap::<String, (crate::db::manifest::TableIdentity, String)>::new();
    let mut table_updates =
        BTreeMap::<crate::db::manifest::TableIdentity, crate::db::DatasetUpdate>::new();
    let mut table_tombstones =
        BTreeMap::<crate::db::manifest::TableIdentity, (String, u64, Option<String>)>::new();

    for table_key in &evolved_tables {
        if added_tables.contains(table_key) {
            continue;
        }
        let source_table_key = renamed_tables.get(table_key).unwrap_or(table_key);
        let entry = snapshot.dataset(source_table_key).ok_or_else(|| {
            OmniError::manifest(format!(
                "missing source table '{}' for schema apply targeting '{}'",
                source_table_key, table_key
            ))
        })?;
        if entry.native_dataset_branch.is_some() {
            return Err(OmniError::manifest_internal(format!(
                "schema apply expected main-owned table '{}', found branch {:?}",
                source_table_key, entry.native_dataset_branch
            )));
        }
        let identity = table_identity_for_schema_key(&desired_ir, table_key)?;
        let accepted_identity = table_identity_for_schema_key(&accepted_ir, source_table_key)?;
        if identity != accepted_identity || identity != entry.identity {
            return Err(OmniError::manifest_internal(format!(
                "schema apply evolution identity mismatch: source '{}' is {}, target '{}' is {}",
                source_table_key, entry.identity, table_key, identity
            )));
        }
    }
    for (target_table_key, source_table_key) in &renamed_tables {
        let source_entry = snapshot.dataset(source_table_key).ok_or_else(|| {
            OmniError::manifest(format!(
                "missing source table '{}' for schema rename",
                source_table_key
            ))
        })?;
        let desired_identity = table_identity_for_schema_key(&desired_ir, target_table_key)?;
        let accepted_identity = table_identity_for_schema_key(&accepted_ir, source_table_key)?;
        if source_entry.identity != desired_identity || desired_identity != accepted_identity {
            return Err(OmniError::manifest_internal(format!(
                "schema rename '{}' -> '{}' changed table identity",
                source_table_key, target_table_key
            )));
        }
        let canonical_target_path =
            crate::db::manifest::table_path_for_identity(target_table_key, desired_identity)?;
        if canonical_target_path != source_entry.dataset_path {
            return Err(OmniError::manifest_internal(format!(
                "schema rename '{}' -> '{}' would change physical path '{}' to '{}'",
                source_table_key,
                target_table_key,
                source_entry.dataset_path,
                canonical_target_path
            )));
        }
    }
    // A DropType tombstones the table's manifest entry at
    // version+1 with no per-table write.
    for dropped_table_key in &dropped_tables {
        let entry = snapshot.dataset(dropped_table_key).ok_or_else(|| {
            OmniError::manifest(format!("missing table '{}' for drop", dropped_table_key))
        })?;
        let tombstone_version = entry.published_dataset_version.saturating_add(1);
        table_tombstones.insert(
            entry.identity,
            (
                dropped_table_key.clone(),
                tombstone_version,
                entry.native_dataset_branch.clone(),
            ),
        );
    }

    // Complete effect envelope: the outer `apply_schema` already holds the
    // graph-wide schema gate, so add main's branch gate and every live table
    // gate in the shared schema -> branch -> sorted-table order. Schema apply
    // is graph-global (including metadata-only changes and type drops), so a
    // rewrite-only subset is not a sufficient envelope.
    let schema_apply_queue_keys: Vec<(String, Option<String>)> = snapshot
        .datasets()
        .map(|entry| (entry.type_key.clone(), entry.native_dataset_branch.clone()))
        .collect();
    let _main_branch_guard = db.write_queue().acquire_branch(None).await;
    let _schema_apply_queue_guards = db
        .write_queue()
        .acquire_many(&schema_apply_queue_keys)
        .await;

    // The snapshot was captured before the branch/table waits. Revalidate the
    // complete authority token now, while those gates are held, so a stale
    // plan never stages an effect. This intent also binds the numeric version:
    // physical-only manifest movement requires a fresh preparation.
    db.refresh_coordinator_only().await?;
    let (current_branch_identifier, current_graph_head) = {
        let coordinator = db.coordinator.read().await;
        (
            coordinator.branch_identifier().await?,
            coordinator.exact_graph_head(),
        )
    };
    if current_branch_identifier != base_branch_identifier {
        return Err(OmniError::manifest_read_set_changed(
            "branch_identifier:main",
            Some(
                serde_json::to_string(&base_branch_identifier).map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "serialize captured main branch identifier: {error}"
                    ))
                })?,
            ),
            Some(
                serde_json::to_string(&current_branch_identifier).map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "serialize current main branch identifier: {error}"
                    ))
                })?,
            ),
        ));
    }
    if current_graph_head != base_graph_head {
        return Err(OmniError::manifest_read_set_changed(
            "graph_head:main",
            base_graph_head.clone(),
            current_graph_head,
        ));
    }
    let current_snapshot = db.coordinator.read().await.snapshot();
    if current_snapshot.graph_manifest_version() != snapshot.graph_manifest_version() {
        return Err(OmniError::manifest_read_set_changed(
            "prepared_schema_manifest_version",
            Some(snapshot.graph_manifest_version().to_string()),
            Some(current_snapshot.graph_manifest_version().to_string()),
        ));
    }
    let (current_catalog, current_identity) =
        db.accepted_catalog_for_snapshot(&current_snapshot).await?;
    validate_bound_catalog_against_snapshot(&current_catalog, &current_snapshot)?;
    if current_identity != accepted_identity {
        return Err(OmniError::manifest_read_set_changed(
            "schema_identity",
            Some(format!(
                "{}:{}",
                accepted_identity.schema_identity_version, accepted_identity.schema_ir_hash
            )),
            Some(format!(
                "{}:{}",
                current_identity.schema_identity_version, current_identity.schema_ir_hash
            )),
        ));
    }

    let mut existing_heads = HashMap::<String, SnapshotHandle>::new();
    for entry in snapshot.datasets() {
        let dataset_uri = db.storage().dataset_uri(&entry.dataset_path);
        let head = db.open_pinned_for_write(&dataset_uri, entry).await?;
        existing_heads.insert(entry.type_key.clone(), head);
    }

    // Plan every table's schema evolution from its manifest before any table
    // moves: a column whose type or nullability would change, a required
    // added column or a mismatched property identity refuses here, with the
    // graph unchanged. The effects stage exactly these plans.
    let mut evolutions = BTreeMap::<String, crate::table_store::SchemaEvolution>::new();
    for table_key in &evolved_tables {
        if added_tables.contains(table_key) {
            continue;
        }
        let source_table_key = renamed_tables.get(table_key).unwrap_or(table_key);
        let source_ds = existing_heads.get(source_table_key).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "missing preflighted source table '{}' for schema evolution",
                source_table_key
            ))
        })?;
        let target = schema_for_table_key(&desired_catalog, table_key)?;
        let mut renames = property_renames
            .get(table_key)
            .map(|renames| {
                renames
                    .iter()
                    .map(|(to, from)| (from.clone(), to.clone()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        renames.sort();
        let evolution = TableStore::plan_schema_evolution(source_ds.dataset(), &target, &renames)
            .map_err(|error| {
            OmniError::manifest(format!(
                "schema apply cannot evolve table '{source_table_key}' in place: {error}"
            ))
        })?;
        evolutions.insert(table_key.clone(), evolution);
    }

    let mut published_commit: Option<String> = None;
    let effects = async {
        fail(&SCHEMA_APPLY_POST_LOCK_PRE_EFFECT)?;
        let mut expected_table_versions = HashMap::<crate::db::manifest::TableIdentity, u64>::new();

        for table_key in &added_tables {
            let identity = table_identity_for_schema_key(&desired_ir, table_key)?;
            let table_path = crate::db::manifest::table_path_for_identity(table_key, identity)?;
            let dataset_uri = db.storage().dataset_uri(&table_path);
            let schema = schema_for_table_key(&desired_catalog, table_key)?;
            let existing = match db.storage().open_dataset_head(&dataset_uri, None).await {
                Ok(existing) => Some(existing),
                Err(error)
                    if error.storage_failure().is_some_and(|failure| {
                        failure.kind == crate::error::StorageFailureKind::NotFound
                    }) =>
                {
                    None
                }
                Err(error) => return Err(error),
            };
            let (ds, detached_transaction) = if let Some(existing) = existing {
                if db
                    .storage()
                    .validate_initial_empty_table(&existing, &schema)
                    .await?
                {
                    (existing, None)
                } else {
                    let staged = db
                        .storage()
                        .stage_overwrite(&existing, RecordBatch::new_empty(schema))
                        .await?;
                    let witness = crate::table_store::StagingWitness::new(
                        &base_branch_identifier,
                        base_graph_head.as_deref(),
                    )?;
                    let (detached, transaction) = db
                        .storage()
                        .commit_staged_detached(existing, staged, &witness)
                        .await?;
                    (detached, Some(transaction))
                }
            } else {
                let staged = db
                    .storage()
                    .stage_create(&dataset_uri, RecordBatch::new_empty(schema))
                    .await?;
                let outcome = db
                    .storage()
                    .commit_staged_create_exact(&dataset_uri, staged)
                    .await?;
                if !outcome.is_exact() {
                    return Err(OmniError::manifest_internal(format!(
                        "SchemaApply first-touch '{}' committed outside its version-one create",
                        table_key
                    )));
                }
                (outcome.into_snapshot(), None)
            };
            let state = db.storage().table_state(&dataset_uri, &ds).await?;
            let (published_dataset_version, version_metadata) =
                if let Some(transaction) = detached_transaction {
                    (
                        2,
                        state
                            .version_metadata
                            .with_staged(state.version, transaction.uuid)
                            .with_last_linear_version(Some(1)),
                    )
                } else {
                    (1, state.version_metadata.with_last_linear_version(Some(1)))
                };
            expected_table_versions.insert(identity, 0);
            table_registrations.insert(table_key.clone(), (identity, table_path));
            table_updates.insert(
                identity,
                crate::db::DatasetUpdate {
                    identity,
                    type_key: table_key.clone(),
                    published_dataset_version,
                    native_dataset_branch: None,
                    entity_count: state.row_count,
                    version_metadata,
                },
            );
            fail(&SCHEMA_APPLY_POST_TABLE_COMMIT)?;
        }

        for (table_key, mut evolution) in evolutions {
            let table_key = &table_key;
            let source_table_key = renamed_tables.get(table_key).unwrap_or(table_key);
            let entry = snapshot.dataset(source_table_key).ok_or_else(|| {
                OmniError::manifest(format!(
                    "missing source table '{}' for schema apply targeting '{}'",
                    source_table_key, table_key
                ))
            })?;
            // Reuse the handle that was opened and pin-checked before the
            // effects; reopening here would introduce a second HEAD observation.
            let mut tip = existing_heads.remove(source_table_key).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "missing preflighted source table '{}' for schema apply",
                    source_table_key
                ))
            })?;
            let identity = table_identity_for_schema_key(&desired_ir, table_key)?;
            if identity != entry.identity {
                return Err(OmniError::manifest_internal(format!(
                    "SchemaApply evolution '{}' changed table identity {} to {}",
                    table_key, entry.identity, identity
                )));
            }
            let dataset_uri = db.storage().dataset_uri(&entry.dataset_path);
            let witness = crate::table_store::StagingWitness::new(
                &base_branch_identifier,
                base_graph_head.as_deref(),
            )?;
            // A Project for renames and drops, then a Merge for additions,
            // each a detached commit of the previous; the pin names the tip.
            // Neither writes a data file, so every surviving index keeps its
            // coverage and no row or Blob payload is read.
            let mut tip_transaction = None;
            while let Some(staged) = db
                .storage()
                .stage_schema_evolution(&tip, &mut evolution)
                .await?
            {
                let (next, transaction) = db
                    .storage()
                    .commit_staged_detached(tip, staged, &witness)
                    .await?;
                tip = next;
                tip_transaction = Some(transaction);
                fail(&SCHEMA_APPLY_POST_TABLE_COMMIT)?;
            }
            let Some(transaction) = tip_transaction else {
                // The table already has the target schema: nothing to publish.
                continue;
            };
            let state = db.storage().table_state(&dataset_uri, &tip).await?;
            let published_dataset_version = entry.published_dataset_version + 1;
            let version_metadata = state
                .version_metadata
                .with_staged(state.version, transaction.uuid.clone())
                .with_last_linear_version(entry.version_metadata.last_linear_version());
            expected_table_versions.insert(identity, entry.published_dataset_version);
            table_updates.insert(
                identity,
                crate::db::DatasetUpdate {
                    identity,
                    type_key: table_key.clone(),
                    published_dataset_version,
                    native_dataset_branch: None,
                    entity_count: state.row_count,
                    version_metadata,
                },
            );
        }

        // Index-only changes (AddConstraint, i.e. adding an `@index`) are pure
        // metadata: the new `@index` intent is recorded in the desired catalog/IR
        // persisted below, and the physical index is materialized off the critical
        // path by `ensure_indices`/`optimize` (iss-848). Schema apply touches no
        // table data for them, so there is no per-table loop here and no pin.
        // Reads stay correct meanwhile via a scan.

        let mut manifest_changes = Vec::new();
        let mut expected_versions = crate::db::manifest::ExpectedTableVersions::new();
        for (table_key, (identity, table_path)) in table_registrations {
            expected_versions.insert(
                identity,
                crate::db::manifest::TableVersionExpectation {
                    table_key: table_key.clone(),
                    table_version: 0,
                    native_ref: crate::db::manifest::NativeRefPin::Unchecked,
                },
            );
            manifest_changes.push(ManifestChange::RegisterTable(TableRegistration {
                identity,
                table_key,
                table_path,
            }));
        }
        for (target_table_key, source_table_key) in &renamed_tables {
            let source_entry = snapshot.dataset(source_table_key).ok_or_else(|| {
                OmniError::manifest(format!(
                    "missing source table '{}' for schema rename publication",
                    source_table_key
                ))
            })?;
            expected_versions.insert(
                source_entry.identity,
                crate::db::manifest::TableVersionExpectation {
                    table_key: source_table_key.clone(),
                    table_version: source_entry.published_dataset_version,
                    native_ref: crate::db::manifest::NativeRefPin::Exact(
                        source_entry.native_dataset_branch.clone(),
                    ),
                },
            );
            manifest_changes.push(ManifestChange::RenameTable(
                crate::db::manifest::TableRename {
                    identity: source_entry.identity,
                    expected_table_key: source_table_key.clone(),
                    table_key: target_table_key.clone(),
                    table_path: source_entry.dataset_path.clone(),
                },
            ));
        }
        for update in table_updates.into_values() {
            let expected = expected_table_versions
                .get(&update.identity)
                .copied()
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "missing SchemaApply expected version for '{}'",
                        update.type_key
                    ))
                })?;
            let expected_table_key = renamed_tables
                .get(&update.type_key)
                .cloned()
                .unwrap_or_else(|| update.type_key.clone());
            let native_ref = snapshot
                .dataset(&expected_table_key)
                .map(|entry| {
                    crate::db::manifest::NativeRefPin::Exact(entry.native_dataset_branch.clone())
                })
                .unwrap_or(crate::db::manifest::NativeRefPin::Unchecked);
            expected_versions.insert(
                update.identity,
                crate::db::manifest::TableVersionExpectation {
                    table_key: expected_table_key,
                    table_version: expected,
                    native_ref,
                },
            );
            manifest_changes.push(ManifestChange::Update(update));
        }
        for (identity, (table_key, tombstone_version, native_ref)) in table_tombstones {
            expected_versions.insert(
                identity,
                crate::db::manifest::TableVersionExpectation {
                    table_key: table_key.clone(),
                    table_version: tombstone_version.saturating_sub(1),
                    native_ref: crate::db::manifest::NativeRefPin::Exact(native_ref),
                },
            );
            manifest_changes.push(ManifestChange::Tombstone(TableTombstone {
                identity,
                table_key,
                tombstone_version,
            }));
        }

        manifest_changes.push(ManifestChange::SchemaContract(render_schema_contract(
            &desired_ir,
            desired_schema_source,
        )?));

        let precondition = crate::db::manifest::PublishPrecondition::ExactGraphVersion {
            authority: crate::db::manifest::GraphHeadExpectation::new(
                None,
                base_branch_identifier.clone(),
                base_graph_head.clone(),
            ),
            version: snapshot.graph_manifest_version(),
        };
        let published = db
            .coordinator
            .write()
            .await
            .commit_changes_with_intent_and_expected(
                &manifest_changes,
                &expected_versions,
                lineage_intent,
                &precondition,
            )
            .await?;
        published_commit = Some(published.commit.graph_commit_id.clone());

        db.store_schema_view(
            desired_catalog,
            desired_schema_source.to_string(),
            &desired_ir,
        )?;
        db.runtime_cache.invalidate_all().await;
        if changed_edge_tables {
            db.invalidate_graph_index().await;
        }
        fail(&SCHEMA_APPLY_AFTER_MANIFEST_COMMIT)?;
        Ok::<PublishedSnapshot, OmniError>(published)
    }
    .await;

    let published = match effects {
        Ok(published) => published,
        Err(error) => {
            return Err(match published_commit {
                Some(graph_commit_id) => {
                    OmniError::recovery_required(graph_commit_id, error.to_string())
                }
                None => error,
            });
        }
    };

    Ok(SchemaApplyResult {
        supported: true,
        applied: true,
        graph_manifest_version: published.graph_manifest_version,
        steps: plan.steps,
        commit: Some(published.commit),
        contract: prepared.desired_contract().clone(),
    })
}
