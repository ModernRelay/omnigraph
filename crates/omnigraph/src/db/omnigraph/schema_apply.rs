use super::*;
use crate::blob::ExternalBlobRef;
use crate::seams::{decide_seam, fail};
use futures::TryStreamExt;

const SCHEMA_BLOB_DESCRIPTOR_SCAN_ROWS: usize = 1024;
const SCHEMA_BLOB_DESCRIPTOR_SCAN_BYTES: u64 = 4 * 1024 * 1024;

/// Operator-supplied options that gate schema-apply behavior.
///
/// Today the only knob is `allow_data_loss`, which promotes
/// `DropMode::Soft` steps to `DropMode::Hard` (per chassis v1
/// commit #5). Soft is the default — drops are reversible via Lance
/// time travel until cleanup runs. Hard runs `cleanup_old_versions`
/// on the affected datasets immediately after the manifest publish,
/// making the prior column data unreachable.
#[derive(Debug, Clone, Default)]
pub struct SchemaApplyOptions {
    /// Allow destructive (data-loss) schema changes. When true, the
    /// planner promotes every `DropMode::Soft` step to
    /// `DropMode::Hard`, and the apply path runs
    /// `cleanup_old_versions` on affected datasets after the publish.
    pub allow_data_loss: bool,
}

/// Promote every `Soft` drop variant in the plan to `Hard` when
/// `allow_data_loss` is set. Idempotent on non-drop steps.
fn promote_drops_to_hard(plan: &mut SchemaMigrationPlan, allow_data_loss: bool) {
    if !allow_data_loss {
        return;
    }
    for step in &mut plan.steps {
        match step {
            SchemaMigrationStep::DropType { mode, .. }
            | SchemaMigrationStep::DropProperty { mode, .. } => {
                *mode = DropMode::Hard;
            }
            _ => {}
        }
    }
}

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

pub(super) async fn plan_schema(
    db: &Omnigraph,
    desired_schema_source: &str,
    options: SchemaApplyOptions,
) -> Result<SchemaMigrationPlan> {
    let accepted_ir = accepted_ir_for_planning(db).await?;
    let desired_ir = resolve_desired_schema_ir(&accepted_ir, desired_schema_source)?;
    let mut plan = plan_schema_migration(&accepted_ir, &desired_ir)
        .map_err(|err| OmniError::manifest(err.to_string()))?;
    promote_drops_to_hard(&mut plan, options.allow_data_loss);
    Ok(plan)
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
    options: SchemaApplyOptions,
) -> Result<PlannedSchemaApply> {
    let accepted_ir = accepted_ir_for_planning(db).await?;
    plan_schema_for_apply_from_accepted(db, desired_schema_source, options, &accepted_ir).await
}

decide_seam! {
    pub static SCHEMA_APPLY_AFTER_MANIFEST_COMMIT = ("schema_apply.after_manifest_commit", Unreachable, [Fail]);
}

decide_seam! {
    /// After each SchemaApply table effect commits (a detached rewrite or a
    /// new-table create), before the next table effect or the publication.
    pub static SCHEMA_APPLY_POST_TABLE_COMMIT = ("schema_apply.post_table_commit", Unreachable, [Fail]);
}

decide_seam! {
    /// Under the schema, branch and table gates, before the first table effect.
    pub static SCHEMA_APPLY_POST_LOCK_PRE_EFFECT = ("schema_apply.post_lock_pre_effect", Unreachable, [Fail]);
}

async fn plan_schema_for_apply_from_accepted(
    db: &Omnigraph,
    desired_schema_source: &str,
    options: SchemaApplyOptions,
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
    let mut plan = plan_schema_migration(accepted_ir, &desired_ir)
        .map_err(|err| OmniError::manifest(err.to_string()))?;
    promote_drops_to_hard(&mut plan, options.allow_data_loss);
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
    options: SchemaApplyOptions,
) -> Result<SchemaApplyPreview> {
    let planned = plan_schema_for_apply(db, desired_schema_source, options).await?;
    Ok(SchemaApplyPreview {
        plan: planned.plan,
        catalog: planned.desired_catalog,
    })
}

pub(super) async fn apply_schema<F>(
    db: &Omnigraph,
    desired_schema_source: &str,
    options: SchemaApplyOptions,
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
    apply_schema_with_lock(db, desired_schema_source, options, actor, validate_catalog).await
}

pub(super) async fn apply_schema_with_lock<F>(
    db: &Omnigraph,
    desired_schema_source: &str,
    options: SchemaApplyOptions,
    actor: Option<&str>,
    validate_catalog: F,
) -> Result<SchemaApplyResult>
where
    F: FnOnce(&Catalog) -> Result<()>,
{
    db.refresh_coordinator_only()
        .await
        .map_err(OmniError::before_effect)?;
    let (accepted_catalog, accepted_identity) = {
        let snapshot = db.coordinator.read().await.snapshot();
        db.accepted_catalog_for_snapshot(&snapshot)
            .await
            .map_err(OmniError::before_effect)?
    };
    let accepted_ir = accepted_catalog.bound_schema_ir().cloned().ok_or_else(|| {
        OmniError::manifest_internal("accepted catalog carries no bound SchemaIR")
    })?;
    let planned =
        plan_schema_for_apply_from_accepted(db, desired_schema_source, options, &accepted_ir)
            .await
            .map_err(OmniError::before_effect)?;
    validate_catalog(&planned.desired_catalog)?;
    let PlannedSchemaApply {
        plan,
        desired_ir,
        desired_catalog,
    } = planned;
    if plan.steps.is_empty() {
        return Ok(SchemaApplyResult {
            supported: true,
            applied: false,
            graph_manifest_version: db.version().await,
            steps: plan.steps,
        });
    }

    let (snapshot, base_branch_identifier, base_graph_head, lineage_intent) = {
        let coordinator = db.coordinator.read().await;
        (
            coordinator.snapshot(),
            coordinator.branch_identifier().await?,
            coordinator.exact_graph_head(),
            coordinator.new_lineage_intent(actor, None)?,
        )
    };
    let mut added_tables = BTreeSet::new();
    // Resolve every rename before classifying dependent property steps. The
    // planner currently emits RenameType first, but correctness must not depend
    // on step ordering: a same-apply rename + hard property drop still cleans
    // the source incarnation captured under its old alias.
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
    let mut rewritten_tables = BTreeSet::new();
    let mut dropped_tables = BTreeSet::new();
    // Hard-drop cleanup targets: (table_key, full_dataset_uri).
    // Populated for DropProperty { Hard } and DropType { Hard }; the
    // post-publish cleanup runs `cleanup_old_versions` on each
    // dataset to reclaim prior versions, making time-travel back
    // to pre-drop state unreachable.
    let mut hard_cleanup_targets: Vec<(String, String)> = Vec::new();
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
                rewritten_tables.insert(table_key);
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
                rewritten_tables.insert(table_key.clone());
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
                mode,
                ..
            } => {
                if matches!(type_kind, SchemaTypeKind::Interface) {
                    continue;
                }
                // Both Soft and Hard route through the existing
                // stage_overwrite rewrite path. batch_for_schema_apply_rewrite
                // iterates the *target* schema fields, so a property
                // absent from desired_catalog is naturally projected
                // away in the rebuilt batch.
                //
                // The difference between Soft and Hard is what
                // happens AFTER the manifest publish:
                //   * Soft: nothing — the prior dataset version
                //     retains the dropped column; reads at
                //     snapshot_at_graph_manifest_version(pre_drop) still see it.
                //   * Hard: run cleanup_old_versions on the dataset
                //     post-publish, removing the prior version (and
                //     reclaiming any fragments unique to it). After
                //     cleanup, time-travel back fails.
                let table_key = schema_table_key(*type_kind, type_name);
                if table_key.starts_with("edge:") {
                    changed_edge_tables = true;
                }
                if matches!(mode, DropMode::Hard) {
                    let source_table_key = renamed_tables.get(&table_key).unwrap_or(&table_key);
                    let entry = snapshot.dataset(source_table_key).ok_or_else(|| {
                        OmniError::manifest(format!(
                            "missing source table '{}' for hard property drop targeting '{}'",
                            source_table_key, table_key
                        ))
                    })?;
                    let full_uri = format!("{}/{}", db.root_uri, entry.dataset_path);
                    hard_cleanup_targets.push((table_key.clone(), full_uri));
                }
                rewritten_tables.insert(table_key);
            }
            SchemaMigrationStep::DropType {
                type_kind,
                name,
                mode,
            } => {
                if matches!(type_kind, SchemaTypeKind::Interface) {
                    continue;
                }
                // Both Soft and Hard tombstone the table's entry in
                // the current __manifest version (no per-table write).
                //
                // The difference is what happens after publish:
                //   * Soft: dataset files retained; prior __manifest
                //     versions still reference them; Lance time
                //     travel + branch-from-snapshot can read the
                //     dropped table.
                //   * Hard: run cleanup_old_versions on the orphan
                //     dataset post-publish. Prior dataset versions
                //     (and their fragments) are reclaimed. The dataset
                //     directory itself persists until a future
                //     orphan-cleanup pass — operators who need the
                //     directory gone too should run `omnigraph cleanup`
                //     and (for now) remove the directory out-of-band.
                let table_key = schema_table_key(*type_kind, name);
                if table_key.starts_with("edge:") {
                    changed_edge_tables = true;
                }
                if matches!(mode, DropMode::Hard) {
                    let entry = snapshot.dataset(&table_key).ok_or_else(|| {
                        OmniError::manifest(format!(
                            "missing table '{}' for hard type drop",
                            table_key
                        ))
                    })?;
                    let full_uri = format!("{}/{}", db.root_uri, entry.dataset_path);
                    hard_cleanup_targets.push((table_key.clone(), full_uri));
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

    for table_key in &rewritten_tables {
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
                "schema apply rewrite identity mismatch: source '{}' is {}, target '{}' is {}",
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
    // Soft and hard DropType tombstone the table's manifest entry at
    // version+1 with no per-table write. The dataset files stay reachable
    // through older manifest versions until cleanup (hard drops reclaim their
    // old versions right after publication).
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
    // is graph-global (including metadata-only changes and hard drops), so a
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
    // plan never stages an effect. Physical-only __manifest compaction may
    // change its numeric version without changing this logical authority.
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

    // Lance's logical Blob rewrite input cannot represent an existing
    // external offset/length range. Discover that unsupported persisted state
    // across the complete rewrite set before any added / lexically earlier
    // table can move. The builder repeats this check as a defensive invariant.
    for table_key in &rewritten_tables {
        if added_tables.contains(table_key) {
            continue;
        }
        let source_table_key = renamed_tables.get(table_key).unwrap_or(table_key);
        let source_ds = existing_heads.get(source_table_key).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "missing preflighted source table '{}' for schema Blob range validation",
                source_table_key
            ))
        })?;
        validate_schema_rewrite_external_ranges(
            source_ds,
            source_table_key,
            accepted_catalog.as_ref(),
            table_key,
            &desired_catalog,
            property_renames.get(table_key),
        )
        .await?;
    }

    let graph_commit_id = lineage_intent.graph_commit_id.clone();

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

        for table_key in &rewritten_tables {
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
            let source_ds = existing_heads.remove(source_table_key).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "missing preflighted source table '{}' for schema apply",
                    source_table_key
                ))
            })?;
            let batch = batch_for_schema_apply_rewrite(
                db,
                &source_ds,
                source_table_key,
                accepted_catalog.as_ref(),
                table_key,
                &desired_catalog,
                property_renames.get(table_key),
            )
            .await?;
            let dataset_uri = db.storage().dataset_uri(&entry.dataset_path);
            // Reuse the handle that was opened and pin-checked before the
            // effects; reopening here would introduce a second HEAD observation.
            let staged = db.storage().stage_overwrite(&source_ds, batch).await?;
            let identity = table_identity_for_schema_key(&desired_ir, table_key)?;
            if identity != entry.identity {
                return Err(OmniError::manifest_internal(format!(
                    "SchemaApply rewrite '{}' changed table identity {} to {}",
                    table_key, entry.identity, identity
                )));
            }
            let witness = crate::table_store::StagingWitness::new(
                &base_branch_identifier,
                base_graph_head.as_deref(),
            )?;
            let (detached, transaction) = db
                .storage()
                .commit_staged_detached(source_ds, staged, &witness)
                .await?;
            // The rewrite drops the table's existing index coverage; it is
            // restored off the critical path by optimize's optimize_indices /
            // ensure_indices (iss-848). Reads scan uncovered fragments meanwhile.
            let state = db.storage().table_state(&dataset_uri, &detached).await?;
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
            fail(&SCHEMA_APPLY_POST_TABLE_COMMIT)?;
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

        let precondition = crate::db::manifest::PublishPrecondition::ExactGraphHead(
            crate::db::manifest::GraphHeadExpectation::new(
                None,
                base_branch_identifier.clone(),
                base_graph_head.clone(),
            ),
        );
        let PublishedSnapshot {
            graph_manifest_version,
            ..
        } = db
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
        published_commit = Some(graph_commit_id);

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
        Ok::<u64, OmniError>(graph_manifest_version)
    }
    .await;

    let manifest_version = match effects {
        Ok(manifest_version) => manifest_version,
        Err(error) => {
            return Err(match published_commit {
                Some(graph_commit_id) => {
                    OmniError::recovery_required(graph_commit_id, error.to_string())
                }
                None => error,
            });
        }
    };

    // Hard-drop cleanup: run cleanup_old_versions on each dataset
    // that had a Hard mode drop step. Best-effort — the schema apply
    // is already durable. If cleanup fails, the prior data fragments
    // remain on disk as orphans (reclaimable via `omnigraph cleanup`).
    // We do NOT fail the apply on cleanup error; the manifest change
    // is the load-bearing operation.
    for (table_key, full_uri) in &stock_reclaim_targets() {
        match cleanup_dataset_old_versions(db, full_uri).await {
            Ok(()) => {}
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    table_key = table_key.as_str(),
                    "hard-drop cleanup_old_versions failed; rerun `omnigraph cleanup` to reclaim",
                );
            }
        }
    }

    Ok(SchemaApplyResult {
        supported: true,
        applied: true,
        graph_manifest_version: manifest_version,
        steps: plan.steps,
    })
}

/// The hard-drop targets stock version GC may reclaim: none, since the prior
/// version is a detached pin whose files stock GC would strip; `cleanup`
/// reclaims it once `--keep` prunes its version.
fn stock_reclaim_targets() -> Vec<(String, String)> {
    Vec::new()
}

/// Run `cleanup_old_versions` on a dataset URI with `before_timestamp = now`.
/// Removes every version older than the current, making time-travel back
/// to those versions unreachable. Used by Hard mode drops to enforce
/// "data is gone" semantics post-apply.
///
/// The dataset itself isn't deleted — for DropType { Hard }, the
/// dataset directory persists with only its current version (or, if
/// no current version was written, its pre-drop version). A future
/// orphan-cleanup pass should remove the directory entirely.
async fn cleanup_dataset_old_versions(db: &Omnigraph, full_uri: &str) -> Result<()> {
    use lance::dataset::cleanup::CleanupPolicy;
    let ds = crate::instrumentation::open_dataset(
        full_uri,
        crate::instrumentation::VersionResolution::Latest,
        None,
        crate::instrumentation::table_wrapper(),
    )
    .await?;
    let policy = CleanupPolicy {
        before_timestamp: Some(crate::dst_clock::now_utc()),
        before_version: None,
        delete_unverified: false,
        error_if_tagged_old_versions: false,
        clean_referenced_branches: false,
        delete_rate_limit: None,
    };
    let _removed = lance::dataset::cleanup::cleanup_old_versions(&ds, policy)
        .await
        .map_err(OmniError::storage)?;
    let _ = db;
    Ok(())
}

pub(super) async fn batch_for_schema_apply_rewrite(
    db: &Omnigraph,
    source_ds: &SnapshotHandle,
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
        db.storage()
            .scan_with_row_id(source_ds, None, None, None, true)
            .await?
    } else {
        db.storage().scan_batches(source_ds).await?
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
                    OmniError::manifest_internal(format!(
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
                            OmniError::blob_integrity(format!(
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

    RecordBatch::try_new(target_schema, columns).map_err(OmniError::arrow_internal)
}

/// Descriptor-only pre-effect validation for external Blob cells that a schema
/// rewrite will carry. Project only the source Blob columns that survive in
/// the target schema; this performs no external-object lookup or payload read.
async fn validate_schema_rewrite_external_ranges(
    source_ds: &SnapshotHandle,
    source_table_key: &str,
    source_catalog: &Catalog,
    target_table_key: &str,
    target_catalog: &Catalog,
    property_renames: Option<&HashMap<String, String>>,
) -> Result<()> {
    let source_blob_properties = blob_properties_for_table_key(source_catalog, source_table_key)?;
    let target_blob_properties = blob_properties_for_table_key(target_catalog, target_table_key)?;
    let mut source_columns = target_blob_properties
        .iter()
        .filter_map(|target_name| {
            let source_name = property_renames
                .and_then(|renames| renames.get(target_name))
                .unwrap_or(target_name);
            source_blob_properties
                .contains(source_name)
                .then(|| source_name.clone())
        })
        .collect::<Vec<_>>();
    source_columns.sort();
    source_columns.dedup();
    if source_columns.is_empty() {
        return Ok(());
    }

    let projection = source_columns
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut batches = crate::table_store::TableStore::scan_stream_bounded(
        source_ds.dataset(),
        Some(&projection),
        None,
        None,
        false,
        SCHEMA_BLOB_DESCRIPTOR_SCAN_ROWS,
        SCHEMA_BLOB_DESCRIPTOR_SCAN_BYTES,
    )
    .await?;
    while let Some(batch) = batches.try_next().await.map_err(OmniError::storage)? {
        for source_name in &source_columns {
            let descriptions = batch
                .column_by_name(source_name)
                .and_then(|column| column.as_any().downcast_ref::<StructArray>())
                .ok_or_else(|| {
                    OmniError::blob_integrity(format!(
                        "expected blob descriptions for '{}.{}' during pre-arm schema validation",
                        source_table_key, source_name
                    ))
                })?;
            let decoder = crate::blob::BlobDescriptorDecoder::try_new(descriptions)?;
            for row in 0..descriptions.len() {
                if let crate::blob::BlobDescriptor::External {
                    uri,
                    offset,
                    length,
                } = decoder.classify(row)?
                {
                    whole_external_uri_for_schema_rewrite(uri, offset, length)?;
                }
            }
        }
    }
    Ok(())
}

async fn rebuild_blob_column(
    _db: &Omnigraph,
    source_ds: &SnapshotHandle,
    column_name: &str,
    descriptions: &StructArray,
    row_ids: &[u64],
) -> Result<Arc<dyn Array>> {
    let decoder = crate::blob::BlobDescriptorDecoder::try_new(descriptions)?;
    let mut builder = BlobArrayBuilder::new(row_ids.len());
    let mut managed_row_ids = Vec::new();
    let mut row_descriptors = Vec::with_capacity(row_ids.len());

    for (row, row_id) in row_ids.iter().enumerate() {
        let descriptor = decoder.classify(row)?;
        if matches!(descriptor, crate::blob::BlobDescriptor::Managed { .. }) {
            managed_row_ids.push(*row_id);
        }
        row_descriptors.push(descriptor);
    }

    // Only managed rows are selected: given an external row, Lance would
    // resolve and read the referenced object, which a schema rewrite carries
    // as its URI without touching.
    let mut managed_blobs = if managed_row_ids.is_empty() {
        None
    } else {
        Some(
            Arc::new(source_ds.dataset().clone())
                .read_blobs(column_name)
                .map_err(OmniError::storage)?
                .with_row_ids(managed_row_ids)
                .preserve_order(true)
                .with_io_buffer_size_bytes(crate::storage_layer::BLOB_REBUILD_IO_BUFFER_BYTES)
                .try_into_stream()
                .await
                .map_err(OmniError::storage)?,
        )
    };

    for descriptor in row_descriptors {
        match descriptor {
            crate::blob::BlobDescriptor::Null => {
                builder.push_null().map_err(OmniError::lance_internal)?
            }
            crate::blob::BlobDescriptor::External {
                uri,
                offset,
                length,
            } => {
                let uri = whole_external_uri_for_schema_rewrite(uri, offset, length)?;
                builder.push_uri(uri).map_err(OmniError::lance_internal)?;
            }
            crate::blob::BlobDescriptor::Managed { length } => {
                let next = match managed_blobs.as_mut() {
                    Some(stream) => stream.try_next().await.map_err(OmniError::storage)?,
                    None => None,
                };
                let data = next
                    .ok_or_else(|| {
                        OmniError::blob_integrity(format!(
                            "blob rewrite for '{}' lost alignment with managed source rows",
                            column_name
                        ))
                    })?
                    .data
                    .ok_or_else(|| {
                        OmniError::blob_integrity(format!(
                            "blob rewrite for '{}' returned null for a managed description",
                            column_name
                        ))
                    })?;
                if data.len() as u64 != length {
                    return Err(OmniError::blob_integrity(format!(
                        "blob rewrite for '{}' observed managed length {}, descriptor recorded {length}",
                        column_name,
                        data.len()
                    )));
                }
                crate::instrumentation::record_blob_payload_read();
                builder
                    .push_bytes(data)
                    .map_err(OmniError::lance_internal)?;
            }
        }
    }

    if let Some(stream) = managed_blobs.as_mut()
        && stream
            .try_next()
            .await
            .map_err(OmniError::storage)?
            .is_some()
    {
        return Err(OmniError::blob_integrity(format!(
            "blob rewrite for '{}' produced extra source blobs",
            column_name
        )));
    }

    builder.finish().map_err(OmniError::lance_internal)
}

/// Lance's logical Blob input can retain a whole-object URI but cannot encode
/// a descriptor range. Refuse a valid ranged descriptor before schema staging
/// instead of silently widening it to the entire target object.
fn whole_external_uri_for_schema_rewrite(
    uri: String,
    offset: u64,
    length: Option<u64>,
) -> Result<String> {
    let reference = ExternalBlobRef {
        uri,
        offset,
        length,
    };
    if let Err(ranged) = reference.whole_object_uri() {
        return Err(OmniError::manifest(format!(
            "schema rewrite cannot preserve {ranged}"
        )));
    }
    Ok(reference.uri)
}

#[cfg(test)]
mod blob_rewrite_tests {
    use super::whole_external_uri_for_schema_rewrite;
    use crate::error::OmniError;

    #[test]
    fn schema_rewrite_never_widens_an_external_blob_range() {
        assert_eq!(
            whole_external_uri_for_schema_rewrite("s3://bucket/base/object".to_string(), 0, None,)
                .unwrap(),
            "s3://bucket/base/object"
        );
        for (offset, length) in [(1, None), (0, Some(1)), (7, Some(0))] {
            let error = whole_external_uri_for_schema_rewrite(
                "s3://user:secret@bucket/base/object?signature=private".to_string(),
                offset,
                length,
            )
            .unwrap_err();
            assert!(matches!(error, OmniError::Manifest(_)));
            assert!(
                error
                    .to_string()
                    .contains("cannot preserve ranged external Blob descriptor")
            );
            assert!(!error.to_string().contains("secret"));
            assert!(!error.to_string().contains("signature"));
            assert!(!error.to_string().contains("private"));
        }
    }
}
