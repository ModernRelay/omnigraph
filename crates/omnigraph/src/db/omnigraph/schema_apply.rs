use super::*;

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

fn pre_minted_schema_transaction(
    read_version: u64,
) -> crate::table_store::StagedTransactionIdentity {
    crate::table_store::StagedTransactionIdentity {
        read_version,
        uuid: format!("omnigraph-schema-{}", ulid::Ulid::new()),
    }
}

fn resolve_desired_schema_ir(
    accepted_ir: &SchemaIR,
    desired_schema_source: &str,
) -> Result<SchemaIR> {
    let desired_shape = read_schema_shape_from_source(desired_schema_source)?;
    let resolution = omnigraph_compiler::resolve_schema_ir(accepted_ir, &desired_shape)
        .map_err(|error| OmniError::manifest(error.to_string()))?;
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
    db.ensure_schema_state_valid().await?;
    let accepted_ir = read_accepted_schema_ir(db.uri(), Arc::clone(&db.storage)).await?;
    let desired_ir = resolve_desired_schema_ir(&accepted_ir, desired_schema_source)?;
    let mut plan = plan_schema_migration(&accepted_ir, &desired_ir)
        .map_err(|err| OmniError::manifest(err.to_string()))?;
    promote_drops_to_hard(&mut plan, options.allow_data_loss);
    Ok(plan)
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
    db.ensure_schema_state_valid().await?;
    let accepted_ir = read_accepted_schema_ir(db.uri(), Arc::clone(&db.storage)).await?;
    plan_schema_for_apply_from_accepted(db, desired_schema_source, options, &accepted_ir).await
}

async fn plan_schema_for_apply_from_accepted(
    db: &Omnigraph,
    desired_schema_source: &str,
    options: SchemaApplyOptions,
    accepted_ir: &SchemaIR,
) -> Result<PlannedSchemaApply> {
    let branches = db.coordinator.read().await.all_branches().await?;
    // Skip `main` and internal system branches (the schema-apply lock branch,
    // the cluster-wide schema-apply serializer). Legacy `__run__*` staging
    // branches were swept off `__manifest` by the v2→v3 migration that runs in
    // `Omnigraph::open(ReadWrite)` before this check (MR-770), so they no
    // longer appear here.
    let blocking_branches = branches
        .into_iter()
        .filter(|branch| branch != "main" && !is_internal_system_branch(branch))
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
    fixup_blob_schemas(&mut desired_catalog);
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

    // Converge any pending recovery sidecar before planning: a table
    // rewrite over sidecar-covered drift would otherwise re-plan from
    // the manifest pin and orphan the drifted Phase-B commit (silently
    // dropping its rows) while the stale sidecar lingers to misclassify
    // against the post-apply pins. Runs before the apply's own sidecar
    // exists, so the heal can never observe it.
    db.heal_pending_recovery_sidecars().await?;

    // Process-local schema-control gate. RFC-022 mutation/load commit paths
    // acquire this before their branch/table gates and retain it through
    // publication. Taking it before the durable sentinel closes the old race in
    // which schema apply could create the sentinel while a mutation already held
    // a table queue, causing that mutation to advance Lance HEAD and only then
    // discover the schema lock. The native sentinel remains the cross-handle /
    // crash-visible authority; this queue removes the avoidable same-handle race.
    let schema_gate_key = crate::db::manifest::schema_apply_serial_queue_key();
    let _schema_gate = db.write_queue().acquire(&schema_gate_key).await;
    acquire_schema_apply_lock(db).await?;
    let result =
        apply_schema_with_lock(db, desired_schema_source, options, actor, validate_catalog).await;
    let release_result = release_schema_apply_lock(db).await;
    match (result, release_result) {
        (Ok(result), Ok(())) => Ok(result),
        (Ok(_), Err(err)) => Err(err),
        (Err(err), Ok(())) => Err(err),
        (Err(err), Err(_)) => Err(err),
    }
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
    // Capture the accepted contract, compiled catalog, manifest snapshot, and
    // main authority as one operation-local view while the schema gate and
    // durable SchemaApply sentinel are held. A long-lived handle's ArcSwap
    // catalog may lag another handle, so it is never an authority here.
    db.refresh_coordinator_only().await?;
    let (accepted_ir, accepted_schema_state) =
        load_validated_schema_contract(db.uri(), Arc::clone(&db.storage)).await?;
    let mut accepted_catalog = build_catalog_from_ir(&accepted_ir)?;
    fixup_blob_schemas(&mut accepted_catalog);
    let accepted_catalog = Arc::new(accepted_catalog);
    let planned =
        plan_schema_for_apply_from_accepted(db, desired_schema_source, options, &accepted_ir)
            .await?;
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
            manifest_version: db.version().await,
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
    let mut renamed_tables = BTreeMap::new();
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
                renamed_tables.insert(target_key, source_key);
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
                //     snapshot_at_version(pre_drop) still see it.
                //   * Hard: run cleanup_old_versions on the dataset
                //     post-publish, removing the prior version (and
                //     reclaiming any fragments unique to it). After
                //     cleanup, time-travel back fails.
                let table_key = schema_table_key(*type_kind, type_name);
                if table_key.starts_with("edge:") {
                    changed_edge_tables = true;
                }
                if matches!(mode, DropMode::Hard) {
                    let entry = snapshot.entry(&table_key).ok_or_else(|| {
                        OmniError::manifest(format!(
                            "missing table '{}' for hard property drop",
                            table_key
                        ))
                    })?;
                    let full_uri = format!("{}/{}", db.root_uri, entry.table_path);
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
                    let entry = snapshot.entry(&table_key).ok_or_else(|| {
                        OmniError::manifest(format!(
                            "missing table '{}' for hard type drop",
                            table_key
                        ))
                    })?;
                    let full_uri = format!("{}/{}", db.root_uri, entry.table_path);
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
        BTreeMap::<crate::db::manifest::TableIdentity, crate::db::SubTableUpdate>::new();
    let mut table_tombstones = BTreeMap::<crate::db::manifest::TableIdentity, (String, u64)>::new();

    // Pre-mint every table transaction before recovery is armed. Existing
    // rewrites are exact Overwrite transactions at their manifest pins;
    // AddType targets are strict first-version Overwrites at read_version=0.
    // A type rename is metadata-only: it preserves identity, path, version,
    // and Lance history. When a rename also changes properties, only that
    // property rewrite advances the existing dataset at the preserved path.
    // Metadata/tombstone-only applies deliberately have no table effects but
    // still arm recovery because schema staging is durable state.
    let mut recovery_pins = Vec::new();
    let mut recovery_effects = Vec::new();
    let mut recovery_slots = Vec::new();
    let mut planned_transactions = HashMap::<
        crate::db::manifest::TableIdentity,
        crate::table_store::StagedTransactionIdentity,
    >::new();
    for table_key in &added_tables {
        let identity = table_identity_for_schema_key(&desired_ir, table_key)?;
        let table_path = crate::db::manifest::table_path_for_identity(table_key, identity)?;
        let planned = pre_minted_schema_transaction(0);
        recovery_pins.push(crate::db::manifest::SidecarTablePin {
            identity,
            table_key: table_key.clone(),
            table_path: db.storage().dataset_uri(&table_path),
            expected_version: 0,
            post_commit_pin: 1,
            confirmed_version: None,
            table_branch: None,
        });
        planned_transactions.insert(identity, planned.clone());
        recovery_effects.push(crate::db::manifest::RecoverySchemaApplyEffect {
            identity,
            table_key: table_key.clone(),
            kind: crate::db::manifest::RecoverySchemaApplyEffectKind::FirstTouchDataset {
                planned_transaction: planned,
                confirmed_transaction: None,
            },
        });
        recovery_slots.push(crate::db::manifest::RecoveryTableUpdateSlot {
            identity,
            table_key: table_key.clone(),
            expected_version: 0,
            table_branch: None,
            confirmed: None,
        });
    }
    for table_key in &rewritten_tables {
        if added_tables.contains(table_key) {
            continue;
        }
        let source_table_key = renamed_tables.get(table_key).unwrap_or(table_key);
        let entry = snapshot.entry(source_table_key).ok_or_else(|| {
            OmniError::manifest(format!(
                "missing source table '{}' for schema apply recovery plan targeting '{}'",
                source_table_key, table_key
            ))
        })?;
        if entry.table_branch.is_some() {
            return Err(OmniError::manifest_internal(format!(
                "schema apply expected main-owned table '{}', found branch {:?}",
                source_table_key, entry.table_branch
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
        let planned = pre_minted_schema_transaction(entry.table_version);
        recovery_pins.push(crate::db::manifest::SidecarTablePin {
            identity,
            table_key: table_key.clone(),
            table_path: db.storage().dataset_uri(&entry.table_path),
            expected_version: entry.table_version,
            post_commit_pin: entry.table_version + 1,
            confirmed_version: None,
            table_branch: None,
        });
        planned_transactions.insert(identity, planned.clone());
        recovery_effects.push(crate::db::manifest::RecoverySchemaApplyEffect {
            identity,
            table_key: table_key.clone(),
            kind: crate::db::manifest::RecoverySchemaApplyEffectKind::ExistingOverwrite {
                planned_transaction: planned,
                confirmed_transaction: None,
            },
        });
        recovery_slots.push(crate::db::manifest::RecoveryTableUpdateSlot {
            identity,
            table_key: table_key.clone(),
            expected_version: entry.table_version,
            table_branch: None,
            confirmed: None,
        });
    }
    // Capture registrations, metadata-only renames, and tombstones for the sidecar so
    // recovery can publish them alongside the per-table updates. Without
    // this, an added type's dataset is created in Phase B but the
    // manifest never gains an entry for it after roll-forward — the
    // live `_schema.pg` declares a type the manifest doesn't know about
    // and reads through the engine fail with "no manifest entry for X".
    let mut sidecar_registrations: Vec<crate::db::manifest::SidecarTableRegistration> = Vec::new();
    for table_key in &added_tables {
        let identity = table_identity_for_schema_key(&desired_ir, table_key)?;
        sidecar_registrations.push(crate::db::manifest::SidecarTableRegistration {
            identity,
            table_key: table_key.clone(),
            table_path: crate::db::manifest::table_path_for_identity(table_key, identity)?,
            table_branch: None,
        });
    }
    let mut sidecar_renames = Vec::<crate::db::manifest::SidecarTableRename>::new();
    for (target_table_key, source_table_key) in &renamed_tables {
        let source_entry = snapshot.entry(source_table_key).ok_or_else(|| {
            OmniError::manifest(format!(
                "missing source table '{}' for schema rename when building recovery sidecar",
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
        if canonical_target_path != source_entry.table_path {
            return Err(OmniError::manifest_internal(format!(
                "schema rename '{}' -> '{}' would change physical path '{}' to '{}'",
                source_table_key, target_table_key, source_entry.table_path, canonical_target_path
            )));
        }
        sidecar_renames.push(crate::db::manifest::SidecarTableRename {
            identity: desired_identity,
            expected_table_key: source_table_key.clone(),
            expected_version: source_entry.table_version,
            table_key: target_table_key.clone(),
            table_path: source_entry.table_path.clone(),
        });
    }
    let mut sidecar_tombstones: Vec<crate::db::manifest::SidecarTombstone> = Vec::new();
    // Soft DropType: mark each dropped table for tombstoning in the
    // recovery sidecar AND in the live table_tombstones map. The
    // mechanism mirrors rename's source-table tombstone — manifest
    // entry removed at version+1, dataset files retained, time-travel
    // reachable until cleanup. No Phase B write happens for these
    // tables; the recovery sidecar is purely the manifest delta.
    for dropped_table_key in &dropped_tables {
        let entry = snapshot.entry(dropped_table_key).ok_or_else(|| {
            OmniError::manifest(format!(
                "missing table '{}' for soft drop when building recovery sidecar",
                dropped_table_key
            ))
        })?;
        let tombstone_version = entry.table_version.saturating_add(1);
        sidecar_tombstones.push(crate::db::manifest::SidecarTombstone {
            identity: entry.identity,
            table_key: dropped_table_key.clone(),
            tombstone_version,
        });
        table_tombstones.insert(
            entry.identity,
            (dropped_table_key.clone(), tombstone_version),
        );
    }

    // Complete effect envelope: the outer `apply_schema` already holds the
    // graph-wide schema gate, so add main's branch gate and every live table
    // gate in the shared schema -> branch -> sorted-table order. Schema apply
    // is graph-global (including metadata-only changes and hard drops), so a
    // rewrite-only subset is not a sufficient envelope.
    let schema_apply_queue_keys: Vec<(String, Option<String>)> = snapshot
        .entries()
        .map(|entry| (entry.table_key.clone(), entry.table_branch.clone()))
        .collect();
    // The outer `apply_schema` holds the schema-control serialization key from
    // before sentinel creation through sentinel release. Per-table guards here
    // therefore cover only the concrete table effects; acquiring the schema key
    // again would deadlock because these queues are intentionally non-reentrant.
    let _main_branch_guard = db.write_queue().acquire_branch(None).await;
    let _schema_apply_queue_guards = db
        .write_queue()
        .acquire_many(&schema_apply_queue_keys)
        .await;

    // The entry heal ran before planning, but another writer can arm recovery
    // while this apply waits for its effect gates. List again under the complete
    // envelope before this writer claims any physical state.
    db.ensure_no_pending_recovery_sidecars_under_gates(&[None], "schema_apply")
        .await?;

    // The snapshot was captured before the branch/table waits. Revalidate the
    // complete authority token now, while those gates are held, so a stale
    // plan never writes a sidecar. Physical-only __manifest compaction may
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
    let current_schema_state = read_schema_state_identity(db.uri(), db.storage.as_ref()).await?;
    if current_schema_state != accepted_schema_state {
        return Err(OmniError::manifest_read_set_changed(
            "schema_identity",
            Some(format!(
                "{}:{}",
                accepted_schema_state.schema_identity_version, accepted_schema_state.schema_ir_hash
            )),
            Some(format!(
                "{}:{}",
                current_schema_state.schema_identity_version, current_schema_state.schema_ir_hash
            )),
        ));
    }

    // Prove every existing physical ref is still exactly at its manifest pin
    // before arming the exact SchemaApply sidecar. Retain the verified handles
    // and reuse them below: reopening after this ownership check would create a
    // needless second HEAD observation and blur the pre-arm boundary.
    let mut existing_heads = HashMap::<String, SnapshotHandle>::new();
    for entry in snapshot.entries() {
        let dataset_uri = db.storage().dataset_uri(&entry.table_path);
        let head = db
            .storage()
            .open_dataset_head(&dataset_uri, entry.table_branch.as_deref())
            .await?;
        db.ensure_existing_effect_baseline(
            &entry.table_key,
            entry.table_branch.as_deref(),
            entry.table_version,
            &head,
        )
        .await?;
        existing_heads.insert(entry.table_key.clone(), head);
    }

    // Only added types are first-touch paths. Rename targets deliberately reuse
    // the existing identity-owned path and therefore must already exist.
    for table_key in &added_tables {
        let identity = table_identity_for_schema_key(&desired_ir, table_key)?;
        let table_path = crate::db::manifest::table_path_for_identity(table_key, identity)?;
        let dataset_uri = db.storage().dataset_uri(&table_path);
        if db.storage_adapter().exists(&dataset_uri).await? {
            return Err(OmniError::manifest_conflict(format!(
                "schema apply target table '{}' has an existing dataset at '{}' but no live manifest entry; refusing to claim unowned physical state — inspect and remove the orphaned dataset before retrying",
                table_key, dataset_uri,
            )));
        }
    }

    // Every non-empty migration writes a sidecar, including table-effect-free
    // index/enum/metadata changes. Their empty table-pin set is intentional:
    // schema staging is the durable threshold that lets recovery deterministically
    // roll back a pre-staging crash or roll forward a post-staging crash.
    // `branch=None` because schema_apply publishes against main — the
    // `__schema_apply_lock__` branch is purely a serialization sentinel.
    let target_schema_ir_hash = omnigraph_compiler::schema_ir_hash(&desired_ir)
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    let recovery_authority = crate::db::manifest::RecoveryAuthorityToken {
        branch_identifier: base_branch_identifier.clone(),
        graph_head: base_graph_head.clone(),
        schema_identity_domain: accepted_ir.schema_identity_domain.as_str().to_string(),
        schema_ir_hash: accepted_schema_state.schema_ir_hash.clone(),
        schema_identity_version: accepted_schema_state.schema_identity_version,
    };
    let recovery_lineage = crate::db::manifest::RecoveryLineageIntent {
        graph_commit_id: lineage_intent.graph_commit_id.clone(),
        branch: lineage_intent.branch.clone(),
        actor_id: lineage_intent.actor_id.clone(),
        merged_parent_commit_id: lineage_intent.merged_parent_commit_id.clone(),
        created_at: lineage_intent.created_at,
    };
    let mut sidecar = crate::db::manifest::new_schema_apply_sidecar_v9(
        actor.map(str::to_string),
        recovery_pins,
        recovery_authority,
        recovery_lineage,
        recovery_effects,
        crate::db::manifest::RecoveryManifestDelta {
            table_updates: recovery_slots,
            registrations: sidecar_registrations,
            renames: sidecar_renames,
            tombstones: sidecar_tombstones,
        },
        target_schema_ir_hash,
    )?;
    let recovery_handle =
        crate::db::manifest::write_sidecar(db.root_uri(), db.storage_adapter(), &sidecar).await?;
    let recovery_operation_id = recovery_handle.operation_id.clone();

    let post_arm_result = async {
        crate::failpoints::maybe_fail(
            crate::failpoints::names::SCHEMA_APPLY_POST_SIDECAR_PRE_EFFECT,
        )?;
        let mut committed_transactions = HashMap::new();

        for table_key in &added_tables {
            let identity = table_identity_for_schema_key(&desired_ir, table_key)?;
            let table_path =
                crate::db::manifest::table_path_for_identity(table_key, identity)?;
            let dataset_uri = db.storage().dataset_uri(&table_path);
            let schema = schema_for_table_key(&desired_catalog, table_key)?;
            let batch = RecordBatch::new_empty(schema);
            let mut staged = db.storage().stage_create(&dataset_uri, batch).await?;
            let planned = planned_transactions.get(&identity).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "missing planned SchemaApply transaction for '{}'",
                    table_key
                ))
            })?;
            staged.bind_transaction_identity(planned)?;
            let outcome = db
                .storage()
                .commit_staged_create_exact(&dataset_uri, staged)
                .await?;
            if !outcome.is_exact() {
                return Err(OmniError::manifest_internal(format!(
                    "SchemaApply first-touch '{}' committed outside its exact version-one transaction plan",
                    table_key
                )));
            }
            committed_transactions.insert(identity, outcome.committed_transaction().clone());
            let ds = outcome.into_snapshot();
            // Indexes for the new table are materialized off the critical path by
            // ensure_indices/optimize (iss-848); a 0-row table is never trainable
            // anyway. The @index intent is recorded in the persisted catalog/IR.
            let state = db.storage().table_state(&dataset_uri, &ds).await?;
            table_registrations.insert(table_key.clone(), (identity, table_path));
            table_updates.insert(
                identity,
                crate::db::SubTableUpdate {
                    identity,
                    table_key: table_key.clone(),
                    table_version: state.version,
                    table_branch: None,
                    row_count: state.row_count,
                    version_metadata: state.version_metadata,
                },
            );
            crate::failpoints::maybe_fail(
                crate::failpoints::names::SCHEMA_APPLY_POST_TABLE_COMMIT,
            )?;
        }

        for table_key in &rewritten_tables {
            if added_tables.contains(table_key) {
                continue;
            }
            let source_table_key = renamed_tables.get(table_key).unwrap_or(table_key);
            let entry = snapshot.entry(source_table_key).ok_or_else(|| {
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
            let dataset_uri = db.storage().dataset_uri(&entry.table_path);
            // Reuse the handle that was opened and pin-checked before arming;
            // reopening here would introduce a second HEAD observation.
            let existing = source_ds;
            let mut staged = db.storage().stage_overwrite(&existing, batch).await?;
            let identity = table_identity_for_schema_key(&desired_ir, table_key)?;
            if identity != entry.identity {
                return Err(OmniError::manifest_internal(format!(
                    "SchemaApply rewrite '{}' changed table identity {} to {}",
                    table_key, entry.identity, identity
                )));
            }
            let planned = planned_transactions.get(&identity).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "missing planned SchemaApply transaction for '{}'",
                    table_key
                ))
            })?;
            staged.bind_transaction_identity(planned)?;
            let outcome = db
                .storage()
                .commit_staged_exact(existing, staged)
                .await?;
            if !outcome.is_exact() {
                return Err(OmniError::manifest_internal(format!(
                    "SchemaApply rewrite '{}' committed outside its exact transaction/version plan",
                    table_key
                )));
            }
            committed_transactions.insert(identity, outcome.committed_transaction().clone());
            let target_ds = outcome.into_snapshot();
            // The rewrite drops the table's existing index coverage; it is
            // restored off the critical path by optimize's optimize_indices /
            // ensure_indices (iss-848). Reads scan uncovered fragments meanwhile.
            let state = db.storage().table_state(&dataset_uri, &target_ds).await?;
            table_updates.insert(
                identity,
                crate::db::SubTableUpdate {
                    identity,
                    table_key: table_key.clone(),
                    table_version: state.version,
                    table_branch: None,
                    row_count: state.row_count,
                    version_metadata: state.version_metadata,
                },
            );
            crate::failpoints::maybe_fail(
                crate::failpoints::names::SCHEMA_APPLY_POST_TABLE_COMMIT,
            )?;
        }

        // Index-only changes (AddConstraint, i.e. adding an `@index`) are pure
        // metadata: the new `@index` intent is recorded in the desired catalog/IR
        // persisted below, and the physical index is materialized off the critical
        // path by `ensure_indices`/`optimize` (iss-848). Schema apply touches no
        // table data for them, so there is no per-table loop here and no recovery
        // pin (no Lance HEAD advances). Reads stay correct meanwhile via a scan.

        let mut manifest_changes = Vec::new();
        let mut expected_versions = crate::db::manifest::ExpectedTableVersions::new();
        for (table_key, (identity, table_path)) in table_registrations {
            expected_versions.insert(
                identity,
                crate::db::manifest::TableVersionExpectation {
                    table_key: table_key.clone(),
                    table_version: 0,
                },
            );
            manifest_changes.push(ManifestChange::RegisterTable(TableRegistration {
                identity,
                table_key,
                table_path,
            }));
        }
        for (target_table_key, source_table_key) in &renamed_tables {
            let source_entry = snapshot.entry(source_table_key).ok_or_else(|| {
                OmniError::manifest(format!(
                    "missing source table '{}' for schema rename publication",
                    source_table_key
                ))
            })?;
            expected_versions.insert(
                source_entry.identity,
                crate::db::manifest::TableVersionExpectation {
                    table_key: source_table_key.clone(),
                    table_version: source_entry.table_version,
                },
            );
            manifest_changes.push(ManifestChange::RenameTable(
                crate::db::manifest::TableRename {
                    identity: source_entry.identity,
                    expected_table_key: source_table_key.clone(),
                    table_key: target_table_key.clone(),
                    table_path: source_entry.table_path.clone(),
                },
            ));
        }
        let confirmed_updates = table_updates.into_values().collect::<Vec<_>>();
        for update in &confirmed_updates {
            let expected = planned_transactions
                .get(&update.identity)
                .map(|transaction| transaction.read_version)
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "missing SchemaApply expected version for '{}'",
                        update.table_key
                    ))
                })?;
            let expected_table_key = renamed_tables
                .get(&update.table_key)
                .cloned()
                .unwrap_or_else(|| update.table_key.clone());
            expected_versions.insert(
                update.identity,
                crate::db::manifest::TableVersionExpectation {
                    table_key: expected_table_key,
                    table_version: expected,
                },
            );
            manifest_changes.push(ManifestChange::Update(update.clone()));
        }
        for (identity, (table_key, tombstone_version)) in table_tombstones {
            expected_versions.insert(
                identity,
                crate::db::manifest::TableVersionExpectation {
                    table_key: table_key.clone(),
                    table_version: tombstone_version.saturating_sub(1),
                },
            );
            manifest_changes.push(ManifestChange::Tombstone(TableTombstone {
                identity,
                table_key,
                tombstone_version,
            }));
        }

        // Atomic schema apply: schema staging is part of the exact Phase-B
        // confirmation. Armed always means rollback; EffectsConfirmed is eligible
        // for the fixed exact-head manifest commit and subsequent promotion.
        crate::failpoints::maybe_fail(
            crate::failpoints::names::SCHEMA_APPLY_BEFORE_STAGING_WRITE,
        )?;

        let staging_pg_uri = schema_source_staging_uri(&db.root_uri);
        db.storage
            .write_text(&staging_pg_uri, desired_schema_source)
            .await?;
        write_schema_contract_staging(&db.root_uri, db.storage.as_ref(), &desired_ir).await?;
        let target_hash = sidecar
            .protocol_v7
            .as_ref()
            .expect("new SchemaApply sidecar is v7")
            .target_schema_ir_hash
            .clone();
        crate::db::schema_state::validate_exact_schema_staging_target(
            db.root_uri(),
            db.storage_adapter(),
            &target_hash,
        )
        .await?;

        crate::db::manifest::confirm_schema_apply_sidecar_v9(
            db.root_uri(),
            db.storage_adapter(),
            &mut sidecar,
            &confirmed_updates,
            &committed_transactions,
        )
        .await?;
        // Preserve the existing failpoint contract: "after staging" is the
        // recoverable roll-forward seam immediately before manifest publication.
        // In v7 the exact physical identities and complete manifest delta must be
        // durably confirmed before that seam is exposed.
        crate::failpoints::maybe_fail(
            crate::failpoints::names::SCHEMA_APPLY_AFTER_STAGING_WRITE,
        )?;

        let precondition = crate::db::manifest::PublishPrecondition::ExactGraphHead(
            crate::db::manifest::GraphHeadExpectation::new(
                None,
                base_branch_identifier.clone(),
                base_graph_head.clone(),
            ),
        );
        let PublishedSnapshot {
            manifest_version,
            _snapshot_id: _,
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

        crate::failpoints::maybe_fail(
            crate::failpoints::names::SCHEMA_APPLY_AFTER_MANIFEST_COMMIT,
        )?;
        crate::db::schema_state::promote_exact_schema_staging(
            db.root_uri(),
            db.storage_adapter(),
            &target_hash,
        )
        .await?;

        db.store_schema_view(
            desired_catalog,
            desired_schema_source.to_string(),
            &desired_ir,
        )?;
        db.coordinator.write().await.refresh().await?;
        db.runtime_cache.invalidate_all().await;
        if changed_edge_tables {
            db.invalidate_graph_index().await;
        }
        Ok::<u64, OmniError>(manifest_version)
    }
    .await;

    let manifest_version = match post_arm_result {
        Ok(manifest_version) => manifest_version,
        Err(error) => {
            return Err(OmniError::recovery_required(
                recovery_operation_id,
                error.to_string(),
            ));
        }
    };

    // Recovery sidecar lifecycle: delete after the manifest commit
    // succeeded. Best-effort: if this delete fails, the sidecar persists
    // and on next open the sweep sees every table at the post-publish
    // manifest pin (NoMovement) and the sidecar is treated as a stale
    // artifact (recovery is a no-op and the sidecar is cleaned up).
    // Failing the schema_apply call would report failure for a migration
    // that already succeeded.
    if let Err(err) =
        crate::db::manifest::delete_sidecar(&recovery_handle, db.storage_adapter()).await
    {
        tracing::warn!(
            error = %err,
            operation_id = recovery_handle.operation_id.as_str(),
            "recovery sidecar cleanup failed; the next open's recovery sweep will resolve it"
        );
    }

    // Hard-drop cleanup: run cleanup_old_versions on each dataset
    // that had a Hard mode drop step. Best-effort — the schema apply
    // is already durable. If cleanup fails, the prior data fragments
    // remain on disk as orphans (reclaimable via `omnigraph cleanup`).
    // We do NOT fail the apply on cleanup error; the manifest change
    // is the load-bearing operation.
    for (table_key, full_uri) in &hard_cleanup_targets {
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
        manifest_version,
        steps: plan.steps,
    })
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
    use chrono::Utc;
    use lance::dataset::cleanup::CleanupPolicy;
    let ds = crate::instrumentation::open_dataset(
        full_uri,
        crate::instrumentation::VersionResolution::Latest,
        None,
        crate::instrumentation::table_wrapper(),
    )
    .await?;
    let policy = CleanupPolicy {
        before_timestamp: Some(Utc::now()),
        before_version: None,
        delete_unverified: false,
        error_if_tagged_old_versions: false,
        clean_referenced_branches: false,
        delete_rate_limit: None,
    };
    let _removed = lance::dataset::cleanup::cleanup_old_versions(&ds, policy)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    let _ = db;
    Ok(())
}

pub(super) async fn ensure_schema_apply_idle(db: &Omnigraph, operation: &str) -> Result<()> {
    db.refresh_coordinator_only().await?;
    ensure_schema_apply_not_locked(db, operation).await
}

pub(super) async fn acquire_schema_apply_lock(db: &Omnigraph) -> Result<()> {
    db.ensure_schema_state_valid().await?;
    db.refresh_coordinator_only().await?;
    let branches = db.coordinator.read().await.all_branches().await?;
    if branches
        .iter()
        .any(|branch| is_schema_apply_lock_branch(branch))
    {
        return Err(OmniError::manifest_conflict(
            "schema apply is already in progress".to_string(),
        ));
    }

    db.coordinator
        .write()
        .await
        .branch_create(SCHEMA_APPLY_LOCK_BRANCH)
        .await?;
    db.refresh_coordinator_only().await?;

    let blocking_branches = db
        .coordinator
        .read()
        .await
        .all_branches()
        .await?
        .into_iter()
        .filter(|branch| branch != "main" && !is_internal_system_branch(branch))
        .collect::<Vec<_>>();
    if !blocking_branches.is_empty() {
        let _ = release_schema_apply_lock(db).await;
        return Err(OmniError::manifest_conflict(format!(
            "schema apply requires a graph with only main; found non-main branches: {}",
            blocking_branches.join(", ")
        )));
    }

    Ok(())
}

pub(super) async fn release_schema_apply_lock(db: &Omnigraph) -> Result<()> {
    db.coordinator
        .write()
        .await
        .branch_delete(SCHEMA_APPLY_LOCK_BRANCH)
        .await?;
    // Use refresh_coordinator_only — the full Omnigraph::refresh would
    // run roll-forward-only recovery, and on the failure path the
    // in-flight schema_apply sidecar is still on disk; recovery would
    // race the caller's own publish (or roll forward an aborted apply
    // we want to leave for next-open).
    db.refresh_coordinator_only().await
}

pub(super) async fn ensure_schema_apply_not_locked(db: &Omnigraph, operation: &str) -> Result<()> {
    if db
        .coordinator
        .read()
        .await
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
    source_ds: &SnapshotHandle,
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
        Arc::new(source_ds.dataset().clone())
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
