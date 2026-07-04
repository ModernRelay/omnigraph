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

pub(super) async fn plan_schema(
    db: &Omnigraph,
    desired_schema_source: &str,
    options: SchemaApplyOptions,
) -> Result<SchemaMigrationPlan> {
    db.ensure_schema_state_valid().await?;
    let accepted_ir = read_accepted_schema_ir(db.uri(), Arc::clone(&db.storage)).await?;
    let desired_ir = read_schema_ir_from_source(desired_schema_source)?;
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

    let accepted_ir = read_accepted_schema_ir(db.uri(), Arc::clone(&db.storage)).await?;
    let desired_ir = read_schema_ir_from_source(desired_schema_source)?;
    let mut plan = plan_schema_migration(&accepted_ir, &desired_ir)
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

    acquire_schema_apply_lock(db).await?;
    let result = apply_schema_with_lock(db, desired_schema_source, options, validate_catalog).await;
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
    validate_catalog: F,
) -> Result<SchemaApplyResult>
where
    F: FnOnce(&Catalog) -> Result<()>,
{
    let planned = plan_schema_for_apply(db, desired_schema_source, options).await?;
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

    let snapshot = db.snapshot().await;
    let base_manifest_version = snapshot.version();
    let mut added_tables = BTreeSet::new();
    let mut renamed_tables = HashMap::new();
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
            // AddConstraint is only ever an `@index` addition (every other
            // added constraint plans as UnsupportedChange). It records intent
            // in the desired catalog/IR; the physical index is built off the
            // critical path by ensure_indices/optimize (iss-848), so the apply
            // does no table work for it — a pure metadata change like the two
            // metadata steps below.
            SchemaMigrationStep::AddConstraint { .. }
            | SchemaMigrationStep::UpdateTypeMetadata { .. }
            | SchemaMigrationStep::UpdatePropertyMetadata { .. } => {}
            SchemaMigrationStep::DropProperty {
                type_kind,
                type_name,
                mode,
                ..
            } => {
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

    let mut table_registrations = HashMap::<String, String>::new();
    let mut table_updates = HashMap::<String, crate::db::SubTableUpdate>::new();
    let mut table_tombstones = HashMap::<String, u64>::new();

    // Recovery sidecar: protect the per-table `stage_overwrite` +
    // `commit_staged` in rewritten_tables — the only tables that advance Lance
    // HEAD inline now that index building is deferred to the reconciler
    // (iss-848). Each rewritten table is exactly one commit, so
    // `post_commit_pin = expected + 1` is now exact (it was a loose lower bound
    // when index builds added extra commits); the classifier's loose-match for
    // SidecarKind::SchemaApply still accepts it.
    let recovery_pins: Vec<crate::db::manifest::SidecarTablePin> = rewritten_tables
        .iter()
        .filter_map(|table_key| {
            let entry = snapshot.entry(table_key)?;
            Some(crate::db::manifest::SidecarTablePin {
                table_key: table_key.clone(),
                table_path: db.storage().dataset_uri(&entry.table_path),
                expected_version: entry.table_version,
                post_commit_pin: entry.table_version + 1,
                // SchemaApply uses the loose match, not BranchMerge's Phase-B
                // confirmation — left None.
                confirmed_version: None,
                table_branch: entry.table_branch.clone(),
            })
        })
        .collect();
    // Capture additional registrations + tombstones for the sidecar so
    // recovery can publish them alongside the per-table updates. Without
    // this, an added type's dataset is created in Phase B but the
    // manifest never gains an entry for it after roll-forward — the
    // live `_schema.pg` declares a type the manifest doesn't know about
    // and reads through the engine fail with "no manifest entry for X".
    let mut sidecar_registrations: Vec<crate::db::manifest::SidecarTableRegistration> = Vec::new();
    for table_key in &added_tables {
        sidecar_registrations.push(crate::db::manifest::SidecarTableRegistration {
            table_key: table_key.clone(),
            table_path: table_path_for_table_key(table_key)?,
            table_branch: None,
        });
    }
    for target_table_key in renamed_tables.keys() {
        sidecar_registrations.push(crate::db::manifest::SidecarTableRegistration {
            table_key: target_table_key.clone(),
            table_path: table_path_for_table_key(target_table_key)?,
            table_branch: None,
        });
    }
    let mut sidecar_tombstones: Vec<crate::db::manifest::SidecarTombstone> = Vec::new();
    for source_table_key in renamed_tables.values() {
        let source_entry = snapshot.entry(source_table_key).ok_or_else(|| {
            OmniError::manifest(format!(
                "missing source table '{}' for schema rename when building recovery sidecar",
                source_table_key
            ))
        })?;
        sidecar_tombstones.push(crate::db::manifest::SidecarTombstone {
            table_key: source_table_key.clone(),
            tombstone_version: source_entry.table_version.saturating_add(1),
        });
    }
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
            table_key: dropped_table_key.clone(),
            tombstone_version,
        });
        table_tombstones.insert(dropped_table_key.clone(), tombstone_version);
    }

    // Acquire per-(table_key, branch) queues for every existing table
    // that schema_apply will rewrite or re-index. New tables (added or
    // renamed targets) aren't acquired — they have no existing dataset
    // to race against. Held across the per-table commit loop and the
    // manifest publish via `commit_changes_with_actor` below.
    //
    // Schema-apply already holds the graph-wide `__schema_apply_lock__`
    // sentinel branch, so these per-table acquisitions are uncontended in
    // practice. They exist for symmetry with the recovery reconciler, which
    // acquires the same queues before any `Dataset::restore` it issues for
    // SchemaApply sidecars.
    let mut schema_apply_queue_keys: Vec<(String, Option<String>)> = recovery_pins
        .iter()
        .map(|pin| (pin.table_key.clone(), pin.table_branch.clone()))
        .collect();
    // The serialization key the write-entry heal acquires before touching
    // schema staging or a SchemaApply sidecar. Per-table keys alone don't
    // cover a registration-only migration (no pins, but a sidecar and
    // staging files on disk) — without this, a concurrent write's heal can
    // promote this apply's staging files and publish its registrations out
    // from under it. Acquired whenever a sidecar will be written, held
    // through Phase D (the guards live to the end of this function).
    let writes_sidecar = !(recovery_pins.is_empty()
        && sidecar_registrations.is_empty()
        && sidecar_tombstones.is_empty());
    if writes_sidecar {
        schema_apply_queue_keys.push(crate::db::manifest::schema_apply_serial_queue_key());
    }
    let _schema_apply_queue_guards = db
        .write_queue()
        .acquire_many(&schema_apply_queue_keys)
        .await;

    let recovery_handle = if !writes_sidecar {
        None
    } else {
        // `branch=None` because schema_apply publishes against main —
        // the `__schema_apply_lock__` branch is purely a serialization
        // sentinel (acquire_schema_apply_lock creates it; the manifest
        // publish via coordinator.commit_changes_with_actor below targets
        // the coordinator's active branch, which is the pre-lock branch).
        // If the lock release fires before recovery, the lock branch is
        // gone — the sidecar must not reference it.
        let mut sidecar = crate::db::manifest::new_sidecar(
            crate::db::manifest::SidecarKind::SchemaApply,
            None,
            // `apply_schema` doesn't currently take an actor (no `apply_schema_as`
            // public API). The HTTP server's /schema/apply handler can pass actor
            // context through a follow-up addition. For now, system-attributed.
            None,
            recovery_pins,
        );
        sidecar.additional_registrations = sidecar_registrations;
        sidecar.tombstones = sidecar_tombstones;
        Some(
            crate::db::manifest::write_sidecar(db.root_uri(), db.storage_adapter(), &sidecar)
                .await?,
        )
    };

    for table_key in &added_tables {
        let table_path = table_path_for_table_key(table_key)?;
        let dataset_uri = db.storage().dataset_uri(&table_path);
        let schema = schema_for_table_key(&desired_catalog, table_key)?;
        let ds =
            SnapshotHandle::new(TableStore::create_empty_dataset(&dataset_uri, &schema).await?);
        // Indexes for the new table are materialized off the critical path by
        // ensure_indices/optimize (iss-848); a 0-row table is never trainable
        // anyway. The @index intent is recorded in the persisted catalog/IR.
        let state = db.storage().table_state(&dataset_uri, &ds).await?;
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
        let source_ds = db
            .storage()
            .open_snapshot_at_table(&snapshot, source_table_key)
            .await?;
        let current_catalog = db.catalog();
        let batch = batch_for_schema_apply_rewrite(
            db,
            &source_ds,
            source_table_key,
            &current_catalog,
            target_table_key,
            &desired_catalog,
            property_renames.get(target_table_key),
        )
        .await?;
        let table_path = table_path_for_table_key(target_table_key)?;
        let dataset_uri = db.storage().dataset_uri(&table_path);
        let target_ds = SnapshotHandle::new(TableStore::write_dataset(&dataset_uri, batch).await?);
        // Indexes on the renamed table are reconciled later (iss-848).
        let state = db.storage().table_state(&dataset_uri, &target_ds).await?;
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
        let source_ds = db
            .storage()
            .open_snapshot_at_table(&snapshot, table_key)
            .await?;
        let current_catalog = db.catalog();
        let batch = batch_for_schema_apply_rewrite(
            db,
            &source_ds,
            table_key,
            &current_catalog,
            table_key,
            &desired_catalog,
            property_renames.get(table_key),
        )
        .await?;
        let dataset_uri = db.storage().dataset_uri(&entry.table_path);
        // Pass `entry.table_branch.as_deref()` (not `None`) for
        // consistency with the indexed_tables block below. Schema
        // apply runs under `__schema_apply_lock__` which today rejects
        // non-main branches, so `entry.table_branch` is expected to be
        // `None`. But the defensive passthrough means a future relaxation
        // of the lock-check can't quietly open the wrong HEAD here.
        let existing = db
            .storage()
            .open_dataset_head(&dataset_uri, entry.table_branch.as_deref())
            .await?;
        let staged = db.storage().stage_overwrite(&existing, batch).await?;
        let target_ds = db.storage().commit_staged(existing, staged).await?;
        // The rewrite drops the table's existing index coverage; it is
        // restored off the critical path by optimize's optimize_indices /
        // ensure_indices (iss-848). Reads scan uncovered fragments meanwhile.
        let state = db.storage().table_state(&dataset_uri, &target_ds).await?;
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

    // Index-only changes (AddConstraint, i.e. adding an `@index`) are pure
    // metadata: the new `@index` intent is recorded in the desired catalog/IR
    // persisted below, and the physical index is materialized off the critical
    // path by `ensure_indices`/`optimize` (iss-848). Schema apply touches no
    // table data for them, so there is no per-table loop here and no recovery
    // pin (no Lance HEAD advances). Reads stay correct meanwhile via a scan.

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

    db.refresh_coordinator_only().await?;
    let current_manifest_version = db.version().await;
    if current_manifest_version != base_manifest_version {
        return Err(OmniError::manifest_conflict(format!(
            "schema apply lost its write lease: main advanced from v{} to v{} while schema apply was in progress",
            base_manifest_version, current_manifest_version,
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
    crate::failpoints::maybe_fail(crate::failpoints::names::SCHEMA_APPLY_BEFORE_STAGING_WRITE)?;

    let staging_pg_uri = schema_source_staging_uri(&db.root_uri);
    db.storage
        .write_text(&staging_pg_uri, desired_schema_source)
        .await?;
    write_schema_contract_staging(&db.root_uri, db.storage.as_ref(), &desired_ir).await?;

    crate::failpoints::maybe_fail(crate::failpoints::names::SCHEMA_APPLY_AFTER_STAGING_WRITE)?;

    // `apply_schema` doesn't currently take an actor; system-attributed.
    let PublishedSnapshot {
        manifest_version,
        _snapshot_id: _,
    } = db
        .coordinator
        .write()
        .await
        .commit_changes_with_actor(&manifest_changes, None)
        .await?;

    crate::failpoints::maybe_fail(crate::failpoints::names::SCHEMA_APPLY_AFTER_MANIFEST_COMMIT)?;

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

    db.store_catalog(desired_catalog);
    db.store_schema_source(desired_schema_source.to_string());
    db.coordinator.write().await.refresh().await?;
    db.runtime_cache.invalidate_all().await;
    if changed_edge_tables {
        db.invalidate_graph_index().await;
    }

    // Recovery sidecar lifecycle: delete after the manifest commit
    // succeeded. Best-effort: if this delete fails, the sidecar persists
    // and on next open the sweep sees every table at the post-publish
    // manifest pin (NoMovement) and the sidecar is treated as a stale
    // artifact (recovery is a no-op and the sidecar is cleaned up).
    // Failing the schema_apply call would report failure for a migration
    // that already succeeded.
    if let Some(handle) = recovery_handle {
        if let Err(err) = crate::db::manifest::delete_sidecar(&handle, db.storage_adapter()).await {
            tracing::warn!(
                error = %err,
                operation_id = handle.operation_id.as_str(),
                "recovery sidecar cleanup failed; the next open's recovery sweep will resolve it"
            );
        }
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

pub(super) async fn ensure_snapshot_entry_head_matches(
    db: &Omnigraph,
    entry: &SubTableEntry,
) -> Result<()> {
    let dataset_uri = db.storage().dataset_uri(&entry.table_path);
    let ds = db
        .storage()
        .open_dataset_head(
            &dataset_uri,
            entry.table_branch.as_deref(),
        )
        .await?;
    db.storage()
        .ensure_expected_version(&ds, &entry.table_key, entry.table_version)
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
