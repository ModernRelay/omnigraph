//! RFC 0040 Rollout step 3: the explicit system-column upgrade of one
//! legacy-vintage graph. Renames `id`/`src`/`dst` to `__id`/`__src`/`__dst`
//! in every node and edge table, advances main's `__manifest` stamp from 8 to
//! 9, and promotes the current-vintage schema contract, as one exact
//! `SchemaApply` intent whose only recovery outcome is roll-forward.

use super::*;
use crate::db::manifest::UpgradeMode;
use crate::db::schema_state::SchemaState;
use omnigraph_compiler::{SYSTEM_COLUMNS_LEGACY, SYSTEM_COLUMNS_V3};
use serde::Serialize;

/// Diagnostic code every preflight refusal carries (RFC 0040 §The upgrade).
pub const SYSTEM_COLUMNS_PREFLIGHT: &str = "system_columns_preflight";

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemColumnUpgradeOptions {
    /// Run the preflight only and write nothing.
    pub check: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SystemColumnUpgradeOutcome {
    /// The graph already spells its system columns at the current vintage.
    AlreadyCurrent,
    /// Every preflight passed; nothing was written.
    CheckPassed,
    /// The stamp reads 9 and the current-vintage contract is live.
    Completed,
    /// A preflight failed; nothing was written. See `findings`.
    Refused,
}

#[derive(Debug, Clone, Serialize)]
pub struct SystemColumnUpgradeFinding {
    pub code: &'static str,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SystemColumnUpgradeReport {
    pub location: String,
    pub mode: UpgradeMode,
    pub outcome: SystemColumnUpgradeOutcome,
    pub stamp_before: u32,
    pub stamp_after: u32,
    /// Every node and edge table the upgrade renames (or renamed).
    pub tables: Vec<String>,
    pub findings: Vec<SystemColumnUpgradeFinding>,
    pub graph_manifest_version: Option<u64>,
}

impl SystemColumnUpgradeReport {
    pub fn success(&self) -> bool {
        !matches!(self.outcome, SystemColumnUpgradeOutcome::Refused)
    }

    fn refuse(&mut self, message: String) {
        self.findings.push(SystemColumnUpgradeFinding {
            code: SYSTEM_COLUMNS_PREFLIGHT,
            message,
        });
    }
}

/// The rename-only alterations one table needs: `id` on every table, `src`
/// and `dst` on an edge table.
pub(crate) fn system_column_renames(table_key: &str) -> Vec<(String, String)> {
    let legacy = SYSTEM_COLUMNS_LEGACY;
    let current = SYSTEM_COLUMNS_V3;
    let mut renames = vec![(legacy.id.to_string(), current.id.to_string())];
    if table_key.starts_with("edge:") {
        renames.push((legacy.src.to_string(), current.src.to_string()));
        renames.push((legacy.dst.to_string(), current.dst.to_string()));
    }
    renames
}

/// The current-vintage contract the upgrade promotes, derived from the
/// accepted legacy contract alone so the writer and the recovery executor
/// render byte-identical staging.
pub(crate) struct SystemColumnUpgradeTarget {
    pub(crate) desired_ir: SchemaIR,
    pub(crate) desired_source: String,
}

pub(crate) fn render_system_column_upgrade_target(
    accepted_ir: &SchemaIR,
    accepted_source: &str,
) -> Result<SystemColumnUpgradeTarget> {
    let desired_ir = omnigraph_compiler::into_system_columns_vintage(accepted_ir.clone());
    omnigraph_compiler::validate_schema_ir(&desired_ir)
        .map_err(|error| OmniError::manifest(error.to_string()))?;
    let desired_source =
        omnigraph_compiler::schema::parser::respell_legacy_system_field_references(accepted_source)
            .map_err(|error| OmniError::manifest(error.to_string()))?;
    let desired_shape = read_schema_shape_for_vintage(&desired_source, SYSTEM_COLUMNS_V3)?;
    let source_hash = omnigraph_compiler::schema_shape_hash(&desired_shape)
        .map_err(|error| OmniError::manifest(error.to_string()))?;
    let ir_hash = omnigraph_compiler::schema_shape_hash_from_ir(&desired_ir)
        .map_err(|error| OmniError::manifest(error.to_string()))?;
    if source_hash != ir_hash {
        return Err(OmniError::manifest(
            "the respelled schema source does not match the current-vintage schema; refusing before the upgrade stages anything",
        ));
    }
    Ok(SystemColumnUpgradeTarget {
        desired_ir,
        desired_source,
    })
}

/// Every property whose name a current-vintage graph reserves (a leading
/// `_`), as `Type.property`, so a refusal can name all of them at once.
pub(crate) fn reserved_property_offenders(accepted_ir: &SchemaIR) -> Vec<String> {
    let interfaces = accepted_ir
        .interfaces
        .iter()
        .flat_map(|entry| entry.properties.iter().map(move |p| (&entry.name, &p.name)));
    let nodes = accepted_ir
        .nodes
        .iter()
        .flat_map(|entry| entry.properties.iter().map(move |p| (&entry.name, &p.name)));
    let edges = accepted_ir
        .edges
        .iter()
        .flat_map(|entry| entry.properties.iter().map(move |p| (&entry.name, &p.name)));
    interfaces
        .chain(nodes)
        .chain(edges)
        .filter(|(_, property)| {
            omnigraph_compiler::schema::is_reserved_system_column_name(property)
        })
        .map(|(owner, property)| format!("{owner}.{property}"))
        .collect()
}

pub(super) async fn upgrade_system_columns(
    db: &Omnigraph,
    options: SystemColumnUpgradeOptions,
    actor: Option<&str>,
) -> Result<SystemColumnUpgradeReport> {
    db.enforce(
        omnigraph_policy::PolicyAction::SchemaApply,
        &omnigraph_policy::ResourceScope::TargetBranch("main".to_string()),
        actor,
    )?;
    let from_stamp = crate::db::manifest::stamp_for_system_columns(SYSTEM_COLUMNS_LEGACY)?;
    let to_stamp = crate::db::manifest::stamp_for_system_columns(SYSTEM_COLUMNS_V3)?;
    let mut report = SystemColumnUpgradeReport {
        location: db.uri().to_string(),
        mode: if options.check {
            UpgradeMode::Check
        } else {
            UpgradeMode::Execute
        },
        outcome: SystemColumnUpgradeOutcome::Refused,
        stamp_before: 0,
        stamp_after: 0,
        tables: Vec::new(),
        findings: Vec::new(),
        graph_manifest_version: None,
    };

    if !options.check {
        db.heal_pending_recovery_sidecars().await?;
    }
    let schema_gate_key = crate::db::manifest::schema_apply_serial_queue_key();
    let _schema_gate = db.write_queue().acquire(&schema_gate_key).await;
    db.refresh_coordinator_only().await?;
    let stamp = crate::db::manifest::internal_schema_stamp_at(db.uri(), None)
        .await?
        .ok_or_else(|| OmniError::manifest_internal("opened graph has no internal-schema stamp"))?;
    report.stamp_before = stamp;
    report.stamp_after = stamp;
    let (accepted_ir, accepted_schema_state) =
        load_validated_schema_contract(db.uri(), Arc::clone(&db.storage)).await?;
    if accepted_ir.system_columns() == SYSTEM_COLUMNS_V3 {
        report.outcome = SystemColumnUpgradeOutcome::AlreadyCurrent;
        return Ok(report);
    }

    preflight(db, &accepted_ir, stamp, from_stamp, &mut report).await?;
    if !report.findings.is_empty() {
        return Ok(report);
    }
    if options.check {
        report.outcome = SystemColumnUpgradeOutcome::CheckPassed;
        return Ok(report);
    }

    let _export_exclusion = db.reserve_export_destructive_control()?;
    super::schema_apply::acquire_schema_apply_lock(db).await?;
    let result = execute_with_lock(
        db,
        actor,
        &accepted_ir,
        &accepted_schema_state,
        from_stamp,
        to_stamp,
    )
    .await;
    let release_result = super::schema_apply::release_schema_apply_lock(db).await;
    let (graph_manifest_version, recovery_handle) = match (result, release_result) {
        (Ok(done), Ok(())) => done,
        (Ok(_), Err(err)) | (Err(err), _) => return Err(err),
    };
    if let Err(err) =
        crate::db::manifest::delete_sidecar(&recovery_handle, db.storage_adapter()).await
    {
        tracing::warn!(
            error = %err,
            operation_id = recovery_handle.operation_id.as_str(),
            "system-column upgrade sidecar cleanup failed; the next open's recovery sweep will resolve it"
        );
    }
    report.outcome = SystemColumnUpgradeOutcome::Completed;
    report.stamp_after = to_stamp;
    report.graph_manifest_version = Some(graph_manifest_version);
    Ok(report)
}

async fn preflight(
    db: &Omnigraph,
    accepted_ir: &SchemaIR,
    stamp: u32,
    from_stamp: u32,
    report: &mut SystemColumnUpgradeReport,
) -> Result<()> {
    let coordinator = db.coordinator.read().await;
    let blocking_branches = coordinator
        .all_branches()
        .await?
        .into_iter()
        .filter(|branch| branch != "main" && !is_internal_system_branch(branch))
        .collect::<Vec<_>>();
    if !blocking_branches.is_empty() {
        report.refuse(format!(
            "the system-column upgrade requires a graph with only main; found non-main branches: {}; merge what you need, then delete them, before the upgrade",
            blocking_branches.join(", ")
        ));
    }
    report.tables = coordinator
        .snapshot()
        .datasets()
        .map(|entry| entry.type_key.clone())
        .collect();
    report.tables.sort();
    drop(coordinator);

    let offenders = reserved_property_offenders(accepted_ir);
    if !offenders.is_empty() {
        report.refuse(format!(
            "properties use names reserved for system columns (names starting with '_'): {}; rename them with @rename_from in a schema apply before the upgrade",
            offenders.join(", ")
        ));
    }
    if stamp != from_stamp {
        report.refuse(format!(
            "__manifest is stamped at v{stamp}; the system-column upgrade converts a v{from_stamp} graph, so run `omnigraph upgrade` for the storage conversions first"
        ));
    }
    if report.findings.is_empty() {
        let source = db
            .storage
            .read_text(&schema_source_uri(&db.root_uri))
            .await?;
        if let Err(error) = render_system_column_upgrade_target(accepted_ir, &source) {
            report.refuse(error.to_string());
        }
    }
    Ok(())
}

/// Returns the published graph version and the armed intent's handle: the
/// caller retires the sidecar only after it released `__schema_apply_lock__`,
/// so a crash in between leaves the record that re-enters lock cleanup.
async fn execute_with_lock(
    db: &Omnigraph,
    actor: Option<&str>,
    accepted_ir: &SchemaIR,
    accepted_schema_state: &SchemaState,
    from_stamp: u32,
    to_stamp: u32,
) -> Result<(u64, crate::db::manifest::RecoverySidecarHandle)> {
    db.refresh_coordinator_only().await?;
    let accepted_source = db
        .storage
        .read_text(&schema_source_uri(&db.root_uri))
        .await?;
    let SystemColumnUpgradeTarget {
        desired_ir,
        desired_source,
    } = render_system_column_upgrade_target(accepted_ir, &accepted_source)?;
    let mut desired_catalog = build_catalog_from_ir(&desired_ir)?;
    fixup_physical_schemas(&mut desired_catalog)?;

    let (snapshot, base_branch_identifier, base_graph_head, lineage_intent) = {
        let coordinator = db.coordinator.read().await;
        (
            coordinator.snapshot(),
            coordinator.branch_identifier().await?,
            coordinator.exact_graph_head(),
            coordinator.new_lineage_intent(actor, None)?,
        )
    };

    let mut recovery_pins = Vec::new();
    let mut recovery_effects = Vec::new();
    let mut recovery_slots = Vec::new();
    let mut planned_transactions = HashMap::<
        crate::db::manifest::TableIdentity,
        crate::table_store::StagedTransactionIdentity,
    >::new();
    for entry in snapshot.datasets() {
        let planned =
            super::schema_apply::pre_minted_schema_transaction(entry.published_dataset_version);
        recovery_pins.push(crate::db::manifest::SidecarTablePin {
            table_fork_owner: None,
            identity: entry.identity,
            table_key: entry.type_key.clone(),
            table_path: db.storage().dataset_uri(&entry.dataset_path),
            expected_version: entry.published_dataset_version,
            post_commit_pin: entry.published_dataset_version + 1,
            confirmed_version: None,
            table_branch: entry.native_dataset_branch.clone(),
        });
        planned_transactions.insert(entry.identity, planned.clone());
        recovery_effects.push(crate::db::manifest::RecoverySchemaApplyEffect {
            identity: entry.identity,
            table_key: entry.type_key.clone(),
            kind: crate::db::manifest::RecoverySchemaApplyEffectKind::SystemColumnRename {
                planned_transaction: planned,
                confirmed_transaction: None,
            },
        });
        recovery_slots.push(crate::db::manifest::RecoveryTableUpdateSlot {
            identity: entry.identity,
            table_key: entry.type_key.clone(),
            expected_version: entry.published_dataset_version,
            table_branch: entry.native_dataset_branch.clone(),
            confirmed: None,
        });
    }

    let queue_keys: Vec<(String, Option<String>)> = snapshot
        .datasets()
        .map(|entry| (entry.type_key.clone(), entry.native_dataset_branch.clone()))
        .collect();
    let _main_branch_guard = db.write_queue().acquire_branch(None).await;
    let _table_guards = db.write_queue().acquire_many(&queue_keys).await;
    db.ensure_no_pending_recovery_sidecars_under_gates(&[None], "system_column_upgrade")
        .await?;

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
    if &current_schema_state != accepted_schema_state {
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

    let mut existing_heads = HashMap::<String, SnapshotHandle>::new();
    for entry in snapshot.datasets() {
        let dataset_uri = db.storage().dataset_uri(&entry.dataset_path);
        let head = db
            .storage()
            .open_dataset_head(&dataset_uri, entry.native_dataset_branch.as_deref())
            .await?;
        db.ensure_existing_effect_baseline(
            &entry.type_key,
            entry.native_dataset_branch.as_deref(),
            entry.published_dataset_version,
            &head,
        )
        .await?;
        TableStore::renamed_schema(head.dataset(), &system_column_renames(&entry.type_key))
            .map_err(|error| {
                OmniError::manifest(format!(
                    "{SYSTEM_COLUMNS_PREFLIGHT}: table '{}' cannot be renamed in place: {error}",
                    entry.type_key
                ))
            })?;
        existing_heads.insert(entry.type_key.clone(), head);
    }

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
    let mut sidecar = crate::db::manifest::new_system_column_upgrade_sidecar_v9(
        actor.map(str::to_string),
        recovery_pins,
        recovery_authority,
        recovery_lineage,
        recovery_effects,
        crate::db::manifest::RecoveryManifestDelta {
            table_updates: recovery_slots,
            registrations: Vec::new(),
            renames: Vec::new(),
            tombstones: Vec::new(),
        },
        target_schema_ir_hash.clone(),
        crate::db::manifest::RecoverySystemColumnUpgrade {
            from_stamp,
            to_stamp,
            manifest_version_after_stamp: None,
        },
    )?;
    let recovery_handle =
        crate::db::manifest::write_sidecar(db.root_uri(), db.storage_adapter(), &sidecar).await?;
    let recovery_operation_id = recovery_handle.operation_id.clone();

    let post_arm_result = async {
        crate::failpoints::maybe_fail(
            crate::failpoints::names::SCHEMA_APPLY_POST_SIDECAR_PRE_EFFECT,
        )?;
        let manifest_version_after_stamp =
            crate::db::manifest::publish_stamp_advance(db.root_uri(), from_stamp, to_stamp)
                .await?;
        sidecar
            .protocol_v7
            .as_mut()
            .expect("new system-column upgrade sidecar is v7")
            .system_column_upgrade
            .as_mut()
            .expect("new system-column upgrade sidecar carries its intent")
            .manifest_version_after_stamp = Some(manifest_version_after_stamp);
        db.refresh_coordinator_only().await?;
        crate::failpoints::maybe_fail(
            crate::failpoints::names::SYSTEM_COLUMN_UPGRADE_AFTER_STAMP_ADVANCE,
        )?;

        crate::failpoints::maybe_fail(
            crate::failpoints::names::SCHEMA_APPLY_BEFORE_STAGING_WRITE,
        )?;
        db.storage
            .write_text(&schema_source_staging_uri(&db.root_uri), &desired_source)
            .await?;
        write_schema_contract_staging(&db.root_uri, db.storage.as_ref(), &desired_ir).await?;
        crate::db::schema_state::validate_exact_schema_staging_target(
            db.root_uri(),
            db.storage_adapter(),
            &target_schema_ir_hash,
        )
        .await?;

        let mut committed_transactions = HashMap::new();
        let mut confirmed_updates = Vec::new();
        for entry in snapshot.datasets() {
            let head = existing_heads.remove(&entry.type_key).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "missing preflighted table '{}' for the system-column upgrade",
                    entry.type_key
                ))
            })?;
            let renames = system_column_renames(&entry.type_key);
            let mut staged = db.storage().stage_rename_columns(&head, &renames).await?;
            let planned = planned_transactions.get(&entry.identity).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "missing planned system-column rename transaction for '{}'",
                    entry.type_key
                ))
            })?;
            staged.bind_transaction_identity(planned)?;
            let outcome = db.storage().commit_staged_exact(head, staged).await?;
            if !outcome.is_exact() {
                return Err(OmniError::manifest_internal(format!(
                    "system-column rename of '{}' committed outside its exact transaction/version plan",
                    entry.type_key
                )));
            }
            committed_transactions.insert(entry.identity, outcome.committed_transaction().clone());
            let renamed = outcome.into_snapshot();
            let dataset_uri = db.storage().dataset_uri(&entry.dataset_path);
            let state = db.storage().table_state(&dataset_uri, &renamed).await?;
            confirmed_updates.push(crate::db::DatasetUpdate {
                identity: entry.identity,
                type_key: entry.type_key.clone(),
                published_dataset_version: state.version,
                native_dataset_branch: entry.native_dataset_branch.clone(),
                entity_count: state.row_count,
                version_metadata: state.version_metadata,
            });
            crate::failpoints::maybe_fail(
                crate::failpoints::names::SCHEMA_APPLY_POST_TABLE_COMMIT,
            )?;
        }

        crate::db::manifest::confirm_schema_apply_sidecar_v9(
            db.root_uri(),
            db.storage_adapter(),
            &mut sidecar,
            &confirmed_updates,
            &committed_transactions,
        )
        .await?;
        crate::failpoints::maybe_fail(
            crate::failpoints::names::SCHEMA_APPLY_AFTER_STAGING_WRITE,
        )?;

        let mut manifest_changes = Vec::with_capacity(confirmed_updates.len());
        let mut expected_versions = crate::db::manifest::ExpectedTableVersions::new();
        for update in &confirmed_updates {
            let planned = planned_transactions
                .get(&update.identity)
                .expect("every renamed table was planned");
            expected_versions.insert(
                update.identity,
                crate::db::manifest::TableVersionExpectation {
                    table_key: update.type_key.clone(),
                    table_version: planned.read_version,
                    native_ref: crate::db::manifest::NativeRefPin::Exact(
                        update.native_dataset_branch.clone(),
                    ),
                },
            );
            manifest_changes.push(ManifestChange::Update(update.clone()));
        }
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
        crate::failpoints::maybe_fail(
            crate::failpoints::names::SCHEMA_APPLY_AFTER_MANIFEST_COMMIT,
        )?;
        crate::db::schema_state::promote_exact_schema_staging(
            db.root_uri(),
            db.storage_adapter(),
            &target_schema_ir_hash,
        )
        .await?;

        db.store_schema_view(desired_catalog, desired_source, &desired_ir)?;
        db.coordinator.write().await.refresh().await?;
        db.runtime_cache.invalidate_all().await;
        db.invalidate_graph_index().await;
        Ok::<u64, OmniError>(graph_manifest_version)
    }
    .await;

    match post_arm_result {
        Ok(version) => Ok((version, recovery_handle)),
        Err(error) => Err(OmniError::recovery_required(
            recovery_operation_id,
            error.to_string(),
        )),
    }
}
