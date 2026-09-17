//! RFC 0040 Rollout step 3: the explicit system-column upgrade of one
//! legacy-vintage graph. Renames `id`/`src`/`dst` to `__id`/`__src`/`__dst`
//! in every node and edge table and installs the current-vintage schema
//! contract. Since RFC 0067 it has schema apply's shape: each rename-only
//! `Project` commits detached from the table's promoted pin, the staged
//! contract names the graph commit that publishes it, one manifest CAS
//! publishes every pin, and the held renames promote afterwards. It arms no
//! recovery sidecar: a failure before the CAS leaves only reclaimable detached
//! versions and a staging the next read-write open discards; one after it
//! leaves pins the next writer promotes and a contract the next read-write
//! open (or this handle's next write) installs. Since v10 both vintages share
//! one `__manifest` stamp, so the upgrade moves no stamp; the vintage is the
//! contract's `system-columns` feature.

use super::*;
use crate::db::manifest::UpgradeMode;
use crate::db::schema_state::SchemaState;
use crate::seams::{catalog, fail};
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
    /// The current-vintage contract is live.
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
    let result = execute_with_lock(db, actor, &accepted_ir, &accepted_schema_state).await;
    let release_result = super::schema_apply::release_schema_apply_lock(db).await;
    let graph_manifest_version = match (result, release_result) {
        (Ok(version), Ok(())) => version,
        (Ok(_), Err(err)) | (Err(err), _) => return Err(err),
    };
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

/// Stage, publish and install the upgrade under the schema-apply sentinel.
/// Returns the published graph manifest version.
async fn execute_with_lock(
    db: &Omnigraph,
    actor: Option<&str>,
    accepted_ir: &SchemaIR,
    accepted_schema_state: &SchemaState,
) -> Result<u64> {
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

    // Graph-global writer: promote every pending pin before planning, refuse
    // a blocked one, and prove each rename applies before any effect.
    let mut existing_heads = HashMap::<String, SnapshotHandle>::new();
    for entry in snapshot.datasets() {
        let dataset_uri = db.storage().dataset_uri(&entry.dataset_path);
        let head = db
            .storage()
            .open_dataset_head(&dataset_uri, entry.native_dataset_branch.as_deref())
            .await?;
        let head = db
            .promote_pending_pin(&entry.type_key, &dataset_uri, entry, head)
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

    // The staged contract is bound to this upgrade's graph commit (RFC 0067):
    // recovery installs it once that commit is in lineage and discards it
    // otherwise.
    let publication = crate::db::schema_state::SchemaPublication {
        graph_commit_id: lineage_intent.graph_commit_id.clone(),
        parent_commit_id: base_graph_head.clone(),
    };

    let mut published_commit: Option<String> = None;
    let effects = async {
        fail(&catalog::SCHEMA_APPLY_POST_LOCK_PRE_EFFECT)?;
        let mut promotions = Vec::<crate::db::HeldPromotion>::new();
        let mut manifest_changes = Vec::new();
        let mut expected_versions = crate::db::manifest::ExpectedTableVersions::new();
        for entry in snapshot.datasets() {
            let head = existing_heads.remove(&entry.type_key).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "missing preflighted table '{}' for the system-column upgrade",
                    entry.type_key
                ))
            })?;
            let renames = system_column_renames(&entry.type_key);
            let staged = db.storage().stage_rename_columns(&head, &renames).await?;
            // The rename lands as a detached version behind a pin one past
            // the published version; promotion replays it onto the linear
            // HEAD after the manifest publishes. Lance refuses a `Project`
            // replayed over its own twin, so racing promoters leave nothing.
            let dataset_uri = db.storage().dataset_uri(&entry.dataset_path);
            let base = head.clone();
            let (detached, transaction) =
                db.storage().commit_staged_detached(head, staged).await?;
            let state = db.storage().table_state(&dataset_uri, &detached).await?;
            let published_dataset_version = entry.published_dataset_version + 1;
            let version_metadata = state
                .version_metadata
                .with_staged(state.version, transaction.uuid.clone());
            promotions.push(crate::db::HeldPromotion {
                table_key: entry.type_key.clone(),
                dataset_path: entry.dataset_path.clone(),
                full_path: dataset_uri,
                table_branch: entry.native_dataset_branch.clone(),
                base,
                chain: Vec::new(),
                detached,
                target: published_dataset_version,
                uuid: transaction.uuid,
                e_tag: version_metadata.e_tag().map(str::to_string),
            });
            expected_versions.insert(
                entry.identity,
                crate::db::manifest::TableVersionExpectation {
                    table_key: entry.type_key.clone(),
                    table_version: entry.published_dataset_version,
                    native_ref: crate::db::manifest::NativeRefPin::Exact(
                        entry.native_dataset_branch.clone(),
                    ),
                },
            );
            manifest_changes.push(ManifestChange::Update(crate::db::DatasetUpdate {
                identity: entry.identity,
                type_key: entry.type_key.clone(),
                published_dataset_version,
                native_dataset_branch: entry.native_dataset_branch.clone(),
                entity_count: state.row_count,
                version_metadata,
            }));
            fail(&catalog::SCHEMA_APPLY_POST_TABLE_COMMIT)?;
        }

        // The state file is written last, so a complete staging is exactly
        // one whose state file exists; recovery reads the marker from it.
        fail(&catalog::SCHEMA_APPLY_BEFORE_STAGING_WRITE)?;
        let (_, ir_json, state_json) = crate::db::schema_state::render_schema_contract(
            &desired_ir,
            Some(publication.clone()),
        )?;
        db.storage
            .write_text(&schema_source_staging_uri(&db.root_uri), &desired_source)
            .await?;
        db.storage
            .write_text(
                &crate::db::schema_state::schema_ir_staging_uri(&db.root_uri),
                &ir_json,
            )
            .await?;
        db.storage
            .write_text(
                &crate::db::schema_state::schema_state_staging_uri(&db.root_uri),
                &state_json,
            )
            .await?;
        fail(&catalog::SCHEMA_APPLY_AFTER_STAGING_WRITE)?;

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
        published_commit = Some(publication.graph_commit_id.clone());

        fail(&catalog::SCHEMA_APPLY_AFTER_MANIFEST_COMMIT)?;
        // Install the contract from memory rather than by renaming the
        // staging (another process's open may have discarded it), then retire
        // the staging. Every write is idempotent; a crash here leaves the
        // staged copy for the next open to install the same way.
        db.storage
            .write_text(&schema_source_uri(&db.root_uri), &desired_source)
            .await?;
        crate::db::schema_state::write_schema_contract(
            &db.root_uri,
            db.storage.as_ref(),
            &crate::db::schema_state::SchemaContractText {
                source: desired_source.clone(),
                ir_json,
                state_json,
            },
        )
        .await?;
        crate::db::schema_state::cleanup_staging_files(&db.root_uri, db.storage.as_ref())
            .await?;

        db.store_schema_view(desired_catalog, desired_source, &desired_ir)?;
        db.coordinator.write().await.refresh().await?;
        db.runtime_cache.invalidate_all().await;
        db.invalidate_graph_index().await;
        match fail(&catalog::SCHEMA_APPLY_POST_PUBLISH_PRE_PROMOTION) {
            Ok(()) => db.promote_held_all(promotions).await,
            Err(error) => {
                tracing::warn!(error = %error, "system-column upgrade promotion interrupted; the next writer promotes")
            }
        }
        Ok::<u64, OmniError>(graph_manifest_version)
    }
    .await;

    match effects {
        Ok(version) => Ok(version),
        // Before publication nothing referenced is durable: the detached
        // renames and the staged contract are garbage the next open and
        // cleanup retire. After it the manifest is authoritative and only the
        // contract installation is pending, which the next read-write open or
        // this handle's write-entry heal completes from the staged copy.
        Err(error) => Err(match published_commit {
            Some(graph_commit_id) => {
                db.pending_schema_install
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                OmniError::recovery_required(graph_commit_id, error.to_string())
            }
            None => error,
        }),
    }
}
