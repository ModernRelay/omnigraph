//! Explicit repair for uncovered published-dataset/Lance-HEAD drift.
//!
//! Pending pins converge automatically: the next writer or `cleanup` promotes
//! them. This module is for the different case: a dataset's Lance HEAD is
//! ahead of the version recorded in `__manifest` and no published pin explains
//! the movement. `repair` classifies that uncovered drift from Lance
//! transactions and only auto-publishes maintenance-only drift when the
//! operator confirms.

use lance::Dataset;
use lance::dataset::transaction::Operation;

use super::*;

/// Options for [`Omnigraph::repair`].
#[derive(Debug, Clone, Copy, Default)]
pub struct RepairOptions {
    /// Preview by default. With `confirm`, verified maintenance drift is
    /// published to `__manifest`.
    pub confirm: bool,
    /// Also publish suspicious/unverifiable drift. Requires `confirm`.
    pub force: bool,
}

/// Classification of a dataset's graph-manifest/Lance-HEAD state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RepairClassification {
    /// Lance HEAD equals the published dataset version.
    NoDrift,
    /// Every uncovered Lance transaction is maintenance-only (`Rewrite` or
    /// `ReserveFragments`), so publishing the HEAD is content-preserving.
    VerifiedMaintenance,
    /// At least one uncovered transaction is semantic (`Append`, `Delete`,
    /// `Update`, etc.).
    Suspicious,
    /// A needed transaction could not be read, so the drift cannot be judged.
    Unverifiable,
    /// The published pin's detached twin cannot land: a foreign linear commit
    /// occupies its target version (RFC 0067). Reads resolve the pin and
    /// mutations chain behind it; repair never adopts the foreign commit.
    BlockedPromotion,
}

impl RepairClassification {
    /// Stable machine-readable token for serialized output.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NoDrift => "no_drift",
            Self::VerifiedMaintenance => "verified_maintenance",
            Self::Suspicious => "suspicious",
            Self::Unverifiable => "unverifiable",
            Self::BlockedPromotion => "blocked_promotion",
        }
    }
}

impl std::fmt::Display for RepairClassification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What repair did for a backing dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RepairAction {
    /// Nothing to do.
    NoOp,
    /// Drift was reported but not published because this was a preview.
    Preview,
    /// Verified maintenance drift was published to `__manifest`.
    Healed,
    /// Suspicious/unverifiable drift was published because `force` was set.
    Forced,
    /// Drift was left untouched because it was not safe to publish without
    /// `force`.
    Refused,
}

impl RepairAction {
    /// Stable machine-readable token for serialized output.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NoOp => "no_op",
            Self::Preview => "preview",
            Self::Healed => "healed",
            Self::Forced => "forced",
            Self::Refused => "refused",
        }
    }
}

impl std::fmt::Display for RepairAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Per-dataset repair outcome.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DatasetRepairStats {
    /// Qualified graph type key, or `__manifest` for the system dataset.
    pub type_key: String,
    /// Published dataset version before repair.
    pub published_dataset_version: u64,
    pub lance_head_version: u64,
    pub classification: RepairClassification,
    pub action: RepairAction,
    pub operations: Vec<String>,
    pub error: Option<String>,
}

/// Whole-graph repair outcome.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RepairStats {
    pub datasets: Vec<DatasetRepairStats>,
    /// New graph-manifest version if repair published any dataset pins.
    pub graph_manifest_version: Option<u64>,
}

struct ClassificationResult {
    classification: RepairClassification,
    operations: Vec<String>,
    error: Option<String>,
}

struct RepairTableTask {
    identity: crate::db::manifest::TableIdentity,
    table_key: String,
    full_path: String,
    pinned_data_version: u64,
    pinned_native_ref: Option<String>,
    entry: crate::db::manifest::DatasetEntry,
}

pub async fn repair_all_datasets(db: &Omnigraph, options: RepairOptions) -> Result<RepairStats> {
    if options.force && !options.confirm {
        return Err(OmniError::manifest("repair --force requires --confirm"));
    }

    db.ensure_schema_state_valid().await?;
    db.ensure_schema_apply_idle("repair").await?;

    // Repair may adopt physical HEADs into graph authority. Bind the entire
    // attempt to one accepted view and revalidate after schema -> main -> table
    // gates so concurrent graph movement refuses before publication.
    let authority_txn = db.open_write_txn(None).await?;

    // Repair publishes manifest authority, so it joins the canonical writer
    // envelope. The accepted catalog, identity/path pairs, raw Lance reads, and
    // final publish all remain under schema -> main -> sorted-table gates. This
    // prevents a concurrent drop/re-add from pairing the old dataset path with
    // the replacement's same public alias and new identity.
    let _schema_guard = db
        .write_queue()
        .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
        .await;
    db.refresh_coordinator_only().await?;
    db.ensure_schema_apply_not_locked("repair").await?;
    let catalog = db.load_accepted_catalog_with_schema_gate_held().await?;
    let _main_branch_guard = db.write_queue().acquire_branch(None).await;

    let table_keys = optimize::all_table_keys(&catalog);
    let queue_keys = table_keys
        .iter()
        .map(|table_key| (table_key.clone(), None))
        .collect::<Vec<_>>();
    let _table_guards = db.write_queue().acquire_many(&queue_keys).await;

    let snapshot = db.revalidate_write_txn(&authority_txn).await?;
    let table_tasks = table_keys
        .into_iter()
        .filter_map(|table_key| {
            let entry = snapshot.dataset(&table_key)?;
            Some(RepairTableTask {
                identity: entry.identity,
                table_key,
                full_path: format!("{}/{}", db.root_uri, entry.dataset_path),
                pinned_data_version: entry.published_dataset_version,
                pinned_native_ref: entry.native_dataset_branch.clone(),
                entry: entry.clone(),
            })
        })
        .collect::<Vec<_>>();

    if table_tasks.is_empty() {
        return Ok(RepairStats {
            datasets: Vec::new(),
            graph_manifest_version: None,
        });
    }

    let mut tables = Vec::with_capacity(table_tasks.len());
    let mut updates = Vec::new();
    let mut expected = crate::db::manifest::ExpectedTableVersions::new();
    let mut any_forced = false;

    for task in table_tasks {
        let RepairTableTask {
            identity,
            table_key,
            full_path,
            pinned_data_version,
            pinned_native_ref,
            entry,
        } = task;
        // `classify_drift` inspects raw Lance transaction history
        // (`read_transaction_by_version`), a Lance-only maintenance read the
        // staged-write trait does not surface. The raw borrow is an enumerated,
        // read-only escape; repair never takes ownership or moves Lance HEAD.
        let handle = db.storage().open_dataset_head(&full_path, None).await?;
        // A pending pin is promoted before drift is judged; a blocked one is
        // reported, never adopted.
        let handle = match super::promotion::promote_pin_at_head(
            db, &table_key, &full_path, &entry, &handle,
        )
        .await?
        {
            super::promotion::PinAtHead::Carried => handle,
            super::promotion::PinAtHead::Promoted(handle) => handle,
            super::promotion::PinAtHead::Blocked(reason) => {
                tables.push(DatasetRepairStats {
                    type_key: table_key,
                    published_dataset_version: pinned_data_version,
                    lance_head_version: handle.version(),
                    classification: RepairClassification::BlockedPromotion,
                    action: RepairAction::Refused,
                    operations: Vec::new(),
                    error: Some(reason),
                });
                continue;
            }
        };
        let ds = handle.dataset();
        let lance_head_version = ds.version().version;

        if lance_head_version < pinned_data_version {
            return Err(OmniError::manifest_internal(format!(
                "{} is at Lance HEAD version {}, behind published dataset version {}",
                dataset_subject(&table_key),
                lance_head_version,
                pinned_data_version
            )));
        }

        if lance_head_version == pinned_data_version {
            tables.push(DatasetRepairStats {
                type_key: table_key,
                published_dataset_version: pinned_data_version,
                lance_head_version,
                classification: RepairClassification::NoDrift,
                action: RepairAction::NoOp,
                operations: Vec::new(),
                error: None,
            });
            continue;
        }

        let classification = classify_drift(ds, pinned_data_version, lance_head_version).await;
        let action = match (
            options.confirm,
            options.force,
            classification.classification,
        ) {
            (false, _, _) => RepairAction::Preview,
            (true, _, RepairClassification::VerifiedMaintenance) => RepairAction::Healed,
            (true, true, RepairClassification::Suspicious | RepairClassification::Unverifiable) => {
                any_forced = true;
                RepairAction::Forced
            }
            (true, _, RepairClassification::Suspicious | RepairClassification::Unverifiable) => {
                RepairAction::Refused
            }
            (true, _, RepairClassification::NoDrift) => RepairAction::NoOp,
            // Reported above, before drift is classified; never adopted.
            (true, _, RepairClassification::BlockedPromotion) => RepairAction::Refused,
        };

        if matches!(action, RepairAction::Healed | RepairAction::Forced) {
            let state = db.storage().table_state(&full_path, &handle).await?;
            updates.push(crate::db::DatasetUpdate {
                identity,
                type_key: table_key.clone(),
                published_dataset_version: state.version,
                native_dataset_branch: None,
                entity_count: state.row_count,
                version_metadata: state.version_metadata,
            });
            expected.insert(
                identity,
                crate::db::manifest::TableVersionExpectation {
                    table_key: table_key.clone(),
                    table_version: pinned_data_version,
                    native_ref: crate::db::manifest::NativeRefPin::Exact(pinned_native_ref),
                },
            );
        }

        tables.push(DatasetRepairStats {
            type_key: table_key,
            published_dataset_version: pinned_data_version,
            lance_head_version,
            classification: classification.classification,
            action,
            operations: classification.operations,
            error: classification.error,
        });
    }

    let mut judged = snapshot
        .datasets()
        .map(pin_key)
        .collect::<std::collections::HashSet<_>>();
    for branch in optimize::cleanup_graph_branches(db)
        .await?
        .into_iter()
        .flatten()
    {
        if crate::db::is_internal_system_branch(&branch) {
            continue;
        }
        let branch_snapshot = match db.fresh_snapshot_for_branch(Some(&branch)).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::warn!(branch, %error, "repair could not read a branch's pins; its blocked pins are not reported");
                continue;
            }
        };
        for entry in branch_snapshot.datasets() {
            if !judged.insert(pin_key(entry)) {
                continue;
            }
            let reason = match super::promotion::blocked_pin_reason(db, entry).await {
                Ok(Some(reason)) => reason,
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!(branch, table = %entry.type_key, %error, "repair could not judge a branch pin; it is not reported");
                    continue;
                }
            };
            let full_path = format!("{}/{}", db.root_uri, entry.dataset_path);
            let head = match db
                .storage()
                .open_dataset_head(&full_path, entry.native_dataset_branch.as_deref())
                .await
            {
                Ok(head) => head,
                Err(error) => {
                    tracing::warn!(branch, table = %entry.type_key, %error, "repair could not open a branch table head; its blocked pin is not reported");
                    continue;
                }
            };
            tables.push(DatasetRepairStats {
                type_key: entry.type_key.clone(),
                published_dataset_version: entry.published_dataset_version,
                lance_head_version: head.version(),
                classification: RepairClassification::BlockedPromotion,
                action: RepairAction::Refused,
                operations: Vec::new(),
                error: Some(format!("branch '{branch}': {reason}")),
            });
        }
    }

    let manifest_version = if updates.is_empty() {
        None
    } else {
        let actor = if any_forced {
            Some("omnigraph:repair:force")
        } else {
            Some("omnigraph:repair")
        };
        let PublishedSnapshot {
            graph_manifest_version: manifest_version,
            ..
        } = db
            .coordinator
            .write()
            .await
            .commit_updates_with_actor_with_expected(&updates, &expected, actor)
            .await?;
        db.runtime_cache.invalidate_all().await;
        if updates
            .iter()
            .any(|update| update.type_key.starts_with("edge:"))
        {
            db.invalidate_graph_index().await;
        }
        Some(manifest_version)
    };

    Ok(RepairStats {
        datasets: tables,
        graph_manifest_version: manifest_version,
    })
}

/// One pin as several branches can share it: table, lineage and target.
fn pin_key(
    entry: &crate::db::manifest::DatasetEntry,
) -> (crate::db::manifest::TableIdentity, Option<String>, u64) {
    (
        entry.identity,
        entry.native_dataset_branch.clone(),
        entry.published_dataset_version,
    )
}

async fn classify_drift(
    ds: &Dataset,
    pinned_data_version: u64,
    lance_head_version: u64,
) -> ClassificationResult {
    let mut operations = Vec::new();
    let mut saw_suspicious = false;
    let mut error = None;

    for version in pinned_data_version.saturating_add(1)..=lance_head_version {
        match ds.read_transaction_by_version(version).await {
            Ok(Some(transaction)) => {
                let operation = transaction.operation;
                operations.push(operation.name().to_string());
                if !matches!(
                    operation,
                    Operation::Rewrite { .. } | Operation::ReserveFragments { .. }
                ) {
                    saw_suspicious = true;
                }
            }
            Ok(None) => {
                error = Some(format!("missing Lance transaction for version {version}"));
                break;
            }
            Err(err) => {
                error = Some(format!(
                    "failed to read Lance transaction for version {version}: {err}"
                ));
                break;
            }
        }
    }

    let classification = if error.is_some() {
        RepairClassification::Unverifiable
    } else if saw_suspicious {
        RepairClassification::Suspicious
    } else {
        RepairClassification::VerifiedMaintenance
    };

    ClassificationResult {
        classification,
        operations,
        error,
    }
}
