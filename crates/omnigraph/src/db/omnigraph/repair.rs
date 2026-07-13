//! Explicit repair for uncovered manifest/head drift.
//!
//! Recovery sidecars handle deterministic crash residuals automatically. This
//! module is for the different case: a table's Lance HEAD is ahead of the
//! version recorded in `__manifest` and there is no sidecar encoding writer
//! intent. `repair` classifies that uncovered drift from Lance transactions and
//! only auto-publishes maintenance-only drift when the operator confirms.

use std::collections::HashMap;

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

/// Classification of a table's manifest/head state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RepairClassification {
    /// Lance HEAD equals the manifest pin.
    NoDrift,
    /// Every uncovered Lance transaction is maintenance-only (`Rewrite` or
    /// `ReserveFragments`), so publishing the HEAD is content-preserving.
    VerifiedMaintenance,
    /// At least one uncovered transaction is semantic (`Append`, `Delete`,
    /// `Update`, etc.).
    Suspicious,
    /// A needed transaction could not be read, so the drift cannot be judged.
    Unverifiable,
}

impl RepairClassification {
    /// Stable machine-readable token for serialized output.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NoDrift => "no_drift",
            Self::VerifiedMaintenance => "verified_maintenance",
            Self::Suspicious => "suspicious",
            Self::Unverifiable => "unverifiable",
        }
    }
}

impl std::fmt::Display for RepairClassification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What repair did for a table.
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

/// Per-table repair outcome.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TableRepairStats {
    pub table_key: String,
    pub manifest_version: u64,
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
    pub tables: Vec<TableRepairStats>,
    /// New graph manifest version if repair published any table pins.
    pub manifest_version: Option<u64>,
}

struct ClassificationResult {
    classification: RepairClassification,
    operations: Vec<String>,
    error: Option<String>,
}

pub async fn repair_all_tables(db: &Omnigraph, options: RepairOptions) -> Result<RepairStats> {
    if options.force && !options.confirm {
        return Err(OmniError::manifest("repair --force requires --confirm"));
    }

    db.ensure_schema_state_valid().await?;
    db.ensure_schema_apply_idle("repair").await?;
    ensure_no_pending_recovery_sidecars(db, "repair").await?;

    let snapshot = db.fresh_snapshot_for_branch(None).await?;
    let table_tasks: Vec<(String, String)> = {
        let catalog = db.catalog();
        let mut tasks = Vec::new();
        for table_key in optimize::all_table_keys(&catalog) {
            let Some(entry) = snapshot.entry(&table_key) else {
                continue;
            };
            let full_path = format!("{}/{}", db.root_uri, entry.table_path);
            tasks.push((table_key, full_path));
        }
        tasks
    };

    if table_tasks.is_empty() {
        return Ok(RepairStats {
            tables: Vec::new(),
            manifest_version: None,
        });
    }

    let queue_keys: Vec<(String, Option<String>)> = table_tasks
        .iter()
        .map(|(table_key, _)| (table_key.clone(), None))
        .collect();
    let _guards = db.write_queue().acquire_many(&queue_keys).await;
    ensure_no_pending_recovery_sidecars(db, "repair").await?;

    let snapshot = db.fresh_snapshot_for_branch(None).await?;
    let mut tables = Vec::with_capacity(table_tasks.len());
    let mut updates = Vec::new();
    let mut expected = HashMap::new();
    let mut any_forced = false;

    for (table_key, full_path) in table_tasks {
        // `classify_drift` inspects raw Lance transaction history
        // (`read_transaction_by_version`), a Lance-only maintenance read the
        // staged-write trait does not surface. The raw borrow is an enumerated,
        // read-only escape; repair never takes ownership or moves Lance HEAD.
        let handle = db.storage().open_dataset_head(&full_path, None).await?;
        let ds = handle.dataset();
        let manifest_version = snapshot
            .entry(&table_key)
            .map(|e| e.table_version)
            .ok_or_else(|| OmniError::manifest(format!("no manifest entry for {}", table_key)))?;
        let lance_head_version = ds.version().version;

        if lance_head_version < manifest_version {
            return Err(OmniError::manifest_internal(format!(
                "table '{}' Lance HEAD version {} is behind manifest version {}",
                table_key, lance_head_version, manifest_version
            )));
        }

        if lance_head_version == manifest_version {
            tables.push(TableRepairStats {
                table_key,
                manifest_version,
                lance_head_version,
                classification: RepairClassification::NoDrift,
                action: RepairAction::NoOp,
                operations: Vec::new(),
                error: None,
            });
            continue;
        }

        let classification = classify_drift(ds, manifest_version, lance_head_version).await;
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
        };

        if matches!(action, RepairAction::Healed | RepairAction::Forced) {
            let state = db.storage().table_state(&full_path, &handle).await?;
            updates.push(crate::db::SubTableUpdate {
                table_key: table_key.clone(),
                table_version: state.version,
                table_branch: None,
                row_count: state.row_count,
                version_metadata: state.version_metadata,
            });
            expected.insert(table_key.clone(), manifest_version);
        }

        tables.push(TableRepairStats {
            table_key,
            manifest_version,
            lance_head_version,
            classification: classification.classification,
            action,
            operations: classification.operations,
            error: classification.error,
        });
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
            manifest_version,
            _snapshot_id: _,
        } = db
            .coordinator
            .write()
            .await
            .commit_updates_with_actor_with_expected(&updates, &expected, actor)
            .await?;
        db.runtime_cache.invalidate_all().await;
        if updates
            .iter()
            .any(|update| update.table_key.starts_with("edge:"))
        {
            db.invalidate_graph_index().await;
        }
        Some(manifest_version)
    };

    Ok(RepairStats {
        tables,
        manifest_version,
    })
}

async fn ensure_no_pending_recovery_sidecars(db: &Omnigraph, operation: &str) -> Result<()> {
    if !crate::db::manifest::list_sidecars(db.root_uri(), db.storage_adapter())
        .await?
        .is_empty()
    {
        return Err(OmniError::manifest_conflict(format!(
            "{operation} requires a clean recovery state; reopen the graph to run the \
             recovery sweep before repairing"
        )));
    }
    Ok(())
}

async fn classify_drift(
    ds: &Dataset,
    manifest_version: u64,
    lance_head_version: u64,
) -> ClassificationResult {
    let mut operations = Vec::new();
    let mut saw_suspicious = false;
    let mut error = None;

    for version in manifest_version.saturating_add(1)..=lance_head_version {
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
