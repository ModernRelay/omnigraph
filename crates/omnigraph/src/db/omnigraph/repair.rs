//! The repair report: uncovered published-dataset/Lance-HEAD drift.
//!
//! Pending pins converge automatically: the next writer or `cleanup` promotes
//! them. This module is for the different case: a dataset's Lance HEAD is
//! ahead of the version recorded in `__manifest` and no published pin explains
//! the movement. The report classifies that drift from Lance transactions. It
//! publishes nothing: no read or write resolves a table's linear HEAD, so
//! drift never needs adopting.

use lance::Dataset;
use lance::dataset::transaction::Operation;

use super::*;

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
    /// Linear commits above the table's last linear version, which no read or
    /// write resolves (RFC "Detached-only tables"); reported, never published.
    ForeignDrift,
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
            Self::ForeignDrift => "foreign_drift",
        }
    }
}

impl std::fmt::Display for RepairClassification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What the repair report says about a backing dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RepairAction {
    /// Nothing to report: no drift, or foreign drift no read or write resolves.
    NoOp,
    /// Drift the report classified and left in place.
    Preview,
    /// The table could not be judged; its `error` says why.
    Refused,
}

impl RepairAction {
    /// Stable machine-readable token for serialized output.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NoOp => "no_op",
            Self::Preview => "preview",
            Self::Refused => "refused",
        }
    }
}

impl std::fmt::Display for RepairAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One dataset's row of the repair report.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DatasetRepairStats {
    /// Qualified graph type key, or `__manifest` for the system dataset.
    pub type_key: String,
    /// The published dataset version the registration pins.
    pub published_dataset_version: u64,
    pub lance_head_version: u64,
    pub classification: RepairClassification,
    pub action: RepairAction,
    pub operations: Vec<String>,
    pub error: Option<String>,
}

/// The repair report for the whole graph.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RepairStats {
    pub datasets: Vec<DatasetRepairStats>,
}

struct ClassificationResult {
    classification: RepairClassification,
    operations: Vec<String>,
    error: Option<String>,
}

struct RepairTableTask {
    table_key: String,
    full_path: String,
    pinned_data_version: u64,
    entry: crate::db::manifest::DatasetEntry,
}

/// The repair report on `main`, under `maintain` on `main`.
pub(super) async fn repair_report(db: &Omnigraph, actor: Option<&str>) -> Result<RepairStats> {
    db.enforce(
        omnigraph_policy::PolicyAction::Maintain,
        &omnigraph_policy::ResourceScope::Branch("main".to_string()),
        actor,
    )?;
    // The report only reads, so every failure is one before any effect.
    report_under_gates(db)
        .await
        .map_err(OmniError::before_effect_if_opaque)
}

async fn report_under_gates(db: &Omnigraph) -> Result<RepairStats> {
    db.ensure_schema_state_valid().await?;

    // Bind the whole report to one accepted view, revalidated after the
    // schema -> main -> table gates. The accepted catalog, the identity/path
    // pairs and every raw Lance read stay under those gates, so a concurrent
    // schema apply cannot pair an old dataset path with a replacement's alias,
    // and a cleanup cannot remove a version the report is reading.
    let authority_txn = db.open_write_txn(None).await?;
    let _schema_permit = db.write_queue().acquire_schema_shared().await;
    db.refresh_coordinator_only().await?;
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
                table_key,
                full_path: format!("{}/{}", db.root_uri, entry.dataset_path),
                pinned_data_version: entry.published_dataset_version,
                entry: entry.clone(),
            })
        })
        .collect::<Vec<_>>();

    let mut tables = Vec::with_capacity(table_tasks.len());
    for task in table_tasks {
        let RepairTableTask {
            table_key,
            full_path,
            pinned_data_version,
            entry,
        } = task;
        // `classify_drift` inspects raw Lance transaction history
        // (`read_transaction_by_version`), a Lance-only maintenance read the
        // staged-write trait does not surface. The raw borrow is an enumerated,
        // read-only escape; the report never takes ownership or moves Lance HEAD.
        let handle = db.storage().open_dataset_head(&full_path, None).await?;
        if let Some(row) = judge_against_last_linear_version(&table_key, &entry, &handle)? {
            tables.push(row);
            continue;
        }
        // A pending pin is promoted before drift is judged; a blocked one is
        // reported, never adopted.
        let handle = match super::promotion::promote_pin_at_head(
            db, &table_key, &full_path, &entry, &handle,
        )
        .await?
        {
            super::promotion::PinAtHead::Carried => handle,
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
        tables.push(DatasetRepairStats {
            type_key: table_key,
            published_dataset_version: pinned_data_version,
            lance_head_version,
            classification: classification.classification,
            action: RepairAction::Preview,
            operations: classification.operations,
            error: classification.error,
        });
    }

    Ok(RepairStats { datasets: tables })
}

/// The detached-only rule: the recorded last linear version is the baseline
/// (`no_drift` at it, `foreign_drift` above, an error below); a staged pin
/// without one is `no_drift`, a linear pin without one takes the v10 rule.
fn judge_against_last_linear_version(
    table_key: &str,
    entry: &crate::db::manifest::DatasetEntry,
    handle: &crate::storage_layer::SnapshotHandle,
) -> Result<Option<DatasetRepairStats>> {
    let head = handle.version();
    let row = |classification, operations| DatasetRepairStats {
        type_key: table_key.to_string(),
        published_dataset_version: entry.published_dataset_version,
        lance_head_version: head,
        classification,
        action: RepairAction::NoOp,
        operations,
        error: None,
    };
    match (
        entry.version_metadata.last_linear_version(),
        entry.version_metadata.staged_version(),
    ) {
        (Some(last), _) if head == last => Ok(Some(row(RepairClassification::NoDrift, Vec::new()))),
        (Some(last), _) if head > last => Ok(Some(row(
            RepairClassification::ForeignDrift,
            vec![format!(
                "last linear version {last}, Lance HEAD {head}: {} foreign linear version(s) above it, not resolved by any read or write",
                head - last
            )],
        ))),
        (Some(last), _) => Err(OmniError::manifest_internal(format!(
            "{} is at Lance HEAD version {head}, behind its last linear version {last}",
            dataset_subject(table_key)
        ))),
        (None, Some(_)) => Ok(Some(row(RepairClassification::NoDrift, Vec::new()))),
        (None, None) => Ok(None),
    }
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
