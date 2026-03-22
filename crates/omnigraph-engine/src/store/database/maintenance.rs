use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray, UInt32Array, UInt64Array};
use arrow_schema::DataType;
use lance::Dataset;
use lance::dataset::cleanup::CleanupPolicyBuilder;
use lance::dataset::optimize::{CompactionOptions as LanceCompactionOptions, compact_files};

use super::persist::{read_sparse_edge_batch, read_sparse_node_batch};
use super::{
    CDC_ANALYTICS_DATASET_DIR, CDC_ANALYTICS_STATE_FILE, CdcAnalyticsMaterializeOptions,
    CdcAnalyticsMaterializeResult, CdcAnalyticsState, CleanupOptions, CleanupResult,
    CompactOptions, CompactResult, Database, DoctorReport, now_unix_seconds_string,
};
use crate::catalog::schema_ir::SchemaIR;
use crate::error::{NanoError, Result};
use crate::store::lance_io::write_lance_batch;
use crate::store::manifest::GraphManifest;
use crate::store::metadata::DatabaseMetadata;
use crate::store::txlog::{
    CdcLogEntry, commit_manifest_and_logs, prune_logs_for_replay_window, read_cdc_log_entries,
    read_tx_catalog_entries, read_visible_cdc_entries, reconcile_logs_to_manifest,
};

/// Compact all manifest-tracked Lance datasets and commit updated dataset versions.
pub async fn compact_database(db_path: &Path, options: CompactOptions) -> Result<CompactResult> {
    let previous_manifest = GraphManifest::read(db_path)?;
    reconcile_logs_to_manifest(db_path, previous_manifest.db_version)?;
    let mut next_manifest = previous_manifest.clone();
    let mut result = CompactResult {
        datasets_considered: next_manifest.datasets.len(),
        ..Default::default()
    };

    for entry in &mut next_manifest.datasets {
        let dataset_path = db_path.join(&entry.dataset_path);
        let uri = dataset_path.to_string_lossy().to_string();
        let dataset = Dataset::open(&uri)
            .await
            .map_err(|e| NanoError::Lance(format!("open error: {}", e)))?;
        let mut dataset = dataset
            .checkout_version(entry.dataset_version)
            .await
            .map_err(|e| {
                NanoError::Lance(format!(
                    "checkout version {} error: {}",
                    entry.dataset_version, e
                ))
            })?;
        let before_version = dataset.version().version;

        let compact_opts = LanceCompactionOptions {
            target_rows_per_fragment: options.target_rows_per_fragment,
            materialize_deletions: options.materialize_deletions,
            materialize_deletions_threshold: options.materialize_deletions_threshold,
            ..Default::default()
        };

        let metrics = compact_files(&mut dataset, compact_opts, None)
            .await
            .map_err(|e| NanoError::Lance(format!("compact error: {}", e)))?;
        result.fragments_removed += metrics.fragments_removed;
        result.fragments_added += metrics.fragments_added;
        result.files_removed += metrics.files_removed;
        result.files_added += metrics.files_added;

        let after_version = dataset.version().version;
        if after_version != before_version {
            entry.dataset_version = after_version;
            result.datasets_compacted += 1;
        }
    }

    if result.datasets_compacted > 0 {
        next_manifest.db_version = previous_manifest.db_version.saturating_add(1);
        next_manifest.last_tx_id = format!("manifest-{}", next_manifest.db_version);
        next_manifest.committed_at = now_unix_seconds_string();
        commit_manifest_and_logs(db_path, &next_manifest, &[], "maintenance:compact")?;
        result.manifest_committed = true;
    }

    Ok(result)
}

/// Prune tx/CDC logs and old Lance dataset versions while preserving manifest-visible state.
pub async fn cleanup_database(db_path: &Path, options: CleanupOptions) -> Result<CleanupResult> {
    if options.retain_tx_versions == 0 {
        return Err(NanoError::Storage(
            "retain_tx_versions must be >= 1".to_string(),
        ));
    }
    if options.retain_dataset_versions == 0 {
        return Err(NanoError::Storage(
            "retain_dataset_versions must be >= 1".to_string(),
        ));
    }

    let manifest = GraphManifest::read(db_path)?;
    reconcile_logs_to_manifest(db_path, manifest.db_version)?;
    let log_prune = prune_logs_for_replay_window(db_path, options.retain_tx_versions)?;
    let mut result = CleanupResult {
        tx_rows_removed: log_prune.tx_rows_removed,
        tx_rows_kept: log_prune.tx_rows_kept,
        cdc_rows_removed: log_prune.cdc_rows_removed,
        cdc_rows_kept: log_prune.cdc_rows_kept,
        ..Default::default()
    };

    for entry in &manifest.datasets {
        let dataset_path = db_path.join(&entry.dataset_path);
        let uri = dataset_path.to_string_lossy().to_string();
        let dataset = Dataset::open(&uri)
            .await
            .map_err(|e| NanoError::Lance(format!("open error: {}", e)))?;
        dataset
            .checkout_version(entry.dataset_version)
            .await
            .map_err(|e| {
                NanoError::Lance(format!(
                    "checkout version {} error: {}",
                    entry.dataset_version, e
                ))
            })?;

        let versions = dataset
            .versions()
            .await
            .map_err(|e| NanoError::Lance(format!("list versions error: {}", e)))?;
        let effective_retain_n = versions
            .iter()
            .position(|v| v.version == entry.dataset_version)
            .map(|idx| {
                let needed_for_manifest = versions.len().saturating_sub(idx);
                options.retain_dataset_versions.max(needed_for_manifest)
            })
            .unwrap_or(options.retain_dataset_versions);
        let policy = CleanupPolicyBuilder::default()
            .retain_n_versions(&dataset, effective_retain_n)
            .await
            .map_err(|e| NanoError::Lance(format!("cleanup policy error: {}", e)))?
            .build();
        let stats = dataset
            .cleanup_with_policy(policy)
            .await
            .map_err(|e| NanoError::Lance(format!("cleanup error: {}", e)))?;
        if stats.old_versions > 0 {
            result.datasets_cleaned += 1;
        }
        result.dataset_old_versions_removed += stats.old_versions;
        result.dataset_bytes_removed += stats.bytes_removed;
    }

    Ok(result)
}

impl Database {
    pub async fn compact(&self, options: CompactOptions) -> Result<CompactResult> {
        let _writer = self.lock_writer().await;
        compact_database(&self.path, options).await
    }

    pub async fn cleanup(&self, options: CleanupOptions) -> Result<CleanupResult> {
        let _writer = self.lock_writer().await;
        cleanup_database(&self.path, options).await
    }

    /// Materialize visible CDC rows into a derived Lance dataset for analytics workloads.
    ///
    /// JSONL remains the authoritative CDC source; this dataset is best-effort acceleration.
    pub async fn materialize_cdc_analytics(
        &self,
        options: CdcAnalyticsMaterializeOptions,
    ) -> Result<CdcAnalyticsMaterializeResult> {
        let manifest = GraphManifest::read(&self.path)?;
        reconcile_logs_to_manifest(&self.path, manifest.db_version)?;

        let rows = read_visible_cdc_entries(&self.path, 0, Some(manifest.db_version))?;
        let source_rows = rows.len();
        let state = read_cdc_analytics_state(&self.path)?;
        let previous_rows = state.rows_materialized;
        let shrank = source_rows < previous_rows || manifest.db_version < state.manifest_db_version;
        let new_rows_since_last_run = if shrank {
            source_rows
        } else {
            source_rows.saturating_sub(previous_rows)
        };

        if !options.force
            && !shrank
            && options.min_new_rows > 0
            && new_rows_since_last_run < options.min_new_rows
        {
            return Ok(CdcAnalyticsMaterializeResult {
                source_rows,
                previously_materialized_rows: previous_rows,
                new_rows_since_last_run,
                materialized_rows: previous_rows.min(source_rows),
                dataset_written: false,
                skipped_by_threshold: true,
                dataset_version: state.dataset_version,
            });
        }

        let dataset_path = cdc_analytics_dataset_path(&self.path);
        let mut dataset_written = false;
        let dataset_version = if rows.is_empty() {
            if dataset_path.exists() {
                std::fs::remove_dir_all(&dataset_path)?;
                dataset_written = true;
            }
            None
        } else {
            let batch = cdc_rows_to_analytics_batch(&rows)?;
            let version = write_lance_batch(&dataset_path, batch).await?;
            dataset_written = true;
            Some(version)
        };

        write_cdc_analytics_state(
            &self.path,
            &CdcAnalyticsState {
                rows_materialized: source_rows,
                manifest_db_version: manifest.db_version,
                dataset_version,
                updated_at_unix: now_unix_seconds_string(),
            },
        )?;

        Ok(CdcAnalyticsMaterializeResult {
            source_rows,
            previously_materialized_rows: previous_rows,
            new_rows_since_last_run,
            materialized_rows: source_rows,
            dataset_written,
            skipped_by_threshold: false,
            dataset_version,
        })
    }

    /// Validate manifest/log/dataset consistency and in-memory graph integrity.
    pub async fn doctor(&self) -> Result<DoctorReport> {
        let manifest = GraphManifest::read(&self.path)?;
        reconcile_logs_to_manifest(&self.path, manifest.db_version)?;
        let metadata = DatabaseMetadata::open(&self.path)?;
        let mut issues = Vec::new();
        let mut warnings = Vec::new();
        let mut datasets = Vec::new();

        let tx_rows = read_tx_catalog_entries(&self.path)?;
        for (idx, window) in tx_rows.windows(2).enumerate() {
            if window[0].db_version >= window[1].db_version {
                issues.push(format!(
                    "non-monotonic tx db_version at rows {} and {}",
                    idx + 1,
                    idx + 2
                ));
            }
        }
        if let Some(last) = tx_rows.last() {
            if last.db_version > manifest.db_version {
                issues.push(format!(
                    "tx catalog db_version {} exceeds manifest db_version {}",
                    last.db_version, manifest.db_version
                ));
            } else if last.db_version < manifest.db_version {
                warnings.push(format!(
                    "tx catalog trimmed to db_version {} while manifest is {}",
                    last.db_version, manifest.db_version
                ));
            }
        }

        let mut datasets_checked = 0usize;
        for entry in &manifest.datasets {
            let dataset_path = self.path.join(&entry.dataset_path);
            if !dataset_path.exists() {
                issues.push(format!("dataset path missing: {}", dataset_path.display()));
                continue;
            }
            let uri = dataset_path.to_string_lossy().to_string();
            match Dataset::open(&uri).await {
                Ok(dataset) => match dataset.checkout_version(entry.dataset_version).await {
                    Ok(dataset) => {
                        datasets_checked += 1;
                        let storage_version = dataset
                            .manifest
                            .data_storage_format
                            .lance_file_version()
                            .map(|version| version.to_string())
                            .unwrap_or_else(|_| "unknown".to_string());
                        datasets.push(crate::store::database::DoctorDatasetReport {
                            kind: entry.kind.clone(),
                            type_name: entry.type_name.clone(),
                            dataset_path: entry.dataset_path.clone(),
                            dataset_version: entry.dataset_version,
                            storage_version,
                        });
                    }
                    Err(e) => {
                        issues.push(format!(
                            "dataset {} missing pinned version {}: {}",
                            entry.dataset_path, entry.dataset_version, e
                        ));
                    }
                },
                Err(e) => {
                    issues.push(format!(
                        "failed to open dataset {}: {}",
                        entry.dataset_path, e
                    ));
                }
            }
        }

        for edge_def in self.schema_ir.edge_types() {
            let src_nodes = collect_existing_ids(
                read_sparse_node_batch(&metadata, &edge_def.src_type_name).await?,
            )?;
            let dst_nodes = collect_existing_ids(
                read_sparse_node_batch(&metadata, &edge_def.dst_type_name).await?,
            )?;
            if let Some(edge_batch) = read_sparse_edge_batch(&metadata, &edge_def.name).await? {
                let src_arr = edge_batch
                    .column_by_name("src")
                    .ok_or_else(|| NanoError::Storage("edge batch missing src column".to_string()))?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| {
                        NanoError::Storage("edge src column is not UInt64".to_string())
                    })?;
                let dst_arr = edge_batch
                    .column_by_name("dst")
                    .ok_or_else(|| NanoError::Storage("edge batch missing dst column".to_string()))?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| {
                        NanoError::Storage("edge dst column is not UInt64".to_string())
                    })?;
                let mut orphan_count = 0usize;
                for row in 0..edge_batch.num_rows() {
                    if !src_nodes.contains(&src_arr.value(row))
                        || !dst_nodes.contains(&dst_arr.value(row))
                    {
                        orphan_count += 1;
                    }
                }
                if orphan_count > 0 {
                    issues.push(format!(
                        "edge type {} has {} orphan endpoint row(s)",
                        edge_def.name, orphan_count
                    ));
                }
            }
        }

        let cdc_rows = read_cdc_log_entries(&self.path)?;
        let healthy = issues.is_empty();
        Ok(DoctorReport {
            healthy,
            issues,
            warnings,
            manifest_db_version: manifest.db_version,
            datasets_checked,
            datasets,
            tx_rows: tx_rows.len(),
            cdc_rows: cdc_rows.len(),
        })
    }
}

fn collect_existing_ids(batch: Option<RecordBatch>) -> Result<HashSet<u64>> {
    let mut ids = HashSet::new();
    let Some(batch) = batch else {
        return Ok(ids);
    };
    let id_arr = batch
        .column_by_name("id")
        .ok_or_else(|| NanoError::Storage("batch missing id column".to_string()))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| NanoError::Storage("id column is not UInt64".to_string()))?;
    for row in 0..batch.num_rows() {
        ids.insert(id_arr.value(row));
    }
    Ok(ids)
}

/// Remove Lance dirs under nodes/ and edges/ that are not in the manifest.
pub(super) fn cleanup_stale_dirs(db_path: &Path, manifest: &GraphManifest) -> Result<()> {
    let valid_node_dirs: HashSet<String> = manifest
        .datasets
        .iter()
        .filter(|d| d.kind == "node")
        .map(|d| SchemaIR::dir_name(d.type_id))
        .collect();
    let valid_edge_dirs: HashSet<String> = manifest
        .datasets
        .iter()
        .filter(|d| d.kind == "edge")
        .map(|d| SchemaIR::dir_name(d.type_id))
        .collect();

    for (subdir, valid) in [("nodes", &valid_node_dirs), ("edges", &valid_edge_dirs)] {
        let dir = db_path.join(subdir);
        if dir.exists() {
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                if let Some(name) = entry.file_name().to_str()
                    && !valid.contains(name)
                {
                    let _ = std::fs::remove_dir_all(entry.path());
                }
            }
        }
    }

    Ok(())
}

fn cdc_analytics_dataset_path(db_path: &Path) -> PathBuf {
    db_path.join(CDC_ANALYTICS_DATASET_DIR)
}

fn cdc_analytics_state_path(db_path: &Path) -> PathBuf {
    db_path.join(CDC_ANALYTICS_STATE_FILE)
}

pub(super) fn read_cdc_analytics_state(db_path: &Path) -> Result<CdcAnalyticsState> {
    let path = cdc_analytics_state_path(db_path);
    if !path.exists() {
        return Ok(CdcAnalyticsState::default());
    }

    let raw = std::fs::read_to_string(&path)?;
    let state: CdcAnalyticsState = serde_json::from_str(&raw).map_err(|e| {
        NanoError::Manifest(format!(
            "parse CDC analytics state {}: {}",
            path.display(),
            e
        ))
    })?;
    Ok(state)
}

fn write_cdc_analytics_state(db_path: &Path, state: &CdcAnalyticsState) -> Result<()> {
    let path = cdc_analytics_state_path(db_path);
    let json = serde_json::to_string_pretty(state)
        .map_err(|e| NanoError::Manifest(format!("serialize CDC analytics state: {}", e)))?;
    std::fs::write(path, json)?;
    Ok(())
}

fn cdc_rows_to_analytics_batch(rows: &[CdcLogEntry]) -> Result<RecordBatch> {
    use arrow_schema::{Field, Schema};

    let payload_json: Vec<String> = rows
        .iter()
        .map(|row| {
            serde_json::to_string(&row.payload)
                .map_err(|e| NanoError::Manifest(format!("serialize CDC payload: {}", e)))
        })
        .collect::<Result<Vec<_>>>()?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("tx_id", DataType::Utf8, false),
        Field::new("db_version", DataType::UInt64, false),
        Field::new("seq_in_tx", DataType::UInt32, false),
        Field::new("op", DataType::Utf8, false),
        Field::new("entity_kind", DataType::Utf8, false),
        Field::new("type_name", DataType::Utf8, false),
        Field::new("entity_key", DataType::Utf8, false),
        Field::new("payload_json", DataType::Utf8, false),
        Field::new("committed_at", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.tx_id.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.db_version).collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                rows.iter().map(|row| row.seq_in_tx).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.op.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.entity_kind.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.type_name.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.entity_key.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(payload_json)),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.committed_at.clone())
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|e| NanoError::Storage(format!("build CDC analytics batch: {}", e)))
}
