use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{NanoError, Result};
use crate::store::manifest::GraphManifest;

const WAL_FILENAME: &str = "_wal.jsonl";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TxCatalogEntry {
    pub tx_id: String,
    pub db_version: u64,
    pub dataset_versions: BTreeMap<String, u64>,
    pub committed_at: String,
    pub op_summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CdcLogEntry {
    pub tx_id: String,
    pub db_version: u64,
    pub seq_in_tx: u32,
    pub op: String,
    pub entity_kind: String,
    pub type_name: String,
    pub entity_key: String,
    pub payload: serde_json::Value,
    pub committed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WalEntry {
    pub tx_id: String,
    pub db_version: u64,
    pub dataset_versions: BTreeMap<String, u64>,
    pub committed_at: String,
    pub op_summary: String,
    #[serde(default)]
    pub changes: Vec<CdcLogEntry>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LogPruneStats {
    pub(crate) tx_rows_removed: usize,
    pub(crate) tx_rows_kept: usize,
    pub(crate) cdc_rows_removed: usize,
    pub(crate) cdc_rows_kept: usize,
}

impl WalEntry {
    fn from_manifest(manifest: &GraphManifest, op_summary: &str, changes: &[CdcLogEntry]) -> Self {
        let dataset_versions = manifest
            .datasets
            .iter()
            .map(|entry| (entry.dataset_path.clone(), entry.dataset_version))
            .collect();
        Self {
            tx_id: manifest.last_tx_id.clone(),
            db_version: manifest.db_version,
            dataset_versions,
            committed_at: manifest.committed_at.clone(),
            op_summary: op_summary.to_string(),
            changes: changes.to_vec(),
        }
    }

    fn tx_catalog_entry(&self) -> TxCatalogEntry {
        TxCatalogEntry {
            tx_id: self.tx_id.clone(),
            db_version: self.db_version,
            dataset_versions: self.dataset_versions.clone(),
            committed_at: self.committed_at.clone(),
            op_summary: self.op_summary.clone(),
        }
    }
}

fn wal_path(db_dir: &Path) -> PathBuf {
    db_dir.join(WAL_FILENAME)
}

fn repair_wal_log(db_dir: &Path) -> Result<()> {
    truncate_trailing_partial_jsonl(&wal_path(db_dir))?;
    Ok(())
}

pub(crate) fn reconcile_logs_to_manifest(db_dir: &Path, manifest_db_version: u64) -> Result<()> {
    repair_wal_log(db_dir)?;
    let path = wal_path(db_dir);
    let keep_len = compute_wal_visible_prefix(&path, manifest_db_version)?;
    truncate_file_to_len(&path, keep_len)?;
    Ok(())
}

fn append_wal_entry(db_dir: &Path, entry: &WalEntry) -> Result<(u64, u64)> {
    append_jsonl_row(&wal_path(db_dir), entry)
}

#[allow(dead_code)]
pub(crate) fn append_tx_catalog_entry(db_dir: &Path, entry: &TxCatalogEntry) -> Result<(u64, u64)> {
    append_wal_entry(
        db_dir,
        &WalEntry {
            tx_id: entry.tx_id.clone(),
            db_version: entry.db_version,
            dataset_versions: entry.dataset_versions.clone(),
            committed_at: entry.committed_at.clone(),
            op_summary: entry.op_summary.clone(),
            changes: Vec::new(),
        },
    )
}

pub fn read_wal_entries(db_dir: &Path) -> Result<Vec<WalEntry>> {
    read_jsonl_rows(&wal_path(db_dir))
}

pub fn read_tx_catalog_entries(db_dir: &Path) -> Result<Vec<TxCatalogEntry>> {
    Ok(read_wal_entries(db_dir)?
        .into_iter()
        .map(|entry| entry.tx_catalog_entry())
        .collect())
}

pub(crate) fn read_cdc_log_entries(db_dir: &Path) -> Result<Vec<CdcLogEntry>> {
    Ok(flatten_cdc_rows(read_wal_entries(db_dir)?))
}

/// Read CDC rows that are visible through the committed manifest window.
///
/// Visibility rules:
/// - only WAL rows with `db_version <= manifest.db_version` are considered
/// - rows are filtered to `(from_db_version_exclusive, to_db_version_inclusive]`
/// - output is ordered by `(db_version, seq_in_tx, tx_id)`
pub fn read_visible_cdc_entries(
    db_dir: &Path,
    from_db_version_exclusive: u64,
    to_db_version_inclusive: Option<u64>,
) -> Result<Vec<CdcLogEntry>> {
    let manifest = GraphManifest::read(db_dir)?;
    reconcile_logs_to_manifest(db_dir, manifest.db_version)?;

    let upper = to_db_version_inclusive
        .unwrap_or(manifest.db_version)
        .min(manifest.db_version);
    if upper <= from_db_version_exclusive {
        return Ok(Vec::new());
    }

    let visible = read_wal_entries(db_dir)?
        .into_iter()
        .filter(|entry| entry.db_version > from_db_version_exclusive && entry.db_version <= upper)
        .collect::<Vec<_>>();
    Ok(flatten_cdc_rows(visible))
}

pub(crate) fn commit_manifest_and_logs(
    db_dir: &Path,
    manifest: &GraphManifest,
    cdc_entries: &[CdcLogEntry],
    op_summary: &str,
) -> Result<()> {
    let wal_entry = WalEntry::from_manifest(manifest, op_summary, cdc_entries);
    append_wal_entry(db_dir, &wal_entry)?;
    manifest.write_atomic(db_dir)?;
    Ok(())
}

/// Prune WAL history to the last N visible db versions.
pub(crate) fn prune_logs_for_replay_window(
    db_dir: &Path,
    retain_tx_versions: u64,
) -> Result<LogPruneStats> {
    if retain_tx_versions == 0 {
        return Err(NanoError::Manifest(
            "retain_tx_versions must be >= 1".to_string(),
        ));
    }

    let manifest = GraphManifest::read(db_dir)?;
    reconcile_logs_to_manifest(db_dir, manifest.db_version)?;

    let wal_rows = read_wal_entries(db_dir)?;
    let tx_rows_before = wal_rows.len();
    let cdc_rows_before = wal_rows
        .iter()
        .map(|entry| entry.changes.len())
        .sum::<usize>();

    if wal_rows.is_empty() {
        rewrite_jsonl_rows(&wal_path(db_dir), &[] as &[WalEntry])?;
        return Ok(LogPruneStats {
            tx_rows_removed: 0,
            tx_rows_kept: 0,
            cdc_rows_removed: 0,
            cdc_rows_kept: 0,
        });
    }

    let min_db_version = manifest
        .db_version
        .saturating_sub(retain_tx_versions.saturating_sub(1));
    let kept_wal: Vec<WalEntry> = wal_rows
        .into_iter()
        .filter(|entry| {
            entry.db_version >= min_db_version && entry.db_version <= manifest.db_version
        })
        .collect();
    let tx_rows_kept = kept_wal.len();
    let cdc_rows_kept = kept_wal
        .iter()
        .map(|entry| entry.changes.len())
        .sum::<usize>();

    rewrite_jsonl_rows(&wal_path(db_dir), &kept_wal)?;

    Ok(LogPruneStats {
        tx_rows_removed: tx_rows_before.saturating_sub(tx_rows_kept),
        tx_rows_kept,
        cdc_rows_removed: cdc_rows_before.saturating_sub(cdc_rows_kept),
        cdc_rows_kept,
    })
}

fn flatten_cdc_rows(entries: Vec<WalEntry>) -> Vec<CdcLogEntry> {
    let mut rows: Vec<CdcLogEntry> = entries
        .into_iter()
        .flat_map(|entry| entry.changes.into_iter())
        .collect();
    rows.sort_by(|a, b| {
        a.db_version
            .cmp(&b.db_version)
            .then(a.seq_in_tx.cmp(&b.seq_in_tx))
            .then(a.tx_id.cmp(&b.tx_id))
    });
    rows
}

fn append_jsonl_row<T: Serialize>(path: &Path, row: &T) -> Result<(u64, u64)> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let start_offset = file.metadata()?.len();
    let json = serde_json::to_vec(row)
        .map_err(|e| NanoError::Manifest(format!("serialize JSONL row: {}", e)))?;
    file.write_all(&json)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    let end_offset = file.metadata()?.len();
    Ok((start_offset, end_offset))
}

fn rewrite_jsonl_rows<T: Serialize>(path: &Path, rows: &[T]) -> Result<()> {
    let tmp_path = path.with_extension("tmp");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;

        for row in rows {
            let json = serde_json::to_vec(row)
                .map_err(|e| NanoError::Manifest(format!("serialize JSONL row: {}", e)))?;
            file.write_all(&json)?;
            file.write_all(b"\n")?;
        }
        file.sync_all()?;
    }
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

fn read_jsonl_rows<T>(path: &Path) -> Result<Vec<T>>
where
    T: for<'de> Deserialize<'de>,
{
    if !path.exists() {
        return Ok(Vec::new());
    }

    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut out = Vec::new();

    for (line_no, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let parsed: T = serde_json::from_str(&line).map_err(|e| {
            NanoError::Manifest(format!(
                "parse JSONL row {} in {}: {}",
                line_no + 1,
                path.display(),
                e
            ))
        })?;
        out.push(parsed);
    }

    Ok(out)
}

fn truncate_trailing_partial_jsonl(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    let bytes = std::fs::read(path)?;
    if bytes.is_empty() || bytes.last() == Some(&b'\n') {
        return Ok(());
    }

    let keep_len = bytes
        .iter()
        .rposition(|b| *b == b'\n')
        .map(|idx| idx + 1)
        .unwrap_or(0);
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(keep_len as u64)?;
    file.sync_all()?;
    Ok(())
}

fn compute_wal_visible_prefix(path: &Path, manifest_db_version: u64) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }

    let bytes = std::fs::read(path)?;
    if bytes.is_empty() {
        return Ok(0);
    }

    let mut keep_len = 0u64;
    let mut prev_db_version = None;
    let mut offset = 0u64;

    for (line_no, chunk) in bytes.split_inclusive(|b| *b == b'\n').enumerate() {
        let next_offset = offset + chunk.len() as u64;
        let line = if chunk.last() == Some(&b'\n') {
            &chunk[..chunk.len().saturating_sub(1)]
        } else {
            chunk
        };

        let line_no = line_no + 1;
        if line.iter().all(|b| b.is_ascii_whitespace()) {
            keep_len = next_offset;
            offset = next_offset;
            continue;
        }

        let line_str = std::str::from_utf8(line).map_err(|e| {
            NanoError::Manifest(format!(
                "invalid UTF-8 in WAL line {} ({}): {}",
                line_no,
                path.display(),
                e
            ))
        })?;
        let entry: WalEntry = serde_json::from_str(line_str).map_err(|e| {
            NanoError::Manifest(format!(
                "parse WAL line {} ({}): {}",
                line_no,
                path.display(),
                e
            ))
        })?;

        if let Some(prev) = prev_db_version
            && entry.db_version <= prev
        {
            return Err(NanoError::Manifest(format!(
                "non-monotonic db_version in WAL at line {} (prev {}, got {})",
                line_no, prev, entry.db_version
            )));
        }
        prev_db_version = Some(entry.db_version);

        if entry.db_version > manifest_db_version {
            break;
        }

        keep_len = next_offset;
        offset = next_offset;
    }

    Ok(keep_len)
}

fn truncate_file_to_len(path: &Path, keep_len: u64) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let current_len = std::fs::metadata(path)?.len();
    if keep_len >= current_len {
        return Ok(());
    }

    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(keep_len)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_manifest(db_version: u64) -> GraphManifest {
        let mut manifest = GraphManifest::new("abc".to_string());
        manifest.db_version = db_version;
        manifest.last_tx_id = format!("manifest-{}", db_version);
        manifest.committed_at = format!("170000000{}", db_version);
        manifest
    }

    fn sample_cdc(tx_id: &str, db_version: u64, seq_in_tx: u32, key: &str) -> CdcLogEntry {
        CdcLogEntry {
            tx_id: tx_id.to_string(),
            db_version,
            seq_in_tx,
            op: "insert".to_string(),
            entity_kind: "node".to_string(),
            type_name: "Person".to_string(),
            entity_key: key.to_string(),
            payload: serde_json::json!({ "key": key }),
            committed_at: format!("170000000{}", db_version),
        }
    }

    fn sample_tx_entry() -> TxCatalogEntry {
        let mut dataset_versions = BTreeMap::new();
        dataset_versions.insert("nodes/99c1bf00".to_string(), 3);
        dataset_versions.insert("edges/f7012952".to_string(), 1);
        TxCatalogEntry {
            tx_id: "tx-1".to_string(),
            db_version: 1,
            dataset_versions,
            committed_at: "1700000000".to_string(),
            op_summary: "test".to_string(),
        }
    }

    #[test]
    fn wal_roundtrip_projects_tx_and_cdc_views() {
        let dir = TempDir::new().unwrap();
        let entry = WalEntry {
            tx_id: "tx-1".to_string(),
            db_version: 1,
            dataset_versions: BTreeMap::from([("nodes/x".to_string(), 3)]),
            committed_at: "1700000001".to_string(),
            op_summary: "load:merge".to_string(),
            changes: vec![sample_cdc("tx-1", 1, 0, "Alice")],
        };

        let offsets = append_wal_entry(dir.path(), &entry).unwrap();
        assert!(offsets.1 > offsets.0);

        let wal = read_wal_entries(dir.path()).unwrap();
        assert_eq!(wal, vec![entry.clone()]);

        let tx = read_tx_catalog_entries(dir.path()).unwrap();
        assert_eq!(
            tx,
            vec![TxCatalogEntry {
                tx_id: entry.tx_id.clone(),
                db_version: entry.db_version,
                dataset_versions: entry.dataset_versions.clone(),
                committed_at: entry.committed_at.clone(),
                op_summary: entry.op_summary.clone(),
            }]
        );

        let cdc = read_cdc_log_entries(dir.path()).unwrap();
        assert_eq!(cdc, entry.changes);
    }

    #[test]
    fn tx_catalog_append_is_projected_as_empty_change_wal_row() {
        let dir = TempDir::new().unwrap();
        let entry = sample_tx_entry();

        append_tx_catalog_entry(dir.path(), &entry).unwrap();

        let wal = read_wal_entries(dir.path()).unwrap();
        assert_eq!(wal.len(), 1);
        assert!(wal[0].changes.is_empty());

        let tx = read_tx_catalog_entries(dir.path()).unwrap();
        assert_eq!(tx, vec![entry]);
    }

    #[test]
    fn commit_manifest_and_logs_appends_single_wal_row() {
        let dir = TempDir::new().unwrap();
        let manifest = sample_manifest(1);
        let cdc = vec![sample_cdc("manifest-1", 1, 0, "Alice")];

        commit_manifest_and_logs(dir.path(), &manifest, &cdc, "test_commit").unwrap();

        let wal = read_wal_entries(dir.path()).unwrap();
        assert_eq!(wal.len(), 1);
        assert_eq!(wal[0].db_version, 1);
        assert_eq!(wal[0].op_summary, "test_commit");
        assert_eq!(wal[0].changes, cdc);
    }

    #[test]
    fn visible_cdc_respects_manifest_gate() {
        let dir = TempDir::new().unwrap();

        let manifest1 = sample_manifest(1);
        let cdc1 = vec![sample_cdc("manifest-1", 1, 0, "Alice")];
        commit_manifest_and_logs(dir.path(), &manifest1, &cdc1, "tx1").unwrap();

        let future = WalEntry {
            tx_id: "future".to_string(),
            db_version: 2,
            dataset_versions: BTreeMap::new(),
            committed_at: "1700000002".to_string(),
            op_summary: "future".to_string(),
            changes: vec![sample_cdc("future", 2, 0, "Bob")],
        };
        append_wal_entry(dir.path(), &future).unwrap();

        let visible = read_visible_cdc_entries(dir.path(), 0, None).unwrap();
        assert_eq!(visible, cdc1);

        let wal_after = read_wal_entries(dir.path()).unwrap();
        assert_eq!(wal_after.len(), 1);
        assert_eq!(wal_after[0].db_version, 1);
    }

    #[test]
    fn visible_cdc_honors_version_window() {
        let dir = TempDir::new().unwrap();

        commit_manifest_and_logs(
            dir.path(),
            &sample_manifest(1),
            &[sample_cdc("manifest-1", 1, 0, "Alice")],
            "tx1",
        )
        .unwrap();
        commit_manifest_and_logs(
            dir.path(),
            &sample_manifest(2),
            &[sample_cdc("manifest-2", 2, 0, "Bob")],
            "tx2",
        )
        .unwrap();
        commit_manifest_and_logs(
            dir.path(),
            &sample_manifest(3),
            &[sample_cdc("manifest-3", 3, 0, "Charlie")],
            "tx3",
        )
        .unwrap();

        let rows_since_1 = read_visible_cdc_entries(dir.path(), 1, None).unwrap();
        assert_eq!(
            rows_since_1
                .iter()
                .map(|row| row.entity_key.as_str())
                .collect::<Vec<_>>(),
            vec!["Bob", "Charlie"]
        );

        let rows_range_2_only = read_visible_cdc_entries(dir.path(), 1, Some(2)).unwrap();
        assert_eq!(rows_range_2_only.len(), 1);
        assert_eq!(rows_range_2_only[0].entity_key, "Bob");
    }

    #[test]
    fn reconcile_truncates_partial_last_line() {
        let dir = TempDir::new().unwrap();
        let manifest = sample_manifest(1);
        commit_manifest_and_logs(
            dir.path(),
            &manifest,
            &[sample_cdc("manifest-1", 1, 0, "Alice")],
            "tx1",
        )
        .unwrap();

        let path = wal_path(dir.path());
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(br#"{"tx_id":"partial""#).unwrap();
        file.sync_all().unwrap();

        reconcile_logs_to_manifest(dir.path(), 1).unwrap();
        let wal = read_wal_entries(dir.path()).unwrap();
        assert_eq!(wal.len(), 1);
    }

    #[test]
    fn prune_logs_for_replay_window_rewrites_single_wal() {
        let dir = TempDir::new().unwrap();
        commit_manifest_and_logs(
            dir.path(),
            &sample_manifest(1),
            &[sample_cdc("manifest-1", 1, 0, "Alice")],
            "tx1",
        )
        .unwrap();
        commit_manifest_and_logs(
            dir.path(),
            &sample_manifest(2),
            &[sample_cdc("manifest-2", 2, 0, "Bob")],
            "tx2",
        )
        .unwrap();
        commit_manifest_and_logs(
            dir.path(),
            &sample_manifest(3),
            &[sample_cdc("manifest-3", 3, 0, "Charlie")],
            "tx3",
        )
        .unwrap();

        let stats = prune_logs_for_replay_window(dir.path(), 2).unwrap();
        assert_eq!(stats.tx_rows_removed, 1);
        assert_eq!(stats.tx_rows_kept, 2);
        assert_eq!(stats.cdc_rows_removed, 1);
        assert_eq!(stats.cdc_rows_kept, 2);

        let tx = read_tx_catalog_entries(dir.path()).unwrap();
        assert_eq!(tx.len(), 2);
        assert_eq!(tx[0].db_version, 2);
        assert_eq!(tx[1].db_version, 3);

        let cdc = read_visible_cdc_entries(dir.path(), 0, None).unwrap();
        assert_eq!(
            cdc.iter()
                .map(|row| row.entity_key.as_str())
                .collect::<Vec<_>>(),
            vec!["Bob", "Charlie"]
        );
    }
}
