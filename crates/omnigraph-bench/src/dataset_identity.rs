//! Rebuild-stable current-content and lineage-shape witnesses. Historical row
//! images and unkeyed edge identities are outside this equivalence domain.
use crate::gqt_case::sha256_bytes;
use crate::model::typed_sha256;
use omnigraph::db::Omnigraph;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::Path;
pub const DATASET_LOGICAL_ALGORITHM: &str = "omnigraph-gqt-branches-lineage-equivalence-v1";
pub const REGISTERED_LOGICAL_ALGORITHM: &str =
    "omnigraph-gqt-branches-lineage-and-registered-identities-v1";
const MAX_COMMITS: usize = 1_000_000;
const MAX_BRANCHES: usize = 1024;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchLogicalV1 {
    pub name: String,
    pub content_sha256: String,
    pub schema_sha256: String,
    pub lineage_sha256: String,
    pub history_commits: u64,
    pub node_tables: Vec<crate::fixture_reference::GraphTableCountV1>,
    pub edge_tables: Vec<crate::fixture_reference::GraphTableCountV1>,
    pub indexes: Vec<crate::fixture_reference::RealGraphIndexV1>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetLogicalV1 {
    pub algorithm: String,
    pub logical_content_sha256: String,
    pub branches: Vec<BranchLogicalV1>,
    pub relocation_self_contained: bool,
    pub limitations: Vec<String>,
}
pub async fn observe(root: &Path) -> Result<DatasetLogicalV1, String> {
    let db = Omnigraph::open_read_only(root.to_str().ok_or("non-UTF8 dataset path")?)
        .await
        .map_err(|e| e.to_string())?;
    let mut names = db.branch_list().await.map_err(|e| e.to_string())?;
    names.sort();
    names.dedup();
    if names.is_empty() || names.len() > MAX_BRANCHES {
        return Err("dataset branch inventory exceeds its bound".into());
    }
    let mut branches = Vec::new();
    let mut self_contained = true;
    for name in &names {
        let observed = crate::real_graph::observe_branch(&db, name, names.clone())
            .await
            .map_err(|e| e.to_string())?;
        let (lineage_sha256, history_commits) = lineage(&db, name).await?;
        self_contained &= observed.relocation_self_contained;
        branches.push(BranchLogicalV1 {
            name: name.clone(),
            content_sha256: observed.logical_content.sha256,
            schema_sha256: observed.schema_shape.sha256,
            lineage_sha256,
            history_commits,
            node_tables: observed.node_tables,
            edge_tables: observed.edge_tables,
            indexes: observed.indexes,
        });
    }
    if db
        .branch_list()
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .collect::<BTreeSet<_>>()
        != names.into_iter().collect()
    {
        return Err("dataset branches changed during observation".into());
    }
    let logical_content_sha256 =
        typed_sha256(&(DATASET_LOGICAL_ALGORITHM, &branches)).map_err(|e| e.to_string())?;
    Ok(DatasetLogicalV1 {
        algorithm: DATASET_LOGICAL_ALGORITHM.into(),
        logical_content_sha256,
        branches,
        relocation_self_contained: self_contained,
        limitations: vec![
            "unkeyed-edge-ids-excluded".into(),
            "historical-row-images-not-attested".into(),
        ],
    })
}
async fn lineage(db: &Omnigraph, branch: &str) -> Result<(String, u64), String> {
    let head = db
        .resolve_snapshot(branch)
        .await
        .map_err(|e| e.to_string())?
        .as_str()
        .to_owned();
    let mut pending = vec![head.clone()];
    let mut commits = BTreeMap::new();
    while let Some(id) = pending.pop() {
        if commits.contains_key(&id) {
            continue;
        }
        if commits.len() >= MAX_COMMITS {
            return Err("dataset history exceeds the lineage visit budget".into());
        }
        let c = db
            .get_commit(&id)
            .await
            .map_err(|e| format!("required dataset history {id} unavailable: {e}"))?;
        if c.graph_commit_id != id {
            return Err("history lookup returned another commit identity".into());
        }
        for parent in [&c.parent_commit_id, &c.merged_parent_commit_id]
            .into_iter()
            .flatten()
        {
            pending.push(parent.clone())
        }
        commits.insert(id, c);
    }
    normalized_lineage(&head, &commits)
}
fn normalized_lineage(
    head: &str,
    commits: &BTreeMap<String, omnigraph::db::GraphCommit>,
) -> Result<(String, u64), String> {
    if commits.is_empty() || commits.len() > MAX_COMMITS || !commits.contains_key(head) {
        return Err("lineage head or inventory is unavailable".into());
    }
    let mut ordered = commits.values().collect::<Vec<_>>();
    ordered.sort_by_key(|c| c.generation);
    let mut hashes = BTreeMap::new();
    let mut multiset = Vec::new();
    for c in ordered {
        let parent = |id: &Option<String>| -> Result<Option<String>, String> {
            id.as_ref()
                .map(|id| {
                    let parent = commits.get(id).ok_or("missing lineage parent")?;
                    if parent.generation >= c.generation {
                        return Err("lineage parent generation must be strictly smaller".into());
                    }
                    hashes
                        .get(id)
                        .cloned()
                        .ok_or_else(|| "cyclic or non-increasing commit generation".into())
                })
                .transpose()
        };
        let hash = typed_sha256(&(
            "omnigraph-lineage-shape-v1",
            &c.graph_branch,
            c.generation,
            &c.actor_id,
            parent(&c.parent_commit_id)?,
            parent(&c.merged_parent_commit_id)?,
        ))
        .map_err(|e| e.to_string())?;
        hashes.insert(c.graph_commit_id.clone(), hash.clone());
        multiset.push(hash);
    }
    multiset.sort();
    let hash = typed_sha256(&(
        "omnigraph-lineage-dag-shape-v1",
        hashes.get(head),
        &multiset,
    ))
    .map_err(|e| e.to_string())?;
    Ok((hash, commits.len() as u64))
}
/// The registered source is fixed: retain IDs as logical identity, before any
/// GQT preparation introduces automatically generated edge IDs.
pub async fn registered_identity(root: &Path) -> Result<String, String> {
    let db = Omnigraph::open_read_only(root.to_str().ok_or("non-UTF8 source path")?)
        .await
        .map_err(|e| e.to_string())?;
    let mut branches = db.branch_list().await.map_err(|e| e.to_string())?;
    branches.sort();
    let mut evidence = Vec::new();
    for branch in branches {
        let mut sink = IdentitySink::default();
        db.export_jsonl_unordered_to_writer(&branch, &[], &mut sink)
            .await
            .map_err(|e| e.to_string())?;
        if !sink.pending.is_empty() {
            return Err("unterminated registered export".into());
        }
        evidence.push((branch, sink.rows, sink.sum));
    }
    typed_sha256(&("omnigraph-export-identities-multiset-v1", evidence)).map_err(|e| e.to_string())
}
#[derive(Default)]
struct IdentitySink {
    pending: Vec<u8>,
    sum: [u8; 32],
    rows: u64,
}
impl Write for IdentitySink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.pending.extend_from_slice(bytes);
        while let Some(n) = self.pending.iter().position(|b| *b == b'\n') {
            if n > 16 * 1024 * 1024 {
                return Err(std::io::Error::other("registered export row exceeds bound"));
            }
            let line = self.pending.drain(..=n).collect::<Vec<_>>();
            if n == 0 {
                continue;
            }
            let value: serde_json::Value =
                serde_json::from_slice(&line[..n]).map_err(std::io::Error::other)?;
            let canonical =
                crate::real_graph::canonical_json_bytes(&value).map_err(std::io::Error::other)?;
            let hash = sha256_bytes(&canonical);
            let mut carry = 0u16;
            for (i, pair) in hash.as_bytes().chunks(2).enumerate().rev() {
                let part = std::str::from_utf8(pair).map_err(std::io::Error::other)?;
                let byte = u8::from_str_radix(part, 16).map_err(std::io::Error::other)?;
                let sum = u16::from(self.sum[i]) + u16::from(byte) + carry;
                self.sum[i] = sum as u8;
                carry = sum >> 8;
            }
            self.rows = self
                .rows
                .checked_add(1)
                .ok_or_else(|| std::io::Error::other("registered row count overflow"))?;
        }
        if self.pending.len() > 16 * 1024 * 1024 {
            return Err(std::io::Error::other("registered export row exceeds bound"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn validate(logical: &DatasetLogicalV1, registered: Option<&str>) -> Result<(), String> {
    if logical.branches.is_empty()
        || logical.branches.len() > MAX_BRANCHES
        || logical.branches.windows(2).any(|b| b[0].name >= b[1].name)
        || !logical.branches.iter().any(|b| b.name == "main")
    {
        return Err("invalid canonical dataset branch inventory".into());
    }
    for b in &logical.branches {
        if b.name.is_empty()
            || b.name.len() > 1024
            || b.history_commits == 0
            || b.history_commits > MAX_COMMITS as u64
        {
            return Err("invalid branch history witness".into());
        }
        for digest in [&b.content_sha256, &b.schema_sha256, &b.lineage_sha256] {
            if !crate::gqt_case::digest(digest) {
                return Err("invalid branch digest".into());
            }
        }
        for tables in [&b.node_tables, &b.edge_tables] {
            if tables.len() > 4096
                || tables.windows(2).any(|t| t[0].name >= t[1].name)
                || tables
                    .iter()
                    .any(|t| t.name.is_empty() || t.name.len() > 1024)
            {
                return Err("invalid canonical dataset table inventory".into());
            }
        }
        if b.indexes.len() > 16384 || b.indexes.windows(2).any(|i| i[0] >= i[1]) {
            return Err("invalid canonical dataset index inventory".into());
        }
    }
    let base =
        typed_sha256(&(DATASET_LOGICAL_ALGORITHM, &logical.branches)).map_err(|e| e.to_string())?;
    let expected = match (logical.algorithm.as_str(), registered) {
        (DATASET_LOGICAL_ALGORITHM, None) => base,
        (REGISTERED_LOGICAL_ALGORITHM, Some(source)) if crate::gqt_case::digest(source) => {
            typed_sha256(&(DATASET_LOGICAL_ALGORITHM, &base, source)).map_err(|e| e.to_string())?
        }
        _ => return Err("unknown dataset logical domain or missing registered witness".into()),
    };
    if expected != logical.logical_content_sha256
        || logical.limitations
            != [
                "unkeyed-edge-ids-excluded",
                "historical-row-images-not-attested",
            ]
    {
        return Err("dataset logical witness digest or limitations mismatch".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn registered_identity_preserves_float_to_edge_id_associations() {
        fn digest(rows: &str) -> ([u8; 32], u64) {
            let mut sink = IdentitySink::default();
            sink.write_all(rows.as_bytes()).unwrap();
            assert!(sink.pending.is_empty());
            (sink.sum, sink.rows)
        }
        let first = concat!(
            "{\"edge\":\"Transfer\",\"id\":\"e1\",\"from\":\"a\",\"to\":\"b\",\"data\":{\"amount\":0.1000000000001}}\n",
            "{\"edge\":\"Transfer\",\"id\":\"e2\",\"from\":\"a\",\"to\":\"b\",\"data\":{\"amount\":0.1000000000002}}\n"
        );
        let swapped = first
            .replace("e1", "temp")
            .replace("e2", "e1")
            .replace("temp", "e2");
        assert_ne!(digest(first), digest(&swapped));
        let reordered = first
            .lines()
            .rev()
            .map(|line| {
                let value: serde_json::Value = serde_json::from_str(line).unwrap();
                format!("{}\n", serde_json::to_string(&value).unwrap())
            })
            .collect::<String>();
        assert_eq!(digest(first), digest(&reordered));
        assert_eq!(digest(first).1, 2);
    }
    fn commit(id: &str, generation: u64, parent: Option<&str>) -> omnigraph::db::GraphCommit {
        omnigraph::db::GraphCommit {
            graph_commit_id: id.into(),
            graph_branch: Some("main".into()),
            graph_manifest_version: generation,
            generation,
            parent_commit_id: parent.map(str::to_owned),
            merged_parent_commit_id: None,
            actor_id: None,
            created_at: 0,
        }
    }
    #[test]
    fn normalized_history_refuses_missing_or_equal_generation_parents_independent_of_ids() {
        for (parent, child) in [("a", "z"), ("z", "a")] {
            let malformed = BTreeMap::from([
                (parent.into(), commit(parent, 1, None)),
                (child.into(), commit(child, 1, Some(parent))),
            ]);
            assert!(normalized_lineage(child, &malformed).is_err());
            let missing = BTreeMap::from([(child.into(), commit(child, 2, Some(parent)))]);
            assert!(normalized_lineage(child, &missing).is_err());
        }
        let first = BTreeMap::from([
            ("a".into(), commit("a", 0, None)),
            ("b".into(), commit("b", 1, Some("a"))),
        ]);
        let mut renamed = BTreeMap::from([
            ("x".into(), commit("x", 0, None)),
            ("y".into(), commit("y", 1, Some("x"))),
        ]);
        renamed.get_mut("y").unwrap().created_at = 500;
        renamed.get_mut("y").unwrap().graph_manifest_version = 200;
        assert_eq!(
            normalized_lineage("b", &first).unwrap(),
            normalized_lineage("y", &renamed).unwrap()
        );
    }
}
