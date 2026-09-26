//! Helpers for the contracts that outlived the recovery sidecars (RFC 0067):
//! tests assert that `__recovery/` stays empty and read a branch's head commit.

use std::path::Path;

use omnigraph::db::commit_graph::CommitGraph;
use omnigraph::error::{OmniError, Result};

/// Operation ids of the sidecars under `__recovery/`, sorted. No writer arms
/// one, so every caller asserts this is empty.
pub fn sidecar_operation_ids(graph_root: &Path) -> Vec<String> {
    let dir = graph_root.join("__recovery");
    if !dir.exists() {
        return Vec::new();
    }
    let mut ids = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                return None;
            }
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .map(str::to_string)
        })
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

pub async fn branch_head_commit_id(graph_root: &Path, branch: &str) -> Result<String> {
    let uri = graph_root.to_str().unwrap();
    let graph = match branch {
        "main" => CommitGraph::open(uri).await?,
        branch => CommitGraph::open_at_branch(uri, branch).await?,
    };
    graph.head_commit_id().await?.ok_or_else(|| {
        OmniError::manifest_internal(format!("commit graph for branch {branch} has no head"))
    })
}
