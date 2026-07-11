use std::path::{Path, PathBuf};

use arrow_array::{Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use lance::Dataset;
use omnigraph::db::commit_graph::CommitGraph;
use omnigraph::db::{GraphCommit, Omnigraph, ReadTarget, SubTableEntry};
use omnigraph::error::{OmniError, Result};
use omnigraph_compiler::ir::ParamMap;
use serde::Deserialize;

const RECOVERY_ACTOR: &str = "omnigraph:recovery";

#[derive(Debug)]
pub enum RecoveryExpectation {
    RolledForward {
        tables: Vec<TableExpectation>,
    },
    /// RFC-022 v3 mutation/load and v4 branch-merge recovery republish the
    /// writer's fixed lineage intent rather than minting a synthetic recovery
    /// commit.
    RolledForwardOriginalLineage {
        tables: Vec<TableExpectation>,
    },
    RolledBack {
        tables: Vec<TableExpectation>,
    },
    Deferred,
    NoOp,
}

#[derive(Debug)]
pub struct TableExpectation {
    pub table_key: String,
    pub branch: Option<String>,
    pub expected_lance_head: Option<u64>,
    pub expected_main_manifest_pin: Option<u64>,
    pub expected_recovery_parent_commit_id: Option<String>,
    pub follow_up_mutation: Option<FollowUpMutation>,
}

#[derive(Debug)]
pub struct FollowUpMutation {
    pub branch: String,
    pub query_source: String,
    pub query_name: String,
    pub params: ParamMap,
}

#[derive(Debug, Clone)]
struct RecoveryAuditRow {
    graph_commit_id: String,
    recovery_kind: String,
    recovery_for_actor: Option<String>,
    operation_id: String,
    sidecar_writer_kind: String,
    per_table_outcomes: Vec<TableOutcome>,
}

#[derive(Debug, Clone, Deserialize)]
struct TableOutcome {
    table_key: String,
    from_version: u64,
    to_version: u64,
}

impl TableExpectation {
    pub fn main(table_key: impl Into<String>) -> Self {
        Self::new(table_key, None::<String>)
    }

    pub fn branch(table_key: impl Into<String>, branch: impl Into<String>) -> Self {
        Self::new(table_key, Some(branch))
    }

    pub fn new(table_key: impl Into<String>, branch: Option<impl Into<String>>) -> Self {
        Self {
            table_key: table_key.into(),
            branch: branch.map(Into::into),
            expected_lance_head: None,
            expected_main_manifest_pin: None,
            expected_recovery_parent_commit_id: None,
            follow_up_mutation: None,
        }
    }

    pub fn expected_lance_head(mut self, version: u64) -> Self {
        self.expected_lance_head = Some(version);
        self
    }

    pub fn expected_main_manifest_pin(mut self, version: u64) -> Self {
        self.expected_main_manifest_pin = Some(version);
        self
    }

    pub fn expected_recovery_parent_commit_id(mut self, commit_id: impl Into<String>) -> Self {
        self.expected_recovery_parent_commit_id = Some(commit_id.into());
        self
    }

    pub fn follow_up_mutation(mut self, mutation: FollowUpMutation) -> Self {
        self.follow_up_mutation = Some(mutation);
        self
    }
}

impl FollowUpMutation {
    pub fn new(
        branch: impl Into<String>,
        query_source: impl Into<String>,
        query_name: impl Into<String>,
        params: ParamMap,
    ) -> Self {
        Self {
            branch: branch.into(),
            query_source: query_source.into(),
            query_name: query_name.into(),
            params,
        }
    }
}

pub fn single_sidecar_operation_id(graph_root: &Path) -> String {
    let ids = sidecar_operation_ids(graph_root);
    assert_eq!(
        ids.len(),
        1,
        "expected exactly one recovery sidecar under __recovery/, got {:?}",
        ids,
    );
    ids.into_iter().next().unwrap()
}

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

/// Recovery-audit rows' `recovery_kind` values at `graph_root`, in
/// storage order. Empty when the audit dataset doesn't exist yet.
pub async fn recovery_audit_kinds(graph_root: &Path) -> Vec<String> {
    let recoveries_dir = graph_root.join("_graph_commit_recoveries.lance");
    if !recoveries_dir.exists() {
        return Vec::new();
    }
    let ds = Dataset::open(recoveries_dir.to_str().unwrap())
        .await
        .expect("recoveries dataset opens");
    let batches: Vec<RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut out = Vec::new();
    for batch in batches {
        let kinds = batch
            .column_by_name("recovery_kind")
            .expect("recovery_kind column present")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("recovery_kind is Utf8");
        for i in 0..kinds.len() {
            out.push(kinds.value(i).to_string());
        }
    }
    out
}

pub async fn branch_head_commit_id(graph_root: &Path, branch: &str) -> Result<String> {
    let graph = match branch {
        "main" => CommitGraph::open(&graph_uri(graph_root)).await?,
        branch => CommitGraph::open_at_branch(&graph_uri(graph_root), branch).await?,
    };
    graph.head_commit_id().await?.ok_or_else(|| {
        OmniError::manifest_internal(format!("commit graph for branch {branch} has no head"))
    })
}

pub async fn assert_post_recovery_invariants(
    graph_root: &Path,
    operation_id: &str,
    expectation: RecoveryExpectation,
) -> Result<()> {
    match expectation {
        RecoveryExpectation::RolledForward { tables } => {
            assert_sidecar_absent(graph_root, operation_id);
            let audit = read_audit_row(graph_root, operation_id).await?;
            assert_eq!(
                audit.recovery_kind, "RolledForward",
                "audit row for {operation_id} recorded the wrong recovery_kind",
            );
            assert_manifest_pins_match_lance_heads(graph_root, &tables).await?;
            assert_audit_to_versions_match_lance_heads(graph_root, &audit, &tables).await?;
            assert_recovery_commit_shape(graph_root, &audit, &tables, false).await?;
            assert_non_main_did_not_move_main(graph_root, &tables).await?;
            assert_idempotent_reopen(graph_root, operation_id).await?;
            run_follow_up_mutations(graph_root, tables).await?;
        }
        RecoveryExpectation::RolledForwardOriginalLineage { tables } => {
            assert_sidecar_absent(graph_root, operation_id);
            let audit = read_audit_row(graph_root, operation_id).await?;
            assert_eq!(
                audit.recovery_kind, "RolledForward",
                "audit row for {operation_id} recorded the wrong recovery_kind",
            );
            assert_manifest_pins_match_lance_heads(graph_root, &tables).await?;
            assert_audit_to_versions_match_lance_heads(graph_root, &audit, &tables).await?;
            assert_recovery_commit_shape(graph_root, &audit, &tables, true).await?;
            assert_non_main_did_not_move_main(graph_root, &tables).await?;
            assert_idempotent_reopen(graph_root, operation_id).await?;
            run_follow_up_mutations(graph_root, tables).await?;
        }
        RecoveryExpectation::RolledBack { tables } => {
            assert_sidecar_absent(graph_root, operation_id);
            let audit = read_audit_row(graph_root, operation_id).await?;
            assert_eq!(
                audit.recovery_kind, "RolledBack",
                "audit row for {operation_id} recorded the wrong recovery_kind",
            );
            assert_rollback_outcomes_record_drift(&audit);
            // Roll-back now publishes the restored HEAD, so manifest == Lance
            // HEAD afterward (symmetric with roll-forward) — no residual drift.
            assert_manifest_pins_match_lance_heads(graph_root, &tables).await?;
            assert_recovery_commit_shape(graph_root, &audit, &tables, false).await?;
            assert_non_main_did_not_move_main(graph_root, &tables).await?;
            assert_idempotent_reopen(graph_root, operation_id).await?;
            run_follow_up_mutations(graph_root, tables).await?;
        }
        RecoveryExpectation::Deferred => {
            assert!(
                sidecar_path(graph_root, operation_id).exists(),
                "deferred recovery must leave sidecar {operation_id} on disk",
            );
            assert!(
                read_audit_row(graph_root, operation_id).await.is_err(),
                "deferred recovery must not record an audit row for {operation_id}",
            );
        }
        RecoveryExpectation::NoOp => {
            assert_sidecar_absent(graph_root, operation_id);
            assert!(
                read_audit_row(graph_root, operation_id).await.is_err(),
                "no-op recovery must not record an audit row for {operation_id}",
            );
        }
    }

    Ok(())
}

fn branch_context(tables: &[TableExpectation]) -> Option<String> {
    tables
        .iter()
        .filter_map(|table| table.branch.as_deref())
        .find(|branch| *branch != "main")
        .map(str::to_string)
}

fn sidecar_path(graph_root: &Path, operation_id: &str) -> PathBuf {
    graph_root
        .join("__recovery")
        .join(format!("{operation_id}.json"))
}

fn assert_sidecar_absent(graph_root: &Path, operation_id: &str) {
    assert!(
        !sidecar_path(graph_root, operation_id).exists(),
        "recovery sidecar {operation_id} must be deleted after successful recovery",
    );
}

async fn assert_manifest_pins_match_lance_heads(
    graph_root: &Path,
    tables: &[TableExpectation],
) -> Result<()> {
    let uri = graph_uri(graph_root);
    let db = Omnigraph::open(&uri).await?;
    for table in tables {
        let (entry, lance_head) = entry_and_lance_head(&db, &uri, table).await?;
        assert_eq!(
            entry.table_version, lance_head,
            "manifest pin for {} on {:?} must match Lance HEAD after roll-forward",
            table.table_key, table.branch,
        );
        if let Some(expected) = table.expected_lance_head {
            assert_eq!(
                lance_head, expected,
                "Lance HEAD for {} on {:?} did not match the test's expected value",
                table.table_key, table.branch,
            );
        }
    }
    Ok(())
}

async fn assert_audit_to_versions_match_lance_heads(
    graph_root: &Path,
    audit: &RecoveryAuditRow,
    tables: &[TableExpectation],
) -> Result<()> {
    let uri = graph_uri(graph_root);
    let db = Omnigraph::open(&uri).await?;
    for table in tables {
        let (_, lance_head) = entry_and_lance_head(&db, &uri, table).await?;
        let outcome = audit
            .per_table_outcomes
            .iter()
            .find(|outcome| outcome.table_key == table.table_key)
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "audit row for {} has no outcome for {}",
                    audit.operation_id, table.table_key,
                ))
            })?;
        assert_eq!(
            outcome.to_version, lance_head,
            "audit to_version for {} must match the published Lance HEAD",
            table.table_key,
        );
    }
    Ok(())
}

/// For RolledBack outcomes, `from_version` records the Lance HEAD
/// observed BEFORE the restore (the actual drift) and `to_version`
/// records the manifest pin we restored to. If both equal, the audit
/// row is uninformative — operators cannot tell how far Lance HEAD
/// drifted from the manifest. This assertion catches any regression
/// that reverts `from_version` to `manifest_pinned`.
fn assert_rollback_outcomes_record_drift(audit: &RecoveryAuditRow) {
    for outcome in &audit.per_table_outcomes {
        assert!(
            outcome.from_version > outcome.to_version,
            "rollback outcome for {} must record drift via `from_version > to_version` \
             (Lance HEAD before restore > manifest pin restored to); got from={}, to={}",
            outcome.table_key,
            outcome.from_version,
            outcome.to_version,
        );
    }
}

async fn assert_non_main_did_not_move_main(
    graph_root: &Path,
    tables: &[TableExpectation],
) -> Result<()> {
    let uri = graph_uri(graph_root);
    let db = Omnigraph::open(&uri).await?;
    let main = db.snapshot_of(ReadTarget::branch("main")).await?;
    for table in tables {
        let Some(expected) = table.expected_main_manifest_pin else {
            continue;
        };
        let entry = main.entry(&table.table_key).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "main snapshot has no entry for {}",
                table.table_key,
            ))
        })?;
        assert_eq!(
            entry.table_version, expected,
            "non-main recovery for {} on {:?} must not move main's manifest pin",
            table.table_key, table.branch,
        );
    }
    Ok(())
}

async fn assert_recovery_commit_shape(
    graph_root: &Path,
    audit: &RecoveryAuditRow,
    tables: &[TableExpectation],
    preserves_original_intent: bool,
) -> Result<()> {
    let branch = branch_context(tables);
    let expected_parent = expected_recovery_parent(tables)?;
    let branch = branch.as_deref();
    let commit = read_recovery_commit(graph_root, audit, branch).await?;

    // RFC-022 mutation/load roll-forward publishes the writer's original,
    // pre-minted lineage intent. That preserves its original actor (including
    // `None`) and makes a crash after publish distinguishable from a new
    // synthetic recovery content commit. Rollback and legacy writer recovery
    // still publish an explicit `omnigraph:recovery` commit.
    let expected_actor = if preserves_original_intent {
        audit.recovery_for_actor.as_deref()
    } else {
        Some(RECOVERY_ACTOR)
    };
    assert_eq!(
        commit.actor_id.as_deref(),
        expected_actor,
        "recovery commit {} for operation {} recorded the wrong actor",
        commit.graph_commit_id,
        audit.operation_id,
    );

    if let Some(expected_parent) = expected_parent {
        assert_eq!(
            commit.parent_commit_id.as_deref(),
            Some(expected_parent.as_str()),
            "recovery commit {} for operation {} recorded the wrong parent",
            commit.graph_commit_id,
            audit.operation_id,
        );
    }

    if audit.sidecar_writer_kind == "BranchMerge" {
        assert!(
            commit.merged_parent_commit_id.is_some(),
            "recovered BranchMerge must record merged_parent_commit_id",
        );

        if let Some(branch) = branch {
            let graph = CommitGraph::open_at_branch(&graph_uri(graph_root), branch).await?;
            let commits = graph.load_commits().await?;
            let parent = commit.parent_commit_id.as_deref().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "recovered BranchMerge commit {} has no parent_commit_id",
                    commit.graph_commit_id,
                ))
            })?;
            assert!(
                commits
                    .iter()
                    .any(|candidate| candidate.graph_commit_id == parent),
                "recovered BranchMerge parent_commit_id {} is not on target branch {}",
                parent,
                branch,
            );
        }
    }

    Ok(())
}

fn expected_recovery_parent(tables: &[TableExpectation]) -> Result<Option<String>> {
    let mut expected = None;
    for table in tables {
        let Some(candidate) = &table.expected_recovery_parent_commit_id else {
            continue;
        };
        match &expected {
            None => expected = Some(candidate.clone()),
            Some(existing) if existing == candidate => {}
            Some(existing) => {
                return Err(OmniError::manifest_internal(format!(
                    "conflicting expected recovery parents in table expectations: {existing} vs {candidate}",
                )));
            }
        }
    }
    Ok(expected)
}

async fn assert_idempotent_reopen(graph_root: &Path, operation_id: &str) -> Result<()> {
    let before = matching_audit_rows(graph_root, operation_id).await?;
    let uri = graph_uri(graph_root);
    let _db = Omnigraph::open(&uri).await?;
    assert_sidecar_absent(graph_root, operation_id);
    let after = matching_audit_rows(graph_root, operation_id).await?;
    assert_eq!(
        after.len(),
        before.len(),
        "immediate reopen after recovery must be a clean no-op for {operation_id}",
    );
    Ok(())
}

async fn run_follow_up_mutations(graph_root: &Path, tables: Vec<TableExpectation>) -> Result<()> {
    let mut db: Option<Omnigraph> = None;
    for table in tables {
        let Some(mutation) = table.follow_up_mutation else {
            continue;
        };
        if db.is_none() {
            db = Some(Omnigraph::open(&graph_uri(graph_root)).await?);
        }
        let db = db.as_mut().unwrap();
        db.mutate(
            &mutation.branch,
            &mutation.query_source,
            &mutation.query_name,
            &mutation.params,
        )
        .await
        .map_err(|err| {
            OmniError::manifest_internal(format!(
                "follow-up mutation {} on {} after recovery failed: {}",
                mutation.query_name, table.table_key, err,
            ))
        })?;
    }
    Ok(())
}

async fn entry_and_lance_head(
    db: &Omnigraph,
    root_uri: &str,
    table: &TableExpectation,
) -> Result<(SubTableEntry, u64)> {
    let branch = table.branch.as_deref().unwrap_or("main");
    let snapshot = db.snapshot_of(ReadTarget::branch(branch)).await?;
    let entry = snapshot
        .entry(&table.table_key)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "snapshot for branch {branch} has no entry for {}",
                table.table_key,
            ))
        })?
        .clone();
    let lance_head = lance_head_for_entry(root_uri, &entry).await?;
    Ok((entry, lance_head))
}

async fn lance_head_for_entry(root_uri: &str, entry: &SubTableEntry) -> Result<u64> {
    let table_uri = format!("{}/{}", root_uri.trim_end_matches('/'), entry.table_path);
    let ds = Dataset::open(&table_uri)
        .await
        .map_err(|err| OmniError::Lance(err.to_string()))?;
    let ds = match entry.table_branch.as_deref() {
        Some(branch) if branch != "main" => ds
            .checkout_branch(branch)
            .await
            .map_err(|err| OmniError::Lance(err.to_string()))?,
        _ => ds,
    };
    Ok(ds.version().version)
}

async fn read_recovery_commit(
    graph_root: &Path,
    audit: &RecoveryAuditRow,
    branch: Option<&str>,
) -> Result<GraphCommit> {
    let uri = graph_uri(graph_root);
    let graph = match branch {
        Some(branch) => CommitGraph::open_at_branch(&uri, branch).await?,
        None => CommitGraph::open(&uri).await?,
    };
    graph
        .load_commits()
        .await?
        .into_iter()
        .find(|commit| commit.graph_commit_id == audit.graph_commit_id)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "recovery commit {} for operation {} was not found",
                audit.graph_commit_id, audit.operation_id,
            ))
        })
}

async fn read_audit_row(graph_root: &Path, operation_id: &str) -> Result<RecoveryAuditRow> {
    let mut rows = matching_audit_rows(graph_root, operation_id).await?;
    if rows.len() != 1 {
        return Err(OmniError::manifest_internal(format!(
            "expected exactly one recovery audit row for {operation_id}, got {}",
            rows.len(),
        )));
    }
    Ok(rows.remove(0))
}

async fn matching_audit_rows(
    graph_root: &Path,
    operation_id: &str,
) -> Result<Vec<RecoveryAuditRow>> {
    let recoveries_dir = graph_root.join("_graph_commit_recoveries.lance");
    if !recoveries_dir.exists() {
        return Ok(Vec::new());
    }
    let ds = Dataset::open(recoveries_dir.to_str().unwrap())
        .await
        .map_err(|err| OmniError::Lance(err.to_string()))?;
    let batches: Vec<RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .map_err(|err| OmniError::Lance(err.to_string()))?
        .try_collect()
        .await
        .map_err(|err| OmniError::Lance(err.to_string()))?;

    let mut rows = Vec::new();
    for batch in batches {
        let graph_commit_ids = string_column(&batch, "graph_commit_id")?;
        let kinds = string_column(&batch, "recovery_kind")?;
        let recovery_actors = string_column(&batch, "recovery_for_actor")?;
        let ops = string_column(&batch, "operation_id")?;
        let writers = string_column(&batch, "sidecar_writer_kind")?;
        let outcomes_json = string_column(&batch, "per_table_outcomes_json")?;
        for row in 0..batch.num_rows() {
            if ops.value(row) != operation_id {
                continue;
            }
            let per_table_outcomes =
                serde_json::from_str(outcomes_json.value(row)).map_err(|err| {
                    OmniError::manifest_internal(format!(
                        "failed to parse recovery audit outcomes for {operation_id}: {err}",
                    ))
                })?;
            rows.push(RecoveryAuditRow {
                graph_commit_id: graph_commit_ids.value(row).to_string(),
                recovery_kind: kinds.value(row).to_string(),
                recovery_for_actor: (!recovery_actors.is_null(row))
                    .then(|| recovery_actors.value(row).to_string()),
                operation_id: ops.value(row).to_string(),
                sidecar_writer_kind: writers.value(row).to_string(),
                per_table_outcomes,
            });
        }
    }
    Ok(rows)
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("recovery audit batch missing '{name}'"))
        })?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("recovery audit column '{name}' is not Utf8"))
        })
}

fn graph_uri(graph_root: &Path) -> String {
    graph_root.to_str().unwrap().to_string()
}
