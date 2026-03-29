use omnigraph::db::{ReadTarget, RunRecord, Snapshot};
use omnigraph_compiler::result::QueryResult;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotTableOutput {
    pub table_key: String,
    pub table_path: String,
    pub table_version: u64,
    pub table_branch: Option<String>,
    pub row_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotOutput {
    pub branch: String,
    pub manifest_version: u64,
    pub tables: Vec<SnapshotTableOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunOutput {
    pub run_id: String,
    pub target_branch: String,
    pub run_branch: String,
    pub base_snapshot_id: String,
    pub base_manifest_version: u64,
    pub operation_hash: Option<String>,
    pub status: String,
    pub published_snapshot_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunListOutput {
    pub runs: Vec<RunOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadTargetOutput {
    pub branch: Option<String>,
    pub snapshot: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadOutput {
    pub query_name: String,
    pub target: ReadTargetOutput,
    pub row_count: usize,
    pub rows: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeOutput {
    pub branch: String,
    pub query_name: String,
    pub affected_nodes: usize,
    pub affected_edges: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadRequest {
    pub query_source: String,
    pub query_name: Option<String>,
    pub params: Option<Value>,
    pub branch: Option<String>,
    pub snapshot: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeRequest {
    pub query_source: String,
    pub query_name: Option<String>,
    pub params: Option<Value>,
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SnapshotQuery {
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthOutput {
    pub status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    BadRequest,
    NotFound,
    Conflict,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorOutput {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<ErrorCode>,
}

pub fn snapshot_payload(branch: &str, snapshot: &Snapshot) -> SnapshotOutput {
    let mut entries: Vec<_> = snapshot.entries().cloned().collect();
    entries.sort_by(|a, b| a.table_key.cmp(&b.table_key));
    let tables = entries
        .iter()
        .map(|entry| SnapshotTableOutput {
            table_key: entry.table_key.clone(),
            table_path: entry.table_path.clone(),
            table_version: entry.table_version,
            table_branch: entry.table_branch.clone(),
            row_count: entry.row_count,
        })
        .collect::<Vec<_>>();
    SnapshotOutput {
        branch: branch.to_string(),
        manifest_version: snapshot.version(),
        tables,
    }
}

pub fn run_output(run: &RunRecord) -> RunOutput {
    RunOutput {
        run_id: run.run_id.as_str().to_string(),
        target_branch: run.target_branch.clone(),
        run_branch: run.run_branch.clone(),
        base_snapshot_id: run.base_snapshot_id.as_str().to_string(),
        base_manifest_version: run.base_manifest_version,
        operation_hash: run.operation_hash.clone(),
        status: run.status.as_str().to_string(),
        published_snapshot_id: run.published_snapshot_id.clone(),
        created_at: run.created_at,
        updated_at: run.updated_at,
    }
}

pub fn read_output(query_name: String, target: &ReadTarget, result: QueryResult) -> ReadOutput {
    ReadOutput {
        query_name,
        target: read_target_output(target),
        row_count: result.num_rows(),
        rows: result.to_rust_json(),
    }
}

pub fn read_target_output(target: &ReadTarget) -> ReadTargetOutput {
    match target {
        ReadTarget::Branch(branch) => ReadTargetOutput {
            branch: Some(branch.clone()),
            snapshot: None,
        },
        ReadTarget::Snapshot(snapshot) => ReadTargetOutput {
            branch: None,
            snapshot: Some(snapshot.as_str().to_string()),
        },
    }
}
