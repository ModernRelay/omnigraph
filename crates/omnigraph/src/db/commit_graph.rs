use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{
    Array, RecordBatch, RecordBatchIterator, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use lance_file::version::LanceFileVersion;

use crate::error::{OmniError, Result};

const GRAPH_COMMITS_DIR: &str = "_graph_commits.lance";

#[derive(Debug, Clone)]
pub struct GraphCommit {
    pub graph_commit_id: String,
    pub manifest_branch: Option<String>,
    pub manifest_version: u64,
    pub parent_commit_id: Option<String>,
    pub merged_parent_commit_id: Option<String>,
    pub created_at: i64,
}

pub struct CommitGraph {
    root_uri: String,
    dataset: Dataset,
    active_branch: Option<String>,
}

impl CommitGraph {
    pub async fn init(root_uri: &str, manifest_version: u64) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let uri = graph_commits_uri(root);
        let genesis = GraphCommit {
            graph_commit_id: ulid::Ulid::new().to_string(),
            manifest_branch: None,
            manifest_version,
            parent_commit_id: None,
            merged_parent_commit_id: None,
            created_at: now_micros()?,
        };

        let batch = commits_to_batch(&[genesis])?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], commit_graph_schema());
        let params = WriteParams {
            mode: WriteMode::Create,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            ..Default::default()
        };
        let dataset = Dataset::write(reader, &uri as &str, Some(params))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        Ok(Self {
            root_uri: root.to_string(),
            dataset,
            active_branch: None,
        })
    }

    pub async fn open_if_exists(root_uri: &str) -> Result<Option<Self>> {
        let root = root_uri.trim_end_matches('/');
        if !Path::new(&graph_commits_uri(root)).exists() {
            return Ok(None);
        }
        Ok(Some(Self::open(root).await?))
    }

    pub async fn open(root_uri: &str) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let dataset = Dataset::open(&graph_commits_uri(root))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(Self {
            root_uri: root.to_string(),
            dataset,
            active_branch: None,
        })
    }

    pub async fn open_at_branch(root_uri: &str, branch: &str) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let dataset = Dataset::open(&graph_commits_uri(root))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let dataset = dataset
            .checkout_branch(branch)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(Self {
            root_uri: root.to_string(),
            dataset,
            active_branch: Some(branch.to_string()),
        })
    }

    pub async fn ensure_initialized(root_uri: &str, manifest_version: u64) -> Result<()> {
        if Path::new(&graph_commits_uri(root_uri.trim_end_matches('/'))).exists() {
            return Ok(());
        }
        let _ = Self::init(root_uri, manifest_version).await?;
        Ok(())
    }

    pub async fn refresh(&mut self) -> Result<()> {
        let root = self.root_uri.clone();
        self.dataset = Dataset::open(&graph_commits_uri(&root))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        if let Some(branch) = &self.active_branch {
            self.dataset = self
                .dataset
                .checkout_branch(branch)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;
        }
        Ok(())
    }

    pub fn version(&self) -> u64 {
        self.dataset.version().version
    }

    pub async fn create_branch(&mut self, name: &str) -> Result<()> {
        let mut ds = self.dataset.clone();
        ds.create_branch(name, self.version(), None)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(())
    }

    pub async fn append_commit(
        &mut self,
        manifest_branch: Option<&str>,
        manifest_version: u64,
    ) -> Result<String> {
        let parent_commit_id = self.head_commit_id().await?;
        self.append_commit_with_parents(
            manifest_branch,
            manifest_version,
            parent_commit_id.as_deref(),
            None,
        )
        .await
    }

    pub async fn append_merge_commit(
        &mut self,
        manifest_branch: Option<&str>,
        manifest_version: u64,
        parent_commit_id: &str,
        merged_parent_commit_id: &str,
    ) -> Result<String> {
        self.append_commit_with_parents(
            manifest_branch,
            manifest_version,
            Some(parent_commit_id),
            Some(merged_parent_commit_id),
        )
        .await
    }

    async fn append_commit_with_parents(
        &mut self,
        manifest_branch: Option<&str>,
        manifest_version: u64,
        parent_commit_id: Option<&str>,
        merged_parent_commit_id: Option<&str>,
    ) -> Result<String> {
        let graph_commit_id = ulid::Ulid::new().to_string();
        let commit = GraphCommit {
            graph_commit_id: graph_commit_id.clone(),
            manifest_branch: manifest_branch.map(|s| s.to_string()),
            manifest_version,
            parent_commit_id: parent_commit_id.map(|s| s.to_string()),
            merged_parent_commit_id: merged_parent_commit_id.map(|s| s.to_string()),
            created_at: now_micros()?,
        };

        let batch = commits_to_batch(&[commit])?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], commit_graph_schema());
        let mut ds = self.dataset.clone();
        ds.append(reader, None)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        self.dataset = ds;

        Ok(graph_commit_id)
    }

    pub async fn head_commit(&self) -> Result<Option<GraphCommit>> {
        let commits = self.load_commits().await?;
        Ok(commits.into_iter().max_by(|a, b| {
            a.manifest_version
                .cmp(&b.manifest_version)
                .then_with(|| a.created_at.cmp(&b.created_at))
                .then_with(|| a.graph_commit_id.cmp(&b.graph_commit_id))
        }))
    }

    pub async fn head_commit_id(&self) -> Result<Option<String>> {
        Ok(self.head_commit().await?.map(|c| c.graph_commit_id))
    }

    pub async fn load_commits(&self) -> Result<Vec<GraphCommit>> {
        let batches: Vec<RecordBatch> = self
            .dataset
            .scan()
            .try_into_stream()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        let mut commits = Vec::new();
        for batch in &batches {
            let ids = batch
                .column_by_name("graph_commit_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let branches = batch
                .column_by_name("manifest_branch")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let versions = batch
                .column_by_name("manifest_version")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let parents = batch
                .column_by_name("parent_commit_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let merged_parents = batch
                .column_by_name("merged_parent_commit_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let created = batch
                .column_by_name("created_at")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();

            for row in 0..batch.num_rows() {
                commits.push(GraphCommit {
                    graph_commit_id: ids.value(row).to_string(),
                    manifest_branch: if branches.is_null(row) {
                        None
                    } else {
                        Some(branches.value(row).to_string())
                    },
                    manifest_version: versions.value(row),
                    parent_commit_id: if parents.is_null(row) {
                        None
                    } else {
                        Some(parents.value(row).to_string())
                    },
                    merged_parent_commit_id: if merged_parents.is_null(row) {
                        None
                    } else {
                        Some(merged_parents.value(row).to_string())
                    },
                    created_at: created.value(row),
                });
            }
        }

        Ok(commits)
    }

    pub async fn merge_base(
        root_uri: &str,
        source_branch: Option<&str>,
        target_branch: Option<&str>,
    ) -> Result<Option<GraphCommit>> {
        let source = open_for_branch(root_uri, source_branch).await?;
        let target = open_for_branch(root_uri, target_branch).await?;

        let source_head = match source.head_commit().await? {
            Some(commit) => commit,
            None => return Ok(None),
        };
        let target_head = match target.head_commit().await? {
            Some(commit) => commit,
            None => return Ok(None),
        };

        let mut commits = HashMap::new();
        for commit in source.load_commits().await? {
            commits.insert(commit.graph_commit_id.clone(), commit);
        }
        for commit in target.load_commits().await? {
            commits.insert(commit.graph_commit_id.clone(), commit);
        }

        let source_distances = ancestor_distances(&source_head.graph_commit_id, &commits);
        let target_distances = ancestor_distances(&target_head.graph_commit_id, &commits);

        let best = source_distances
            .iter()
            .filter_map(|(id, source_distance)| {
                target_distances.get(id).and_then(|target_distance| {
                    commits.get(id).map(|commit| {
                        (
                            (
                                *source_distance + *target_distance,
                                u64::MAX - commit.manifest_version,
                            ),
                            commit.clone(),
                        )
                    })
                })
            })
            .min_by_key(|(score, _)| *score)
            .map(|(_, commit)| commit);

        Ok(best)
    }
}

fn graph_commits_uri(root_uri: &str) -> String {
    format!("{}/{}", root_uri.trim_end_matches('/'), GRAPH_COMMITS_DIR)
}

fn commit_graph_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("graph_commit_id", DataType::Utf8, false),
        Field::new("manifest_branch", DataType::Utf8, true),
        Field::new("manifest_version", DataType::UInt64, false),
        Field::new("parent_commit_id", DataType::Utf8, true),
        Field::new("merged_parent_commit_id", DataType::Utf8, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn commits_to_batch(commits: &[GraphCommit]) -> Result<RecordBatch> {
    let ids: Vec<&str> = commits.iter().map(|c| c.graph_commit_id.as_str()).collect();
    let branches: Vec<Option<&str>> = commits
        .iter()
        .map(|c| c.manifest_branch.as_deref())
        .collect();
    let versions: Vec<u64> = commits.iter().map(|c| c.manifest_version).collect();
    let parents: Vec<Option<&str>> = commits
        .iter()
        .map(|c| c.parent_commit_id.as_deref())
        .collect();
    let merged_parents: Vec<Option<&str>> = commits
        .iter()
        .map(|c| c.merged_parent_commit_id.as_deref())
        .collect();
    let created_at: Vec<i64> = commits.iter().map(|c| c.created_at).collect();

    RecordBatch::try_new(
        commit_graph_schema(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(branches)),
            Arc::new(UInt64Array::from(versions)),
            Arc::new(StringArray::from(parents)),
            Arc::new(StringArray::from(merged_parents)),
            Arc::new(TimestampMicrosecondArray::from(created_at)),
        ],
    )
    .map_err(|e| OmniError::Lance(e.to_string()))
}

fn ancestor_distances(
    start_id: &str,
    commits: &HashMap<String, GraphCommit>,
) -> HashMap<String, u64> {
    let mut distances = HashMap::new();
    let mut queue = VecDeque::from([(start_id.to_string(), 0u64)]);

    while let Some((id, distance)) = queue.pop_front() {
        if let Some(existing) = distances.get(&id) {
            if *existing <= distance {
                continue;
            }
        }
        distances.insert(id.clone(), distance);

        if let Some(commit) = commits.get(&id) {
            if let Some(parent) = &commit.parent_commit_id {
                queue.push_back((parent.clone(), distance + 1));
            }
            if let Some(parent) = &commit.merged_parent_commit_id {
                queue.push_back((parent.clone(), distance + 1));
            }
        }
    }

    distances
}

async fn open_for_branch(root_uri: &str, branch: Option<&str>) -> Result<CommitGraph> {
    match branch {
        Some(branch) if branch != "main" => CommitGraph::open_at_branch(root_uri, branch).await,
        _ => CommitGraph::open(root_uri).await,
    }
}

fn now_micros() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| OmniError::Manifest(format!("system clock before UNIX_EPOCH: {}", e)))?;
    Ok(duration.as_micros() as i64)
}
