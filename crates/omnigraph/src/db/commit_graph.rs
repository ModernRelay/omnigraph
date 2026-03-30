use std::collections::{HashMap, VecDeque};
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
    commit_by_id: HashMap<String, GraphCommit>,
    head_commit: Option<GraphCommit>,
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

        let batch = commits_to_batch(&[genesis.clone()])?;
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
            commit_by_id: HashMap::from([(genesis.graph_commit_id.clone(), genesis.clone())]),
            head_commit: Some(genesis),
        })
    }

    pub async fn open(root_uri: &str) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let dataset = Dataset::open(&graph_commits_uri(root))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let (commit_by_id, head_commit) = load_commit_cache(&dataset).await?;
        Ok(Self {
            root_uri: root.to_string(),
            dataset,
            active_branch: None,
            commit_by_id,
            head_commit,
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
        let (commit_by_id, head_commit) = load_commit_cache(&dataset).await?;
        Ok(Self {
            root_uri: root.to_string(),
            dataset,
            active_branch: Some(branch.to_string()),
            commit_by_id,
            head_commit,
        })
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
        let (commit_by_id, head_commit) = load_commit_cache(&self.dataset).await?;
        self.commit_by_id = commit_by_id;
        self.head_commit = head_commit;
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

        let batch = commits_to_batch(&[commit.clone()])?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], commit_graph_schema());
        let mut ds = self.dataset.clone();
        ds.append(reader, None)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        self.dataset = ds;
        self.commit_by_id
            .insert(graph_commit_id.clone(), commit.clone());
        if should_replace_head(self.head_commit.as_ref(), &commit) {
            self.head_commit = Some(commit);
        }

        Ok(graph_commit_id)
    }

    pub async fn head_commit(&self) -> Result<Option<GraphCommit>> {
        Ok(self.head_commit.clone())
    }

    pub async fn head_commit_id(&self) -> Result<Option<String>> {
        Ok(self.head_commit().await?.map(|c| c.graph_commit_id))
    }

    pub async fn load_commits(&self) -> Result<Vec<GraphCommit>> {
        let mut commits = self.commit_by_id.values().cloned().collect::<Vec<_>>();
        commits.sort_by(|a, b| {
            a.manifest_version
                .cmp(&b.manifest_version)
                .then_with(|| a.created_at.cmp(&b.created_at))
                .then_with(|| a.graph_commit_id.cmp(&b.graph_commit_id))
        });
        Ok(commits)
    }

    pub fn get_commit(&self, commit_id: &str) -> Option<GraphCommit> {
        self.commit_by_id.get(commit_id).cloned()
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

async fn load_commit_cache(dataset: &Dataset) -> Result<(HashMap<String, GraphCommit>, Option<GraphCommit>)> {
    let batches: Vec<RecordBatch> = dataset
        .scan()
        .try_into_stream()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .try_collect()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    let commits = load_commits_from_batches(&batches)?;
    let mut commit_by_id = HashMap::with_capacity(commits.len());
    let mut head_commit = None;
    for commit in commits {
        if should_replace_head(head_commit.as_ref(), &commit) {
            head_commit = Some(commit.clone());
        }
        commit_by_id.insert(commit.graph_commit_id.clone(), commit);
    }
    Ok((commit_by_id, head_commit))
}

fn load_commits_from_batches(batches: &[RecordBatch]) -> Result<Vec<GraphCommit>> {
    let mut commits = Vec::new();
    for batch in batches {
        let ids = string_column(batch, "graph_commit_id", "commit graph")?;
        let branches = string_column(batch, "manifest_branch", "commit graph")?;
        let versions = u64_column(batch, "manifest_version", "commit graph")?;
        let parents = string_column(batch, "parent_commit_id", "commit graph")?;
        let merged_parents = string_column(batch, "merged_parent_commit_id", "commit graph")?;
        let created = timestamp_micros_column(batch, "created_at", "commit graph")?;

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

fn should_replace_head(current: Option<&GraphCommit>, candidate: &GraphCommit) -> bool {
    current.is_none_or(|existing| {
        candidate
            .manifest_version
            .cmp(&existing.manifest_version)
            .then_with(|| candidate.created_at.cmp(&existing.created_at))
            .then_with(|| candidate.graph_commit_id.cmp(&existing.graph_commit_id))
            .is_gt()
    })
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str, context: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| OmniError::manifest_internal(format!("{context} batch missing '{name}' column")))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest_internal(format!("{context} column '{name}' is not Utf8")))
}

fn u64_column<'a>(batch: &'a RecordBatch, name: &str, context: &str) -> Result<&'a UInt64Array> {
    batch
        .column_by_name(name)
        .ok_or_else(|| OmniError::manifest_internal(format!("{context} batch missing '{name}' column")))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| OmniError::manifest_internal(format!("{context} column '{name}' is not UInt64")))
}

fn timestamp_micros_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
    context: &str,
) -> Result<&'a TimestampMicrosecondArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| OmniError::manifest_internal(format!("{context} batch missing '{name}' column")))?
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "{context} column '{name}' is not Timestamp(Microsecond)"
            ))
        })
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
        .map_err(|e| OmniError::manifest(format!("system clock before UNIX_EPOCH: {}", e)))?;
    Ok(duration.as_micros() as i64)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn load_commits_from_batches_returns_error_for_bad_schema() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("graph_commit_id", DataType::UInt64, false),
                Field::new("manifest_branch", DataType::Utf8, true),
                Field::new("manifest_version", DataType::UInt64, false),
                Field::new("parent_commit_id", DataType::Utf8, true),
                Field::new("merged_parent_commit_id", DataType::Utf8, true),
                Field::new(
                    "created_at",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
            ])),
            vec![
                Arc::new(UInt64Array::from(vec![1_u64])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(UInt64Array::from(vec![1_u64])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(TimestampMicrosecondArray::from(vec![1_i64])),
            ],
        )
        .unwrap();

        let err = load_commits_from_batches(&[batch]).unwrap_err();
        assert!(err.to_string().contains("graph_commit_id"));
    }
}
