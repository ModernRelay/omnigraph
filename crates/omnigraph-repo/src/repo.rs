use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use omnigraph_engine::build_catalog;
use omnigraph_engine::schema::parser::parse_schema;
use omnigraph_engine::store::manifest::GraphManifest;
use serde::{Deserialize, Serialize};

use crate::error::{RepoError, Result};

pub const REPO_METADATA_FILE: &str = "_repo.json";
pub const SCHEMA_FILE_NAME: &str = "_schema.pg";
const BRANCHES_DIR: &str = "_refs/branches";
const DEFAULT_DB_DIR: &str = "_db";
const BRANCH_DATABASES_DIR: &str = "_db_branches";
const REPO_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoMetadata {
    pub format_version: u32,
    pub default_branch: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetRef {
    pub dataset_key: String,
    pub dataset_path: String,
    pub lance_branch: Option<String>,
    pub version: u64,
    pub writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchHead {
    pub name: String,
    pub parent: Option<String>,
    pub snapshot_id: u64,
    pub schema_file: String,
    pub schema_hash: String,
    pub datasets: Vec<DatasetRef>,
}

#[derive(Debug, Clone)]
pub struct RepoSnapshot {
    repo_path: PathBuf,
    branch: String,
    head: BranchHead,
}

#[derive(Debug, Clone)]
pub struct GraphRepo {
    path: PathBuf,
    metadata: RepoMetadata,
}

impl GraphRepo {
    pub fn init(path: &Path, schema_source: &str) -> Result<Self> {
        validate_schema(schema_source)?;
        fs::create_dir_all(path.join(BRANCHES_DIR))?;
        fs::create_dir_all(path.join(BRANCH_DATABASES_DIR))?;

        let metadata = RepoMetadata {
            format_version: REPO_FORMAT_VERSION,
            default_branch: "main".to_string(),
        };
        write_json(path.join(REPO_METADATA_FILE), &metadata)?;
        fs::write(path.join(SCHEMA_FILE_NAME), schema_source)?;

        let main_head = BranchHead {
            name: metadata.default_branch.clone(),
            parent: None,
            snapshot_id: next_snapshot_id(),
            schema_file: SCHEMA_FILE_NAME.to_string(),
            schema_hash: hash_string(schema_source),
            datasets: Vec::new(),
        };
        write_json(branch_head_path(path, &metadata.default_branch), &main_head)?;

        Ok(Self {
            path: path.to_path_buf(),
            metadata,
        })
    }

    pub fn open(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Err(RepoError::RepoNotFound(path.display().to_string()));
        }
        let metadata_path = path.join(REPO_METADATA_FILE);
        if !metadata_path.exists() {
            return Err(RepoError::RepoNotFound(path.display().to_string()));
        }
        let metadata: RepoMetadata = read_json(&metadata_path)?;
        if metadata.format_version != REPO_FORMAT_VERSION {
            return Err(RepoError::InvalidRepo(format!(
                "unsupported format version {}",
                metadata.format_version
            )));
        }
        Ok(Self {
            path: path.to_path_buf(),
            metadata,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn metadata(&self) -> &RepoMetadata {
        &self.metadata
    }

    pub fn branch_db_path(&self, branch: &str) -> Result<PathBuf> {
        validate_branch_name(branch)?;
        Ok(self.branch_db_path_unchecked(branch))
    }

    pub fn create_branch(&self, from: &str, to: &str) -> Result<()> {
        validate_branch_name(from)?;
        validate_branch_name(to)?;
        let target = branch_head_path(&self.path, to);
        if target.exists() {
            return Err(RepoError::BranchExists(to.to_string()));
        }

        self.load_branch_head(from)?;
        self.copy_branch_database(from, to)?;

        let target_head = BranchHead {
            name: to.to_string(),
            parent: Some(from.to_string()),
            snapshot_id: next_snapshot_id(),
            schema_file: SCHEMA_FILE_NAME.to_string(),
            schema_hash: self.repo_schema_hash()?,
            datasets: self.branch_dataset_refs(to)?,
        };
        write_json(target, &target_head)
    }

    pub fn snapshot(&self, branch: Option<&str>) -> Result<RepoSnapshot> {
        let branch = branch.unwrap_or(&self.metadata.default_branch);
        let head = self.sync_branch_head_from_db(branch)?;
        Ok(RepoSnapshot {
            repo_path: self.path.clone(),
            branch: branch.to_string(),
            head,
        })
    }

    pub fn load_branch_head(&self, branch: &str) -> Result<BranchHead> {
        validate_branch_name(branch)?;
        let path = branch_head_path(&self.path, branch);
        if !path.exists() {
            return Err(RepoError::BranchNotFound(branch.to_string()));
        }
        read_json(&path)
    }

    pub fn sync_branch_head_from_db(&self, branch: &str) -> Result<BranchHead> {
        let mut head = self.load_branch_head(branch)?;
        let datasets = self.branch_dataset_refs(branch)?;
        let schema_hash = self.repo_schema_hash()?;

        if head.schema_file != SCHEMA_FILE_NAME
            || head.schema_hash != schema_hash
            || head.datasets != datasets
        {
            head.schema_file = SCHEMA_FILE_NAME.to_string();
            head.schema_hash = schema_hash;
            head.datasets = datasets;
            head.snapshot_id = next_snapshot_id();
            write_json(branch_head_path(&self.path, branch), &head)?;
        }

        Ok(head)
    }

    fn branch_db_path_unchecked(&self, branch: &str) -> PathBuf {
        if branch == self.metadata.default_branch {
            self.path.join(DEFAULT_DB_DIR)
        } else {
            self.path.join(BRANCH_DATABASES_DIR).join(branch)
        }
    }

    fn branch_dataset_refs(&self, branch: &str) -> Result<Vec<DatasetRef>> {
        let db_path = self.branch_db_path_unchecked(branch);
        if !db_path.exists() {
            return Ok(Vec::new());
        }

        let manifest = GraphManifest::read(&db_path).map_err(|err| {
            RepoError::InvalidRepo(format!(
                "failed to read branch database for {branch}: {err}"
            ))
        })?;
        let db_prefix = if branch == self.metadata.default_branch {
            DEFAULT_DB_DIR.to_string()
        } else {
            format!("{BRANCH_DATABASES_DIR}/{branch}")
        };

        Ok(manifest
            .datasets
            .into_iter()
            .map(|dataset| DatasetRef {
                dataset_key: format!("{}:{}", dataset.kind, dataset.type_name),
                dataset_path: format!("{db_prefix}/{}", dataset.dataset_path),
                lance_branch: None,
                version: dataset.dataset_version,
                writable: true,
            })
            .collect())
    }

    fn repo_schema_hash(&self) -> Result<String> {
        let schema = fs::read_to_string(self.path.join(SCHEMA_FILE_NAME))?;
        Ok(hash_string(&schema))
    }

    fn copy_branch_database(&self, from: &str, to: &str) -> Result<()> {
        let source_db = self.branch_db_path_unchecked(from);
        if !source_db.exists() {
            return Ok(());
        }

        let target_db = self.branch_db_path_unchecked(to);
        if target_db.exists() {
            return Err(RepoError::InvalidRepo(format!(
                "database directory already exists for branch {}",
                to
            )));
        }

        copy_dir_all(&source_db, &target_db)
    }
}

impl RepoSnapshot {
    pub fn repo_path(&self) -> &Path {
        &self.repo_path
    }

    pub fn branch(&self) -> &str {
        &self.branch
    }

    pub fn head(&self) -> &BranchHead {
        &self.head
    }
}

fn validate_schema(schema_source: &str) -> Result<()> {
    let parsed = parse_schema(schema_source).map_err(|err| RepoError::Schema(err.to_string()))?;
    build_catalog(&parsed).map_err(|err| RepoError::Schema(err.to_string()))?;
    Ok(())
}

fn validate_branch_name(branch: &str) -> Result<()> {
    if branch.is_empty() {
        return Err(RepoError::InvalidBranchName(
            "branch names cannot be empty".to_string(),
        ));
    }
    if branch == "." || branch == ".." {
        return Err(RepoError::InvalidBranchName(format!(
            "branch name `{branch}` is reserved"
        )));
    }
    if !branch
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(RepoError::InvalidBranchName(format!(
            "branch name `{branch}` may only contain ASCII letters, digits, '.', '_' or '-'"
        )));
    }
    Ok(())
}

fn branch_head_path(root: &Path, branch: &str) -> PathBuf {
    root.join(BRANCHES_DIR).join(format!("{branch}.json"))
}

fn copy_dir_all(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_all(&src_path, &dst_path)?;
        } else if file_type.is_file() {
            fs::copy(&src_path, &dst_path)?;
        } else {
            return Err(RepoError::InvalidRepo(format!(
                "unsupported filesystem entry in branch database: {}",
                src_path.display()
            )));
        }
    }
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let data = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&data)?)
}

fn write_json<T: Serialize>(path: PathBuf, value: &T) -> Result<()> {
    let data = serde_json::to_string_pretty(value)?;
    fs::write(path, data)?;
    Ok(())
}

fn next_snapshot_id() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

fn hash_string(value: &str) -> String {
    let mut hash: u64 = 14695981039346656037;
    for byte in value.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use omnigraph_engine::store::manifest::{DatasetEntry, GraphManifest};
    use tempfile::TempDir;

    use super::*;

    const SCHEMA: &str = r#"
node Person {
    name: String
}
"#;

    #[test]
    fn init_creates_main_branch() {
        let dir = TempDir::new().unwrap();
        let repo = GraphRepo::init(dir.path(), SCHEMA).unwrap();
        let snapshot = repo.snapshot(None).unwrap();
        assert_eq!(snapshot.branch(), "main");
        assert_eq!(snapshot.head().datasets.len(), 0);
        assert!(dir.path().join(REPO_METADATA_FILE).exists());
        assert!(dir.path().join(SCHEMA_FILE_NAME).exists());
    }

    #[test]
    fn create_branch_copies_source_head() {
        let dir = TempDir::new().unwrap();
        let repo = GraphRepo::init(dir.path(), SCHEMA).unwrap();
        repo.create_branch("main", "feature").unwrap();
        let snapshot = repo.snapshot(Some("feature")).unwrap();
        assert_eq!(snapshot.branch(), "feature");
        assert_eq!(snapshot.head().parent.as_deref(), Some("main"));
        assert_eq!(snapshot.head().schema_file, SCHEMA_FILE_NAME);
    }

    #[test]
    fn snapshot_syncs_branch_head_from_manifest() {
        let dir = TempDir::new().unwrap();
        let repo = GraphRepo::init(dir.path(), SCHEMA).unwrap();
        let db_path = repo.branch_db_path("main").unwrap();
        fs::create_dir_all(&db_path).unwrap();
        write_manifest(&db_path, "nodes/person", 3);

        let snapshot = repo.snapshot(Some("main")).unwrap();
        assert_eq!(snapshot.head().datasets.len(), 1);
        assert_eq!(snapshot.head().datasets[0].dataset_key, "node:Person");
        assert_eq!(snapshot.head().datasets[0].dataset_path, "_db/nodes/person");
        assert_eq!(snapshot.head().datasets[0].version, 3);
    }

    #[test]
    fn create_branch_copies_source_database_into_branch_directory() {
        let dir = TempDir::new().unwrap();
        let repo = GraphRepo::init(dir.path(), SCHEMA).unwrap();
        let main_db = repo.branch_db_path("main").unwrap();
        fs::create_dir_all(main_db.join("nodes")).unwrap();
        fs::write(main_db.join("marker.txt"), "copied").unwrap();
        write_manifest(&main_db, "nodes/person", 7);

        repo.create_branch("main", "feature").unwrap();

        let feature_db = repo.branch_db_path("feature").unwrap();
        assert!(feature_db.join("marker.txt").exists());

        let snapshot = repo.snapshot(Some("feature")).unwrap();
        assert_eq!(snapshot.head().datasets.len(), 1);
        assert_eq!(
            snapshot.head().datasets[0].dataset_path,
            "_db_branches/feature/nodes/person"
        );
        assert_eq!(snapshot.head().datasets[0].version, 7);
    }

    #[test]
    fn branch_names_reject_path_traversal() {
        let dir = TempDir::new().unwrap();
        let repo = GraphRepo::init(dir.path(), SCHEMA).unwrap();

        let err = repo.create_branch("main", "../outside").unwrap_err();
        assert!(matches!(err, RepoError::InvalidBranchName(_)));
    }

    fn write_manifest(db_path: &Path, dataset_path: &str, dataset_version: u64) {
        let mut manifest = GraphManifest::new("schema-hash".to_string());
        manifest.db_version = dataset_version;
        manifest.last_tx_id = format!("manifest-{dataset_version}");
        manifest.committed_at = dataset_version.to_string();
        manifest.datasets.push(DatasetEntry {
            type_id: 1,
            type_name: "Person".to_string(),
            kind: "node".to_string(),
            dataset_path: dataset_path.to_string(),
            dataset_version,
            row_count: 1,
        });
        manifest.write_atomic(db_path).unwrap();
    }
}
