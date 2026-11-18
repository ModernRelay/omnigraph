//! Read-only acceptance of a complete, stopped graph copy. Source and destination
//! must be opened independently, with source storage inaccessible to the destination.
use super::*;
use crate::db::DatasetEntry;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_BRANCHES: usize = 4_096;
const MAX_COMMITS: usize = 100_000;
const MAX_TABLE_VERSIONS: usize = 100_000;
const MAX_RESTORE_EXPORT_BYTES: u64 = 1024 * 1024 * 1024 * 1024;

#[derive(Default)]
struct VerifiedTables {
    digests: BTreeMap<[u8; 32], [u8; 32]>,
    exported_bytes: u64,
    blobs: export::RestoreBlobCache,
}

/// A compact comparison of all live references, retained ancestry and exact
/// table contents; it contains no graph rows or root-specific read locations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreHistoryWitness {
    pub version: u32,
    pub schema: SchemaContractDigest,
    pub branch_heads: BTreeMap<String, String>,
    pub commits: u64,
    pub table_versions: u64,
    pub sha256: String,
}

// This is a versioned sequence of length-prefixed deterministic JSON records.
// Physical graph roots and read locations are not logical identities.
fn record(hash: &mut Sha256, value: impl Serialize) -> Result<()> {
    let bytes =
        serde_json::to_vec(&value).map_err(|err| OmniError::manifest_internal(err.to_string()))?;
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    Ok(())
}

fn bounded(name: &'static str, count: usize, max: usize) -> Result<()> {
    if count > max {
        return Err(OmniError::resource_limit(name, max as u64, count as u64));
    }
    Ok(())
}

#[derive(Serialize, PartialEq, Eq)]
struct RetainedBranch {
    native_branch: Option<String>,
    head: String,
    from: Option<String>,
    from_commit: Option<String>,
    created_at: Option<i64>,
    unpublished: Vec<String>,
    unattached: Vec<String>,
}

impl Omnigraph {
    async fn restore_refs(&self) -> Result<BTreeMap<String, RetainedBranch>> {
        let list = self.branch_list_detailed(&mut |_| true).await?;
        bounded("restore_branches", list.branches.len(), MAX_BRANCHES)?;
        if list.branches.len() != list.details.len() {
            return Err(OmniError::manifest_conflict(
                "branch disappeared during full-history verification",
            ));
        }
        let mut refs = BTreeMap::new();
        let mut count = 0;
        for branch in list.details {
            let name = branch.name;
            let (snapshot, head) = self
                .fresh_snapshot_for_branch_unchecked(normalize_branch_name(&name)?.as_deref())
                .await?;
            if head != branch.head.graph_commit_id {
                return Err(OmniError::manifest_conflict(
                    "branch changed during full-history verification",
                ));
            }
            let mut unpublished: Vec<_> = self
                .list_unpublished_commits(Some(&name))
                .await?
                .into_iter()
                .map(|c| c.graph_commit_id)
                .collect();
            let mut unattached: Vec<_> = self
                .list_unattached_commits(Some(&name))
                .await?
                .into_iter()
                .map(|c| c.graph_commit_id)
                .collect();
            count += unpublished.len() + unattached.len();
            bounded("restore_commit_refs", count, MAX_COMMITS)?;
            unpublished.sort();
            unattached.sort();
            refs.insert(
                name,
                RetainedBranch {
                    native_branch: snapshot.native_branch().map(str::to_owned),
                    head,
                    from: branch.from,
                    from_commit: branch.from_commit_id,
                    created_at: branch.created_at,
                    unpublished,
                    unattached,
                },
            );
        }
        Ok(refs)
    }

    /// Verify the complete reachable graph at this root, including both parents of
    /// merges, every historical schema and table image, and managed Blob contents.
    /// This does no writes, acquires no storage lock and cannot authorize a writer.
    /// Use on an immutable copy or with writers excluded, with storage permissions
    /// confined to that copy. Compare the entire witness with the expected witness.
    pub async fn verify_restored_history(&self) -> Result<RestoreHistoryWitness> {
        if storage_kind_for_uri(self.uri())? == StorageKind::Local {
            omnigraph_storage::FrozenDirectory::verify_local_tree(std::path::Path::new(self.uri()))
                .map_err(|error| OmniError::storage(error.into()))?;
        }
        let refs = self.restore_refs().await?;
        let schema = self.schema_contract_digest();
        let mut hash = Sha256::new();
        record(&mut hash, ("omnigraph.restore-history", 1, &schema, &refs))?;
        let mut tables = VerifiedTables::default();
        let mut commits = BTreeMap::new();
        let mut pending = BTreeSet::new();
        for (name, branch) in &refs {
            if self.manifest_has_external_base_paths(Some(name)).await? {
                return Err(OmniError::manifest(
                    "full-history restore found a graph manifest with external base paths",
                ));
            }
            pending.insert(branch.head.clone());
            pending.extend(
                branch
                    .from_commit
                    .iter()
                    .chain(&branch.unpublished)
                    .chain(&branch.unattached)
                    .cloned(),
            );
            let snapshot = self.snapshot_of(ReadTarget::branch(name.as_str())).await?;
            record(&mut hash, ("branch", name))?;
            self.restore_snapshot(&schema, &snapshot, &mut tables, &mut hash)
                .await
                .map_err(|error| error.with_context(format!("restore branch {name}")))?;
        }
        while let Some(id) = pending.pop_first() {
            bounded("restore_commits", commits.len() + 1, MAX_COMMITS)?;
            if commits.len().is_multiple_of(64) {
                tracing::debug!(
                    commits = commits.len(),
                    table_versions = tables.digests.len(),
                    exported_bytes = tables.exported_bytes,
                    blob_bytes_read = tables.blobs.read_bytes,
                    blob_cache_hits = tables.blobs.hits,
                    "verifying restored graph history"
                );
            }
            let commit = self.get_commit(&id).await?;
            if commit.graph_commit_id != id {
                return Err(OmniError::manifest(
                    "full-history restore resolved another commit",
                ));
            }
            record(&mut hash, ("commit", &commit))?;
            let snapshot = self
                .snapshot_of(ReadTarget::Snapshot(SnapshotId::new(&id)))
                .await?;
            self.restore_snapshot(&schema, &snapshot, &mut tables, &mut hash)
                .await
                .map_err(|error| error.with_context(format!("restore commit {id}")))?;
            let parents = [commit.parent_commit_id, commit.merged_parent_commit_id];
            for parent in parents.iter().flatten() {
                if !commits.contains_key(parent) {
                    pending.insert(parent.clone());
                }
            }
            commits.insert(id.clone(), (commit.generation, parents));
            pending.remove(&id);
        }
        // The generation rises on both edges of a merge. This also excludes
        // a cyclic ancestry, even when every named commit was present.
        for (generation, parents) in commits.values() {
            for parent in parents.iter().flatten() {
                if commits
                    .get(parent)
                    .is_none_or(|(parent_generation, _)| parent_generation >= generation)
                {
                    return Err(OmniError::manifest(
                        "restored commit ancestry is missing or cyclic",
                    ));
                }
            }
        }
        if self.restore_refs().await? != refs || self.schema_contract_digest() != schema {
            return Err(OmniError::manifest_conflict(
                "graph changed during full-history verification",
            ));
        }
        tracing::debug!(
            commits = commits.len(),
            table_versions = tables.digests.len(),
            exported_bytes = tables.exported_bytes,
            blob_bytes_read = tables.blobs.read_bytes,
            blob_cache_hits = tables.blobs.hits,
            "verified restored graph history"
        );
        Ok(RestoreHistoryWitness {
            version: 1,
            schema,
            branch_heads: refs
                .into_iter()
                .map(|(name, branch)| (name, branch.head))
                .collect(),
            commits: commits.len() as u64,
            table_versions: tables.digests.len() as u64,
            sha256: format!("{:x}", hash.finalize()),
        })
    }

    async fn restore_snapshot(
        &self,
        schema: &SchemaContractDigest,
        snapshot: &Snapshot,
        verified: &mut VerifiedTables,
        hash: &mut Sha256,
    ) -> Result<()> {
        let accepted = self.accepted_schema_for_snapshot(snapshot).await?;
        validate_bound_catalog_against_snapshot(&accepted.catalog, snapshot)?;
        if accepted.row.head.schema_identity_domain != schema.schema_identity_domain {
            return Err(OmniError::manifest(
                "restored snapshot belongs to another schema identity domain",
            ));
        }
        record(
            hash,
            (
                snapshot.graph_manifest_version(),
                &accepted.row.head,
                &accepted.row.source,
                &accepted.row.ir,
            ),
        )?;
        let mut entries: Vec<_> = snapshot.datasets().collect();
        entries.sort_by(|a, b| a.type_key.cmp(&b.type_key));
        for table in entries {
            let key = restore_table_key(table, &accepted.row.head.schema_ir_hash)?;
            let digest = if let Some(digest) = verified.digests.get(&key) {
                *digest
            } else {
                bounded(
                    "restore_table_versions",
                    verified.digests.len() + 1,
                    MAX_TABLE_VERSIONS,
                )?;
                let dataset = snapshot.open_lance_dataset(&table.type_key).await?;
                if omnigraph_core::branch_base::has_external_bases(&dataset) {
                    return Err(OmniError::manifest(
                        "full-history restore found a table with external base paths",
                    ));
                }
                let mut paths = collector::CollectorPathSnapshot::default();
                collector::capture_dataset_paths(&mut paths, &dataset, &table.type_key).await?;
                if paths
                    .first_missing_location(BRANCH_START_RETENTION_PROBES)
                    .await?
                    .is_some()
                {
                    return Err(OmniError::manifest(format!(
                        "restored snapshot references a missing object for {} version {}",
                        table.type_key, table.published_dataset_version,
                    )));
                }
                let digest = export::restored_table_digest(
                    &dataset,
                    &accepted.catalog,
                    table,
                    &mut verified.blobs,
                    &mut verified.exported_bytes,
                    MAX_RESTORE_EXPORT_BYTES,
                )
                .await
                .map_err(|error| error.with_context(format!("restore {}", table.type_key)))?;
                verified.digests.insert(key, digest);
                digest
            };
            record(hash, (table.manifest_version, &key, &digest))?;
        }
        Ok(())
    }
}

fn restore_table_key(t: &DatasetEntry, ir_hash: &str) -> Result<[u8; 32]> {
    let mut hash = Sha256::new();
    // Resolve at the restored root. The persisted manifest path can include the
    // original bucket key or local absolute directory; it is not a logical id.
    let pin = &t.version_metadata;
    record(
        &mut hash,
        (
            ir_hash,
            &t.type_key,
            t.identity.stable_table_id,
            t.identity.table_incarnation_id,
            &t.dataset_path,
            &t.native_dataset_branch,
            t.published_dataset_version,
            t.entity_count,
            pin.table_fork_owner(),
            pin.transaction_uuid(),
            pin.staged_version(),
            pin.last_linear_version(),
            pin.e_tag(),
        ),
    )?;
    Ok(hash.finalize().into())
}
