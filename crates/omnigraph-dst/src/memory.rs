//! Fresh graph namespaces over the shared-memory Lance provider.
//! Backend ETag counters and incomplete multipart uploads are not reset.

use std::collections::BTreeSet;
use std::sync::{Arc, LazyLock, Mutex};

use futures::{StreamExt, TryStreamExt};
use lance_io::object_store::providers::shared_memory::SharedMemoryStoreProvider;
use lance_io::object_store::{ObjectStoreParams, ObjectStoreProvider};
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use omnigraph::storage::{ObjectStorageAdapter, StorageAdapter};
use url::Url;

use crate::environment::{UniverseEnvironment, UniverseProcess};

static ACTIVE_AUTHORITIES: LazyLock<Mutex<BTreeSet<String>>> =
    LazyLock::new(|| Mutex::new(BTreeSet::new()));

#[derive(Clone, Debug)]
pub struct MemoryEnvironment {
    root: String,
    seed: u64,
    process: UniverseProcess,
}

impl MemoryEnvironment {
    pub fn new(root: impl Into<String>, seed: u64, process: UniverseProcess) -> Self {
        Self {
            root: root.into(),
            seed,
            process,
        }
    }
}

#[derive(Debug)]
pub struct MemoryStorage {
    pub root: String,
    pub adapter: Arc<ObjectStorageAdapter>,
    lance: Arc<dyn object_store::ObjectStore>,
    prefix: Path,
    _lease: AuthorityLease,
}

/// What a prepared store's restore put into the environment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Restored {
    pub uri: String,
    pub objects: usize,
    pub bytes: u64,
}

/// The directory a `file://` prepared-store URI names, when it exists.
pub fn prepared_root(uri: &str) -> Result<std::path::PathBuf, String> {
    let root = uri
        .strip_prefix("file://")
        .map(std::path::PathBuf::from)
        .ok_or_else(|| format!("a prepared store is a file:// root, got {uri}"))?;
    if !root.is_dir() {
        return Err(format!(
            "prepared store root is not a directory: {}",
            root.display()
        ));
    }
    Ok(root)
}

enum Realm {
    Lance,
    Adapter,
}

/// The realm that reads an object of the prepared store, by the write
/// census taxonomy; an unknown kind refuses the restore.
fn realm_of(key: &str) -> Result<Realm, String> {
    Ok(match crate::write_census::classify(key) {
        "manifest"
        | "lance-transaction"
        | "lance-version-manifest"
        | "lance-index"
        | "lance-deletion"
        | "lance-data-file"
        | "branch-ref" => Realm::Lance,
        "recovery-sidecar"
        | "graph-index-artifact"
        | "history-file-untriaged"
        | "init-claim"
        | "capability-probe" => Realm::Adapter,
        kind => {
            return Err(format!(
                "prepared store holds an object of kind {kind}: {key}; the write census taxonomy names every kind the engine writes"
            ));
        }
    })
}

fn collect_files(
    root: &std::path::Path,
    dir: &std::path::Path,
    keys: &mut Vec<String>,
) -> Result<(), String> {
    let mut entries = std::fs::read_dir(dir)
        .map_err(|e| format!("list {}: {e}", dir.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("list {}: {e}", dir.display()))?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|e| format!("{}: {e}", path.display()))?;
        if file_type.is_symlink() {
            return Err(format!(
                "prepared store holds a symlink: {}; the root is restored from plain files and directories only",
                path.display()
            ));
        }
        if file_type.is_dir() {
            collect_files(root, &path, keys)?;
        } else {
            let relative = path
                .strip_prefix(root)
                .map_err(|e| format!("{}: {e}", path.display()))?;
            keys.push(
                relative
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join("/"),
            );
        }
    }
    Ok(())
}

impl MemoryStorage {
    /// Fill the empty environment from a `file://` graph root: every object
    /// under it goes to the realm that reads it, below every harness
    /// wrapper, so the copy counts as no request; the root is never
    /// written.
    pub async fn restore_from_uri(&self, uri: &str) -> Result<Restored, String> {
        let root = prepared_root(uri)?;
        let mut keys = Vec::new();
        collect_files(&root, &root, &mut keys)?;
        let mut bytes = 0u64;
        for key in &keys {
            let realm = realm_of(key)?;
            let contents = std::fs::read(root.join(key)).map_err(|e| format!("read {key}: {e}"))?;
            bytes += contents.len() as u64;
            match realm {
                Realm::Lance => {
                    let path = Path::parse(format!("{}/{key}", self.prefix))
                        .map_err(|e| format!("{key}: {e}"))?;
                    self.lance
                        .put(&path, PutPayload::from(contents))
                        .await
                        .map_err(|e| format!("restore {key}: {e}"))?;
                }
                Realm::Adapter => {
                    self.adapter
                        .write_bytes(&format!("{}/{key}", self.root), &contents)
                        .await
                        .map_err(|e| format!("restore {key}: {e}"))?;
                }
            }
        }
        Ok(Restored {
            uri: uri.to_string(),
            objects: keys.len(),
            bytes,
        })
    }
}

#[derive(Debug)]
struct AuthorityLease(String);

impl AuthorityLease {
    fn acquire(authority: &str) -> Result<Self, String> {
        let mut active = ACTIVE_AUTHORITIES
            .lock()
            .map_err(|_| "memory environment ownership lock poisoned".to_string())?;
        if !active.insert(authority.to_string()) {
            return Err(format!(
                "memory environment authority is already active: {authority}"
            ));
        }
        Ok(Self(authority.to_string()))
    }
}

impl Drop for AuthorityLease {
    fn drop(&mut self) {
        ACTIVE_AUTHORITIES
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.0);
    }
}

impl UniverseEnvironment for MemoryEnvironment {
    type Resources = MemoryStorage;

    fn seed(&self) -> u64 {
        self.seed
    }

    fn process(&self) -> UniverseProcess {
        self.process
    }

    async fn setup(&self) -> Result<MemoryStorage, String> {
        let url = Url::parse(&self.root)
            .map_err(|error| format!("invalid memory environment root: {error}"))?;
        if url.scheme() != "shared-memory"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(format!(
                "memory environment requires a shared-memory root with an authority and no credentials, query or fragment: {}",
                self.root
            ));
        }
        let lease = AuthorityLease::acquire(url.authority())?;
        let provider = SharedMemoryStoreProvider::default();
        let prefix = provider
            .extract_path(&url)
            .map_err(|error| format!("invalid memory environment path: {error}"))?;
        let lance = provider
            .new_store(url, &ObjectStoreParams::default())
            .await
            .map_err(|error| format!("memory environment storage setup failed: {error}"))?
            .inner;
        if lance
            .list(Some(&prefix))
            .try_next()
            .await
            .map_err(|error| format!("memory environment preflight listing failed: {error}"))?
            .is_some()
        {
            return Err(format!(
                "memory environment root is not empty: {}",
                self.root
            ));
        }
        Ok(MemoryStorage {
            root: self.root.clone(),
            adapter: Arc::new(ObjectStorageAdapter::in_memory()),
            lance,
            prefix,
            _lease: lease,
        })
    }

    async fn teardown(&self, resources: &mut MemoryStorage) -> Result<(), String> {
        let locations = resources
            .lance
            .list(Some(&resources.prefix))
            .map_ok(|metadata| metadata.location)
            .boxed();
        resources
            .lance
            .delete_stream(locations)
            .try_for_each(|_| async { Ok(()) })
            .await
            .map_err(|error| format!("memory environment cleanup failed: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use object_store::{ObjectStoreExt, PutPayload};
    use omnigraph::storage::StorageAdapter;

    use super::*;

    async fn raw_store(root: &str) -> Arc<dyn object_store::ObjectStore> {
        SharedMemoryStoreProvider::default()
            .new_store(Url::parse(root).unwrap(), &ObjectStoreParams::default())
            .await
            .unwrap()
            .inner
    }

    async fn put(store: &dyn object_store::ObjectStore, path: &str) {
        store
            .put(&Path::from(path), PutPayload::from_static(b"stored"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn repeated_descriptor_preserves_both_realms_until_teardown() {
        let environment = MemoryEnvironment::new(
            "shared-memory://memory-env-repeat/graph",
            17,
            UniverseProcess::Shared,
        );
        let mut first = environment.setup().await.unwrap();
        let old_adapter = first.adapter.clone();
        let uri = format!("{}/control", first.root);
        first.adapter.write_text(&uri, "control").await.unwrap();
        put(first.lance.as_ref(), "graph/table").await;
        assert_eq!(first.adapter.read_text(&uri).await.unwrap(), "control");
        assert!(first.lance.head(&Path::from("graph/table")).await.is_ok());
        environment.teardown(&mut first).await.unwrap();
        drop(first);

        let mut second = environment.setup().await.unwrap();
        assert!(!Arc::ptr_eq(&old_adapter, &second.adapter));
        assert!(second.adapter.dst_list_all_keys().await.unwrap().is_empty());
        assert!(
            second
                .lance
                .list(Some(&second.prefix))
                .try_next()
                .await
                .unwrap()
                .is_none()
        );
        environment.teardown(&mut second).await.unwrap();
    }

    #[tokio::test]
    async fn refuses_preexisting_namespace_without_deleting_it() {
        let root = "shared-memory://memory-env-existing/graph";
        let store = raw_store(root).await;
        put(store.as_ref(), "graph/foreign").await;
        let environment = MemoryEnvironment::new(root, 1, UniverseProcess::Shared);
        assert!(environment.setup().await.unwrap_err().contains("not empty"));
        assert!(store.head(&Path::from("graph/foreign")).await.is_ok());
        store.delete(&Path::from("graph/foreign")).await.unwrap();
        let mut resources = environment.setup().await.unwrap();
        environment.teardown(&mut resources).await.unwrap();
    }

    #[tokio::test]
    async fn cleanup_preserves_sibling_prefix() {
        let root = "shared-memory://memory-env-prefix/graph";
        let store = raw_store(root).await;
        put(store.as_ref(), "graph-other/foreign").await;
        let environment = MemoryEnvironment::new(root, 1, UniverseProcess::Shared);
        let mut resources = environment.setup().await.unwrap();
        put(resources.lance.as_ref(), "graph/owned").await;
        environment.teardown(&mut resources).await.unwrap();
        assert!(store.head(&Path::from("graph/owned")).await.is_err());
        assert!(store.head(&Path::from("graph-other/foreign")).await.is_ok());
        store
            .delete(&Path::from("graph-other/foreign"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn refuses_overlapping_authority_until_resources_drop() {
        let first = MemoryEnvironment::new(
            "shared-memory://memory-env-lease/first",
            1,
            UniverseProcess::Shared,
        );
        let second = MemoryEnvironment::new(
            "shared-memory://memory-env-lease/second",
            2,
            UniverseProcess::Shared,
        );
        let resources = first.setup().await.unwrap();
        assert!(second.setup().await.unwrap_err().contains("already active"));
        drop(resources);
        let mut resources = second.setup().await.unwrap();
        second.teardown(&mut resources).await.unwrap();
    }
}
