//! Fresh graph namespaces over the shared-memory Lance provider.
//! Backend ETag counters and incomplete multipart uploads are not reset.

use std::collections::BTreeSet;
use std::sync::{Arc, LazyLock, Mutex};

use futures::{StreamExt, TryStreamExt};
use lance_io::object_store::providers::shared_memory::SharedMemoryStoreProvider;
use lance_io::object_store::{ObjectStoreParams, ObjectStoreProvider};
use object_store::path::Path;
use omnigraph::object_store_seam::{ReadThroughMount, mount_read_through};
use omnigraph::storage::ObjectStorageAdapter;
use omnigraph_storage::FrozenDirectory;
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
    authority: String,
    mount: Option<ReadThroughMount>,
    _lease: AuthorityLease,
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

impl MemoryStorage {
    /// Mount a `file://` graph root under both realms of the empty
    /// environment: reads fall through to it below every harness wrapper,
    /// every write of the run stays in memory, and the root is never
    /// written. The mount lasts until teardown.
    pub async fn mount_prepared(&mut self, uri: &str) -> Result<(), String> {
        let root = prepared_root(uri)?;
        let frozen = FrozenDirectory::open(&root)
            .await
            .map_err(|e| format!("prepared store {uri}: {e}"))?;
        let mount = mount_read_through(&self.authority, Arc::clone(&frozen), self.prefix.clone())?;
        let control_prefix: Path = Path::from(self.authority.as_str())
            .parts()
            .chain(self.prefix.parts())
            .collect();
        self.adapter = Arc::new(ObjectStorageAdapter::read_through(frozen, control_prefix));
        self.mount = Some(mount);
        Ok(())
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
        let authority = url.authority().to_string();
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
            authority,
            mount: None,
            _lease: lease,
        })
    }

    async fn teardown(&self, resources: &mut MemoryStorage) -> Result<(), String> {
        resources.mount = None;
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

    /// The engine's registry store and the adapter both read the mounted
    /// root; the run's writes and deletes stay in memory, teardown clears
    /// them and the mount, and the root's files are what they were.
    #[tokio::test]
    async fn mounted_root_reads_through_both_realms_and_stays_unwritten() {
        /// The scratch root, removed when the test ends however it ends.
        struct Scratch(std::path::PathBuf);
        impl Drop for Scratch {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let scratch = Scratch(
            std::env::temp_dir().join(format!("omnigraph-dst-memory-mount-{}", std::process::id())),
        );
        let dir = scratch.0.as_path();
        std::fs::create_dir_all(dir.join("data")).unwrap();
        std::fs::write(dir.join("data/table.lance"), b"table").unwrap();
        std::fs::write(dir.join("__init.json"), b"claim").unwrap();
        let root = "shared-memory://memory-env-mount/graph";
        let environment = MemoryEnvironment::new(root, 1, UniverseProcess::Shared);
        let mut resources = environment.setup().await.unwrap();
        resources
            .mount_prepared(&format!("file://{}", dir.display()))
            .await
            .unwrap();

        let registry = omnigraph::dst_lance_store_registry();
        let lance = registry
            .get_provider("shared-memory")
            .unwrap()
            .new_store(Url::parse(root).unwrap(), &ObjectStoreParams::default())
            .await
            .unwrap()
            .inner;
        let table = Path::from("graph/data/table.lance");
        assert_eq!(
            lance.get(&table).await.unwrap().bytes().await.unwrap(),
            bytes::Bytes::from_static(b"table")
        );
        let claim = format!("{root}/__init.json");
        assert_eq!(resources.adapter.read_text(&claim).await.unwrap(), "claim");

        lance
            .put(&table, PutPayload::from_static(b"rewritten"))
            .await
            .unwrap();
        lance
            .delete(&Path::from("graph/__init.json"))
            .await
            .unwrap();
        assert!(resources.adapter.read_text(&claim).await.is_err());
        assert_eq!(
            std::fs::read(dir.join("data/table.lance")).unwrap(),
            b"table"
        );
        assert!(dir.join("__init.json").exists());

        environment.teardown(&mut resources).await.unwrap();
        assert!(matches!(
            lance.get(&table).await,
            Err(object_store::Error::NotFound { .. })
        ));
        drop(resources);
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
