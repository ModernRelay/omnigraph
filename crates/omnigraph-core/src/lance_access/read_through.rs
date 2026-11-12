//! Lance stores of a root that is read through a frozen directory: a local
//! directory or an `s3://` prefix.

use std::sync::Arc;

use async_trait::async_trait;
use lance::io::{ObjectStoreParams, ObjectStoreRegistry};
use lance_io::object_store::{ObjectStore, ObjectStoreProvider, WrappingObjectStore};
use object_store::list::PaginatedListStore;
use object_store::path::Path;
use omnigraph_storage::{ReadThroughMount, StorageIoScope};
use url::Url;

/// The mount under every local store and under the `s3` store; when `scope`
/// is given it sits above the mount, as it sits above a plain store, so a
/// scoped clone and a scoped plain root differ in the mount alone. `s3+ddb`
/// is not wrapped: its root URIs are refused before a mount is built, since
/// the storage layer admits `s3://` roots alone.
pub(super) fn registry(
    mount: ReadThroughMount,
    scope: Option<StorageIoScope>,
) -> Arc<ObjectStoreRegistry> {
    let registry = ObjectStoreRegistry::default();
    #[cfg(feature = "dst")]
    super::object_store_seam::hook(&registry);
    for scheme in ["file", "file-object-store", "s3"] {
        if let Some(inner) = registry.get_provider(scheme) {
            registry.insert(
                scheme,
                Arc::new(MountedProvider {
                    inner,
                    mount: mount.clone(),
                }),
            );
        }
    }
    if let Some(scope) = scope {
        super::scoped::wrap_providers(&registry, scope);
    }
    Arc::new(registry)
}

/// Builds every local store as `file-object-store` (a `file` store reads data
/// files by direct local path, below any mount) and every `s3` store as Lance
/// builds it, each with the mount under it.
#[derive(Debug)]
struct MountedProvider {
    inner: Arc<dyn ObjectStoreProvider>,
    mount: ReadThroughMount,
}

#[async_trait]
impl ObjectStoreProvider for MountedProvider {
    async fn new_store(
        &self,
        mut base_path: Url,
        params: &ObjectStoreParams,
    ) -> lance_core::Result<ObjectStore> {
        if base_path.scheme() == "file" {
            base_path = Url::parse(
                &base_path
                    .as_str()
                    .replacen("file:", "file-object-store:", 1),
            )
            .map_err(|_| lance_core::Error::invalid_input("could not mount the local store"))?;
        }
        let mut store = self.inner.new_store(base_path, params).await?;
        store.apply_wrapper(&Mounted(self.mount.clone()));
        Ok(store)
    }

    fn extract_path(&self, url: &Url) -> lance_core::Result<Path> {
        self.inner.extract_path(url)
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        options: Option<&std::collections::HashMap<String, String>>,
    ) -> lance_core::Result<String> {
        self.inner.calculate_object_store_prefix(url, options)
    }
}

/// The mount's store over the store Lance built, put on the store and on its
/// paginated lister together: the lister is given up, so every listing goes
/// through the mount.
#[derive(Debug)]
struct Mounted(ReadThroughMount);

impl WrappingObjectStore for Mounted {
    fn wrap(
        &self,
        _: &str,
        original: Arc<dyn object_store::ObjectStore>,
    ) -> Arc<dyn object_store::ObjectStore> {
        Arc::new(self.0.store(original))
    }

    fn wrap_paginated(
        &self,
        _: &str,
        _: Arc<dyn PaginatedListStore>,
    ) -> Option<Arc<dyn PaginatedListStore>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{
        ListDirBounds, StorageAdapter, storage_read_through, storage_read_through_scoped,
    };
    use object_store::ObjectStoreExt;

    /// The base and the clone root as root URIs: the base holds `table/a`,
    /// `table/b` and `table/sub/c`, the clone root is empty.
    fn fixture(directory: &std::path::Path) -> (String, String) {
        let base = directory.join("base");
        let root = directory.join("root");
        std::fs::create_dir_all(base.join("table/sub")).unwrap();
        std::fs::create_dir(&root).unwrap();
        std::fs::write(base.join("table/a"), b"base a").unwrap();
        std::fs::write(base.join("table/b"), b"base b").unwrap();
        std::fs::write(base.join("table/sub/c"), b"base c").unwrap();
        (
            base.to_str().unwrap().to_owned(),
            root.to_str().unwrap().to_owned(),
        )
    }

    /// Forwards every storage call but not the mount: the shape of a wrapper
    /// written before `read_through_mount` existed.
    #[derive(Debug)]
    struct DropsTheMount(Arc<dyn StorageAdapter>);

    #[async_trait]
    impl StorageAdapter for DropsTheMount {
        fn io_scope(&self) -> Option<StorageIoScope> {
            self.0.io_scope()
        }
        async fn read_text(&self, uri: &str) -> crate::error::Result<String> {
            self.0.read_text(uri).await
        }
        async fn read_text_if_exists(&self, uri: &str) -> crate::error::Result<Option<String>> {
            self.0.read_text_if_exists(uri).await
        }
        async fn read_text_if_exists_bounded(
            &self,
            uri: &str,
            max_bytes: u64,
        ) -> crate::error::Result<Option<String>> {
            self.0.read_text_if_exists_bounded(uri, max_bytes).await
        }
        async fn read_bytes_if_exists_bounded(
            &self,
            uri: &str,
            max_bytes: u64,
        ) -> crate::error::Result<Option<Vec<u8>>> {
            self.0.read_bytes_if_exists_bounded(uri, max_bytes).await
        }
        async fn write_text(&self, uri: &str, contents: &str) -> crate::error::Result<()> {
            self.0.write_text(uri, contents).await
        }
        async fn write_bytes(&self, uri: &str, contents: &[u8]) -> crate::error::Result<()> {
            self.0.write_bytes(uri, contents).await
        }
        async fn write_text_if_absent(
            &self,
            uri: &str,
            contents: &str,
        ) -> crate::error::Result<bool> {
            self.0.write_text_if_absent(uri, contents).await
        }
        async fn exists(&self, uri: &str) -> crate::error::Result<bool> {
            self.0.exists(uri).await
        }
        async fn rename_text(&self, from_uri: &str, to_uri: &str) -> crate::error::Result<()> {
            self.0.rename_text(from_uri, to_uri).await
        }
        async fn delete(&self, uri: &str) -> crate::error::Result<()> {
            self.0.delete(uri).await
        }
        async fn list_dir(&self, dir_uri: &str) -> crate::error::Result<Vec<String>> {
            self.0.list_dir(dir_uri).await
        }
        async fn list_dir_bounded(
            &self,
            dir_uri: &str,
            matching_suffix: &str,
            bounds: ListDirBounds,
        ) -> crate::error::Result<Vec<String>> {
            self.0
                .list_dir_bounded(dir_uri, matching_suffix, bounds)
                .await
        }
        async fn read_text_versioned(&self, uri: &str) -> crate::error::Result<(String, String)> {
            self.0.read_text_versioned(uri).await
        }
        async fn write_text_if_match(
            &self,
            uri: &str,
            contents: &str,
            expected_version: &str,
        ) -> crate::error::Result<Option<String>> {
            self.0
                .write_text_if_match(uri, contents, expected_version)
                .await
        }
        async fn delete_prefix(&self, prefix_uri: &str) -> crate::error::Result<()> {
            self.0.delete_prefix(prefix_uri).await
        }
    }

    #[tokio::test]
    async fn a_wrapper_that_drops_the_mount_leaves_lance_on_the_bare_root() {
        let directory = tempfile::tempdir().unwrap();
        let (base_uri, root_uri) = fixture(directory.path());
        let wrapped = DropsTheMount(storage_read_through(&base_uri, &root_uri).await.unwrap());
        assert_eq!(
            wrapped
                .read_text(&format!("{root_uri}/table/a"))
                .await
                .unwrap(),
            "base a",
            "the control reads still go through the mount the inner adapter holds"
        );
        let context = crate::lance_access::LanceAccessContext::with_storage(&wrapped);
        let table = format!("{root_uri}/table");
        let (lance, table_path) = ObjectStore::from_uri_and_params(
            context.data_session().store_registry(),
            &table,
            &ObjectStoreParams::default(),
        )
        .await
        .unwrap();
        assert!(
            lance.has_direct_local_paths(),
            "without the mount the Lance store is the bare local root"
        );
        assert!(
            matches!(
                lance.inner.get(&table_path.join("a")).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "a base-only key is not there on the bare root"
        );
    }

    #[tokio::test]
    async fn a_scoped_read_through_registry_closes_with_its_scope() {
        let directory = tempfile::tempdir().unwrap();
        let (base_uri, root_uri) = fixture(directory.path());
        let base = std::path::Path::new(&base_uri);
        let scope = StorageIoScope::new();
        let scoped = storage_read_through_scoped(&base_uri, &root_uri, scope.clone())
            .await
            .unwrap();
        let context = crate::lance_access::LanceAccessContext::with_storage(scoped.as_ref());
        let table = format!("{root_uri}/table");
        let (lance, table_path) = ObjectStore::from_uri_and_params(
            context.data_session().store_registry(),
            &table,
            &ObjectStoreParams::default(),
        )
        .await
        .unwrap();
        assert!(!lance.has_direct_local_paths());
        let read = lance
            .inner
            .get(&table_path.clone().join("a"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(
            read.as_ref(),
            b"base a",
            "the scoped store reads through the mount"
        );
        lance
            .inner
            .put(&table_path.clone().join("c"), b"layer c".as_slice().into())
            .await
            .unwrap();
        scope.wait_idle().await;
        scope.close();
        assert!(
            lance
                .inner
                .put(&table_path.join("d"), b"late".as_slice().into())
                .await
                .is_err(),
            "a put after the scope closed is refused above the mount"
        );
        let root = std::path::Path::new(&root_uri);
        assert_eq!(std::fs::read(root.join("table/c")).unwrap(), b"layer c");
        assert!(!root.join("table/d").exists());
        assert_eq!(std::fs::read(base.join("table/a")).unwrap(), b"base a");
    }

    #[tokio::test]
    async fn lance_and_control_stores_of_one_root_share_the_mount() {
        let directory = tempfile::tempdir().unwrap();
        let (base_uri, root_uri) = fixture(directory.path());
        let base = std::path::Path::new(&base_uri);
        let root = std::path::Path::new(&root_uri);
        let root_uri = root_uri.as_str();

        let control = storage_read_through(&base_uri, root_uri).await.unwrap();
        let registry = registry(control.read_through_mount().unwrap(), None);
        let table = format!("{root_uri}/table");
        let (lance, table_path) =
            ObjectStore::from_uri_and_params(registry, &table, &ObjectStoreParams::default())
                .await
                .unwrap();
        assert!(!lance.has_direct_local_paths());

        let a = table_path.clone().join("a");
        let read = lance.inner.get(&a).await.unwrap().bytes().await.unwrap();
        assert_eq!(read.as_ref(), b"base a");
        lance.inner.delete(&a).await.unwrap();
        assert_eq!(
            control
                .read_text_if_exists(&format!("{table}/a"))
                .await
                .unwrap(),
            None,
            "a delete through the Lance store masks the object for the control adapter"
        );
        control.delete(&format!("{table}/b")).await.unwrap();
        assert!(
            lance
                .inner
                .get(&table_path.clone().join("b"))
                .await
                .is_err()
        );
        lance
            .inner
            .put(&table_path.join("c"), b"layer c".as_slice().into())
            .await
            .unwrap();
        assert_eq!(std::fs::read(root.join("table/c")).unwrap(), b"layer c");

        assert_eq!(std::fs::read(base.join("table/a")).unwrap(), b"base a");
        assert_eq!(std::fs::read(base.join("table/b")).unwrap(), b"base b");
        let reopened = storage_read_through(&base_uri, root_uri).await.unwrap();
        assert_eq!(
            reopened
                .list_dir(&table)
                .await
                .unwrap()
                .into_iter()
                .map(|uri| uri.rsplit('/').next().unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["c"],
            "a later adapter over the same root finds the same deletes and writes"
        );
    }

    #[tokio::test]
    async fn removing_a_directory_only_the_base_holds_hides_it_and_answers_not_found() {
        let directory = tempfile::tempdir().unwrap();
        let (base_uri, root_uri) = fixture(directory.path());
        let control = storage_read_through(&base_uri, &root_uri).await.unwrap();
        let registry = registry(control.read_through_mount().unwrap(), None);
        let table = format!("{root_uri}/table");
        let (lance, table_path) =
            ObjectStore::from_uri_and_params(registry, &table, &ObjectStoreParams::default())
                .await
                .unwrap();

        let removal = lance.remove_dir_all(table_path.join("sub")).await;

        assert!(
            matches!(removal, Err(lance_core::Error::NotFound { .. })),
            "the objects are deleted through the mount, then Lance removes the clone root's directory, which it never had: {removal:?}"
        );
        assert_eq!(
            control
                .read_text_if_exists(&format!("{table}/sub/c"))
                .await
                .unwrap(),
            None,
            "the base object under the directory is hidden"
        );
        let root = std::path::Path::new(&root_uri);
        assert!(root.join("__clone/deleted/table/sub/c").is_file());
        assert!(!root.join("table/sub").exists());
        assert_eq!(
            std::fs::read(std::path::Path::new(&base_uri).join("table/sub/c")).unwrap(),
            b"base c"
        );
    }
}
