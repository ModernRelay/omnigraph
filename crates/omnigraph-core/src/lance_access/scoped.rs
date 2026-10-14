//! Lifetime-scoped clients; ordinary embedded sessions keep their existing defaults.

use std::sync::Arc;

use async_trait::async_trait;
use lance::io::{ObjectStoreParams, ObjectStoreRegistry, StorageOptionsAccessor};
use lance_io::object_store::{ObjectStore, ObjectStoreProvider};
use object_store::path::Path;
use omnigraph_storage::StorageIoScope;
use url::Url;

pub(super) fn registry(scope: StorageIoScope) -> Arc<ObjectStoreRegistry> {
    let registry = ObjectStoreRegistry::default();
    #[cfg(feature = "dst")]
    super::object_store_seam::hook(&registry);
    for scheme in ["file", "file-object-store", "s3", "az", "abfss"] {
        if let Some(inner) = registry.get_provider(scheme) {
            registry.insert(
                scheme,
                Arc::new(ScopedProvider {
                    inner,
                    scope: scope.clone(),
                }),
            );
        }
    }
    Arc::new(registry)
}

#[derive(Debug)]
struct ScopedProvider {
    inner: Arc<dyn ObjectStoreProvider>,
    scope: StorageIoScope,
}

#[async_trait]
impl ObjectStoreProvider for ScopedProvider {
    async fn new_store(
        &self,
        mut base_path: Url,
        params: &ObjectStoreParams,
    ) -> lance_core::Result<ObjectStore> {
        // Lance applies this environment override after explicit parameters.
        // Refuse it rather than silently losing the single-attempt guarantee.
        if std::env::var("OBJECT_STORE_CLIENT_MAX_RETRIES")
            .is_ok_and(|value| value.parse::<usize>() != Ok(0))
        {
            return Err(lance_core::Error::invalid_input(
                "lifetime-scoped storage requires OBJECT_STORE_CLIENT_MAX_RETRIES=0 or unset",
            ));
        }
        let mut params = params.clone();
        if params
            .storage_options_accessor
            .as_ref()
            .is_some_and(|accessor| accessor.has_provider())
        {
            return Err(lance_core::Error::invalid_input(
                "lifetime-scoped storage requires fixed client options; native credential refresh remains supported",
            ));
        }
        let mut options = params.storage_options().cloned().unwrap_or_default();
        if options
            .get("use_opendal")
            .is_some_and(|value| value == "true")
        {
            return Err(lance_core::Error::invalid_input(
                "lifetime-scoped storage requires the native object-store provider",
            ));
        }
        options.retain(|key, _| !key.eq_ignore_ascii_case("client_max_retries"));
        options.insert("client_max_retries".into(), "0".into());
        options.insert("lance_aimd_max_retries".into(), "0".into());
        params.storage_options_accessor = Some(Arc::new(
            StorageOptionsAccessor::with_static_options(options),
        ));
        // Lance's optimized file writer bypasses ObjectStore. Its existing
        // object-store file provider preserves paths while keeping writes owned.
        if base_path.scheme() == "file" {
            base_path = Url::parse(
                &base_path
                    .as_str()
                    .replacen("file:", "file-object-store:", 1),
            )
            .map_err(|_| lance_core::Error::invalid_input("could not scope the local store"))?;
        }
        let mut store = self.inner.new_store(base_path, &params).await?;
        store.inner = self.scope.wrap_object_store(store.inner);
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

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;

    #[tokio::test]
    async fn closed_lifetime_rejects_cached_and_new_store_writes() {
        let directory = tempfile::tempdir().unwrap();
        let scope = StorageIoScope::new();
        let registry = registry(scope.clone());
        let first_uri = directory.path().join("first").to_str().unwrap().to_owned();
        let (first, first_path) = ObjectStore::from_uri_and_params(
            registry.clone(),
            &first_uri,
            &ObjectStoreParams::default(),
        )
        .await
        .unwrap();
        assert!(!first.has_direct_local_paths());
        first
            .inner
            .put(&first_path, b"settled".as_slice().into())
            .await
            .unwrap();
        scope.wait_idle().await;
        assert!(!scope.is_uncertain());
        scope.close();
        assert!(
            first
                .inner
                .put(&first_path, b"late".as_slice().into())
                .await
                .is_err()
        );
        let second_uri = directory.path().join("second").to_str().unwrap().to_owned();
        let (second, second_path) =
            ObjectStore::from_uri_and_params(registry, &second_uri, &ObjectStoreParams::default())
                .await
                .unwrap();
        assert!(
            second
                .inner
                .put(&second_path, b"late".as_slice().into())
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(&first_uri).unwrap(), b"settled");
        assert!(!std::path::Path::new(&second_uri).exists());
    }
}
