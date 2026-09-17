use std::sync::{Arc, LazyLock};

use lance::dataset::{DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE};
use lance::io::ObjectStoreRegistry;
use lance::session::Session;

/// Process-wide Lance object-store registry.
///
/// Lance's registry owns the reusable client pool and keys stores by provider
/// identity plus storage parameters. The registry itself keeps weak references,
/// so sharing it does not keep unused stores alive.
static STORE_REGISTRY: LazyLock<Arc<ObjectStoreRegistry>> = LazyLock::new(|| {
    let registry = ObjectStoreRegistry::default();
    #[cfg(feature = "dst")]
    object_store_seam::hook(&registry);
    Arc::new(registry)
});

/// Control-plane session for `__manifest` and other mutable-tip metadata.
///
/// Its caches are deliberately disabled. Control paths still share the
/// process-wide object-store clients, but they cannot retain mutable-tip
/// metadata across branch-recreation boundaries.
static CONTROL_SESSION: LazyLock<Arc<Session>> =
    LazyLock::new(|| Arc::new(Session::new(0, 0, Arc::clone(&STORE_REGISTRY))));

/// The split Lance access context for one graph handle.
///
/// Data tables use a graph-scoped cached session. Control-plane metadata uses a
/// zero-cache session. Both share the same object-store registry/client pool.
#[derive(Clone)]
pub(crate) struct LanceAccessContext {
    data_session: Arc<Session>,
    control_session: Arc<Session>,
}

impl LanceAccessContext {
    pub(crate) fn new() -> Self {
        Self {
            data_session: Arc::new(Session::new(
                DEFAULT_INDEX_CACHE_SIZE,
                DEFAULT_METADATA_CACHE_SIZE,
                Arc::clone(&STORE_REGISTRY),
            )),
            control_session: Arc::clone(&CONTROL_SESSION),
        }
    }

    pub(crate) fn data_session(&self) -> Arc<Session> {
        Arc::clone(&self.data_session)
    }

    pub(crate) fn control_session(&self) -> Arc<Session> {
        Arc::clone(&self.control_session)
    }
}

pub(crate) fn control_session() -> Arc<Session> {
    Arc::clone(&CONTROL_SESSION)
}

/// DST seam (Lance-realm listing): the process-wide Lance object-store
/// registry, exposed so the DST harness can list a realm's keys through the
/// same provider Lance uses. Hidden: a test seam, not public API; production
/// never calls it.
#[doc(hidden)]
#[cfg_attr(not(feature = "dst"), allow(dead_code))]
pub fn store_registry() -> Arc<ObjectStoreRegistry> {
    Arc::clone(&STORE_REGISTRY)
}

/// The Lance-realm storage seam. Lance's registry caches the stores it
/// builds, so a decoration captured at construction would miss every cached
/// store and outlive its guard. The hook below therefore wraps the
/// `shared-memory` and `file` providers once, at registry construction, with
/// a store that reads the seam on EVERY call and hands the call to whatever
/// the installed decorator returns for the base store.
#[cfg(feature = "dst")]
pub mod object_store_seam {
    use std::fmt;
    use std::ops::Range;
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use lance::io::{ObjectStoreParams, ObjectStoreRegistry};
    use lance_io::object_store::{ObjectStore as LanceObjectStore, ObjectStoreProvider};
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    };
    use omnigraph_seams::{Behavior, Global, Op, Seam};
    use url::Url;

    /// What the seam holds: a decorator over the base store Lance built. It
    /// is asked on every call, so it must be cheap and must not capture
    /// per-call state in the base.
    pub trait DecorateObjectStore: Behavior + Send + Sync {
        fn wrap(&self, base: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore>;
    }

    /// The Lance-realm seam.
    pub static OBJECT_STORE: Seam<dyn DecorateObjectStore, Global<dyn DecorateObjectStore>> =
        Seam::new("object_store", Op::Unreachable, Global::new());

    /// The schemes whose stores route through the seam. On `file` roots Lance
    /// reads and writes data files through direct local paths that bypass the
    /// wrapped store, so that lane sees manifest and listing traffic only.
    const HOOKED_SCHEMES: [&str; 2] = ["shared-memory", "file"];

    /// # Panics
    ///
    /// When Lance's default registry lacks a hooked scheme: the seam would
    /// otherwise be silently unhooked after a Lance upgrade.
    pub(super) fn hook(registry: &ObjectStoreRegistry) {
        for scheme in HOOKED_SCHEMES {
            let inner = registry
                .get_provider(scheme)
                .unwrap_or_else(|| panic!("Lance registry has no `{scheme}` provider to hook"));
            registry.insert(scheme, Arc::new(HookedProvider { inner }));
        }
    }

    /// Same store, same path and prefix semantics; the constructed store's
    /// `inner` becomes a [`HookedStore`].
    #[derive(Debug)]
    struct HookedProvider {
        inner: Arc<dyn ObjectStoreProvider>,
    }

    #[async_trait]
    impl ObjectStoreProvider for HookedProvider {
        async fn new_store(
            &self,
            base_path: Url,
            params: &ObjectStoreParams,
        ) -> lance_core::Result<LanceObjectStore> {
            let mut store = self.inner.new_store(base_path, params).await?;
            store.inner = Arc::new(HookedStore {
                base: Arc::clone(&store.inner),
            });
            Ok(store)
        }

        fn extract_path(&self, url: &Url) -> lance_core::Result<Path> {
            self.inner.extract_path(url)
        }

        fn calculate_object_store_prefix(
            &self,
            url: &Url,
            storage_options: Option<&std::collections::HashMap<String, String>>,
        ) -> lance_core::Result<String> {
            self.inner
                .calculate_object_store_prefix(url, storage_options)
        }
    }

    /// Forwards every call through the seam (module doc).
    #[derive(Debug)]
    struct HookedStore {
        base: Arc<dyn ObjectStore>,
    }

    impl HookedStore {
        fn current(&self) -> Arc<dyn ObjectStore> {
            OBJECT_STORE
                .with(|decorator| decorator.wrap(Arc::clone(&self.base)))
                .unwrap_or_else(|| Arc::clone(&self.base))
        }
    }

    impl fmt::Display for HookedStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Seamed({})", self.base)
        }
    }

    #[async_trait]
    impl ObjectStore for HookedStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.current().put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.current().put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.current().get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            self.current().get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.current().delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.current().list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.current().list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.current().list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.current().copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> object_store::Result<()> {
            self.current().rename_opts(from, to, options).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_and_control_sessions_share_only_the_store_registry() {
        let first = LanceAccessContext::new();
        let second = LanceAccessContext::new();

        let first_data = first.data_session();
        let second_data = second.data_session();
        let control = first.control_session();

        assert!(!Arc::ptr_eq(&first_data, &control));
        assert!(!Arc::ptr_eq(&first_data, &second_data));
        assert!(Arc::ptr_eq(
            &first_data.store_registry(),
            &control.store_registry()
        ));
        assert!(Arc::ptr_eq(
            &first_data.store_registry(),
            &second_data.store_registry()
        ));
    }
}
