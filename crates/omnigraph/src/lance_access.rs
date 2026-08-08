use std::sync::{Arc, LazyLock};

use lance::dataset::{DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE};
use lance::io::ObjectStoreRegistry;
use lance::session::Session;

/// Process-wide Lance object-store registry.
///
/// Lance's registry owns the reusable client pool and keys stores by provider
/// identity plus storage parameters. The registry itself keeps weak references,
/// so sharing it does not keep unused stores alive.
static STORE_REGISTRY: LazyLock<Arc<ObjectStoreRegistry>> =
    LazyLock::new(|| Arc::new(ObjectStoreRegistry::default()));

/// Control-plane session for `__manifest` and other mutable-tip metadata.
///
/// Its caches are deliberately disabled. Control paths still share the
/// process-wide object-store clients, but they cannot retain mutable-tip
/// metadata across Restore / branch-recreation boundaries.
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
