//! Public surface of omnigraph_core::storage as the engine exposes it.

#[cfg(feature = "dst")]
pub use omnigraph_core::storage::DecorateStorage;
#[cfg(feature = "dst")]
pub use omnigraph_core::storage::STORAGE;
pub(crate) use omnigraph_core::storage::*;
pub use omnigraph_core::storage::{
    ListDirBounds, ObjectStorageAdapter, StorageAdapter, StorageIoScope, StorageKind, join_uri,
    normalize_root_uri, redacted_storage_uri, storage_for_uri, storage_for_uri_scoped,
    storage_kind_for_uri,
};
