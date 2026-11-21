//! Public surface of omnigraph_catalog as the engine exposes it.

pub use crate::db::snapshot::{Snapshot, SnapshotDataset, SnapshotScanner};
pub use crate::db::upgrade::{
    UpgradeFinding, UpgradeMode, UpgradeOptions, UpgradeOutcome, UpgradeRecovery, UpgradeReport,
    upgrade_storage, upgrade_storage_as,
};
pub(crate) use omnigraph_catalog::Snapshot as CatalogSnapshot;
pub(crate) use omnigraph_catalog::*;
pub use omnigraph_catalog::{
    BRANCH_CREATE_POST_ROW, BRANCH_CREATE_POST_TREE, BRANCH_DELETE_POST_RETIRED_ROW,
    BRANCH_DELETE_POST_TOMBSTONE, BRANCH_DELETE_PRE_TOMBSTONE, DatasetEntry, DatasetUpdate,
    INTERNAL_MANIFEST_SCHEMA_VERSION, MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION,
    READ_REFRESH_POST_STATE_PRE_LINEAGE,
};
