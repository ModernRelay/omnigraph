//! Public surface of omnigraph_catalog as the engine exposes it.

pub use crate::db::snapshot::{Snapshot, SnapshotDataset, SnapshotScanner};
pub use crate::db::upgrade::{
    UpgradeFinding, UpgradeMode, UpgradeOptions, UpgradeOutcome, UpgradeRecovery, UpgradeReport,
    UpgradeWork, upgrade_storage, upgrade_storage_as,
};
pub(crate) use omnigraph_catalog::Snapshot as CatalogSnapshot;
pub(crate) use omnigraph_catalog::*;
pub use omnigraph_catalog::{
    DatasetEntry, DatasetUpdate, INTERNAL_MANIFEST_SCHEMA_VERSION,
    MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION, READ_REFRESH_POST_STATE_PRE_LINEAGE,
};
