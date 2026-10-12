//! The row keys of `__manifest` stamps 5 to 13. A registration or tombstone key
//! carries its identity and a trailing twenty-digit clock (the Lance data
//! version at stamps 5 and 6, the `__manifest` version that wrote the row from
//! stamp 7); a branch head is keyed by its logical name.

use crate::TableIdentity;
use crate::error::{OmniError, Result};

pub(crate) const GRAPH_HEAD_OBJECT_ID_PREFIX: &str = "graph_head:";

#[cfg(any(test, feature = "test-util"))]
fn format_clock(clock: u64) -> String {
    format!("{clock:020}")
}

#[cfg(any(test, feature = "test-util"))]
pub(crate) fn version_object_id(identity: TableIdentity, clock: u64) -> String {
    format!(
        "{}:{:016x}:{:016x}:{}",
        super::OBJECT_TYPE_TABLE_VERSION,
        identity.stable_table_id,
        identity.table_incarnation_id,
        format_clock(clock)
    )
}

#[cfg(any(test, feature = "test-util"))]
pub(crate) fn tombstone_object_id(identity: TableIdentity, clock: u64) -> String {
    format!(
        "{}:{:016x}:{:016x}:{}",
        super::OBJECT_TYPE_TABLE_TOMBSTONE,
        identity.stable_table_id,
        identity.table_incarnation_id,
        format_clock(clock)
    )
}

/// The `graph_head` key of a branch, `graph_head:main` for main.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn graph_head_object_id(logical_branch: Option<&str>) -> String {
    format!(
        "{GRAPH_HEAD_OBJECT_ID_PREFIX}{}",
        logical_branch.unwrap_or(crate::MAIN_BRANCH_HEAD_KEY)
    )
}

/// The clock a registration or tombstone key carries, read from the
/// already-projected `object_id`.
pub(crate) fn manifest_version_from_object_id(
    object_id: &str,
    identity: TableIdentity,
    object_type: &str,
) -> Result<u64> {
    let prefix = format!(
        "{object_type}:{:016x}:{:016x}:",
        identity.stable_table_id, identity.table_incarnation_id
    );
    object_id
        .strip_prefix(&prefix)
        .filter(|suffix| suffix.len() == 20 && suffix.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|suffix| suffix.parse::<u64>().ok())
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "manifest {object_type} row has object_id '{object_id}', expected '{prefix}<manifest version>'"
            ))
        })
}
