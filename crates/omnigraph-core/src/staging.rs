use std::sync::Arc;

use lance::Dataset;
use lance::dataset::transaction::Transaction;
use serde::{Deserialize, Serialize};

use crate::error::{OmniError, Result};
use crate::graph_commit_id::is_valid_graph_commit_id;

/// Stable identity of one Lance transaction.
///
/// Lance persists both fields in the transaction file referenced by the
/// committed manifest. The pair, rather than a numeric table version alone,
/// proves that an observed version was produced by a given staged effect.
/// The UUID distinguishes two writers that started from the same version.
/// Lance may preserve both fields while rebasing, so callers of the exact
/// linear commit must also require the achieved table version to be exactly
/// `read_version + 1`.
///
/// RFC 0067 reads the same pair from the transaction file name the manifest
/// records (`{read_version}-{uuid}.txn`, pinned in `lance_surface_guards`),
/// so identifying a pin's linear twin or following a chain of detached
/// commits costs no request beyond the manifest itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedTransactionIdentity {
    pub read_version: u64,
    pub uuid: String,
}

impl From<&Transaction> for StagedTransactionIdentity {
    fn from(transaction: &Transaction) -> Self {
        Self {
            read_version: transaction.read_version,
            uuid: transaction.uuid.clone(),
        }
    }
}

impl StagedTransactionIdentity {
    /// The identity a manifest records, or `None` when it names no
    /// transaction file or the name has an unrecognized shape.
    pub fn recorded_by(dataset: &Dataset) -> Option<Self> {
        Self::from_transaction_file_name(dataset.manifest().transaction_file.as_deref()?)
    }

    /// The identity a manifest's transaction file name
    /// (`{read_version}-{uuid}.txn`) records.
    fn from_transaction_file_name(name: &str) -> Option<Self> {
        let (read_version, uuid) = name.strip_suffix(".txn")?.split_once('-')?;
        Some(Self {
            read_version: read_version.parse().ok()?,
            uuid: uuid.to_string(),
        })
    }

    /// Whether the transaction was staged on a detached version, which is
    /// how a chain of detached commits links itself without manifest history.
    pub fn base_is_detached(&self) -> bool {
        is_detached_version(self.read_version)
    }
}

/// One commit of a chain of detached commits, read from its manifest file
/// alone: the identity its transaction file name records and the transaction
/// stored inline in the same file.
#[derive(Debug, Clone)]
pub struct DetachedChainLink {
    pub identity: StagedTransactionIdentity,
    pub transaction: Transaction,
}

impl DetachedChainLink {
    /// Read detached `version` of `dataset`'s branch with one whole-object
    /// GET of its manifest, decoding the manifest and its inline transaction
    /// from those bytes.
    ///
    /// Opening the version (`Dataset::checkout_version`) and then
    /// `Dataset::read_transaction` costs five dependent requests on a
    /// detached version, whose location Lance resolves without a size: a
    /// HEAD and a tail read to open the manifest, then a HEAD and range reads
    /// of the same file for the inline transaction. A chain walk needs only
    /// the transaction and the identity, so this reads the file once.
    ///
    /// The framing is Lance's own (`lance_table::io::manifest::read_manifest`
    /// and `Dataset::read_transaction`, pinned by
    /// `lance_surface_guards::detached_manifest_file_holds_its_transaction_inline`):
    /// a 16-byte footer whose first 8 bytes are the manifest's offset and
    /// whose last 4 are `MAGIC`, a length-prefixed `pb::Manifest` ending at
    /// the footer, and the length-prefixed `pb::Transaction` at the
    /// manifest's `transaction_section`. Returns `Ok(None)` for every other
    /// shape (no inline transaction, another version or branch, a framing
    /// that does not add up), so the caller can take Lance's own route.
    pub async fn read(dataset: &Dataset, version: u64) -> lance::Result<Option<Self>> {
        use lance_io::utils::read_message_from_buf;
        use lance_table::format::{MAGIC, pb};
        use lance_table::io::commit::ManifestNamingScheme;
        use object_store::ObjectStoreExt as _;

        if !is_detached_version(version) {
            return Ok(None);
        }
        let store = dataset.object_store(None).await?;
        let path = ManifestNamingScheme::V2.manifest_path(&dataset.branch_location().path, version);
        let bytes = store.inner.get(&path).await?.bytes().await?;
        let Some(footer_start) = bytes.len().checked_sub(16) else {
            return Ok(None);
        };
        if !bytes.ends_with(MAGIC) {
            return Ok(None);
        }
        let Ok(manifest_pos) = usize::try_from(u64::from_le_bytes(
            bytes[footer_start..footer_start + 8]
                .try_into()
                .expect("an 8-byte slice"),
        )) else {
            return Ok(None);
        };
        let Some(manifest) = length_prefixed_message(&bytes, manifest_pos, footer_start, true)
        else {
            return Ok(None);
        };
        let manifest = read_message_from_buf::<pb::Manifest>(&manifest)?;
        if manifest.version != version || manifest.branch != dataset.manifest().branch {
            return Ok(None);
        }
        let Some(identity) = Self::identity_of(&manifest) else {
            return Ok(None);
        };
        let Some(transaction_pos) = manifest
            .transaction_section
            .and_then(|pos| usize::try_from(pos).ok())
        else {
            return Ok(None);
        };
        let Some(transaction) =
            length_prefixed_message(&bytes, transaction_pos, manifest_pos, false)
        else {
            return Ok(None);
        };
        let transaction = read_message_from_buf::<pb::Transaction>(&transaction)?;
        Ok(Some(Self {
            identity,
            transaction: Transaction::try_from(transaction)?,
        }))
    }

    fn identity_of(
        manifest: &lance_table::format::pb::Manifest,
    ) -> Option<StagedTransactionIdentity> {
        if manifest.transaction_file.is_empty() {
            return None;
        }
        StagedTransactionIdentity::from_transaction_file_name(&manifest.transaction_file)
    }
}

/// The bytes of the message Lance writes at `pos`, a little-endian `u32`
/// length then the body: the body must end exactly at `end` when `exact`,
/// and at or before it otherwise. `None` when the framing does not fit, so a
/// malformed offset never slices out of bounds.
fn length_prefixed_message(
    bytes: &bytes::Bytes,
    pos: usize,
    end: usize,
    exact: bool,
) -> Option<bytes::Bytes> {
    if end > bytes.len() {
        return None;
    }
    let body_start = pos.checked_add(4).filter(|start| *start <= end)?;
    let length = u32::from_le_bytes(bytes[pos..body_start].try_into().ok()?);
    let body_end = body_start.checked_add(usize::try_from(length).ok()?)?;
    if body_end > end || (exact && body_end != end) {
        return None;
    }
    Some(bytes.slice(pos..body_end))
}

/// Whether a Lance version id names a detached version (RFC 0067).
pub fn is_detached_version(version: u64) -> bool {
    version & lance_table::format::DETACHED_VERSION_MASK != 0
}

/// Transaction property naming the graph branch incarnation a detached
/// commit was staged against (detached-only RFC §Garbage collection).
pub const STAGED_AGAINST_BRANCH_INCARNATION: &str = "omnigraph.staged_against_branch_incarnation";
/// Transaction property naming the logical graph head a detached commit was
/// staged against; empty when the branch had no materialized head.
pub const STAGED_AGAINST_GRAPH_HEAD: &str = "omnigraph.staged_against_graph_head";

/// The publication authority a detached commit was staged against: the
/// branch incarnation and the logical graph head its publish compares and
/// swaps on. Written into the staged transaction's properties so the
/// collector reads a manifest's owner from the manifest alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagingWitness {
    branch_incarnation: String,
    graph_head: Option<String>,
}

impl StagingWitness {
    pub fn new(
        identifier: &lance::dataset::refs::BranchIdentifier,
        graph_head: Option<&str>,
    ) -> Result<Self> {
        let branch_incarnation = serde_json::to_string(identifier).map_err(|error| {
            OmniError::manifest_internal(format!("branch identifier is not serializable: {error}"))
        })?;
        Ok(Self {
            branch_incarnation,
            graph_head: graph_head.map(str::to_string),
        })
    }

    pub fn branch_incarnation(&self) -> &str {
        &self.branch_incarnation
    }

    pub fn graph_head(&self) -> Option<&str> {
        self.graph_head.as_deref()
    }

    /// The witness a manifest's transaction records; `None` when a writer
    /// from before the witness staged it, or its authority is malformed.
    pub fn from_transaction(transaction: &Transaction) -> Option<Self> {
        let properties = transaction.transaction_properties.as_deref()?;
        let branch_incarnation = properties.get(STAGED_AGAINST_BRANCH_INCARNATION)?.clone();
        let identifier: lance::dataset::refs::BranchIdentifier =
            serde_json::from_str(&branch_incarnation).ok()?;
        if identifier == lance::dataset::refs::BranchIdentifier::missing_identifier_sentinel()
            || serde_json::to_string(&identifier).ok()? != branch_incarnation
            || identifier.version_mapping.iter().any(|(_, uuid)| {
                uuid.len() != 32
                    || uuid.bytes().all(|byte| byte == b'0')
                    || !uuid
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
        {
            return None;
        }
        let recorded_head = properties.get(STAGED_AGAINST_GRAPH_HEAD)?;
        let graph_head = if recorded_head.is_empty() {
            None
        } else {
            if !is_valid_graph_commit_id(recorded_head) {
                return None;
            }
            Some(recorded_head.clone())
        };
        Some(Self {
            branch_incarnation,
            graph_head,
        })
    }

    pub fn stamp(&self, transaction: &mut Transaction) {
        let mut properties = transaction
            .transaction_properties
            .as_deref()
            .cloned()
            .unwrap_or_default();
        properties.insert(
            STAGED_AGAINST_BRANCH_INCARNATION.to_string(),
            self.branch_incarnation.clone(),
        );
        properties.insert(
            STAGED_AGAINST_GRAPH_HEAD.to_string(),
            self.graph_head.clone().unwrap_or_default(),
        );
        transaction.transaction_properties = Some(Arc::new(properties));
    }
}

#[cfg(test)]
mod tests {
    use super::length_prefixed_message;
    use bytes::Bytes;

    fn framed(body: &[u8]) -> Vec<u8> {
        let mut out = u32::try_from(body.len()).unwrap().to_le_bytes().to_vec();
        out.extend_from_slice(body);
        out
    }

    #[test]
    fn length_prefixed_message_takes_exactly_the_framed_body() {
        let mut file = vec![9_u8; 3];
        file.extend(framed(b"abc"));
        let bytes = Bytes::from(file);
        assert_eq!(
            length_prefixed_message(&bytes, 3, 10, true).as_deref(),
            Some(&framed(b"abc")[..])
        );
        assert!(length_prefixed_message(&bytes, 3, 10, false).is_some());
        let mut padded = bytes.to_vec();
        padded.push(0);
        let padded = Bytes::from(padded);
        assert!(
            length_prefixed_message(&padded, 3, 11, true).is_none(),
            "an exact message must end at the bound"
        );
        assert!(length_prefixed_message(&padded, 3, 11, false).is_some());
    }

    #[test]
    fn length_prefixed_message_refuses_a_frame_that_does_not_fit() {
        let bytes = Bytes::from(framed(b"abc"));
        assert!(
            length_prefixed_message(&bytes, 0, 6, false).is_none(),
            "body past the bound"
        );
        assert!(
            length_prefixed_message(&bytes, 4, 7, false).is_none(),
            "no room for a length"
        );
        assert!(length_prefixed_message(&bytes, usize::MAX, 7, false).is_none());
        assert!(
            length_prefixed_message(&bytes, 0, 8, false).is_none(),
            "bound past the bytes"
        );
        let huge = Bytes::from(u32::MAX.to_le_bytes().to_vec());
        assert!(
            length_prefixed_message(&huge, 0, 4, false).is_none(),
            "length past the end"
        );
    }
}
