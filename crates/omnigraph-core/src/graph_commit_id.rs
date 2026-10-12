//! Addressable graph commit identities and canonical public-ID validation.

use std::fmt;

use ulid::Ulid;

use crate::error::{OmniError, Result};

/// The slot ceiling of one history block. A block closes on a byte budget
/// long before it, so the ceiling only bounds a buffer of tiny commits.
pub const HISTORY_BLOCK_SLOTS: u16 = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryBlockId {
    pub block: Ulid,
    pub slot: u16,
    pub nonce: Ulid,
}

impl HistoryBlockId {
    pub fn new(block: Ulid, slot: u16, nonce: Ulid) -> Result<Self> {
        if slot >= HISTORY_BLOCK_SLOTS {
            return Err(invalid_id());
        }
        Ok(Self { block, slot, nonce })
    }
}

impl fmt::Display for HistoryBlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "hb1.{}.{}.{}", self.block, self.slot, self.nonce)
    }
}

/// The ULID `text` spells in its one canonical form, or `None`.
pub fn canonical_ulid(text: &str) -> Option<Ulid> {
    let id = Ulid::from_string(text).ok()?;
    (id.to_string() == text).then_some(id)
}

fn invalid_id() -> OmniError {
    OmniError::manifest_internal("invalid canonical history-block commit id")
}

/// Decode an addressable ID; other IDs have no block address.
/// Malformed IDs in the reserved `hb1` namespace are errors.
pub fn parse_history_block_id(text: &str) -> Result<Option<HistoryBlockId>> {
    if text != "hb1" && !text.starts_with("hb1.") {
        return Ok(None);
    }
    let mut parts = text.split('.');
    let _ = parts.next();
    let block = parts
        .next()
        .and_then(canonical_ulid)
        .ok_or_else(invalid_id)?;
    let slot_text = parts.next().ok_or_else(invalid_id)?;
    let slot: u16 = slot_text.parse().map_err(|_| invalid_id())?;
    if slot.to_string() != slot_text || parts.clone().count() != 1 {
        return Err(invalid_id());
    }
    let nonce = parts
        .next()
        .and_then(canonical_ulid)
        .ok_or_else(invalid_id)?;
    HistoryBlockId::new(block, slot, nonce).map(Some)
}

/// Whether the published commit `commit_id` is the one `requested` names:
/// the same ID, or the addressable ID a publish gave the intent nonce `requested`.
pub fn commit_id_answers(commit_id: &str, requested: &str) -> Result<bool> {
    Ok(commit_id == requested
        || parse_history_block_id(commit_id)?.is_some_and(|id| id.nonce.to_string() == requested))
}

/// The intent nonce of the published commit `commit_id`: an `hb1` id's nonce,
/// a canonical ULID itself; any other id has none and is refused.
pub fn intent_nonce(commit_id: &str) -> Result<String> {
    if let Some(id) = parse_history_block_id(commit_id)? {
        return Ok(id.nonce.to_string());
    }
    canonical_ulid(commit_id)
        .map(|nonce| nonce.to_string())
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("commit id {commit_id} carries no intent nonce"))
        })
}

/// Public snapshot IDs are canonical ULIDs or canonical addressable IDs.
pub fn is_valid_graph_commit_id(text: &str) -> bool {
    canonical_ulid(text).is_some() || matches!(parse_history_block_id(text), Ok(Some(_)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_ids_have_one_canonical_spelling() {
        let block = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        for slot in 0..HISTORY_BLOCK_SLOTS {
            let text = format!("hb1.{block}.{slot}.{block}");
            assert_eq!(
                parse_history_block_id(&text).unwrap().unwrap().to_string(),
                text
            );
            assert!(is_valid_graph_commit_id(&text));
        }
        for text in [
            format!("hb1.{block}.{HISTORY_BLOCK_SLOTS}.{block}"),
            format!("hb1.{block}.65536.{block}"),
            format!("hb1.{block}.00.{block}"),
            format!("hb1.{block}.+1.{block}"),
            format!("hb1.{block}.1.{block}.extra"),
            format!("hb1.{block}.1.{}", block.to_lowercase()),
            format!("hb1.{block}/x.1.{block}"),
            "hb1".to_string(),
        ] {
            assert!(parse_history_block_id(&text).is_err(), "{text}");
            assert!(!is_valid_graph_commit_id(&text));
        }
        assert!(is_valid_graph_commit_id(block));
        assert!(!is_valid_graph_commit_id(&block.to_lowercase()));
        assert!(!is_valid_graph_commit_id("fixture"));
        assert_eq!(parse_history_block_id("fixture").unwrap(), None);
    }

    #[test]
    fn intent_nonce_reads_the_nonce_a_publish_addressed() {
        let block = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let nonce = "01BX5ZZKBKACTAV9WEVGEMMVRZ";
        for (commit_id, expected) in [
            (format!("hb1.{block}.0.{nonce}"), Some(nonce)),
            (format!("hb1.{block}.16383.{nonce}"), Some(nonce)),
            (nonce.to_string(), Some(nonce)),
            (format!("hb1.{block}.00.{nonce}"), None),
            (nonce.to_lowercase(), None),
            ("fixture".to_string(), None),
            ("hb1".to_string(), None),
        ] {
            assert_eq!(
                intent_nonce(&commit_id).ok().as_deref(),
                expected,
                "{commit_id}"
            );
        }
    }
}
