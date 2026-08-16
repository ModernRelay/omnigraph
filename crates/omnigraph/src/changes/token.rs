//! Opaque continuation tokens and feed cursors.
//!
//! Three payload kinds share one envelope: `base64url_no_pad(json ‖ sha256)`.
//! A commit page token continues one bounded commit diff; a feed cursor is the
//! caller-owned durable position after a complete commit; a feed page token
//! continues one interrupted poll at its captured cut. Kind tags make every
//! cross-use a typed rejection instead of a silently misread position.
//!
//! Payload contents are private transport state. They never expose table
//! aliases, dataset paths, Lance versions, or row addresses; the numeric table
//! identity components ride only inside the checksummed opaque value.

use base64::Engine;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::model::{ChangeEntityKind, ChangeFeedScope, ChangeOpKind};
use crate::db::manifest::TableIdentity;
use crate::error::{OmniError, Result};

pub(crate) const TOKEN_VERSION: u8 = 1;
const TOKEN_CHECKSUM_BYTES: usize = 32;

pub(crate) const KIND_COMMIT_PAGE: &str = "commit-page";
pub(crate) const KIND_FEED_CURSOR: &str = "feed-cursor";
pub(crate) const KIND_FEED_PAGE: &str = "feed-page";

/// Cursor purpose for the v1 forward entity feed.
pub(crate) const FEED_PURPOSE: &str = "changes/forward";

pub(crate) fn cursor_rejected(reason: impl Into<String>) -> OmniError {
    OmniError::ChangeCursorRejected {
        reason: reason.into(),
    }
}

/// Continues one bounded commit entity diff. Binds the exact commit and the
/// canonical filter scope; carries no captured cut and no durable position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CommitPageTokenV1 {
    pub version: u8,
    pub kind: String,
    /// Hashed graph identity (schema identity domain), never the raw domain.
    pub graph_identity: String,
    pub commit_id: String,
    pub filter_digest: String,
    /// Opaque graph type identity of the last emitted change. A token's
    /// decodable payload never carries numeric table or incarnation
    /// components — the checksum is integrity, not encryption.
    pub type_id: String,
    pub id: String,
    pub operation_rank: u8,
    pub change_index: usize,
}

/// Durable caller-owned feed position: everything needed to prove the resumed
/// chain is the same graph, history incarnation, branch incarnation, and
/// filter contract, plus the last completed commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FeedCursorV1 {
    pub version: u8,
    pub kind: String,
    pub graph_identity: String,
    /// First-parent root of the captured chain — the graph-history incarnation.
    pub genesis_commit_id: String,
    pub purpose: String,
    /// Normalized branch name; `None` means main.
    pub branch: Option<String>,
    /// Hash of the branch's native incarnation witness. Fences same-name
    /// delete/recreate; main uses a fixed witness because it cannot be
    /// recreated.
    pub branch_witness: String,
    pub filter_digest: String,
    /// Last complete commit the caller consumed; the genesis id encodes
    /// "before everything" for a `Beginning` start.
    pub after_commit_id: String,
}

/// Continues one interrupted bounded poll: full cursor scope, the cut captured
/// when the poll began, and the in-commit continuation key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FeedPageTokenV1 {
    pub version: u8,
    pub kind: String,
    pub graph_identity: String,
    pub genesis_commit_id: String,
    pub purpose: String,
    pub branch: Option<String>,
    pub branch_witness: String,
    pub filter_digest: String,
    /// Head captured when this poll started; later commits stay outside it.
    pub cut_commit_id: String,
    /// Last complete commit before the split block.
    pub after_commit_id: String,
    /// The commit whose block this token splits.
    pub current_commit_id: String,
    /// Opaque graph type identity of the last emitted change.
    pub type_id: String,
    pub id: String,
    pub operation_rank: u8,
    pub change_index: usize,
}

fn kind_label(kind: &str) -> &'static str {
    match kind {
        KIND_COMMIT_PAGE => "commit changes page token",
        KIND_FEED_CURSOR => "change feed cursor",
        KIND_FEED_PAGE => "change feed page token",
        _ => "unrecognized continuation",
    }
}

/// Hash a raw identity string (graph identity domain or a branch witness
/// source) into its opaque URL-safe form.
pub(crate) fn hashed_identity(source: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(source.as_bytes()))
}

/// Opaque graph-scoped type identity: a projection of the persisted schema
/// identity domain plus the immutable table identity. Survives a supported
/// rename (identity unchanged) and changes after drop/re-add (new incarnation)
/// without revealing the raw numeric components.
pub(crate) fn opaque_type_id(schema_identity_domain: &str, identity: TableIdentity) -> String {
    let mut hasher = Sha256::new();
    hasher.update(schema_identity_domain.as_bytes());
    hasher.update([0x1f]);
    hasher.update(identity.stable_table_id.to_le_bytes());
    hasher.update([0x1f]);
    hasher.update(identity.table_incarnation_id.to_le_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize())
}

/// Canonical digest of one feed scope. Sorted and deduplicated per dimension,
/// with an explicit all-variants selection collapsing to the same digest as an
/// absent filter, so equivalent scopes always bind the same continuation.
pub(crate) fn filter_digest(scope: &ChangeFeedScope) -> String {
    fn canonical_kinds(kinds: &Option<Vec<ChangeEntityKind>>) -> Option<Vec<&'static str>> {
        let kinds = kinds.as_ref()?;
        let mut names: Vec<&'static str> = kinds
            .iter()
            .map(|kind| match kind {
                ChangeEntityKind::Node => "node",
                ChangeEntityKind::Edge => "edge",
            })
            .collect();
        names.sort_unstable();
        names.dedup();
        if names == ["edge", "node"] {
            return None;
        }
        Some(names)
    }

    fn canonical_ops(ops: &Option<Vec<ChangeOpKind>>) -> Option<Vec<&'static str>> {
        let ops = ops.as_ref()?;
        let mut names: Vec<&'static str> = ops
            .iter()
            .map(|op| match op {
                ChangeOpKind::Insert => "insert",
                ChangeOpKind::Update => "update",
                ChangeOpKind::Delete => "delete",
            })
            .collect();
        names.sort_unstable();
        names.dedup();
        if names == ["delete", "insert", "update"] {
            return None;
        }
        Some(names)
    }

    fn canonical_types(names: &Option<Vec<String>>) -> Option<Vec<String>> {
        let names = names.as_ref()?;
        let mut names = names.clone();
        names.sort_unstable();
        names.dedup();
        Some(names)
    }

    let canonical = serde_json::json!({
        "v": 1,
        "kinds": canonical_kinds(&scope.kinds),
        "ops": canonical_ops(&scope.ops),
        "types": canonical_types(&scope.type_names),
    });
    hashed_identity(&canonical.to_string())
}

pub(crate) fn encode_token<T: Serialize>(payload: &T) -> Result<String> {
    let payload = serde_json::to_vec(payload)
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    let digest = Sha256::digest(&payload);
    let mut encoded = Vec::with_capacity(payload.len() + digest.len());
    encoded.extend_from_slice(&payload);
    encoded.extend_from_slice(&digest);
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(encoded))
}

/// Decode one token, enforcing the checksum, the version, and the expected
/// kind. Every failure is the typed cursor rejection with a reason that
/// distinguishes caller-side corruption from cross-use.
fn decode_token<T: DeserializeOwned>(token: &str, expected_kind: &'static str) -> Result<T> {
    let expected = kind_label(expected_kind);
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| cursor_rejected(format!("invalid {expected} encoding")))?;
    if bytes.len() <= TOKEN_CHECKSUM_BYTES {
        return Err(cursor_rejected(format!("invalid {expected}")));
    }
    let (payload, checksum) = bytes.split_at(bytes.len() - TOKEN_CHECKSUM_BYTES);
    if Sha256::digest(payload).as_slice() != checksum {
        return Err(cursor_rejected(format!("invalid {expected} checksum")));
    }
    let value: serde_json::Value = serde_json::from_slice(payload)
        .map_err(|_| cursor_rejected(format!("invalid {expected} payload")))?;
    let actual_kind = value.get("kind").and_then(serde_json::Value::as_str);
    match actual_kind {
        Some(kind) if kind == expected_kind => {}
        Some(kind) => {
            return Err(cursor_rejected(format!(
                "a {} cannot be used as a {expected}",
                kind_label(kind)
            )));
        }
        None => return Err(cursor_rejected(format!("invalid {expected} payload"))),
    }
    let version = value.get("version").and_then(serde_json::Value::as_u64);
    if version != Some(u64::from(TOKEN_VERSION)) {
        return Err(cursor_rejected(format!("unsupported {expected} version")));
    }
    serde_json::from_value(value)
        .map_err(|_| cursor_rejected(format!("invalid {expected} payload")))
}

pub(crate) fn decode_commit_page_token(token: &str) -> Result<CommitPageTokenV1> {
    let decoded: CommitPageTokenV1 = decode_token(token, KIND_COMMIT_PAGE)?;
    if decoded.type_id.is_empty() || decoded.operation_rank > ChangeOpKind::Delete.rank() {
        return Err(cursor_rejected(
            "invalid commit changes page token identity",
        ));
    }
    Ok(decoded)
}

pub(crate) fn decode_feed_cursor(token: &str) -> Result<FeedCursorV1> {
    let decoded: FeedCursorV1 = decode_token(token, KIND_FEED_CURSOR)?;
    if decoded.purpose != FEED_PURPOSE {
        return Err(cursor_rejected(
            "change feed cursor purpose does not match this feed",
        ));
    }
    Ok(decoded)
}

pub(crate) fn decode_feed_page_token(token: &str) -> Result<FeedPageTokenV1> {
    let decoded: FeedPageTokenV1 = decode_token(token, KIND_FEED_PAGE)?;
    if decoded.purpose != FEED_PURPOSE {
        return Err(cursor_rejected(
            "change feed page token purpose does not match this feed",
        ));
    }
    if decoded.type_id.is_empty() || decoded.operation_rank > ChangeOpKind::Delete.rank() {
        return Err(cursor_rejected("invalid change feed page token identity"));
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit_page_token() -> CommitPageTokenV1 {
        CommitPageTokenV1 {
            version: TOKEN_VERSION,
            kind: KIND_COMMIT_PAGE.to_string(),
            graph_identity: hashed_identity("graph"),
            commit_id: "commit".to_string(),
            filter_digest: filter_digest(&ChangeFeedScope::default()),
            type_id: opaque_type_id(
                "graph",
                TableIdentity {
                    stable_table_id: 3,
                    table_incarnation_id: 7,
                },
            ),
            id: "alice".to_string(),
            operation_rank: 1,
            change_index: 41,
        }
    }

    fn feed_cursor() -> FeedCursorV1 {
        FeedCursorV1 {
            version: TOKEN_VERSION,
            kind: KIND_FEED_CURSOR.to_string(),
            graph_identity: hashed_identity("graph"),
            genesis_commit_id: "genesis".to_string(),
            purpose: FEED_PURPOSE.to_string(),
            branch: None,
            branch_witness: hashed_identity("main"),
            filter_digest: filter_digest(&ChangeFeedScope::default()),
            after_commit_id: "commit".to_string(),
        }
    }

    #[test]
    fn tokens_round_trip() {
        let token = commit_page_token();
        let decoded = decode_commit_page_token(&encode_token(&token).unwrap()).unwrap();
        assert_eq!(decoded, token);

        let cursor = feed_cursor();
        let decoded = decode_feed_cursor(&encode_token(&cursor).unwrap()).unwrap();
        assert_eq!(decoded, cursor);
    }

    #[test]
    fn checksum_and_encoding_corruption_are_typed() {
        let encoded = encode_token(&commit_page_token()).unwrap();
        // Flip one payload character; the checksum no longer matches.
        let mut corrupted = encoded.clone().into_bytes();
        corrupted[4] = if corrupted[4] == b'A' { b'B' } else { b'A' };
        let corrupted = String::from_utf8(corrupted).unwrap();
        let err = decode_commit_page_token(&corrupted).unwrap_err();
        assert!(
            matches!(err, OmniError::ChangeCursorRejected { .. }),
            "{err:?}"
        );

        let err = decode_commit_page_token("not base64 ***").unwrap_err();
        assert!(
            matches!(err, OmniError::ChangeCursorRejected { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn kind_cross_use_is_typed_with_both_names() {
        let cursor = encode_token(&feed_cursor()).unwrap();
        let err = decode_commit_page_token(&cursor).unwrap_err();
        let OmniError::ChangeCursorRejected { reason } = err else {
            panic!("expected rejection");
        };
        assert!(
            reason.contains("change feed cursor") && reason.contains("commit changes page token"),
            "{reason}"
        );

        let page = encode_token(&commit_page_token()).unwrap();
        let err = decode_feed_cursor(&page).unwrap_err();
        let OmniError::ChangeCursorRejected { reason } = err else {
            panic!("expected rejection");
        };
        assert!(
            reason.contains("commit changes page token") && reason.contains("change feed cursor"),
            "{reason}"
        );

        let err = decode_feed_page_token(&page).unwrap_err();
        assert!(matches!(err, OmniError::ChangeCursorRejected { .. }));
    }

    #[test]
    fn unsupported_versions_are_typed() {
        let mut token = commit_page_token();
        token.version = 2;
        let err = decode_commit_page_token(&encode_token(&token).unwrap()).unwrap_err();
        let OmniError::ChangeCursorRejected { reason } = err else {
            panic!("expected rejection");
        };
        assert!(reason.contains("version"), "{reason}");
    }

    #[test]
    fn filter_digest_canonicalizes_equivalent_scopes() {
        let empty = filter_digest(&ChangeFeedScope::default());
        let match_all = filter_digest(&ChangeFeedScope {
            kinds: Some(vec![ChangeEntityKind::Edge, ChangeEntityKind::Node]),
            type_names: None,
            ops: Some(vec![
                ChangeOpKind::Delete,
                ChangeOpKind::Insert,
                ChangeOpKind::Update,
            ]),
        });
        assert_eq!(empty, match_all);

        let unordered = filter_digest(&ChangeFeedScope {
            kinds: None,
            type_names: Some(vec!["Person".to_string(), "Company".to_string()]),
            ops: None,
        });
        let ordered_with_duplicate = filter_digest(&ChangeFeedScope {
            kinds: None,
            type_names: Some(vec![
                "Company".to_string(),
                "Person".to_string(),
                "Person".to_string(),
            ]),
            ops: None,
        });
        assert_eq!(unordered, ordered_with_duplicate);
        assert_ne!(empty, unordered);
    }

    #[test]
    fn opaque_type_ids_are_rename_stable_and_incarnation_fresh() {
        let identity = TableIdentity {
            stable_table_id: 5,
            table_incarnation_id: 9,
        };
        // Same domain + identity → same id regardless of any alias/name input
        // (names are simply not part of the projection).
        assert_eq!(
            opaque_type_id("domain", identity),
            opaque_type_id("domain", identity)
        );
        // A drop/re-add mints a new incarnation → a different opaque id.
        let reincarnated = TableIdentity {
            stable_table_id: 5,
            table_incarnation_id: 10,
        };
        assert_ne!(
            opaque_type_id("domain", identity),
            opaque_type_id("domain", reincarnated)
        );
        // A rebuilt graph (new identity domain) never collides.
        assert_ne!(
            opaque_type_id("domain", identity),
            opaque_type_id("other-domain", identity)
        );
    }
}
