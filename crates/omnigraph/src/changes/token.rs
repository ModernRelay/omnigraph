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
/// Continuations commonly travel in one URL query parameter. Keep their
/// encoded representation bounded even when a caller supplied a pathological
/// logical key (or an oversized, checksummed value back to the decoder).
pub(crate) const TOKEN_MAX_ENCODED_BYTES: usize = 4 * 1024;

/// Small IDs ride exactly in a continuation, preserving the normal
/// `id > last_id` seek. Large IDs use a fixed-size prefix plus SHA-256 instead
/// of making the token grow with the row key. The prefix is long enough that a
/// digest resume only scans a narrow equal-prefix range in the exceptional
/// large-ID case.
pub(crate) const CONTINUATION_EXACT_ID_MAX_BYTES: usize = 256;
const CONTINUATION_ID_PREFIX_MAX_BYTES: usize = 64;

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

/// Fixed-size binding for the normalized branch in a feed continuation.
///
/// Branch names have no logical length ceiling, so carrying the raw name would
/// make an otherwise bounded cursor grow with caller input. The domain tag and
/// explicit main/named discriminator keep `main` distinct from every named
/// branch while the SHA-256 projection makes the wire representation constant
/// size. The current request still supplies the exact normalized branch name;
/// validation projects it through the same function and compares the complete
/// digest before any position is accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct BranchScopeV1(String);

impl BranchScopeV1 {
    pub(crate) fn for_branch(branch: Option<&str>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"omnigraph/change-feed-branch-scope/v1\0");
        match branch {
            None => hasher.update([0]),
            Some(branch) => {
                hasher.update([1]);
                hasher.update(branch.as_bytes());
            }
        }
        Self(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize()))
    }

    fn validate(&self, expected: &str) -> Result<()> {
        let digest = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&self.0)
            .map_err(|_| cursor_rejected(format!("invalid {expected} branch scope")))?;
        if digest.len() != TOKEN_CHECKSUM_BYTES
            || base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&digest) != self.0
        {
            return Err(cursor_rejected(format!("invalid {expected} branch scope")));
        }
        Ok(())
    }
}

/// Bounded representation of the last emitted logical ID.
///
/// `Exact` is the hot path: enumeration can push `id > value` into Lance.
/// `Digest` is reserved for IDs above the fixed exact cap. On resume the
/// enumerator starts after the bounded prefix, resolves the one matching
/// SHA-256 inside that prefix range, rejects zero/multiple matches, and then
/// resumes after the resolved exact ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum IdPositionV1 {
    Exact { id: String },
    Digest { prefix: String, sha256: String },
}

impl IdPositionV1 {
    pub(crate) fn for_id(id: &str) -> Self {
        if id.len() <= CONTINUATION_EXACT_ID_MAX_BYTES {
            return Self::Exact { id: id.to_string() };
        }
        let mut prefix_end = CONTINUATION_ID_PREFIX_MAX_BYTES.min(id.len());
        while !id.is_char_boundary(prefix_end) {
            prefix_end -= 1;
        }
        Self::Digest {
            prefix: id[..prefix_end].to_string(),
            sha256: hashed_identity(id),
        }
    }

    /// Lower bound for the ordered scan. Exact positions skip the already
    /// emitted row directly; digest positions begin at their bounded prefix
    /// and are resolved by the enumerator before any response is published.
    pub(crate) fn scan_after(&self) -> &str {
        match self {
            Self::Exact { id } => id,
            Self::Digest { prefix, .. } => prefix,
        }
    }

    pub(crate) fn is_digest(&self) -> bool {
        matches!(self, Self::Digest { .. })
    }

    /// Whether a long-ID candidate is the position this digest names. Decode
    /// validation has already established a canonical bounded prefix/digest.
    pub(crate) fn matches_digest(&self, candidate: &str) -> bool {
        match self {
            Self::Digest { prefix, sha256 } => {
                candidate.len() > CONTINUATION_EXACT_ID_MAX_BYTES
                    && candidate.starts_with(prefix)
                    && hashed_identity(candidate) == *sha256
            }
            Self::Exact { .. } => false,
        }
    }

    pub(crate) fn prefix_contains(&self, candidate: &str) -> bool {
        match self {
            Self::Digest { prefix, .. } => candidate.starts_with(prefix),
            Self::Exact { .. } => false,
        }
    }

    fn validate(&self, expected: &str) -> Result<()> {
        match self {
            Self::Exact { id } if id.len() <= CONTINUATION_EXACT_ID_MAX_BYTES => Ok(()),
            Self::Exact { .. } => Err(cursor_rejected(format!(
                "invalid {expected} exact-id position"
            ))),
            Self::Digest { prefix, sha256 } => {
                // UTF-8 scalar values occupy at most four bytes. A canonical
                // 64-byte truncation can therefore end only in [61, 64];
                // rejecting shorter prefixes prevents a crafted token from
                // widening the rare digest-resolution scan arbitrarily.
                if prefix.len() < CONTINUATION_ID_PREFIX_MAX_BYTES - 3
                    || prefix.len() > CONTINUATION_ID_PREFIX_MAX_BYTES
                {
                    return Err(cursor_rejected(format!("invalid {expected} digest prefix")));
                }
                let digest = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(sha256)
                    .map_err(|_| cursor_rejected(format!("invalid {expected} id digest")))?;
                if digest.len() != TOKEN_CHECKSUM_BYTES
                    || base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&digest) != *sha256
                {
                    return Err(cursor_rejected(format!("invalid {expected} id digest")));
                }
                Ok(())
            }
        }
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
    pub position: IdPositionV1,
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
    /// Fixed-size digest of the normalized branch scope.
    pub branch: BranchScopeV1,
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
    /// Fixed-size digest of the normalized branch scope.
    pub branch: BranchScopeV1,
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
    pub position: IdPositionV1,
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
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(encoded);
    if encoded.len() > TOKEN_MAX_ENCODED_BYTES {
        return Err(OmniError::resource_limit(
            "change_continuation_token_encoded_bytes",
            TOKEN_MAX_ENCODED_BYTES as u64,
            encoded.len() as u64,
        ));
    }
    Ok(encoded)
}

/// Decode one token, enforcing the checksum, the version, and the expected
/// kind. Every failure is the typed cursor rejection with a reason that
/// distinguishes caller-side corruption from cross-use.
fn decode_token<T: DeserializeOwned>(token: &str, expected_kind: &'static str) -> Result<T> {
    let expected = kind_label(expected_kind);
    if token.len() > TOKEN_MAX_ENCODED_BYTES {
        return Err(cursor_rejected(format!(
            "{expected} exceeds the encoded-size limit"
        )));
    }
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
    decoded.position.validate("commit changes page token")?;
    Ok(decoded)
}

pub(crate) fn decode_feed_cursor(token: &str) -> Result<FeedCursorV1> {
    let decoded: FeedCursorV1 = decode_token(token, KIND_FEED_CURSOR)?;
    if decoded.purpose != FEED_PURPOSE {
        return Err(cursor_rejected(
            "change feed cursor purpose does not match this feed",
        ));
    }
    decoded.branch.validate("change feed cursor")?;
    Ok(decoded)
}

pub(crate) fn decode_feed_page_token(token: &str) -> Result<FeedPageTokenV1> {
    let decoded: FeedPageTokenV1 = decode_token(token, KIND_FEED_PAGE)?;
    if decoded.purpose != FEED_PURPOSE {
        return Err(cursor_rejected(
            "change feed page token purpose does not match this feed",
        ));
    }
    decoded.branch.validate("change feed page token")?;
    if decoded.type_id.is_empty() || decoded.operation_rank > ChangeOpKind::Delete.rank() {
        return Err(cursor_rejected("invalid change feed page token identity"));
    }
    decoded.position.validate("change feed page token")?;
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
            position: IdPositionV1::for_id("alice"),
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
            branch: BranchScopeV1::for_branch(None),
            branch_witness: hashed_identity("main"),
            filter_digest: filter_digest(&ChangeFeedScope::default()),
            after_commit_id: "commit".to_string(),
        }
    }

    fn feed_page_token() -> FeedPageTokenV1 {
        let cursor = feed_cursor();
        FeedPageTokenV1 {
            version: TOKEN_VERSION,
            kind: KIND_FEED_PAGE.to_string(),
            graph_identity: cursor.graph_identity,
            genesis_commit_id: cursor.genesis_commit_id,
            purpose: cursor.purpose,
            branch: cursor.branch,
            branch_witness: cursor.branch_witness,
            filter_digest: cursor.filter_digest,
            cut_commit_id: "cut".to_string(),
            after_commit_id: cursor.after_commit_id,
            current_commit_id: "current".to_string(),
            type_id: opaque_type_id(
                "graph",
                TableIdentity {
                    stable_table_id: 3,
                    table_incarnation_id: 7,
                },
            ),
            position: IdPositionV1::for_id("alice"),
            operation_rank: 1,
            change_index: 41,
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

        let page = feed_page_token();
        let decoded = decode_feed_page_token(&encode_token(&page).unwrap()).unwrap();
        assert_eq!(decoded, page);
    }

    #[test]
    fn id_positions_are_exact_only_below_the_fixed_cap_and_large_ids_stay_bounded() {
        let exact_id = "e".repeat(CONTINUATION_EXACT_ID_MAX_BYTES);
        assert_eq!(
            IdPositionV1::for_id(&exact_id),
            IdPositionV1::Exact {
                id: exact_id.clone()
            }
        );

        // End the 64-byte prefix in the middle of a multi-byte scalar. The
        // position must retreat to a valid UTF-8 boundary, stay canonical,
        // and still match only the original long ID.
        let long_id = format!(
            "{}é{}",
            "x".repeat(CONTINUATION_ID_PREFIX_MAX_BYTES - 1),
            "y".repeat(CONTINUATION_EXACT_ID_MAX_BYTES)
        );
        let position = IdPositionV1::for_id(&long_id);
        position.validate("test position").unwrap();
        assert!(position.is_digest());
        assert!(position.matches_digest(&long_id));
        assert!(!position.matches_digest(&format!("{long_id}-other")));

        let mut token = commit_page_token();
        token.position = position;
        let encoded = encode_token(&token).unwrap();
        assert!(encoded.len() <= TOKEN_MAX_ENCODED_BYTES);
        assert_eq!(decode_commit_page_token(&encoded).unwrap(), token);

        // Token size is independent of the pathological logical-ID tail.
        token.position = IdPositionV1::for_id(&"z".repeat(1024 * 1024));
        let huge_id_token = encode_token(&token).unwrap();
        assert!(huge_id_token.len() <= TOKEN_MAX_ENCODED_BYTES);
    }

    #[test]
    fn malformed_or_ambiguous_id_positions_are_rejected() {
        let mut token = commit_page_token();
        token.position = IdPositionV1::Exact {
            id: "x".repeat(CONTINUATION_EXACT_ID_MAX_BYTES + 1),
        };
        let error = decode_commit_page_token(&encode_token(&token).unwrap()).unwrap_err();
        assert!(
            matches!(error, OmniError::ChangeCursorRejected { reason } if reason.contains("exact-id"))
        );

        token.position = IdPositionV1::Digest {
            prefix: "too-short".to_string(),
            sha256: hashed_identity("anything"),
        };
        let error = decode_commit_page_token(&encode_token(&token).unwrap()).unwrap_err();
        assert!(
            matches!(error, OmniError::ChangeCursorRejected { reason } if reason.contains("prefix"))
        );

        token.position = IdPositionV1::Digest {
            prefix: "p".repeat(CONTINUATION_ID_PREFIX_MAX_BYTES),
            sha256: "not-a-sha256".to_string(),
        };
        let error = decode_commit_page_token(&encode_token(&token).unwrap()).unwrap_err();
        assert!(
            matches!(error, OmniError::ChangeCursorRejected { reason } if reason.contains("digest"))
        );

        // `deny_unknown_fields` keeps an attacker from supplying both exact
        // and digest members and relying on one decoder's field precedence.
        let mut ambiguous = serde_json::to_value(commit_page_token()).unwrap();
        ambiguous["position"]["sha256"] = serde_json::Value::String(hashed_identity("alice"));
        let error = decode_commit_page_token(&encode_token(&ambiguous).unwrap()).unwrap_err();
        assert!(matches!(error, OmniError::ChangeCursorRejected { .. }));
    }

    #[test]
    fn branch_scopes_are_fixed_size_and_distinguish_exact_long_names() {
        let mut cursor = feed_cursor();
        let common = "branch-prefix/".repeat(TOKEN_MAX_ENCODED_BYTES);
        let first = format!("{common}a");
        let second = format!("{common}b");
        cursor.branch = BranchScopeV1::for_branch(Some(&first));
        let first_token = encode_token(&cursor).unwrap();
        assert!(first_token.len() <= TOKEN_MAX_ENCODED_BYTES);
        assert_eq!(decode_feed_cursor(&first_token).unwrap(), cursor);

        let second_scope = BranchScopeV1::for_branch(Some(&second));
        assert_ne!(cursor.branch, second_scope);
        assert_ne!(cursor.branch, BranchScopeV1::for_branch(None));

        let mut page = feed_page_token();
        page.branch = cursor.branch.clone();
        let page_token = encode_token(&page).unwrap();
        assert!(page_token.len() <= TOKEN_MAX_ENCODED_BYTES);
        assert_eq!(decode_feed_page_token(&page_token).unwrap(), page);

        // The cursor's size is independent of the raw branch-name tail.
        cursor.branch = second_scope;
        assert_eq!(encode_token(&cursor).unwrap().len(), first_token.len());

        // A checksummed payload still is not trusted input. Only the
        // canonical fixed-size branch representation is accepted.
        cursor.branch = BranchScopeV1("raw-branch-name".to_string());
        let malformed = encode_token(&cursor).unwrap();
        let error = decode_feed_cursor(&malformed).unwrap_err();
        assert!(
            matches!(error, OmniError::ChangeCursorRejected { reason } if reason.contains("branch scope"))
        );
    }

    #[test]
    fn encoded_token_ceiling_is_enforced_before_decode() {
        let mut cursor = feed_cursor();
        cursor.after_commit_id = "c".repeat(TOKEN_MAX_ENCODED_BYTES);
        assert!(matches!(
            encode_token(&cursor).unwrap_err(),
            OmniError::ResourceLimitExceeded { .. }
        ));

        let oversized = "A".repeat(TOKEN_MAX_ENCODED_BYTES + 1);
        let error = decode_feed_cursor(&oversized).unwrap_err();
        assert!(
            matches!(error, OmniError::ChangeCursorRejected { reason } if reason.contains("encoded-size"))
        );
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
