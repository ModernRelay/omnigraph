//! Native Lance ref names for graph branches.
//!
//! A graph branch's *logical* name is the identity every public surface, write
//! gate, policy scope, and `graph_head:<branch>` row uses. Its *native* Lance
//! ref carries an incarnation suffix: `{logical}.{ulid}`. Every create mints a
//! fresh incarnation, so a recreated branch never shares `tree/{ref}/` bytes
//! with a dead predecessor. A late-settling reclaim of the old ref can only
//! touch a path nothing references any more, which turns the
//! delete/recreate race into ordinary garbage for cleanup instead of silent
//! data loss. Refs without a suffix are legacy incarnations (native ==
//! logical) and keep resolving unchanged.
//!
//! The manifest dataset's ref list is the registry: a logical branch is live
//! iff exactly one native ref splits back to it.

use crate::error::{OmniError, Result};
use crate::graph_commit_id::{is_valid_graph_commit_id, parse_history_block_id};

/// Length of a Crockford-base32 ULID string.
pub const INCARNATION_LEN: usize = 26;

/// Mint a fresh branch incarnation.
pub fn mint_incarnation() -> String {
    crate::dst_ids::new_ulid().to_string()
}

/// The native Lance ref name for one incarnation of a logical branch.
pub fn native_branch_name(logical: &str, incarnation: &str) -> String {
    format!("{logical}.{incarnation}")
}

/// Names can conservatively retain unpublished forks, never authorize ownership.
/// Unknown generated names lack enough evidence to reclaim safely.
pub fn retain_unpublished_table_fork(
    native: &str,
    incarnation_is_live: impl FnOnce(&str) -> bool,
) -> bool {
    let Some(rest) = native.strip_prefix("fork.") else {
        return false;
    };
    let mut parts = rest.split('.');
    let (Some(incarnation), Some(base), Some(commit), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return true;
    };
    if !is_incarnation(incarnation)
        || !is_incarnation(commit)
        || !base.strip_prefix('m').is_some_and(|version| {
            version
                .parse::<u64>()
                .is_ok_and(|parsed| parsed.to_string() == version)
        })
    {
        return true;
    }
    incarnation_is_live(incarnation)
}

fn is_incarnation(candidate: &str) -> bool {
    candidate.len() == INCARNATION_LEN
        && candidate
            .parse::<ulid::Ulid>()
            .is_ok_and(|id| id.to_string() == candidate)
}

/// Split a native ref name into its logical name and incarnation.
///
/// Only the final path segment may carry the suffix. Names without a
/// well-formed suffix are legacy incarnations and split to `(name, None)`.
pub fn split_native_branch_name(native: &str) -> (&str, Option<&str>) {
    if let Some(dot) = native.rfind('.') {
        let (logical, incarnation) = (&native[..dot], &native[dot + 1..]);
        if is_incarnation(incarnation) && !logical.is_empty() && !logical.ends_with('/') {
            return (logical, Some(incarnation));
        }
    }
    (native, None)
}

/// The logical branch a native ref belongs to.
pub fn logical_branch_name(native: &str) -> &str {
    split_native_branch_name(native).0
}

/// Refuse a logical name with an incarnation-shaped suffix in any path
/// segment. A final segment could not be split back unambiguously, and an
/// inner one (`feature.<id>/child`) would place the child's tree under a
/// native ref's physical path, reintroducing the ancestor/descendant overlap
/// the suffix exists to remove.
pub fn ensure_logical_branch_name(logical: &str) -> Result<()> {
    if logical
        .split('/')
        .any(|segment| split_native_branch_name(segment).1.is_some())
    {
        return Err(OmniError::manifest(format!(
            "branch name '{logical}' contains an incarnation-shaped suffix; choose another name"
        )));
    }
    Ok(())
}

pub const MERGE_INPUT_PREFIX: &str = "__omnigraph_merge_input_v1_";

/// Ownership encoded before the tag's single create-if-absent publication.
/// The digest bounds name length even for deeply nested branch identifiers;
/// the actual graph head remains available for the collector's ancestry test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeInputOwner {
    pub incarnation_digest: String,
    pub graph_head: Option<String>,
}

/// `n` for no head, `b` plus a canonical block id as it is (delimiter-safe
/// ASCII; hex would double it past local filename limits when the tag is
/// created), `s` plus the hex of any other head.
pub fn encode_head(head: Option<&str>) -> String {
    match head {
        None => "n".to_string(),
        Some(head) if parse_history_block_id(head).is_ok_and(|id| id.is_some()) => {
            format!("b{head}")
        }
        Some(head) => {
            let mut encoded = String::from("s");
            for byte in head.as_bytes() {
                use std::fmt::Write;
                write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
            }
            encoded
        }
    }
}

/// The inverse of [`encode_head`]. The hex form is checked on its own terms,
/// not through the current encoder, so hex block ids persisted before the `b`
/// form stay valid ownership witnesses.
pub fn decode_head(encoded: &str) -> Option<Option<String>> {
    if encoded == "n" {
        return Some(None);
    }
    if let Some(head) = encoded.strip_prefix('b') {
        return parse_history_block_id(head)
            .ok()
            .flatten()
            .map(|_| Some(head.to_string()));
    }
    let encoded = encoded.strip_prefix('s')?;
    if encoded.is_empty()
        || encoded.len() % 2 != 0
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let bytes = (0..encoded.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&encoded[index..index + 2], 16).ok())
        .collect::<Option<Vec<_>>>()?;
    let head = String::from_utf8(bytes).ok()?;
    if !is_valid_graph_commit_id(&head) {
        return None;
    }
    Some(Some(head))
}

pub fn merge_input_owner(name: &str) -> Result<Option<MergeInputOwner>> {
    let Some(encoded) = name.strip_prefix(MERGE_INPUT_PREFIX) else {
        return Ok(None);
    };
    let invalid = || OmniError::manifest_conflict("malformed merge input retention tag");
    let mut parts = encoded.split('_');
    let digest = parts.next().ok_or_else(invalid)?;
    let head = parts.next().and_then(decode_head).ok_or_else(invalid)?;
    let nonce = parts.next().ok_or_else(invalid)?;
    if parts.next().is_some()
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || nonce
            .parse::<ulid::Ulid>()
            .ok()
            .is_none_or(|id| id.to_string() != nonce)
    {
        return Err(invalid());
    }
    Ok(Some(MergeInputOwner {
        incarnation_digest: digest.to_string(),
        graph_head: head,
    }))
}

/// Only valid engine-owned tags exempt logical retirement; arbitrary native
/// tags retain Lance's existing refusal semantics.
pub fn is_merge_input_tag(name: &str) -> bool {
    matches!(merge_input_owner(name), Ok(Some(_)))
}

/// Resolve a logical branch to its single live native ref.
///
/// `Ok(None)` means no incarnation exists. More than one live incarnation is a
/// registry invariant violation and fails loudly rather than guessing.
pub fn resolve_native_branch<'a>(
    natives: impl IntoIterator<Item = &'a str>,
    logical: &str,
) -> Result<Option<String>> {
    let mut matches: Vec<&str> = natives
        .into_iter()
        .filter(|native| logical_branch_name(native) == logical)
        .collect();
    match matches.len() {
        0 => Ok(None),
        1 => Ok(Some(matches[0].to_string())),
        _ => {
            matches.sort_unstable();
            Err(OmniError::manifest_conflict(format!(
                "branch '{logical}' has {} live native incarnations ({}); run cleanup before \
                 using it",
                matches.len(),
                matches.join(", ")
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_merge_input_heads_preserve_legacy_tags_and_fit_local_filenames() {
        fn legacy_hex(head: &str) -> String {
            let mut encoded = String::from("s");
            for byte in head.as_bytes() {
                use std::fmt::Write;
                write!(&mut encoded, "{byte:02x}").unwrap();
            }
            encoded
        }

        let head = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let nonce = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
        let digest = "ab".repeat(32);
        let owner_of = |encoded: &str| {
            merge_input_owner(&format!("{MERGE_INPUT_PREFIX}{digest}_{encoded}_{nonce}"))
                .unwrap()
                .unwrap()
        };
        assert_eq!(encode_head(None), "n");
        assert_eq!(owner_of("n").graph_head, None);
        assert_eq!(encode_head(Some(head)), legacy_hex(head));
        assert_eq!(
            owner_of(&legacy_hex(head)).graph_head.as_deref(),
            Some(head)
        );
        for slot in [0, 9, 10, 15] {
            let block_head = format!("hb1.{head}.{slot}.{nonce}");
            let encoded = encode_head(Some(&block_head));
            assert_eq!(encoded, format!("b{block_head}"));
            for encoded in [&encoded, &legacy_hex(&block_head)] {
                assert_eq!(
                    owner_of(encoded).graph_head.as_deref(),
                    Some(block_head.as_str())
                );
            }
        }
        let last_slot = crate::graph_commit_id::HISTORY_BLOCK_SLOTS - 1;
        let slot_digits = last_slot.to_string().len();
        let longest_block_head = format!("hb1.{head}.{last_slot}.{nonce}");
        let tag = format!(
            "{MERGE_INPUT_PREFIX}{digest}_{}_{nonce}",
            encode_head(Some(&longest_block_head))
        );
        assert_eq!(tag.len(), 178 + slot_digits);
        let staged = format!("{tag}.json.tmp.{}#1", "0".repeat(32));
        assert_eq!(staged.len(), 222 + slot_digits);
        let longest_counter = format!("{tag}.json.tmp.{}#{}", "0".repeat(32), u64::MAX);
        assert_eq!(longest_counter.len(), 241 + slot_digits);
        assert!(
            longest_counter.len() <= 255,
            "the tag's real staging path (Lance's `.json.tmp.<uuid>` plus the local object \
             store's `#<counter>`) fits a local filename"
        );

        for malformed in [
            "b".to_string(),
            "bn".to_string(),
            format!("b{head}"),
            format!(
                "bhb1.{head}.{}.{nonce}",
                crate::graph_commit_id::HISTORY_BLOCK_SLOTS
            ),
            format!("bhb1.{head}.015.{nonce}"),
            format!("bhb1.{}.15.{nonce}", head.to_lowercase()),
            format!("bhb1.{head}.15.{nonce}/child"),
            format!("bhb1.{head}.15.{nonce}_extra"),
            legacy_hex(head).replacen("5a", "5A", 1),
            legacy_hex(&longest_block_head).replacen("5a", "5A", 1),
        ] {
            assert_eq!(
                decode_head(&malformed),
                None,
                "malformed, or noncanonical like uppercase hex of a valid head: {malformed}"
            );
            let tag = format!("{MERGE_INPUT_PREFIX}{digest}_{malformed}_{nonce}");
            assert!(merge_input_owner(&tag).is_err(), "{tag}");
            assert!(!is_merge_input_tag(&tag));
        }
    }

    #[test]
    fn unpublished_fork_retention_tracks_incarnations_not_logical_names() {
        let incarnation = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let replacement = "01BX5ZZKBKACTAV9WEVGEMMVRZ";
        let native = "fork.01ARZ3NDEKTSV4RRFFQ69G5FAV.m42.01ARZ3NDEKTSV4RRFFQ69G5FAW";
        assert!(retain_unpublished_table_fork(native, |owner| owner == incarnation));
        assert!(!retain_unpublished_table_fork(native, |owner| owner == replacement));
        assert!(!retain_unpublished_table_fork(native, |_| false));
    }

    #[test]
    fn unknown_generated_forks_are_retained_without_authorizing_identity() {
        for native in [
            "fork.legacy.m42.01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "fork.future-format",
            "fork.ZZZZZZZZZZZZZZZZZZZZZZZZZZ.m42.01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "fork.01ARZ3NDEKTSV4RRFFQ69G5FAV.m42.ZZZZZZZZZZZZZZZZZZZZZZZZZZ",
            "fork.01ARZ3NDEKTSV4RRFFQ69G5FAV.m042.01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "fork.01ARZ3NDEKTSV4RRFFQ69G5FAV.m18446744073709551616.01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "fork.01ARZ3NDEKTSV4RRFFQ69G5FAV.mbad.01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "fork.01ARZ3NDEKTSV4RRFFQ69G5FAV.m42.bad",
            "fork.01ARZ3NDEKTSV4RRFFQ69G5FAV.m42.01ARZ3NDEKTSV4RRFFQ69G5FAW/child",
        ] {
            assert!(retain_unpublished_table_fork(native, |_| {
                panic!("unknown names cannot identify an owner")
            }));
        }
        assert!(!retain_unpublished_table_fork("feature", |_| true));
    }

    #[test]
    fn minted_native_names_split_back_to_their_logical_name() {
        let incarnation = mint_incarnation();
        assert_eq!(incarnation.len(), INCARNATION_LEN);
        let native = native_branch_name("feature/x", &incarnation);
        assert_eq!(
            split_native_branch_name(&native),
            ("feature/x", Some(incarnation.as_str()))
        );
        assert_eq!(logical_branch_name(&native), "feature/x");
    }

    #[test]
    fn legacy_and_lookalike_names_are_not_split() {
        assert_eq!(split_native_branch_name("feature"), ("feature", None));
        assert_eq!(split_native_branch_name("v1.2.3"), ("v1.2.3", None));
        assert_eq!(
            split_native_branch_name("x.ZZZZZZZZZZZZZZZZZZZZZZZZZZ"),
            ("x.ZZZZZZZZZZZZZZZZZZZZZZZZZZ", None)
        );
        // Wrong length, wrong alphabet (I/L/O/U, lowercase), or a leading dot.
        assert_eq!(
            split_native_branch_name("x.01ARZ3NDEKTSV4RRFFQ69G5FA"),
            ("x.01ARZ3NDEKTSV4RRFFQ69G5FA", None)
        );
        assert_eq!(
            split_native_branch_name("x.01ARZ3NDEKTSV4RRFFQ69G5FAI"),
            ("x.01ARZ3NDEKTSV4RRFFQ69G5FAI", None)
        );
        assert_eq!(
            split_native_branch_name("x.01arz3ndektsv4rrffq69g5fav"),
            ("x.01arz3ndektsv4rrffq69g5fav", None)
        );
        assert_eq!(
            split_native_branch_name(".01ARZ3NDEKTSV4RRFFQ69G5FAV"),
            (".01ARZ3NDEKTSV4RRFFQ69G5FAV", None)
        );
    }

    #[test]
    fn logical_names_that_look_suffixed_are_refused() {
        ensure_logical_branch_name("feature").unwrap();
        ensure_logical_branch_name("release.1.2").unwrap();
        let err = ensure_logical_branch_name("feature.01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap_err();
        assert!(err.to_string().contains("incarnation-shaped"), "{err}");
    }

    #[test]
    fn resolution_returns_the_single_live_incarnation_or_fails_loudly() {
        let a = native_branch_name("feature", "01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let b = native_branch_name("feature", "01BX5ZZKBKACTAV9WEVGEMMVRZ");
        let other = native_branch_name("other", "01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let natives = [a.as_str(), other.as_str(), "legacy"];
        assert_eq!(
            resolve_native_branch(natives, "feature")
                .unwrap()
                .as_deref(),
            Some(a.as_str())
        );
        assert_eq!(
            resolve_native_branch(natives, "legacy").unwrap().as_deref(),
            Some("legacy")
        );
        assert_eq!(resolve_native_branch(natives, "missing").unwrap(), None);
        let err = resolve_native_branch([a.as_str(), b.as_str()], "feature").unwrap_err();
        assert!(
            err.to_string().contains("2 live native incarnations"),
            "{err}"
        );
    }
}
