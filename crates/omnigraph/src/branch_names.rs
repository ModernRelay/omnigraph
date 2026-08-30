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

/// Length of a Crockford-base32 ULID string.
pub(crate) const INCARNATION_LEN: usize = 26;

/// Mint a fresh branch incarnation.
pub(crate) fn mint_incarnation() -> String {
    ulid::Ulid::new().to_string()
}

/// The native Lance ref name for one incarnation of a logical branch.
pub(crate) fn native_branch_name(logical: &str, incarnation: &str) -> String {
    format!("{logical}.{incarnation}")
}

fn is_incarnation(candidate: &str) -> bool {
    candidate.len() == INCARNATION_LEN
        && candidate.bytes().all(|byte| {
            matches!(
                byte,
                b'0'..=b'9' | b'A'..=b'H' | b'J' | b'K' | b'M' | b'N' | b'P'..=b'T' | b'V'..=b'Z'
            )
        })
}

/// Split a native ref name into its logical name and incarnation.
///
/// Only the final path segment may carry the suffix. Names without a
/// well-formed suffix are legacy incarnations and split to `(name, None)`.
pub(crate) fn split_native_branch_name(native: &str) -> (&str, Option<&str>) {
    if let Some(dot) = native.rfind('.') {
        let (logical, incarnation) = (&native[..dot], &native[dot + 1..]);
        if is_incarnation(incarnation) && !logical.is_empty() && !logical.ends_with('/') {
            return (logical, Some(incarnation));
        }
    }
    (native, None)
}

/// The logical branch a native ref belongs to.
pub(crate) fn logical_branch_name(native: &str) -> &str {
    split_native_branch_name(native).0
}

/// Refuse a logical name with an incarnation-shaped suffix in any path
/// segment. A final segment could not be split back unambiguously, and an
/// inner one (`feature.<id>/child`) would place the child's tree under a
/// native ref's physical path, reintroducing the ancestor/descendant overlap
/// the suffix exists to remove.
pub(crate) fn ensure_logical_branch_name(logical: &str) -> Result<()> {
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

/// Resolve a logical branch to its single live native ref.
///
/// `Ok(None)` means no incarnation exists. More than one live incarnation is a
/// registry invariant violation and fails loudly rather than guessing.
pub(crate) fn resolve_native_branch<'a>(
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
