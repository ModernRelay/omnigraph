//! Per-life branch identity: the logical-name to native-ref mapping.
//!
//! A user-facing (logical) branch name maps to an internal Lance native ref
//! that carries a per-life incarnation token: `{logical}--{ulid}`. Every
//! branch life gets a fresh token, so a deleted-and-recreated branch lives at
//! new storage paths and therefore in a new dataset-URI cache namespace by
//! construction (issue #562: Lance's per-handle `Session` file-metadata cache
//! is keyed by dataset URI and observes no lifecycle events; rotating the
//! namespace replaces clearing it). The `__manifest` branch-registry row owns
//! the mapping; see `db/manifest/state.rs::OBJECT_TYPE_BRANCH`.
//!
//! The separator is `--`, chosen from Lance's legal ref charset
//! (`[A-Za-z0-9._-]` per `/`-segment; `@` is rejected by
//! `check_valid_branch`). Public branch names reject `--` at
//! `ensure_public_branch_ref` (the sigil-reservation pattern), so no
//! user-named branch can collide with a native ref minted here. Legacy
//! branches created before this mapping keep their bare ref name (the
//! registry has no row for them; resolution falls back to the bare name) and
//! mint their first token on their next rebirth.

/// Reserved separator between the logical name and the per-life token in a
/// native branch ref. Also the substring rejected in public branch names.
pub(crate) const BRANCH_INCARNATION_SEPARATOR: &str = "--";

/// Length of the incarnation token: a ULID in Crockford base32.
const INCARNATION_TOKEN_LEN: usize = 26;

/// Mint a fresh per-life incarnation token. Routed through the DST seam so
/// simulation runs mint deterministic, seed-reproducible tokens.
pub(crate) fn mint_branch_incarnation() -> String {
    crate::dst_ids::new_ulid().to_string()
}

/// The native ref name for one life of a logical branch.
pub(crate) fn native_branch_ref(logical: &str, incarnation: &str) -> String {
    format!("{logical}{BRANCH_INCARNATION_SEPARATOR}{incarnation}")
}

/// Split a native ref back into `(logical, Some(incarnation))`, or
/// `(name, None)` for a legacy bare ref.
///
/// The suffix is accepted only when it parses as a real ULID: legacy public
/// names could legally contain `--` (the rejection in
/// `ensure_public_branch_ref` arrived with this module), so a bare `a--b`
/// ref must stay a legacy name, not mis-split.
pub(crate) fn split_native_branch_ref(native: &str) -> (&str, Option<&str>) {
    if let Some(idx) = native.rfind(BRANCH_INCARNATION_SEPARATOR) {
        let candidate = &native[idx + BRANCH_INCARNATION_SEPARATOR.len()..];
        if candidate.len() == INCARNATION_TOKEN_LEN && ulid::Ulid::from_string(candidate).is_ok() {
            return (&native[..idx], Some(candidate));
        }
    }
    (native, None)
}

/// Whether a public (user-supplied) branch name is allowed to enter the
/// namespace: it must not contain the reserved separator.
pub(crate) fn public_name_reserves_separator(name: &str) -> bool {
    name.contains(BRANCH_INCARNATION_SEPARATOR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_ref_round_trips() {
        let token = mint_branch_incarnation();
        let native = native_branch_ref("feature/x", &token);
        let (logical, incarnation) = split_native_branch_ref(&native);
        assert_eq!(logical, "feature/x");
        assert_eq!(incarnation, Some(token.as_str()));
    }

    #[test]
    fn legacy_names_with_separator_do_not_missplit() {
        // A pre-reservation public name containing `--` must stay whole.
        let (logical, incarnation) = split_native_branch_ref("a--b");
        assert_eq!(logical, "a--b");
        assert!(incarnation.is_none());
        // Even a 26-char suffix that is not a valid ULID stays whole.
        let (logical, incarnation) = split_native_branch_ref("x--uuuuuuuuuuuuuuuuuuuuuuuuuu");
        assert_eq!(logical, "x--uuuuuuuuuuuuuuuuuuuuuuuuuu");
        assert!(incarnation.is_none());
    }

    #[test]
    fn separator_reservation_matches_contains() {
        assert!(public_name_reserves_separator("a--b"));
        assert!(!public_name_reserves_separator("a-b"));
        assert!(!public_name_reserves_separator("plain"));
    }
}
