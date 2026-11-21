//! The control facts a `__manifest` version carries in its version file's
//! `config` map (RFC "Branch existence in the catalog"). Main's chain holds
//! one `branch` row per branch lifetime under `omnigraph.branch.<native>`; the
//! retired row is the same key with the value `-`. A branch's tree holds its
//! tombstone under `omnigraph.tombstone`. A branch's tree starts as a shallow
//! clone of its source's version and carries that version's map, so the decode
//! reads branch entries on main's chain only and the tombstone on a tree only.

use std::collections::{BTreeMap, BTreeSet};

use lance::dataset::refs::BranchIdentifier;
use lance_table::format::Manifest;

use crate::MAIN_BRANCH_HEAD_KEY;
use crate::branch_names::{logical_branch_name, split_native_branch_name};
use crate::error::{OmniError, Result};

/// The key prefix of a `branch` row; what follows it is the native name,
/// which may itself contain `.`.
pub const BRANCH_ENTRY_PREFIX: &str = "omnigraph.branch.";

/// The key of a tree's tombstone.
pub const TOMBSTONE_ENTRY_KEY: &str = "omnigraph.tombstone";

/// The value of a retired row: the key stays, since a strict overwrite adds
/// to the map and never removes from it; the collector removes the entry.
pub const RETIRED_ENTRY_VALUE: &str = "-";

/// The encoding version every entry leads with under this stamp.
const ENTRY_VERSION: &str = "1";

/// The field separator; no field contains it, since Lance's branch alphabet
/// admits none and the other fields are numbers, a ULID and an identifier.
const FIELD_SEPARATOR: char = '|';

/// One `branch` row: a branch lifetime, keyed by its native name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchRow {
    /// The native name `{logical}.{ULID}`; a legacy lifetime has no suffix and
    /// its logical name is the whole native name.
    pub native: String,
    /// Lance's lifetime identifier; the row is its home.
    pub identifier: BranchIdentifier,
    /// The native name of the source branch, `main` for main.
    pub parent: String,
    /// The source's `__manifest` version the branch starts from.
    pub parent_version: u64,
    /// The creation time in Unix seconds.
    pub created_at: u64,
    /// The ULID the creating operation minted before its first attempt: the
    /// identity of this control fact.
    pub attempt_id: String,
}

impl BranchRow {
    /// The logical name, what precedes the final `.` and the ULID.
    pub fn logical(&self) -> &str {
        logical_branch_name(&self.native)
    }

    /// Whether the branch was created from main.
    pub fn parent_is_main(&self) -> bool {
        self.parent == MAIN_BRANCH_HEAD_KEY
    }

    /// The config key of this row.
    pub fn key(&self) -> String {
        branch_entry_key(&self.native)
    }

    /// The config entry this row is written as.
    pub fn entry(&self) -> (String, String) {
        let value = [
            ENTRY_VERSION.to_string(),
            identifier_text(&self.identifier),
            self.parent.clone(),
            self.parent_version.to_string(),
            self.created_at.to_string(),
            self.attempt_id.clone(),
        ]
        .join(&FIELD_SEPARATOR.to_string());
        (self.key(), value)
    }
}

/// The config key of the `branch` row of `native`.
pub fn branch_entry_key(native: &str) -> String {
    format!("{BRANCH_ENTRY_PREFIX}{native}")
}

/// The retired row of `native`, as `branch delete` writes it in place of the row.
pub fn retired_entry(native: &str) -> (String, String) {
    (branch_entry_key(native), RETIRED_ENTRY_VALUE.to_string())
}

/// A tree's tombstone: the control fact that ends its chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TombstoneRow {
    /// The ULID the deleting operation minted before its first attempt.
    pub attempt_id: String,
    /// The deletion time in Unix seconds.
    pub deleted_at: u64,
}

impl TombstoneRow {
    /// The config entry this tombstone is written as.
    pub fn entry(&self) -> (String, String) {
        let value = [
            ENTRY_VERSION.to_string(),
            self.attempt_id.clone(),
            self.deleted_at.to_string(),
        ]
        .join(&FIELD_SEPARATOR.to_string());
        (TOMBSTONE_ENTRY_KEY.to_string(), value)
    }
}

/// The rows one `__manifest` version's config map decodes into: on main's
/// chain the live `branch` rows and the retired keys, on a tree its tombstone.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BranchRows {
    live: BTreeMap<String, BranchRow>,
    /// The live rows' logical names, each to its native name.
    logical: BTreeMap<String, String>,
    retired: BTreeSet<String>,
    tombstone: Option<TombstoneRow>,
}

impl BranchRows {
    /// The rows of `manifest`'s config map, told main from a tree by the
    /// manifest's own `branch` field. A malformed entry fails naming its key.
    pub fn decode(manifest: &Manifest) -> Result<Self> {
        Self::from_entries(
            manifest.branch.as_deref(),
            manifest
                .config
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
    }

    /// [`Self::decode`] over the entries themselves, for the chain whose
    /// native name is `native_branch` (`None` for main).
    pub(crate) fn from_entries<'a>(
        native_branch: Option<&str>,
        entries: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Self> {
        let mut rows = Self::default();
        for (key, value) in entries {
            if let Some(native) = key.strip_prefix(BRANCH_ENTRY_PREFIX) {
                if native_branch.is_some() {
                    continue;
                }
                if value == RETIRED_ENTRY_VALUE {
                    rows.retired.insert(native.to_string());
                    continue;
                }
                let row =
                    decode_branch_value(native, value).map_err(|what| malformed(key, &what))?;
                if let Some(other) = rows.logical.get(row.logical()) {
                    return Err(OmniError::manifest_internal(format!(
                        "`__manifest` holds two live branch rows under the logical name '{}': \
                         '{}' and '{}'",
                        row.logical(),
                        other,
                        row.native
                    )));
                }
                rows.logical
                    .insert(row.logical().to_string(), native.to_string());
                rows.live.insert(native.to_string(), row);
            } else if key == TOMBSTONE_ENTRY_KEY {
                if native_branch.is_none() {
                    continue;
                }
                let tombstone =
                    decode_tombstone_value(value).map_err(|what| malformed(key, &what))?;
                rows.tombstone = Some(tombstone);
            }
        }
        Ok(rows)
    }

    /// The live rows, by native name.
    pub fn live(&self) -> impl Iterator<Item = &BranchRow> {
        self.live.values()
    }

    /// The number of live rows.
    pub fn live_count(&self) -> usize {
        self.live.len()
    }

    /// The live row of the native name `native`.
    pub fn branch(&self, native: &str) -> Option<&BranchRow> {
        self.live.get(native)
    }

    /// The live row whose logical name is `logical`; at most one exists.
    pub fn by_logical(&self, logical: &str) -> Option<&BranchRow> {
        self.logical
            .get(logical)
            .and_then(|native| self.live.get(native))
    }

    /// Refuse a create of `target` (RFC "Branch existence in the catalog"
    /// §Create step 2): `BranchExists` while a live row holds the name or a
    /// name above or below it, which would share its physical Lance path;
    /// a conflict while a retired legacy lifetime, named without an
    /// incarnation, still owns a path above it.
    pub fn refuse_create_overlap(&self, target: &str) -> Result<()> {
        let target_prefix = format!("{target}/");
        let overlaps = |candidate: &str| {
            candidate == target
                || candidate.starts_with(&target_prefix)
                || target.starts_with(&format!("{candidate}/"))
        };
        if let Some(live) = self.logical.keys().find(|name| overlaps(name)) {
            return Err(OmniError::BranchExists {
                branch: target.to_string(),
                live: live.clone(),
            });
        }
        let legacy_above = self
            .retired
            .iter()
            .filter(|native| split_native_branch_name(native).1.is_none())
            .find(|native| target.starts_with(&format!("{native}/")));
        if let Some(retired) = legacy_above {
            return Err(OmniError::manifest_conflict(format!(
                "cannot create branch '{target}' while retired branch '{retired}' shares its \
                 physical Lance path"
            )));
        }
        Ok(())
    }

    /// Whether `native`'s row is retired: the branch does not exist and its
    /// tree is the collector's.
    pub fn is_retired(&self, native: &str) -> bool {
        self.retired.contains(native)
    }

    /// The native names whose rows are retired.
    pub fn retired(&self) -> impl Iterator<Item = &str> {
        self.retired.iter().map(String::as_str)
    }

    /// The tree's tombstone; `None` on main's chain and on a live tree.
    pub fn tombstone(&self) -> Option<&TombstoneRow> {
        self.tombstone.as_ref()
    }

    /// Whether the map carried no row of any kind.
    pub fn is_empty(&self) -> bool {
        self.live.is_empty() && self.retired.is_empty() && self.tombstone.is_none()
    }
}

/// Lance's `ver1:token1:ver2:token2` form of an identifier, the one
/// [`BranchIdentifier::parse`] reads.
pub fn identifier_text(identifier: &BranchIdentifier) -> String {
    identifier
        .version_mapping
        .iter()
        .map(|(version, token)| format!("{version}:{token}"))
        .collect::<Vec<_>>()
        .join(":")
}

fn malformed(key: &str, what: &str) -> OmniError {
    OmniError::manifest_internal(format!(
        "`__manifest` version file entry '{key}' is malformed: {what}"
    ))
}

fn decode_branch_value(native: &str, value: &str) -> std::result::Result<BranchRow, String> {
    let fields: Vec<&str> = value.split(FIELD_SEPARATOR).collect();
    let &[
        version,
        identifier,
        parent,
        parent_version,
        created_at,
        attempt_id,
    ] = fields.as_slice()
    else {
        return Err(format!("{} fields where a branch row has 6", fields.len()));
    };
    check_entry_version(version)?;
    if parent.is_empty() {
        return Err("the parent field is empty".to_string());
    }
    Ok(BranchRow {
        native: native.to_string(),
        identifier: parse_identifier(identifier)?,
        parent: parent.to_string(),
        parent_version: parse_number("parent_version", parent_version)?,
        created_at: parse_number("created_at", created_at)?,
        attempt_id: parse_attempt_id(attempt_id)?,
    })
}

fn decode_tombstone_value(value: &str) -> std::result::Result<TombstoneRow, String> {
    let fields: Vec<&str> = value.split(FIELD_SEPARATOR).collect();
    let &[version, attempt_id, deleted_at] = fields.as_slice() else {
        return Err(format!("{} fields where a tombstone has 3", fields.len()));
    };
    check_entry_version(version)?;
    Ok(TombstoneRow {
        attempt_id: parse_attempt_id(attempt_id)?,
        deleted_at: parse_number("deleted_at", deleted_at)?,
    })
}

fn check_entry_version(version: &str) -> std::result::Result<(), String> {
    if version == ENTRY_VERSION {
        return Ok(());
    }
    Err(format!(
        "encoding version '{version}' is not {ENTRY_VERSION}"
    ))
}

fn parse_number(field: &str, text: &str) -> std::result::Result<u64, String> {
    text.parse()
        .map_err(|error| format!("{field} '{text}' is not a version number: {error}"))
}

fn parse_attempt_id(text: &str) -> std::result::Result<String, String> {
    ulid::Ulid::from_string(text)
        .map(|_| text.to_string())
        .map_err(|error| format!("attempt_id '{text}' is not a ULID: {error}"))
}

fn parse_identifier(text: &str) -> std::result::Result<BranchIdentifier, String> {
    let identifier = BranchIdentifier::parse(text).map_err(|error| error.to_string())?;
    if identifier.version_mapping.is_empty() {
        return Err("the identifier has no (version, token) pair".to_string());
    }
    for (_, token) in &identifier.version_mapping {
        let lowercase_hex = token.len() == 32
            && token
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        if !lowercase_hex {
            return Err(format!(
                "identifier token '{token}' is not 32 lowercase hexadecimal digits"
            ));
        }
    }
    Ok(identifier)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ATTEMPT: &str = "01JD0000000000000000000000";

    fn row(native: &str, parent: &str) -> BranchRow {
        let identifier = BranchIdentifier::new(&BranchIdentifier::main(), 3);
        BranchRow {
            native: native.to_string(),
            identifier,
            parent: parent.to_string(),
            parent_version: 3,
            created_at: 1_763_500_000,
            attempt_id: ATTEMPT.to_string(),
        }
    }

    fn decoded(native_branch: Option<&str>, entries: &[(String, String)]) -> Result<BranchRows> {
        BranchRows::from_entries(
            native_branch,
            entries
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
    }

    #[test]
    fn a_branch_row_round_trips_through_its_entry() {
        let written = row("agent-7.01JC0000000000000000000000", MAIN_BRANCH_HEAD_KEY);
        let (key, value) = written.entry();
        assert_eq!(key, "omnigraph.branch.agent-7.01JC0000000000000000000000");
        assert_eq!(value.split('|').count(), 6);
        let rows = decoded(None, &[(key, value)]).unwrap();
        assert_eq!(rows.live_count(), 1);
        let read = rows.branch(&written.native).unwrap();
        assert_eq!(read, &written);
        assert_eq!(read.logical(), "agent-7");
        assert!(read.parent_is_main());
        assert_eq!(
            rows.by_logical("agent-7").map(|row| row.native.as_str()),
            Some(written.native.as_str())
        );
    }

    #[test]
    fn a_tombstone_round_trips_on_a_tree_and_is_ignored_on_main() {
        let tombstone = TombstoneRow {
            attempt_id: ATTEMPT.to_string(),
            deleted_at: 1_763_500_100,
        };
        let entries = [tombstone.entry()];
        assert_eq!(entries[0].0, TOMBSTONE_ENTRY_KEY);
        let on_tree = decoded(Some("agent-7.01JC0000000000000000000000"), &entries).unwrap();
        assert_eq!(on_tree.tombstone(), Some(&tombstone));
        let on_main = decoded(None, &entries).unwrap();
        assert!(on_main.is_empty());
    }

    #[test]
    fn a_tree_ignores_the_branch_entries_its_clone_copied() {
        let entries = [row("agent-7.01JC0000000000000000000000", MAIN_BRANCH_HEAD_KEY).entry()];
        let on_tree = decoded(Some("agent-7.01JC0000000000000000000000"), &entries).unwrap();
        assert!(on_tree.is_empty());
    }

    #[test]
    fn a_retired_entry_is_skipped_and_remembered() {
        let native = "agent-7.01JC0000000000000000000000";
        let rows = decoded(None, &[retired_entry(native)]).unwrap();
        assert_eq!(rows.live_count(), 0);
        assert!(rows.is_retired(native));
        assert_eq!(rows.retired().collect::<Vec<_>>(), vec![native]);
        assert!(rows.by_logical("agent-7").is_none());
    }

    #[test]
    fn the_row_set_of_a_version_equals_its_entries() {
        let first = row("a.01JC0000000000000000000000", MAIN_BRANCH_HEAD_KEY);
        let second = row("team/b.01JC0000000000000000000001", &first.native);
        let legacy = row("legacy", MAIN_BRANCH_HEAD_KEY);
        let rows = decoded(
            None,
            &[
                first.entry(),
                second.entry(),
                legacy.entry(),
                retired_entry("gone.01JC0000000000000000000002"),
                ("lance.auto_cleanup.interval".to_string(), "7".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(
            rows.live()
                .map(|row| row.native.as_str())
                .collect::<Vec<_>>(),
            vec![
                first.native.as_str(),
                legacy.native.as_str(),
                second.native.as_str()
            ]
        );
        assert_eq!(rows.branch(&second.native).unwrap().parent, first.native);
        assert_eq!(rows.branch("legacy").unwrap().logical(), "legacy");
        assert!(rows.is_retired("gone.01JC0000000000000000000002"));
    }

    #[test]
    fn a_create_is_refused_by_the_live_names_above_and_below_it() {
        let rows = decoded(
            None,
            &[
                row("team/a.01JC0000000000000000000000", MAIN_BRANCH_HEAD_KEY).entry(),
                retired_entry("old"),
            ],
        )
        .unwrap();
        for target in ["team/a", "team", "team/a/x"] {
            let error = rows.refuse_create_overlap(target).unwrap_err();
            assert!(
                matches!(&error, OmniError::BranchExists { branch, live } if branch == target && live == "team/a"),
                "{target}: {error}"
            );
        }
        assert!(rows.refuse_create_overlap("team/b").is_ok());
        assert!(rows.refuse_create_overlap("old").is_ok());
        let error = rows.refuse_create_overlap("old/child").unwrap_err();
        assert!(
            error.to_string().contains("retired branch 'old'"),
            "{error}"
        );
    }

    #[test]
    fn two_live_rows_under_one_logical_name_fail_the_decode() {
        let first = row("a.01JC0000000000000000000000", MAIN_BRANCH_HEAD_KEY);
        let second = row("a.01JC0000000000000000000001", MAIN_BRANCH_HEAD_KEY);
        let error = decoded(None, &[first.entry(), second.entry()]).unwrap_err();
        assert!(
            error.to_string().contains("two live branch rows"),
            "{error}"
        );
    }

    #[test]
    fn a_malformed_entry_fails_naming_its_key() {
        let key = branch_entry_key("a.01JC0000000000000000000000");
        let good = row("a.01JC0000000000000000000000", MAIN_BRANCH_HEAD_KEY)
            .entry()
            .1;
        let identifier = identifier_text(&BranchIdentifier::new(&BranchIdentifier::main(), 3));
        let cases = [
            ("2|".to_string() + &good[2..], "encoding version '2'"),
            (good.rsplit_once('|').unwrap().0.to_string(), "5 fields"),
            (
                format!("1|{identifier}|main|three|1|{ATTEMPT}"),
                "parent_version",
            ),
            (
                format!("1|{identifier}||3|1|{ATTEMPT}"),
                "parent field is empty",
            ),
            (format!("1|{identifier}|main|3|1|not-a-ulid"), "not a ULID"),
            (format!("1|3|main|3|1|{ATTEMPT}"), "identifier"),
            (
                format!("1|3:ZZ|main|3|1|{ATTEMPT}"),
                "lowercase hexadecimal",
            ),
            ("1|".to_string(), "fields"),
        ];
        for (value, expected) in cases {
            let error = decoded(None, &[(key.clone(), value.clone())]).unwrap_err();
            let message = error.to_string();
            assert!(message.contains(&key), "{value}: {message}");
            assert!(message.contains(expected), "{value}: {message}");
        }
        let tombstone_error = decoded(
            Some("a.01JC0000000000000000000000"),
            &[(TOMBSTONE_ENTRY_KEY.to_string(), format!("1|{ATTEMPT}"))],
        )
        .unwrap_err();
        assert!(tombstone_error.to_string().contains(TOMBSTONE_ENTRY_KEY));
        assert!(tombstone_error.to_string().contains("2 fields"));
    }
}
