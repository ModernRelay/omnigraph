//! Distinct Utf8 identity keys numbered by first appearance, hashed in one
//! vectorized pass with DataFusion's `create_hashes` into a `HashTable`, the
//! pattern `hash_join` uses for its build side. A group holds the row of its
//! key's first appearance, so the table stores no string copy and two keys
//! with one hash stay separate groups, told apart by their strings.

use arrow_array::{Array, StringArray};
use datafusion::common::Result as DfResult;
use datafusion::common::hash_utils::{RandomState, create_hashes};
use hashbrown::HashTable;

use super::hash_join::HASH_SEED;
use super::memory::WorkMemory;
use crate::error::OmniError;

/// The groups of one Utf8 key array: `(hash, group)` entries, and per group
/// the row of `keys` where it first appears.
pub(in crate::engine) struct KeyGroups<'a> {
    keys: &'a StringArray,
    state: RandomState,
    table: HashTable<(u64, u32)>,
    firsts: Vec<u32>,
}

impl<'a> KeyGroups<'a> {
    /// Charges `memory` for what [`Self::build`] holds over `rows` input
    /// rows at most: a table entry and a first row per group (every row a
    /// group, by `entries`' hash-table convention), plus the exact hash
    /// buffer and per-row groups.
    pub(in crate::engine) fn charge(memory: &WorkMemory, rows: usize) -> DfResult<()> {
        memory.entries::<(u64, u32)>(rows)?;
        memory.entries::<u32>(rows)?;
        memory.grow(
            rows.saturating_mul(std::mem::size_of::<u64>() + std::mem::size_of::<Option<u32>>()),
        )
    }

    /// Every non-null key of `keys` numbered by first appearance, and each
    /// row's group (`None` for a null key).
    pub(in crate::engine) fn build(keys: &'a StringArray) -> DfResult<(Self, Vec<Option<u32>>)> {
        u32::try_from(keys.len()).map_err(|_| {
            super::external(OmniError::manifest_internal(
                "key groups exceed the UInt32 row range",
            ))
        })?;
        let state = RandomState::with_seed(HASH_SEED);
        let mut hashes = vec![0u64; keys.len()];
        create_hashes([keys as &dyn Array], &state, &mut hashes)?;
        let mut groups = Self {
            keys,
            state,
            table: HashTable::new(),
            firsts: Vec::new(),
        };
        let mut row_groups = Vec::with_capacity(keys.len());
        for (row, &hash) in hashes.iter().enumerate() {
            if keys.is_null(row) {
                row_groups.push(None);
                continue;
            }
            let group = match groups.find(hash, keys.value(row)) {
                Some(group) => group,
                None => {
                    let group = groups.firsts.len() as u32;
                    groups.firsts.push(row as u32);
                    groups
                        .table
                        .insert_unique(hash, (hash, group), |&(hash, _)| hash);
                    group
                }
            };
            row_groups.push(Some(group));
        }
        Ok((groups, row_groups))
    }

    fn find(&self, hash: u64, key: &str) -> Option<u32> {
        self.table
            .find(hash, |&(entry_hash, group)| {
                entry_hash == hash && self.key(group) == key
            })
            .map(|&(_, group)| group)
    }

    /// The number of groups.
    pub(in crate::engine) fn len(&self) -> usize {
        self.firsts.len()
    }

    /// The key of `group`.
    pub(in crate::engine) fn key(&self, group: u32) -> &'a str {
        self.keys.value(self.firsts[group as usize] as usize)
    }

    /// `groups` filled with the group of every row of `probe`: `None` for a
    /// null key or a key no group holds. `hashes` is the caller's reusable
    /// hash buffer.
    pub(in crate::engine) fn probe(
        &self,
        probe: &StringArray,
        hashes: &mut Vec<u64>,
        groups: &mut Vec<Option<u32>>,
    ) -> DfResult<()> {
        hashes.clear();
        hashes.resize(probe.len(), 0);
        create_hashes([probe as &dyn Array], &self.state, hashes)?;
        groups.clear();
        groups.extend(hashes.iter().enumerate().map(|(row, &hash)| {
            probe
                .is_valid(row)
                .then(|| self.find(hash, probe.value(row)))
                .flatten()
        }));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    /// The groups equal a string map's: one per distinct non-null key in
    /// first-appearance order, nulls in none, and a probe finds exactly the
    /// keys the build held, including keys that share a prefix or are empty.
    #[test]
    fn key_groups_number_keys_like_a_string_map_issue_1340() {
        let keys = StringArray::from(vec![
            Some("b"),
            None,
            Some("a"),
            Some("b"),
            Some(""),
            Some("ab"),
            Some("a"),
            None,
            Some(""),
        ]);
        let (groups, row_groups) = KeyGroups::build(&keys).unwrap();
        let mut expected: HashMap<&str, u32> = HashMap::new();
        let expected_rows: Vec<Option<u32>> = (0..keys.len())
            .map(|row| {
                keys.is_valid(row).then(|| {
                    let next = expected.len() as u32;
                    *expected.entry(keys.value(row)).or_insert(next)
                })
            })
            .collect();
        assert_eq!(row_groups, expected_rows);
        assert_eq!(groups.len(), expected.len());
        for (key, group) in &expected {
            assert_eq!(groups.key(*group), *key);
        }

        let probe = StringArray::from(vec![
            Some("a"),
            Some("c"),
            None,
            Some(""),
            Some("ab"),
            Some("b"),
            Some("abc"),
        ]);
        let (mut hashes, mut found) = (Vec::new(), Vec::new());
        groups.probe(&probe, &mut hashes, &mut found).unwrap();
        let expected_found: Vec<Option<u32>> = probe
            .iter()
            .map(|key| key.and_then(|key| expected.get(key).copied()))
            .collect();
        assert_eq!(found, expected_found);

        // A sliced probe hashes its own rows, not its parent's.
        let sliced = probe.slice(3, 3);
        groups.probe(&sliced, &mut hashes, &mut found).unwrap();
        assert_eq!(found, expected_found[3..6]);
    }
}
