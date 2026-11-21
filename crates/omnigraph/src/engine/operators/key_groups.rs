//! Distinct Utf8 identity keys numbered by first appearance, hashed in one
//! vectorized pass with DataFusion's `create_hashes` into a `HashTable`. A
//! group holds the row of its key's first appearance, so the table stores no
//! string copy and two keys with one hash stay separate groups, told apart
//! by their strings. This is the one string-keyed table of the engine's
//! operators: the hash join's build side, the key-set block's outer keys and
//! the bound-edge source grouping all use it, so null handling, collision
//! comparison and the hash seed have one owner.

use arrow_array::{Array, StringArray};
use datafusion::common::Result as DfResult;
use datafusion::common::hash_utils::{RandomState, create_hashes};
use hashbrown::HashTable;

use super::memory::WorkMemory;
use crate::error::OmniError;

/// The groups' hash, keyed at random per build: the keys are node ids that
/// writers choose, and a public fixed key would let ids that share a bucket
/// make a build and its probes quadratic. Only foldhash's per-hasher seed
/// is random: DataFusion's `RandomState` is foldhash's `FixedState`, which
/// always hashes with the public `SharedSeed::global_fixed()`, and
/// `create_hashes` accepts no other state. That still removes the fixed
/// public key; foldhash does not claim to resist an interactive attacker.
/// Nothing depends on the hash values: groups are numbered by first
/// appearance and a probe answers in its own row order, and a build keeps
/// the state its probes hash with.
fn key_hash_state() -> RandomState {
    use std::hash::BuildHasher;
    RandomState::with_seed(std::collections::hash_map::RandomState::new().hash_one(0u64))
}

/// The groups of one Utf8 key array: `(hash, group)` entries, and per group
/// the row of `keys` where it first appears. `keys` is an `Arc`-backed clone
/// of the caller's array, so the groups borrow nothing.
pub(in crate::engine) struct KeyGroups {
    keys: StringArray,
    state: RandomState,
    table: HashTable<(u64, u32)>,
    firsts: Vec<u32>,
}

impl KeyGroups {
    /// What the groups of `rows` keys hold at most: a table entry and a
    /// first row per group, every row a group, by `entries`' hash-table
    /// convention.
    pub(in crate::engine) fn group_bytes(rows: usize) -> usize {
        rows.saturating_mul(
            WorkMemory::entry_bytes::<(u64, u32)>()
                .saturating_add(WorkMemory::entry_bytes::<u32>()),
        )
    }

    /// Charges `memory` for what [`Self::build_numbered`] holds over `rows`
    /// input rows at most: [`Self::group_bytes`], plus the exact hash buffer
    /// and per-row groups.
    pub(in crate::engine) fn charge(memory: &WorkMemory, rows: usize) -> DfResult<()> {
        memory.grow(Self::group_bytes(rows))?;
        memory.grow(
            rows.saturating_mul(std::mem::size_of::<u64>() + std::mem::size_of::<Option<u32>>()),
        )
    }

    /// Every non-null key of `keys` numbered by first appearance. Hashing
    /// holds one `u64` per row until it returns.
    pub(in crate::engine) fn build(keys: &StringArray) -> DfResult<Self> {
        Self::hashed(keys, |_| {})
    }

    /// [`Self::build`], and each row's group (`None` for a null key).
    pub(in crate::engine) fn build_numbered(
        keys: &StringArray,
    ) -> DfResult<(Self, Vec<Option<u32>>)> {
        let mut row_groups = Vec::with_capacity(keys.len());
        let groups = Self::hashed(keys, |group| row_groups.push(group))?;
        Ok((groups, row_groups))
    }

    fn hashed(keys: &StringArray, on_row: impl FnMut(Option<u32>)) -> DfResult<Self> {
        u32::try_from(keys.len()).map_err(|_| {
            super::external(OmniError::manifest_internal(
                "key groups exceed the UInt32 row range",
            ))
        })?;
        let state = key_hash_state();
        let mut hashes = vec![0u64; keys.len()];
        create_hashes([keys as &dyn Array], &state, &mut hashes)?;
        Ok(Self::from_hashes(keys, state, &hashes, on_row))
    }

    /// The groups of `keys` hashed as `hashes`, each row's group passed to
    /// `on_row` in row order. The table is sized for every row a group, which
    /// [`Self::group_bytes`] already charges, so it never rehashes.
    fn from_hashes(
        keys: &StringArray,
        state: RandomState,
        hashes: &[u64],
        mut on_row: impl FnMut(Option<u32>),
    ) -> Self {
        let mut groups = Self {
            keys: keys.clone(),
            state,
            table: HashTable::with_capacity(keys.len()),
            firsts: Vec::with_capacity(keys.len()),
        };
        for (row, &hash) in hashes.iter().enumerate() {
            if keys.is_null(row) {
                on_row(None);
                continue;
            }
            let group = match groups.group_of(hash, keys.value(row)) {
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
            on_row(Some(group));
        }
        groups
    }

    /// The group of `key`, hashed as `hash` by [`Self::hash`].
    pub(in crate::engine) fn group_of(&self, hash: u64, key: &str) -> Option<u32> {
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

    /// The row of the build keys where `group` first appears.
    pub(in crate::engine) fn first_row(&self, group: u32) -> u32 {
        self.firsts[group as usize]
    }

    /// The key of `group`.
    pub(in crate::engine) fn key(&self, group: u32) -> &str {
        self.keys.value(self.first_row(group) as usize)
    }

    /// `hashes` filled with the hash of every row of `probe` (nulls included,
    /// which a caller skips), as the build hashed its keys. `hashes` is the
    /// caller's reusable buffer.
    pub(in crate::engine) fn hash(
        &self,
        probe: &StringArray,
        hashes: &mut Vec<u64>,
    ) -> DfResult<()> {
        hashes.clear();
        hashes.resize(probe.len(), 0);
        create_hashes([probe as &dyn Array], &self.state, hashes)?;
        Ok(())
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
        self.hash(probe, hashes)?;
        groups.clear();
        groups.extend(hashes.iter().enumerate().map(|(row, &hash)| {
            probe
                .is_valid(row)
                .then(|| self.group_of(hash, probe.value(row)))
                .flatten()
        }));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

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
        let (groups, row_groups) = KeyGroups::build_numbered(&keys).unwrap();
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
            let first = (0..keys.len())
                .find(|row| keys.is_valid(*row) && keys.value(*row) == *key)
                .unwrap();
            assert_eq!(groups.first_row(*group) as usize, first);
        }
        let unnumbered = KeyGroups::build(&keys).unwrap();
        assert_eq!(unnumbered.firsts, groups.firsts);

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

    /// A group keeps the first row of a duplicate key, two keys with one
    /// hash stay separate groups told apart by their strings, and a null
    /// never joins a group, even one whose hash it shares.
    #[test]
    fn key_groups_keep_the_first_row_and_resolve_a_collision() {
        let keys = StringArray::from(vec![Some("a"), Some("b"), Some("a"), None, Some("c")]);
        // `a` and `b` collide; the null shares `c`'s hash and must stay out.
        let hashes = [7, 7, 7, 9, 9];
        let mut row_groups = Vec::new();
        let groups = KeyGroups::from_hashes(&keys, key_hash_state(), &hashes, |group| {
            row_groups.push(group)
        });
        assert_eq!(groups.len(), 3);
        assert_eq!(row_groups, [Some(0), Some(1), Some(0), None, Some(2)]);
        let first = |hash, key| groups.group_of(hash, key).map(|g| groups.first_row(g));
        assert_eq!(first(7, "a"), Some(0));
        assert_eq!(first(7, "b"), Some(1));
        assert_eq!(first(9, "c"), Some(4));
        assert_eq!(first(7, "c"), None);
        assert_eq!(first(9, "a"), None);
    }

    /// The string map the groups replaced, kept here only as the
    /// instrument's baseline: one SipHash lookup per row.
    fn string_map_groups(keys: &StringArray, probe: &StringArray) -> (usize, usize) {
        let mut groups: HashMap<&str, u32> = HashMap::new();
        let mut row_groups = Vec::with_capacity(keys.len());
        for row in 0..keys.len() {
            row_groups.push(keys.is_valid(row).then(|| {
                let next = groups.len() as u32;
                *groups.entry(keys.value(row)).or_insert(next)
            }));
        }
        let found: Vec<Option<u32>> = (0..probe.len())
            .map(|row| {
                probe
                    .is_valid(row)
                    .then(|| groups.get(probe.value(row)).copied())
                    .flatten()
            })
            .collect();
        (groups.len(), found.iter().flatten().count())
    }

    fn best_of(runs: usize, mut run: impl FnMut()) -> Duration {
        (0..runs)
            .map(|_| {
                let start = Instant::now();
                run();
                start.elapsed()
            })
            .min()
            .unwrap()
    }

    /// Decision instrument for the CPU claim of `KeyGroups` against the
    /// string map it replaced: 100,000 outer keys (50,000 distinct) built
    /// and 1,000,000 inner keys probed, best of 10, nanoseconds per row.
    /// Run in a release build:
    /// `cargo test --release -p omnigraph-engine --lib key_groups_against_a_string_map_cost -- --ignored --nocapture`.
    #[test]
    #[ignore = "instrument: CPU per row of KeyGroups against the std string map it replaced (run with --release)"]
    fn key_groups_against_a_string_map_cost() {
        let outer =
            StringArray::from_iter_values((0..100_000).map(|row| format!("id-{}", row % 50_000)));
        let inner = StringArray::from_iter_values(
            (0..1_000_000).map(|row| format!("id-{}", (row * 7) % 75_000)),
        );
        let rows = (outer.len() + inner.len()) as f64;
        let expected = string_map_groups(&outer, &inner);
        let map = best_of(10, || {
            assert_eq!(string_map_groups(&outer, &inner), expected);
        });
        let (mut hashes, mut found) = (Vec::new(), Vec::new());
        let table = best_of(10, || {
            let (groups, _row_groups) = KeyGroups::build_numbered(&outer).unwrap();
            groups.probe(&inner, &mut hashes, &mut found).unwrap();
            assert_eq!((groups.len(), found.iter().flatten().count()), expected);
        });
        println!(
            "key groups: string map {:.1} ns/row, KeyGroups {:.1} ns/row",
            map.as_nanos() as f64 / rows,
            table.as_nanos() as f64 / rows,
        );
    }
}
