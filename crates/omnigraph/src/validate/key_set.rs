//! An unchained hash set over integer ids for the load's endpoint check: a
//! directory of `(end offset << 16 | 16-bit Bloom tag)` entries over keys
//! stored contiguously by hash prefix, so a probe that misses is answered by
//! the tag without touching the keys (Birler, Schmidt, Fent, Neumann, "Simple,
//! Efficient, and Robust Hash Tables for Join Processing", DaMoN 2024).

const TAG_BITS: u32 = 16;
const MAX_LINEAR_BUCKET: usize = 32;

/// Checked sizes shared by memory admission and allocation. Offsets leave the
/// low 16 directory bits for tags; every vector must also fit Rust's byte limit.
pub(crate) struct IntKeySetLayout {
    len: usize,
    slots: usize,
    pub(crate) retained_bytes: usize,
    pub(crate) scratch_bytes: usize,
}

impl IntKeySetLayout {
    pub(crate) fn new(len: usize) -> Option<Self> {
        if u64::try_from(len).ok()? > u64::MAX >> TAG_BITS {
            return None;
        }
        let wanted = len.checked_mul(9)? / 8;
        let slots = wanted.max(2).checked_next_power_of_two()?;
        let retained_bytes = key_bytes(slots.checked_add(1)?)?.checked_add(key_bytes(len)?)?;
        let scratch_bytes = slots.checked_mul(size_of::<usize>())?;
        if scratch_bytes > isize::MAX as usize {
            return None;
        }
        Some(Self {
            len,
            slots,
            retained_bytes,
            scratch_bytes,
        })
    }
}

pub(crate) fn key_bytes(len: usize) -> Option<usize> {
    len.checked_mul(size_of::<i64>())
        .filter(|&bytes| bytes <= isize::MAX as usize)
}

/// The committed integer ids of one node table, built once per statement and
/// probed per endpoint value of the delta.
pub(crate) struct IntKeySet {
    shift: u32,
    directory: Vec<u64>,
    keys: Vec<i64>,
}

impl IntKeySet {
    /// Count per slot, exclusive prefix sum, copy: the keys land contiguously
    /// in slot order and each slot's tag is the OR of its keys' tags.
    pub(crate) fn build(keys: &[i64], layout: IntKeySetLayout) -> Self {
        debug_assert_eq!(keys.len(), layout.len);
        let slots = layout.slots;
        let bits = slots.trailing_zeros();
        let shift = 64 - bits;
        let mut directory = vec![0u64; slots + 1];
        for &key in keys {
            directory[slot_of(mix(key), shift) + 1] += 1 << TAG_BITS;
        }
        for slot in 1..=slots {
            directory[slot] += directory[slot - 1];
        }
        let mut fill = vec![0usize; slots];
        for (offset, entry) in fill.iter_mut().zip(&directory) {
            *offset = (entry >> TAG_BITS) as usize;
        }
        let mut placed = vec![0i64; keys.len()];
        for &key in keys {
            let hash = mix(key);
            let slot = slot_of(hash, shift);
            placed[fill[slot]] = key;
            fill[slot] += 1;
            directory[slot + 1] |= u64::from(tag(hash));
        }
        drop(fill);
        for offsets in directory.windows(2) {
            let start = (offsets[0] >> TAG_BITS) as usize;
            let end = (offsets[1] >> TAG_BITS) as usize;
            if end - start > MAX_LINEAR_BUCKET {
                placed[start..end].sort_unstable();
            }
        }
        Self {
            shift,
            directory,
            keys: placed,
        }
    }

    pub(crate) fn contains(&self, key: i64) -> bool {
        let hash = mix(key);
        let slot = slot_of(hash, self.shift);
        let entry = self.directory[slot + 1];
        if tag(hash) & !(entry as u16) != 0 {
            return false;
        }
        let start = (self.directory[slot] >> TAG_BITS) as usize;
        let end = (entry >> TAG_BITS) as usize;
        let bucket = &self.keys[start..end];
        if bucket.len() > MAX_LINEAR_BUCKET {
            bucket.binary_search(&key).is_ok()
        } else {
            bucket.contains(&key)
        }
    }
}

fn slot_of(hash: u64, shift: u32) -> usize {
    (hash >> shift) as usize
}

/// The 64-bit finalizer of splitmix64: slots take the high bits, tags the low.
fn mix(key: i64) -> u64 {
    let mut hash = key as u64;
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}

/// Four bits of a 16-bit Bloom tag from the low hash bits (the paper's optimum).
fn tag(hash: u64) -> u16 {
    let low = hash as u16;
    (1 << (low & 15))
        | (1 << ((low >> 4) & 15))
        | (1 << ((low >> 8) & 15))
        | (1 << ((low >> 12) & 15))
}

/// `text` as the integer whose canonical id it is: the exact spelling
/// `i64::to_string` produces, so a non-canonical form (`007`, `-0`, `+1`) is
/// not an id of an integer-keyed table and parses as `None`.
pub(crate) fn canonical_i64(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    let (negative, digits) = match bytes {
        [b'-', rest @ ..] => (true, rest),
        _ => (false, bytes),
    };
    if digits.is_empty() || (digits[0] == b'0' && (digits.len() > 1 || negative)) {
        return None;
    }
    let mut value: i64 = 0;
    for &byte in digits {
        if !byte.is_ascii_digit() {
            return None;
        }
        let digit = i64::from(byte - b'0');
        value = value.checked_mul(10)?;
        value = if negative {
            value.checked_sub(digit)?
        } else {
            value.checked_add(digit)?
        };
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(keys: &[i64]) -> IntKeySet {
        IntKeySet::build(keys, IntKeySetLayout::new(keys.len()).unwrap())
    }

    #[test]
    fn layout_rejects_overflow_without_allocating() {
        assert!(IntKeySetLayout::new(usize::MAX).is_none());
        if let Ok(len) = usize::try_from(1u64 << 48) {
            assert!(IntKeySetLayout::new(len).is_none());
        }
        for len in [0, 1, 32, 33, 16_384] {
            let layout = IntKeySetLayout::new(len).unwrap();
            let retained = layout.retained_bytes;
            let scratch = layout.scratch_bytes;
            let keys = vec![0; len];
            let set = IntKeySet::build(&keys, layout);
            assert_eq!(
                retained,
                set.keys.capacity() * size_of::<i64>()
                    + set.directory.capacity() * size_of::<u64>()
            );
            assert_eq!(scratch, (set.directory.len() - 1) * size_of::<usize>());
        }
    }

    #[test]
    fn members_hit_and_non_members_miss() {
        let keys: Vec<i64> = (0..100_000).map(|i| i * 7 - 50_000).collect();
        let set = build(&keys);
        for &key in &keys {
            assert!(set.contains(key), "{key} is a member");
        }
        for key in [-50_001, 2, 3, 4, 349_995, 649_994, i64::MAX, i64::MIN] {
            assert!(!set.contains(key), "{key} is not a member");
        }
    }

    #[test]
    fn empty_and_singleton_sets() {
        let empty = build(&[]);
        assert!(!empty.contains(0));
        let one = build(&[42]);
        assert!(one.contains(42));
        assert!(!one.contains(41));
    }

    #[test]
    fn duplicates_stay_members() {
        let set = build(&[5, 5, 5, 9]);
        assert!(set.contains(5));
        assert!(set.contains(9));
        assert!(!set.contains(7));
    }

    #[test]
    fn colliding_buckets_bound_lookup_work() {
        fn unmix(mut hash: u64) -> i64 {
            hash ^= (hash >> 31) ^ (hash >> 62);
            hash = hash.wrapping_mul(0x3196_42b2_d24d_8ec3);
            hash ^= (hash >> 27) ^ (hash >> 54);
            hash = hash.wrapping_mul(0x96de_1b17_3f11_9089);
            hash ^= (hash >> 30) ^ (hash >> 60);
            hash as i64
        }
        for count in [32, 33, 4096] {
            let keys: Vec<_> = (0..count)
                .map(|i| unmix(((2 * i) << 16) | 0x1234))
                .collect();
            let set = build(&keys);
            assert!(keys.iter().all(|&key| slot_of(mix(key), set.shift) == 0));
            if count > 32 {
                assert!(
                    set.keys.is_sorted(),
                    "oversized buckets must use ordered lookup"
                );
            }
            for (i, &key) in keys.iter().enumerate() {
                assert!(set.contains(key));
                assert!(!set.contains(unmix(((2 * i as u64 + 1) << 16) | 0x1234)));
            }
        }
        let repeated = build(&[5; 64]);
        assert!(repeated.contains(5));
        assert!(!repeated.contains(6));
    }

    #[test]
    fn canonical_integers_only() {
        for (text, want) in [
            ("0", Some(0)),
            ("933", Some(933)),
            ("-12", Some(-12)),
            ("9223372036854775807", Some(i64::MAX)),
            ("-9223372036854775808", Some(i64::MIN)),
        ] {
            assert_eq!(canonical_i64(text), want, "{text}");
        }
        for text in [
            "",
            "-",
            "-0",
            "007",
            "+1",
            "1 ",
            "12a",
            "9223372036854775808",
            "-9223372036854775809",
            "10000000000000000000",
            "1.0",
        ] {
            assert_eq!(canonical_i64(text), None, "{text}");
        }
        for key in [0, 1, -1, 1_000_000_007, i64::MAX, i64::MIN] {
            assert_eq!(canonical_i64(&key.to_string()), Some(key));
        }
    }
}
