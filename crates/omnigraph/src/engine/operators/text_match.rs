//! The Aho-Corasick matcher over the sorted distinct `(len, text)` non-empty
//! needles and its memory model, built once by the scan's runtime filter and
//! shared with the join's needle rows. It charges no pool and reads no batch.

use aho_corasick::{AhoCorasick, AhoCorasickKind, MatchKind};

/// One entry of the set that deduplicates the needles, ordered by length; its
/// text is borrowed from the collected left side, which is charged already.
pub(super) const SET_ENTRY_BYTES: usize = std::mem::size_of::<(usize, &str)>() + 32;

/// Row sizes read from aho-corasick 1.1.4's `nfa/noncontiguous.rs` (the peak test
/// catches drift): a `State` five `u32`s, a `Transition` a byte and two `u32`s,
/// a `Match` two `u32`s, a dense cell or pattern length one `u32`.
const STATE_BYTES: usize = 20;
const TRANSITION_BYTES: usize = 9;
const MATCH_BYTES: usize = 8;
const ID_BYTES: usize = 4;

/// The build's fixed-size parts: byte classes, prefilter tables, headers.
const BUILD_SLACK_BYTES: usize = 64 << 10;

/// The automaton over the sorted distinct `needles`, pattern `p` the `p`th:
/// one pass per text, built only as the noncontiguous NFA kind, whose tables
/// `build_peak_bytes` models.
pub(super) fn build(needles: &[(usize, &str)]) -> Option<AhoCorasick> {
    AhoCorasick::builder()
        .match_kind(MatchKind::Standard)
        .kind(Some(AhoCorasickKind::NoncontiguousNFA))
        .build(needles.iter().map(|(_, needle)| needle))
        .ok()
}

/// The most needles that are proper suffixes of one needle. A state's match
/// list holds the needles its text ends with: the longest and its suffixes.
fn most_nested_suffixes(needles: &[(usize, &str)]) -> usize {
    let mut lengths: Vec<usize> = Vec::new();
    for (len, _) in needles {
        if lengths.last() != Some(len) {
            lengths.push(*len);
        }
    }
    needles
        .iter()
        .map(|&(len, needle)| {
            lengths
                .iter()
                .take_while(|&&shorter| shorter < len)
                .filter(|&&shorter| {
                    let start = len - shorter;
                    needle.is_char_boundary(start)
                        && needles.binary_search(&(shorter, &needle[start..])).is_ok()
                })
                .count()
        })
        .max()
        .unwrap_or(0)
}

/// The most `build` holds at once for non-empty `needles`: every table at
/// three times its final length (a doubling `Vec` and its reallocation copy),
/// plus the shuffle's two state maps and the failure pass's queue.
pub(super) fn build_peak_bytes(needles: &[(usize, &str)]) -> usize {
    let patterns = needles.len();
    let bytes = needles
        .iter()
        .fold(0usize, |sum, (len, _)| sum.saturating_add(*len));
    let longest = needles.last().map_or(0, |&(len, _)| len);
    let mut seen = [false; 256];
    for (_, needle) in needles {
        for &byte in needle.as_bytes() {
            seen[usize::from(byte)] = true;
        }
    }
    let distinct = seen.iter().filter(|&&seen| seen).count();
    let classes = distinct.saturating_mul(2).saturating_add(1).min(256);
    let states = bytes.saturating_add(4);
    let transitions = bytes.saturating_add(3 * 256 + 1);
    let matches = bytes
        .saturating_mul(most_nested_suffixes(needles).saturating_add(1))
        .saturating_add(1);
    let dense_rows = (1..=3u32)
        .map(|depth| patterns.min(distinct.saturating_pow(depth)))
        .fold(2usize, usize::saturating_add);
    let ids = dense_rows
        .saturating_mul(classes)
        .saturating_add(1)
        .saturating_add(patterns);
    let tables = states
        .saturating_mul(STATE_BYTES)
        .saturating_add(transitions.saturating_mul(TRANSITION_BYTES))
        .saturating_add(matches.saturating_mul(MATCH_BYTES))
        .saturating_add(ids.saturating_mul(ID_BYTES));
    tables
        .saturating_mul(3)
        .saturating_add(states.saturating_mul(5 * ID_BYTES))
        .saturating_add(longest.saturating_mul(2))
        .saturating_add(BUILD_SLACK_BYTES)
}

#[cfg(test)]
mod tests {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    use super::*;

    /// `needles` sorted and deduplicated as `(len, text)`, the build's input.
    fn needle_set(needles: &[String]) -> Vec<(usize, &str)> {
        let mut set: Vec<(usize, &str)> = needles
            .iter()
            .map(|needle| (needle.len(), needle.as_str()))
            .collect();
        set.sort_unstable();
        set.dedup();
        set
    }

    /// The system allocator, counting the live bytes of a thread that turned
    /// `COUNTING` on; a reallocation counts the new block before the old goes.
    /// Every `unsafe` block below forwards the call to `System` unchanged.
    struct Counting;

    thread_local! {
        static COUNTING: Cell<bool> = const { Cell::new(false) };
        static LIVE: Cell<isize> = const { Cell::new(0) };
        static PEAK: Cell<isize> = const { Cell::new(0) };
    }

    fn count(bytes: isize) {
        if COUNTING.get() {
            let live = LIVE.get() + bytes;
            LIVE.set(live);
            PEAK.set(PEAK.get().max(live));
        }
    }

    // SAFETY: every method forwards the caller's arguments to `System` unchanged; the counters allocate nothing.
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            // SAFETY: `layout` is the caller's, passed through unchanged.
            let block = unsafe { System.alloc(layout) };
            if !block.is_null() {
                count(layout.size() as isize);
            }
            block
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            // SAFETY: `layout` is the caller's, passed through unchanged.
            let block = unsafe { System.alloc_zeroed(layout) };
            if !block.is_null() {
                count(layout.size() as isize);
            }
            block
        }

        unsafe fn dealloc(&self, block: *mut u8, layout: Layout) {
            // SAFETY: `block` came from `System` through the methods above, with this same `layout`.
            unsafe { System.dealloc(block, layout) };
            count(-(layout.size() as isize));
        }

        unsafe fn realloc(&self, block: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            // SAFETY: `block` came from `System` with `layout`; `new_size` is the caller's, unchanged.
            let moved = unsafe { System.realloc(block, layout, new_size) };
            if !moved.is_null() {
                count(new_size as isize);
                count(-(layout.size() as isize));
            }
            moved
        }
    }

    #[global_allocator]
    static ALLOCATOR: Counting = Counting;

    /// The most bytes `build` held at once on this thread, and the size of
    /// the matcher it returned.
    fn build_peak(set: &[(usize, &str)]) -> (usize, usize) {
        LIVE.set(0);
        PEAK.set(0);
        COUNTING.set(true);
        let matcher = build(set);
        COUNTING.set(false);
        let used = matcher.as_ref().map_or(0, AhoCorasick::memory_usage);
        drop(matcher);
        (usize::try_from(PEAK.get()).unwrap_or(0), used)
    }

    fn numbers(count: usize) -> Vec<String> {
        (0..count).map(|n| format!("1000-{n:05}")).collect()
    }

    /// Nested suffixes under one long needle: a state holds a match per length.
    fn suffixes() -> Vec<String> {
        let mut suffixes: Vec<String> = (1..=64).map(|len| "a".repeat(len)).collect();
        suffixes.push("a".repeat(4096));
        suffixes
    }

    /// A thousand needles of 200 lengths, 10 to 209 bytes, none a suffix of
    /// another.
    fn many_lengths() -> Vec<String> {
        (0..1_000)
            .map(|n| format!("{n:04}{}", "-".repeat(6 + n / 5)))
            .collect()
    }

    /// A thousand CJK needles with Latin letters, 1 to 13 ideographs and a
    /// tail unique to each (`{n:03}` and one ideograph), so none is a suffix
    /// of another; three-byte UTF-8 spans far more byte values than digits.
    fn mixed_cjk_latin() -> Vec<String> {
        let ideograph = |n: usize| char::from_u32(0x4E00 + (n % 20_000) as u32).unwrap();
        (0..1_000)
            .map(|n| {
                let body: String = (0..=n % 13)
                    .map(|i| {
                        let latin = char::from(b'a' + ((n + i) % 26) as u8);
                        format!("{}{latin}", ideograph(n * 37 + i * 101))
                    })
                    .collect();
                format!("{body}{n:03}{}", ideograph(n * 7919))
            })
            .collect()
    }

    /// Issue 775's 266 matter numbers, 25 times as many, nested suffixes,
    /// many lengths and mixed CJK/Latin: the build never holds more than the
    /// charge taken for it, and each 1,000-needle fill fits the 150 MiB pool.
    #[test]
    fn the_charge_taken_for_the_build_covers_its_peak() {
        for needles in [
            numbers(266),
            numbers(6_500),
            suffixes(),
            many_lengths(),
            mixed_cjk_latin(),
        ] {
            let set = needle_set(&needles);
            let (peak, used) = build_peak(&set);
            let charged = build_peak_bytes(&set);
            assert!(used > 0 && peak >= used, "{peak} counted, {used} kept");
            assert!(
                peak <= charged,
                "{} needles: {peak} held at once, {charged} charged",
                set.len()
            );
            if set.len() == 1_000 {
                let fill = set
                    .len()
                    .saturating_mul(SET_ENTRY_BYTES)
                    .saturating_add(charged);
                assert!(fill <= 150 << 20, "{fill} charged for the fill");
            }
        }
    }
}
