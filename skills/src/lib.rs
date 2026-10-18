//! The `omnigraph` agent skill, as the CLI ships it.
//!
//! `skills/omnigraph` in the repository is the one copy of the skill. The
//! build script embeds every file below it byte for byte, so a binary carries
//! exactly the skill written beside its own source. Names starting with `.`
//! are not part of the skill.

/// One file of the skill.
#[derive(Debug, Clone, Copy)]
pub struct File {
    /// Path below the skill root, `/`-separated: `SKILL.md`,
    /// `references/queries.md`.
    pub path: &'static str,
    /// The file's bytes.
    pub contents: &'static [u8],
}

/// The skill's name: its directory name and the `name` its `SKILL.md`
/// declares.
pub const NAME: &str = "omnigraph";

/// The skill's entry point, relative to the skill root.
pub const ENTRY: &str = "SKILL.md";

/// Every file of the skill, sorted by path in byte order, which [`file`]
/// searches. A `static`, so the embedded bytes exist once in a binary.
pub static FILES: &[File] = include!(concat!(env!("OUT_DIR"), "/files.rs"));

/// The file at `path` below the skill root, if the skill has one.
pub fn file(path: &str) -> Option<&'static File> {
    FILES
        .binary_search_by_key(&path, |file| file.path)
        .ok()
        .map(|index| &FILES[index])
}

#[cfg(test)]
mod tests {
    use super::{FILES, file};

    #[test]
    fn files_are_sorted_and_each_is_found_by_its_path() {
        assert!(
            FILES.windows(2).all(|pair| pair[0].path < pair[1].path),
            "the generated table is not sorted by path"
        );
        for expected in FILES {
            let found = file(expected.path).unwrap_or_else(|| panic!("{}", expected.path));
            assert_eq!(found.path, expected.path);
            assert_eq!(found.contents, expected.contents);
        }
        assert!(file("references/absent.md").is_none());
        assert!(file("").is_none());
    }
}
