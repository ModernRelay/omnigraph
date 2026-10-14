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

/// Every file of the skill, sorted by path.
pub const FILES: &[File] = include!(concat!(env!("OUT_DIR"), "/files.rs"));

/// The file at `path` below the skill root, if the skill has one.
pub fn file(path: &str) -> Option<&'static File> {
    FILES.iter().find(|file| file.path == path)
}
