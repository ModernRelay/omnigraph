//! `omnigraph skill`: the agent skill built into this binary.
//!
//! The `omnigraph-skill` crate embeds `skills/omnigraph` byte for byte, so the
//! skill a consumer exports always matches the CLI that exports it. An export
//! writes the skill's files plus a record of what it wrote ([`RECORD`]); the
//! record is what lets a later `--force` export remove a file the skill no
//! longer has without ever touching a file the export did not write.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Write};
use std::path::{Component, Path};

use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The export record, written beside the skill's files: each written path
/// with the SHA-256 of the bytes written.
pub(crate) const RECORD: &str = ".omnigraph-skill.json";

/// Readers ignore record fields they do not know. A change to what `files`
/// means takes a new format, which older binaries refuse.
const RECORD_FORMAT: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
struct ExportRecord {
    format: u32,
    skill: String,
    skill_version: String,
    cli_version: String,
    http_contract: String,
    /// Path below the skill directory to the lowercase hex SHA-256 of the
    /// bytes the export wrote there.
    files: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SkillExportOutput {
    pub(crate) directory: String,
    pub(crate) skill: &'static str,
    pub(crate) skill_version: String,
    pub(crate) cli_version: &'static str,
    pub(crate) http_contract: &'static str,
    /// The skill files written below `directory`, in path order.
    pub(crate) files: Vec<&'static str>,
    /// The export record written beside them.
    pub(crate) record: &'static str,
    /// Files an earlier export recorded and this skill no longer has,
    /// removed because their bytes were unchanged.
    pub(crate) removed: Vec<String>,
    /// Such files left in place.
    pub(crate) kept: Vec<KeptFile>,
}

#[derive(Debug, Serialize)]
pub(crate) struct KeptFile {
    pub(crate) path: String,
    pub(crate) reason: KeptReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum KeptReason {
    /// Its bytes differ from the ones the earlier export wrote.
    Modified,
    /// It, or a directory on its path, is no longer a regular file or
    /// directory.
    NotAFile,
}

/// What lies at a skill path below the export directory, without following
/// a symbolic link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Entry {
    Absent,
    File,
    Symlink,
    /// A directory or another kind of entry.
    Other,
    /// A directory on the path exists as something other than a directory.
    Blocked,
}

/// `metadata.version` of the built-in `SKILL.md`.
pub(crate) fn skill_version() -> Result<String> {
    #[derive(Deserialize)]
    struct Frontmatter {
        name: String,
        metadata: Metadata,
    }
    #[derive(Deserialize)]
    struct Metadata {
        version: String,
    }
    let entry = omnigraph_skill::file(omnigraph_skill::ENTRY)
        .ok_or_else(|| eyre!("the built-in skill has no {}", omnigraph_skill::ENTRY))?;
    let text =
        std::str::from_utf8(entry.contents).wrap_err("the built-in SKILL.md is not UTF-8")?;
    let lines: Vec<&str> = text.lines().collect();
    let close = (lines.first() == Some(&"---"))
        .then(|| lines.iter().skip(1).position(|line| *line == "---"))
        .flatten()
        .ok_or_else(|| eyre!("the built-in SKILL.md has no YAML frontmatter"))?;
    let frontmatter: Frontmatter = yaml_serde::from_str(&lines[1..=close].join("\n"))
        .wrap_err("the built-in SKILL.md frontmatter has no `name` and `metadata.version`")?;
    if frontmatter.name != omnigraph_skill::NAME {
        bail!(
            "the built-in SKILL.md names the skill `{}`, not `{}`",
            frontmatter.name,
            omnigraph_skill::NAME
        );
    }
    Ok(frontmatter.metadata.version)
}

/// Writes the built-in skill into `dir`. Every refusal happens before the
/// first write, so a refused export leaves `dir` as it was.
pub(crate) fn export(dir: &Path, force: bool) -> Result<SkillExportOutput> {
    let skill_version = skill_version()?;
    if let Ok(metadata) = fs::metadata(dir)
        && !metadata.is_dir()
    {
        bail!("{} is not a directory", dir.display());
    }
    let current: BTreeSet<&str> = omnigraph_skill::FILES
        .iter()
        .map(|file| file.path)
        .collect();
    let mut existing = Vec::new();
    for path in current.iter().copied().chain([RECORD]) {
        match inspect(dir, path)? {
            Entry::Absent => {}
            Entry::Blocked => bail!(
                "a directory on the path of {path} in {} is not a directory; the export does not replace it",
                dir.display()
            ),
            Entry::Other => bail!(
                "{path} in {} is a directory or special file; the export replaces only files",
                dir.display()
            ),
            Entry::File | Entry::Symlink => existing.push(path),
        }
    }
    if !force && !existing.is_empty() {
        let shown = existing.len().min(3);
        let more = match existing.len() - shown {
            0 => String::new(),
            more => format!(" and {more} more"),
        };
        bail!(
            "{} already holds {}{more}; pass --force to replace the skill's files",
            dir.display(),
            existing[..shown].join(", ")
        );
    }
    let previous = if force { read_record(dir)? } else { None };

    fs::create_dir_all(dir).wrap_err_with(|| format!("create {}", dir.display()))?;
    let mut digests = BTreeMap::new();
    for file in omnigraph_skill::FILES {
        write_file(dir, file.path, file.contents)?;
        digests.insert(file.path.to_string(), sha256_hex(file.contents));
    }

    let mut removed = Vec::new();
    let mut kept = Vec::new();
    for (path, digest) in previous.iter().flat_map(|record| &record.files) {
        if names_current_file(path, &current) {
            continue;
        }
        match inspect(dir, path)? {
            Entry::Absent => {}
            Entry::File => {
                let target = dir.join(path);
                let bytes =
                    fs::read(&target).wrap_err_with(|| format!("read {}", target.display()))?;
                if sha256_hex(&bytes) == *digest {
                    fs::remove_file(&target)
                        .wrap_err_with(|| format!("remove {}", target.display()))?;
                    removed.push(path.clone());
                } else {
                    kept.push(KeptFile {
                        path: path.clone(),
                        reason: KeptReason::Modified,
                    });
                }
            }
            Entry::Symlink | Entry::Other | Entry::Blocked => kept.push(KeptFile {
                path: path.clone(),
                reason: KeptReason::NotAFile,
            }),
        }
    }

    let record = ExportRecord {
        format: RECORD_FORMAT,
        skill: omnigraph_skill::NAME.to_string(),
        skill_version: skill_version.clone(),
        cli_version: env!("CARGO_PKG_VERSION").to_string(),
        http_contract: omnigraph_api_types::HTTP_API_CONTRACT.to_string(),
        files: digests,
    };
    let mut bytes = serde_json::to_vec_pretty(&record)?;
    bytes.push(b'\n');
    write_file(dir, RECORD, &bytes)?;

    Ok(SkillExportOutput {
        directory: std::path::absolute(dir)
            .unwrap_or_else(|_| dir.to_path_buf())
            .display()
            .to_string(),
        skill: omnigraph_skill::NAME,
        skill_version,
        cli_version: env!("CARGO_PKG_VERSION"),
        http_contract: omnigraph_api_types::HTTP_API_CONTRACT,
        files: current.into_iter().collect(),
        record: RECORD,
        removed,
        kept,
    })
}

pub(crate) fn write_export_human(output: &SkillExportOutput, stdout: &mut dyn Write) -> Result<()> {
    writeln!(
        stdout,
        "exported the {} skill {} to {}: {} files (CLI {}, HTTP contract {})",
        output.skill,
        output.skill_version,
        output.directory,
        output.files.len(),
        output.cli_version,
        output.http_contract
    )?;
    for path in &output.removed {
        writeln!(
            stdout,
            "removed {path}: an earlier export wrote it and this skill no longer has it"
        )?;
    }
    for file in &output.kept {
        let reason = match file.reason {
            KeptReason::Modified => "changed since an earlier export wrote it",
            KeptReason::NotAFile => "no longer a regular file",
        };
        writeln!(stdout, "kept {}: {reason}", file.path)?;
    }
    Ok(())
}

/// Prints one built-in skill file to stdout.
pub(crate) fn show(path: &str, stdout: &mut dyn Write) -> Result<()> {
    let Some(file) = omnigraph_skill::file(path) else {
        let paths: Vec<&str> = omnigraph_skill::FILES
            .iter()
            .map(|file| file.path)
            .collect();
        bail!("the skill has no {path}; its files: {}", paths.join(", "));
    };
    match stdout
        .write_all(file.contents)
        .and_then(|()| stdout.flush())
    {
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => Ok(()),
        result => Ok(result?),
    }
}

/// The record of an earlier export in `dir`, if there is one. A record this
/// binary cannot read refuses the export rather than guessing which files it
/// may remove.
fn read_record(dir: &Path) -> Result<Option<ExportRecord>> {
    let target = dir.join(RECORD);
    let unreadable = |detail: String| {
        eyre!(
            "{} is not an export record this CLI can read ({detail}); remove it to export without removing files an earlier export wrote",
            target.display()
        )
    };
    match inspect(dir, RECORD)? {
        Entry::Absent => return Ok(None),
        Entry::File => {}
        _ => return Err(unreadable("not a regular file".to_string())),
    }
    let bytes = fs::read(&target).wrap_err_with(|| format!("read {}", target.display()))?;
    let record: ExportRecord =
        serde_json::from_slice(&bytes).map_err(|error| unreadable(error.to_string()))?;
    if record.format != RECORD_FORMAT {
        return Err(unreadable(format!("format {}", record.format)));
    }
    if record.skill != omnigraph_skill::NAME {
        return Err(unreadable(format!(
            "it records the `{}` skill",
            record.skill
        )));
    }
    for (path, digest) in &record.files {
        if !is_skill_path(path) {
            return Err(unreadable(format!(
                "path {path:?} leaves the skill directory"
            )));
        }
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(unreadable(format!("digest of {path} is not a SHA-256")));
        }
    }
    Ok(Some(record))
}

/// Whether a recorded path names a file this export just wrote. On a
/// case-insensitive file system a path differing only in ASCII case opens the
/// same file, so removing it as stale would delete a current file. Skill paths
/// are ASCII (the build script and [`is_skill_path`] refuse others), so ASCII
/// case is the only folding that can alias two of them.
fn names_current_file(path: &str, current: &BTreeSet<&str>) -> bool {
    current.iter().any(|file| file.eq_ignore_ascii_case(path))
}

/// A `/`-separated relative path of ASCII names, none starting with `.`:
/// every path the embedded skill can hold, and nothing that reaches outside
/// the export directory or onto the record itself.
fn is_skill_path(path: &str) -> bool {
    path.is_ascii()
        && !path.contains(['\\', ':'])
        && path
            .split('/')
            .all(|part| !part.is_empty() && !part.starts_with('.'))
        && Path::new(path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

/// Inspects `dir/path` one component at a time, so a symbolic link on the
/// way is seen rather than followed.
fn inspect(dir: &Path, path: &str) -> Result<Entry> {
    let parts: Vec<&str> = path.split('/').collect();
    let mut current = dir.to_path_buf();
    for (index, part) in parts.iter().enumerate() {
        current.push(part);
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Entry::Absent),
            Err(error) => {
                return Err(error).wrap_err_with(|| format!("inspect {}", current.display()));
            }
        };
        let kind = metadata.file_type();
        if index + 1 == parts.len() {
            return Ok(if kind.is_file() {
                Entry::File
            } else if kind.is_symlink() {
                Entry::Symlink
            } else {
                Entry::Other
            });
        }
        if !kind.is_dir() {
            return Ok(Entry::Blocked);
        }
    }
    Ok(Entry::Absent)
}

/// Writes one file; a symbolic link at the path is replaced, not followed.
fn write_file(dir: &Path, path: &str, contents: &[u8]) -> Result<()> {
    let target = dir.join(path);
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).wrap_err_with(|| format!("create {}", parent.display()))?;
    }
    if inspect(dir, path)? == Entry::Symlink {
        fs::remove_file(&target).wrap_err_with(|| format!("replace {}", target.display()))?;
    }
    fs::write(&target, contents).wrap_err_with(|| format!("write {}", target.display()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use clap::CommandFactory;

    use super::{is_skill_path, names_current_file};

    #[test]
    fn show_uses_the_supplied_sink_and_only_suppresses_broken_pipe() {
        use std::io::{self, Write};
        struct Fault {
            kind: io::ErrorKind,
            on_flush: bool,
            accepted: Vec<u8>,
        }
        impl Write for Fault {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if !self.on_flush {
                    return Err(io::Error::new(self.kind, "sink refused write"));
                }
                self.accepted.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Err(io::Error::new(self.kind, "sink refused flush"))
            }
        }
        for on_flush in [false, true] {
            for kind in [io::ErrorKind::BrokenPipe, io::ErrorKind::PermissionDenied] {
                let mut writer = Fault {
                    kind,
                    on_flush,
                    accepted: Vec::new(),
                };
                let result = super::show("SKILL.md", &mut writer);
                if kind == io::ErrorKind::BrokenPipe {
                    result.unwrap();
                } else {
                    assert_eq!(
                        result
                            .unwrap_err()
                            .downcast_ref::<io::Error>()
                            .unwrap()
                            .kind(),
                        kind
                    );
                }
                assert_eq!(
                    writer.accepted.as_slice(),
                    if on_flush {
                        omnigraph_skill::file("SKILL.md").unwrap().contents
                    } else {
                        &[]
                    }
                );
            }
        }
        let mut bytes = Vec::new();
        super::show("SKILL.md", &mut bytes).unwrap();
        assert_eq!(bytes, omnigraph_skill::file("SKILL.md").unwrap().contents);
    }

    /// Every public command path of the CLI, from the clap tree: a command
    /// without subcommands is one path, and a command with subcommands
    /// contributes one path per public subcommand.
    fn cli_command_paths() -> BTreeSet<String> {
        fn walk(command: &clap::Command, prefix: Option<&str>, paths: &mut BTreeSet<String>) {
            for child in command
                .get_subcommands()
                .filter(|child| !child.is_hide_set() && child.get_name() != "help")
            {
                let path = match prefix {
                    Some(prefix) => format!("{prefix} {}", child.get_name()),
                    None => child.get_name().to_string(),
                };
                if child.has_subcommands() {
                    walk(child, Some(&path), paths);
                } else {
                    paths.insert(path);
                }
            }
        }
        let mut paths = BTreeSet::new();
        walk(&crate::cli::Cli::command(), None, &mut paths);
        assert!(
            paths.len() > 20,
            "the clap walk found too few commands: {paths:?}"
        );
        paths
    }

    #[test]
    fn operation_inventory_covers_every_public_command() {
        use omnigraph_api_types::operations::{CLI_ONLY_OPERATIONS, HTTP_OPERATIONS};

        let inventoried: BTreeSet<String> = HTTP_OPERATIONS
            .iter()
            .flat_map(|operation| operation.cli.names.iter().copied())
            .chain(CLI_ONLY_OPERATIONS.iter().map(|operation| operation.name))
            .map(str::to_owned)
            .collect();
        assert_eq!(cli_command_paths(), inventoried);
    }

    fn top_level_commands() -> BTreeSet<String> {
        cli_command_paths()
            .into_iter()
            .map(|path| path.split(' ').next().unwrap().to_string())
            .collect()
    }

    fn embedded_text(path: &str) -> &'static str {
        let file = omnigraph_skill::file(path).unwrap_or_else(|| panic!("the skill has no {path}"));
        std::str::from_utf8(file.contents).unwrap()
    }

    /// The body of the `## {heading}` section, up to the next `## ` heading.
    fn section<'a>(markdown: &'a str, heading: &str) -> &'a str {
        let start = markdown
            .find(&format!("\n## {heading}\n"))
            .unwrap_or_else(|| panic!("no `## {heading}` section"));
        let body = &markdown[start + heading.len() + 5..];
        &body[..body.find("\n## ").unwrap_or(body.len())]
    }

    /// The contents of the inline code spans in `text`.
    fn code_spans(text: &str) -> Vec<&str> {
        text.split('`').skip(1).step_by(2).collect()
    }

    fn difference(left: &BTreeSet<String>, right: &BTreeSet<String>) -> Vec<String> {
        left.difference(right).cloned().collect()
    }

    /// The skill's command index lists exactly the CLI's public commands, so
    /// a command added or removed without updating the skill fails here.
    #[test]
    fn skill_command_index_lists_every_public_command() {
        let index = section(embedded_text("references/commands.md"), "Command Index");
        let mut indexed = BTreeSet::new();
        for row in index.lines().filter(|line| line.starts_with("| `")) {
            let cells: Vec<&str> = row.split('|').collect();
            let command = code_spans(cells[1]);
            assert_eq!(command.len(), 1, "one command per index row: {row}");
            let subcommands = code_spans(cells[2]);
            let paths: Vec<String> = if subcommands.is_empty() {
                vec![command[0].to_string()]
            } else {
                subcommands
                    .iter()
                    .map(|subcommand| format!("{} {subcommand}", command[0]))
                    .collect()
            };
            for path in paths {
                assert!(
                    indexed.insert(path.clone()),
                    "the command index repeats `{path}`: {row}"
                );
            }
        }
        let cli = cli_command_paths();
        assert!(
            indexed == cli,
            "skills/omnigraph/references/commands.md, Command Index, disagrees with the CLI.\n\
             Missing from the index: {:?}\nListed but not commands: {:?}",
            difference(&cli, &indexed),
            difference(&indexed, &cli),
        );
    }

    /// The condensed reference in SKILL.md names every top-level command, and
    /// each of its entries that opens with a command opens with a real one.
    #[test]
    fn skill_condensed_reference_names_every_top_level_command() {
        let reference = section(embedded_text("SKILL.md"), "CLI Reference (condensed)");
        let commands = top_level_commands();
        let first_word = |span: &str| span.split_whitespace().next().unwrap_or("").to_string();
        let named: BTreeSet<String> = code_spans(reference).into_iter().map(first_word).collect();
        let missing: Vec<&String> = commands.difference(&named).collect();
        assert!(
            missing.is_empty(),
            "SKILL.md, CLI Reference (condensed), never names {missing:?}"
        );
        for entry in reference.lines().filter(|line| line.starts_with("- `")) {
            let opening = first_word(code_spans(entry)[0]);
            assert!(
                commands.contains(&opening),
                "SKILL.md, CLI Reference (condensed), describes `{opening}`, which is not a command: {entry}"
            );
        }
    }

    /// SKILL.md states the storage formats this binary reports, so a stamp
    /// change fails here until the skill says what `omnigraph version` says.
    #[test]
    fn skill_states_the_storage_formats_this_binary_reports() {
        let line = crate::commands::local::output::VersionOutput::current()
            .internal_schema
            .line();
        assert!(
            embedded_text("SKILL.md").contains(&format!("`{line}`")),
            "SKILL.md must state `{line}`, as `omnigraph version` prints it"
        );
    }

    #[test]
    fn a_recorded_path_differing_only_in_case_is_a_current_file() {
        let current = BTreeSet::from(["SKILL.md", "references/commands.md"]);
        assert!(names_current_file("references/commands.md", &current));
        assert!(names_current_file("references/Commands.md", &current));
        assert!(names_current_file("REFERENCES/COMMANDS.MD", &current));
        assert!(!names_current_file("references/retired.md", &current));
        assert!(!names_current_file("references/commands.md.bak", &current));
    }

    #[test]
    fn record_paths_stay_below_the_export_directory() {
        for path in ["SKILL.md", "references/queries.md", "a/b/c.md"] {
            assert!(is_skill_path(path), "{path}");
        }
        for path in [
            "",
            "/etc/passwd",
            "../outside.md",
            "references/../../outside.md",
            "references//x.md",
            "./SKILL.md",
            ".omnigraph-skill.json",
            "references/.hidden",
            "references\\x.md",
            "C:/x.md",
            "references/caf\u{e9}.md",
        ] {
            assert!(!is_skill_path(path), "{path}");
        }
    }
}
