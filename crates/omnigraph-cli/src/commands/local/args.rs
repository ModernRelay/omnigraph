//! Arguments for local configuration and bundled tooling.

use clap::Subcommand;
use std::path::PathBuf;

#[derive(Debug, Subcommand)]
pub(crate) enum SkillCommand {
    /// Write the agent skill into a directory, creating it when missing.
    ///
    /// Refuses when any file the export writes already exists, unless
    /// --force. A record of the written files (`.omnigraph-skill.json`) lets a
    /// later --force export remove files this skill no longer has, but only
    /// while their bytes are unchanged. Other files are never touched.
    Export {
        /// Skill directory, e.g. ~/.claude/skills/omnigraph.
        dir: PathBuf,
        /// Replace the skill's files and remove unchanged files that an
        /// earlier export recorded and this skill no longer has.
        #[arg(long)]
        force: bool,
        /// Emit JSON: the files written, the skill version, the CLI version
        /// and the HTTP contract.
        #[arg(long)]
        json: bool,
    },
    /// Print one file of the agent skill to stdout.
    Show {
        /// Path below the skill root.
        #[arg(default_value = "SKILL.md", value_name = "PATH")]
        path: String,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ProfileCommand {
    /// List the profiles defined in ~/.omnigraph/config.yaml.
    List {
        #[arg(long)]
        json: bool,
    },
    /// Show a profile's resolved scope. With no name, shows the active
    /// (`$OMNIGRAPH_PROFILE`) profile, else the flat operator defaults.
    Show {
        /// Profile name (optional).
        name: Option<String>,
        #[arg(long)]
        json: bool,
    },
}
