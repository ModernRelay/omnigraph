use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use omnigraph_vocabulary_guard::{
    CheckOptions, GuardSurface, check_in, render_tsv, resolve_tool_executable,
    scan_openapi_at_root, scan_public_rust_tree, scan_rust_tree, scan_user_docs_tree,
};

#[derive(Debug, Parser)]
#[command(about = "Guard classified vocabulary at public OmniGraph boundaries")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Emit deterministic observations with review-owned columns empty.
    Scan {
        #[arg(long, value_enum)]
        surface: Surface,
        #[arg(long, default_value = ".")]
        root: PathBuf,
        #[arg(long, default_value = "openapi.json")]
        openapi: PathBuf,
        #[arg(long, default_value = "cargo-public-api")]
        cargo_public_api: PathBuf,
        #[arg(long, default_value = "target/vocabulary-public-api/current")]
        public_api_target_dir: PathBuf,
    },
    /// Check current observations and classifications against an exact merge base.
    Check {
        #[arg(long, value_enum)]
        surface: Surface,
        #[arg(long)]
        base: String,
        #[arg(long)]
        inventory: PathBuf,
        #[arg(long, default_value = ".")]
        root: PathBuf,
        #[arg(long, default_value = "openapi.json")]
        openapi: PathBuf,
        #[arg(long, default_value = "cargo-public-api")]
        cargo_public_api: PathBuf,
        #[arg(long, default_value = "target/vocabulary-public-api")]
        public_api_target_dir: PathBuf,
        /// Additionally prove this change alters OpenAPI presentation text only.
        #[arg(long)]
        presentation_only: bool,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Surface {
    Openapi,
    RustString,
    UserDocs,
    PublicRust,
    All,
}

impl From<Surface> for GuardSurface {
    fn from(value: Surface) -> Self {
        match value {
            Surface::Openapi => Self::Openapi,
            Surface::RustString => Self::RustString,
            Surface::UserDocs => Self::UserDocs,
            Surface::PublicRust => Self::PublicRust,
            Surface::All => Self::All,
        }
    }
}

fn run() -> Result<(), omnigraph_vocabulary_guard::GuardError> {
    match Cli::parse().command {
        Command::Scan {
            surface,
            root,
            openapi,
            cargo_public_api,
            public_api_target_dir,
        } => {
            let root = root.canonicalize().map_err(|error| {
                omnigraph_vocabulary_guard::GuardError::Git(format!(
                    "cannot canonicalize scan root {}: {error}",
                    root.display()
                ))
            })?;
            let mut observations = Vec::new();
            if matches!(surface, Surface::Openapi | Surface::All) {
                observations.extend(scan_openapi_at_root(&root, &openapi)?);
            }
            if matches!(surface, Surface::RustString | Surface::All) {
                observations.extend(scan_rust_tree(&root)?);
            }
            if matches!(surface, Surface::UserDocs | Surface::All) {
                observations.extend(scan_user_docs_tree(&root)?);
            }
            if matches!(surface, Surface::PublicRust | Surface::All) {
                let cargo_public_api = resolve_tool_executable(&root, &cargo_public_api);
                let target_dir = if public_api_target_dir.is_absolute() {
                    public_api_target_dir
                } else {
                    root.join(public_api_target_dir)
                };
                observations.extend(scan_public_rust_tree(&root, cargo_public_api, target_dir)?);
            }
            print!("{}", render_tsv(&observations)?);
        }
        Command::Check {
            surface,
            base,
            inventory,
            root,
            openapi,
            cargo_public_api,
            public_api_target_dir,
            presentation_only,
        } => {
            let report = check_in(
                &root,
                &CheckOptions {
                    base,
                    inventory,
                    openapi,
                    surface: surface.into(),
                    public_api_tool: cargo_public_api,
                    public_api_target_dir,
                    presentation_only,
                },
            )?;
            println!(
                "Graph vocabulary guard: checked {} current and {} merge-base occurrences",
                report.current_occurrences, report.base_occurrences
            );
        }
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("Graph vocabulary guard failed: {error}");
        std::process::exit(1);
    }
}
