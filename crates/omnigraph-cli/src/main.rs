use std::path::PathBuf;

use clap::{Parser, Subcommand};
use color_eyre::eyre::Result;

#[derive(Debug, Parser)]
#[command(name = "omnigraph")]
#[command(about = "Omnigraph graph database CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Initialize a new repo from a schema
    Init {
        #[arg(long)]
        schema: PathBuf,
        /// Repo URI (local path or s3://)
        uri: String,
    },
    /// Load data into a repo
    Load {
        #[arg(long)]
        data: PathBuf,
        /// Repo URI
        uri: String,
    },
    /// Branch operations
    Branch {
        #[command(subcommand)]
        command: BranchCommand,
    },
    /// Show repo snapshot
    Snapshot {
        /// Repo URI
        uri: String,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum BranchCommand {
    /// Create a new branch
    Create {
        /// Repo URI
        uri: String,
        #[arg(long, default_value = "main")]
        from: String,
        name: String,
    },
}

fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    match cli.command {
        Command::Init { schema, uri } => {
            eprintln!(
                "omnigraph init --schema {} {} (not yet implemented)",
                schema.display(),
                uri
            );
        }
        Command::Load { data, uri } => {
            eprintln!(
                "omnigraph load --data {} {} (not yet implemented)",
                data.display(),
                uri
            );
        }
        Command::Branch { command } => match command {
            BranchCommand::Create { uri, from, name } => {
                eprintln!(
                    "omnigraph branch create {} --from {} {} (not yet implemented)",
                    name, from, uri
                );
            }
        },
        Command::Snapshot { uri, branch, json } => {
            eprintln!(
                "omnigraph snapshot {} --branch {:?} --json {} (not yet implemented)",
                uri, branch, json
            );
        }
    }
    Ok(())
}
