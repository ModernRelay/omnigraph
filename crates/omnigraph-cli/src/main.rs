use std::fs;
use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use color_eyre::eyre::Result;
use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use serde::Serialize;

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
        #[arg(long, default_value = "main")]
        branch: String,
        #[arg(long, default_value = "overwrite")]
        mode: CliLoadMode,
        #[arg(long)]
        json: bool,
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
        #[arg(long)]
        json: bool,
    },
    /// List branches
    List {
        /// Repo URI
        uri: String,
        #[arg(long)]
        json: bool,
    },
    /// Merge a source branch into a target branch
    Merge {
        /// Repo URI
        uri: String,
        source: String,
        #[arg(long, default_value = "main")]
        target: String,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum CliLoadMode {
    Overwrite,
    Append,
    Merge,
}

impl From<CliLoadMode> for LoadMode {
    fn from(value: CliLoadMode) -> Self {
        match value {
            CliLoadMode::Overwrite => LoadMode::Overwrite,
            CliLoadMode::Append => LoadMode::Append,
            CliLoadMode::Merge => LoadMode::Merge,
        }
    }
}

impl CliLoadMode {
    fn as_str(self) -> &'static str {
        match self {
            CliLoadMode::Overwrite => "overwrite",
            CliLoadMode::Append => "append",
            CliLoadMode::Merge => "merge",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CliMergeOutcome {
    AlreadyUpToDate,
    FastForward,
    Merged,
}

impl From<MergeOutcome> for CliMergeOutcome {
    fn from(value: MergeOutcome) -> Self {
        match value {
            MergeOutcome::AlreadyUpToDate => CliMergeOutcome::AlreadyUpToDate,
            MergeOutcome::FastForward => CliMergeOutcome::FastForward,
            MergeOutcome::Merged => CliMergeOutcome::Merged,
        }
    }
}

impl CliMergeOutcome {
    fn as_str(self) -> &'static str {
        match self {
            CliMergeOutcome::AlreadyUpToDate => "already_up_to_date",
            CliMergeOutcome::FastForward => "fast_forward",
            CliMergeOutcome::Merged => "merged",
        }
    }
}

#[derive(Debug, Serialize)]
struct LoadOutput<'a> {
    uri: &'a str,
    branch: &'a str,
    mode: &'a str,
    nodes_loaded: usize,
    edges_loaded: usize,
}

#[derive(Debug, Serialize)]
struct BranchCreateOutput<'a> {
    uri: &'a str,
    from: &'a str,
    name: &'a str,
}

#[derive(Debug, Serialize)]
struct BranchListOutput {
    branches: Vec<String>,
}

#[derive(Debug, Serialize)]
struct BranchMergeOutput<'a> {
    source: &'a str,
    target: &'a str,
    outcome: CliMergeOutcome,
}

#[derive(Debug, Serialize)]
struct SnapshotTableOutput<'a> {
    table_key: &'a str,
    table_path: &'a str,
    table_version: u64,
    table_branch: Option<&'a str>,
    row_count: u64,
}

#[derive(Debug, Serialize)]
struct SnapshotOutput<'a> {
    branch: &'a str,
    manifest_version: u64,
    tables: Vec<SnapshotTableOutput<'a>>,
}

fn ensure_local_repo_parent(uri: &str) -> Result<()> {
    if !uri.contains("://") {
        fs::create_dir_all(uri)?;
    }
    Ok(())
}

fn print_json<T: Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn print_load_human(
    uri: &str,
    branch: &str,
    mode: CliLoadMode,
    nodes_loaded: usize,
    edges_loaded: usize,
) {
    println!(
        "loaded {} on branch {} with {}: {} node types, {} edge types",
        uri,
        branch,
        mode.as_str(),
        nodes_loaded,
        edges_loaded
    );
}

fn print_snapshot_human(branch: &str, manifest_version: u64, entries: &[SnapshotTableOutput<'_>]) {
    println!("branch: {}", branch);
    println!("manifest_version: {}", manifest_version);
    for entry in entries {
        println!(
            "{} v{} branch={} rows={}",
            entry.table_key,
            entry.table_version,
            entry.table_branch.unwrap_or("main"),
            entry.row_count
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    match cli.command {
        Command::Init { schema, uri } => {
            let schema_source = fs::read_to_string(&schema)?;
            ensure_local_repo_parent(&uri)?;
            Omnigraph::init(&uri, &schema_source).await?;
            println!("initialized {}", uri);
        }
        Command::Load {
            data,
            branch,
            mode,
            json,
            uri,
        } => {
            let mut db = Omnigraph::open(&uri).await?;
            let result = db
                .load_file(&branch, &data.to_string_lossy(), mode.into())
                .await?;
            let payload = LoadOutput {
                uri: &uri,
                branch: &branch,
                mode: mode.as_str(),
                nodes_loaded: result.nodes_loaded.len(),
                edges_loaded: result.edges_loaded.len(),
            };
            if json {
                print_json(&payload)?;
            } else {
                print_load_human(
                    &uri,
                    &branch,
                    mode,
                    payload.nodes_loaded,
                    payload.edges_loaded,
                );
            }
        }
        Command::Branch { command } => match command {
            BranchCommand::Create {
                uri,
                from,
                name,
                json,
            } => {
                let mut db = Omnigraph::open(&uri).await?;
                db.branch_create_from(ReadTarget::branch(&from), &name)
                    .await?;
                if json {
                    print_json(&BranchCreateOutput {
                        uri: &uri,
                        from: &from,
                        name: &name,
                    })?;
                } else {
                    println!("created branch {} from {}", name, from);
                }
            }
            BranchCommand::List { uri, json } => {
                let db = Omnigraph::open(&uri).await?;
                let mut branches = db.branch_list().await?;
                branches.sort();
                if json {
                    print_json(&BranchListOutput { branches })?;
                } else {
                    for branch in branches {
                        println!("{}", branch);
                    }
                }
            }
            BranchCommand::Merge {
                uri,
                source,
                target,
                json,
            } => {
                let mut db = Omnigraph::open(&uri).await?;
                let outcome: CliMergeOutcome = db.branch_merge(&source, &target).await?.into();
                if json {
                    print_json(&BranchMergeOutput {
                        source: &source,
                        target: &target,
                        outcome,
                    })?;
                } else {
                    println!("merged {} into {}: {}", source, target, outcome.as_str());
                }
            }
        },
        Command::Snapshot { uri, branch, json } => {
            let db = Omnigraph::open(&uri).await?;
            let branch = branch.unwrap_or_else(|| "main".to_string());
            let snapshot = db.snapshot_of(ReadTarget::branch(branch.as_str())).await?;

            let mut entries: Vec<_> = snapshot.entries().cloned().collect();
            entries.sort_by(|a, b| a.table_key.cmp(&b.table_key));
            let tables = entries
                .iter()
                .map(|entry| SnapshotTableOutput {
                    table_key: entry.table_key.as_str(),
                    table_path: entry.table_path.as_str(),
                    table_version: entry.table_version,
                    table_branch: entry.table_branch.as_deref(),
                    row_count: entry.row_count,
                })
                .collect::<Vec<_>>();
            let payload = SnapshotOutput {
                branch: &branch,
                manifest_version: snapshot.version(),
                tables,
            };

            if json {
                print_json(&payload)?;
            } else {
                print_snapshot_human(&branch, snapshot.version(), &payload.tables);
            }
        }
    }
    Ok(())
}
