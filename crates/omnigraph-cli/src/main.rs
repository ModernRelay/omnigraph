use std::fs;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use color_eyre::eyre::{Result, eyre};
use omnigraph_engine::store::database::Database;
use omnigraph_repo::GraphRepo;

#[derive(Debug, Parser)]
#[command(name = "omnigraph")]
#[command(about = "Omnigraph prototype CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Init {
        #[arg(long)]
        repo: PathBuf,
        #[arg(long)]
        schema: PathBuf,
    },
    Branch {
        #[command(subcommand)]
        command: BranchCommand,
    },
    Snapshot {
        #[arg(long)]
        repo: PathBuf,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Load {
        #[arg(long)]
        repo: PathBuf,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        data: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum BranchCommand {
    Create {
        #[arg(long)]
        repo: PathBuf,
        #[arg(long, default_value = "main")]
        from: String,
        name: String,
    },
}

fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    match cli.command {
        Command::Init { repo, schema } => init_repo(repo, schema),
        Command::Branch { command } => match command {
            BranchCommand::Create { repo, from, name } => create_branch(repo, &from, &name),
        },
        Command::Snapshot { repo, branch, json } => show_snapshot(repo, branch.as_deref(), json),
        Command::Load { repo, branch, data } => load_data(repo, branch.as_deref(), data),
    }
}

fn init_repo(repo: PathBuf, schema_path: PathBuf) -> Result<()> {
    let schema = fs::read_to_string(&schema_path)
        .map_err(|err| eyre!("failed to read schema {}: {err}", schema_path.display()))?;
    GraphRepo::init(&repo, &schema)?;
    println!("initialized repo {}", repo.display());
    Ok(())
}

fn create_branch(repo: PathBuf, from: &str, name: &str) -> Result<()> {
    let repo = GraphRepo::open(&repo)?;
    repo.create_branch(from, name)?;
    println!("created branch {name} from {from}");
    Ok(())
}

fn show_snapshot(repo: PathBuf, branch: Option<&str>, json: bool) -> Result<()> {
    let repo = GraphRepo::open(&repo)?;
    let snapshot = repo.snapshot(branch)?;
    if json {
        println!("{}", serde_json::to_string_pretty(snapshot.head())?);
        return Ok(());
    }

    println!("repo      : {}", snapshot.repo_path().display());
    println!("branch    : {}", snapshot.branch());
    println!("snapshot  : {}", snapshot.head().snapshot_id);
    println!("schema    : {}", snapshot.head().schema_file);
    println!("datasets  : {}", snapshot.head().datasets.len());
    Ok(())
}

fn load_data(repo: PathBuf, branch: Option<&str>, data_path: PathBuf) -> Result<()> {
    let repo = GraphRepo::open(&repo)?;
    let schema = fs::read_to_string(repo.path().join("_schema.pg"))
        .map_err(|err| eyre!("failed to read repo schema: {err}"))?;
    let data_path = fs::canonicalize(&data_path)
        .map_err(|err| eyre!("data file not found {}: {err}", data_path.display()))?;
    let branch_name = branch.unwrap_or(repo.metadata().default_branch.as_str());
    repo.load_branch_head(branch_name)?;

    let db_path = repo
        .branch_db_path(branch_name)
        .map_err(|err| eyre!("branch database error: {err}"))?;
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let db = if db_path.exists() {
            Database::open(&db_path).await
        } else {
            Database::init(&db_path, &schema).await
        }
        .map_err(|err| eyre!("database error: {err}"))?;

        db.load_file(&data_path)
            .await
            .map_err(|err| eyre!("load error: {err}"))?;

        Ok::<(), color_eyre::eyre::Report>(())
    })?;
    repo.sync_branch_head_from_db(branch_name)?;

    println!("loaded {} into branch {}", data_path.display(), branch_name);
    Ok(())
}
