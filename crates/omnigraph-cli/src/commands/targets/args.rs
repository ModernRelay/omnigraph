use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub(crate) enum TargetCommand {
    List {
        #[arg(long)]
        json: bool,
    },
    Inspect {
        name: String,
        #[arg(long)]
        resolved: bool,
        #[arg(long)]
        json: bool,
    },
    #[command(visible_alias = "register")]
    Add {
        name: String,
        #[arg(long)]
        cluster: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Refresh {
        name: String,
        #[arg(long)]
        json: bool,
    },
    Remove {
        name: String,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ConfigCommand {
    RenameConnection {
        from: String,
        to: String,
    },
    RenameTarget {
        from: String,
        to: String,
    },
    RenameAuth {
        #[arg(value_enum)]
        kind: RenameOwner,
        owner: String,
        from: String,
        to: String,
    },
    RecoverRename,
    Validate {
        #[arg(long)]
        json: bool,
    },
    RecoverOperations {
        #[arg(long)]
        source: std::path::PathBuf,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub(crate) enum RenameOwner {
    Connection,
    Server,
}

#[derive(Debug, Subcommand)]
pub(crate) enum CredentialCommand {
    Set {
        #[arg(long, required = true)]
        stdin: bool,
        #[arg(long)]
        replace: bool,
    },
    Status {
        #[arg(long)]
        json: bool,
    },
    Remove,
    Export,
}
