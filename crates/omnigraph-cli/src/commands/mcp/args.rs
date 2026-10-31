use clap::Args;

#[derive(Debug, Args)]
pub(crate) struct McpArgs {
    /// List and admit the server's write tools as well. Without it, only the
    /// tools the server annotates read-only are listed, and a call to any
    /// other is refused before it is relayed. The server's policy still
    /// decides every call.
    #[arg(long)]
    pub(crate) write: bool,
}
