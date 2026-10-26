use clap::Args;

#[derive(Debug, Args)]
pub(crate) struct StudioArgs {
    /// Print the link and its digests as one JSON object; a refusal to open
    /// Studio still carries the link.
    #[arg(long)]
    pub(crate) json: bool,
}
