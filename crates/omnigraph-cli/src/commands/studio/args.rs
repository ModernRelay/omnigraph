use clap::Args;

#[derive(Debug, Args)]
pub(crate) struct StudioArgs {
    /// Print the link and its server digests or managed cluster identity as
    /// one JSON object. An unavailable Studio handler still reports the link.
    #[arg(long)]
    pub(crate) json: bool,
}
