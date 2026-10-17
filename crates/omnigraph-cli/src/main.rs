#![recursion_limit = "256"]

use color_eyre::eyre::Result;

#[tokio::main]
async fn main() -> Result<std::process::ExitCode> {
    // A refusal explains its fix without environment or source-location footers.
    color_eyre::config::HookBuilder::default()
        .display_env_section(false)
        .display_location_section(false)
        .install()?;
    let completion = omnigraph_cli::app::run_from(std::env::args_os()).await?;
    Ok(std::process::ExitCode::from(completion.code() as u8))
}
