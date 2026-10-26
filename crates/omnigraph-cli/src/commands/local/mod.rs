//! Local credentials and bundled utilities.

use crate::embed::EmbedArgs;
use color_eyre::eyre::Result;
use output::VersionOutput;
use std::io::Write;

pub(crate) mod args;
pub(crate) mod output;
pub(crate) use args::SkillCommand;

pub(crate) fn run_skill(command: SkillCommand, stdout: &mut dyn Write) -> Result<()> {
    match command {
        SkillCommand::Export { dir, force, json } => {
            let report = crate::skill::export(&dir, force)?;
            if json {
                crate::output::write_json(&report, stdout)
            } else {
                crate::skill::write_export_human(&report, stdout)
            }
        }
        SkillCommand::Show { path } => crate::skill::show(&path, stdout),
    }
}

pub(crate) fn run_version(json: bool, stdout: &mut dyn Write) -> Result<()> {
    output::write_version(&VersionOutput::current(), json, stdout)
}

pub(crate) async fn run_embed(args: EmbedArgs, stdout: &mut dyn Write) -> Result<()> {
    let report = crate::embed::execute_embed(&args).await?;
    if args.json {
        crate::output::write_json(&report, stdout)
    } else {
        crate::embed::write_human(&report, stdout)
    }
}

#[cfg(test)]
mod tests;
