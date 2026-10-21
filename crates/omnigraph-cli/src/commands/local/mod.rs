//! Local credentials and bundled utilities.

use crate::embed::EmbedArgs;
use crate::helpers::normalize_bearer_token;
use crate::operator::InvocationConfig;
use color_eyre::eyre::{Result, bail, eyre};
use output::VersionOutput;
use std::io::{Read, Write};

pub(crate) mod args;
pub(crate) mod output;
pub(crate) use args::SkillCommand;

fn server_owner(target: &crate::targets::ResolvedTarget) -> Result<(&str, &str)> {
    match &target.binding {
        crate::targets::TargetBinding::Server {
            endpoint,
            auth: crate::targets::ServerAuth::Token { owner },
            ..
        } => Ok((owner, endpoint)),
        _ => bail!(
            "login/logout requires a named server target with a credential owner, or a managed target"
        ),
    }
}

pub(crate) fn run_login(
    operator: &InvocationConfig,
    target: &crate::targets::ResolvedTarget,
    token: Option<String>,
    json: bool,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    _stderr: &mut dyn Write,
) -> Result<()> {
    let (owner, endpoint) = server_owner(target)?;
    let token = match token {
        Some(token) => token,
        None => crate::input::read_line(stdin)?,
    };
    let token = normalize_bearer_token(Some(token))
        .ok_or_else(|| eyre!("no token provided: pass --token or pipe a token on stdin"))?;
    crate::targets::write_server_credential(operator, owner, endpoint, Some(&token))?;
    output::finish_login(target.label(), json, stdout)
}

pub(crate) fn run_logout(
    operator: &InvocationConfig,
    target: &crate::targets::ResolvedTarget,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    let (owner, endpoint) = server_owner(target)?;
    crate::targets::write_server_credential(operator, owner, endpoint, None)?;
    output::finish_logout(target.label(), json, stdout)
}

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
