//! Local credentials, profile inspection and bundled utilities.

use crate::commands::targets::LoginKey;
use crate::embed::EmbedArgs;
use crate::helpers::normalize_bearer_token;
use crate::operator::InvocationConfig;
use clap::ValueEnum;
use color_eyre::eyre::{Result, bail, eyre};
use output::{ProfileDetail, ProfileListItem, VersionOutput};
use std::io::{Read, Write};

pub(crate) mod args;
pub(crate) mod output;
pub(crate) use args::{ProfileCommand, SkillCommand};

pub(crate) fn run_login(
    operator: &InvocationConfig,
    key: LoginKey,
    token: Option<String>,
    json: bool,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    let token = match token {
        Some(token) => token,
        None => crate::input::read_line(stdin)?,
    };
    let Some(token) = normalize_bearer_token(Some(token)) else {
        let command = match &key {
            LoginKey::Server(name) => name.clone(),
            LoginKey::Target { target, .. } => format!("--target {target}"),
        };
        bail!(
            "no token provided: pass --token <TOKEN> or pipe it on stdin (echo $TOKEN | omnigraph login {command})"
        );
    };
    // A target's owner is bound to its endpoint by the target itself; a
    // server name is declared when a `servers:` entry or a server target
    // binds it.
    let declared = match &key {
        LoginKey::Server(name) => operator.get(stderr)?.declares_credential_owner(name),
        LoginKey::Target { .. } => true,
    };
    let path = operator
        .credentials_path()
        .ok_or_else(|| eyre!("no home directory resolvable for the credentials file"))?;
    crate::operator::rewrite_credentials_at(&path, key.owner(), Some(&token))?;
    output::finish_login(&key, &path, declared, json, stdout, stderr)
}

pub(crate) fn run_logout(
    operator: &InvocationConfig,
    key: LoginKey,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    let path = operator
        .credentials_path()
        .ok_or_else(|| eyre!("no home directory resolvable for the credentials file"))?;
    crate::operator::rewrite_credentials_at(&path, key.owner(), None)?;
    output::finish_logout(&key, &path, json, stdout)
}

pub(crate) fn run_profile(
    operator: &InvocationConfig,
    command: ProfileCommand,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    use crate::operator::ScopeBinding;
    let op = operator.get(stderr)?;
    let active = operator.profile().map(str::to_string);
    match command {
        ProfileCommand::List { json } => {
            let items: Vec<ProfileListItem> = op
                .profiles
                .iter()
                .map(|(name, profile)| {
                    let (binding, scope_kind, target, valid, error) = match profile.binding(name) {
                        Ok(ScopeBinding::Server(s)) => (
                            format!("server: {s}"),
                            "server".to_string(),
                            Some(s),
                            true,
                            None,
                        ),
                        Ok(ScopeBinding::Cluster(c)) => (
                            format!("cluster: {c}"),
                            "cluster".to_string(),
                            Some(c),
                            true,
                            None,
                        ),
                        Ok(ScopeBinding::Store(u)) => (
                            format!("store: {u}"),
                            "store".to_string(),
                            Some(u),
                            true,
                            None,
                        ),
                        Err(e) => (
                            format!("invalid: {e}"),
                            "invalid".to_string(),
                            None,
                            false,
                            Some(e.to_string()),
                        ),
                    };
                    ProfileListItem {
                        name: name.clone(),
                        binding,
                        scope_kind,
                        target,
                        valid,
                        error,
                        default_graph: profile.default_graph.clone(),
                        active: active.as_deref() == Some(name.as_str()),
                    }
                })
                .collect();
            output::write_profile_list(&items, json, stdout)?;
        }
        ProfileCommand::Show { name, json } => {
            let detail = match name.or(active) {
                Some(name) => {
                    let profile = op.profile(&name).ok_or_else(|| {
                        color_eyre::eyre::eyre!(
                            "unknown profile '{name}' (not defined under `profiles:`)"
                        )
                    })?;
                    let (kind, target, endpoint) = match profile.binding(&name)? {
                        ScopeBinding::Server(s) => {
                            let endpoint = op.servers.get(&s).map(|sv| sv.url.clone());
                            ("server", Some(s), endpoint)
                        }
                        ScopeBinding::Cluster(c) => {
                            let endpoint = op.cluster_root(&c).map(str::to_string);
                            ("cluster", Some(c), endpoint)
                        }
                        ScopeBinding::Store(u) => ("store", Some(u.clone()), Some(u)),
                    };
                    ProfileDetail {
                        name,
                        scope_kind: kind.to_string(),
                        target,
                        endpoint,
                        default_graph: profile
                            .default_graph
                            .clone()
                            .or_else(|| op.default_graph().map(str::to_string)),
                        output_format: op
                            .output()
                            .and_then(|f| f.to_possible_value())
                            .map(|v| v.get_name().to_string()),
                    }
                }
                // No name and no active profile: the flat operator defaults.
                None => {
                    let (kind, target, endpoint) = if let Some(s) = op.default_server() {
                        let endpoint = op.servers.get(s).map(|sv| sv.url.clone());
                        ("server", Some(s.to_string()), endpoint)
                    } else if let Some(u) = op.default_store() {
                        ("store", Some(u.to_string()), Some(u.to_string()))
                    } else {
                        ("none", None, None)
                    };
                    ProfileDetail {
                        name: "(defaults)".to_string(),
                        scope_kind: kind.to_string(),
                        target,
                        endpoint,
                        default_graph: op.default_graph().map(str::to_string),
                        output_format: op
                            .output()
                            .and_then(|f| f.to_possible_value())
                            .map(|v| v.get_name().to_string()),
                    }
                }
            };
            output::write_profile_detail(&detail, json, stdout)?;
        }
    }
    Ok(())
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
