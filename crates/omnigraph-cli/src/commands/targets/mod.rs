//! The `target` and `credential` command groups: the operator
//! configuration's named targets, and a server target's static token for
//! another program.

mod args;

pub(crate) use args::{CredentialCommand, TargetCommand};

use std::io::{Read, Write};

use color_eyre::Result;
use omnigraph_api_types::ErrorCode;
use serde::Serialize;

use crate::helpers::CliRefusal;
use crate::operator::{InvocationConfig, OperatorConfig};
use crate::output::write_json;
use crate::targets::{
    self, ConfigEditLock, ServerAuth, TARGET_PROTOCOL, TargetBinding, TargetEntry, TargetOutput,
};

/// The most a `target add` entry on stdin may hold.
const ENTRY_MAX_BYTES: u64 = 64 * 1024;

#[derive(Serialize)]
struct ShowOutput {
    protocol: u32,
    default: bool,
    #[serde(flatten)]
    target: TargetOutput,
}

#[derive(Serialize)]
struct AddOutput {
    protocol: u32,
    added: TargetOutput,
}

#[derive(Serialize)]
struct RemoveOutput {
    protocol: u32,
    removed: String,
    /// The credential owner whose stored token, if any, the removal kept.
    #[serde(skip_serializing_if = "Option::is_none")]
    kept_credential: Option<String>,
}

pub(crate) fn run_target(
    input: &InvocationConfig,
    command: TargetCommand,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    match command {
        TargetCommand::List { json } => {
            let config = input.get(stderr)?;
            let listing = targets::list_output(config);
            if json {
                return write_json(&listing, stdout);
            }
            if listing.targets.is_empty() {
                writeln!(
                    stdout,
                    "no targets (add one with `omnigraph target add NAME`)"
                )?;
            }
            for target in &listing.targets {
                let default = listing.default_target.as_deref() == Some(target.name.as_str());
                targets::write_target_line(target, default, stdout)?;
            }
            Ok(())
        }
        TargetCommand::Show { name, json } => {
            let config = input.get(stderr)?;
            let target = TargetOutput::of(&name, config.target(&name)?);
            let default = config.default_target() == Some(name.as_str());
            if json {
                return write_json(
                    &ShowOutput {
                        protocol: TARGET_PROTOCOL,
                        default,
                        target,
                    },
                    stdout,
                );
            }
            targets::write_target_line(&target, default, stdout)
        }
        TargetCommand::Add { name, json } => {
            let path = config_path(input)?;
            if !targets::valid_name(&name) {
                return Err(CliRefusal::report(
                    ErrorCode::TargetInvalid,
                    format!(
                        "target name `{name}` must start with an ASCII letter or digit and use only letters, digits, `-`, `_` or `.`"
                    ),
                ));
            }
            let entry = read_entry(stdin)?;
            entry.binding(&name)?;
            let _lock = ConfigEditLock::acquire(&path)?;
            let before = read_config_text(&path)?;
            let current = crate::operator::parse_operator_config(&before)?;
            if current.targets.contains_key(&name) {
                return Err(CliRefusal::report(
                    ErrorCode::TargetExists,
                    format!(
                        "target '{name}' already exists; remove it first with `omnigraph target remove {name}`"
                    ),
                ));
            }
            let after = targets::add_entry_text(&before, &name, &entry)?;
            let mut expected = current.targets.clone();
            expected.insert(name.clone(), entry.clone());
            targets::verify_edit(&before, &after, &expected)?;
            targets::replace_file(&path, &after)?;
            let added = TargetOutput::of(&name, &entry);
            if json {
                return write_json(
                    &AddOutput {
                        protocol: TARGET_PROTOCOL,
                        added,
                    },
                    stdout,
                );
            }
            write!(stdout, "added ")?;
            targets::write_target_line(&added, false, stdout)
        }
        TargetCommand::Remove { name, json } => {
            let path = config_path(input)?;
            let _lock = ConfigEditLock::acquire(&path)?;
            let before = read_config_text(&path)?;
            let current = crate::operator::parse_operator_config(&before)?;
            let entry = current.target(&name)?.clone();
            if current.default_target() == Some(name.as_str()) {
                return Err(CliRefusal::report(
                    ErrorCode::TargetIsDefault,
                    format!(
                        "target '{name}' is `defaults.target`; change the default before removing it"
                    ),
                ));
            }
            let after = targets::remove_entry_text(&before, &name)?;
            let mut expected = current.targets.clone();
            expected.remove(&name);
            targets::verify_edit(&before, &after, &expected)?;
            targets::replace_file(&path, &after)?;
            let kept_credential = entry.credential.clone();
            if json {
                return write_json(
                    &RemoveOutput {
                        protocol: TARGET_PROTOCOL,
                        removed: name,
                        kept_credential,
                    },
                    stdout,
                );
            }
            writeln!(stdout, "removed target '{name}'")?;
            if let Some(owner) = kept_credential {
                writeln!(
                    stdout,
                    "its stored credential `{owner}`, if any, stays; remove it with `omnigraph logout {owner}`"
                )?;
            }
            Ok(())
        }
    }
}

fn config_path(input: &InvocationConfig) -> Result<std::path::PathBuf> {
    input.config_path().ok_or_else(|| {
        CliRefusal::report(
            ErrorCode::ConfigNotEditable,
            "no home directory resolvable for the operator config; set OMNIGRAPH_HOME",
        )
    })
}

fn read_config_text(path: &std::path::Path) -> Result<String> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(text),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(error) => Err(color_eyre::eyre::eyre!(
            "could not read operator config '{}': {error}",
            path.display()
        )),
    }
}

fn read_entry(stdin: &mut dyn Read) -> Result<TargetEntry> {
    let mut text = String::new();
    stdin.take(ENTRY_MAX_BYTES + 1).read_to_string(&mut text)?;
    if text.len() as u64 > ENTRY_MAX_BYTES {
        return Err(CliRefusal::report(
            ErrorCode::TargetInvalid,
            format!("the target entry on stdin is larger than {ENTRY_MAX_BYTES} bytes"),
        ));
    }
    serde_json::from_str(&text).map_err(|error| {
        CliRefusal::report(
            ErrorCode::TargetInvalid,
            format!("the target entry on stdin is not a JSON target entry: {error}"),
        )
    })
}

/// The selected target: `--target`, else `defaults.target`.
fn selected_target<'a>(input: &'a InvocationConfig, config: &'a OperatorConfig) -> Result<&'a str> {
    input.target().or(config.default_target()).ok_or_else(|| {
        CliRefusal::report(
            ErrorCode::TargetUnknown,
            "no target selected: pass --target NAME or set `defaults.target`",
        )
    })
}

/// A server target's credential owner, the one key its token is stored
/// under.
pub(crate) fn credential_owner(config: &OperatorConfig, name: &str) -> Result<(String, String)> {
    match config.target(name)?.binding(name)? {
        TargetBinding::Server {
            endpoint,
            auth: ServerAuth::Token { owner },
            ..
        } => Ok((owner, endpoint)),
        TargetBinding::Server {
            auth: ServerAuth::Anonymous,
            ..
        } => Err(CliRefusal::report(
            ErrorCode::CredentialAnonymous,
            format!("target '{name}' uses anonymous access, so it has no credential"),
        )),
        TargetBinding::DirectGraph { .. } | TargetBinding::DirectRoot { .. } => {
            Err(CliRefusal::report(
                ErrorCode::TargetKindUnsupported,
                format!(
                    "target '{name}' is a direct target; only a server target has a credential"
                ),
            ))
        }
    }
}

/// The name `login`/`logout` store under: the server name given, or the
/// `--target` server target's credential owner.
pub(crate) fn login_owner(
    input: &InvocationConfig,
    name: Option<String>,
    stderr: &mut dyn Write,
) -> Result<String> {
    if let Some(name) = name {
        return Ok(name);
    }
    let target = input
        .target()
        .expect("clap requires a server name or --target");
    let config = input.get(stderr)?;
    credential_owner(config, target).map(|(owner, _)| owner)
}

#[derive(Serialize)]
struct CredentialOutput<'a> {
    protocol: u32,
    target: &'a str,
    owner: &'a str,
    endpoint: &'a str,
    scheme: &'static str,
    token: &'a str,
    source: &'static str,
}

/// `credential get` writes a token only to something that is not a
/// terminal, so a token never lands in scrollback.
fn refuse_terminal(stdout_is_terminal: bool) -> Result<()> {
    if stdout_is_terminal {
        return Err(CliRefusal::report(
            ErrorCode::CredentialTerminal,
            "credential get writes a token only to a pipe or a file, never to a terminal",
        ));
    }
    Ok(())
}

pub(crate) fn run_credential(
    input: &InvocationConfig,
    command: CredentialCommand,
    stdout_is_terminal: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    match command {
        CredentialCommand::Get { json } => {
            let config = input.get(stderr)?;
            let name = selected_target(input, config)?;
            let (owner, endpoint) = credential_owner(config, name)?;
            refuse_terminal(stdout_is_terminal)?;
            let variable = crate::operator::token_env_name(&owner);
            let from_environment = std::env::var(&variable)
                .ok()
                .map(|token| token.trim().to_string())
                .filter(|token| !token.is_empty());
            let (token, source) = match from_environment {
                Some(token) => (token, "environment"),
                None => {
                    let stored = match input.credentials_path() {
                        Some(path) => crate::operator::read_credential_at(&path, &owner)?,
                        None => None,
                    };
                    match stored {
                        Some(token) => (token, "credentials_file"),
                        None => {
                            return Err(CliRefusal::report(
                                ErrorCode::CredentialMissing,
                                format!(
                                    "no token is stored for credential `{owner}` of target '{name}': run `omnigraph login --target {name}` or set {variable}"
                                ),
                            ));
                        }
                    }
                }
            };
            if json {
                return write_json(
                    &CredentialOutput {
                        protocol: TARGET_PROTOCOL,
                        target: name,
                        owner: &owner,
                        endpoint: &endpoint,
                        scheme: "bearer",
                        token: &token,
                        source,
                    },
                    stdout,
                );
            }
            writeln!(stdout, "{token}")?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_never_goes_to_a_terminal() {
        let refused = refuse_terminal(true).unwrap_err();
        let refusal = refused.downcast_ref::<CliRefusal>().unwrap();
        assert_eq!(refusal.code, ErrorCode::CredentialTerminal);
        refuse_terminal(false).unwrap();
    }
}
