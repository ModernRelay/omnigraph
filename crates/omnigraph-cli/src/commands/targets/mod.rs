//! The `target` and `credential` command groups: the operator
//! configuration's named targets, and a server target's static token for
//! another program.

mod args;

pub(crate) use args::{CredentialCommand, TargetCommand};

use std::io::{Read, Write};

use color_eyre::Result;
use serde::Serialize;

use crate::operator::InvocationConfig;
use crate::output::write_json;
use crate::targets::refusal;
use crate::targets::{
    self, CREDENTIAL_PROTOCOL, ConfigEditLock, ResolvedTarget, ServerAuth, TARGET_PROTOCOL,
    TargetBinding, TargetEntry, TargetOutput, TargetProvenance,
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
        TargetCommand::Import { json } => {
            let path = config_path(input)?;
            let _lock = ConfigEditLock::acquire(&path)?;
            let before = read_config_text(&path)?;
            let imported = targets::import::convert(
                &before,
                path.parent().unwrap_or(std::path::Path::new(".")),
            )?;
            let mut config = crate::operator::parse_operator_config(&imported.text)?;
            config.base_dir = path
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .to_owned();
            let source = std::env::current_dir()?;
            if let Some((api, organization)) = crate::managed::legacy_custody_binding(&source)? {
                let mut selected: Option<crate::managed::Binding> = None;
                for name in config.targets.keys() {
                    if let Ok(TargetBinding::Managed(mut binding)) = config.binding(name) {
                        if binding.api == api
                            && binding.organization_id.as_deref() == Some(&organization)
                        {
                            binding.cluster = None;
                            if selected.as_ref().is_some_and(|other| other != &binding) {
                                return Err(refusal(
                                    "legacy_custody_ambiguous",
                                    "legacy recovery evidence matches conflicting managed authentication bindings",
                                ));
                            }
                            selected = Some(binding);
                        }
                    }
                }
                let binding = selected.ok_or_else(|| refusal("legacy_custody_binding_required", "legacy lifecycle evidence requires a complete matching managed target; preserve the original folder and configure that API/organization before importing"))?;
                let home = input.directory().ok_or_else(|| {
                    refusal(
                        "config_not_editable",
                        "set OMNIGRAPH_HOME to import lifecycle custody",
                    )
                })?;
                crate::managed::import_custody(&source, home, &binding)?;
            }
            if imported.text != before {
                let backup = path.with_file_name("config.legacy.yaml");
                match std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&backup)
                {
                    Ok(mut file) => {
                        file.write_all(before.as_bytes())?;
                        file.sync_all()?;
                    }
                    Err(error)
                        if error.kind() == std::io::ErrorKind::AlreadyExists
                            && read_config_text(&backup)? == before => {}
                    Err(error) => return Err(error.into()),
                }
                if let Some(credentials) = input.credentials_path() {
                    for (owner, endpoint) in &imported.credentials {
                        if let Some(saved) =
                            crate::operator::read_bound_credential_at(&credentials, owner)?
                        {
                            if let Some(bound) = &saved.endpoint {
                                if crate::graph_http::RequestTarget::of(bound, bound)?
                                    != crate::graph_http::RequestTarget::of(endpoint, endpoint)?
                                {
                                    return Err(refusal(
                                        "credential_scope_mismatch",
                                        "legacy credential is already bound to a different endpoint; nothing was imported",
                                    ));
                                }
                            }
                            crate::operator::rewrite_bound_credentials_at(
                                &credentials,
                                owner,
                                Some(endpoint),
                                Some(&saved.token),
                            )?;
                        }
                    }
                }
                targets::replace_file(&path, &imported.text)?;
            }
            if json {
                write_json(&targets::list_output(&config), stdout)
            } else {
                writeln!(
                    stdout,
                    "imported {} targets; original config is preserved as config.legacy.yaml",
                    config.targets.len()
                )?;
                Ok(())
            }
        }
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
            let target = TargetOutput::of(&name, config.target(&name)?, &config.base_dir);
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
                return Err(refusal(
                    "target_invalid",
                    format!(
                        "target name `{name}` must contain 1–64 ASCII letters, digits, `-`, `_` or `.` and not be reserved"
                    ),
                ));
            }
            let entry = read_entry(stdin)?;
            entry.binding_at(&name, path.parent().unwrap_or(std::path::Path::new(".")))?;
            let _lock = ConfigEditLock::acquire(&path)?;
            let before = read_config_text(&path)?;
            let current = crate::operator::parse_operator_config(&before)
                .map_err(|error| refusal("target_config_invalid", error.to_string()))?;
            if current.targets.contains_key(&name) {
                return Err(refusal(
                    "target_exists",
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
            let added = TargetOutput::of(
                &name,
                &entry,
                path.parent().unwrap_or(std::path::Path::new(".")),
            );
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
            let current = crate::operator::parse_operator_config(&before)
                .map_err(|error| refusal("target_config_invalid", error.to_string()))?;
            let entry = current.target(&name)?.clone();
            if current.default_target() == Some(name.as_str()) {
                return Err(refusal(
                    "target_is_default",
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
                    "its stored credential `{owner}`, if any, stays; log out before removing its last target to clear it"
                )?;
            }
            Ok(())
        }
    }
}

fn config_path(input: &InvocationConfig) -> Result<std::path::PathBuf> {
    input.config_path().ok_or_else(|| {
        refusal(
            "config_not_editable",
            "no home directory resolvable for the operator config; set OMNIGRAPH_HOME",
        )
    })
}

fn read_config_text(path: &std::path::Path) -> Result<String> {
    crate::operator::read_operator_file(path, "operator config")
        .map(|text| text.unwrap_or_default())
        .map_err(|error| refusal("target_config_invalid", error.to_string()))
}

fn read_entry(stdin: &mut dyn Read) -> Result<TargetEntry> {
    let mut text = String::new();
    stdin.take(ENTRY_MAX_BYTES + 1).read_to_string(&mut text)?;
    if text.len() as u64 > ENTRY_MAX_BYTES {
        return Err(refusal(
            "target_invalid",
            format!("the target entry on stdin is larger than {ENTRY_MAX_BYTES} bytes"),
        ));
    }
    serde_json::from_str(&text).map_err(|error| {
        refusal(
            "target_invalid",
            format!("the target entry on stdin is not a JSON target entry: {error}"),
        )
    })
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
        return Err(refusal(
            "credential_terminal",
            "credential get writes a token only to a pipe or a file, never to a terminal",
        ));
    }
    Ok(())
}

pub(crate) fn run_credential(
    input: &InvocationConfig,
    target: &ResolvedTarget,
    command: CredentialCommand,
    stdout_is_terminal: bool,
    stdout: &mut dyn Write,
    _stderr: &mut dyn Write,
) -> Result<()> {
    refuse_terminal(stdout_is_terminal)?;
    let name = match &target.provenance {
        TargetProvenance::Explicit { name } | TargetProvenance::Default { name } => name,
        TargetProvenance::Inline { .. } => {
            return Err(refusal(
                "target_kind_unsupported",
                "credential get requires a registered server target",
            ));
        }
    };
    let (endpoint, auth, owner) = match &target.binding {
        TargetBinding::Server {
            endpoint,
            auth: auth @ ServerAuth::Token { owner },
            ..
        } => (endpoint, auth, owner),
        TargetBinding::Server {
            auth: ServerAuth::Anonymous,
            ..
        } => {
            return Err(refusal(
                "credential_anonymous",
                format!("target '{name}' uses anonymous access, so it has no credential"),
            ));
        }
        _ => {
            return Err(refusal(
                "target_kind_unsupported",
                "credential get requires a registered server target",
            ));
        }
    };
    match command {
        CredentialCommand::Get { json } => {
            let variable = crate::operator::token_env_name(owner);
            let source = if std::env::var(&variable)
                .ok()
                .is_some_and(|v| !v.trim().is_empty())
            {
                "environment"
            } else {
                "credentials_file"
            };
            let token = targets::resolve_server_token(input, auth, endpoint)?
                .ok_or_else(|| refusal("credential_missing", "selected target has no token"))?;
            if json {
                return write_json(
                    &CredentialOutput {
                        protocol: CREDENTIAL_PROTOCOL,
                        target: name,
                        owner,
                        endpoint,
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
        let refusal = refused
            .downcast_ref::<crate::targets::TargetError>()
            .unwrap();
        assert_eq!(refusal.code, "credential_terminal");
        refuse_terminal(false).unwrap();
        let input = InvocationConfig::at(None, None);
        let target = ResolvedTarget::new(
            TargetBinding::Server {
                endpoint: "https://example.com".into(),
                auth: ServerAuth::Token {
                    owner: "not-configured".into(),
                },
                default_graph: None,
            },
            TargetProvenance::Explicit {
                name: "selected".into(),
            },
        );
        let mut output = Vec::new();
        let error = run_credential(
            &input,
            &target,
            CredentialCommand::Get { json: false },
            true,
            &mut output,
            &mut Vec::new(),
        )
        .unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<crate::targets::TargetError>()
                .unwrap()
                .code,
            "credential_terminal"
        );
        assert!(output.is_empty());
    }
}
