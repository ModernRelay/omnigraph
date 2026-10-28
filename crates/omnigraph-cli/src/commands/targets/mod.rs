//! The `target` and `credential` command groups and `access`: the operator
//! configuration's named targets, a server target's static token for another
//! program, and what the caller may do at a target.

mod access;
mod args;
use crate::credential_sources::{self as credentials, SecretStore};
use crate::operator::InvocationConfig;
use crate::operator_schema::{AuthProfile, Target, TokenSource};
use crate::targets::{self, ResolvedTarget, TargetBinding};
pub(crate) use access::run_access;
pub(crate) use args::{AccessArgs, ConfigCommand, CredentialCommand, TargetCommand};
use color_eyre::eyre::{Result, bail, eyre};
use std::io::{Read, Write};

fn path(input: &InvocationConfig) -> Result<std::path::PathBuf> {
    input
        .config_path()
        .ok_or_else(|| eyre!("no operator configuration path"))
}

fn edit(
    input: &InvocationConfig,
    change: impl FnOnce(&mut crate::operator_schema::OperatorConfig) -> Result<()>,
) -> Result<()> {
    let path = path(input)?;
    let _lock = targets::ConfigEditLock::acquire(&path)?;
    let _state_lock = crate::operator_routing::lock_auth_state(input)?;
    input.check_rename_pending()?;
    let before = crate::operator::read_operator_file(&path, "operator config")?
        .unwrap_or_else(|| "version: 1\n".into());
    let mut config = crate::operator_schema::parse(&before, path.parent().unwrap())?;
    change(&mut config)?;
    let after = yaml_serde::to_string(&config)?;
    crate::operator_schema::parse(&after, path.parent().unwrap())?;
    targets::replace_file(&path, &after)
}

pub(crate) async fn onboard(
    input: &InvocationConfig,
    name: &str,
    api: &str,
    organization: Option<&str>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    if !targets::valid_name(name) {
        bail!("profile names must start with an ASCII letter");
    }
    let configured = input.get(stderr)?;
    if configured
        .schema
        .as_ref()
        .is_some_and(|s| s.profiles.contains_key(name))
    {
        bail!("profile already exists; use login --profile");
    }
    let candidate = serde_json::json!({"version":1,"profiles":{name:{"api":api,"organization_id":"org_pending","auth":{"provider":"omnigraph"}}}});
    let validated =
        crate::operator_schema::parse(&serde_json::to_string(&candidate)?, &configured.base_dir)?;
    let mut profile = validated.profiles[name].clone();
    let route = profile.api.trim_end_matches('/').to_owned();
    profile.api = route.clone();
    let auth = crate::operator_routing::registration_auth(input, name, &route)?;
    crate::operator_routing::AUTH.scope(Some(auth), async {
            let receipt = crate::managed::register_login(route.clone(), organization, stderr).await?;
        let organization = receipt["data"]["account_id"].as_str().ok_or_else(|| eyre!("verified login response has no organization"))?;
        profile.organization_id = organization.to_owned();
        if let Err(error) = edit(input, |config| {
            if config.profiles.contains_key(name) { bail!("profile was registered concurrently"); }
            config.profiles.insert(name.into(), profile);
            Ok(())
        }) {
            let cleanup = crate::managed::discard_registration(route, organization).await;
            bail!("profile could not be saved: {error}; session cleanup completed: {}", cleanup.is_ok());
        }
        crate::output::write_json(&serde_json::json!({"registered":name,"profile":name,"organization_id":organization}), stdout)
    }).await
}

pub(crate) fn run_config(
    input: &InvocationConfig,
    selected: Option<&ResolvedTarget>,
    command: ConfigCommand,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    if matches!(command, ConfigCommand::RecoverRename) {
        crate::operator_routing::recover_rename(input)?;
        writeln!(stdout, "configuration rename recovered")?;
        return Ok(());
    }
    input.get(stderr)?;
    match command {
        ConfigCommand::RenameProfile { from, to } => {
            crate::operator_routing::rename(
                input,
                crate::operator_routing::Rename::Profile,
                &from,
                &to,
            )?;
            writeln!(stdout, "profile renamed; profile custody preserved")?;
            Ok(())
        }
        ConfigCommand::RenameTarget { from, to } => {
            crate::operator_routing::rename(
                input,
                crate::operator_routing::Rename::Target,
                &from,
                &to,
            )?;
            writeln!(stdout, "target renamed; profile custody preserved")?;
            Ok(())
        }
        ConfigCommand::RecoverRename => unreachable!(),
        ConfigCommand::RecoverOperations { source, json } => {
            let TargetBinding::Managed(binding) = &selected
                .ok_or_else(|| eyre!("select --profile NAME"))?
                .binding
            else {
                bail!("operation recovery requires a profile");
            };
            let home = input
                .directory()
                .ok_or_else(|| eyre!("operation recovery requires an operator home"))?;
            crate::managed::import_custody(&source, home, binding)?;
            if json {
                crate::output::write_json(
                    &serde_json::json!({"operation_evidence_preserved":true}),
                    stdout,
                )
            } else {
                writeln!(
                    stdout,
                    "operation evidence preserved; resume with the original verified principal and explicit original idempotency key"
                )?;
                Ok(())
            }
        }
        ConfigCommand::Validate { json: true } => {
            crate::output::write_json(&serde_json::json!({"valid":true,"version":1}), stdout)
        }
        ConfigCommand::Validate { json: false } => {
            writeln!(stdout, "operator configuration is valid")?;
            Ok(())
        }
    }
}

pub(crate) async fn run_target(
    input: &InvocationConfig,
    selected: Option<&ResolvedTarget>,
    command: TargetCommand,
    stdin: &mut dyn Read,
    stdin_is_terminal: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    match command {
        TargetCommand::List { json } => {
            let config = input.get(stderr)?;
            let source = config.schema.as_ref().map(|s| &s.targets);
            if json {
                return crate::output::write_json(&source, stdout);
            }
            if source.is_none_or(|targets| targets.is_empty()) {
                writeln!(
                    stdout,
                    "No targets configured. Use target add NAME --cluster ID for a managed cluster, or pipe a target JSON object to target add NAME."
                )?;
                return Ok(());
            }
            writeln!(stdout, "NAME\tKIND\tDESTINATION\tPROFILE\tDEFAULT")?;
            if let Some(source) = source {
                for (name, target) in source {
                    let (kind, destination, profile) = match target {
                        Target::Managed {
                            endpoint,
                            cluster_id,
                            profile,
                            ..
                        } => (
                            "managed",
                            endpoint.as_deref().unwrap_or(cluster_id),
                            profile.as_str(),
                        ),
                        Target::Server { endpoint, .. } => ("server", endpoint.as_str(), "-"),
                        Target::Direct { uri, .. } => ("direct", uri.as_str(), "-"),
                    };
                    let default = if config.default_target() == Some(name.as_str()) {
                        "*"
                    } else {
                        ""
                    };
                    writeln!(
                        stdout,
                        "{name}\t{kind}\t{destination}\t{profile}\t{default}"
                    )?;
                }
            }
            Ok(())
        }
        TargetCommand::Inspect {
            name,
            resolved,
            json,
        } => {
            let config = input.get(stderr)?;
            let source = config
                .schema
                .as_ref()
                .ok_or_else(|| eyre!("no operator configuration"))?;
            let target = source
                .targets
                .get(&name)
                .ok_or_else(|| eyre!("unknown target '{name}'"))?;
            let mut output = serde_json::json!({"name":name,"target":target});
            // The digests a Studio link carries, recomputed for this home, so
            // a client resolving the target itself can compare them.
            if let Some(binding) = crate::target_identity::binding_digest(target)? {
                output["binding"] = binding.into();
            }
            output["home"] = crate::target_identity::home_digest(input)?.into();
            if resolved {
                if let Target::Managed { profile, .. } = target {
                    output["profile"] = serde_json::to_value(&source.profiles[profile])?;
                }
                output["identity"] =
                    serde_json::json!({"status":"unknown","source":"offline inspection"});
            }
            if json {
                crate::output::write_json(&output, stdout)
            } else {
                writeln!(stdout, "{}", serde_json::to_string_pretty(&output)?)?;
                Ok(())
            }
        }
        TargetCommand::Add {
            name,
            cluster,
            json,
        } => {
            let frozen = input.get(stderr)?;
            if !targets::valid_name(&name) {
                bail!(
                    "target names must start with an ASCII letter and contain only ASCII letters, digits, dot, underscore or hyphen"
                );
            }
            if frozen.targets.contains_key(&name) {
                bail!("target already exists");
            }
            let profile = if cluster.is_some() {
                let target = selected
                    .ok_or_else(|| eyre!("select --profile NAME or configure defaults.profile"))?;
                match &target.binding {
                    TargetBinding::Managed(binding) if binding.cluster.is_none() => {
                        Some(target.label())
                    }
                    _ => bail!("managed registration requires a profile"),
                }
            } else {
                None
            };
            let expected_profile = profile.and_then(|name| {
                frozen
                    .schema
                    .as_ref()
                    .and_then(|source| source.profiles.get(name))
            });
            let entry = if let Some(cluster) = cluster {
                let profile = profile.expect("managed registration resolves a profile");
                let TargetBinding::Managed(binding) = &selected
                    .ok_or_else(|| eyre!("no selected profile"))?
                    .binding
                else {
                    bail!("managed registration requires a profile");
                };
                crate::managed::register_target(binding, profile, &cluster).await?
            } else {
                if stdin_is_terminal {
                    bail!(
                        "target add NAME reads a target JSON object from stdin; pipe one into this command, edit the operator configuration, or use --cluster ID for managed discovery"
                    );
                }
                let mut bytes = Vec::new();
                stdin.take(65537).read_to_end(&mut bytes)?;
                if bytes.len() > 65536 {
                    bail!("target input exceeds 64 KiB");
                }
                if bytes.iter().all(u8::is_ascii_whitespace) {
                    bail!(
                        "no target JSON on stdin; pipe a target object to target add NAME, or use --cluster ID for managed discovery"
                    );
                }
                serde_json::from_slice::<Target>(&bytes)?
            };
            edit(input, |config| {
                if let Some(profile) = profile {
                    if config.profiles.get(profile) != expected_profile {
                        bail!("profile changed during registration");
                    }
                }
                if config.targets.contains_key(&name) {
                    bail!("target already exists");
                }
                config.targets.insert(name.clone(), entry);
                Ok(())
            })?;
            if json {
                crate::output::write_json(&serde_json::json!({"registered":name}), stdout)
            } else {
                writeln!(stdout, "registered target '{name}'")?;
                Ok(())
            }
        }
        TargetCommand::Refresh { name, json } => {
            let config = input.get(stderr)?;
            let source = config
                .schema
                .as_ref()
                .ok_or_else(|| eyre!("no operator configuration"))?;
            let old = source
                .targets
                .get(&name)
                .ok_or_else(|| eyre!("unknown target"))?;
            let Target::Managed {
                profile,
                cluster_id,
                cluster_incarnation,
                ..
            } = old
            else {
                bail!("refresh requires a managed target");
            };
            let TargetBinding::Managed(binding) =
                &selected.ok_or_else(|| eyre!("no selected target"))?.binding
            else {
                bail!("refresh requires a managed target");
            };
            let fresh = crate::managed::register_target(binding, profile, cluster_id).await?;
            let Target::Managed {
                cluster_incarnation: incarnation,
                endpoint,
                ..
            } = fresh
            else {
                unreachable!()
            };
            if incarnation != *cluster_incarnation {
                bail!("cluster incarnation changed; refresh cannot rebind a resource");
            }
            let previous = match old {
                Target::Managed { endpoint, .. } => endpoint.clone(),
                _ => unreachable!(),
            };
            let endpoint_available = endpoint.is_some();
            let endpoint = endpoint.or_else(|| previous.clone());
            edit(input, |current| {
                if current.profiles.get(profile) != source.profiles.get(profile) {
                    bail!("profile changed during refresh");
                }
                let entry = current
                    .targets
                    .get_mut(&name)
                    .ok_or_else(|| eyre!("target was removed"))?;
                if entry != old {
                    bail!("target changed during refresh");
                }
                if let Target::Managed {
                    endpoint: current, ..
                } = entry
                {
                    *current = endpoint.clone();
                }
                Ok(())
            })?;
            let result = serde_json::json!({"target":name,"previous_endpoint":previous,"endpoint":endpoint,"endpoint_available":endpoint_available});
            if json {
                crate::output::write_json(&result, stdout)
            } else {
                writeln!(stdout, "{}", serde_json::to_string_pretty(&result)?)?;
                Ok(())
            }
        }
        TargetCommand::Remove { name, json } => {
            edit(input, |config| {
                if config.targets.remove(&name).is_none() {
                    bail!("unknown target");
                }
                Ok(())
            })?;
            if json {
                crate::output::write_json(
                    &serde_json::json!({"removed":name,"credentials_preserved":true}),
                    stdout,
                )
            } else {
                writeln!(
                    stdout,
                    "removed target '{name}'; credential custody and operation journals preserved"
                )?;
                Ok(())
            }
        }
    }
}

pub(crate) fn stdout_is_pipe() -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileTypeExt;
        std::fs::metadata("/dev/stdout").is_ok_and(|m| m.file_type().is_fifo())
    }
    #[cfg(not(unix))]
    {
        false
    }
}

pub(crate) async fn run_credential(
    target: &ResolvedTarget,
    command: CredentialCommand,
    profile_name: &str,
    stdin_is_terminal: bool,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
) -> Result<()> {
    if matches!(command, CredentialCommand::Set { .. }) && stdin_is_terminal {
        bail!("credentials set --stdin requires redirected input");
    }
    if matches!(command, CredentialCommand::Export) && !stdout_is_pipe() {
        bail!(
            "credentials export requires stdout to be a pipe; terminals and regular files are refused"
        );
    }
    if matches!(&target.binding, TargetBinding::Managed(binding) if binding.cluster.is_some())
        && !matches!(command, CredentialCommand::Export)
    {
        bail!("set/status/remove select the managed profile with --profile");
    }
    let auth = crate::operator_routing::AUTH.with(Clone::clone);
    if auth.is_none() && matches!(command, CredentialCommand::Status { .. }) {
        return crate::output::write_json(
            &serde_json::json!({"owner":target.label(),"profile":profile_name,"availability":"anonymous","identity":"unknown"}),
            stdout,
        );
    }
    let auth = auth.ok_or_else(|| eyre!("anonymous access has no credential"))?;
    match command {
        CredentialCommand::Set { replace, .. } => {
            if !matches!(
                auth.profile,
                AuthProfile::Token {
                    source: TokenSource::Store { .. }
                }
            ) {
                bail!("set requires provider: token with source: store");
            }
            let _lock = crate::managed::credential_lock().await?;
            if !replace && credentials::OsSecretStore.get(&auth.owner)?.is_some() {
                bail!("credential already exists; use --replace");
            }
            let mut bytes = Vec::new();
            stdin
                .take(credentials::MAX_CREDENTIAL_BYTES as u64 + 1)
                .read_to_end(&mut bytes)?;
            let secret = credentials::BearerSecret::from_input(bytes)?;
            auth.invalidate()?;
            crate::managed::clear_profile_data()?;
            credentials::OsSecretStore.put(&auth.owner, &secret)?;
            writeln!(stdout, "credential installed; server validity unverified")?;
        }
        CredentialCommand::Remove => {
            if !matches!(
                auth.profile,
                AuthProfile::Token {
                    source: TokenSource::Store { .. }
                }
            ) {
                bail!("remove requires provider: token with source: store");
            }
            let _lock = crate::managed::credential_lock().await?;
            auth.invalidate()?;
            credentials::OsSecretStore.remove(&auth.owner)?;
            crate::managed::clear_profile_data()?;
            writeln!(
                stdout,
                "local credential removed; remote revocation not requested"
            )?;
        }
        CredentialCommand::Status { json } => {
            let availability = match &auth.profile {
                AuthProfile::Token {
                    source: TokenSource::Store { .. },
                } => "unknown",
                AuthProfile::Token {
                    source: TokenSource::Env { env },
                } => {
                    if std::env::var_os(env).is_some() {
                        "present"
                    } else {
                        "absent"
                    }
                }
                AuthProfile::Token {
                    source: TokenSource::File { file },
                } => {
                    if std::fs::metadata(file).is_ok_and(|m| m.is_file()) {
                        "present"
                    } else {
                        "unavailable"
                    }
                }
                AuthProfile::Exec { .. } => "not executed",
                AuthProfile::OauthClientCredentials { client_secret, .. } => match client_secret {
                    crate::operator_schema::ClientSecretSource::Env { env } => {
                        if std::env::var_os(env).is_some() {
                            "present"
                        } else {
                            "absent"
                        }
                    }
                    crate::operator_schema::ClientSecretSource::File { file } => {
                        if std::fs::metadata(file).is_ok_and(|m| m.is_file()) {
                            "present"
                        } else {
                            "unavailable"
                        }
                    }
                },
                AuthProfile::Omnigraph => "unknown",
                AuthProfile::Anonymous => "anonymous",
            };
            let mut result = serde_json::json!({"owner":auth.owner,"profile":profile_name,"availability":availability,"identity":"unknown","server_validity":"unverified","observed_at":time::OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339)?,"verified_at":null});
            if matches!(auth.profile, AuthProfile::Omnigraph) {
                if let TargetBinding::Managed(binding) = &target.binding {
                    result["session"] = crate::managed::credential_status(binding);
                }
            }
            if json {
                crate::output::write_json(&result, stdout)?;
            } else {
                writeln!(stdout, "{}", serde_json::to_string_pretty(&result)?)?;
            }
        }
        CredentialCommand::Export => {
            let token = match &target.binding {
                TargetBinding::Managed(binding) => {
                    crate::managed::export_credential(binding).await?
                }
                _ => auth
                    .token()
                    .await?
                    .ok_or_else(|| eyre!("selected profile has no exportable credential"))?,
            };
            writeln!(stdout, "{token}")?;
        }
    }
    Ok(())
}
