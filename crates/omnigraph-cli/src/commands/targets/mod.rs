//! The `target` and `credential` command groups: the operator
//! configuration's named targets, and a server target's static token for
//! another program.

mod args;

pub(crate) use args::{AccessArgs, CredentialCommand, TargetCommand};

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

/// The name `login`/`logout` store a token under.
pub(crate) enum LoginKey {
    /// A server name given on the command line.
    Server(String),
    /// The `--target` server target and its credential owner.
    Target { target: String, owner: String },
}

impl LoginKey {
    /// The credentials-file section the token is stored under.
    pub(crate) fn owner(&self) -> &str {
        match self {
            Self::Server(name) => name,
            Self::Target { owner, .. } => owner,
        }
    }
}

/// The key `login`/`logout` store under: the server name given, or the
/// `--target` server target's credential owner.
pub(crate) fn login_owner(
    input: &InvocationConfig,
    name: Option<String>,
    stderr: &mut dyn Write,
) -> Result<LoginKey> {
    if let Some(name) = name {
        return Ok(LoginKey::Server(name));
    }
    let target = input
        .target()
        .expect("clap requires a server name or --target");
    let config = input.get(stderr)?;
    let (owner, _) = credential_owner(config, target)?;
    Ok(LoginKey::Target {
        target: target.to_string(),
        owner,
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

/// The addressing `access` resolves, from the global flags.
pub(crate) struct AccessScope<'a> {
    pub(crate) server: Option<&'a str>,
    pub(crate) profile: Option<&'a str>,
    pub(crate) store: Option<&'a str>,
    pub(crate) cluster: Option<&'a str>,
    pub(crate) graph: Option<&'a str>,
    pub(crate) actor: Option<&'a str>,
}

#[derive(Serialize)]
struct AccessReport<'a> {
    protocol: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<&'a str>,
    /// `server` when a server decided, `direct` when the CLI did.
    source: &'static str,
    #[serde(flatten)]
    access: omnigraph_api_types::AccessOutput,
}

/// Check that the cluster serves `graph` before its policy is read. A
/// quarantined graph's bundle is withheld from the snapshot and an unknown
/// graph has none, so an absent bundle means "no policy" only for a served
/// graph; the others refuse as a server refuses them.
fn served_graph(snapshot: &omnigraph_cluster::ServingSnapshot, graph: &str) -> Result<()> {
    if snapshot
        .quarantined_graphs
        .iter()
        .any(|blocked| blocked.graph_id == graph)
    {
        let path = format!("graph.{graph}");
        let reasons: Vec<&str> = snapshot
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.path == path)
            .map(|diagnostic| diagnostic.message.as_str())
            .collect();
        return Err(CliRefusal::report(
            ErrorCode::GraphUnavailable,
            format!(
                "graph `{graph}` is not served: {}",
                if reasons.is_empty() {
                    "the cluster quarantined it".to_string()
                } else {
                    reasons.join("; ")
                }
            ),
        ));
    }
    if !snapshot
        .graphs
        .iter()
        .any(|served| served.graph_id == graph)
    {
        return Err(CliRefusal::report(
            ErrorCode::NotFound,
            format!(
                "graph `{graph}` is not applied (applied graphs: [{}])",
                snapshot.applied_graphs.join(", ")
            ),
        ));
    }
    Ok(())
}

/// `access`: who the caller is at a target and what it may do there. A
/// server decides through its access route; for direct storage the CLI
/// evaluates the graph's installed policy for the declared actor, and a
/// graph without one allows every action, as direct writes do.
pub(crate) async fn run_access(
    input: &InvocationConfig,
    scope: AccessScope<'_>,
    args: AccessArgs,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    use omnigraph_api_types::AccessOutput;
    for branch in [args.branch.as_deref(), args.target_branch.as_deref()]
        .into_iter()
        .flatten()
    {
        omnigraph_policy::ensure_canonical_branch_name(branch)
            .map_err(|error| CliRefusal::report(ErrorCode::BadRequest, error.to_string()))?;
    }
    let config = input.get(stderr)?;
    let resolved = crate::scope::resolve_scope(
        config,
        input.profile(),
        crate::planes::Capability::Any,
        crate::scope::ScopeFlags {
            target: input.target(),
            profile: scope.profile,
            store: scope.store,
            server: scope.server,
            cluster: scope.cluster,
            graph: scope.graph,
            uri: None,
            reads_root: true,
        },
    )?;
    let actor = scope
        .actor
        .map(str::to_string)
        .or_else(|| config.actor().map(str::to_string));
    let target = input.target().or(
        if scope.server.is_none()
            && scope.store.is_none()
            && scope.cluster.is_none()
            && scope.profile.is_none()
        {
            config.default_target()
        } else {
            None
        },
    );
    let (source, access) = if let Some(server) = resolved.server.as_deref() {
        let graph = resolved.graph.as_deref().ok_or_else(|| {
            CliRefusal::report(
                ErrorCode::BadRequest,
                "access needs a graph: pass --graph ID, or give the target a default_graph",
            )
        })?;
        let base =
            crate::helpers::resolve_server_flag_with_config(config, Some(server), Some(graph))?
                .expect("a server address resolves");
        let root = crate::helpers::resolve_server_flag_with_config(config, Some(server), None)?
            .expect("a server address resolves");
        let (http, token) = crate::helpers::served_client_with_config(input, config, &root, &base)?;
        let http = http.with_expected_actor(scope.actor);
        let mut query = Vec::new();
        if let Some(branch) = args.branch.as_deref() {
            query.push(("branch", branch));
        }
        if let Some(target_branch) = args.target_branch.as_deref() {
            query.push(("target_branch", target_branch));
        }
        let url = crate::helpers::remote_url(&base, &["access"], &query)?;
        let access: AccessOutput =
            crate::helpers::remote_json(&http, reqwest::Method::GET, url, None, token.as_deref())
                .await?;
        ("server", access)
    } else if let Some(cluster) = resolved.cluster.as_deref() {
        let graph = resolved.cluster_graph.as_deref().ok_or_else(|| {
            CliRefusal::report(
                ErrorCode::BadRequest,
                "access on a cluster needs a graph: pass --graph ID, or give the target a default_graph",
            )
        })?;
        let snapshot = crate::commands::control::read_serving_snapshot_or_report(cluster).await?;
        served_graph(&snapshot, graph)?;
        let binding = format!("graph.{graph}");
        let bundles: Vec<_> = snapshot
            .policies
            .iter()
            .filter(|policy| policy.applies_to.iter().any(|scope| scope == &binding))
            .collect();
        let engine = match bundles.as_slice() {
            [] => None,
            [bundle] => Some(omnigraph_policy::PolicyEngine::load_graph_from_source(
                &bundle.source,
                graph,
            )?),
            many => color_eyre::eyre::bail!(
                "graph `{graph}` matches {} policy bundles; the cluster binds one per graph",
                many.len()
            ),
        };
        let access = AccessOutput::evaluate(
            actor.clone(),
            graph.to_string(),
            args.branch.clone(),
            args.target_branch.clone(),
            |access, branch, target_branch| -> Result<bool> {
                let Some(engine) = &engine else {
                    return Ok(true);
                };
                let Some(actor) = actor.as_deref() else {
                    return Ok(false);
                };
                Ok(engine
                    .authorize(
                        actor,
                        &omnigraph_policy::PolicyRequest {
                            action: access.into(),
                            branch: branch.map(str::to_string),
                            target_branch: target_branch.map(str::to_string),
                        },
                    )?
                    .allowed)
            },
        )?;
        ("direct", access)
    } else if resolved.uri.is_some() {
        let access = AccessOutput::evaluate(
            actor.clone(),
            "default".to_string(),
            args.branch.clone(),
            args.target_branch.clone(),
            |_, _, _| Ok::<_, color_eyre::Report>(true),
        )?;
        ("direct", access)
    } else {
        return Err(CliRefusal::report(
            ErrorCode::TargetUnknown,
            "access needs a target: pass --target NAME or set `defaults.target`",
        ));
    };
    let report = AccessReport {
        protocol: TARGET_PROTOCOL,
        target,
        source,
        access,
    };
    if args.json {
        return write_json(&report, stdout);
    }
    let names = |actions: &[omnigraph_api_types::AccessAction]| -> String {
        let names: Vec<String> = actions
            .iter()
            .map(|action| {
                serde_json::to_value(action)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_string))
                    .unwrap_or_default()
            })
            .collect();
        if names.is_empty() {
            "none".to_string()
        } else {
            names.join(", ")
        }
    };
    writeln!(
        stdout,
        "actor: {}",
        report.access.actor.as_deref().unwrap_or("(none)")
    )?;
    writeln!(stdout, "graph: {} ({source})", report.access.graph_id)?;
    writeln!(stdout, "graph-wide: {}", names(&report.access.graph_wide))?;
    if let Some(branch) = &report.access.branch {
        writeln!(stdout, "on {branch}: {}", names(&report.access.actions))?;
    }
    Ok(())
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

    #[test]
    fn access_reads_policy_only_for_a_graph_the_cluster_serves() {
        let snapshot = omnigraph_cluster::ServingSnapshot {
            graphs: vec![omnigraph_cluster::ServingGraph {
                graph_id: "knowledge".into(),
                root: std::path::PathBuf::new(),
                embedding: None,
                external_blob_policy: Default::default(),
            }],
            queries: Vec::new(),
            policies: Vec::new(),
            diagnostics: vec![omnigraph_cluster::Diagnostic {
                code: "recovery_pending".into(),
                severity: omnigraph_cluster::DiagnosticSeverity::Error,
                path: "graph.archive".into(),
                message: "a recovery is pending".into(),
                detail: None,
            }],
            config_digest: None,
            state_revision: 1,
            state_cas: None,
            applied_graphs: vec!["archive".into(), "knowledge".into()],
            quarantined_graphs: vec![omnigraph_cluster::ServingBlockedGraph {
                graph_id: "archive".into(),
                root: std::path::PathBuf::new(),
            }],
        };
        served_graph(&snapshot, "knowledge").unwrap();
        for (graph, code, says) in [
            (
                "archive",
                ErrorCode::GraphUnavailable,
                "a recovery is pending",
            ),
            ("missing", ErrorCode::NotFound, "archive, knowledge"),
        ] {
            let refused = served_graph(&snapshot, graph).unwrap_err();
            let refusal = refused.downcast_ref::<CliRefusal>().unwrap();
            assert_eq!(refusal.code, code, "{graph}");
            assert!(
                refusal.message.contains(says),
                "{graph}: {}",
                refusal.message
            );
        }
    }
}
