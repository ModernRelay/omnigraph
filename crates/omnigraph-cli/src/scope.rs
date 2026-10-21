//! Capture exactly one destination. Command inputs never participate in routing.
use crate::cli::{BlobCommand, BranchCommand, Cli, Command, CommitCommand, SchemaCommand};
use crate::operator::InvocationConfig;
use crate::planes::{self, Requirement};
use crate::targets::{self, ResolvedTarget, StorageProvider, TargetBinding, TargetProvenance};
use color_eyre::eyre::{Result, bail};
use std::io::Write;
use std::path::Path;

pub(crate) fn resolve(
    cli: &Cli,
    input: &InvocationConfig,
    cwd: &Path,
    explicit_config: bool,
    stderr: &mut dyn Write,
) -> Result<Option<ResolvedTarget>> {
    planes::guard_addressing(cli)
        .map_err(|e| targets::refusal("argument_conflict", e.to_string()))?;
    let need = planes::requirement(&cli.command);
    if need == Requirement::Local {
        return Ok(None);
    }
    let config = input.get(stderr)?;
    let connection_class = matches!(need, Requirement::ManagedAccount)
        || (need == Requirement::Authentication && cli.target.is_none())
        || (cli.connection.is_some()
            && matches!(
                need,
                Requirement::Authentication
                    | Requirement::Credential
                    | Requirement::ManagedOperation
            ));
    if connection_class {
        let source = config
            .schema
            .as_ref()
            .ok_or_else(|| color_eyre::eyre::eyre!("operator configuration requires version: 1"))?;
        let name = cli
            .connection
            .as_deref()
            .or(source.defaults.connection.as_deref())
            .ok_or_else(|| {
                targets::refusal(
                    "connection_required",
                    "select --connection NAME or configure defaults.connection",
                )
            })?;
        let connection = source.connections.get(name).ok_or_else(|| {
            targets::refusal("connection_unknown", format!("unknown connection '{name}'"))
        })?;
        let target = ResolvedTarget::new(
            TargetBinding::Managed(crate::managed::Binding {
                api: connection.api.trim_end_matches('/').to_owned(),
                organization_id: Some(connection.organization_id.clone()),
                cluster: None,
                control_auth: crate::managed::ControlAuth::HumanDevice,
            }),
            if cli.connection.is_some() {
                TargetProvenance::Explicit { name: name.into() }
            } else {
                TargetProvenance::Default { name: name.into() }
            },
        );
        planes::validate_target(cli, &target, explicit_config)?;
        return Ok(Some(target));
    }
    let init_uri = match &cli.command {
        Command::Init(args) => args.uri.as_deref(),
        _ => None,
    };
    if init_uri.is_some() && cli.target.is_some() {
        return Err(targets::refusal(
            "argument_conflict",
            "init URI and --target are mutually exclusive",
        ));
    }
    let alias = match &cli.command {
        Command::Alias(args) => Some(config.alias(&args.name)?),
        _ => None,
    };
    let explicit = init_uri
        .or(cli.target.as_deref())
        .or(match &cli.command {
            Command::Target {
                command: crate::cli::TargetCommand::Refresh { name, .. },
            } => Some(name.as_str()),
            _ => None,
        })
        .or(alias.map(|a| a.target.as_str()));
    let (selection, is_default) = match explicit {
        Some(value) => (value, false),
        None => (
            config.default_target().ok_or_else(|| {
                targets::refusal(
                    "target_required",
                    "select --target NAME|ADDRESS or configure defaults.target",
                )
            })?,
            true,
        ),
    };
    if selection.is_empty() {
        return Err(targets::refusal(
            "target_invalid",
            "target must not be empty",
        ));
    }
    let inline = matches!(selection, "." | "..")
        || init_uri.is_some()
        || selection.contains(':')
        || selection.contains('/')
        || selection.starts_with('~');
    if (is_default
        || alias.is_some()
        || matches!(need, Requirement::Authentication | Requirement::Credential))
        && inline
    {
        return Err(targets::refusal(
            "target_invalid",
            "this selection requires a registered target name",
        ));
    }
    if inline
        && (crate::graph_http::server_scheme(selection).is_some()
            || selection.contains("://") && !selection.starts_with("file://"))
    {
        bail!("inline access supports local storage only; configure a server or managed target");
    }
    let target = if inline {
        let binding = {
            let uri = targets::normalize_storage_uri(selection, cwd)
                .map_err(|error| targets::refusal("target_invalid", error.to_string()))?;
            let provider = StorageProvider::Local;
            if init_uri.is_none() && (planes::root_input(&cli.command) || cli.graph.is_some()) {
                TargetBinding::DirectRoot {
                    uri,
                    provider,
                    default_graph: None,
                }
            } else {
                TargetBinding::DirectGraph { uri, provider }
            }
        };
        ResolvedTarget::new(
            binding,
            TargetProvenance::Inline {
                address: selection.into(),
            },
        )
    } else {
        ResolvedTarget::new(
            config.binding(selection)?,
            if is_default {
                TargetProvenance::Default {
                    name: selection.into(),
                }
            } else {
                TargetProvenance::Explicit {
                    name: selection.into(),
                }
            },
        )
    };
    planes::validate_target(cli, &target, explicit_config)
        .map_err(|e| targets::refusal("target_incompatible", e.to_string()))?;
    if matches!(need, Requirement::Graph { .. }) {
        graph_selection(&target, cli.graph.as_deref())
            .map_err(|error| targets::refusal("target_incompatible", error.to_string()))?;
    }
    if matches!(
        &target.binding,
        TargetBinding::DirectGraph { .. } | TargetBinding::DirectRoot { .. }
    ) {
        target.storage()?;
    }
    Ok(Some(target))
}

pub(crate) fn graph_selection<'a>(
    target: &'a ResolvedTarget,
    graph: Option<&'a str>,
) -> Result<Option<&'a str>> {
    let default = match &target.binding {
        TargetBinding::Server { default_graph, .. }
        | TargetBinding::DirectRoot { default_graph, .. } => default_graph.as_deref(),
        TargetBinding::Managed(binding) => binding
            .cluster
            .as_ref()
            .and_then(|c| c.default_graph.as_deref()),
        TargetBinding::DirectGraph { .. } | TargetBinding::DirectCatalogue => {
            if graph.is_some() {
                bail!("this target already identifies its graph; remove --graph");
            }
            return Ok(None);
        }
    };
    let graph = graph.or(default).ok_or_else(|| targets::refusal("graph_required", "select --graph ID or configure target.default_graph; graphs are never selected by discovery"))?;
    crate::helpers::require_graph_segment(graph)?;
    Ok(Some(graph))
}

pub(crate) fn direct_root_uri(target: &ResolvedTarget) -> Result<&str> {
    match &target.binding {
        TargetBinding::DirectRoot { uri, .. } => Ok(uri),
        _ => bail!("this operation requires a direct root target"),
    }
}

pub(crate) async fn direct_graph_uri(
    target: &ResolvedTarget,
    graph: Option<&str>,
) -> Result<String> {
    match &target.binding {
        TargetBinding::DirectGraph { uri, .. } => {
            graph_selection(target, graph)?;
            Ok(uri.clone())
        }
        TargetBinding::DirectRoot { .. } => {
            let graph = graph_selection(target, graph)?.expect("root requires graph");
            omnigraph_cluster::resolve_graph_storage_uri_with_binding(
                &capture_storage_binding(target)?,
                graph,
            )
            .await
            .map_err(|d| color_eyre::eyre::eyre!("{}", d.message))
        }
        _ => bail!("this operation requires direct storage"),
    }
}

/// The write scope known without opening a graph or interpreting stored code.
/// Commit-addressed and opaque source operations must not claim a default branch.
#[derive(Clone, Copy)]
pub(crate) enum WriteScope<'a> {
    Branch(&'a str),
    Commit(&'a str),
    StoredMutation(&'a str),
    BackendResolved,
}

impl std::fmt::Display for WriteScope<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Branch(branch) => write!(f, "branch={branch}"),
            Self::Commit(commit) => write!(f, "commit={commit}"),
            Self::StoredMutation(name) => {
                write!(f, "stored-mutation={name}; branch=backend-resolved")
            }
            Self::BackendResolved => f.write_str("branch=backend-resolved"),
        }
    }
}

pub(crate) fn command_write_scope(command: &Command) -> Option<WriteScope<'_>> {
    match command {
        Command::Load(args) => Some(WriteScope::Branch(args.branch.as_deref().unwrap_or("main"))),
        Command::Branch { command } => match command {
            BranchCommand::Create { name, .. } | BranchCommand::Delete { name, .. } => {
                Some(WriteScope::Branch(name))
            }
            BranchCommand::Merge { into, .. } => {
                Some(WriteScope::Branch(into.as_deref().unwrap_or("main")))
            }
            BranchCommand::List { .. }
            | BranchCommand::Diff { .. }
            | BranchCommand::MergeBase { .. } => None,
        },
        Command::Blob {
            command: BlobCommand::Put { branch, .. } | BlobCommand::Clear { branch, .. },
        } => Some(WriteScope::Branch(branch.as_deref().unwrap_or("main"))),
        Command::RebuildFullTextIndexes(args) => Some(WriteScope::Branch(&args.branch)),
        Command::Init(_)
        | Command::Schema {
            command: SchemaCommand::Apply { .. },
        } => Some(WriteScope::Branch("main")),
        Command::Commit {
            command:
                CommitCommand::Publish { commit_id, .. } | CommitCommand::Drop { commit_id, .. },
        } => Some(WriteScope::Commit(commit_id)),
        // mutate resolves its scope from the existing source preparation below
        // the initial destination barrier. Root/whole-graph work has no branch.
        _ => None,
    }
}

pub(crate) fn report_write_scope(scope: WriteScope<'_>, stderr: &mut dyn Write) -> Result<()> {
    writeln!(stderr, "target write scope: {scope}")?;
    stderr.flush()?;
    Ok(())
}

pub(crate) fn report(
    input: &InvocationConfig,
    target: &ResolvedTarget,
    graph: Option<&str>,
    write_scope: Option<WriteScope<'_>>,
    stderr: &mut dyn Write,
) -> Result<()> {
    let source = match &target.provenance {
        TargetProvenance::Explicit { .. } => "explicit",
        TargetProvenance::Default { .. } => "default",
        TargetProvenance::Inline { .. } => "inline",
    };
    let destination = match &target.binding {
        TargetBinding::DirectGraph { uri, provider } => {
            format!(
                "direct graph {}; requested={uri}; provider={provider:?}",
                target.storage()?.uri()
            )
        }
        TargetBinding::DirectRoot { uri, provider, .. } => {
            format!(
                "direct root {}; requested={uri}; provider={provider:?}",
                target.storage()?.uri()
            )
        }
        TargetBinding::DirectCatalogue => "direct catalogue og_catalog".into(),
        TargetBinding::Server { endpoint, .. } => format!("server {endpoint}"),
        TargetBinding::Managed(binding) if binding.cluster.is_none() => format!(
            "api {}; organization={}",
            binding.api,
            binding.organization_id.as_deref().unwrap_or("unselected")
        ),
        TargetBinding::Managed(binding) => format!(
            "managed {}; organization={}; cluster={}; incarnation={}; endpoint={}",
            binding.api,
            binding.organization_id.as_deref().unwrap_or("unselected"),
            binding
                .cluster
                .as_ref()
                .map_or("none", |c| c.cluster_id.as_str()),
            binding
                .cluster
                .as_ref()
                .map_or("none", |c| c.cluster_incarnation.as_str()),
            binding
                .cluster
                .as_ref()
                .and_then(|c| c.endpoint.as_deref())
                .unwrap_or("none")
        ),
    };
    let config = if source == "inline" {
        String::new()
    } else {
        format!(
            "; config={}",
            input
                .config_path()
                .map_or_else(|| "none".into(), |p| p.display().to_string())
        )
    };
    let selection_kind = if matches!(&target.binding, TargetBinding::Managed(binding) if binding.cluster.is_none())
    {
        "connection"
    } else {
        "target"
    };
    writeln!(
        stderr,
        "{selection_kind} {} ({source}): {destination}{}{}{}",
        target.label(),
        graph.map_or_else(String::new, |g| format!("; graph={g}")),
        write_scope.map_or_else(String::new, |scope| format!("; {scope}")),
        config
    )?;
    stderr.flush()?;
    Ok(())
}

/// Convert the validated CLI destination into the storage layer's captured binding.
pub(crate) fn capture_storage_binding(
    target: &ResolvedTarget,
) -> Result<omnigraph::storage::CapturedStorageBinding> {
    target.storage()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invocation::parse_from;

    #[test]
    fn connection_report_does_not_present_an_account_as_a_resource_target() {
        let input = InvocationConfig::at(None, None);
        let selected = ResolvedTarget::new(
            TargetBinding::Managed(crate::managed::Binding {
                api: "https://control.example".into(),
                organization_id: Some("org_one".into()),
                cluster: None,
                control_auth: crate::managed::ControlAuth::HumanDevice,
            }),
            TargetProvenance::Explicit {
                name: "work".into(),
            },
        );
        let mut output = Vec::new();
        report(&input, &selected, None, None, &mut output).unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(output.starts_with(
            "connection work (explicit): api https://control.example; organization=org_one"
        ));
        assert!(!output.contains("cluster="));
        assert!(!output.contains("target work"));
    }

    fn fixture() -> (tempfile::TempDir, InvocationConfig) {
        let dir = tempfile::tempdir().unwrap();
        let source = r#"version: 1
connections:
  work:
    api: https://control.example/prefix
    organization_id: org_one
    auth: {me: {provider: omnigraph}}
targets:
  local: {kind: direct, scope: graph, uri: 'file:///tmp/graph'}
  cluster:
    kind: managed
    connection: work
    cluster_id: cluster-one
    cluster_incarnation: inc-one
    endpoint: https://data.example/
    default_graph: knowledge
  server:
    kind: server
    endpoint: https://example.test/
    auth: {public: {provider: anonymous}}
    default_graph: knowledge
defaults: {target: local, connection: work}
"#;
        std::fs::write(dir.path().join("config.yaml"), source).unwrap();
        let input = InvocationConfig::at(Some(dir.path().to_owned()), None);
        (dir, input)
    }

    fn selected(
        args: &[&str],
        input: &InvocationConfig,
        cwd: &Path,
    ) -> Result<Option<ResolvedTarget>> {
        let parsed = parse_from(std::iter::once("omnigraph").chain(args.iter().copied())).unwrap();
        resolve(
            &parsed.cli,
            input,
            cwd,
            parsed.explicit_config,
            &mut Vec::new(),
        )
    }

    #[test]
    fn connection_and_resource_defaults_are_independent() {
        let (dir, input) = fixture();
        let resource = selected(&["query", "-e", "q"], &input, dir.path())
            .unwrap()
            .unwrap();
        assert!(matches!(
            resource.binding,
            TargetBinding::DirectGraph { .. }
        ));
        let control = selected(&["cluster", "list"], &input, dir.path())
            .unwrap()
            .unwrap();
        assert!(
            matches!(control.binding, TargetBinding::Managed(binding) if binding.cluster.is_none() && binding.api == "https://control.example/prefix")
        );
        assert!(
            selected(
                &["cluster", "list", "--target", "cluster"],
                &input,
                dir.path()
            )
            .is_err()
        );
        assert!(
            selected(
                &["query", "-e", "q", "--connection", "work"],
                &input,
                dir.path()
            )
            .is_err()
        );
    }

    #[test]
    fn managed_resource_has_every_supported_served_command() {
        let (dir, input) = fixture();
        for args in [
            vec!["query", "-e", "q"],
            vec!["schema", "show"],
            vec!["branch", "list"],
            vec!["snapshot"],
            vec!["export"],
            vec!["commit", "show", "commit-one"],
            vec!["graphs", "list"],
            vec!["changes", "poll"],
        ] {
            for target in ["server", "cluster"] {
                let mut args = args.clone();
                args.extend(["--target", target]);
                assert!(selected(&args, &input, dir.path()).is_ok(), "{args:?}");
            }
        }
        assert!(selected(&["optimize", "--target", "cluster"], &input, dir.path()).is_err());
        assert!(
            selected(
                &["queries", "list", "--target", "cluster"],
                &input,
                dir.path()
            )
            .is_err()
        );
    }

    #[test]
    fn local_commands_ignore_invalid_config_but_refuse_auth_and_routes() {
        let (dir, _) = fixture();
        std::fs::write(dir.path().join("config.yaml"), "invalid: [").unwrap();
        let input = InvocationConfig::at(Some(dir.path().to_owned()), None);
        assert!(
            selected(&["version"], &input, dir.path())
                .unwrap()
                .is_none()
        );
        assert!(selected(&["version", "--target", "cluster"], &input, dir.path()).is_err());
        assert!(selected(&["version", "--auth", "me"], &input, dir.path()).is_err());
    }
}
