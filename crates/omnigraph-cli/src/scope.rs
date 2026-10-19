//! Capture exactly one destination. Command inputs never participate in routing.
use crate::cli::{Cli, Command};
use crate::operator::InvocationConfig;
use crate::planes::{self, Requirement};
use crate::targets::{
    self, CredentialOwner, ResolvedTarget, ServerAuth, StorageProvider, TargetBinding,
    TargetProvenance,
};
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
        .or(alias.map(|a| a.target.as_str()));
    let (selection, is_default) = match explicit {
        Some(value) => (value, false),
        None => {
            if input.has_profile() {
                return Err(targets::refusal(
                    "legacy_routing",
                    "OMNIGRAPH_PROFILE is retired; unset it and select --target explicitly or configure defaults.target",
                ));
            }
            // The old folder binding applied to managed data/control commands.
            if matches!(
                need,
                Requirement::Graph { managed: true, .. }
                    | Requirement::Registry
                    | Requirement::Deployment
                    | Requirement::ManagedAccount
                    | Requirement::ManagedCluster
                    | Requirement::ManagedOperation
                    | Requirement::Authentication
            ) {
                match std::fs::symlink_metadata(cwd.join(".omnigraph/context")) {
                    Ok(_) => {
                        return Err(targets::refusal(
                            "legacy_routing",
                            "folder routing is retired; select --target explicitly, or import recovery custody with target import and archive .omnigraph/context before using defaults.target",
                        ));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            (
                config.default_target().ok_or_else(|| {
                    targets::refusal(
                        "target_required",
                        "select --target NAME|ADDRESS or configure defaults.target",
                    )
                })?,
                true,
            )
        }
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
    let target = if selection == "og_catalog" && !is_default && alias.is_none() {
        ResolvedTarget::new(
            TargetBinding::DirectCatalogue,
            TargetProvenance::Inline {
                address: selection.into(),
            },
        )
    } else if inline {
        let binding = if crate::graph_http::server_scheme(selection).is_some() {
            targets::validate_server_endpoint(selection)
                .map_err(|error| targets::refusal("target_invalid", error.to_string()))?;
            let request = crate::graph_http::RequestTarget::of(selection, selection)?;
            let auth = match config.credential_owner_for(&request) {
                Some(CredentialOwner::Keyed(owner)) => ServerAuth::Token {
                    owner: owner.into(),
                },
                _ => ServerAuth::Anonymous,
            };
            TargetBinding::Server {
                endpoint: selection.into(),
                auth,
                default_graph: None,
            }
        } else {
            let uri = targets::normalize_storage_uri(selection, cwd)
                .map_err(|error| targets::refusal("target_invalid", error.to_string()))?;
            let provider = StorageProvider::capture(&uri)
                .map_err(|error| targets::refusal("target_invalid", error.to_string()))?;
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

pub(crate) fn report(
    input: &InvocationConfig,
    target: &ResolvedTarget,
    graph: Option<&str>,
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
    writeln!(
        stderr,
        "target {} ({source}): {destination}{}{}",
        target.label(),
        graph.map_or_else(String::new, |g| format!("; graph={g}")),
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

    fn fixture(yaml: &str) -> (tempfile::TempDir, InvocationConfig) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("config.yaml"), yaml).unwrap();
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
    const CONFIG: &str = "target_config_version: 1\ndefaults:\n  target: missing\ntargets:\n  local:\n    kind: direct\n    scope: graph\n    uri: ./graph.omni\n    provider:\n      kind: local\n  server:\n    kind: server\n    endpoint: https://example.test/api\n    auth: anonymous\n    default_graph: knowledge\n";

    #[test]
    fn explicit_target_never_falls_back_or_validates_an_unused_default() {
        let (dir, input) = fixture(CONFIG);
        let target = selected(
            &["query", "-e", "query { return 1 }", "--target", "local"],
            &input,
            dir.path(),
        )
        .unwrap()
        .unwrap();
        assert!(matches!(target.binding, TargetBinding::DirectGraph { .. }));
        for name in ["", "absent"] {
            assert!(selected(&["query", "q", "--target", name], &input, dir.path()).is_err());
        }
        assert!(
            selected(&["query", "q"], &input, dir.path())
                .unwrap_err()
                .to_string()
                .contains("missing")
        );
    }

    #[test]
    fn named_paths_use_operator_directory_and_inline_paths_use_invocation_directory() {
        let (dir, input) = fixture(CONFIG);
        let cwd = tempfile::tempdir().unwrap();
        for (value, expected) in [("local", dir.path()), ("./graph.omni", cwd.path())] {
            let target = selected(&["query", "-e", "q", "--target", value], &input, cwd.path())
                .unwrap()
                .unwrap();
            let TargetBinding::DirectGraph { uri, .. } = target.binding else {
                panic!("direct graph");
            };
            assert_eq!(uri, expected.join("graph.omni").to_string_lossy());
        }
    }

    #[test]
    fn local_commands_ignore_bad_config_but_refuse_irrelevant_selectors() {
        let (dir, input) = fixture("invalid: [");
        assert!(
            selected(&["version"], &input, dir.path())
                .unwrap()
                .is_none()
        );
        assert!(selected(&["version", "--target", "anything"], &input, dir.path()).is_err());
    }

    #[test]
    fn default_selection_refuses_retired_routing_and_explicit_selection_does_not() {
        let (dir, _) = fixture(&CONFIG.replace("target: missing", "target: local"));
        let input = InvocationConfig::at(Some(dir.path().to_owned()), Some("old".into()));
        assert!(
            selected(&["query", "q"], &input, dir.path())
                .unwrap_err()
                .to_string()
                .contains("OMNIGRAPH_PROFILE")
        );
        assert!(
            selected(
                &["query", "-e", "q", "--target", "local"],
                &input,
                dir.path()
            )
            .is_ok()
        );
        let input = InvocationConfig::at(Some(dir.path().to_owned()), None);
        std::fs::create_dir(dir.path().join(".omnigraph")).unwrap();
        std::fs::write(dir.path().join(".omnigraph/context"), "unparsed evidence").unwrap();
        assert!(
            selected(&["query", "q"], &input, dir.path())
                .unwrap_err()
                .to_string()
                .contains("folder routing")
        );
        assert!(
            selected(
                &["query", "-e", "q", "--target", "local"],
                &input,
                dir.path()
            )
            .is_ok()
        );
        #[cfg(unix)]
        {
            let context = dir.path().join(".omnigraph/context");
            std::fs::remove_file(&context).unwrap();
            std::os::unix::fs::symlink("missing-context", &context).unwrap();
            assert!(
                selected(&["query", "-e", "q"], &input, dir.path())
                    .unwrap_err()
                    .to_string()
                    .contains("folder routing")
            );
            assert!(
                selected(
                    &["query", "-e", "q", "--target", "local"],
                    &input,
                    dir.path()
                )
                .is_ok()
            );
        }
    }

    #[test]
    fn graph_selection_is_explicit_and_registry_ignores_graph_defaults() {
        let (dir, input) = fixture(CONFIG);
        assert!(
            selected(
                &["query", "q", "--target", "https://example.test"],
                &input,
                dir.path()
            )
            .unwrap_err()
            .to_string()
            .contains("graph_required")
        );
        let target = selected(&["query", "q", "--target", "server"], &input, dir.path())
            .unwrap()
            .unwrap();
        assert_eq!(graph_selection(&target, None).unwrap(), Some("knowledge"));
        assert!(
            selected(
                &["graphs", "list", "--target", "server"],
                &input,
                dir.path()
            )
            .is_ok()
        );
        assert!(
            selected(
                &[
                    "graphs",
                    "list",
                    "--target",
                    "server",
                    "--graph",
                    "knowledge"
                ],
                &input,
                dir.path()
            )
            .is_err()
        );
        assert!(
            selected(
                &[
                    "query",
                    "-e",
                    "q",
                    "--target",
                    "local",
                    "--graph",
                    "knowledge"
                ],
                &input,
                dir.path()
            )
            .is_err()
        );
    }

    #[test]
    fn init_uri_is_explicit_inline_and_conflicts_with_target() {
        let (dir, input) = fixture(CONFIG);
        let target = selected(
            &["init", "scratch.omni", "--schema", "unused.pg"],
            &input,
            dir.path(),
        )
        .unwrap()
        .unwrap();
        assert!(matches!(target.provenance, TargetProvenance::Inline { .. }));
        assert!(
            selected(
                &[
                    "init",
                    "scratch.omni",
                    "--schema",
                    "unused.pg",
                    "--target",
                    "local"
                ],
                &input,
                dir.path()
            )
            .is_err()
        );
    }

    #[test]
    fn unsupported_kind_is_rejected_without_reading_credentials() {
        let (dir, input) = fixture(CONFIG);
        std::fs::write(dir.path().join("credentials"), "invalid credential bytes").unwrap();
        let error = selected(&["cleanup", "--target", "server"], &input, dir.path()).unwrap_err();
        assert!(error.to_string().contains("not available"), "{error}");
        for args in [
            vec!["query", "stored", "--target", "local"],
            vec!["query", "stored", "--target", "og_catalog"],
            vec![
                "mutate",
                "stored",
                "--target",
                "./unopened-root",
                "--graph",
                "knowledge",
            ],
            vec!["query", "--target", "server"],
            vec!["mutate", "--target", "server"],
        ] {
            let error = selected(&args, &input, dir.path()).unwrap_err();
            assert!(
                error.to_string().contains("stored query"),
                "{args:?}: {error}"
            );
        }
        assert!(!dir.path().join("unopened-root").exists());
        for args in [
            vec!["query", "stored", "--target", "server"],
            vec!["mutate", "stored", "--target", "server"],
            vec!["query", "--query", "unread.gq", "--target", "local"],
            vec!["mutate", "-e", "unparsed source", "--target", "local"],
        ] {
            assert!(selected(&args, &input, dir.path()).is_ok(), "{args:?}");
        }
    }
}
