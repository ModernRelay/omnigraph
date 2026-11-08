//! `omnigraph studio`: open OmniGraph Studio on an existing server target.
//!
//! The command validates the target, builds one `omnigraph-studio://open`
//! link, hands it to the platform and exits; it owns nothing at runtime and
//! sends no request. The link names the target, the graph `--graph` chose,
//! and the digests of the target's binding and of the operator home
//! ([`crate::target_identity`]). Studio resolves the target itself through the
//! CLI, whose `target inspect --json` reports the same digests, and refuses a
//! link that disagrees with its own resolution. A link carries no secret,
//! never names a path or an executable, and opening one runs no write.

mod args;
mod desktop;
mod open;

pub(crate) use args::StudioArgs;
pub(crate) use open::{Platform, SystemRunner};

use std::io::Write;

use color_eyre::Result;
use serde::Serialize;

use self::open::{INSTALL_URL, Opening, Runner, SCHEME};
use crate::app::Completion;
use crate::operator::InvocationConfig;
use crate::targets::{ResolvedTarget, STUDIO_PROTOCOL, TargetBinding, TargetProvenance, refusal};

/// The exit of this command's refusals, the one `finish` gives a target
/// refusal on a command without a classified outcome.
const REFUSED: i32 = 2;

/// The target the link opens, as the invocation resolved it.
pub(crate) struct StudioScope<'a> {
    pub(crate) operator: &'a InvocationConfig,
    pub(crate) target: &'a ResolvedTarget,
    pub(crate) graph: Option<&'a str>,
}

/// One validated link and the facts it was built from: `studio --json`.
#[derive(Debug, Serialize)]
struct StudioLink {
    protocol: u32,
    target: String,
    binding: String,
    home: String,
    link: String,
}

pub(crate) fn run(
    args: StudioArgs,
    scope: StudioScope<'_>,
    platform: Platform,
    runner: &mut dyn Runner,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let link = prepare(&scope)?;
    let opening = open::open(&platform, &link.link, runner);
    deliver(&link, args.json, opening, stdout, stderr)
}

fn prepare(scope: &StudioScope<'_>) -> Result<StudioLink> {
    let TargetBinding::Server { endpoint, .. } = &scope.target.binding else {
        // Applicability refused every other kind before resolution ended.
        return Err(refusal(
            "target_kind_unsupported",
            "studio opens an existing server target",
        ));
    };
    if crate::graph_http::server_scheme(endpoint) == Some("unix") {
        return Err(refusal(
            "studio_transport_unsupported",
            format!(
                "target '{}' is a server on a Unix socket, and Studio's client speaks TCP; \
                 select a target with an http(s) endpoint",
                scope.target.label()
            ),
        ));
    }
    let name = match &scope.target.provenance {
        TargetProvenance::Explicit { name } | TargetProvenance::Default { name } => name,
        TargetProvenance::Inline { .. } => {
            return Err(refusal(
                "target_invalid",
                "a Studio link names a registered target; register this one with `omnigraph target add`",
            ));
        }
    };
    if let Some(graph) = scope.graph {
        crate::helpers::require_graph_segment(graph)
            .map_err(|error| refusal("target_incompatible", error.to_string()))?;
    }
    let entry = scope
        .operator
        .get(&mut std::io::sink())?
        .schema
        .as_ref()
        .and_then(|source| source.targets.get(name))
        .ok_or_else(|| {
            refusal(
                "target_unknown",
                format!("unknown target '{name}'; register it with `omnigraph target add {name}`"),
            )
        })?;
    let binding = crate::target_identity::binding_digest(entry)?.ok_or_else(|| {
        refusal(
            "target_kind_unsupported",
            "studio opens an existing server target",
        )
    })?;
    let home = crate::target_identity::home_digest(scope.operator)?;
    let link = link(name, &binding, &home, scope.graph);
    Ok(StudioLink {
        protocol: STUDIO_PROTOCOL,
        target: name.clone(),
        binding,
        home,
        link,
    })
}

/// `omnigraph-studio://open?target=NAME&binding=DIGEST&home=DIGEST[&graph=G]`.
fn link(target: &str, binding: &str, home: &str, graph: Option<&str>) -> String {
    let mut link = format!(
        "{SCHEME}://open?target={}&binding={}&home={}",
        query_value(target),
        query_value(binding),
        query_value(home)
    );
    if let Some(graph) = graph {
        link.push_str("&graph=");
        link.push_str(&query_value(graph));
    }
    link
}

/// Target names, graph ids and digests are already query-safe; anything else
/// is percent-encoded, so no value can add a parameter.
fn query_value(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b':') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn deliver(
    link: &StudioLink,
    json: bool,
    opening: Opening,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let reason = match opening {
        Opening::Opened => {
            if json {
                crate::output::write_json(link, stdout)?;
            } else {
                writeln!(stdout, "{}", link.link)?;
                writeln!(
                    stderr,
                    "opened OmniGraph Studio on target '{}'",
                    link.target
                )?;
            }
            return Ok(Completion::success());
        }
        Opening::Unavailable(reason) => reason,
    };
    let error = format!(
        "studio_unavailable: OmniGraph Studio did not open: {reason}; install it from \
         {INSTALL_URL}, then open the link"
    );
    if json {
        crate::output::write_json(
            &serde_json::json!({
                "error": error,
                "code": "studio_unavailable",
                "link": link.link,
                "install": INSTALL_URL,
            }),
            stdout,
        )?;
    } else {
        writeln!(stdout, "{}", link.link)?;
        writeln!(stderr, "{error}")?;
    }
    stdout.flush()?;
    stderr.flush()?;
    Ok(Completion::exit(REFUSED))
}

#[cfg(test)]
mod tests {
    use super::open::tests::{Recorded, exited};
    use super::*;

    const B: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const H: &str = "sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

    #[test]
    fn a_link_names_the_target_its_digests_and_an_explicit_graph() {
        assert_eq!(
            link("staging", B, H, None),
            format!("omnigraph-studio://open?target=staging&binding={B}&home={H}")
        );
        assert_eq!(
            link("prod.eu_1", B, H, Some("knowledge")),
            format!(
                "omnigraph-studio://open?target=prod.eu_1&binding={B}&home={H}&graph=knowledge"
            )
        );
        // A value cannot add a parameter or change the link's path.
        assert_eq!(query_value("a&b=c/d e?#"), "a%26b%3Dc%2Fd%20e%3F%23");
    }

    /// An operator home holding `targets`, and a target resolved from it.
    fn resolved(
        targets: &str,
        name: &str,
    ) -> (tempfile::TempDir, InvocationConfig, ResolvedTarget) {
        let home = tempfile::tempdir().unwrap();
        std::fs::write(
            home.path().join("config.yaml"),
            format!("version: 1\ntargets:\n{targets}"),
        )
        .unwrap();
        let input = InvocationConfig::at(Some(home.path().to_owned()), None);
        let binding = input
            .get(&mut std::io::sink())
            .unwrap()
            .binding(name)
            .unwrap();
        let target = ResolvedTarget::new(binding, TargetProvenance::Explicit { name: name.into() });
        (home, input, target)
    }

    const STAGING: &str = "  staging:\n    kind: server\n    endpoint: https://graph.example.com\n    \
                           auth: {provider: token, source: {env: OG_STAGING_TOKEN}}\n    \
                           default_graph: knowledge\n";

    fn run_with(
        scope: StudioScope<'_>,
        json: bool,
        platform: Platform,
        runner: &mut Recorded,
    ) -> (Result<Completion>, String, String) {
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let result = run(
            StudioArgs { json },
            scope,
            platform,
            runner,
            &mut stdout,
            &mut stderr,
        );
        (
            result,
            String::from_utf8(stdout).unwrap(),
            String::from_utf8(stderr).unwrap(),
        )
    }

    #[test]
    fn studio_opens_one_link_whose_digests_come_from_the_resolution() {
        let (_home, input, target) = resolved(STAGING, "staging");
        let entry = &input
            .get(&mut std::io::sink())
            .unwrap()
            .schema
            .as_ref()
            .unwrap()
            .targets["staging"];
        let binding = crate::target_identity::binding_digest(entry)
            .unwrap()
            .unwrap();
        let home = crate::target_identity::home_digest(&input).unwrap();
        let expected = format!(
            "omnigraph-studio://open?target=staging&binding={binding}&home={home}&graph=notes"
        );
        let mut runner = Recorded::answering(vec![("open", exited(true, ""))]);
        let scope = StudioScope {
            operator: &input,
            target: &target,
            graph: Some("notes"),
        };
        let (result, stdout, _) = run_with(scope, true, Platform::MacOs, &mut runner);
        assert_eq!(result.unwrap(), Completion::success());
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&stdout).unwrap(),
            serde_json::json!({
                "protocol": 1,
                "target": "staging",
                "binding": binding,
                "home": home,
                "link": expected,
            })
        );
        // The opener receives the link and nothing else this command chose.
        assert_eq!(
            runner.calls,
            [vec![
                "open".to_owned(),
                "-b".into(),
                "com.modernrelay.omnigraph-studio".into(),
                expected.clone()
            ]]
        );

        let mut runner = Recorded::answering(vec![("open", exited(true, ""))]);
        let scope = StudioScope {
            operator: &input,
            target: &target,
            graph: None,
        };
        let (result, stdout, stderr) = run_with(scope, false, Platform::MacOs, &mut runner);
        assert_eq!(result.unwrap(), Completion::success());
        assert_eq!(
            stdout,
            format!("omnigraph-studio://open?target=staging&binding={binding}&home={home}\n")
        );
        assert!(stderr.contains("opened OmniGraph Studio"), "{stderr}");
    }

    #[test]
    fn without_a_handler_it_prints_the_link_and_refuses_studio_unavailable() {
        let (_home, input, target) = resolved(STAGING, "staging");
        for json in [true, false] {
            let mut runner = Recorded::answering(vec![("xdg-mime", exited(true, ""))]);
            let scope = StudioScope {
                operator: &input,
                target: &target,
                graph: None,
            };
            let (result, stdout, stderr) = run_with(
                scope,
                json,
                Platform::Freedesktop {
                    data_dirs: Vec::new(),
                },
                &mut runner,
            );
            assert_eq!(result.unwrap(), Completion::exit(REFUSED));
            // Only the handler was asked; nothing was opened.
            assert_eq!(runner.calls.len(), 1);
            assert_eq!(runner.calls[0][0], "xdg-mime");
            if json {
                let body: serde_json::Value = serde_json::from_str(&stdout).unwrap();
                assert_eq!(body["code"], "studio_unavailable");
                assert_eq!(body["install"], INSTALL_URL);
                assert!(
                    body["link"]
                        .as_str()
                        .unwrap()
                        .starts_with("omnigraph-studio://open?target=staging&binding=sha256:")
                );
                assert!(stderr.is_empty(), "{stderr}");
            } else {
                assert!(stdout.starts_with("omnigraph-studio://open?target=staging&"));
                assert!(stderr.starts_with("studio_unavailable: "), "{stderr}");
                assert!(stderr.contains(INSTALL_URL), "{stderr}");
            }
        }
    }

    #[test]
    fn a_target_studio_cannot_open_refuses_before_the_platform_is_asked() {
        let (_home, input, server) = resolved(STAGING, "staging");
        let unix = ResolvedTarget::new(
            TargetBinding::Server {
                endpoint: "unix:///run/omnigraph.sock".into(),
                auth: crate::targets::ServerAuth::Anonymous,
                default_graph: None,
            },
            TargetProvenance::Explicit {
                name: "staging".into(),
            },
        );
        let inline = ResolvedTarget::new(
            server.binding.clone(),
            TargetProvenance::Inline {
                address: "./graph.omni".into(),
            },
        );
        let direct = ResolvedTarget::new(
            TargetBinding::DirectGraph {
                uri: "file:///srv/g.omni".into(),
                provider: crate::targets::StorageProvider::Local,
            },
            TargetProvenance::Explicit { name: "g".into() },
        );
        for (target, graph, code) in [
            (&unix, None, "studio_transport_unsupported"),
            (&inline, None, "target_invalid"),
            (&direct, None, "target_kind_unsupported"),
            (&server, Some("../etc"), "target_incompatible"),
        ] {
            let mut runner = Recorded::default();
            let scope = StudioScope {
                operator: &input,
                target,
                graph,
            };
            let (result, stdout, _) = run_with(scope, true, Platform::MacOs, &mut runner);
            let error = result.unwrap_err();
            assert_eq!(
                error
                    .downcast_ref::<crate::targets::TargetError>()
                    .unwrap()
                    .code,
                code
            );
            assert!(stdout.is_empty());
            assert!(runner.calls.is_empty(), "{code}: {:?}", runner.calls);
        }
    }

    /// The version-1 operator document cannot name a Unix-socket server, so
    /// no configured target reaches the transport refusal.
    #[test]
    fn a_unix_socket_server_target_is_not_configurable() {
        let error = crate::operator_schema::parse(
            "version: 1\ntargets:\n  s: {kind: server, endpoint: 'unix:///run/omnigraph.sock', auth: {provider: anonymous}}\n",
            std::path::Path::new("/srv"),
        )
        .unwrap_err();
        assert!(error.to_string().contains("targets.s.endpoint"), "{error}");
    }
}
