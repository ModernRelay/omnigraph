//! The two digests that let another program check a target it was handed by
//! name: `home`, the operator file a resolution read, and `binding`, what a
//! server target resolves to. Neither names a path, an executable or a
//! secret. `omnigraph studio` puts both in its link, and `target inspect
//! --json` reports both, so a client that resolves the target itself through
//! the CLI compares values computed here and nowhere else.

use std::path::{Path, PathBuf};

use color_eyre::{Result, eyre::bail};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::operator::InvocationConfig;
use crate::operator_schema::{AuthProfile, CaMode, Proxy, Target, TokenSource, Transport};

/// Each preimage starts with its own label, so neither digest equals another
/// hash of the same bytes, such as the credential-custody key of the same file.
const HOME_DOMAIN: &[u8] = b"omnigraph operator home v1\0";
const BINDING_DOMAIN: &[u8] = b"omnigraph server target binding v1\0";

/// The digest of the operator file this invocation reads, at the canonical
/// location credential custody is keyed by: `--operator-config FILE`, else
/// `config.yaml` under `OMNIGRAPH_HOME` or `~/.omnigraph`, with directory
/// aliases resolved and the file name kept.
pub(crate) fn home_digest(input: &InvocationConfig) -> Result<String> {
    let path = input.canonical_config_path()?;
    Ok(digest(HOME_DOMAIN, path.as_os_str().as_encoded_bytes()))
}

/// The digest of a server target's non-secret identity: its endpoint, its
/// authentication provider and that provider's source, and its transport. A
/// token's value never enters it, and neither does `default_graph`, which
/// selects within the destination. Equivalent spellings of one endpoint or
/// path digest alike. `None` for a target that is not a server.
pub(crate) fn binding_digest(target: &Target) -> Result<Option<String>> {
    let Target::Server {
        endpoint,
        auth,
        transport,
        ..
    } = target
    else {
        return Ok(None);
    };
    let identity = ServerIdentity {
        kind: "server",
        endpoint: canonical_endpoint(endpoint)?,
        auth: AuthIdentity::of(auth)?,
        transport: TransportIdentity::of(transport.as_ref()),
    };
    Ok(Some(digest(
        BINDING_DOMAIN,
        &serde_json::to_vec(&identity)?,
    )))
}

fn digest(domain: &[u8], bytes: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(bytes);
    let hex: String = hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("sha256:{hex}")
}

/// The URL's serialized form, which lowercases the host and drops a default
/// port, with the trailing slash validation adds to every endpoint.
fn canonical_endpoint(endpoint: &str) -> Result<String> {
    let mut url = url::Url::parse(endpoint)?;
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    Ok(url.into())
}

/// The path with `.` components and repeated separators removed. `..` stays,
/// since resolving it lexically can name another file through a symlink, and
/// symlinks are not followed, so a rotated token or CA file keeps its path.
fn lexical(path: &Path) -> String {
    path.components()
        .collect::<PathBuf>()
        .to_string_lossy()
        .into_owned()
}

#[derive(Serialize)]
struct ServerIdentity<'a> {
    kind: &'static str,
    endpoint: String,
    auth: AuthIdentity<'a>,
    transport: TransportIdentity,
}

#[derive(Serialize)]
#[serde(tag = "provider", rename_all = "snake_case")]
enum AuthIdentity<'a> {
    Anonymous,
    Omnigraph,
    Token {
        source: SourceIdentity<'a>,
    },
    /// The helper's argv, which says where the token comes from; its timeout
    /// does not.
    Exec {
        command: &'a [String],
    },
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum SourceIdentity<'a> {
    Store,
    Env(&'a str),
    File(String),
}

impl<'a> AuthIdentity<'a> {
    fn of(auth: &'a AuthProfile) -> Result<Self> {
        Ok(match auth {
            AuthProfile::Anonymous => Self::Anonymous,
            AuthProfile::Omnigraph => Self::Omnigraph,
            AuthProfile::Token { source } => Self::Token {
                source: match source {
                    TokenSource::Store { .. } => SourceIdentity::Store,
                    TokenSource::Env { env } => SourceIdentity::Env(env),
                    TokenSource::File { file } => SourceIdentity::File(lexical(file)),
                },
            },
            AuthProfile::Exec { command, .. } => Self::Exec { command },
            AuthProfile::OauthClientCredentials { .. } => {
                bail!("OAuth client credentials require a managed profile")
            }
        })
    }
}

/// An absent `transport` and one that spells out its defaults are the same
/// transport.
#[derive(Serialize)]
struct TransportIdentity {
    ca: Option<CaIdentity>,
    proxy: ProxyIdentity,
}

#[derive(Serialize)]
struct CaIdentity {
    mode: CaMode,
    file: String,
}

#[derive(Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
enum ProxyIdentity {
    Direct,
    Url { url: String },
    Environment,
}

impl TransportIdentity {
    fn of(transport: Option<&Transport>) -> Self {
        let Some(transport) = transport else {
            return Self {
                ca: None,
                proxy: ProxyIdentity::Direct,
            };
        };
        Self {
            ca: transport
                .tls
                .as_ref()
                .and_then(|tls| tls.ca.as_ref())
                .map(|ca| CaIdentity {
                    mode: ca.mode,
                    file: lexical(&ca.file),
                }),
            proxy: match &transport.proxy {
                Proxy::Direct => ProxyIdentity::Direct,
                Proxy::Url { url } => ProxyIdentity::Url { url: url.clone() },
                Proxy::Environment => ProxyIdentity::Environment,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    /// The binding digest of target `t` in a `version: 1` document whose
    /// relative paths resolve beside `base`.
    fn binding_at(base: &Path, target: &str) -> Option<String> {
        let document = format!("version: 1\ntargets:\n  t: {target}\n");
        let config = crate::operator_schema::parse(&document, base).unwrap();
        binding_digest(&config.targets["t"]).unwrap()
    }

    fn binding(target: &str) -> Option<String> {
        binding_at(Path::new("/srv/operator"), target)
    }

    fn server(endpoint: &str, rest: &str) -> String {
        format!("{{kind: server, endpoint: '{endpoint}', {rest}}}")
    }

    const STORE: &str = "auth: {provider: token, source: {store: {}}}";

    #[test]
    fn a_binding_is_a_sha256_digest_of_server_targets_only() {
        let digest = binding(&server("https://graph.example.com", STORE)).unwrap();
        let hex = digest.strip_prefix("sha256:").unwrap();
        assert_eq!(hex.len(), 64);
        assert!(
            hex.bytes()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert_eq!(
            binding("{kind: direct, scope: graph, uri: 'file:///srv/graphs/g.omni'}"),
            None
        );
        // Validation normally rejects this before target inspection. A direct
        // caller must not manufacture a standalone binding for managed auth.
        let invalid: Target = yaml_serde::from_str(&server(
            "https://graph.example.com",
            "auth: {provider: oauth_client_credentials, token_endpoint: 'https://auth.example/token', client_id: private-client, client_secret: {env: PRIVATE_SECRET_SOURCE}}",
        ))
        .unwrap();
        assert_eq!(
            binding_digest(&invalid).unwrap_err().to_string(),
            "OAuth client credentials require a managed profile"
        );
    }

    #[test]
    fn equivalent_spellings_of_one_binding_digest_alike() {
        let base = tempfile::tempdir().unwrap();
        for (left, right) in [
            (
                server("https://Graph.Example.COM:443/api", STORE),
                server("https://graph.example.com/api/", STORE),
            ),
            (
                server("https://graph.example.com", STORE),
                server("https://graph.example.com/", STORE),
            ),
            // The graph a target selects by default is not its destination.
            (
                server("https://graph.example.com", STORE),
                server(
                    "https://graph.example.com",
                    &format!("{STORE}, default_graph: knowledge"),
                ),
            ),
            // An absent transport is the default one spelled out.
            (
                server("https://graph.example.com", STORE),
                server(
                    "https://graph.example.com",
                    &format!("{STORE}, transport: {{proxy: {{mode: direct}}}}"),
                ),
            ),
            // A relative path resolves beside the operator file, with `.`
            // and repeated separators removed.
            (
                server(
                    "https://graph.example.com",
                    "auth: {provider: token, source: {file: './secrets//token'}}",
                ),
                server(
                    "https://graph.example.com",
                    &format!(
                        "auth: {{provider: token, source: {{file: '{}'}}}}",
                        base.path().join("secrets/token").display()
                    ),
                ),
            ),
            (
                server(
                    "https://graph.example.com",
                    &format!("{STORE}, transport: {{tls: {{ca: {{mode: append, file: ca.pem}}}}}}"),
                ),
                server(
                    "https://graph.example.com",
                    &format!(
                        "{STORE}, transport: {{tls: {{ca: {{mode: append, file: '{}'}}}}}}",
                        base.path().join("./ca.pem").display()
                    ),
                ),
            ),
            // A helper's timeout is not where its token comes from.
            (
                server(
                    "https://graph.example.com",
                    "auth: {provider: exec, command: [/usr/local/bin/helper, get], timeout: 5s}",
                ),
                server(
                    "https://graph.example.com",
                    "auth: {provider: exec, command: [/usr/local/bin/helper, get]}",
                ),
            ),
        ] {
            assert_eq!(
                binding_at(base.path(), &left),
                binding_at(base.path(), &right),
                "{left} / {right}"
            );
        }
    }

    #[test]
    fn a_tokens_value_never_changes_the_binding() {
        let base = tempfile::tempdir().unwrap();
        let token = base.path().join("token");
        let target = server(
            "https://graph.example.com",
            "auth: {provider: token, source: {file: token}}",
        );
        std::fs::write(&token, "first-token\n").unwrap();
        let first = binding_at(base.path(), &target);
        std::fs::write(&token, "rotated-token\n").unwrap();
        assert_eq!(binding_at(base.path(), &target), first);
        std::fs::remove_file(&token).unwrap();
        assert_eq!(binding_at(base.path(), &target), first);
    }

    #[test]
    fn a_different_destination_credential_source_or_transport_changes_the_binding() {
        let variants = [
            server("https://graph.example.com", STORE),
            server("https://other.example.com", STORE),
            server("https://graph.example.com/a", STORE),
            server("https://graph.example.com:8443", STORE),
            server("http://127.0.0.1:8080", "auth: {provider: anonymous}"),
            server("https://graph.example.com", "auth: {provider: anonymous}"),
            server(
                "https://graph.example.com",
                "auth: {provider: token, source: {env: OG_TOKEN}}",
            ),
            server(
                "https://graph.example.com",
                "auth: {provider: token, source: {env: OG_OTHER_TOKEN}}",
            ),
            server(
                "https://graph.example.com",
                "auth: {provider: token, source: {file: /run/token}}",
            ),
            server(
                "https://graph.example.com",
                "auth: {provider: exec, command: [/usr/local/bin/helper, get]}",
            ),
            server(
                "https://graph.example.com",
                &format!("{STORE}, transport: {{tls: {{ca: {{mode: append, file: ca.pem}}}}}}"),
            ),
            server(
                "https://graph.example.com",
                &format!("{STORE}, transport: {{tls: {{ca: {{mode: replace, file: ca.pem}}}}}}"),
            ),
            server(
                "https://graph.example.com",
                &format!(
                    "{STORE}, transport: {{proxy: {{mode: url, url: 'http://proxy.example.com:8080'}}}}"
                ),
            ),
            server(
                "https://graph.example.com",
                &format!("{STORE}, transport: {{proxy: {{mode: environment}}}}"),
            ),
        ];
        let digests: std::collections::BTreeSet<_> = variants
            .iter()
            .map(|target| binding(target).unwrap())
            .collect();
        assert_eq!(digests.len(), variants.len(), "{digests:?}");
    }

    #[test]
    fn the_home_is_the_canonical_operator_file() {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        let other = root.path().join("other");
        for dir in [&home, &other] {
            std::fs::create_dir(dir).unwrap();
            std::fs::write(dir.join("config.yaml"), "version: 1\n").unwrap();
        }
        let digest_of = |input: InvocationConfig| home_digest(&input).unwrap();
        let at = |dir: &Path| InvocationConfig::at(Some(dir.to_owned()), None);
        let selected = digest_of(at(&home));
        assert!(selected.starts_with("sha256:"));
        // The same file, named explicitly or through a directory alias.
        assert_eq!(
            digest_of(at(&other).with_path(Some("home/config.yaml"), root.path())),
            selected
        );
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&home, root.path().join("alias")).unwrap();
            assert_eq!(digest_of(at(&root.path().join("alias"))), selected);
        }
        // Another home, or another file in the same one, is another home.
        assert_ne!(digest_of(at(&other)), selected);
        assert_ne!(
            digest_of(at(&home).with_path(Some("home/second.yaml"), root.path())),
            selected
        );
        // Not the custody key of the same file.
        let custody = at(&home).auth_state_path().unwrap();
        assert!(!selected.contains(custody.file_stem().unwrap().to_str().unwrap()));
    }
}
