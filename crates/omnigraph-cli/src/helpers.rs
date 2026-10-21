//! Shared actor resolution, request construction, diagnostics and local scaffolding.
//! Backend selection belongs to the typed target resolver.

use omnigraph_api_types::{ChangeRequest, QueryRequest, query_file_refusals};
use omnigraph_compiler::query::ast::QueryFile;

use super::*;
use crate::operator::{InvocationConfig, OperatorConfig};

pub(crate) fn ensure_local_graph_parent(uri: &str) -> Result<()> {
    if !uri.contains("://") {
        fs::create_dir_all(uri)?;
    }
    Ok(())
}

/// Whether a resolved write target is *local* for the purposes of the RFC-011
/// Decision 9 destructive-confirm gate: a bare path or a `file://` URI. Anything
/// else carrying a scheme — `http(s)://` or `unix://` (served), `s3://` /
/// `gs://` / … (object store) — is non-local and a destructive write against it
/// requires explicit consent.
pub(crate) fn uri_is_local(uri: &str) -> bool {
    !uri.contains("://") || uri.starts_with("file://")
}

/// Gate a destructive write (`cleanup`, overwrite `load`, `branch delete`)
/// against a non-local scope (RFC-011 Decision 9). A local target needs no
/// confirmation; otherwise `--yes` consents, an interactive TTY is prompted, and
/// a non-TTY / `--json` run refuses rather than silently proceeding.
pub(crate) fn confirm_destructive(
    label: &str,
    uri: &str,
    yes: bool,
    json: bool,
    input: &mut crate::input::Input<'_>,
    stderr: &mut dyn Write,
) -> Result<()> {
    if uri_is_local(uri) || yes {
        return Ok(());
    }
    if json || !input.is_terminal {
        bail!(
            "refusing destructive `{label}` against non-local target {uri} without confirmation; \
             pass --yes to confirm (an interactive TTY would be prompted instead)"
        );
    }
    write!(
        stderr,
        "About to run a destructive `{label}` against {uri} (not local). Type 'yes' to continue: "
    )?;
    stderr.flush()?;
    let answer = crate::input::read_line(input.reader)?;
    match answer.trim().to_ascii_lowercase().as_str() {
        "yes" | "y" => Ok(()),
        _ => bail!("aborted: destructive `{label}` not confirmed"),
    }
}

/// THE one way the CLI composes a remote request URL. Every remote call
/// routes through here so URL assembly has a single mechanism instead of
/// per-callsite string interpolation.
///
/// - `base` is the resolved server root (single-graph) or `…/graphs/{id}`
///   (multi-graph).
/// - `segments` are appended as individual percent-encoded path segments, so
///   a dynamic component (branch name, commit id, query name) is always one
///   safe segment — e.g. a branch `etl/zendesk/run-1` becomes `%2F`-escaped.
/// - `query` pairs are percent-encoded values.
///
/// Trailing-slash normalization happens exactly once via `pop_if_empty`:
/// `Url::parse` normalizes a path-less base (`http://host`) to a single empty
/// trailing segment, and a `…/graphs/{id}/` base keeps its own. `extend`
/// appends *after* the last segment, so without dropping a trailing empty one
/// the join emits `…/graphs/{id}//branches/{name}` — the empty `//` segment
/// misses the route and 404s. Because callers pass structured segments rather
/// than a pre-joined string, neither a stray `//` nor an un-encoded dynamic
/// component is representable here.
pub(crate) fn remote_url(base: &str, segments: &[&str], query: &[(&str, &str)]) -> Result<String> {
    let mut url = reqwest::Url::parse(base.trim_end_matches('/'))?;
    url.path_segments_mut()
        .map_err(|_| color_eyre::eyre::eyre!("invalid remote base url"))?
        .pop_if_empty()
        .extend(segments);
    if !query.is_empty() {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in query {
            pairs.append_pair(key, value);
        }
    }
    Ok(url.to_string())
}

#[cfg(test)]
pub(crate) fn normalize_bearer_token(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// The Cedar resource id for a graph selection: the explicit graph name when one
/// is given, else the normalized URI (the anonymous fallback). Used by the
/// `policy` tooling to address a graph's bundle.
pub(crate) fn graph_resource_id_for_selection(
    selected_graph: Option<&str>,
    normalized_uri: &str,
) -> String {
    selected_graph.unwrap_or(normalized_uri).to_string()
}

/// THE actor chain (RFC-011) — every command that needs an identity
/// resolves through this one function (one path per concern):
/// `--as` > `operator.actor` in ~/.omnigraph/config.yaml > none.
pub(crate) fn resolve_actor(
    input: &InvocationConfig,
    cli_as: Option<&str>,
    stderr: &mut dyn Write,
) -> Result<Option<String>> {
    if let Some(actor) = cli_as {
        return Ok(Some(actor.to_string()));
    }
    Ok(resolve_actor_with_config(input.get(stderr)?, cli_as))
}

pub(crate) fn resolve_actor_with_config(
    operator_config: &OperatorConfig,
    cli_as: Option<&str>,
) -> Option<String> {
    cli_as
        .or_else(|| operator_config.actor())
        .map(str::to_string)
}

/// Why a served request carries no credential. A 401 to that request names
/// it, so the refusal says how to bind one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MissingCredential {
    /// A declared target deliberately selected anonymous authentication.
    AnonymousTarget(String),
    /// The URL falls under no declared server target. `server_root` is the address
    /// as written, without URL userinfo; `config` is the operator config path.
    Unbound {
        server_root: String,
        config: Option<std::path::PathBuf>,
    },
}

/// An anonymous target or unbound address, with actionable configuration guidance.
/// Missing tokens for credential-bound targets refuse before any HTTP request.
impl Serialize for MissingCredential {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let fields = if matches!(self, Self::AnonymousTarget(_)) {
            3
        } else {
            2
        };
        let mut state = serializer.serialize_struct("MissingCredential", fields)?;
        match self {
            Self::AnonymousTarget(name) => {
                state.serialize_field("target", name)?;
                state.serialize_field("auth", "anonymous")?;
            }
            Self::Unbound { server_root, .. } => state.serialize_field("unbound", server_root)?,
        }
        state.serialize_field("hint", &self.to_string())?;
        state.end()
    }
}

impl std::fmt::Display for MissingCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AnonymousTarget(name) => write!(
                f,
                "target `{name}` uses anonymous authentication: configure and select a token or exec \
                 auth profile; for a token profile with source: store, run \
                 `omnigraph credentials set --target {name} --auth <profile> --stdin`"
            ),
            Self::Unbound {
                server_root,
                config,
            } => write!(
                f,
                "no credential is bound to {server_root}: declare a server target and explicit auth \
                 profile under `targets:` in {}; select it with --target and --auth",
                config.as_deref().map_or_else(
                    || "~/.omnigraph/config.yaml".to_string(),
                    |path| path.display().to_string()
                )
            ),
        }
    }
}

/// The reason a 401 response's request carried no credential.
pub(crate) fn missing_credential(response: &reqwest::Response) -> Option<&MissingCredential> {
    response.extensions().get::<MissingCredential>()
}

/// A refusal message followed by the reason its request carried no
/// credential, when it had one.
pub(crate) fn with_missing_credential(
    message: String,
    missing: Option<&MissingCredential>,
) -> String {
    match missing {
        Some(missing) => format!("{message}\n{missing}"),
        None => message,
    }
}

/// A graph identifier is one unambiguous URL/storage path segment.
pub(crate) fn require_graph_segment(graph: &str) -> Result<()> {
    if graph != "og_catalog" {
        omnigraph_server::GraphId::try_from(graph).map_err(|error| {
            color_eyre::eyre::eyre!(
                "invalid graph `{graph}`: a graph name is one path segment; {error}"
            )
        })?;
    }
    Ok(())
}

pub(crate) fn valid_managed_identifier(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

pub(crate) fn apply_bearer_token(
    request: reqwest::RequestBuilder,
    token: Option<&str>,
) -> reqwest::RequestBuilder {
    if let Some(token) = token {
        request.header(AUTHORIZATION, format!("Bearer {}", token))
    } else {
        request
    }
}

/// A verified request precondition refusal. The command boundary decides
/// whether earlier effects prevent promoting it to whole-command exit 4.
#[derive(Debug)]
pub(crate) struct PreconditionFailedCli {
    pub(crate) output: ErrorOutput,
    pub(crate) http_status: Option<u16>,
    pub(crate) retry_after: Option<String>,
}

impl std::fmt::Display for PreconditionFailedCli {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.output.error)
    }
}

impl std::error::Error for PreconditionFailedCli {}

/// Preserve a typed server refusal through the command dispatch so JSON
/// callers retain its code and detail fields instead of parsing a message.
/// `output` is the server's body as sent; `missing_credential` is the CLI's
/// own account of a request that carried no credential.
#[derive(Debug)]
pub(crate) struct RemoteErrorCli {
    pub(crate) output: ErrorOutput,
    pub(crate) status: reqwest::StatusCode,
    pub(crate) retry_after: Option<String>,
    pub(crate) missing_credential: Option<MissingCredential>,
}

impl std::fmt::Display for RemoteErrorCli {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&with_missing_credential(
            self.output.error.clone(),
            self.missing_credential.as_ref(),
        ))
    }
}

/// A served refusal as `--json` prints it: the server's body unchanged,
/// beside the CLI's account of a request that carried no credential.
#[derive(Serialize)]
pub(crate) struct RemoteRefusalOutput<'a> {
    #[serde(flatten)]
    pub(crate) output: &'a ErrorOutput,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) missing_credential: Option<&'a MissingCredential>,
}

impl std::error::Error for RemoteErrorCli {}

/// Build the typed CAS-lost error for the embedded transport, mirroring the
/// structured body a server would have returned so `--json` output is
/// transport-uniform. `message` is the engine error's own `Display` text, so
/// the wording never drifts from the source variant.
pub(crate) fn precondition_failed_cli(
    message: String,
    expected: String,
    actual: Option<String>,
) -> PreconditionFailedCli {
    let mut output = ErrorOutput::message(message);
    output.precondition_failure =
        Some(omnigraph_api_types::PreconditionFailureOutput { expected, actual });
    PreconditionFailedCli {
        output,
        http_status: None,
        retry_after: None,
    }
}

pub(crate) async fn remote_json<T: DeserializeOwned>(
    client: &crate::graph_http::GraphHttpClient,
    method: Method,
    url: String,
    body: Option<Value>,
    bearer_token: Option<&str>,
) -> Result<T> {
    remote_json_with_graph_commit_precondition(client, method, url, body, bearer_token, None).await
}

/// [`remote_json`] with an optional `Omnigraph-If-Graph-Commit` graph-head
/// precondition (mutation routes only). A 412 whose body carries
/// `precondition_failure` surfaces as the typed [`PreconditionFailedCli`].
pub(crate) async fn remote_json_with_graph_commit_precondition<T: DeserializeOwned>(
    client: &crate::graph_http::GraphHttpClient,
    method: Method,
    url: String,
    body: Option<Value>,
    bearer_token: Option<&str>,
    expected_commit: Option<&str>,
) -> Result<T> {
    remote_json_bounded(
        client,
        method,
        url,
        body,
        bearer_token,
        expected_commit,
        None,
    )
    .await
}

/// Same typed graph protocol with an optional response limit for managed data access.
pub(crate) async fn remote_json_bounded<T: DeserializeOwned>(
    client: &crate::graph_http::GraphHttpClient,
    method: Method,
    url: String,
    body: Option<Value>,
    bearer_token: Option<&str>,
    expected_commit: Option<&str>,
    response_limit: Option<usize>,
) -> Result<T> {
    let request = apply_bearer_token(client.request(method, url)?, bearer_token);
    let request = if let Some(commit_id) = expected_commit {
        request.header(
            omnigraph_api_types::GRAPH_COMMIT_PRECONDITION_HEADER,
            commit_id,
        )
    } else {
        request
    };
    let request = if let Some(body) = body {
        request.json(&body)
    } else {
        request
    };
    remote_response_json_with_precondition(
        client.send(request).await?,
        bearer_token,
        response_limit,
        expected_commit,
    )
    .await
}

/// Decode either JSON requests or raw NDJSON loads through the same bounded,
/// credential-safe response path. The request owner chooses its deadline.
pub(crate) async fn remote_response_json_bounded<T: DeserializeOwned>(
    response: reqwest::Response,
    bearer_token: Option<&str>,
    response_limit: Option<usize>,
) -> Result<T> {
    remote_response_json_with_precondition(response, bearer_token, response_limit, None).await
}

async fn remote_response_json_with_precondition<T: DeserializeOwned>(
    mut response: reqwest::Response,
    bearer_token: Option<&str>,
    response_limit: Option<usize>,
    expected_commit: Option<&str>,
) -> Result<T> {
    let status = response.status();
    let retry_after = crate::command_outcome::retry_after(response.headers())
        .map(|value| crate::command_outcome::scrub_backoff(value, bearer_token));
    let missing = missing_credential(&response).cloned();
    let text = if let Some(limit) = response_limit {
        if status.is_redirection() {
            bail!("managed data redirects are not followed");
        }
        if response.content_length().is_some_and(|n| n > limit as u64) {
            bail!("managed data response exceeds 8 MiB");
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if chunk.len() > limit.saturating_sub(bytes.len()) {
                bail!("managed data response exceeds 8 MiB");
            }
            bytes.extend_from_slice(&chunk);
        }
        String::from_utf8(bytes)?
    } else {
        response.text().await?
    };
    if !status.is_success() {
        let text = match (response_limit, bearer_token) {
            (Some(_), Some(token)) if !token.is_empty() => {
                crate::managed::scrub_response_error(text, token)
            }
            _ => text,
        };
        return Err(remote_refusal(
            status,
            text,
            retry_after,
            expected_commit,
            missing.as_ref(),
        ));
    }
    if response_limit.is_some() {
        // Serde errors can include rejected field values, including a reflected secret.
        serde_json::from_str(&text)
            .map_err(|_| color_eyre::eyre::eyre!("invalid managed data response"))
    } else {
        Ok(serde_json::from_str(&text)?)
    }
}

/// A non-success response as the typed refusal every remote call raises: a
/// verified 412 as [`PreconditionFailedCli`], any other `ErrorOutput` body as
/// [`RemoteErrorCli`], a body that is not one as the status and text. A
/// request that carried no credential keeps the reason: beside the typed
/// body, or after the plain text.
fn remote_refusal(
    status: reqwest::StatusCode,
    text: String,
    retry_after: Option<String>,
    expected_commit: Option<&str>,
    missing: Option<&MissingCredential>,
) -> color_eyre::eyre::Report {
    if let Ok(error) = serde_json::from_str::<ErrorOutput>(&text) {
        let verified_precondition = status == reqwest::StatusCode::PRECONDITION_FAILED
            && error
                .precondition_failure
                .as_ref()
                .is_some_and(|details| Some(details.expected.as_str()) == expected_commit)
            && matches!(
                error.code,
                None | Some(omnigraph_api_types::ErrorCode::PreconditionFailed)
            )
            && crate::command_outcome::single_request()
            && {
                let mut rest = error.clone();
                rest.precondition_failure = None;
                crate::command_outcome::is_plain_refusal(&rest)
            };
        if verified_precondition {
            return PreconditionFailedCli {
                output: error,
                http_status: Some(status.as_u16()),
                retry_after,
            }
            .into();
        }
        return RemoteErrorCli {
            output: error,
            status,
            retry_after,
            missing_credential: missing.cloned(),
        }
        .into();
    }
    color_eyre::eyre::eyre!(
        "{}",
        with_missing_credential(format!("server returned {status}: {text}"), missing)
    )
}

/// [`remote_json`] for a route whose success carries no body
/// (`DELETE /commits/{id}` answers 204): a refusal is raised as there, a
/// success decodes nothing.
pub(crate) async fn remote_no_content(
    client: &crate::graph_http::GraphHttpClient,
    method: Method,
    url: String,
    bearer_token: Option<&str>,
) -> Result<()> {
    let request = apply_bearer_token(client.request(method, url)?, bearer_token);
    let response = client.send(request).await?;
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }
    let retry_after = crate::command_outcome::retry_after(response.headers())
        .map(|value| crate::command_outcome::scrub_backoff(value, bearer_token));
    let missing = missing_credential(&response).cloned();
    let text = response.text().await?;
    Err(remote_refusal(
        status,
        text,
        retry_after,
        None,
        missing.as_ref(),
    ))
}

pub(crate) fn parse_duration_arg(s: &str) -> Result<std::time::Duration> {
    let s = s.trim();
    if s.is_empty() {
        bail!("duration is empty");
    }
    let (num_part, unit) = match s
        .char_indices()
        .rev()
        .find(|(_, c)| c.is_ascii_alphabetic())
    {
        Some((i, _)) => (
            &s[..i + 1 - s[i..].chars().next().unwrap().len_utf8()],
            &s[i..],
        ),
        None => (s, ""),
    };
    let n: u64 = num_part
        .parse()
        .map_err(|e| color_eyre::eyre::eyre!("invalid duration '{}': {}", s, e))?;
    let secs = match unit {
        "" | "s" => n,
        "m" => n * 60,
        "h" => n * 60 * 60,
        "d" => n * 60 * 60 * 24,
        "w" => n * 60 * 60 * 24 * 7,
        _ => bail!("unknown duration unit '{}'. Supported: s, m, h, d, w", unit),
    };
    Ok(std::time::Duration::from_secs(secs))
}

pub(crate) fn resolve_branch(
    cli_branch: Option<String>,
    alias_branch: Option<String>,
    default_branch: &str,
) -> String {
    cli_branch
        .or(alias_branch)
        .unwrap_or_else(|| default_branch.to_string())
}

pub(crate) fn resolve_query_path(
    explicit_query: Option<&PathBuf>,
    alias_query: Option<&str>,
) -> Result<PathBuf> {
    // The `.gq` path is resolved plainly (cwd-relative) — no omnigraph.yaml
    // `query.roots` search.
    explicit_query
        .map(PathBuf::from)
        .or_else(|| alias_query.map(PathBuf::from))
        .ok_or_else(|| {
            color_eyre::eyre::eyre!(
                "exactly one of --query, --query-string, or --alias must be provided"
            )
        })
}

/// Pick the query the caller named out of an already-parsed source. An
/// `explain` statement offers its one declaration (the engine answers the
/// plan); the `Branch` arm stays as the guard: a branch statement never
/// reaches a declaration path.
pub(crate) fn select_named_query(
    query_file: QueryFile,
    requested_name: Option<&str>,
) -> Result<(String, Vec<omnigraph_compiler::query::ast::Param>)> {
    let queries = match query_file.body.into_read_declarations() {
        Ok(queries) => queries,
        Err(message) => bail!("{message}"),
    };
    let query = if let Some(name) = requested_name {
        queries
            .into_iter()
            .find(|query| query.name == name)
            .ok_or_else(|| color_eyre::eyre::eyre!("query '{}' not found", name))?
    } else if queries.len() == 1 {
        queries.into_iter().next().unwrap()
    } else if queries.is_empty() {
        bail!(query_file_refusals::NO_QUERY);
    } else {
        bail!("query file contains multiple queries; pass --name");
    };

    Ok((query.name, query.params))
}

pub(crate) fn query_params_from_json(
    query_params: &[omnigraph_compiler::query::ast::Param],
    params_json: Option<&Value>,
) -> Result<ParamMap> {
    json_params_to_param_map(params_json, query_params, JsonParamMode::Standard)
        .map_err(|err| color_eyre::eyre::eyre!(err.to_string()))
}

/// The `POST /query` body for `branch list` or `show`: the source alone; the
/// caller fills `settings` from its `--set` values.
pub(crate) fn branch_statement_query_request(query_source: &str) -> QueryRequest {
    QueryRequest {
        query: query_source.to_string(),
        name: None,
        params: None,
        branch: None,
        snapshot: None,
        settings: None,
    }
}

/// The `POST /mutate` body for a control write: the source alone; the caller
/// fills `settings` from its `--set` values.
pub(crate) fn branch_statement_change_request(query_source: &str) -> ChangeRequest {
    ChangeRequest {
        query: query_source.to_string(),
        name: None,
        params: None,
        branch: None,
        settings: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_compiler::query::parser::parse_query;

    #[test]
    fn graph_resource_id_for_selection_uses_name_or_anonymous_uri() {
        assert_eq!(
            graph_resource_id_for_selection(Some("local"), "/tmp/graph.omni"),
            "local"
        );
        assert_eq!(
            graph_resource_id_for_selection(None, "/tmp/graph.omni"),
            "/tmp/graph.omni"
        );
    }

    #[test]
    fn select_named_query_refuses_a_branch_statement() {
        let err = select_named_query(parse_query("branch create b0").unwrap(), None).unwrap_err();
        assert_eq!(
            err.to_string(),
            "`branch create` is a branch statement, not a query declaration"
        );
    }

    // RFC-011 Decision 9: locality classifier for the destructive-confirm gate.
    #[test]
    fn uri_is_local_truth_table() {
        // Local: bare path or file://.
        assert!(uri_is_local("graph.omni"));
        assert!(uri_is_local("/abs/path/graph.omni"));
        assert!(uri_is_local("file:///tmp/graph.omni"));
        // Non-local: served or object-store schemes.
        assert!(!uri_is_local("http://host/graphs/g"));
        assert!(!uri_is_local("https://host/graphs/g"));
        assert!(!uri_is_local("unix:///tmp/og.sock/graphs/g"));
        assert!(!uri_is_local("s3://bucket/graph.omni"));
        assert!(!uri_is_local("gs://bucket/graph.omni"));
    }

    #[test]
    fn confirm_destructive_refuses_non_local_without_consent() {
        for (terminal, json) in [(false, false), (false, true), (true, true)] {
            let mut reader = std::io::Cursor::new(b"yes\n");
            let mut stderr = Vec::new();
            let error = confirm_destructive(
                "cleanup",
                "s3://b/g.omni",
                false,
                json,
                &mut crate::input::Input {
                    reader: &mut reader,
                    is_terminal: terminal,
                },
                &mut stderr,
            )
            .unwrap_err();
            assert!(error.to_string().contains("--yes"), "{error}");
            assert_eq!(reader.position(), 0);
            assert!(stderr.is_empty());
        }
    }

    #[test]
    fn confirm_destructive_allows_local_and_explicit_yes() {
        for (uri, yes) in [
            ("file:///tmp/g.omni", false),
            ("graph.omni", false),
            ("s3://b/g.omni", true),
        ] {
            let mut reader = std::io::Cursor::new(b"no\n");
            let mut stderr = Vec::new();
            confirm_destructive(
                "cleanup",
                uri,
                yes,
                true,
                &mut crate::input::Input {
                    reader: &mut reader,
                    is_terminal: false,
                },
                &mut stderr,
            )
            .unwrap();
            assert_eq!(reader.position(), 0);
            assert!(stderr.is_empty());
        }
    }

    #[test]
    fn confirmation_uses_supplied_io_and_consumes_only_the_answer() {
        use std::io::{self, Read, Write};
        for (answer, accepted) in [(" YES ", true), ("y", true), ("no", false), ("", false)] {
            let bytes = format!("{answer}\nnext payload");
            let mut reader = io::Cursor::new(bytes);
            let mut stderr = Vec::new();
            let result = confirm_destructive(
                "cleanup",
                "s3://b/g.omni",
                false,
                false,
                &mut crate::input::Input {
                    reader: &mut reader,
                    is_terminal: true,
                },
                &mut stderr,
            );
            assert_eq!(result.is_ok(), accepted);
            assert_eq!(reader.position(), (answer.len() + 1) as u64);
            assert_eq!(
                String::from_utf8(stderr).unwrap(),
                "About to run a destructive `cleanup` against s3://b/g.omni (not local). Type 'yes' to continue: "
            );
        }
        struct FailedPrompt {
            flush: bool,
        }
        impl Write for FailedPrompt {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if self.flush {
                    Ok(bytes.len())
                } else {
                    Err(io::ErrorKind::BrokenPipe.into())
                }
            }
            fn flush(&mut self) -> io::Result<()> {
                Err(io::ErrorKind::BrokenPipe.into())
            }
        }
        for flush in [false, true] {
            let mut reader = io::Cursor::new(b"yes\n");
            let error = confirm_destructive(
                "cleanup",
                "s3://b/g.omni",
                false,
                false,
                &mut crate::input::Input {
                    reader: &mut reader,
                    is_terminal: true,
                },
                &mut FailedPrompt { flush },
            )
            .unwrap_err();
            assert_eq!(
                error.downcast_ref::<io::Error>().unwrap().kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                reader.position(),
                0,
                "a failed prompt must not consume consent"
            );
        }
        struct FailedInput;
        impl Read for FailedInput {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::ErrorKind::UnexpectedEof.into())
            }
        }
        let error = confirm_destructive(
            "cleanup",
            "s3://b/g.omni",
            false,
            false,
            &mut crate::input::Input {
                reader: &mut FailedInput,
                is_terminal: true,
            },
            &mut Vec::new(),
        )
        .unwrap_err();
        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn actor_resolution_uses_supplied_config_and_explicit_override() {
        let config: OperatorConfig =
            yaml_serde::from_str("operator:\n  actor: configured-actor\n").unwrap();
        assert_eq!(
            resolve_actor_with_config(&config, None).as_deref(),
            Some("configured-actor")
        );
        assert_eq!(
            resolve_actor_with_config(&config, Some("explicit-actor")).as_deref(),
            Some("explicit-actor")
        );
    }

    #[test]
    fn server_targets_validate_unix_sockets_and_refuse_other_schemes_issue_979() {
        for root in ["HTTP://h:8080", "https://h/prefix"] {
            assert!(
                crate::targets::validate_server_endpoint(root).is_ok(),
                "{root}"
            );
        }
        for root in [
            "ftp://h/x",
            "s3://bucket/graph.omni",
            "file:///tmp/graph.omni",
            "https://user:secret@host",
            "https://host?q=1",
        ] {
            assert!(
                crate::targets::validate_server_endpoint(root).is_err(),
                "{root}"
            );
        }
        #[cfg(unix)]
        {
            for root in [
                "unix:///tmp/og.sock",
                "UNIX:///tmp/og.sock",
                "unix:///tmp/gate%20way/og.sock",
            ] {
                assert!(
                    crate::targets::validate_server_endpoint(root).is_ok(),
                    "{root}"
                );
            }
            for root in [
                "unix://host/tmp/og.sock",
                "unix:///",
                "unix:///tmp/og/..",
                "unix:///tmp/og.sock?x=1",
                "unix:///tmp/og.sock#f",
                "unix:///tmp/og%00.sock",
            ] {
                assert!(
                    crate::targets::validate_server_endpoint(root).is_err(),
                    "{root}"
                );
            }
        }
        #[cfg(not(unix))]
        assert!(crate::targets::validate_server_endpoint("unix:///tmp/og.sock").is_err());
    }

    // `branch delete` interpolates the branch into the URL path. The composed
    // path must be exactly `<base-path>/branches/<name>` with no empty `//`
    // segment — an empty segment misses the
    // `/graphs/{graph_id}/branches/{branch}` route and 404s.
    #[test]
    fn remote_url_multi_graph_base_has_no_double_slash() {
        let url = remote_url("http://host/graphs/p9-os", &["branches", "tmpbranch"], &[]).unwrap();
        assert_eq!(url, "http://host/graphs/p9-os/branches/tmpbranch");
        assert!(
            !url.contains("//branches"),
            "double slash before branches: {url}"
        );
    }

    #[test]
    fn remote_url_single_graph_base_has_no_double_slash() {
        let url = remote_url("http://host", &["branches", "tmpbranch"], &[]).unwrap();
        assert_eq!(url, "http://host/branches/tmpbranch");
    }

    #[test]
    fn remote_url_tolerates_trailing_slash_on_base() {
        let url = remote_url("http://host/graphs/p9-os/", &["branches", "tmpbranch"], &[]).unwrap();
        assert_eq!(url, "http://host/graphs/p9-os/branches/tmpbranch");
    }

    #[test]
    fn remote_url_encodes_slashes_in_path_segment() {
        let url = remote_url(
            "http://host/graphs/p9-os",
            &["branches", "etl/zendesk/run-1"],
            &[],
        )
        .unwrap();
        assert_eq!(
            url,
            "http://host/graphs/p9-os/branches/etl%2Fzendesk%2Frun-1"
        );
    }

    // Sibling cases the unified builder closes by construction: a dynamic
    // commit id in the path, and a branch name carried as a query value, are
    // both percent-encoded instead of interpolated raw.
    #[test]
    fn remote_url_encodes_dynamic_path_segment_for_commits() {
        let url = remote_url("http://host/graphs/p9-os", &["commits", "a/b c"], &[]).unwrap();
        assert_eq!(url, "http://host/graphs/p9-os/commits/a%2Fb%20c");
    }

    #[test]
    fn remote_url_encodes_query_values() {
        let url = remote_url(
            "http://host/graphs/p9-os",
            &["snapshot"],
            &[("branch", "feature&x=1")],
        )
        .unwrap();
        assert_eq!(
            url,
            "http://host/graphs/p9-os/snapshot?branch=feature%26x%3D1"
        );
    }
}
