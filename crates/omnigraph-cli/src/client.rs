//! `GraphClient` — the one place the embedded-vs-remote split lives
//! (RFC-009 Phase 3). A CLI command body calls a verb method; the
//! enum routes to the engine (local URI) or HTTP (remote URI). The
//! 15 per-command `if graph.is_remote { … } else { … }` forks collapse
//! into two arms here.
//!
//! Phase 3a put the factory + the uniform read verbs in place. Phase 3b
//! adds the data-plane writes (`load`/`ingest`/`mutate`/`branch_*`/
//! `apply_schema`) and `query`. The wrinkle 3a deferred: writes open the
//! local engine WITH policy (`open_local_db_with_policy`) and carry a
//! resolved actor, while reads/`query` open WITHOUT policy. So the
//! `Embedded` variant grows an optional policy context (`graph`/`actor`)
//! and a second factory (`resolve_with_policy`) fills it; `resolve()`
//! leaves it empty. The open path picks itself from whether `graph` is
//! set, preserving today's two behaviors exactly. Export + graphs-list
//! land in 3c. Behavior is unchanged per verb — the Phase-1 parity matrix
//! is the referee and stays textually unchanged.
//!
//! Enum, not a trait (RFC sketch said "trait"): only two variants ever,
//! and inherent async methods sidestep `async_trait` boxing plus the
//! `apply_schema` catalog-validator closure that is not object-safe.
//! Same one-body-two-impls collapse, less ceremony.

use std::io::Write;
use std::path::Path;

use color_eyre::Result;
use color_eyre::eyre::{bail, eyre};
use futures::stream;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph_api_types::{
    BranchCreateOutput, BranchCreateRequest, BranchDeleteOutput, BranchListOutput,
    BranchMergeOutput, BranchMergeRequest, ChangeOutput, CommitListOutput, CommitOutput,
    ErrorOutput, ExportRequest, GraphListResponse, IngestOutput, IngestRequest,
    InvokeStoredQueryRequest, ReadOutput, ReadRequest, SchemaApplyOutput, SchemaApplyRequest,
    SchemaOutput, SnapshotOutput, StreamEnsureIndicesOutput, StreamOptimizeOutput,
    StreamResumeOutput, StreamStatusOutput, commit_output, ingest_output, read_output,
    schema_apply_output, snapshot_payload,
};
use omnigraph_compiler::catalog::Catalog;
use reqwest::Method;
use reqwest::header::{ACCEPT, CONTENT_TYPE, ETAG, HeaderValue, IF_MATCH};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::cli::CliLoadMode;
use crate::helpers::{
    apply_bearer_token, apply_server_flag, build_http_client, is_remote_uri,
    legacy_change_request_body, query_params_from_json, remote_json, remote_url, resolve_cli_actor,
    resolve_cli_graph, resolve_remote_bearer_token, resolve_server_flag, select_named_query,
};
use crate::output::{LoadOutput, load_output_from_result, load_output_from_tables};

const NDJSON_CONTENT_TYPE: &str = "application/x-ndjson";
const STREAM_INPUT_CHUNK_BYTES: usize = 64 * 1024;

fn strong_etag_from_bare_graph_token(token: &str) -> Result<HeaderValue> {
    if token.is_empty()
        || token.starts_with("W/")
        || token
            .chars()
            .any(|character| character.is_whitespace() || matches!(character, '"' | ',' | '*'))
    {
        bail!(
            "graph token must be one non-empty bare opaque token, without whitespace, quotes, \
             wildcard, or comma syntax"
        );
    }
    Ok(HeaderValue::from_str(&format!("\"{token}\""))?)
}

fn validate_strong_etag(value: &HeaderValue) -> Result<()> {
    let value = value
        .to_str()
        .map_err(|_| eyre!("stream ingest preflight returned a non-text ETag"))?;
    if value.starts_with("W/")
        || value.len() < 2
        || !value.starts_with('"')
        || !value.ends_with('"')
    {
        bail!("stream ingest preflight returned a malformed or weak ETag");
    }
    strong_etag_from_bare_graph_token(&value[1..value.len() - 1])?;
    Ok(())
}

async fn stream_request_error(response: reqwest::Response) -> color_eyre::Report {
    let status = response.status();
    match response.text().await {
        Ok(text) => {
            if let Ok(error) = serde_json::from_str::<ErrorOutput>(&text) {
                eyre!(error.error)
            } else {
                eyre!("server returned {status}: {text}")
            }
        }
        Err(error) => eyre!("server returned {status}; failed to read error body: {error}"),
    }
}

fn streaming_request_body(reader: Box<dyn AsyncRead + Send + Unpin>) -> reqwest::Body {
    let chunks = stream::try_unfold(reader, |mut reader| async move {
        let mut chunk = vec![0_u8; STREAM_INPUT_CHUNK_BYTES];
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            return Ok::<_, std::io::Error>(None);
        }
        chunk.truncate(read);
        Ok(Some((chunk, reader)))
    });
    reqwest::Body::wrap_stream(chunks)
}

async fn open_stream_input(path: &Path) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
    if path == Path::new("-") {
        Ok(Box::new(tokio::io::stdin()))
    } else {
        Ok(Box::new(tokio::fs::File::open(path).await?))
    }
}

pub(crate) enum GraphClient {
    /// Local engine at `uri`. Reads (`resolve()`) leave `actor` empty;
    /// writes (`resolve_with_policy()`) attribute the resolved actor.
    /// Direct-store access carries no Cedar policy (RFC-011: policy lives
    /// in the cluster/server, not in per-operator addressing).
    Embedded { uri: String, actor: Option<String> },
    /// Remote HTTP server. The actor is resolved server-side from the
    /// token; the client never sets identity.
    Remote {
        http: reqwest::Client,
        base_url: String,
        token: Option<String>,
    },
}

/// RFC-011 Decision 7: a server scope that selects no graph (no `--graph`, no
/// `default_graph`) must not silently fall through to the bare server URL when
/// the server is multi-graph. Best-effort probe `GET /graphs`: a populated list
/// forces `--graph` (listing the candidates); a single-graph/flat server (405),
/// a policy-gated `/graphs`, or an unreachable server all proceed — the bare URL
/// is then correct, or the real request surfaces the failure. Only fires on the
/// no-graph path, so a `--graph`/`default_graph` happy path does no extra I/O.
async fn require_graph_for_multi_graph_server(scope: &crate::scope::ResolvedScope) -> Result<()> {
    let (Some(server), None) = (scope.server.as_deref(), scope.graph.as_deref()) else {
        return Ok(());
    };
    let probe = GraphClient::registry_client(server)?;
    if let Ok(resp) = probe.list_graphs().await {
        if !resp.graphs.is_empty() {
            let ids: Vec<&str> = resp.graphs.iter().map(|g| g.graph_id.as_str()).collect();
            bail!(
                "server scope '{server}' has {} {}: [{}]; pass --graph <id> to select one \
                 (or set `default_graph` in your operator config)",
                ids.len(),
                if ids.len() == 1 { "graph" } else { "graphs" },
                ids.join(", ")
            );
        }
    }
    Ok(())
}

/// A remote graph must be addressed with `--server` (RFC-011): a positional or
/// `--uri` `http(s)://` URL no longer auto-dispatches to a server. A remote URL
/// produced by a server scope (`via_server`) is fine.
fn reject_positional_remote(via_server: bool, uri: &str) -> Result<()> {
    if !via_server && is_remote_uri(uri) {
        bail!(
            "a remote graph must be addressed with `--server <url>` — a positional \
             (or `--uri`) http(s):// URL no longer dispatches to a server"
        );
    }
    Ok(())
}

impl GraphClient {
    /// The single owner of registry (`GET /graphs`) addressing: the bare base
    /// URL of `server` (a config name or literal URL) — never `/graphs/<id>`
    /// — with the keyed bearer-token chain. Synchronous: pure config
    /// resolution, no I/O. Used by the RFC-011 D7 multi-graph probe and the
    /// `graphs list` registry factory.
    fn registry_client(server: &str) -> Result<Self> {
        let base = resolve_server_flag(Some(server), None)?.expect("server name is present");
        let token = resolve_remote_bearer_token(Some(&base))?;
        Ok(GraphClient::Remote {
            http: build_http_client()?,
            base_url: base,
            token,
        })
    }

    /// Served-REGISTRY factory (RFC-011): resolve a server scope (`--server`
    /// / `--profile` / `defaults.server`) to the bare server base URL for
    /// `graphs list`. Synchronous by design: the RFC-011 D7 multi-graph probe
    /// (`require_graph_for_multi_graph_server`) is async, so it structurally
    /// cannot run on this path — `graphs list` IS the enumeration the probe
    /// performs. There is no graph selection and no `/graphs/<id>` append; a
    /// scope's `default_graph` is deliberately ignored (rejecting a config
    /// default would make `graphs list` unusable in any profile that sets
    /// one, and the registry is server-scoped either way). An explicit
    /// `--graph` never reaches here — the addressing guard rejects it.
    pub(crate) fn resolve_registry(server: Option<&str>, profile: Option<&str>) -> Result<Self> {
        let scope = crate::scope::resolve_scope(
            &crate::operator::load_operator_config()?,
            crate::planes::Capability::Served,
            crate::scope::ScopeFlags {
                profile,
                store: None,
                server,
                cluster: None,
                graph: None,
                uri: None,
            },
        )?;
        let Some(server) = scope.server.as_deref() else {
            bail!(
                "`graphs list` needs a server scope — pass --server <name|url> or \
                 --profile <name>, or set `defaults.server` in ~/.omnigraph/config.yaml"
            );
        };
        let client = Self::registry_client(server)?;
        if !is_remote_uri(client.uri()) {
            bail!(
                "a server scope resolves to an http(s):// URL; `{}` is not one",
                client.uri()
            );
        }
        Ok(client)
    }

    /// Resolve the addressing (positional URI / `--target` / `--server`)
    /// and credential once, then pick the variant by URI scheme — the
    /// single branch point that replaces every per-command `is_remote`
    /// fork. Mirrors the read verbs' current preamble (`resolve_uri`
    /// path, not the policy-bearing `resolve_cli_graph`). Used by reads
    /// and `query` (which opens without policy, like the reads).
    pub(crate) async fn resolve(
        capability: crate::planes::Capability,
        server: Option<&str>,
        graph: Option<&str>,
        uri: Option<String>,
        profile: Option<&str>,
        store: Option<&str>,
    ) -> Result<Self> {
        // RFC-011: a scope (profile / --store / operator defaults) may stand in
        // for omitted addressing. The explicit branch passes server/graph/uri
        // straight through, so existing invocations are unchanged. The caller
        // threads its verb's declared capability (planes::command_capability)
        // so scope resolution and the addressing guard share one
        // classification; every current caller is a data-plane (`Any`) verb —
        // registry-scoped `graphs list` uses `resolve_registry` instead.
        let scope = crate::scope::resolve_scope(
            &crate::operator::load_operator_config()?,
            capability,
            crate::scope::ScopeFlags {
                profile,
                store,
                server,
                cluster: None,
                graph,
                uri,
            },
        )?;
        require_graph_for_multi_graph_server(&scope).await?;
        let (server, graph, uri) = (scope.server.as_deref(), scope.graph.as_deref(), scope.uri);
        let via_server = server.is_some();
        let uri = apply_server_flag(server, graph, uri)?;
        let token = resolve_remote_bearer_token(uri.as_deref())?;
        let uri = crate::helpers::resolve_uri(uri)?;
        reject_positional_remote(via_server, &uri)?;
        if is_remote_uri(&uri) {
            Ok(GraphClient::Remote {
                http: build_http_client()?,
                base_url: uri,
                token,
            })
        } else {
            Ok(GraphClient::Embedded { uri, actor: None })
        }
    }

    /// Write-path factory: the same addressing/credential resolution as
    /// `resolve()`, but through the stricter `resolve_cli_graph` (which
    /// carries `policy_file`/`graph_id`/`selected`), and with the actor
    /// resolved up front. The embedded arm then opens WITH policy. The
    /// resolution order matches the write arms exactly: server flag →
    /// bearer token → graph.
    pub(crate) async fn resolve_with_policy(
        capability: crate::planes::Capability,
        server: Option<&str>,
        graph: Option<&str>,
        uri: Option<String>,
        cli_as: Option<&str>,
        profile: Option<&str>,
        store: Option<&str>,
    ) -> Result<Self> {
        // RFC-011 scope translation (see `resolve`); explicit addressing passes
        // through unchanged, and the caller threads its verb's declared
        // capability.
        let scope = crate::scope::resolve_scope(
            &crate::operator::load_operator_config()?,
            capability,
            crate::scope::ScopeFlags {
                profile,
                store,
                server,
                cluster: None,
                graph,
                uri,
            },
        )?;
        Self::resolve_with_policy_scope(scope, cli_as).await
    }

    /// Resolve the served graph selected by `stream ingest`. Unlike the
    /// legacy data-command resolver, this surface has no flat/single-graph
    /// server fallback: OmniGraph servers are cluster-only, so the route must
    /// include one graph selected explicitly or by configuration.
    pub(crate) async fn resolve_stream_ingest(
        server: Option<&str>,
        graph: Option<&str>,
        cli_as: Option<&str>,
        profile: Option<&str>,
        store: Option<&str>,
    ) -> Result<Self> {
        Self::resolve_selected_served_graph("stream ingest", server, graph, cli_as, profile, store)
            .await
    }

    /// Resolve the served graph selected by read-only `stream status`.
    /// Like ingest, status has no flat-server fallback: the checked runtime is
    /// owned by one graph selected explicitly or by operator configuration.
    pub(crate) async fn resolve_stream_status(
        server: Option<&str>,
        graph: Option<&str>,
        profile: Option<&str>,
        store: Option<&str>,
    ) -> Result<Self> {
        Self::resolve_selected_served_graph("stream status", server, graph, None, profile, store)
            .await
    }

    /// Resolve one graph-wide served stream control. The public surface has no
    /// declaration/table selector and never accepts a client-supplied actor.
    pub(crate) async fn resolve_stream_control(
        command: &str,
        server: Option<&str>,
        graph: Option<&str>,
        profile: Option<&str>,
        store: Option<&str>,
    ) -> Result<Self> {
        Self::resolve_selected_served_graph(command, server, graph, None, profile, store).await
    }

    /// Shared graph-selection owner for the served-only stream family.
    /// `command` is threaded only into the observable missing-graph error;
    /// keeping the ingest spelling here preserves its existing contract.
    async fn resolve_selected_served_graph(
        command: &str,
        server: Option<&str>,
        graph: Option<&str>,
        cli_as: Option<&str>,
        profile: Option<&str>,
        store: Option<&str>,
    ) -> Result<Self> {
        let scope = crate::scope::resolve_scope(
            &crate::operator::load_operator_config()?,
            crate::planes::Capability::Served,
            crate::scope::ScopeFlags {
                profile,
                store,
                server,
                cluster: None,
                graph,
                uri: None,
            },
        )?;
        if scope.graph.is_none() {
            bail!(
                "`{command}` requires one selected graph; pass --graph <id> with \
                 --server, or configure default_graph on the selected server profile"
            );
        }
        Self::resolve_with_policy_scope(scope, cli_as).await
    }

    async fn resolve_with_policy_scope(
        scope: crate::scope::ResolvedScope,
        cli_as: Option<&str>,
    ) -> Result<Self> {
        require_graph_for_multi_graph_server(&scope).await?;
        let (server, graph, uri) = (scope.server.as_deref(), scope.graph.as_deref(), scope.uri);
        let via_server = server.is_some();
        let uri = apply_server_flag(server, graph, uri)?;
        let token = resolve_remote_bearer_token(uri.as_deref())?;
        let resolved = resolve_cli_graph(uri)?;
        reject_positional_remote(via_server, &resolved.uri)?;
        if resolved.is_remote {
            // A served write resolves the actor server-side from the bearer
            // token; `--as` cannot set identity here and is rejected.
            if cli_as.is_some() {
                bail!(
                    "`--as` is not allowed on a served write — the server resolves the actor \
                     from the bearer token. Remove `--as`, or run the write directly against \
                     storage with `--store <uri>`."
                );
            }
            Ok(GraphClient::Remote {
                http: build_http_client()?,
                base_url: resolved.uri,
                token,
            })
        } else {
            let actor = resolve_cli_actor(cli_as)?;
            Ok(GraphClient::Embedded {
                uri: resolved.uri,
                actor,
            })
        }
    }

    /// The graph URI (local path / remote base URL) this client addresses.
    pub(crate) fn uri(&self) -> &str {
        match self {
            GraphClient::Embedded { uri, .. } => uri,
            GraphClient::Remote { base_url, .. } => base_url,
        }
    }

    pub(crate) fn is_remote(&self) -> bool {
        matches!(self, GraphClient::Remote { .. })
    }

    /// Open the local engine. Direct-store access carries no Cedar policy
    /// (RFC-011), so both read and write paths open bare; the actor is still
    /// attributed on the write via the `_as` engine APIs.
    async fn open_embedded(uri: &str) -> Result<Omnigraph> {
        Ok(Omnigraph::open(uri).await?)
    }

    pub(crate) async fn branch_list(&self) -> Result<BranchListOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                remote_json(
                    http,
                    Method::GET,
                    remote_url(base_url, &["branches"], &[])?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let db = Omnigraph::open(uri).await?;
                let mut branches = db.branch_list().await?;
                branches.sort();
                Ok(BranchListOutput { branches })
            }
        }
    }

    pub(crate) async fn snapshot(&self, branch: &str) -> Result<SnapshotOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                remote_json(
                    http,
                    Method::GET,
                    remote_url(base_url, &["snapshot"], &[("branch", branch)])?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let db = Omnigraph::open(uri).await?;
                let snapshot = db.snapshot_of(ReadTarget::branch(branch)).await?;
                let internal_schema_version = db
                    .internal_schema_version_of(ReadTarget::branch(branch))
                    .await?;
                Ok(snapshot_payload(branch, &snapshot, internal_schema_version))
            }
        }
    }

    pub(crate) async fn schema_source(&self) -> Result<SchemaOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                remote_json(
                    http,
                    Method::GET,
                    remote_url(base_url, &["schema"], &[])?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let db = Omnigraph::open(uri).await?;
                Ok(SchemaOutput {
                    schema_source: db.schema_source().to_string(),
                })
            }
        }
    }

    pub(crate) async fn list_commits(&self, branch: Option<&str>) -> Result<CommitListOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                let url = match branch {
                    Some(branch) => remote_url(base_url, &["commits"], &[("branch", branch)])?,
                    None => remote_url(base_url, &["commits"], &[])?,
                };
                remote_json(http, Method::GET, url, None, token.as_deref()).await
            }
            GraphClient::Embedded { uri, .. } => {
                let db = Omnigraph::open(uri).await?;
                let commits = db
                    .list_commits(branch)
                    .await?
                    .iter()
                    .map(commit_output)
                    .collect::<Vec<_>>();
                Ok(CommitListOutput { commits })
            }
        }
    }

    pub(crate) async fn get_commit(&self, commit_id: &str) -> Result<CommitOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                remote_json(
                    http,
                    Method::GET,
                    remote_url(base_url, &["commits", commit_id], &[])?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let db = Omnigraph::open(uri).await?;
                Ok(commit_output(&db.get_commit(commit_id).await?))
            }
        }
    }

    /// `load` — bulk-load `data` (a file path) onto `branch`, forking from
    /// `from` if missing. Returns the CLI `LoadOutput`; each arm keeps its
    /// own mapping (remote sums the wire `IngestOutput.tables`, embedded
    /// reads the richer `LoadResult` directly) — preserved exactly.
    pub(crate) async fn load(
        &self,
        branch: &str,
        from: Option<&str>,
        data: &str,
        mode: CliLoadMode,
    ) -> Result<LoadOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                let data = std::fs::read_to_string(data)?;
                // RFC-009 Phase 5: the canonical `load` verb targets the
                // canonical `/load` route (the deprecated `ingest` verb below
                // still rides `/ingest`).
                let output = remote_json::<IngestOutput>(
                    http,
                    Method::POST,
                    remote_url(base_url, &["load"], &[])?,
                    Some(serde_json::to_value(IngestRequest {
                        branch: Some(branch.to_string()),
                        from: from.map(ToOwned::to_owned),
                        mode: Some(mode.into()),
                        data,
                    })?),
                    token.as_deref(),
                )
                .await?;
                Ok(load_output_from_tables(
                    base_url,
                    branch,
                    mode.as_str(),
                    &output,
                ))
            }
            GraphClient::Embedded { uri, actor } => {
                let db = Self::open_embedded(uri).await?;
                let result = db
                    .load_file_as(branch, from, data, mode.into(), actor.as_deref())
                    .await?;
                Ok(load_output_from_result(uri, branch, mode.as_str(), &result))
            }
        }
    }

    /// `ingest` — the deprecated alias of `load`. Same operation, but the
    /// surfaced shape is the wire `IngestOutput` (printed by
    /// `print_ingest_human`), so it is its own method. The embedded arm
    /// echoes `actor_id: None` in the output exactly as the legacy arm did
    /// (the actor is still attributed on the commit via `load_file_as`).
    pub(crate) async fn ingest(
        &self,
        branch: &str,
        from: &str,
        data: &str,
        mode: CliLoadMode,
    ) -> Result<IngestOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                let data = std::fs::read_to_string(data)?;
                remote_json(
                    http,
                    Method::POST,
                    remote_url(base_url, &["ingest"], &[])?,
                    Some(serde_json::to_value(IngestRequest {
                        branch: Some(branch.to_string()),
                        from: Some(from.to_string()),
                        mode: Some(mode.into()),
                        data,
                    })?),
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, actor } => {
                let db = Self::open_embedded(uri).await?;
                let result = db
                    .load_file_as(branch, Some(from), data, mode.into(), actor.as_deref())
                    .await?;
                Ok(ingest_output(uri, &result, mode.into(), None))
            }
        }
    }

    /// `mutate` — run a change query against `branch`. Folds
    /// `execute_change` / `execute_change_remote` + the legacy request body.
    pub(crate) async fn mutate(
        &self,
        branch: &str,
        query_source: &str,
        query_name: Option<&str>,
        params_json: Option<&Value>,
    ) -> Result<ChangeOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                remote_json(
                    http,
                    Method::POST,
                    remote_url(base_url, &["change"], &[])?,
                    Some(legacy_change_request_body(
                        query_source,
                        query_name,
                        branch,
                        params_json,
                    )),
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, actor } => {
                let (selected_name, query_params) = select_named_query(query_source, query_name)?;
                let params = query_params_from_json(&query_params, params_json)?;
                let db = Self::open_embedded(uri).await?;
                let actor = actor.as_deref();
                let result = db
                    .mutate_as(branch, query_source, &selected_name, &params, actor)
                    .await?;
                Ok(ChangeOutput {
                    branch: branch.to_string(),
                    query_name: selected_name,
                    affected_nodes: result.affected_nodes,
                    affected_edges: result.affected_edges,
                    actor_id: actor.map(String::from),
                })
            }
        }
    }

    /// `query` — run a read query against `target`. Folds `execute_read` /
    /// `execute_read_remote`; the embedded arm opens WITHOUT policy (reads
    /// never attach one), so this verb resolves via `resolve()`.
    pub(crate) async fn query(
        &self,
        target: ReadTarget,
        query_source: &str,
        query_name: Option<&str>,
        params_json: Option<&Value>,
    ) -> Result<ReadOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                let (branch, snapshot) = match &target {
                    ReadTarget::Branch(branch) => (Some(branch.clone()), None),
                    ReadTarget::Snapshot(snapshot) => (None, Some(snapshot.as_str().to_string())),
                };
                remote_json(
                    http,
                    Method::POST,
                    remote_url(base_url, &["read"], &[])?,
                    Some(serde_json::to_value(ReadRequest {
                        query_source: query_source.to_string(),
                        query_name: query_name.map(ToOwned::to_owned),
                        params: params_json.cloned(),
                        branch,
                        snapshot,
                    })?),
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let (selected_name, query_params) = select_named_query(query_source, query_name)?;
                let params = query_params_from_json(&query_params, params_json)?;
                let db = Self::open_embedded(uri).await?;
                let result = db
                    .query(target.clone(), query_source, &selected_name, &params)
                    .await?;
                Ok(read_output(selected_name, &target, result))
            }
        }
    }

    /// `invoke_named` — run a stored query **by catalog name** (RFC-011 D3).
    /// Served-only: the catalog is server-owned, so a `--store` (embedded)
    /// scope has nothing to resolve the name against. `expect_mutation` carries
    /// the verb's asserted kind; the server rejects a mismatch (400) before
    /// running, so the response is exactly the expected envelope — the caller
    /// deserializes it as the concrete `T` (`ReadOutput` for `query`,
    /// `ChangeOutput` for `mutate`), sidestepping the untagged wire enum.
    pub(crate) async fn invoke_named<T: serde::de::DeserializeOwned>(
        &self,
        name: &str,
        expect_mutation: bool,
        params_json: Option<&Value>,
        branch: Option<String>,
        snapshot: Option<String>,
    ) -> Result<T> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                let body = InvokeStoredQueryRequest {
                    params: params_json.cloned(),
                    branch,
                    snapshot,
                    expect_mutation: Some(expect_mutation),
                };
                remote_json(
                    http,
                    Method::POST,
                    remote_url(base_url, &["queries", name], &[])?,
                    Some(serde_json::to_value(body)?),
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { .. } => bail!(
                "by-name invocation needs a server (the stored-query catalog is \
                 server-owned); use -e '<gq>' or --query <file> for an ad-hoc query \
                 against --store, or address a server with --server / --profile"
            ),
        }
    }

    pub(crate) async fn branch_create_from(
        &self,
        from: &str,
        name: &str,
    ) -> Result<BranchCreateOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                remote_json(
                    http,
                    Method::POST,
                    remote_url(base_url, &["branches"], &[])?,
                    Some(serde_json::to_value(BranchCreateRequest {
                        from: Some(from.to_string()),
                        name: name.to_string(),
                    })?),
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, actor } => {
                let db = Self::open_embedded(uri).await?;
                let actor = actor.as_deref();
                db.branch_create_from_as(ReadTarget::branch(from), name, actor)
                    .await?;
                Ok(BranchCreateOutput {
                    uri: uri.clone(),
                    from: from.to_string(),
                    name: name.to_string(),
                    actor_id: actor.map(String::from),
                })
            }
        }
    }

    pub(crate) async fn branch_delete(&self, name: &str) -> Result<BranchDeleteOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                remote_json(
                    http,
                    Method::DELETE,
                    remote_url(base_url, &["branches", name], &[])?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, actor } => {
                let db = Self::open_embedded(uri).await?;
                let actor = actor.as_deref();
                db.branch_delete_as(name, actor).await?;
                Ok(BranchDeleteOutput {
                    uri: uri.clone(),
                    name: name.to_string(),
                    actor_id: actor.map(String::from),
                })
            }
        }
    }

    pub(crate) async fn branch_merge(
        &self,
        source: &str,
        into: &str,
        delete_branch: bool,
    ) -> Result<BranchMergeOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                remote_json(
                    http,
                    Method::POST,
                    remote_url(base_url, &["branches", "merge"], &[])?,
                    Some(serde_json::to_value(BranchMergeRequest {
                        source: source.to_string(),
                        target: Some(into.to_string()),
                        delete_branch,
                    })?),
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, actor } => {
                let db = Self::open_embedded(uri).await?;
                let actor = actor.as_deref();
                let outcome = db.branch_merge_as(source, into, actor).await?;
                // Composed exactly like the server handler: the merge is
                // durable, so a deletion refusal/failure is reported in the
                // payload, never as an error (parity_matrix pins the two
                // composition sites against drift).
                let (branch_deleted, branch_delete_error) = if delete_branch {
                    match db.branch_delete_as(source, actor).await {
                        Ok(()) => (Some(true), None),
                        Err(err) => (Some(false), Some(err.to_string())),
                    }
                } else {
                    (None, None)
                };
                Ok(BranchMergeOutput {
                    source: source.to_string(),
                    target: into.to_string(),
                    outcome: outcome.into(),
                    actor_id: actor.map(String::from),
                    branch_deleted,
                    branch_delete_error,
                })
            }
        }
    }

    /// `apply_schema` — apply `schema_source`. The embedded arm runs the
    /// caller's catalog validator (stored-query registry check) inside the
    /// engine's `apply_schema_as_with_catalog_check`; the remote arm runs
    /// the server's own check and IGNORES `validate`. The `impl FnOnce`
    /// validator is exactly why this is an enum, not a trait (non-object-
    /// safe).
    pub(crate) async fn apply_schema<F>(
        &self,
        schema_source: &str,
        allow_data_loss: bool,
        validate: F,
    ) -> Result<SchemaApplyOutput>
    where
        F: FnOnce(&Catalog) -> omnigraph::error::Result<()>,
    {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                // MR-694 PR B: SchemaApplyRequest carries allow_data_loss so
                // Hard-mode drops are no longer CLI-only; the server's
                // `server_schema_apply` honors it (and runs its own catalog
                // check, so `validate` does not apply here).
                remote_json::<SchemaApplyOutput>(
                    http,
                    Method::POST,
                    remote_url(base_url, &["schema", "apply"], &[])?,
                    Some(serde_json::to_value(SchemaApplyRequest {
                        schema_source: schema_source.to_string(),
                        allow_data_loss,
                    })?),
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, actor } => {
                let db = Self::open_embedded(uri).await?;
                let result = db
                    .apply_schema_as_with_catalog_check(
                        schema_source,
                        omnigraph::db::SchemaApplyOptions { allow_data_loss },
                        actor.as_deref(),
                        validate,
                    )
                    .await?;
                Ok(schema_apply_output(uri, result))
            }
        }
    }

    /// `stream ingest` — send one incremental NDJSON request to a served
    /// graph and copy its ordered NDJSON outcomes directly into `writer`.
    ///
    /// With no caller-supplied graph token, the client first makes one empty
    /// request and requires the server's 428 strong-ETag challenge. Only then
    /// does it open `data`, so a refused preflight cannot consume stdin or
    /// touch a file. A supplied token skips the preflight. In either case the
    /// input body is opened and invoked at most once: 412 and every other
    /// response error are surfaced without replacing the token or replaying
    /// an owned body.
    pub(crate) async fn stream_ingest<W: Write>(
        &self,
        data: &Path,
        graph_token: Option<&str>,
        writer: &mut W,
    ) -> Result<()> {
        let GraphClient::Remote {
            http,
            base_url,
            token,
        } = self
        else {
            bail!(
                "`stream ingest` requires a served graph; address it with --server <name|url> \
                 or a server-backed --profile"
            );
        };

        let url = remote_url(base_url, &["stream", "ingest"], &[])?;
        let graph_etag = match graph_token {
            Some(token) => strong_etag_from_bare_graph_token(token)?,
            None => {
                let preflight =
                    apply_bearer_token(http.request(Method::POST, url.clone()), token.as_deref())
                        .header(CONTENT_TYPE, NDJSON_CONTENT_TYPE)
                        .header(ACCEPT, NDJSON_CONTENT_TYPE)
                        .send()
                        .await?;
                if preflight.status() != reqwest::StatusCode::PRECONDITION_REQUIRED {
                    return Err(stream_request_error(preflight).await);
                }
                let etag =
                    preflight.headers().get(ETAG).cloned().ok_or_else(|| {
                        eyre!("stream ingest preflight omitted its ETag challenge")
                    })?;
                validate_strong_etag(&etag)?;
                // The ETag is authoritative. Drop rather than buffer an
                // untrusted challenge body; the exact request may use a fresh
                // connection.
                drop(preflight);
                etag
            }
        };

        // This is deliberately below the successful preflight. The resulting
        // Body is single-use and no response branch reconstructs it.
        let body = streaming_request_body(open_stream_input(data).await?);
        let request = apply_bearer_token(http.request(Method::POST, url), token.as_deref())
            .header(CONTENT_TYPE, NDJSON_CONTENT_TYPE)
            .header(ACCEPT, NDJSON_CONTENT_TYPE)
            .header(IF_MATCH, graph_etag)
            .body(body);
        let mut response = request.send().await?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(stream_request_error(response).await);
        }
        let response_is_ndjson = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case(NDJSON_CONTENT_TYPE));
        if !response_is_ndjson {
            bail!(
                "stream ingest requires an exact 200 application/x-ndjson response from the server"
            );
        }
        while let Some(chunk) = response.chunk().await? {
            writer.write_all(&chunk)?;
        }
        writer.flush()?;
        Ok(())
    }

    /// `stream status` — obtain one checked, graph-logical operational cut
    /// from the selected served graph. The wire DTO is deliberately redacted;
    /// the CLI never receives physical lane, dataset, or recovery identities.
    pub(crate) async fn stream_operational_status(&self) -> Result<StreamStatusOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                remote_json(
                    http,
                    Method::GET,
                    remote_url(base_url, &["stream", "status"], &[])?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { .. } => bail!(
                "internal error: `stream status` reached an embedded client — stream status \
                 addressing always resolves a server"
            ),
        }
    }

    /// Reopen every currently sealed declaration in the selected graph.
    pub(crate) async fn stream_resume(&self) -> Result<StreamResumeOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                remote_json(
                    http,
                    Method::POST,
                    remote_url(base_url, &["stream", "resume"], &[])?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { .. } => bail!(
                "internal error: `stream resume` reached an embedded client — stream controls \
                 always resolve a server"
            ),
        }
    }

    /// Reconcile declared indexes across the graph. Any enrolled declaration
    /// changed by the operation must be sealed; physical datasets stay private
    /// behind the graph coordinator.
    pub(crate) async fn stream_ensure_indices(&self) -> Result<StreamEnsureIndicesOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                remote_json(
                    http,
                    Method::POST,
                    remote_url(base_url, &["stream", "maintenance", "ensure-indices"], &[])?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { .. } => bail!(
                "internal error: `stream maintenance ensure-indices` reached an embedded client"
            ),
        }
    }

    /// Compact the graph through the existing coordinated publish. Any
    /// enrolled declaration changed by the operation must be sealed.
    pub(crate) async fn stream_optimize(&self) -> Result<StreamOptimizeOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                remote_json(
                    http,
                    Method::POST,
                    remote_url(base_url, &["stream", "maintenance", "optimize"], &[])?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { .. } => {
                bail!("internal error: `stream maintenance optimize` reached an embedded client")
            }
        }
    }

    /// `export` — stream the branch as JSONL into `writer`. The streaming
    /// shape (a `W: Write`, not a returned DTO) is why this lands in 3c
    /// rather than 3b. Opens WITHOUT policy (like reads), so it is reached
    /// via `resolve()`; the Embedded arm opens bare. The Remote arm streams
    /// the chunked response body straight through (no buffering the whole
    /// export in memory).
    pub(crate) async fn export<W: Write>(
        &self,
        branch: &str,
        type_names: &[String],
        table_keys: &[String],
        writer: &mut W,
    ) -> Result<()> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                let request = apply_bearer_token(
                    http.request(Method::POST, remote_url(base_url, &["export"], &[])?),
                    token.as_deref(),
                )
                .json(&ExportRequest {
                    branch: Some(branch.to_string()),
                    type_names: type_names.to_vec(),
                    table_keys: table_keys.to_vec(),
                });
                let mut response = request.send().await?;
                let status = response.status();
                if !status.is_success() {
                    let text = response.text().await?;
                    if let Ok(error) = serde_json::from_str::<ErrorOutput>(&text) {
                        bail!(error.error);
                    }
                    bail!("server returned {}: {}", status, text);
                }
                while let Some(chunk) = response.chunk().await? {
                    writer.write_all(&chunk)?;
                }
                writer.flush()?;
                Ok(())
            }
            GraphClient::Embedded { uri, .. } => {
                let db = Omnigraph::open(uri).await?;
                db.export_jsonl_to_writer(branch, type_names, table_keys, writer)
                    .await?;
                writer.flush()?;
                Ok(())
            }
        }
    }

    /// `graphs list` — enumerate the graphs a multi-graph server serves
    /// (`GET /graphs`). Reached only through registry-addressed clients
    /// (`resolve_registry` / the D7 probe's `registry_client`), which always
    /// build the Remote variant — the Embedded arm is unreachable by
    /// construction and kept as a defensive internal-invariant bail.
    pub(crate) async fn list_graphs(&self) -> Result<GraphListResponse> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                remote_json(
                    http,
                    Method::GET,
                    remote_url(base_url, &["graphs"], &[])?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { .. } => bail!(
                "internal error: `graphs list` reached an embedded client — registry \
                 addressing always resolves a server"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    struct CapturedRequest {
        head: String,
        body: Vec<u8>,
    }

    async fn ensure_buffered(stream: &mut TcpStream, bytes: &mut Vec<u8>, needed: usize) {
        while bytes.len() < needed {
            let mut chunk = [0_u8; 4096];
            let read = stream.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "connection closed before the request completed");
            bytes.extend_from_slice(&chunk[..read]);
        }
    }

    async fn read_http_request(stream: &mut TcpStream) -> CapturedRequest {
        let mut bytes = Vec::new();
        let header_end = loop {
            if let Some(pos) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break pos + 4;
            }
            let needed = bytes.len() + 1;
            ensure_buffered(stream, &mut bytes, needed).await;
        };
        let head = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
        let mut remaining = bytes.split_off(header_end);
        let lowercase = head.to_ascii_lowercase();
        let body = if lowercase.contains("transfer-encoding: chunked") {
            let mut body = Vec::new();
            loop {
                let line_end = loop {
                    if let Some(pos) = remaining.windows(2).position(|window| window == b"\r\n") {
                        break pos;
                    }
                    let needed = remaining.len() + 1;
                    ensure_buffered(stream, &mut remaining, needed).await;
                };
                let size_text = std::str::from_utf8(&remaining[..line_end]).unwrap();
                let size =
                    usize::from_str_radix(size_text.split(';').next().unwrap().trim(), 16).unwrap();
                remaining.drain(..line_end + 2);
                if size == 0 {
                    ensure_buffered(stream, &mut remaining, 2).await;
                    assert_eq!(&remaining[..2], b"\r\n");
                    break;
                }
                ensure_buffered(stream, &mut remaining, size + 2).await;
                body.extend_from_slice(&remaining[..size]);
                assert_eq!(&remaining[size..size + 2], b"\r\n");
                remaining.drain(..size + 2);
            }
            body
        } else {
            let content_length = lowercase
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .map(|value| value.trim().parse::<usize>().unwrap())
                .unwrap_or(0);
            ensure_buffered(stream, &mut remaining, content_length).await;
            remaining[..content_length].to_vec()
        };
        CapturedRequest { head, body }
    }

    async fn write_response(
        stream: &mut TcpStream,
        status: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) {
        let mut head = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n",
            body.len()
        );
        for (name, value) in headers {
            head.push_str(name);
            head.push_str(": ");
            head.push_str(value);
            head.push_str("\r\n");
        }
        head.push_str("\r\n");
        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        stream.shutdown().await.unwrap();
    }

    #[test]
    fn resolve_registry_is_sync_and_yields_the_bare_base_url() {
        // Structural proof the RFC-011 D7 multi-graph probe cannot fire on the
        // graphs-list path: resolve_registry is synchronous (called here with
        // no tokio runtime), while the probe is async and performs GET
        // /graphs. Also pins the URL-corruption fix: the bare base URL with
        // the trailing slash trimmed and no `/graphs/<id>` segment. A literal
        // `://` --server value bypasses the operator server registry, so a
        // developer's real config cannot change the outcome.
        let client = GraphClient::resolve_registry(Some("http://server.invalid:9/"), None).unwrap();
        assert_eq!(client.uri(), "http://server.invalid:9");
        assert!(client.is_remote());
    }

    #[tokio::test]
    async fn stream_ingest_preflights_before_opening_input_and_copies_jsonl() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("created-after-preflight.ndjson");
        assert!(!data.exists());
        let server_data = data.clone();
        let input = b"{\"type\":\"Person\",\"data\":{\"name\":\"Alice\"}}\n".to_vec();
        let expected_input = input.clone();
        let output =
            b"{\"ordinal\":0,\"status\":\"durable\"}\n{\"ordinal\":1,\"status\":\"invalid\"}\n"
                .to_vec();
        let expected_output = output.clone();

        let server = tokio::spawn(async move {
            let (mut preflight, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut preflight).await;
            assert!(request.head.starts_with("POST /stream/ingest HTTP/1.1"));
            assert!(!request.head.to_ascii_lowercase().contains("if-match:"));
            assert!(request.body.is_empty());
            tokio::fs::write(&server_data, &input).await.unwrap();
            write_response(
                &mut preflight,
                "428 Precondition Required",
                &[
                    ("Content-Type", "application/json"),
                    ("ETag", "\"sha256:graph-a\""),
                ],
                br#"{"graph_token":"sha256:graph-a"}"#,
            )
            .await;

            let (mut ingest, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut ingest).await;
            assert!(request.head.starts_with("POST /stream/ingest HTTP/1.1"));
            let lowercase = request.head.to_ascii_lowercase();
            assert!(lowercase.contains("content-type: application/x-ndjson"));
            assert!(lowercase.contains("accept: application/x-ndjson"));
            assert!(lowercase.contains("if-match: \"sha256:graph-a\""));
            write_response(
                &mut ingest,
                "200 OK",
                &[("Content-Type", "application/x-ndjson")],
                &output,
            )
            .await;
            request.body
        });

        let client = GraphClient::Remote {
            http: reqwest::Client::new(),
            base_url,
            token: None,
        };
        let mut actual_output = Vec::new();
        client
            .stream_ingest(&data, None, &mut actual_output)
            .await
            .unwrap();
        assert_eq!(server.await.unwrap(), expected_input);
        assert_eq!(actual_output, expected_output);
    }

    #[tokio::test]
    async fn supplied_graph_token_is_sent_once_and_never_replaced_on_412() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("input.ndjson");
        let input = b"{\"type\":\"Person\",\"data\":{\"name\":\"Alice\"}}\n";
        tokio::fs::write(&data, input).await.unwrap();

        let server = tokio::spawn(async move {
            let (mut ingest, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut ingest).await;
            let lowercase = request.head.to_ascii_lowercase();
            assert!(lowercase.contains("if-match: \"sha256:stale\""));
            assert_eq!(request.body, input);
            write_response(
                &mut ingest,
                "412 Precondition Failed",
                &[("Content-Type", "application/json")],
                br#"{"error":"stale graph token"}"#,
            )
            .await;
            tokio::time::timeout(std::time::Duration::from_millis(150), listener.accept())
                .await
                .is_err()
        });

        let client = GraphClient::Remote {
            http: reqwest::Client::new(),
            base_url,
            token: None,
        };
        let error = client
            .stream_ingest(&data, Some("sha256:stale"), &mut Vec::new())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("stale graph token"));
        assert!(server.await.unwrap(), "the client retried a consumed body");
    }

    #[tokio::test]
    async fn stream_ingest_requires_exact_200_ndjson_response() {
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("input.ndjson");
        tokio::fs::write(&data, b"{}\n").await.unwrap();

        for (status, content_type) in [
            ("204 No Content", "application/x-ndjson"),
            ("206 Partial Content", "application/x-ndjson"),
            ("200 OK", "application/json"),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base_url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (mut ingest, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut ingest).await;
                assert_eq!(request.body, b"{}\n");
                write_response(&mut ingest, status, &[("Content-Type", content_type)], &[]).await;
            });
            let client = GraphClient::Remote {
                http: reqwest::Client::new(),
                base_url,
                token: None,
            };
            let error = client
                .stream_ingest(&data, Some("sha256:graph-a"), &mut Vec::new())
                .await
                .unwrap_err()
                .to_string();
            if status == "200 OK" {
                assert!(error.contains("exact 200 application/x-ndjson"), "{error}");
            } else {
                assert!(
                    error.contains(status.split_whitespace().next().unwrap()),
                    "{error}"
                );
            }
            server.await.unwrap();
        }
    }

    #[test]
    fn graph_token_is_always_bare_and_strong_quoted() {
        assert_eq!(
            strong_etag_from_bare_graph_token("sha256:abc")
                .unwrap()
                .to_str()
                .unwrap(),
            "\"sha256:abc\""
        );
        for invalid in [
            "",
            "*",
            "W/sha256:abc",
            "\"sha256:abc\"",
            "sha256:a,sha256:b",
            "sha256:a b",
            "a\nb",
        ] {
            assert!(strong_etag_from_bare_graph_token(invalid).is_err());
        }
    }

    #[tokio::test]
    async fn invalid_graph_token_is_rejected_before_input_open_or_network() {
        let client = GraphClient::Remote {
            http: reqwest::Client::new(),
            base_url: "http://127.0.0.1:9".to_string(),
            token: None,
        };
        let error = client
            .stream_ingest(
                Path::new("/must-not-open.ndjson"),
                Some("*"),
                &mut Vec::new(),
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("graph token must be one"), "{error}");
    }

    #[tokio::test]
    async fn stream_status_gets_the_selected_graph_and_decodes_the_redacted_cut() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}/graphs/knowledge", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            assert!(
                request
                    .head
                    .starts_with("GET /graphs/knowledge/stream/status HTTP/1.1")
            );
            assert!(
                request
                    .head
                    .to_ascii_lowercase()
                    .contains("authorization: bearer status-token")
            );
            assert!(request.body.is_empty());
            write_response(
                &mut stream,
                "200 OK",
                &[("Content-Type", "application/json")],
                br#"{
                    "manifest_version":7,
                    "profile_mode":"enabled",
                    "profile_revision":3,
                    "enrolled_declarations":[{
                        "kind":"node",
                        "type":"Person",
                        "lifecycle":"open",
                        "lifecycle_revision":2,
                        "pending":{"state":"exact","rows":1,"arrow_bytes":64,"batches":1}
                    }],
                    "token_counts":{"present":1,"withdrawn":0,"dead_lettered":0},
                    "recovery_pending_count":0,
                    "driver":{
                        "scope":"checked_runtime",
                        "authoritative":false,
                        "state":"running",
                        "pending_count":1,
                        "published_open_folds":0
                    },
                    "rebuild":{"ready":false,"blockers":[{"reason":"profile_not_terminal"}]}
                }"#,
            )
            .await;
        });

        let client = GraphClient::Remote {
            http: reqwest::Client::new(),
            base_url,
            token: Some("status-token".to_string()),
        };
        let output = client.stream_operational_status().await.unwrap();
        assert_eq!(output.manifest_version, 7);
        assert_eq!(output.profile_revision, 3);
        assert_eq!(output.enrolled_declarations.len(), 1);
        assert_eq!(
            output.enrolled_declarations[0].declaration.type_name,
            "Person"
        );
        assert_eq!(output.token_counts.present, 1);
        assert!(!output.rebuild.ready);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn graph_stream_controls_post_bodyless_aggregate_requests() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}/graphs/knowledge", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for (path, response) in [
                (
                    "/graphs/knowledge/stream/resume",
                    r#"{"profile_revision":4,"enrolled_declarations":3,"resumed_declarations":2,"already_open_declarations":1}"#,
                ),
                (
                    "/graphs/knowledge/stream/maintenance/ensure-indices",
                    r#"{"changed":true,"pending_index_count":0}"#,
                ),
                (
                    "/graphs/knowledge/stream/maintenance/optimize",
                    r#"{"changed":false,"pending_index_count":1,"requires_repair":false}"#,
                ),
            ] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut stream).await;
                assert!(
                    request.head.starts_with(&format!("POST {path} HTTP/1.1")),
                    "{}",
                    request.head
                );
                assert!(
                    request
                        .head
                        .to_ascii_lowercase()
                        .contains("authorization: bearer manage-token")
                );
                assert!(request.body.is_empty());
                write_response(
                    &mut stream,
                    "200 OK",
                    &[("Content-Type", "application/json")],
                    response.as_bytes(),
                )
                .await;
            }
        });

        let client = GraphClient::Remote {
            http: reqwest::Client::new(),
            base_url,
            token: Some("manage-token".to_string()),
        };
        let resumed = client.stream_resume().await.unwrap();
        assert_eq!(resumed.resumed_declarations, 2);
        assert_eq!(resumed.already_open_declarations, 1);
        let indexed = client.stream_ensure_indices().await.unwrap();
        assert!(indexed.changed);
        assert_eq!(indexed.pending_index_count, 0);
        let optimized = client.stream_optimize().await.unwrap();
        assert!(!optimized.changed);
        assert_eq!(optimized.pending_index_count, 1);
        assert!(!optimized.requires_repair);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn stream_commands_require_and_resolve_a_selected_graph_without_network_io() {
        let error = match GraphClient::resolve_stream_ingest(
            Some("http://127.0.0.1:9"),
            None,
            None,
            None,
            None,
        )
        .await
        {
            Ok(_) => panic!("stream ingest accepted a server without a selected graph"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("requires one selected graph"), "{error}");

        let client = GraphClient::resolve_stream_ingest(
            Some("http://127.0.0.1:9"),
            Some("knowledge"),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(client.uri(), "http://127.0.0.1:9/graphs/knowledge");

        let error =
            match GraphClient::resolve_stream_status(Some("http://127.0.0.1:9"), None, None, None)
                .await
            {
                Ok(_) => panic!("stream status accepted a server without a selected graph"),
                Err(error) => error.to_string(),
            };
        assert!(
            error.contains("`stream status` requires one selected graph"),
            "{error}"
        );

        let client = GraphClient::resolve_stream_status(
            Some("http://127.0.0.1:9"),
            Some("knowledge"),
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(client.uri(), "http://127.0.0.1:9/graphs/knowledge");

        let error = match GraphClient::resolve_stream_control(
            "stream resume",
            Some("http://127.0.0.1:9"),
            None,
            None,
            None,
        )
        .await
        {
            Ok(_) => panic!("stream resume accepted a server without a selected graph"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("`stream resume` requires one selected graph"),
            "{error}"
        );
    }
}
