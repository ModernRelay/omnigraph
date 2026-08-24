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

use color_eyre::Result;
use color_eyre::eyre::{bail, eyre};
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::{BLOB_READ_RANGE_MAX_BYTES, BlobContent};
use omnigraph_api_types::{
    BlobReadQuery, BlobStatOutput, BranchCreateOutput, BranchCreateRequest, BranchDeleteOutput,
    BranchListOutput, BranchMergeOutput, BranchMergeRequest, ChangeBaselineOutput,
    ChangeBaselineRecord, ChangeBaselineRequest, ChangeFeedOutput, ChangeOpOutput, ChangeOutput,
    ChangeRequest, CommitChangesOutput, CommitListOutput, CommitOutput, EntityKindOutput,
    ErrorOutput, ExportRequest, GraphBatchLoadOutput, GraphListResponse, IngestOutput,
    IngestRequest, InvokeStoredQueryRequest, QueryRequest, ReadOutput, SchemaApplyOutput,
    SchemaApplyRequest, SchemaOutput, SnapshotOutput, change_baseline_output, change_feed_output,
    change_scope, commit_changes_output, commit_output, ingest_receipt_output, read_output,
    schema_apply_output, snapshot_payload,
};
use omnigraph_compiler::catalog::Catalog;
use reqwest::header::{CONTENT_RANGE, RANGE};
use reqwest::{Method, StatusCode};
use serde_json::Value;

use crate::blob_cli::{
    BlobRangeRequest, blob_cell, blob_read_target, blob_url, external_response_headers,
    managed_response_headers, map_embedded_blob_error, remote_blob_error, whole_external_uri,
};
use crate::cli::CliLoadMode;
use crate::helpers::{
    apply_bearer_token, apply_server_flag, build_blob_http_client, build_http_client,
    is_remote_uri, legacy_change_request_body, precondition_failed_cli, query_params_from_json,
    remote_json, remote_json_with_graph_commit_precondition, remote_url, resolve_cli_actor,
    resolve_cli_graph, resolve_remote_bearer_token, resolve_server_flag, select_named_query,
};
use crate::output::{LoadOutput, load_output_from_graph_batch, load_output_from_receipt};

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
                snapshot_payload(branch, &snapshot, internal_schema_version)
                    .map_err(|error| eyre!(error))
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
                    system_columns: Some(db.catalog().system_columns.into()),
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

    /// Fetch one bounded page of a commit's entity diff. Auto-pagination is
    /// deliberately owned by the command output loop, which emits each page
    /// before fetching the next one instead of rebuilding an unbounded result.
    pub(crate) async fn commit_changes_page(
        &self,
        commit_id: &str,
        page_token: Option<&str>,
        limit: Option<usize>,
        filter: &ChangeFilterArgs<'_>,
    ) -> Result<CommitChangesOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                let limit_value = limit.map(|limit| limit.to_string());
                let mut query = Vec::new();
                if let Some(page_token) = page_token {
                    query.push(("page_token", page_token));
                }
                if let Some(limit) = limit_value.as_deref() {
                    query.push(("limit", limit));
                }
                let filter_pairs = filter.query_pairs();
                query.extend(
                    filter_pairs
                        .iter()
                        .map(|(name, value)| (*name, value.as_str())),
                );
                remote_json(
                    http,
                    Method::GET,
                    remote_url(base_url, &["commits", commit_id, "changes"], &query)?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let db = Omnigraph::open(uri).await?;
                let scope = change_scope(filter.kinds, filter.types, filter.ops);
                let page = db
                    .commit_changes_page(commit_id, &scope, page_token, limit, None)
                    .await?;
                Ok(commit_changes_output(&page))
            }
        }
    }

    /// Fetch one bounded page of a captured feed poll. The caller continues
    /// with `next_page_token`; this method never aggregates pages in memory.
    pub(crate) async fn poll_changes_page(
        &self,
        branch: Option<&str>,
        cursor: Option<&str>,
        start: Option<&str>,
        page_token: Option<&str>,
        limit: Option<usize>,
        filter: &ChangeFilterArgs<'_>,
    ) -> Result<ChangeFeedOutput> {
        // A page token continues one poll and supersedes the start position.
        let (cursor, start) = if page_token.is_some() {
            (None, None)
        } else {
            (cursor, start)
        };
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                let limit_value = limit.map(|limit| limit.to_string());
                let mut query = Vec::new();
                if let Some(branch) = branch {
                    query.push(("branch", branch));
                }
                if let Some(cursor) = cursor {
                    query.push(("cursor", cursor));
                }
                if let Some(start) = start {
                    query.push(("start", start));
                }
                if let Some(page_token) = page_token {
                    query.push(("page_token", page_token));
                }
                if let Some(limit) = limit_value.as_deref() {
                    query.push(("limit", limit));
                }
                let filter_pairs = filter.query_pairs();
                query.extend(
                    filter_pairs
                        .iter()
                        .map(|(name, value)| (*name, value.as_str())),
                );
                remote_json(
                    http,
                    Method::GET,
                    remote_url(base_url, &["changes"], &query)?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let db = Omnigraph::open(uri).await?;
                let position = if let Some(token) = page_token {
                    omnigraph::changes::ChangeFeedPosition::PageToken(token.to_string())
                } else if let Some(cursor) = cursor {
                    omnigraph::changes::ChangeFeedPosition::Cursor(cursor.to_string())
                } else {
                    omnigraph::changes::ChangeFeedPosition::Start(parse_change_feed_start(
                        start.unwrap_or("now"),
                    )?)
                };
                let page = db
                    .poll_change_feed(omnigraph::changes::ChangeFeedRequest {
                        branch: branch.map(str::to_string),
                        position,
                        scope: change_scope(filter.kinds, filter.types, filter.ops),
                        max_changes: limit,
                        max_bytes: None,
                        max_commits: None,
                    })
                    .await?;
                Ok(change_feed_output(&page))
            }
        }
    }

    /// Capture a change baseline: stream the snapshot records into `writer`
    /// and return the terminal handshake. The terminal record itself is NOT
    /// written to `writer` — a stream that ends without one is an error, so a
    /// usable cursor never outlives a broken snapshot.
    pub(crate) async fn change_baseline<W: Write>(
        &self,
        branch: Option<&str>,
        filter: &ChangeFilterArgs<'_>,
        writer: &mut W,
    ) -> Result<ChangeBaselineOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                let request = apply_bearer_token(
                    http.request(
                        Method::POST,
                        remote_url(base_url, &["changes", "baseline"], &[])?,
                    ),
                    token.as_deref(),
                )
                .json(&ChangeBaselineRequest {
                    branch: branch.map(str::to_string),
                    kind: filter.kinds.to_vec(),
                    r#type: filter.types.to_vec(),
                    op: filter.ops.to_vec(),
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
                // Hold back the most recent complete line while streaming: at
                // EOF it must be the terminal handshake record. Everything
                // before it is snapshot data.
                let mut pending: Vec<u8> = Vec::new();
                let mut held: Option<Vec<u8>> = None;
                while let Some(chunk) = response.chunk().await? {
                    pending.extend_from_slice(&chunk);
                    while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
                        let mut line: Vec<u8> = pending.drain(..=newline).collect();
                        line.pop();
                        if let Some(previous) = held.replace(line) {
                            writer.write_all(&previous)?;
                            writer.write_all(b"\n")?;
                        }
                    }
                }
                writer.flush()?;
                if !pending.is_empty() {
                    bail!("baseline stream ended mid-record — no usable cursor");
                }
                let terminal =
                    held.ok_or_else(|| eyre!("baseline stream carried no terminal record"))?;
                let record: ChangeBaselineRecord =
                    serde_json::from_slice(&terminal).map_err(|_| {
                        eyre!("baseline stream ended without a terminal record — no usable cursor")
                    })?;
                Ok(record.baseline)
            }
            GraphClient::Embedded { uri, .. } => {
                let db = Omnigraph::open(uri).await?;
                let scope = change_scope(filter.kinds, filter.types, filter.ops);
                let baseline = db
                    .capture_change_baseline(branch.unwrap_or("main"), &scope, writer)
                    .await?;
                writer.flush()?;
                Ok(change_baseline_output(&baseline))
            }
        }
    }

    /// `load` — bulk-load `data` (a file path) onto `branch`, forking from
    /// `from` if missing. Returns the CLI `LoadOutput`; each arm keeps its
    /// own mapping (remote uses the logical graph-batch result, embedded reads
    /// the engine `LoadResult` directly).
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
                let mut query = vec![("branch", branch), ("mode", mode.as_str())];
                if let Some(from) = from {
                    query.push(("from", from));
                }
                let request = apply_bearer_token(
                    http.request(
                        Method::POST,
                        remote_url(base_url, &["load", "ndjson"], &query)?,
                    ),
                    token.as_deref(),
                )
                .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
                .body(data);
                let response = request.send().await?;
                let status = response.status();
                let text = response.text().await?;
                if !status.is_success() {
                    if let Ok(error) = serde_json::from_str::<ErrorOutput>(&text) {
                        bail!(error.error);
                    }
                    bail!("server returned {}: {}", status, text);
                }
                let output: GraphBatchLoadOutput = serde_json::from_str(&text)?;
                Ok(load_output_from_graph_batch(
                    base_url,
                    mode.as_str(),
                    &output,
                ))
            }
            GraphClient::Embedded { uri, actor } => {
                let db = Self::open_embedded(uri).await?;
                let data = std::fs::read_to_string(data)?;
                let receipt = db
                    .load_graph_batch_as_with_receipt(
                        branch,
                        from,
                        &data,
                        mode.into(),
                        actor.as_deref(),
                    )
                    .await?;
                Ok(load_output_from_receipt(
                    uri,
                    branch,
                    mode.as_str(),
                    &receipt,
                ))
            }
        }
    }

    /// `ingest` — the deprecated loader-compatible path. Unlike canonical
    /// `load`, it retains the historical permissive parser and `/ingest`
    /// endpoint. The embedded arm echoes `actor_id: None` in the output
    /// exactly as the legacy arm did (the actor is still attributed on the
    /// commit via `load_file_as_with_receipt`).
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
                let receipt = db
                    .load_file_as_with_receipt(
                        branch,
                        Some(from),
                        data,
                        mode.into(),
                        actor.as_deref(),
                    )
                    .await?;
                Ok(ingest_receipt_output(uri, &receipt, mode.into(), None))
            }
        }
    }

    /// `mutate` — run a change query against `branch`. Folds
    /// `execute_change` / `execute_change_remote` + the legacy request body.
    ///
    /// `expected_head` is the `--if-commit` compare-and-swap precondition:
    /// the write runs only if the branch head commit still equals it. A
    /// mismatch surfaces as the typed [`PreconditionFailedCli`] on both
    /// transports so the verb can exit with `EXIT_PRECONDITION_FAILED` (4).
    pub(crate) async fn mutate(
        &self,
        branch: &str,
        query_source: &str,
        query_name: Option<&str>,
        params_json: Option<&Value>,
        expected_head: Option<&str>,
    ) -> Result<ChangeOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
            } => {
                let (url, body) = if expected_head.is_some() {
                    (
                        remote_url(base_url, &["mutate", "if-graph-commit"], &[])?,
                        serde_json::to_value(ChangeRequest {
                            query: query_source.to_string(),
                            name: query_name.map(ToOwned::to_owned),
                            params: params_json.cloned(),
                            branch: Some(branch.to_string()),
                        })?,
                    )
                } else {
                    (
                        remote_url(base_url, &["change"], &[])?,
                        legacy_change_request_body(query_source, query_name, branch, params_json),
                    )
                };
                remote_json_with_graph_commit_precondition(
                    http,
                    Method::POST,
                    url,
                    Some(body),
                    token.as_deref(),
                    expected_head,
                )
                .await
            }
            GraphClient::Embedded { uri, actor } => {
                let (selected_name, query_params) = select_named_query(query_source, query_name)?;
                let params = query_params_from_json(&query_params, params_json)?;
                let db = Self::open_embedded(uri).await?;
                let actor = actor.as_deref();
                let receipt = db
                    .mutate_as_with_expected_head_receipt(
                        branch,
                        query_source,
                        &selected_name,
                        &params,
                        actor,
                        expected_head,
                    )
                    .await
                    .map_err(|err| {
                        let message = err.to_string();
                        match err {
                            omnigraph::error::OmniError::PreconditionFailed {
                                branch: _,
                                expected,
                                actual,
                            } => precondition_failed_cli(message, expected, actual).into(),
                            other => color_eyre::eyre::Report::from(other),
                        }
                    })?;
                Ok(ChangeOutput {
                    branch: branch.to_string(),
                    query_name: selected_name,
                    affected_nodes: receipt.result.affected_nodes,
                    affected_edges: receipt.result.affected_edges,
                    actor_id: actor.map(String::from),
                    commit: receipt.commit.as_ref().map(commit_output),
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
                    remote_url(base_url, &["query"], &[])?,
                    Some(serde_json::to_value(QueryRequest {
                        query: query_source.to_string(),
                        name: query_name.map(ToOwned::to_owned),
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
                let (result, graph_commit_id) = db
                    .query_with_head(target.clone(), query_source, &selected_name, &params)
                    .await?;
                Ok(read_output(selected_name, &target, result, graph_commit_id))
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
        expected_head: Option<&str>,
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
                remote_json_with_graph_commit_precondition(
                    http,
                    Method::POST,
                    if expected_head.is_some() {
                        remote_url(base_url, &["queries", name, "if-graph-commit"], &[])?
                    } else {
                        remote_url(base_url, &["queries", name], &[])?
                    },
                    Some(serde_json::to_value(body)?),
                    token.as_deref(),
                    expected_head,
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
                db.export_jsonl_to_writer(branch, type_names, writer)
                    .await?;
                writer.flush()?;
                Ok(())
            }
        }
    }

    /// Stream one managed Blob without buffering the whole value. External
    /// descriptors are reported but never followed or dereferenced.
    pub(crate) async fn blob_get<W: Write + ?Sized>(
        &self,
        query: &BlobReadQuery,
        range: Option<BlobRangeRequest>,
        writer: &mut W,
    ) -> Result<()> {
        match self {
            GraphClient::Embedded { uri, .. } => {
                let db = Self::open_embedded(uri).await?;
                let read = db
                    .read_blob_at(blob_read_target(query), blob_cell(query))
                    .await
                    .map_err(map_embedded_blob_error)?;
                match read.content {
                    BlobContent::Managed { length, reader, .. } => {
                        let selected = match range {
                            Some(range) => range.resolve(length)?,
                            None => 0..length,
                        };
                        let mut cursor = selected.start;
                        while cursor < selected.end {
                            let end = selected
                                .end
                                .min(cursor.saturating_add(BLOB_READ_RANGE_MAX_BYTES));
                            let bytes = reader
                                .read_range(cursor..end)
                                .await
                                .map_err(map_embedded_blob_error)?;
                            let expected = usize::try_from(end - cursor)
                                .map_err(|_| color_eyre::eyre::eyre!("Blob delivery failed"))?;
                            if bytes.len() != expected {
                                bail!("Blob delivery failed: managed range length mismatch");
                            }
                            writer.write_all(&bytes)?;
                            cursor = end;
                        }
                        writer.flush()?;
                        Ok(())
                    }
                    BlobContent::External(reference) => {
                        let uri = whole_external_uri(&reference)?;
                        bail!(
                            "external Blob is not downloaded; URI: {uri}; use `blob stat --json` \
                             to inspect the descriptor"
                        )
                    }
                }
            }
            GraphClient::Remote {
                base_url, token, ..
            } => {
                let http = build_blob_http_client()?;
                let mut request = apply_bearer_token(
                    http.request(Method::GET, blob_url(base_url, query)?),
                    token.as_deref(),
                );
                if let Some(range) = range {
                    request = request.header(RANGE, range.header_value());
                }
                let mut response = request
                    .send()
                    .await
                    .map_err(|_| color_eyre::eyre::eyre!("Blob server request failed"))?;
                let status = response.status();
                if status == StatusCode::FOUND {
                    let (uri, _snapshot_id) = external_response_headers(response.headers())?;
                    bail!(
                        "external Blob is not downloaded; URI: {uri}; use `blob stat --json` \
                         to inspect the descriptor"
                    );
                }
                let expected_status = if range.is_some() {
                    StatusCode::PARTIAL_CONTENT
                } else {
                    StatusCode::OK
                };
                if status != expected_status {
                    return Err(remote_blob_error(status));
                }
                let headers = managed_response_headers(response.headers())?;
                if let Some(range) = range {
                    validate_content_range(response.headers(), range, headers.length)?;
                }
                let mut written = 0_u64;
                while let Some(chunk) = response
                    .chunk()
                    .await
                    .map_err(|_| color_eyre::eyre::eyre!("Blob delivery stream failed"))?
                {
                    writer.write_all(&chunk)?;
                    written = written
                        .checked_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX))
                        .ok_or_else(|| color_eyre::eyre::eyre!("Blob delivery failed"))?;
                }
                if written != headers.length {
                    bail!(
                        "Blob delivery stream ended after {written} bytes; expected {}",
                        headers.length
                    );
                }
                writer.flush()?;
                Ok(())
            }
        }
    }

    /// Inspect one Blob descriptor. Neither arm reads managed payload bytes;
    /// external references are classified without probing their target.
    pub(crate) async fn blob_stat(&self, query: &BlobReadQuery) -> Result<BlobStatOutput> {
        match self {
            GraphClient::Embedded { uri, .. } => {
                let db = Self::open_embedded(uri).await?;
                let read = db
                    .read_blob_at(blob_read_target(query), blob_cell(query))
                    .await
                    .map_err(map_embedded_blob_error)?;
                let resolved_snapshot = read.resolved_target.snapshot_id.to_string();
                match read.content {
                    BlobContent::Managed { length, etag, .. } => Ok(BlobStatOutput::managed(
                        query,
                        resolved_snapshot,
                        length,
                        etag.into_string(),
                    )),
                    BlobContent::External(reference) => Ok(BlobStatOutput::external(
                        query,
                        resolved_snapshot,
                        whole_external_uri(&reference)?.to_string(),
                    )),
                }
            }
            GraphClient::Remote {
                base_url, token, ..
            } => {
                let http = build_blob_http_client()?;
                let response = apply_bearer_token(
                    http.request(Method::HEAD, blob_url(base_url, query)?),
                    token.as_deref(),
                )
                .send()
                .await
                .map_err(|_| color_eyre::eyre::eyre!("Blob server request failed"))?;
                match response.status() {
                    StatusCode::OK => {
                        let headers = managed_response_headers(response.headers())?;
                        Ok(BlobStatOutput::managed(
                            query,
                            headers.snapshot_id,
                            headers.length,
                            headers.etag,
                        ))
                    }
                    StatusCode::FOUND => {
                        let (uri, snapshot_id) = external_response_headers(response.headers())?;
                        Ok(BlobStatOutput::external(query, snapshot_id, uri))
                    }
                    status => Err(remote_blob_error(status)),
                }
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

fn validate_content_range(
    headers: &reqwest::header::HeaderMap,
    requested: BlobRangeRequest,
    served_length: u64,
) -> Result<()> {
    let raw = headers
        .get(CONTENT_RANGE)
        .ok_or_else(|| color_eyre::eyre::eyre!("Blob server response omitted Content-Range"))?
        .to_str()
        .map_err(|_| color_eyre::eyre::eyre!("Blob server returned an invalid Content-Range"))?;
    let Some(spec) = raw.strip_prefix("bytes ") else {
        bail!("Blob server returned an invalid Content-Range");
    };
    let Some((bounds, total)) = spec.split_once('/') else {
        bail!("Blob server returned an invalid Content-Range");
    };
    let Some((start, end)) = bounds.split_once('-') else {
        bail!("Blob server returned an invalid Content-Range");
    };
    let start = start
        .parse::<u64>()
        .map_err(|_| color_eyre::eyre::eyre!("Blob server returned an invalid Content-Range"))?;
    let end = end
        .parse::<u64>()
        .map_err(|_| color_eyre::eyre::eyre!("Blob server returned an invalid Content-Range"))?;
    let total = total
        .parse::<u64>()
        .map_err(|_| color_eyre::eyre::eyre!("Blob server returned an invalid Content-Range"))?;
    let actual = end
        .checked_sub(start)
        .and_then(|length| length.checked_add(1))
        .ok_or_else(|| color_eyre::eyre::eyre!("Blob server returned an invalid Content-Range"))?;
    let available = total.checked_sub(requested.start()).ok_or_else(|| {
        color_eyre::eyre::eyre!("Blob server returned an inconsistent Content-Range")
    })?;
    let expected = requested
        .requested_length()
        .map_or(available, |length| length.min(available));
    if start != requested.start() || actual != served_length || actual != expected || end >= total {
        bail!("Blob server returned an inconsistent Content-Range");
    }
    Ok(())
}

/// Shared spelling of the change-surface filters across the three verbs; one
/// translation (`change_scope`) is used by the embedded arm and the server.
pub(crate) struct ChangeFilterArgs<'a> {
    pub kinds: &'a [EntityKindOutput],
    pub types: &'a [String],
    pub ops: &'a [ChangeOpOutput],
}

impl ChangeFilterArgs<'_> {
    fn query_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::new();
        for kind in self.kinds {
            pairs.push(("kind", kind.as_str().to_string()));
        }
        for type_name in self.types {
            pairs.push(("type", type_name.clone()));
        }
        for op in self.ops {
            pairs.push(("op", op.as_str().to_string()));
        }
        pairs
    }
}

/// The embedded arm's twin of the served start-mode parser.
fn parse_change_feed_start(start: &str) -> Result<omnigraph::changes::ChangeFeedStart> {
    match start {
        "now" => Ok(omnigraph::changes::ChangeFeedStart::Now),
        "beginning" => Ok(omnigraph::changes::ChangeFeedStart::Beginning),
        other => other
            .strip_prefix("after:")
            .filter(|commit_id| !commit_id.is_empty())
            .map(|commit_id| {
                omnigraph::changes::ChangeFeedStart::AfterCommit(commit_id.to_string())
            })
            .ok_or_else(|| eyre!("start must be now | beginning | after:<commit_id>")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn content_range_headers(value: &'static str) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(CONTENT_RANGE, value.parse().unwrap());
        headers
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

    #[test]
    fn content_range_must_cover_the_exact_requested_or_eof_clamped_length() {
        let exact = BlobRangeRequest::new(Some(0), Some(6)).unwrap().unwrap();
        validate_content_range(&content_range_headers("bytes 0-5/100"), exact, 6).unwrap();
        assert!(
            validate_content_range(&content_range_headers("bytes 0-2/100"), exact, 3).is_err(),
            "a self-consistent but truncated 206 must not be accepted as the requested range"
        );

        let clamped = BlobRangeRequest::new(Some(98), Some(6)).unwrap().unwrap();
        validate_content_range(&content_range_headers("bytes 98-99/100"), clamped, 2).unwrap();

        let open = BlobRangeRequest::new(Some(97), None).unwrap().unwrap();
        validate_content_range(&content_range_headers("bytes 97-99/100"), open, 3).unwrap();
    }
}
