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
use color_eyre::eyre::bail;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph_api_types::{
    BranchCreateOutput, BranchCreateRequest, BranchDeleteOutput, BranchListOutput,
    BranchMergeOutput, BranchMergeRequest, ChangeOutput, CommitListOutput, CommitOutput,
    ErrorOutput, ExportRequest, GraphListResponse, IngestOutput, IngestRequest, ReadOutput,
    ReadRequest, SchemaApplyOutput, SchemaApplyRequest, SchemaOutput, SnapshotOutput, commit_output,
    ingest_output, read_output, schema_apply_output, snapshot_payload,
};
use omnigraph_compiler::catalog::Catalog;
use reqwest::Method;
use serde_json::Value;

use crate::cli::CliLoadMode;
use crate::helpers::{
    ResolvedCliGraph, apply_bearer_token, apply_server_flag, build_http_client, is_remote_uri,
    legacy_change_request_body, open_local_db_with_policy, query_params_from_json,
    remote_json, remote_url, resolve_cli_actor, resolve_cli_graph, resolve_remote_bearer_token,
    select_named_query,
};
use crate::output::{LoadOutput, load_output_from_result, load_output_from_tables};
use omnigraph_server::config::OmnigraphConfig;

pub(crate) enum GraphClient {
    /// Local engine at `uri`. Reads (`resolve()`) leave `graph`/`actor`
    /// empty and open without policy; writes (`resolve_with_policy()`)
    /// fill them, opening through `open_local_db_with_policy` and
    /// attributing the resolved actor.
    Embedded {
        uri: String,
        graph: Option<ResolvedCliGraph>,
        actor: Option<String>,
    },
    /// Remote HTTP server. The actor is resolved server-side from the
    /// token; the client never sets identity.
    Remote {
        http: reqwest::Client,
        base_url: String,
        token: Option<String>,
    },
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
    /// Resolve the addressing (positional URI / `--target` / `--server`)
    /// and credential once, then pick the variant by URI scheme — the
    /// single branch point that replaces every per-command `is_remote`
    /// fork. Mirrors the read verbs' current preamble (`resolve_uri`
    /// path, not the policy-bearing `resolve_cli_graph`). Used by reads
    /// and `query` (which opens without policy, like the reads).
    pub(crate) fn resolve(
        config: &OmnigraphConfig,
        server: Option<&str>,
        graph: Option<&str>,
        uri: Option<String>,
        target: Option<&str>,
        profile: Option<&str>,
        store: Option<&str>,
    ) -> Result<Self> {
        // RFC-011: a scope (profile / --store / operator defaults) may stand in
        // for omitted addressing. The explicit branch passes server/graph/uri/
        // target straight through, so existing invocations are unchanged.
        let scope = crate::scope::resolve_scope(
            &crate::operator::load_operator_config()?,
            crate::planes::Capability::Any,
            crate::scope::ScopeFlags { profile, store, server, graph, uri, target },
        )?;
        let (server, graph, uri, target) = (
            scope.server.as_deref(),
            scope.graph.as_deref(),
            scope.uri,
            scope.target.as_deref(),
        );
        let via_server = server.is_some();
        let uri = apply_server_flag(server, graph, uri, target)?;
        let token = resolve_remote_bearer_token(config, uri.as_deref(), target)?;
        let uri = crate::helpers::resolve_uri(config, uri, target)?;
        reject_positional_remote(via_server, &uri)?;
        if is_remote_uri(&uri) {
            Ok(GraphClient::Remote {
                http: build_http_client()?,
                base_url: uri,
                token,
            })
        } else {
            Ok(GraphClient::Embedded {
                uri,
                graph: None,
                actor: None,
            })
        }
    }

    /// Write-path factory: the same addressing/credential resolution as
    /// `resolve()`, but through the stricter `resolve_cli_graph` (which
    /// carries `policy_file`/`graph_id`/`selected`), and with the actor
    /// resolved up front. The embedded arm then opens WITH policy. The
    /// resolution order matches the write arms exactly: server flag →
    /// bearer token → graph.
    pub(crate) fn resolve_with_policy(
        config: &OmnigraphConfig,
        server: Option<&str>,
        graph: Option<&str>,
        uri: Option<String>,
        target: Option<&str>,
        cli_as: Option<&str>,
        profile: Option<&str>,
        store: Option<&str>,
    ) -> Result<Self> {
        // RFC-011 scope translation (see `resolve`); explicit addressing passes
        // through unchanged.
        let scope = crate::scope::resolve_scope(
            &crate::operator::load_operator_config()?,
            crate::planes::Capability::Any,
            crate::scope::ScopeFlags { profile, store, server, graph, uri, target },
        )?;
        let (server, graph, uri, target) = (
            scope.server.as_deref(),
            scope.graph.as_deref(),
            scope.uri,
            scope.target.as_deref(),
        );
        let via_server = server.is_some();
        let uri = apply_server_flag(server, graph, uri, target)?;
        let token = resolve_remote_bearer_token(config, uri.as_deref(), target)?;
        let resolved = resolve_cli_graph(config, uri, target)?;
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
            let actor = resolve_cli_actor(cli_as, config)?;
            Ok(GraphClient::Embedded {
                uri: resolved.uri.clone(),
                graph: Some(resolved),
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

    /// The selected graph name, when a policy-bearing embedded client was
    /// resolved against a named graph. `None` for remote and for reads.
    pub(crate) fn selected(&self) -> Option<&str> {
        match self {
            GraphClient::Embedded { graph, .. } => graph.as_ref().and_then(ResolvedCliGraph::selected),
            GraphClient::Remote { .. } => None,
        }
    }

    pub(crate) fn is_remote(&self) -> bool {
        matches!(self, GraphClient::Remote { .. })
    }

    /// Open the local engine the way the resolved client demands: with
    /// policy when a `graph` context is present (write path), bare
    /// otherwise (read/`query` path). Captures today's two open paths in
    /// one place so each verb stays a single match arm.
    async fn open_embedded(uri: &str, graph: &Option<ResolvedCliGraph>) -> Result<Omnigraph> {
        match graph {
            Some(graph) => open_local_db_with_policy(graph).await,
            None => Ok(Omnigraph::open(uri).await?),
        }
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
                Ok(snapshot_payload(branch, &snapshot))
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
                Ok(load_output_from_tables(base_url, branch, mode.as_str(), &output))
            }
            GraphClient::Embedded { uri, graph, actor } => {
                let db = Self::open_embedded(uri, graph).await?;
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
            GraphClient::Embedded { uri, graph, actor } => {
                let db = Self::open_embedded(uri, graph).await?;
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
            GraphClient::Embedded { uri, graph, actor } => {
                let (selected_name, query_params) = select_named_query(query_source, query_name)?;
                let params = query_params_from_json(&query_params, params_json)?;
                let db = Self::open_embedded(uri, graph).await?;
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
            GraphClient::Embedded { uri, graph, .. } => {
                let (selected_name, query_params) = select_named_query(query_source, query_name)?;
                let params = query_params_from_json(&query_params, params_json)?;
                let db = Self::open_embedded(uri, graph).await?;
                let result = db
                    .query(target.clone(), query_source, &selected_name, &params)
                    .await?;
                Ok(read_output(selected_name, &target, result))
            }
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
            GraphClient::Embedded { uri, graph, actor } => {
                let db = Self::open_embedded(uri, graph).await?;
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
            GraphClient::Embedded { uri, graph, actor } => {
                let db = Self::open_embedded(uri, graph).await?;
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

    pub(crate) async fn branch_merge(&self, source: &str, into: &str) -> Result<BranchMergeOutput> {
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
                    })?),
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, graph, actor } => {
                let db = Self::open_embedded(uri, graph).await?;
                let actor = actor.as_deref();
                let outcome = db.branch_merge_as(source, into, actor).await?;
                Ok(BranchMergeOutput {
                    source: source.to_string(),
                    target: into.to_string(),
                    outcome: outcome.into(),
                    actor_id: actor.map(String::from),
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
            GraphClient::Embedded { uri, graph, actor } => {
                let db = Self::open_embedded(uri, graph).await?;
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

    /// `graphs list` — enumerate the graphs a remote multi-graph server
    /// serves (`GET /graphs`). Remote-only by design: there is no local
    /// enumeration endpoint, so the Embedded arm fails loudly pointing the
    /// operator at `omnigraph.yaml`. Routing it through the enum still buys
    /// the shared `resolve()` addressing/token preamble.
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
                "`omnigraph graphs list` requires a remote multi-graph server URL \
                 (http:// or https://). To enumerate local graphs, read `omnigraph.yaml` \
                 directly."
            ),
        }
    }
}
