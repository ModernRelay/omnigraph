//! `GraphClient` — the one place the embedded-vs-remote split lives
//! (RFC-009 Phase 3). A CLI command body calls a verb method; the
//! enum routes to the engine (local URI) or HTTP (remote URI). The
//! 15 per-command `if graph.is_remote { … } else { … }` forks collapse
//! into two arms here.
//!
//! Phase 3a scope: the factory + the uniform read verbs (snapshot,
//! schema show, branch list, commit list/show — all of which open the
//! local engine WITHOUT policy today, preserved exactly). Write verbs
//! and the policy-bearing `query`/`mutate` arrive in 3b (the Embedded
//! variant will grow the policy context then); export + graphs-list in
//! 3c. Behavior is unchanged per verb — the Phase-1 parity matrix is the
//! referee and stays textually unchanged.
//!
//! Enum, not a trait (RFC sketch said "trait"): only two variants ever,
//! and inherent async methods sidestep `async_trait` boxing plus the
//! `apply_schema` catalog-validator closure that is not object-safe.
//! Same one-body-two-impls collapse, less ceremony.

use reqwest::Method;
use color_eyre::Result;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph_api_types::{
    BranchListOutput, CommitListOutput, CommitOutput, SchemaOutput, SnapshotOutput, commit_output,
    snapshot_payload,
};

use crate::helpers::{
    apply_server_flag, build_http_client, is_remote_uri, remote_json, remote_url,
    resolve_remote_bearer_token,
};
use omnigraph_server::config::OmnigraphConfig;

pub(crate) enum GraphClient {
    /// Local engine at `uri`. Reads open the dataset per call (no policy
    /// attached — matches today's read behavior; the write verbs in 3b
    /// add a policy-bearing context).
    Embedded { uri: String },
    /// Remote HTTP server. The actor is resolved server-side from the
    /// token; the client never sets identity.
    Remote {
        http: reqwest::Client,
        base_url: String,
        token: Option<String>,
    },
}

impl GraphClient {
    /// Resolve the addressing (positional URI / `--target` / `--server`)
    /// and credential once, then pick the variant by URI scheme — the
    /// single branch point that replaces every per-command `is_remote`
    /// fork. Mirrors the read verbs' current preamble (`resolve_uri`
    /// path, not the policy-bearing `resolve_cli_graph`).
    pub(crate) fn resolve(
        config: &OmnigraphConfig,
        server: Option<&str>,
        graph: Option<&str>,
        uri: Option<String>,
        target: Option<&str>,
    ) -> Result<Self> {
        let uri = apply_server_flag(server, graph, uri, target)?;
        let token = resolve_remote_bearer_token(config, uri.as_deref(), target)?;
        let uri = crate::helpers::resolve_uri(config, uri, target)?;
        if is_remote_uri(&uri) {
            Ok(GraphClient::Remote {
                http: build_http_client()?,
                base_url: uri,
                token,
            })
        } else {
            Ok(GraphClient::Embedded { uri })
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
                    remote_url(base_url, "/branches"),
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri } => {
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
                    format!("{}?branch={}", remote_url(base_url, "/snapshot"), branch),
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri } => {
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
                    remote_url(base_url, "/schema"),
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri } => {
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
                    Some(branch) => format!("{}?branch={}", remote_url(base_url, "/commits"), branch),
                    None => remote_url(base_url, "/commits"),
                };
                remote_json(http, Method::GET, url, None, token.as_deref()).await
            }
            GraphClient::Embedded { uri } => {
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
                    remote_url(base_url, &format!("/commits/{commit_id}")),
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri } => {
                let db = Omnigraph::open(uri).await?;
                Ok(commit_output(&db.get_commit(commit_id).await?))
            }
        }
    }
}
