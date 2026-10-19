//! Cached data credentials have a separate keychain namespace and transport.
use super::auth::{self, Store};
use super::{Api, Context, Failure, Method, Output, Result, canonical_origin, json};
use crate::cli::{Cli, Command, CommitCommand, GraphsCommand};
use crate::client::GraphClient;
use crate::commands::transfer::LoadArgs;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const MAX_CREDENTIAL: usize = 64 * 1024;
const MAX_TOKEN: usize = 8192;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Credential {
    version: u8,
    api: String,
    cluster_id: String,
    endpoint: String,
    token: String,
    expires_at: String,
    kid: String,
    actor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cluster_incarnation: Option<String>,
}

/// Parsed only to reject mismatched issuance/cache metadata, never to select
/// configuration or grant authority. The server still verifies the signature.
fn parse_identity_claims(
    token: &str,
) -> Option<omnigraph_server::data_tokens::IdentityTokenClaims> {
    if token.len() > MAX_TOKEN {
        return None;
    }
    let mut parts = token.split('.');
    let _header = parts.next()?;
    let claims = parts.next()?;
    let _signature = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let claims: omnigraph_server::data_tokens::IdentityTokenClaims =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(claims).ok()?).ok()?;
    (claims.version == 2).then_some(claims)
}

fn key(context: &Context) -> String {
    format!(
        "{}/organizations/{}/clusters/{}",
        context.api, context.organization_id, context.cluster
    )
}

fn invalid() -> Failure {
    Failure::refused(
        "data_credential_invalid",
        "the cached data credential is invalid; run cluster token --target NAME",
    )
}

fn graph_id(graph: &str) -> Result<()> {
    // The server's graph selector is a single path segment, never path syntax.
    if graph.is_empty()
        || graph.len() > 64
        || !graph.as_bytes()[0].is_ascii_alphabetic()
        || !graph
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-')
        || matches!(graph, "policies" | "healthz" | "openapi" | "graphs")
    {
        return Err(Failure::refused(
            "graph_invalid",
            "--graph must select one valid graph id",
        ));
    }
    Ok(())
}

impl Credential {
    fn validate(&self, context: &Context) -> Result<()> {
        let now = OffsetDateTime::now_utc();
        let expires = OffsetDateTime::parse(&self.expires_at, &Rfc3339).map_err(|_| invalid())?;
        if self.version != 2
            || self.api != context.api
            || self.cluster_id != context.cluster
            || !canonical_origin(&self.endpoint).is_ok_and(|o| o == self.endpoint)
            || self.token.is_empty()
            || self.token.len() > MAX_TOKEN
            || self.token.split('.').count() != 3
            || self.token.split('.').any(str::is_empty)
            || !self
                .token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            || self.kid.len() != 64
            || !self
                .kid
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || !self.actor.starts_with("principal:")
            || self.actor.len() == "principal:".len()
            || self.actor.len() > 1024
            || self
                .actor
                .chars()
                .any(|c| c.is_control() || c.is_whitespace())
            || expires > now + time::Duration::seconds(86430)
        {
            return Err(invalid());
        }
        let claims = parse_identity_claims(&self.token).ok_or_else(invalid)?;
        let header = self.token.split('.').next().ok_or_else(invalid)?;
        let header: omnigraph_server::data_tokens::DataTokenHeader =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(header).map_err(|_| invalid())?)
                .map_err(|_| invalid())?;
        if claims.iss != self.api
            || claims.account_id != context.organization_id
            || claims.cluster_id != self.cluster_id
            || self.cluster_incarnation.as_deref() != Some(claims.cluster_incarnation.as_str())
            || claims.cluster_incarnation != context.incarnation
            || claims.aud != format!("urn:omnigraph:data:{}", self.cluster_id)
            || self.actor != format!("principal:{}", claims.sub)
            || i64::try_from(claims.exp).ok() != Some(expires.unix_timestamp())
            || header.kid != self.kid
            || header.typ != "JWT"
            || header.alg != "ES256"
            || !claims
                .exp
                .checked_sub(claims.iat)
                .is_some_and(|ttl| (60..=86400).contains(&ttl))
            || claims.iat
                > u64::try_from(now.unix_timestamp())
                    .unwrap_or_default()
                    .saturating_add(30)
        {
            return Err(invalid());
        }
        if context
            .endpoint
            .as_ref()
            .is_some_and(|endpoint| endpoint != &self.endpoint)
        {
            let mut failure = Failure::refused(
                "data_endpoint_mismatch",
                "the issued data endpoint differs from the selected target; update the target explicitly before acquiring a replacement credential",
            );
            failure.body["endpoint"] = json!(self.endpoint);
            return Err(failure);
        }
        if expires <= now {
            return Err(Failure::refused(
                "data_credential_expired",
                "the data credential has expired; run cluster token --target NAME",
            ));
        }
        Ok(())
    }

    fn metadata(&self) -> Value {
        let mut metadata = json!({"cluster_id":self.cluster_id,"endpoint":self.endpoint,"expires_at":self.expires_at,"kid":self.kid,"actor":self.actor});
        metadata["version"] = json!(2);
        metadata
    }
}

pub(crate) fn parse_ttl(input: &str) -> std::result::Result<u64, String> {
    let (number, multiplier) = match input.as_bytes().last() {
        Some(b's') => (&input[..input.len() - 1], 1),
        Some(b'm') => (&input[..input.len() - 1], 60),
        Some(b'h') => (&input[..input.len() - 1], 3600),
        Some(b'd') => (&input[..input.len() - 1], 86400),
        _ => (input, 1),
    };
    number
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(multiplier))
        .filter(|n| (60..=86400).contains(n))
        .ok_or_else(|| {
            "TTL must be 60–86400 seconds, optionally suffixed s, m, h, or d".to_string()
        })
}

fn scope(cli: &Cli) -> Result<()> {
    if cli.as_actor.is_some() {
        return Err(Failure::refused(
            "managed_scope_conflict",
            "managed data identity is derived from its credential; --as does not apply",
        ));
    }
    Ok(())
}

async fn mint(store: &impl Store, context: &Context, api: &Api, ttl: u64) -> Result<Value> {
    mint_for_principal(store, context, api, ttl, None).await
}

async fn mint_for_principal(
    store: &impl Store,
    context: &Context,
    api: &Api,
    ttl: u64,
    expected_principal: Option<&str>,
) -> Result<Value> {
    // Fail early if the platform cannot access its credential store.
    let _ = store.get(&key(context))?;
    let body = api
        .request(
            Method::POST,
            &format!("/v1/clusters/{}/tokens", context.cluster),
            Some(&json!({"version":2,"ttl_seconds":ttl})),
            None,
        )
        .await?;
    context.verify(&body)?;
    let data = &body["data"];
    if data["version"] != 2
        || ["grants", "roles", "actions", "groups", "policy"]
            .iter()
            .any(|field| data.get(field).is_some())
    {
        return Err(Failure::protocol());
    }
    let string = |field| {
        data.get(field)
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(Failure::protocol)
    };
    let credential = Credential {
        version: 2,
        api: context.api.clone(),
        cluster_id: context.cluster.clone(),
        endpoint: string("endpoint")?,
        token: string("token")?,
        expires_at: string("expires_at")?,
        kid: string("kid")?,
        actor: string("actor")?,
        cluster_incarnation: Some(
            body["meta"]["incarnation"]
                .as_str()
                .ok_or_else(Failure::protocol)?
                .to_owned(),
        ),
    };
    credential.validate(context)?;
    if expected_principal.is_some_and(|id| credential.actor != format!("principal:{id}")) {
        return Err(Failure::protocol());
    }
    if OffsetDateTime::parse(&credential.expires_at, &Rfc3339).map_err(|_| invalid())?
        > OffsetDateTime::now_utc() + time::Duration::seconds(ttl as i64 + 30)
    {
        return Err(Failure::protocol());
    }
    let saved = serde_json::to_string(&credential).map_err(|_| invalid())?;
    if saved.len() > MAX_CREDENTIAL {
        return Err(invalid());
    }
    store.put(&key(context), &saved)?;
    // Construct output from a strict metadata allowlist; never echo provider/API extras.
    let mut metadata = credential.metadata();
    auth::scrub_value(&mut metadata, &credential.token);
    Ok(json!({"data":metadata,"meta":{"cluster_id":context.cluster}}))
}

fn clear(store: &impl Store, context: &Context) -> Result<Value> {
    store.remove(&key(context))?;
    Ok(
        json!({"data":{"cluster_id":context.cluster,"local_credential_removed":true,"revocation_performed":false},"meta":{"cluster_id":context.cluster}}),
    )
}

pub(super) fn token_scope(cli: &Cli, ttl: Option<u64>, clear: bool) -> Result<()> {
    scope(cli)?;
    if clear && (cli.graph.is_some() || ttl.is_some()) {
        return Err(Failure::refused(
            "token_clear_conflict",
            "--clear forgets the whole cached cluster credential and cannot select a graph or TTL",
        ));
    }
    if cli.graph.is_some() {
        return Err(Failure::refused(
            "token_graph_conflict",
            "identity credentials do not select a graph; use --graph on the graph operation",
        ));
    }
    Ok(())
}

pub(super) async fn token(
    cli: &Cli,
    context: &Context,
    ttl: Option<u64>,
    clear: bool,
) -> Result<Value> {
    token_scope(cli, ttl, clear)?;
    if clear {
        return self::clear(&auth::DATA_STORE, context);
    }
    let api = context.client().await?;
    mint(&auth::DATA_STORE, context, &api, ttl.unwrap_or(3600)).await
}

fn load_credential(store: &impl Store, context: &Context) -> Result<Credential> {
    let raw = store.get(&key(context))?.ok_or_else(|| {
        Failure::refused(
            "data_credential_required",
            "no data credential is cached for this cluster; run cluster token --target NAME",
        )
    })?;
    if raw.len() > MAX_CREDENTIAL {
        return Err(invalid());
    }
    let credential: Credential = serde_json::from_str(&raw).map_err(|_| invalid())?;
    credential.validate(context)?;
    Ok(credential)
}

fn load(store: &impl Store, context: &Context, graph: &str) -> Result<GraphClient> {
    let endpoint = context.endpoint.as_deref().ok_or_else(|| {
        Failure::refused(
            "data_endpoint_required",
            "complete the managed target endpoint before data operations",
        )
    })?;
    let credential = load_credential(store, context)?;
    GraphClient::managed(endpoint, graph, credential.token).map_err(|_| {
        Failure::new(
            "transport_failed",
            "could not initialize the managed data client",
            1,
        )
    })
}

/// Qualified data operations only; adding a wrapper needs its own protocol proof.
fn supports(cli: &Cli) -> bool {
    matches!(
        cli.command,
        Command::Query(_)
            | Command::Mutate(_)
            | Command::Graphs {
                command: GraphsCommand::List { .. }
            }
            | Command::Load(LoadArgs { .. })
            | Command::Commit {
                command: CommitCommand::List { .. } | CommitCommand::Show { .. }
            }
    )
}

fn resolve_context(cli: &Cli, context: &Context, store: &impl Store) -> Result<GraphClient> {
    if !supports(cli) {
        return Err(Failure::refused(
            "managed_operation_unsupported",
            "this operation is not supported on managed targets",
        ));
    }
    scope(cli)?;
    let endpoint = context.endpoint.as_deref().ok_or_else(|| Failure::refused("data_endpoint_required", "complete the managed target's endpoint before data operations; cluster token reports the issuer endpoint"))?;
    if matches!(cli.command, Command::Graphs { .. }) {
        if cli.graph.is_some() {
            return Err(Failure::refused(
                "graph_scope_conflict",
                "graphs list enumerates a cluster; omit --graph",
            ));
        }
        let credential = load_credential(store, context)?;
        return GraphClient::managed_registry(endpoint, credential.token).map_err(|_| {
            Failure::new(
                "transport_failed",
                "could not initialize the managed discovery client",
                1,
            )
        });
    }
    let graph = cli
        .graph
        .as_deref()
        .or(context.default_graph.as_deref())
        .ok_or_else(|| {
            Failure::refused(
                "graph_required",
                "managed data requires --graph or a target default_graph",
            )
        })?;
    graph_id(graph)?;
    load(store, context, graph)
}

/// Acquire authority before constructing the operation request. Unsupported
/// or malformed caches refuse before contacting the issuer.
async fn resolve_with_acquisition(
    cli: &Cli,
    context: &Context,
    store: &impl Store,
    identity: impl AsyncFn(&Context) -> Result<Option<String>>,
    api: impl AsyncFn(&Context) -> Result<Api>,
) -> Result<GraphClient> {
    let mut resolved = resolve_context(cli, context, store);
    let expected = if matches!(&resolved, Ok(_)) {
        let expected = identity(context).await?;
        if expected.as_ref().is_some_and(|id| {
            load_credential(store, context)
                .is_ok_and(|saved| saved.actor != format!("principal:{id}"))
        }) {
            resolved = Err(Failure::refused(
                "data_credential_identity_mismatch",
                "the cached graph credential belongs to another signed-in principal",
            ));
        }
        expected
    } else {
        None
    };
    match resolved {
        Ok(client) => Ok(client),
        Err(failure)
            if matches!(
                failure.body["type"].as_str(),
                Some(
                    "data_credential_required"
                        | "data_credential_expired"
                        | "data_credential_identity_mismatch"
                )
            ) =>
        {
            let expected = match expected {
                Some(id) => Some(id),
                None => identity(context).await?,
            };
            // A cluster cache lock serializes independent CLI invocations. The
            // control credential lock is acquired only inside this one.
            let _lock = auth::cache_lock(&format!("data:{}", key(context))).await?;
            if let Some(raw) = store.get(&key(context))? {
                if raw.len() > MAX_CREDENTIAL {
                    return Err(invalid());
                }
                let saved: Credential = serde_json::from_str(&raw).map_err(|_| invalid())?;
                match saved.validate(context) {
                    Ok(()) => {
                        if expected
                            .as_ref()
                            .is_none_or(|id| saved.actor == format!("principal:{id}"))
                        {
                            return resolve_context(cli, context, store);
                        }
                    }
                    Err(error) if error.body["type"] == "data_credential_expired" => {}
                    Err(error) => return Err(error),
                }
            }
            let api = api(context).await?;
            if let (Some(selected), Some(expected)) = (&api.selection, &expected) {
                api.matches_principal(expected, &context.organization_id)?;
                if selected.account_id != context.organization_id {
                    return Err(Failure::protocol());
                }
            }
            mint_for_principal(store, context, &api, 3600, expected.as_deref()).await?;
            resolve_context(cli, context, store)
        }
        Err(failure) => Err(failure),
    }
}

pub(crate) async fn client(
    cli: &Cli,
    binding: &super::Binding,
) -> std::result::Result<GraphClient, Output> {
    let json = match &cli.command {
        Command::Query(args) => {
            args.json
                || matches!(
                    args.format,
                    Some(crate::read_format::ReadOutputFormat::Json)
                )
        }
        Command::Mutate(args) => args.json,
        Command::Load(args) => args.json,
        Command::Commit {
            command: CommitCommand::List { json, .. } | CommitCommand::Show { json, .. },
        } => *json,
        Command::Graphs {
            command: GraphsCommand::List { json, .. },
        } => *json,
        _ => false,
    };
    let result = async {
        let context = binding.context()?;
        resolve_with_acquisition(
            cli,
            &context,
            &auth::DATA_STORE,
            async |context| {
                auth::selected_principal(
                    &context.api,
                    &context.organization_id,
                    context.control_auth,
                )
                .await
            },
            async |context| context.client().await,
        )
        .await
    }
    .await;
    result.map_err(|e| Output::from_result(Err(e), json, 2))
}

#[cfg(test)]
mod tests;
