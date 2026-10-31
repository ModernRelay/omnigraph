//! Cached data credentials have a separate keychain namespace and transport.
use super::auth::{self, Store};
use super::{Api, Context, Failure, Method, Result, canonical_origin, json};
use crate::cli::Cli;
#[cfg(test)]
use crate::cli::Command;
use crate::client::GraphClient;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const MAX_CREDENTIAL: usize = 64 * 1024;
const MAX_TOKEN: usize = 8192;

#[derive(Default)]
pub(crate) struct InvocationCredentials(
    std::sync::Mutex<std::collections::BTreeMap<String, String>>,
);

impl std::fmt::Debug for InvocationCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("InvocationCredentials([redacted])")
    }
}

impl Store for InvocationCredentials {
    fn persistent(&self) -> bool {
        false
    }

    fn get(&self, key: &str) -> Result<Option<String>> {
        Ok(self.0.lock().map_err(|_| invalid())?.get(key).cloned())
    }

    fn put(&self, key: &str, value: &str) -> Result<()> {
        if value.len() > MAX_CREDENTIAL {
            return Err(invalid());
        }
        let mut values = self.0.lock().map_err(|_| invalid())?;
        if values.len() >= 128 && !values.contains_key(key) {
            return Err(Failure::refused(
                "credential_cache_limit",
                "invocation data credential limit reached",
            ));
        }
        values.insert(key.into(), value.into());
        Ok(())
    }

    fn remove(&self, key: &str) -> Result<()> {
        self.0.lock().map_err(|_| invalid())?.remove(key);
        Ok(())
    }
}

enum ResourceStore<'a> {
    Persistent(&'a dyn Store),
    StoredToken(&'a dyn Store),
    Invocation(&'a InvocationCredentials),
}

impl<'a> ResourceStore<'a> {
    // Explicit token administration retains existing providers' persistent
    // custody. OAuth credentials stay in invocation memory for every command.
    fn for_profile(
        profile: &crate::operator_schema::AuthProfile,
        invocation: &'a InvocationCredentials,
        persistent: &'a dyn Store,
        explicit_token: bool,
    ) -> Self {
        use crate::operator_schema::{AuthProfile, TokenSource};
        match profile {
            AuthProfile::OauthClientCredentials { .. } => Self::Invocation(invocation),
            AuthProfile::Token {
                source: TokenSource::Env { .. } | TokenSource::File { .. },
            }
            | AuthProfile::Exec { .. }
                if !explicit_token =>
            {
                Self::Invocation(invocation)
            }
            AuthProfile::Token {
                source: TokenSource::Store { .. },
            } => Self::StoredToken(persistent),
            _ => Self::Persistent(persistent),
        }
    }

    fn selected(&self) -> &dyn Store {
        match self {
            Self::Persistent(store) | Self::StoredToken(store) => *store,
            Self::Invocation(store) => *store,
        }
    }
}

impl Store for ResourceStore<'_> {
    fn cache_without_control(&self) -> Result<bool> {
        if !matches!(self, Self::StoredToken(_)) {
            return Ok(false);
        }
        let auth = crate::operator_routing::AUTH
            .try_with(Clone::clone)
            .ok()
            .flatten()
            .ok_or_else(|| {
                Failure::refused(
                    "credential_binding_required",
                    "stored-token cache requires a frozen owner/profile binding",
                )
            })?;
        if !matches!(
            auth.profile,
            crate::operator_schema::AuthProfile::Token {
                source: crate::operator_schema::TokenSource::Store { .. }
            }
        ) {
            return Err(Failure::protocol());
        }
        auth.ensure_current().map_err(|_| {
            Failure::refused(
                "credential_generation_changed",
                "stored-token binding changed; retry with a fresh invocation",
            )
        })?;
        Ok(true)
    }
    fn persistent(&self) -> bool {
        self.selected().persistent()
    }
    fn get(&self, key: &str) -> Result<Option<String>> {
        self.selected().get(key)
    }
    fn put(&self, key: &str, value: &str) -> Result<()> {
        self.selected().put(key, value)
    }
    fn remove(&self, key: &str) -> Result<()> {
        self.selected().remove(key)
    }
}

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
        "{}/organizations/{}/clusters/{}/incarnations/{}",
        context.api, context.organization_id, context.cluster, context.incarnation
    )
}

fn invalid() -> Failure {
    Failure::refused(
        "data_credential_invalid",
        "the cached data credential is invalid; run cluster token --target NAME",
    )
}

fn graph_id(graph: &str) -> Result<()> {
    if crate::helpers::require_graph_segment(graph).is_err() {
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
        let issuer = url::Url::parse(&context.api)
            .map_err(|_| invalid())?
            .origin()
            .ascii_serialization();
        if claims.iss != issuer
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
        if context.endpoint.as_ref().is_some_and(|endpoint| {
            endpoint.trim_end_matches('/') != self.endpoint.trim_end_matches('/')
        }) {
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
    let credential = issue_for_principal(store, context, api, ttl, expected_principal).await?;
    let mut metadata = credential.metadata();
    auth::scrub_value(&mut metadata, &credential.token);
    Ok(json!({"data":metadata,"meta":{"cluster_id":context.cluster}}))
}

async fn issue_for_principal(
    store: &impl Store,
    context: &Context,
    api: &Api,
    ttl: u64,
    expected_principal: Option<&str>,
) -> Result<Credential> {
    let path = format!("/v1/clusters/{}/tokens", context.cluster);
    let request = json!({"version":2,"ttl_seconds":ttl});
    let issued = api.request(Method::POST, &path, Some(&request), None).await;
    let body = match issued {
        // This exact refusal is an issuance preflight: no native credential or
        // graph/configuration effect was sent. Renew once; never retry a transport
        // failure, authentication refusal, or any graph operation.
        Err(failure)
            if failure.body["type"] == "credential_near_expiry"
                && failure.response_status == Some(reqwest::StatusCode::CONFLICT) =>
        {
            let Some(selected) = api.selection.as_ref() else {
                return Err(failure);
            };
            if !selected.renew_before_issuance(&api.origin).await? {
                return Err(failure);
            }
            api.request(Method::POST, &path, Some(&request), None)
                .await?
        }
        other => other?,
    };
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
    Ok(credential)
}

fn clear(store: &impl Store, context: &Context) -> Result<Value> {
    // Persistent removal retains its existing remove-only contract; inspecting
    // it first could add an OS credential prompt. Memory has no such boundary.
    let removed = store.persistent() || store.get(&key(context))?.is_some();
    store.remove(&key(context))?;
    Ok(
        json!({"data":{"cluster_id":context.cluster,"local_credential_removed":removed,"revocation_performed":false},"meta":{"cluster_id":context.cluster}}),
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
    let authentication = crate::operator_routing::AUTH
        .try_with(Clone::clone)
        .ok()
        .flatten();
    let store = match authentication.as_ref() {
        Some(auth) => ResourceStore::for_profile(
            &auth.profile,
            &auth.derived_credentials,
            &auth::DATA_STORE,
            true,
        ),
        None => ResourceStore::Persistent(&auth::DATA_STORE),
    };
    token_with_store(cli, context, ttl, clear, &store).await
}

async fn token_with_store(
    cli: &Cli,
    context: &Context,
    ttl: Option<u64>,
    clear: bool,
    store: &impl Store,
) -> Result<Value> {
    token_scope(cli, ttl, clear)?;
    if clear {
        return self::clear(store, context);
    }
    let api = context.client().await?;
    mint(store, context, &api, ttl.unwrap_or(3600)).await
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

/// Qualified data operations only; adding a wrapper needs its own protocol proof.
#[cfg(test)]
fn resolve_context(cli: &Cli, context: &Context, store: &impl Store) -> Result<GraphClient> {
    scope(cli)?;
    let graph = if matches!(cli.command, Command::Graphs { .. }) {
        None
    } else {
        Some(
            cli.graph
                .as_deref()
                .or(context.default_graph.as_deref())
                .ok_or_else(|| {
                    Failure::refused(
                        "graph_required",
                        "select --graph or configure default_graph",
                    )
                })?,
        )
    };
    resolve_resource(graph, context, store)
}

#[cfg(test)]
fn resolve_resource(
    graph: Option<&str>,
    context: &Context,
    store: &impl Store,
) -> Result<GraphClient> {
    let credential = load_resource(graph, context, store)?;
    resolve_loaded(graph, context, credential)
}

fn load_resource(graph: Option<&str>, context: &Context, store: &impl Store) -> Result<Credential> {
    if context.endpoint.is_none() {
        return Err(Failure::refused(
            "data_endpoint_required",
            "complete the managed target's endpoint before data operations; cluster token reports the issuer endpoint",
        ));
    }
    if let Some(graph) = graph {
        graph_id(graph)?;
    }
    load_credential(store, context)
}

fn resolve_loaded(
    graph: Option<&str>,
    context: &Context,
    credential: Credential,
) -> Result<GraphClient> {
    credential.validate(context)?;
    let endpoint = context.endpoint.as_deref().ok_or_else(|| Failure::refused("data_endpoint_required", "complete the managed target's endpoint before data operations; cluster token reports the issuer endpoint"))?;
    use crate::credential_sources::{
        AcquiredCredential, CredentialRequest, OwnerBinding, OwnerKind, Purpose, ResourceIdentity,
    };
    // This issued credential binds its verified principal and resource. Its
    // lifetime is independent from the control credential used to acquire it.
    let owner = OwnerBinding::new(
        OwnerKind::Profile,
        context.organization_id.clone(),
        credential.actor.clone(),
        context.api.clone(),
        Some(context.organization_id.clone()),
    )
    .map_err(|_| invalid())?;
    let request = CredentialRequest::new(
        owner,
        Purpose::Data,
        endpoint.to_owned(),
        Some(format!("urn:omnigraph:data:{}", context.cluster)),
        Some(ResourceIdentity {
            cluster_id: context.cluster.clone(),
            cluster_incarnation: context.incarnation.clone(),
        }),
    )
    .map_err(|_| invalid())?;
    let credential = AcquiredCredential::issued(
        request,
        credential.token,
        OffsetDateTime::parse(&credential.expires_at, &Rfc3339).map_err(|_| invalid())?,
    )
    .map_err(|_| invalid())?;
    let Some(graph) = graph else {
        return GraphClient::managed_registry(endpoint, credential).map_err(|_| {
            Failure::new(
                "transport_failed",
                "could not initialize the managed discovery client",
                1,
            )
        });
    };
    graph_id(graph)?;
    GraphClient::managed(endpoint, graph, credential).map_err(|_| {
        Failure::new(
            "transport_failed",
            "could not initialize the managed data client",
            1,
        )
    })
}

/// Acquire authority before constructing the operation request. Unsupported
/// or malformed caches refuse before contacting the issuer.
#[cfg(test)]
async fn resolve_with_acquisition(
    cli: &Cli,
    context: &Context,
    store: &impl Store,
    identity: impl AsyncFn(&Context) -> Result<Option<String>>,
    api: impl AsyncFn(&Context) -> Result<Api>,
) -> Result<GraphClient> {
    scope(cli)?;
    let graph = if matches!(cli.command, Command::Graphs { .. }) {
        None
    } else {
        Some(
            cli.graph
                .as_deref()
                .or(context.default_graph.as_deref())
                .ok_or_else(|| {
                    Failure::refused(
                        "graph_required",
                        "select --graph or configure default_graph",
                    )
                })?,
        )
    };
    acquire_resource(graph, context, store, identity, api).await
}

async fn acquire_resource(
    graph: Option<&str>,
    context: &Context,
    store: &impl Store,
    identity: impl AsyncFn(&Context) -> Result<Option<String>>,
    api: impl AsyncFn(&Context) -> Result<Api>,
) -> Result<GraphClient> {
    let mut resolved = load_resource(graph, context, store);
    if resolved.is_ok() && store.cache_without_control()? {
        return resolved.and_then(|credential| resolve_loaded(graph, context, credential));
    }
    let expected = if resolved.is_ok() {
        let expected = identity(context).await?;
        if !store.persistent() && expected.is_none() {
            return Err(Failure::protocol());
        }
        if expected.as_ref().is_some_and(|id| {
            resolved
                .as_ref()
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
        Ok(credential) => resolve_loaded(graph, context, credential),
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
            if !store.persistent() && expected.is_none() {
                return Err(Failure::protocol());
            }
            // A cluster cache lock serializes independent CLI invocations. The
            // control credential lock is acquired only inside this one.
            let _lock = if store.persistent() {
                Some(auth::cache_lock(&format!("data:{}", key(context))).await?)
            } else {
                None
            };
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
                            return resolve_loaded(graph, context, saved);
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
            let credential =
                issue_for_principal(store, context, &api, 3600, expected.as_deref()).await?;
            resolve_loaded(graph, context, credential)
        }
        Err(failure) => Err(failure),
    }
}

pub(crate) async fn resource(
    binding: &super::Binding,
    graph: Option<&str>,
) -> color_eyre::Result<GraphClient> {
    let result = async {
        let context = binding.context()?;
        let authentication = crate::operator_routing::AUTH
            .try_with(Clone::clone)
            .ok()
            .flatten();
        let store = match authentication.as_ref() {
            Some(auth) => ResourceStore::for_profile(
                &auth.profile,
                &auth.derived_credentials,
                &auth::DATA_STORE,
                false,
            ),
            None => ResourceStore::Persistent(&auth::DATA_STORE),
        };
        acquire_resource(
            graph,
            &context,
            &store,
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
    result.map_err(|failure: Failure| color_eyre::eyre::eyre!("{}", failure.body))
}

/// Forget only the exact rejected credential of this resource. A concurrent
/// replacement must survive; the next acquisition still verifies current
/// profile authority and never opens an interactive login.
pub(crate) async fn forget_refused(
    binding: &super::Binding,
    refused_token: &str,
) -> color_eyre::Result<()> {
    let result = async {
        let context = binding.context()?;
        let authentication = crate::operator_routing::AUTH
            .try_with(Clone::clone)
            .ok()
            .flatten();
        let store = match authentication.as_ref() {
            Some(auth) => ResourceStore::for_profile(
                &auth.profile,
                &auth.derived_credentials,
                &auth::DATA_STORE,
                false,
            ),
            None => ResourceStore::Persistent(&auth::DATA_STORE),
        };
        let _lock = if store.persistent() {
            Some(auth::cache_lock(&format!("data:{}", key(&context))).await?)
        } else {
            None
        };
        remove_refused(&store, &context, refused_token)
    }
    .await;
    result.map_err(|failure: Failure| color_eyre::eyre::eyre!("{}", failure.body))
}

fn remove_refused(store: &impl Store, context: &Context, refused_token: &str) -> Result<()> {
    if let Some(raw) = store.get(&key(context))? {
        if raw.len() > MAX_CREDENTIAL {
            return Err(invalid());
        }
        let credential: Credential = serde_json::from_str(&raw).map_err(|_| invalid())?;
        if credential.token == refused_token {
            store.remove(&key(context))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
