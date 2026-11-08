//! The engine catalogue scope served over HTTP: `/graphs/og_catalog/query`
//! and `/graphs/og_catalog/schema`, the virtual resource every binary
//! serves. It holds no `Omnigraph`, no storage URI, no branch and no
//! graph commit; a read is authorized against the `graph.og_catalog` policy
//! with the resource `og_catalog` and no branch, under the same credential
//! rules as a stored graph. Every other route under the prefix is refused
//! before any effect.

use axum::extract::State;
use axum::{Extension, Json};
use omnigraph::catalogue::{CATALOGUE_SCOPE, CatalogueScope};
use omnigraph::db::ReadTarget;
use omnigraph_compiler::query::ast::FileBody;
use omnigraph_policy::{PolicyAction, PolicyRequest};

use super::dispatch::{classify, refuse_empty_file, refuse_process_settings};
use super::{authorize_request, query_params_from_json, render_error, select_named_query_decl};
use crate::api::{self, QueryRequest, ReadOutput, ReadTargetOutput, SchemaOutput};
use crate::extract::ApiJson;
use crate::identity::AuthenticatedActor;
use crate::{ApiError, AppState};

impl AppState {
    /// The catalogue scope of one request: the process defaults with the
    /// request's settings applied, as a graph session would start.
    fn catalogue_scope(
        &self,
        request: Option<&api::SettingsRequest>,
    ) -> std::result::Result<CatalogueScope, ApiError> {
        let mut scope = CatalogueScope::new(self.process_defaults.settings.clone())
            .map_err(ApiError::from_omni)?;
        for (id, value) in request
            .map(api::SettingsRequest::assignments)
            .unwrap_or_default()
        {
            scope
                .set(id, &value)
                .map_err(|error| ApiError::bad_request(error.to_string()))?;
        }
        Ok(scope)
    }

    fn authorize_catalogue_read(
        &self,
        actor: Option<&AuthenticatedActor>,
    ) -> std::result::Result<(), ApiError> {
        authorize_request(
            actor,
            self.catalogue_policy().as_deref(),
            PolicyRequest {
                action: PolicyAction::Read,
                branch: None,
                target_branch: None,
            },
        )
    }
}

/// `POST /graphs/og_catalog/query`: a read declaration or an `explain` over
/// one, answered from the binary's registries.
pub(crate) async fn catalogue_query(
    State(state): State<AppState>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiJson(request): ApiJson<QueryRequest>,
) -> std::result::Result<Json<ReadOutput>, ApiError> {
    state.authorize_catalogue_read(actor.as_ref().map(|Extension(actor)| actor))?;
    if request.branch.is_some() || request.snapshot.is_some() {
        return Err(ApiError::bad_request(format!(
            "`{CATALOGUE_SCOPE}` has no branches or snapshots; omit `branch` and `snapshot`"
        )));
    }
    let file = classify(&request.query)?;
    refuse_process_settings(&file.settings)?;
    refuse_empty_file(&file)?;
    let queries = match file.body {
        FileBody::Queries(queries) => queries,
        body @ FileBody::Explain(_) => body
            .into_read_declarations()
            .map_err(ApiError::bad_request)?,
        FileBody::Branch(_)
        | FileBody::Commit(_)
        | FileBody::Load(_)
        | FileBody::ExplainLoad(_)
        | FileBody::Show(_) => {
            return Err(ApiError::bad_request(format!(
                "`{CATALOGUE_SCOPE}` serves read query declarations and `explain` only"
            )));
        }
    };
    let decl = select_named_query_decl(queries, request.name.as_deref())
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    if !decl.mutations.is_empty() {
        return Err(ApiError::bad_request(format!(
            "query '{}' contains mutations; `{CATALOGUE_SCOPE}` is read-only",
            decl.name
        )));
    }
    let params = query_params_from_json(&decl.params, request.params.as_ref())
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let scope = state.catalogue_scope(request.settings.as_ref())?;
    let result = scope
        .query(&request.query, &decl.name, &params)
        .await
        .map_err(ApiError::from_omni)?;
    let mut output = api::read_output(decl.name, &ReadTarget::branch("main"), result, None)
        .map_err(render_error)?;
    output.target = ReadTargetOutput {
        branch: None,
        snapshot: None,
    };
    Ok(Json(output))
}

/// `GET /graphs/og_catalog/schema`: the catalogue types as declarations.
pub(crate) async fn catalogue_schema(
    State(state): State<AppState>,
    actor: Option<Extension<AuthenticatedActor>>,
) -> std::result::Result<Json<SchemaOutput>, ApiError> {
    state.authorize_catalogue_read(actor.as_ref().map(|Extension(actor)| actor))?;
    let scope = state.catalogue_scope(None)?;
    Ok(Json(SchemaOutput {
        schema_source: String::new(),
        system_columns: Some(scope.catalog().system_columns.into()),
        catalogue_source: Some(scope.schema_source()),
    }))
}

/// Every other route under the prefix: no mutation, load, blob, branch,
/// commit, snapshot, export, change feed or stored query exists here.
pub(crate) async fn catalogue_refused() -> ApiError {
    ApiError::method_not_allowed(format!(
        "`{CATALOGUE_SCOPE}` serves `POST /query` and `GET /schema` only; it has no storage, branches, commits or stored queries"
    ))
}
