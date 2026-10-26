//! Bind the shared HTTP operation inventory to the production router.
//!
//! Handlers and their layers stay explicit here. The inventory supplies only
//! the method and path; it does not decide authentication or execution.

use std::collections::BTreeMap;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::handler::Handler;
use axum::http::Method;
use axum::routing::{MethodFilter, MethodRouter};
use omnigraph_api_types::operations::{HttpMethod, HttpOperation, named_operation};

use crate::{AppState, INGEST_REQUEST_BODY_LIMIT_BYTES, deployment, handlers, maintenance};

pub(crate) fn operation(name: &str) -> &'static HttpOperation {
    named_operation(name).expect("a registered HTTP operation belongs to the shared inventory")
}

pub(crate) fn method(method: HttpMethod) -> Method {
    match method {
        HttpMethod::Get => Method::GET,
        HttpMethod::Head => Method::HEAD,
        HttpMethod::Post => Method::POST,
        HttpMethod::Put => Method::PUT,
        HttpMethod::Delete => Method::DELETE,
    }
}

pub(crate) struct HttpRoutes {
    routes: BTreeMap<String, MethodRouter<AppState>>,
    mount: &'static str,
    #[cfg(test)]
    registered: Vec<(HttpMethod, String)>,
}

impl HttpRoutes {
    pub(crate) fn new(mount: &'static str) -> Self {
        Self {
            routes: BTreeMap::new(),
            mount,
            #[cfg(test)]
            registered: Vec::new(),
        }
    }

    pub(crate) fn route<H, T>(self, name: &str, handler: H) -> Self
    where
        H: Handler<T, AppState>,
        T: 'static,
    {
        self.route_with(name, handler, std::convert::identity)
    }

    pub(crate) fn route_with<H, T>(
        self,
        name: &str,
        handler: H,
        configure: impl FnOnce(MethodRouter<AppState>) -> MethodRouter<AppState>,
    ) -> Self
    where
        H: Handler<T, AppState>,
        T: 'static,
    {
        let operation = operation(name);
        let path = operation
            .path
            .strip_prefix(self.mount)
            .expect("an operation belongs to the router it is mounted under");
        self.install(operation, path, handler, configure)
    }

    /// OAuth's path-suffixed metadata route captures the entire resource path.
    /// OpenAPI names that parameter without Axum's catch-all marker.
    pub(crate) fn resource_metadata<H, T>(
        self,
        handler: H,
        configure: impl FnOnce(MethodRouter<AppState>) -> MethodRouter<AppState>,
    ) -> Self
    where
        H: Handler<T, AppState>,
        T: 'static,
    {
        let operation = operation("oauth protected resource metadata at resource path");
        let path = operation
            .path
            .replace("{resource_path}", "{*resource_path}");
        self.install(operation, &path, handler, configure)
    }

    fn install<H, T>(
        mut self,
        operation: &'static HttpOperation,
        path: &str,
        handler: H,
        configure: impl FnOnce(MethodRouter<AppState>) -> MethodRouter<AppState>,
    ) -> Self
    where
        H: Handler<T, AppState>,
        T: 'static,
    {
        let filter = MethodFilter::try_from(method(operation.method))
            .expect("the inventory uses supported HTTP methods");
        // Add methods to one MethodRouter before mounting it. Merging
        // separate GET and HEAD routers concatenates their Allow headers,
        // duplicating HEAD; MethodRouter::on preserves the original chain.
        let route = self.routes.remove(path).unwrap_or_default();
        self.routes
            .insert(path.to_string(), configure(route.on(filter, handler)));
        #[cfg(test)]
        self.registered
            .push((operation.method, format!("{}{path}", self.mount)));
        self
    }

    pub(crate) fn into_router(self) -> Router<AppState> {
        self.routes
            .into_iter()
            .fold(Router::new(), |router, (path, methods)| {
                router.route(&path, methods)
            })
    }

    #[cfg(test)]
    pub(crate) fn registered(mut self) -> Vec<(HttpMethod, String)> {
        let registered = std::mem::take(&mut self.registered);
        // Inventory checks construct the same router too, so overlapping
        // paths and methods cannot pass merely by recording valid metadata.
        let _router = self.into_router();
        registered
    }
}

pub(crate) fn graph() -> HttpRoutes {
    let routes = HttpRoutes::new(handlers::CLUSTER_PATH_PREFIX)
        .route("snapshot", handlers::server_snapshot)
        .route("access", handlers::server_access)
        .route("blob get", handlers::server_blob_get)
        // A dedicated HEAD handler avoids opening the GET payload stream.
        .route("blob stat", handlers::server_blob_head)
        .route("blob put", handlers::server_blob_put)
        .route("blob clear", handlers::server_blob_delete)
        .route("export", handlers::server_export)
        .route("query", handlers::server_query)
        .route("mutate", handlers::server_mutate)
        .route(
            "mutate if graph commit",
            handlers::server_mutate_if_graph_commit,
        )
        .route("queries list", handlers::server_list_queries)
        .route("query invoke", handlers::server_invoke_query)
        .route(
            "query invoke if graph commit",
            handlers::server_invoke_query_if_graph_commit,
        )
        .route("schema show", handlers::server_schema_get)
        .route_with("load", handlers::server_load, |route| {
            route.layer(DefaultBodyLimit::max(INGEST_REQUEST_BODY_LIMIT_BYTES))
        })
        .route("load ndjson", handlers::server_load_ndjson)
        .route("branch list", handlers::server_branch_list)
        .route("branch create", handlers::server_branch_create)
        // Keep one dynamic route: DELETE accepts every name, while the
        // fallback dispatches the BranchAction inventory by name and method.
        .route_with("branch delete", handlers::server_branch_delete, |route| {
            route.fallback(handlers::server_branch_action)
        })
        .route("commit list", handlers::server_commit_list)
        .route("commit show", handlers::server_commit_show)
        .route("commit drop", handlers::server_commit_drop)
        .route("commit publish", handlers::server_commit_publish)
        .route("commit changes", handlers::server_commit_changes)
        .route("diff", handlers::server_diff)
        .route("changes poll", handlers::server_changes_feed)
        .route("changes baseline", handlers::server_changes_baseline);
    #[cfg(test)]
    let routes = {
        let mut routes = routes;
        routes
            .registered
            .extend(handlers::branch_action_http_routes());
        routes
    };
    routes
}

pub(crate) fn management() -> HttpRoutes {
    HttpRoutes::new("")
        .route("list graphs", handlers::server_graphs_list)
        .route("discover graphs", handlers::server_graphs_discovery)
}

pub(crate) fn deployments() -> HttpRoutes {
    HttpRoutes::new("")
        .route("deployment plan", deployment::plan)
        .route("deployment status", deployment::status)
        .route("deployment apply", deployment::apply)
        .route("deployment lookup", deployment::lookup)
}

pub(crate) fn probes() -> HttpRoutes {
    HttpRoutes::new("")
        .route("health", handlers::server_health)
        .route("health head", handlers::server_health)
        .route("readiness", handlers::server_ready)
}

pub(crate) fn maintenance() -> HttpRoutes {
    HttpRoutes::new("")
        .route("maintenance status", maintenance::status)
        .route("optimize", maintenance::submit)
        .route("maintenance lookup", maintenance::lookup)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use omnigraph_api_types::operations::HTTP_OPERATIONS;

    use super::*;

    #[test]
    fn production_router_registration_matches_the_operation_inventory() {
        let mut registered = graph().registered();
        registered.extend(management().registered());
        registered.extend(deployments().registered());
        registered.extend(maintenance().registered());
        registered.extend(probes().registered());
        registered.extend(crate::mcp::metadata_http_operations());
        let count = registered.len();
        let registered: BTreeSet<_> = registered
            .into_iter()
            .map(|(method, path)| (method, path.replace("{*resource_path}", "{resource_path}")))
            .collect();
        assert_eq!(registered.len(), count, "an operation was registered twice");
        let declared: BTreeSet<_> = HTTP_OPERATIONS
            .iter()
            .map(|operation| (operation.method, operation.path.to_string()))
            .collect();
        assert_eq!(registered, declared);
        // Implicit HEAD for ordinary GET routes is the router's HTTP
        // behavior, not another documented operation. Only health and Blob
        // stat register HEAD explicitly; /mcp, /openapi.json and the virtual
        // catalogue dispatch remain the declared protocol/scope exclusions.
    }
}
