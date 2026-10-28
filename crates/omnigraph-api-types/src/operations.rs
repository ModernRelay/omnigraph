//! The public operation inventory shared by HTTP, MCP and the CLI.
//!
//! These rows describe existing bindings, not promised parity. A missing or
//! restricted binding names its reason; adding a tool or command does not
//! silently turn it into another operation. The HTTP rows include distinct
//! conditional and payload variants because they have distinct wire contracts.
//! Runtime dispatch, authentication, schemas and MCP boot gates keep their
//! existing owners. In particular, the engine-free MCP crate does not depend on
//! this module: the server compares its real tool list with this inventory.

/// An explicitly documented HTTP method. Automatic HEAD handling for GET is
/// owned by the router and is not another operation, except where HEAD has its
/// own contract (health and Blob stat).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum HttpMethod {
    Get,
    Head,
    Post,
    Put,
    Delete,
}

impl HttpMethod {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Delete => "DELETE",
        }
    }

    pub const fn as_lowercase(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Head => "head",
            Self::Post => "post",
            Self::Put => "put",
            Self::Delete => "delete",
        }
    }
}

/// What an operation can do, rather than what its HTTP method suggests.
/// A write-capable operation can also refuse or complete without changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    Read,
    Write,
    /// The separate fail-closed route requires a graph-commit precondition.
    ConditionalWrite,
}

impl Capability {
    pub const fn may_write(self) -> bool {
        matches!(self, Self::Write | Self::ConditionalWrite)
    }
}

/// Existing names on one surface. An exception explains either absence or a
/// narrower binding, such as MCP exposing only reads of a stored-query route.
#[derive(Debug, Clone, Copy)]
pub struct Surface {
    pub names: &'static [&'static str],
    pub exception: Option<&'static str>,
}

impl Surface {
    pub const fn bound(names: &'static [&'static str]) -> Self {
        Self {
            names,
            exception: None,
        }
    }

    pub const fn restricted(names: &'static [&'static str], reason: &'static str) -> Self {
        Self {
            names,
            exception: Some(reason),
        }
    }

    pub const fn absent(reason: &'static str) -> Self {
        Self::restricted(&[], reason)
    }
}

/// One documented HTTP operation and its existing tool/command bindings.
#[derive(Debug, Clone, Copy)]
pub struct HttpOperation {
    /// Canonical words; `operation_id` renders them as lowerCamelCase.
    pub name: &'static str,
    pub method: HttpMethod,
    /// The full mounted path, including the current parameter spellings.
    pub path: &'static str,
    pub capability: Capability,
    /// Pinned SDK names for non-graph operations and standard protocols.
    /// Graph operation IDs always render from the canonical name.
    pub operation_id_override: Option<&'static str>,
    pub mcp: Surface,
    pub cli: Surface,
}

impl HttpOperation {
    /// Render the canonical words without maintaining another name table.
    pub fn operation_id(&self) -> String {
        if let Some(pinned) = self.operation_id_override {
            return pinned.to_owned();
        }
        let mut words = self.name.split_ascii_whitespace();
        let mut rendered = words.next().unwrap_or_default().to_string();
        for word in words {
            let mut chars = word.chars();
            if let Some(first) = chars.next() {
                rendered.push(first.to_ascii_uppercase());
                rendered.push_str(chars.as_str());
            }
        }
        rendered
    }
}

const fn operation(
    name: &'static str,
    method: HttpMethod,
    path: &'static str,
    capability: Capability,
    operation_id_override: Option<&'static str>,
    mcp: Surface,
    cli: Surface,
) -> HttpOperation {
    HttpOperation {
        name,
        method,
        path,
        capability,
        operation_id_override,
        mcp,
        cli,
    }
}

const PROBE: &str = "Process probe, not a graph command.";
const OAUTH: &str = "Protocol-defined OAuth discovery, not a graph operation.";
const DEPLOYMENT: &str = "Deployment control is not an MCP agent tool.";
const BYTE_STREAM: &str = "MCP tools do not carry this byte-stream contract.";

/// Every operation in the generated HTTP specification. Protocol endpoints
/// (`/mcp`, `/openapi.json`) and the catalogue's reserved-id dispatch retain
/// their explicit route classes rather than masquerading as graph operations.
pub const HTTP_OPERATIONS: &[HttpOperation] = &[
    operation(
        "oauth protected resource metadata",
        HttpMethod::Get,
        "/.well-known/oauth-protected-resource",
        Capability::Read,
        Some("oauthProtectedResourceMetadata"),
        Surface::absent(OAUTH),
        Surface::absent(OAUTH),
    ),
    operation(
        "oauth protected resource metadata at resource path",
        HttpMethod::Get,
        "/.well-known/oauth-protected-resource/{resource_path}",
        Capability::Read,
        Some("oauthProtectedResourceMetadataAtResourcePath"),
        Surface::absent(OAUTH),
        Surface::absent(OAUTH),
    ),
    operation(
        "deployment status",
        HttpMethod::Get,
        "/cluster/deployments",
        Capability::Read,
        Some("deployment_status"),
        Surface::absent(DEPLOYMENT),
        Surface::restricted(
            &["cluster status"],
            "Without --deployment-id, read the current deployment state.",
        ),
    ),
    operation(
        "deployment apply",
        HttpMethod::Post,
        "/cluster/deployments",
        Capability::Write,
        Some("deployment_apply"),
        Surface::absent(DEPLOYMENT),
        Surface::bound(&["cluster apply"]),
    ),
    operation(
        "deployment lookup",
        HttpMethod::Get,
        "/cluster/deployments/{deployment_id}",
        Capability::Read,
        Some("deployment_lookup"),
        Surface::absent(DEPLOYMENT),
        Surface::restricted(
            &["cluster status"],
            "--deployment-id selects this exact-receipt route.",
        ),
    ),
    operation(
        "deployment plan",
        HttpMethod::Post,
        "/cluster/plan",
        Capability::Read,
        Some("deployment_plan"),
        Surface::absent(DEPLOYMENT),
        Surface::bound(&["cluster plan"]),
    ),
    operation(
        "list graphs",
        HttpMethod::Get,
        "/graphs",
        Capability::Read,
        Some("listGraphs"),
        Surface::restricted(
            &["graphs"],
            "A static or signed bearer credential gets this listing.",
        ),
        Surface::restricted(&["graphs list"], "Ordinary listing, without --discovery."),
    ),
    operation(
        "discover graphs",
        HttpMethod::Get,
        "/graphs/discovery",
        Capability::Read,
        Some("discoverGraphs"),
        Surface::restricted(
            &["graphs"],
            "An identity credential gets this discovery shape.",
        ),
        Surface::restricted(
            &["graphs list"],
            "--discovery selects the identity-only discovery route.",
        ),
    ),
    operation(
        "access",
        HttpMethod::Get,
        "/graphs/{graph_id}/access",
        Capability::Read,
        None,
        Surface::bound(&["access"]),
        Surface::bound(&["access"]),
    ),
    operation(
        "blob get",
        HttpMethod::Get,
        "/graphs/{graph_id}/blob",
        Capability::Read,
        None,
        Surface::absent(BYTE_STREAM),
        Surface::bound(&["blob get"]),
    ),
    operation(
        "blob put",
        HttpMethod::Put,
        "/graphs/{graph_id}/blob",
        Capability::Write,
        None,
        Surface::absent(BYTE_STREAM),
        Surface::bound(&["blob put"]),
    ),
    operation(
        "blob clear",
        HttpMethod::Delete,
        "/graphs/{graph_id}/blob",
        Capability::Write,
        None,
        Surface::bound(&["blobs_clear"]),
        Surface::bound(&["blob clear"]),
    ),
    operation(
        "blob stat",
        HttpMethod::Head,
        "/graphs/{graph_id}/blob",
        Capability::Read,
        None,
        Surface::restricted(
            &["blobs_stat"],
            "The tool answers the route's headers as blob stat JSON; conditional headers are not arguments.",
        ),
        Surface::bound(&["blob stat"]),
    ),
    operation(
        "branch list",
        HttpMethod::Get,
        "/graphs/{graph_id}/branches",
        Capability::Read,
        None,
        Surface::bound(&["branches_list"]),
        Surface::bound(&["branch list"]),
    ),
    operation(
        "branch create",
        HttpMethod::Post,
        "/graphs/{graph_id}/branches",
        Capability::Write,
        None,
        Surface::bound(&["branches_create"]),
        Surface::bound(&["branch create"]),
    ),
    operation(
        "branch diff",
        HttpMethod::Get,
        "/graphs/{graph_id}/branches/diff",
        Capability::Read,
        None,
        Surface::bound(&["branches_diff"]),
        Surface::bound(&["branch diff"]),
    ),
    operation(
        "branch merge",
        HttpMethod::Post,
        "/graphs/{graph_id}/branches/merge",
        Capability::Write,
        None,
        Surface::restricted(
            &["branches_merge"],
            "The dedicated tool is write-gated; ad-hoc read can express a GQ dry run.",
        ),
        Surface::bound(&["branch merge"]),
    ),
    operation(
        "branch merge base",
        HttpMethod::Get,
        "/graphs/{graph_id}/branches/merge-base",
        Capability::Read,
        None,
        Surface::bound(&["branches_merge_base"]),
        Surface::bound(&["branch merge-base"]),
    ),
    operation(
        "branch delete",
        HttpMethod::Delete,
        "/graphs/{graph_id}/branches/{branch_name}",
        Capability::Write,
        None,
        Surface::bound(&["branches_delete"]),
        Surface::bound(&["branch delete"]),
    ),
    operation(
        "changes poll",
        HttpMethod::Get,
        "/graphs/{graph_id}/changes",
        Capability::Read,
        None,
        Surface::bound(&["changes_poll"]),
        Surface::bound(&["changes poll"]),
    ),
    operation(
        "changes baseline",
        HttpMethod::Post,
        "/graphs/{graph_id}/changes/baseline",
        Capability::Read,
        None,
        Surface::absent(
            "The baseline is an NDJSON snapshot export, a byte stream MCP tools do not carry; its cursor is valid only once that snapshot is installed.",
        ),
        Surface::bound(&["changes baseline"]),
    ),
    operation(
        "commit list",
        HttpMethod::Get,
        "/graphs/{graph_id}/commits",
        Capability::Read,
        None,
        Surface::bound(&["commits_list"]),
        Surface::bound(&["commit list"]),
    ),
    operation(
        "commit show",
        HttpMethod::Get,
        "/graphs/{graph_id}/commits/{commit_id}",
        Capability::Read,
        None,
        Surface::bound(&["commits_get"]),
        Surface::bound(&["commit show"]),
    ),
    operation(
        "commit drop",
        HttpMethod::Delete,
        "/graphs/{graph_id}/commits/{commit_id}",
        Capability::Write,
        None,
        Surface::restricted(
            &["commits_drop"],
            "The route answers 204 without a body; the tool answers an empty object.",
        ),
        Surface::bound(&["commit drop"]),
    ),
    operation(
        "commit changes",
        HttpMethod::Get,
        "/graphs/{graph_id}/commits/{commit_id}/changes",
        Capability::Read,
        None,
        Surface::bound(&["commits_changes"]),
        Surface::bound(&["commit changes"]),
    ),
    operation(
        "commit publish",
        HttpMethod::Post,
        "/graphs/{graph_id}/commits/{commit_id}/publish",
        Capability::Write,
        None,
        Surface::bound(&["commits_publish"]),
        Surface::bound(&["commit publish"]),
    ),
    operation(
        "diff",
        HttpMethod::Get,
        "/graphs/{graph_id}/diff",
        Capability::Read,
        None,
        Surface::bound(&["diff"]),
        Surface::bound(&["diff"]),
    ),
    operation(
        "export",
        HttpMethod::Post,
        "/graphs/{graph_id}/export",
        Capability::Read,
        None,
        Surface::absent(BYTE_STREAM),
        Surface::bound(&["export"]),
    ),
    operation(
        "load",
        HttpMethod::Post,
        "/graphs/{graph_id}/load",
        Capability::Write,
        None,
        Surface::bound(&["load"]),
        Surface::absent("The CLI load command sends the separate NDJSON request contract."),
    ),
    operation(
        "load ndjson",
        HttpMethod::Post,
        "/graphs/{graph_id}/load/ndjson",
        Capability::Write,
        None,
        Surface::absent("The MCP load tool sends the separate JSON request contract."),
        Surface::bound(&["load"]),
    ),
    operation(
        "mutate",
        HttpMethod::Post,
        "/graphs/{graph_id}/mutate",
        Capability::Write,
        None,
        Surface::restricted(
            &["mutate"],
            "query without if_graph_commit calls the unconditional handler.",
        ),
        Surface::restricted(
            &["mutate"],
            "Ad-hoc source without --if-commit selects this route.",
        ),
    ),
    operation(
        "mutate if graph commit",
        HttpMethod::Post,
        "/graphs/{graph_id}/mutate/if-graph-commit",
        Capability::ConditionalWrite,
        None,
        Surface::restricted(
            &["mutate"],
            "query with if_graph_commit selects this fail-closed conditional handler.",
        ),
        Surface::restricted(
            &["mutate"],
            "Ad-hoc source with --if-commit selects this route.",
        ),
    ),
    operation(
        "queries list",
        HttpMethod::Get,
        "/graphs/{graph_id}/queries",
        Capability::Read,
        None,
        Surface::bound(&["queries"]),
        Surface::bound(&["queries list"]),
    ),
    operation(
        "query invoke",
        HttpMethod::Post,
        "/graphs/{graph_id}/queries/{query_name}",
        Capability::Write,
        None,
        Surface::restricted(
            &["query", "mutate"],
            "query invokes a stored read; mutate with name, no query and no if_graph_commit invokes a stored mutation.",
        ),
        Surface::restricted(
            &["query", "mutate", "alias"],
            "Named query or mutation without source invokes this route; alias resolves a stored-read binding. Mutations with --if-commit use its conditional variant.",
        ),
    ),
    operation(
        "query invoke if graph commit",
        HttpMethod::Post,
        "/graphs/{graph_id}/queries/{query_name}/if-graph-commit",
        Capability::ConditionalWrite,
        None,
        Surface::restricted(
            &["mutate"],
            "mutate with name, no query and if_graph_commit selects this fail-closed route.",
        ),
        Surface::restricted(
            &["mutate"],
            "A named stored mutation with --if-commit selects this route.",
        ),
    ),
    operation(
        "query",
        HttpMethod::Post,
        "/graphs/{graph_id}/query",
        Capability::Read,
        None,
        Surface::bound(&["read"]),
        Surface::restricted(
            &["query"],
            "Ad-hoc source selects this route; a stored name selects query invoke.",
        ),
    ),
    operation(
        "schema show",
        HttpMethod::Get,
        "/graphs/{graph_id}/schema",
        Capability::Read,
        None,
        Surface::bound(&["schema"]),
        Surface::bound(&["schema show"]),
    ),
    operation(
        "snapshot",
        HttpMethod::Get,
        "/graphs/{graph_id}/snapshot",
        Capability::Read,
        None,
        Surface::bound(&["snapshot"]),
        Surface::bound(&["snapshot"]),
    ),
    operation(
        "health",
        HttpMethod::Get,
        "/healthz",
        Capability::Read,
        Some("health"),
        Surface::bound(&["health"]),
        Surface::absent(PROBE),
    ),
    operation(
        "health head",
        HttpMethod::Head,
        "/healthz",
        Capability::Read,
        Some("health_head"),
        Surface::absent("MCP health returns the GET health body, not the process HEAD probe."),
        Surface::absent(PROBE),
    ),
    operation(
        "readiness",
        HttpMethod::Get,
        "/readyz",
        Capability::Read,
        Some("readiness"),
        Surface::absent(PROBE),
        Surface::absent(PROBE),
    ),
];

/// Find a binding by the same full path and method the specification exposes.
pub fn http_operation(method: HttpMethod, path: &str) -> Option<&'static HttpOperation> {
    HTTP_OPERATIONS
        .iter()
        .find(|operation| operation.method == method && operation.path == path)
}

/// Find the canonical operation a router registration names.
pub fn named_operation(name: &str) -> Option<&'static HttpOperation> {
    HTTP_OPERATIONS
        .iter()
        .find(|operation| operation.name == name)
}

/// A canonical CLI leaf with no ordinary server HTTP operation. Aliases such
/// as `org` are Clap aliases of the same leaf and do not create operations.
#[derive(Debug, Clone, Copy)]
pub struct CliOnlyOperation {
    pub name: &'static str,
    pub reason: &'static str,
}

const fn cli_only(name: &'static str, reason: &'static str) -> CliOnlyOperation {
    CliOnlyOperation { name, reason }
}

const STORAGE: &str =
    "Runs against direct storage; served graph declarations change through cluster apply.";
const APPLIED: &str =
    "Local inspection or maintenance of applied cluster state, outside the graph HTTP API.";
const LOCAL: &str = "Local tooling has no server graph operation.";
const MANAGED: &str = "Managed control-plane API, not the graph server's HTTP contract.";
const OPERATOR: &str = "Operator configuration or credential custody is local client work.";

/// Exact CLI leaves, not wildcard families: a new command requires a reviewed
/// HTTP binding or a reason here. Combined with HTTP_OPERATIONS.cli, these rows
/// cover the complete public Clap command tree.
pub const CLI_ONLY_OPERATIONS: &[CliOnlyOperation] = &[
    cli_only(
        "catalogue",
        "Queries the binary's local catalogue without a server or graph target.",
    ),
    cli_only("schema plan", STORAGE),
    cli_only("schema apply", STORAGE),
    cli_only("schema upgrade-system-columns", STORAGE),
    cli_only("init", STORAGE),
    cli_only("upgrade", STORAGE),
    cli_only("optimize", STORAGE),
    cli_only("index build", STORAGE),
    cli_only("index rebuild", STORAGE),
    cli_only("repair", STORAGE),
    cli_only("cleanup", STORAGE),
    cli_only("lint", LOCAL),
    cli_only("queries validate", APPLIED),
    cli_only(
        "cluster validate",
        "Validates local deployment sources without dispatching a deployment.",
    ),
    cli_only("cluster observe", APPLIED),
    cli_only("cluster force-unlock", APPLIED),
    cli_only("cluster upgrade-ledger", APPLIED),
    cli_only("cluster create", MANAGED),
    cli_only("cluster list", MANAGED),
    cli_only("cluster members list", MANAGED),
    cli_only("cluster pull", MANAGED),
    cli_only("cluster push", MANAGED),
    cli_only("cluster delete", MANAGED),
    cli_only("cluster undo-delete", MANAGED),
    cli_only("cluster token", MANAGED),
    cli_only("cluster operation", MANAGED),
    cli_only("cluster history", MANAGED),
    cli_only("cluster cancel", MANAGED),
    cli_only("organization members list", MANAGED),
    cli_only("organization members set", MANAGED),
    cli_only("organization members offboard", MANAGED),
    cli_only("organization invitations list", MANAGED),
    cli_only("organization invitations send", MANAGED),
    cli_only("organization invitations resend", MANAGED),
    cli_only("organization invitations revoke", MANAGED),
    cli_only("policy validate", APPLIED),
    cli_only("policy test", APPLIED),
    cli_only("policy explain", APPLIED),
    cli_only("embed", LOCAL),
    cli_only("login", OPERATOR),
    cli_only("logout", OPERATOR),
    cli_only("target list", OPERATOR),
    cli_only("target inspect", OPERATOR),
    cli_only("target add", OPERATOR),
    cli_only("target refresh", OPERATOR),
    cli_only("target remove", OPERATOR),
    cli_only("credentials set", OPERATOR),
    cli_only("credentials status", OPERATOR),
    cli_only("credentials remove", OPERATOR),
    cli_only("credentials export", OPERATOR),
    cli_only("config validate", OPERATOR),
    cli_only("config rename-profile", OPERATOR),
    cli_only("config rename-target", OPERATOR),
    cli_only("config recover-rename", OPERATOR),
    cli_only("config recover-operations", OPERATOR),
    cli_only(
        "mcp",
        "Relays the MCP protocol to an existing server; --write opts into write tools. It is not a graph HTTP command.",
    ),
    cli_only(
        "studio",
        "Opens OmniGraph Studio on an existing server target through a link; it sends no request to the server and is not a graph HTTP command.",
    ),
    cli_only("skill export", LOCAL),
    cli_only("skill show", LOCAL),
    cli_only("version", LOCAL),
];

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn operation_inventory_has_unique_names_routes_and_renderings() {
        let mut names = BTreeSet::new();
        let mut routes = BTreeSet::new();
        let mut ids = BTreeSet::new();
        for operation in HTTP_OPERATIONS {
            assert!(names.insert(operation.name), "{}", operation.name);
            assert!(
                routes.insert((operation.method, operation.path)),
                "{} {}",
                operation.method.as_str(),
                operation.path
            );
            assert!(ids.insert(operation.operation_id()), "{}", operation.name);
            assert!(operation.path.starts_with('/'));
            if operation.path.starts_with("/graphs/{graph_id}/") {
                assert!(
                    operation.operation_id_override.is_none(),
                    "graph IDs derive from their name: {}",
                    operation.name
                );
            }
            assert!(operation.name.split(' ').all(|word| {
                !word.is_empty() && word.bytes().all(|byte| byte.is_ascii_lowercase())
            }));
            for surface in [operation.mcp, operation.cli] {
                assert!(
                    !surface.names.is_empty() || surface.exception.is_some(),
                    "{} needs an absence reason",
                    operation.name
                );
                if let Some(reason) = surface.exception {
                    assert!(!reason.trim().is_empty(), "{}", operation.name);
                }
                let unique: BTreeSet<_> = surface.names.iter().collect();
                assert_eq!(unique.len(), surface.names.len(), "{}", operation.name);
                assert!(surface.names.iter().all(|name| !name.trim().is_empty()));
            }
        }
    }

    #[test]
    fn cli_only_operations_do_not_hide_http_bindings() {
        let bound: BTreeSet<_> = HTTP_OPERATIONS
            .iter()
            .flat_map(|operation| operation.cli.names.iter().copied())
            .collect();
        let mut local = BTreeSet::new();
        for operation in CLI_ONLY_OPERATIONS {
            assert!(local.insert(operation.name), "{}", operation.name);
            assert!(!bound.contains(operation.name), "{}", operation.name);
            assert!(!operation.reason.trim().is_empty(), "{}", operation.name);
        }
    }
}
