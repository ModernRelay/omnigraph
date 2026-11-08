# HTTP API conventions

**Audience:** contributors adding or changing HTTP operations
**Authority:** transport conventions; runtime contracts remain with their owners

The generated [OpenAPI document](../../openapi.json) describes the served HTTP
surface. Extend its existing test owner,
`crates/omnigraph-server/tests/integration/openapi.rs`, with each operation.
`spec_follows_the_api_conventions` reads the table below. A temporary exception
names its current spelling exactly; adding another exception requires a
reviewed row, and an unused exception fails the check.

## Conventions and exceptions

The `Check`, `Scope`, and `Value` columns are consumed by the test. They are
fixed checks over the generated specification, not a general rules language.
Response naming applies to the named response body, including a union's named
alternatives; a nested field type can retain its domain name. Protocol-defined
paths and the baseline record have permanent exceptions. The conditional
commit-page default remains with the separate pagination design.

| Check | Scope | Value | Reason and owner |
|---|---|---|---|
| `graph-prefix` | Graph operations | `/graphs/{graph_id}/` | The server is cluster-only. |
| `operation-id` | New operation ids | `lowerCamelCase` | Render the canonical operation name; flat protocol and control classes retain explicit names. |
| `flat-path` | `/graphs` | `get=listGraphs` | Graph management. |
| `flat-path` | `/graphs/discovery` | `get=discoverGraphs` | Graph management. |
| `flat-path` | `/cluster/plan` | `post=deployment_plan` | Deployment control keeps its existing SDK name. |
| `flat-path` | `/cluster/deployments` | `get=deployment_status post=deployment_apply` | Deployment control keeps its existing SDK name. |
| `flat-path` | `/cluster/deployments/{deployment_id}` | `get=deployment_lookup` | Deployment control keeps its existing SDK name. |
| `flat-path` | `/healthz` | `get=health head=health_head` | Process probes. |
| `flat-path` | `/readyz` | `get=readiness` | Process probe. |
| `flat-path` | `/.well-known/oauth-protected-resource` | `get=oauthProtectedResourceMetadata` | RFC 9728 path; retain the SDK name for compatibility. |
| `flat-path` | `/.well-known/oauth-protected-resource/{resource_path}` | `get=oauthProtectedResourceMetadataAtResourcePath` | RFC 9728 path; retain the SDK name for compatibility. |
| `path-parameters` | New path parameters | `graph_id branch_name commit_id query_name deployment_id resource_path` | Resource-qualified names; OAuth keeps its protocol name. |
| `query-parameters` | Query parameter names | `snake_case` | Choices use one enum; commit listing uses history, unpublished or unattached. |
| `parameter-descriptions` | Every parameter | `required` | Document meaning, defaults, and refusal conditions. |
| `header-names` | Parameters and response headers | `Title-Case` | Standard headers keep their HTTP meaning. |
| `request-schema-suffix` | Request body schemas | `Request` | Generated SDK request names. |
| `response-schema-suffix` | Response body schemas | `Output` | Generated SDK result names. |
| `response-schema-exception` | `ChangeBaselineRecord` | `permanent` | The engine's durable baseline record is also the HTTP response. |
| `enum-schema-suffixes` | String enums | `Request Output Response` | Enums carry none of these transport suffixes. |
| `limit-bounds` | Every limit parameter | `minimum maximum default` | Positive minimum and finite ceiling, with the default and ceiling described. Commit history remains unbounded when both paging inputs are absent. |
| `limit-default-exception` | `/graphs/{graph_id}/commits` | `conditional` | The default applies only with a page token; an unconditional schema default would turn unbounded history into a page. Separate pagination design. |
| `page-envelope` | Paged response bodies | `next_page_token` | Items use a named array alongside the continuation. |
| `operation-tags` | Every operation | `declared` | Every tag belongs to the top-level tags array. |

## Operation inventory

`omnigraph-api-types::operations` names each HTTP operation once, with its
method, path, write capability and existing MCP and CLI bindings. An absent
or restricted binding has a reason. CLI commands whose owners are local
storage, the operator configuration or the managed control plane have exact
named entries with those reasons. The inventory adds no commands or tools.

The server registers native routes through this inventory. Its route owner
also records branch actions from the actual `BranchAction` dispatch and OAuth
metadata from the production constructor. Tests compare those registrations
with the inventory and the generated specification. The CLI compares its
Clap command leaves; the MCP integration owner compares real `tools/list`
responses across its boot gates. The MCP crate remains engine-free.

When adding an operation, update its inventory row, handler registration,
OpenAPI annotation and existing test owner together. The convention test uses
write capabilities for required unknown-outcome coverage; HTTP method alone
cannot distinguish read POSTs from writes. Graph operationIds render the
inventory name as lowerCamelCase; flat classes retain their explicit ids.

## Error and response coverage

`ErrorCode` owns code spellings and allowed HTTP statuses. The convention test
checks descriptions that name a code against that registry. Separate required
response assertions hold the errors an operation can send: omitting a response
or merely removing its code must fail. A POST is not necessarily a write;
stored-query calls can write, whereas inline query and export calls read.

The final returned `disposition.action` controls retries. Registry defaults
cannot override unknown effects or a recovery intent. In particular, 503 does
not by itself authorize retry, and an uncertain write requires reconciliation
or recovery before another write. The server's error owner tests these runtime
facts; the OpenAPI owner tests their published description.

## Compatibility and coverage

Changing an operationId or schema name changes generated SDK source even when
the wire is unchanged. The released 0.14 contract uses `actor_id` in access
results and the `listing` enum for commit lists. When upgrading from a
prerelease build, update clients and servers together and regenerate SDKs:
earlier 0.14 builds share the header, so admission cannot distinguish their
older shapes. Retired `unpublished` and `unattached` query keys are refused
even when false; they
cannot silently select history. MCP has its own tool-schema migration and
does not require the HTTP contract header. See the
[client migration](../../changelog.d/http-contract-014.breaking.md) and
[MCP guide](../user/operations/mcp.md).

The specification includes graph, graph-management, deployment, probe and
OAuth metadata operations. `/openapi.json` does not list itself. `/mcp` is
described through `tools/list` and the [MCP guide](../user/operations/mcp.md).
`og_catalog` is a reserved graph id documented on `graph_id`; it serves only
inline query and schema requests and refuses other operations.

New collections are paged from their first release. Existing unbounded graph,
branch and query lists, and unpaged commit history, keep their behavior until
the separate pagination design changes it. See the
[HTTP conventions RFC](https://github.com/ModernRelay/omnigraph/pull/1089) for the rename
batch and the [server guide](../user/operations/server.md) for clients.
