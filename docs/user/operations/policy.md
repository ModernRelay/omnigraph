# Authorization and actors

OmniGraph uses Cedar policy bundles to authorize graph, server, and cluster
configuration actions.
Policies are declared in `cluster.yaml`, applied with the cluster, and loaded
when the server starts.

## Actions

Graph-scoped actions:

| Action | Covers |
|---|---|
| `read` | Queries, snapshots, branches, and commits |
| `export` | Snapshot export |
| `change` | Mutations and loads |
| `schema_apply` | Schema changes |
| `branch_create` | Branch creation |
| `branch_delete` | Branch deletion |
| `branch_merge` | Branch merge |
| `invoke_query` | Entry to a stored query |
| `admin` | Reserved graph administration |

`graph_list` is server-scoped and controls the metadata catalog at
`GET /graphs`. It does not control the minimal identity-authenticated
`GET /graphs/discovery` route: every valid cluster identity can discover all
applied graph IDs and display names, without gaining permission to read their
schema or contents. See [signed data credentials](server.md#signed-data-credentials).

`config_manage` is cluster-scoped. It authorizes configuration changes through
the identity-authorized cluster API, including policy membership, stored
queries, graph creation, and a new graph's initial schema. Changing an existing
graph's schema requires that graph's `schema_apply` permission;
reading its remote schema or migration preview requires `read` on `main`.
The reserved graph `admin` action does not grant cluster management.

A stored mutation requires both `invoke_query` and `change`. A stored read
requires `invoke_query` and `read`.

## Bind a policy

```yaml
# cluster.yaml
policies:
  graph-access:
    file: graph.policy.yaml
    applies_to: [knowledge]
  server-access:
    file: server.policy.yaml
    applies_to: [cluster]
```

A bundle may target graph IDs or the `cluster` scope, but not both. Only one
bundle may bind a given scope.

Example graph policy:

```yaml
version: 1
groups:
  readers: [act-alice, act-bob]
rules:
  - id: readers-can-read
    allow:
      actors: { group: readers }
      actions: [read]
      branch_scope: any
  - id: readers-can-invoke
    allow:
      actors: { group: readers }
      actions: [invoke_query]
```

A cluster-bound bundle can contain separate configuration and inventory rules:

```yaml
version: 1
groups:
  owners: [principal:alice]
rules:
  - id: owners-manage-config
    allow:
      actors: { group: owners }
      actions: [config_manage]
  - id: owners-list-graphs
    allow:
      actors: { group: owners }
      actions: [graph_list]
```

`config_manage` uses the Cedar `Cluster::"root"` resource; `graph_list` uses
`Server::"root"`. Put them in separate rules and do not give either a branch
scope. Bind this file with `applies_to: [cluster]` as above.

For an identity-authorized apply, the **current applied policy** authorizes all
planned effects before any graph or configuration effect. A proposed policy
cannot give its author permission to install itself. A saved plan does not
transfer its author's permissions: another caller must pass the current policy
checks. Explicit first initialization can install a declared initial management
policy under an exact bootstrap capability; missing policy on an existing
cluster is an error, not permission to bootstrap again. Existing storage-holder
cluster APIs keep their explicit trust boundary.

Graph rules may use `branch_scope` for a source branch or
`target_branch_scope` for a destination branch. Values are `any`, `protected`,
or `unprotected`; a rule may not set both. Server actions and graph-wide
`invoke_query` rules do not take branch scopes.

## Validate and test

Policy commands read the applied bundle from a cluster:

```bash
omnigraph policy validate --cluster ./company-brain --graph knowledge
omnigraph policy test --tests policy.tests.yaml \
  --cluster ./company-brain --graph knowledge
omnigraph policy explain \
  --cluster ./company-brain --graph knowledge \
  --actor act-alice --action read --branch main
```

Run `cluster apply` and restart servers after changing a policy source.

## Actor identity

For HTTP requests, the server maps the bearer token to an actor. Headers,
query parameters, and request bodies cannot override that identity.
Signed credentials use `principal:<immutable-principal-id>`; groups and
permissions come from applied policy. An identity credential contains no
graph/action grants. Legacy restricted credentials retain an additional
ceiling; they cannot override a policy denial.

For direct CLI writes, actor resolution is:

1. `--as <ACTOR>`;
2. `operator.actor` in `~/.omnigraph/config.yaml`;
3. no actor.

When a policy is installed, a missing actor is denied. Served writes reject
`--as` because only the server may resolve their actor.

Successful graph commits record the actor for the whole atomic change. Inspect
the audit trail with:

```bash
omnigraph commit list --store ./graph.omni --json
omnigraph commit show <COMMIT_ID> --store ./graph.omni --json
```

## Server startup modes

For static token authentication:

| Tokens | Policy | Startup and authorization |
|---|---|---|
| none | none | Requires explicit `--unauthenticated`; otherwise startup fails |
| configured | none | Only the `read` action is allowed; all other actions are denied |
| configured | configured | The policy decides each action |
| none | configured | Startup fails because no request could establish an actor |

`GET /graphs` is denied unless a `cluster`-scoped policy grants `graph_list`,
including when graph policies exist.

Signed-token trust also enables authenticated startup. Signed credentials
require an explicit policy permit for protected operations even when no
static tokens are configured. Only the minimal identity discovery route is
independent of policy membership; it exposes existence, not graph access.

Policy is enforced for graph writes inside the engine as well as at the HTTP
boundary. This keeps direct and embedded writers subject to the same action
checks when a policy engine is installed. Per-entity and per-property
authorization is not currently supported; authorization is graph/branch scoped.
