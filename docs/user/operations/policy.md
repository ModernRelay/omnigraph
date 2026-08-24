# Authorization and actors

OmniGraph uses Cedar policy bundles to authorize graph and server actions.
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

`graph_list` is server-scoped and controls `GET /graphs`.

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

| Tokens | Policy | Startup and authorization |
|---|---|---|
| none | none | Requires explicit `--unauthenticated`; otherwise startup fails |
| configured | none | Only the `read` action is allowed; all other actions are denied |
| configured | configured | The policy decides each action |
| none | configured | Startup fails because no request could establish an actor |

`GET /graphs` is denied unless a `cluster`-scoped policy grants `graph_list`,
including when graph policies exist.

Policy is enforced for graph writes inside the engine as well as at the HTTP
boundary. This keeps direct and embedded writers subject to the same action
checks when a policy engine is installed. Per-entity and per-property
authorization is not currently supported; authorization is graph/branch scoped.
