# Authorization and actors

OmniGraph uses Cedar policy bundles to authorize graph, server, and cluster
configuration actions.
Policies are declared in `cluster.yaml` and installed during cluster bootstrap
or graph creation. Servers load them at startup or when activating a new graph.

## Actions

Graph-scoped actions:

| Action | Covers |
|---|---|
| `read` | Queries, snapshots, branches, and commits |
| `export` | Snapshot export |
| `change` | Mutations, loads, and source metadata probes for `explain load csv` |
| `schema_apply` | Schema changes |
| `branch_create` | Branch creation |
| `branch_delete` | Branch deletion |
| `branch_merge` | Branch merge |
| `commit_publish` | Publication of an unpublished commit onto its recorded target branch |
| `commit_drop` | Drop of an unpublished commit |
| `invoke_query` | Entry to a stored query |
| `admin` | Reserved graph administration |

`graph_list` is server-scoped and controls the metadata catalog at
`GET /graphs`. It does not control the minimal identity-authenticated
`GET /graphs/discovery` route: every valid cluster identity can discover all
applied graph IDs and display names, without gaining permission to read their
schema or contents. See [signed data credentials](server.md#signed-data-credentials).

`config_manage` is cluster-scoped. It authorizes configuration changes through
the identity-authorized cluster API, including stored queries, graph creation,
and policy membership/binding changes on existing graphs. Grants and revocations
are authorized by the currently applied policy, never the proposed policy.
Changing an existing
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
bundle may bind a given scope. The target `og_catalog` binds the
[engine catalogue](../queries/catalogue.md), which every server serves and no
`graphs:` entry declares: its `read` rule decides catalogue reads with the
resource `og_catalog` and no branch, and the bundle is applied and activated
like a graph's.

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
or `unprotected`; a rule may not set both. A branch is protected when the
bundle's `protected_branches` list names it exactly:

```yaml
protected_branches: [main, release]
```

Branch names are compared as written. A `protected_branches` entry that is
empty, has leading or trailing whitespace, or contains a control character
fails policy validation, and a request naming such a branch is refused as a
bad request before any rule is evaluated (see
[branch names](../branching/index.md#branch-names)). Server actions and graph-wide
`invoke_query` rules do not take branch scopes. `commit_publish` and
`commit_drop` take `target_branch_scope`: the scope is the branch the
unpublished commit recorded as its target. Writing an unpublished commit
needs no new action: a mutation `as commit` is a `change` on its branch and a
merge `as commit` is a `branch_merge`, so an actor with ordinary write rights
can omit `as commit`; this is a voluntary review workflow, not an approval
boundary (see [Unpublished graph commits](../../rfcs/2026-10-10-unpublished-graph-commits.md)).
A mutation `as unattached branch` is likewise a `change` on the branch it
records as its target: the first write's branch, and for a write `on "<id>"`
the target the base snapshot recorded, whatever branch the request names.
`commit list --unattached` and a query `--snapshot <id>` of an unattached
snapshot need `read` on that recorded target; `commit show <id>` keeps its
scope-free `read`; `commit drop` takes `commit_drop` on the recorded target as
for an unpublished commit (see [Unattached graph branches](../../rfcs/2026-10-11-unattached-graph-branches.md)).
`branch_create` from a branch checks that branch as the source and the new
branch as the target. A start at a commit (`from_commit`) checks the branch
that wrote the commit as the source; a denial answers `404`, as an id no
commit has does, so the id discloses nothing (see
[start a branch at a commit](../branching/index.md#start-a-branch-at-a-commit)).

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

Edit the policy file or its `applies_to` bindings, then run
`cluster apply --server URL --config DIR`. The server finishes admitted requests
before activating matching HTTP and engine permissions. Later requests use the
new rules. Cluster management policy changes take effect in the same deployment;
a newly granted administrator cannot authorize that grant.

## Learn your access

`GET /graphs/{graph_id}/access` tells the caller what its own credential may do
on a graph, so a client can show only the actions it is allowed instead of
learning them from a refusal:

```bash
curl -H "Authorization: Bearer $TOKEN" -H "Omnigraph-Http-Api: 0.14" \
  "https://graph.example/graphs/knowledge/access?branch=feature&target_branch=main"
```

With the example graph policy above, `act-alice` gets:

```json
{"actor":"act-alice","graph_id":"knowledge","graph_wide":["read","invoke_query"],
 "branch":"feature","target_branch":"main","actions":["read"]}
```

- `graph_wide` is the decision with no branch named: `invoke_query`, and an
  action one rule allows on every branch, with no branch scope, `branch_scope:
  any` or `target_branch_scope: any`. Rules that cover every branch only
  together, one for protected branches and one for the rest, allow each named
  branch but not the decision without one, so read `actions` for a branch.
  `read` here also decides the reads that name no branch: listing branches, the
  schema, `show` and `commit show`. A commit list authorizes `read` on its
  branch, `main` when the query names none.
- With `branch`, `actions` lists the actions allowed on it: `read`, `export`
  and `change`, and `branch_delete`, `commit_publish`, `commit_drop` and
  `schema_apply` with that branch as their target. Schema apply and the
  system-column upgrade target `main` only, and a storage upgrade authorizes
  each live branch, so `schema_apply` on another branch does not make schema
  apply possible there.
- With `target_branch` as well, `actions` also lists `branch_create` from
  `branch` into the target and `branch_merge` of `branch` into it.

The route evaluates the installed policy for the caller's resolved actor, each
action shaped as the route that performs it shapes it, so it answers exactly as
those routes decide. It needs no action of its own, reads no graph data, and
judges branches by name without checking that they exist. It names actions,
never the rules behind them, and reveals nothing about other actors. A static
token on a graph without a policy gets `read` alone, and a signed data
credential without one nothing, as those routes decide; a server without
authentication allows every graph action and reports no `actor`. The MCP
`access` tool returns the same body.

It reports policy actions, not actor views, so a view can still refuse a
`change` the policy allows.

## Actor identity

For HTTP requests, the server maps the bearer token to an actor. Headers,
query parameters, and request bodies cannot override that identity.
Signed credentials use `principal:<immutable-principal-id>`; groups and
permissions come from applied policy. An identity credential contains no
graph/action grants. Version-1 restricted credentials are not accepted.

For direct CLI writes, actor resolution is:

1. `--as <ACTOR>`;
2. `operator.actor` in `~/.omnigraph/config.yaml`;
3. no actor.

When a policy is installed, a missing actor is denied. Only the server
resolves a served write's actor; there `--as` is an assertion the server checks
against the token ([Assert the actor](server.md#assert-the-actor)).

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
