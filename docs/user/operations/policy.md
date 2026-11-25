# Authorization and actors

Omnigraph uses Cedar policy bundles to authorize graph, server, and cluster
configuration actions.
Policies are declared in `cluster.yaml` and installed by `cluster apply`.
Servers load applied policies at startup and activate policy changes during
live deployment, including changes to existing graphs.

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
| `branch_claim` | Claim of a branch's [writer epoch](troubleshooting.md#writer-epoch) |
| `commit_publish` | Publication of an unpublished commit onto its recorded target branch |
| `commit_drop` | Drop of an unpublished commit |
| `maintain` | Content-preserving maintenance: optimize, index builds and the full-text rebuild |
| `reclaim` | Cleanup, which permanently removes table versions and files no retained commit needs |
| `invoke_query` | Entry to a stored query |
| `admin` | Reserved graph administration |

Listing needs no permit. `GET /graphs/discovery` (the CLI's `graphs list`,
MCP's `graphs`) answers every credential with its rule, `visibility: granted`,
the actor it resolves to (`actor_id`, and `client_id` for an OIDC access
token) and the IDs and display names of the registered graphs it holds any
permission on: some action, on any branch,
by the rules every request obeys, available or not. A rule scoped to
unprotected branches counts, and so does a grant without `read`, such as a
writer's or a stored-query invoker's. A holder of `graph_list` lists every
graph. A caller with no permission anywhere gets an empty list. It is the
identity check for any credential, and listing a graph grants nothing; ask
[`access`](#learn-your-access) what you may do on it. A body without
`visibility` comes from an older release, which lists every graph and names no
caller; `graphs list` reports it as `unfiltered`.

A registered graph is listed exactly when its routes do not answer
`graph not found`. Every route under `/graphs/{graph_id}` and every MCP tool
answers a graph the caller does not list exactly as an unknown graph: the same
404 body, `{"error":"graph not found","code":"not_found"}` with disposition
`effects: none`, `action: fix`, so a probe cannot tell them apart. That refusal
does not say that a grant would help. After its policy changes, a caller lists
the graphs again rather than retrying the refused request. A caller that lists
a graph reaches its routes and gets 403 for an action it lacks; a subresource
of a listed graph, such as an absent branch or a stored query hidden from a
caller without `invoke_query`, can still answer 404 with its own message. The
built-in catalogue graph `og_catalog` is not registered: it is never listed
and keeps its own `read` authorization.

`graph_list` is server-scoped and grants the catalog: `GET /graphs`, every
registered graph with its root and availability. A holder lists every graph,
so it is told that a graph is unavailable (503) or that an action is refused
(403), rather than that it does not exist. See
[signed data credentials](server.md#signed-data-credentials).

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

`maintain` and `reclaim` are separate from `change`, so an automation account
can compact a graph without the right to write data, and can hold `maintain`
without the right to delete history. `maintain` is judged on the branch the
operation maintains (`main` for optimize) and takes
`branch_scope`; `reclaim` covers every branch's history and takes no branch
scope. `change` admits no maintenance: index builds and the full-text rebuild
need `maintain` on their branch. The engine checks these actions whenever a policy is installed on the
graph handle, which a direct command on a cluster graph does (see
[direct writes](#actor-identity)).

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
bundle may bind a given scope. `cluster validate` refuses a configuration that
breaks either rule, and the server, direct writes, `access` and the `policy`
commands refuse an applied state that breaks one, so they always select the
same bundle for a graph. The target `og_catalog` binds the
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
`invoke_query` and `reclaim` rules do not take branch scopes. `commit_publish` and
`commit_drop` take `target_branch_scope`: the scope is the branch the
unpublished commit recorded as its target. `branch_claim` takes
`target_branch_scope` too: the scope is the branch the claim names. Writing an unpublished commit
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

Policy commands read the applied bundles from a cluster and ask the bundle a
server asks: graph actions go to the bundle bound to `--graph`, and
`graph_list` and `config_manage` go to the bundle bound to `cluster`.
`validate` checks each of the two that is applied, as its own kind; `test`
runs each case against the bundle for its action. An action whose scope has
no applied bundle is refused; the other scope's bundle never answers it.

```bash
omnigraph policy validate --target ./company-brain --graph knowledge
omnigraph policy test --tests policy.tests.yaml \
  --target ./company-brain --graph knowledge
omnigraph policy explain \
  --target ./company-brain --graph knowledge \
  --actor act-alice --action read --branch main
```

Edit the policy file or its `applies_to` bindings, then run
`cluster apply --target NAME --config DIR` using a registered server target.
The server finishes admitted requests before activating matching HTTP and
engine permissions. Later requests use the
new rules. Cluster management policy changes take effect in the same deployment;
a newly granted administrator cannot authorize that grant.

## Learn your access

`GET /graphs/{graph_id}/access` tells the caller what its own credential may do
on a graph it lists, so a client can show only the actions it is allowed
instead of learning them from a refusal. On a graph the caller does not list it
answers 404 like every other route; `GET /graphs/discovery` names the caller's
actor whatever it may do:

```bash
curl -H "Authorization: Bearer $TOKEN" -H "Omnigraph-Http-Api: 0.14" \
  "https://graph.example/graphs/knowledge/access?branch=feature&target_branch=main"
```

With the example graph policy above, `act-alice` gets:

```json
{"actor_id":"act-alice","graph_id":"knowledge","graph_wide":["read","invoke_query"],
 "branch":"feature","target_branch":"main","actions":["read"]}
```

- `client_id` appears when the credential is a verified OIDC access token: the
  OAuth client it was issued to. The actor stays the person's
  `principal:<id>`, so policy decides alike for every client a person uses.
- `graph_wide` is the decision with no branch named: `invoke_query`, and an
  action one rule allows on every branch, with no branch scope, `branch_scope:
  any` or `target_branch_scope: any`. Rules that cover every branch only
  together, one for protected branches and one for the rest, allow each named
  branch but not the decision without one, so read `actions` for a branch.
  `read` here also decides the reads that name no branch: listing branches, the
  schema, `show` and `commit show`. A commit list authorizes `read` on its
  branch, `main` when the query names none.
- With `branch`, `actions` lists the actions allowed on it: `read`, `export`,
  `change` and `maintain` (served optimize asks it on `main`), and
  `branch_delete`, `commit_publish`, `commit_drop` and `schema_apply` with that
  branch as their target. Schema apply and the
  system-column upgrade target `main` only, and a storage upgrade authorizes
  each live branch, so `schema_apply` on another branch does not make schema
  apply possible there. `branch_claim` is not listed: no command or route
  performs it in this release.
- With `target_branch` as well, `actions` also lists `branch_create` from
  `branch` into the target and `branch_merge` of `branch` into it.

The route evaluates the installed policy for the caller's resolved actor, each
action shaped as the route that performs it shapes it, so it answers exactly as
those routes decide. It needs no action of its own, reads no graph data, and
judges branches by name without checking that they exist. It names actions,
never the rules behind them, and reveals nothing about other actors. A
credential granted nothing on a graph, including every credential on a graph
without a policy, is answered 404, as those routes are (a static token gets
`read` when the server sets `OMNIGRAPH_ALLOW_READS_WITHOUT_POLICY=1`); a server
without authentication allows every graph action and reports `actor_id: null`. The MCP
`access` tool returns the same body, and `omnigraph access` prints it for a
server or managed target ([what you may do at a target](../cli/targets.md#what-you-may-do-at-a-target)).
For a direct cluster target it evaluates the policy the cluster applied to the
graph, which also judges the CLI's direct writes and maintenance there. Direct
reads are not judged: storage access is their trust boundary.

It reports policy actions, not actor views, so a view can still refuse a
`change` the policy allows. It does not report `reclaim`, since no server
route runs cleanup.

## Actor identity

For HTTP requests, the server maps the bearer token to an actor. Headers,
query parameters, and request bodies cannot override that identity.
Signed credentials use `principal:<immutable-principal-id>`; groups and
permissions come from applied policy. An identity credential contains no
graph/action grants. Version-1 restricted credentials are not accepted.

For direct CLI writes, actor resolution is:

1. `--as <ACTOR>`;
2. no actor.

The version-1 operator document has no actor field.

A direct write to a cluster graph, through its root target with `--graph`
or through its own store, installs the policy the cluster applied to that
graph, the policy `omnigraph access` reports for that target. Every write and
maintenance command is then judged by it before any effect, and a missing actor is
denied. A standalone graph has no policy. A direct actor is self-declared, so
this keeps a storage owner within the policy rather than authenticating them:
storage permissions remain the trust boundary. A refused direct write, like
every direct writer, keeps the cluster's admission lock
([S3 cluster roots](s3-cluster-roots.md)). Only the server resolves a
served write's actor; there `--as` is an assertion the server checks against
the token ([Assert the actor](server.md#assert-the-actor)).

Successful graph commits record the actor for the whole atomic change. Inspect
the audit trail with:

```bash
omnigraph commit list --target ./graph.omni --json
omnigraph commit show <COMMIT_ID> --target ./graph.omni --json
```

## Server startup modes

For static token authentication:

| Tokens | Policy | Startup and authorization |
|---|---|---|
| none | none | Requires explicit `--unauthenticated`; otherwise startup fails |
| configured | none | Every action is denied, reads included; `OMNIGRAPH_ALLOW_READS_WITHOUT_POLICY=1` lets static tokens `read` ([authentication](server.md#authentication)) |
| configured | configured | The policy decides each action |
| none | configured | Startup fails because no request could establish an actor |

`GET /graphs` is denied unless a `cluster`-scoped policy grants `graph_list`,
including when graph policies exist. `GET /graphs/discovery` lists the graphs
each caller holds a permission on, in every row: with `--unauthenticated`, every graph except
one whose configuration or policy was refused at startup; none under default
deny; and the graphs with no policy for a static token when
`OMNIGRAPH_ALLOW_READS_WITHOUT_POLICY=1`. A graph a caller does not list
answers it 404.

Signed-token trust also enables authenticated startup. Signed credentials
require an explicit policy permit for protected operations even when no
static tokens are configured.

Policy is enforced for graph writes inside the engine as well as at the HTTP
boundary. This keeps direct and embedded writers subject to the same action
checks when a policy engine is installed. Per-entity and per-property
authorization is not currently supported; authorization is graph/branch scoped.
