# Stored-Query Registries

A **stored query** is a `.gq` query that the *server* loads, type-checks at startup, and exposes by name — without ever accepting ad-hoc query source from the client. It's how you publish a vetted, typed query surface to remote callers and MCP tools.

It is distinct from CLI `aliases:` (see [`aliases.md`](aliases.md)): an alias
is local client ergonomics; a stored query is a server-published,
policy-gated endpoint.

## Declaring stored queries (`cluster.yaml`)

Stored queries are declared in the cluster's `cluster.yaml` — every `query <name>` in the listed `.gq` files registers:

```yaml
graphs:
  <id>:
    schema: schema.pg
    queries: queries/            # discover every `query <name>` in queries/*.gq
```

`queries` also accepts an explicit file list (`[a.gq, b.gq]`) or a fine-grained `name: { file: … }` map; an unparseable `.gq`, a query that does not type-check against the graph's desired schema, or a duplicate query name fails `cluster validate` (`T83` within one file, `duplicate_query_name` across files). A relative `queries` path must stay inside the config
directory: a `..` segment fails with `config_path_escape`, and a symbolic link
on the path (or a symlinked discovered `.gq` file) fails with
`config_path_symlink`. `cluster apply` publishes them to the content-addressed catalog, and the `--target` server type-checks and serves every applied query. Every applied query is listed.

Registry files must contain only named `query` declarations: a statement
(`branch …`, `commit …`, `load csv`, `show`, `explain`) fails `lint`,
`cluster validate` and server registry loading, and a `set`/`reset` line,
which `lint` accepts, fails `cluster validate` and registry loading. A stored
query runs under the server's process defaults: the invoke body takes no
`settings` field, and `omnigraph query <name>` without `-e`/`--query` refuses
`--set`.

## CLI

```bash
omnigraph queries validate --target .             # all applied graphs
omnigraph queries validate --target . --graph dev # one graph
omnigraph queries list --target . --graph dev     # names and typed params
omnigraph queries list --target prod --graph dev --json # a server's GET /graphs/{graph_id}/queries body
```

- `cluster validate --config .` checks desired `.gq` sources before apply.
  `queries validate` instead audits the **applied** registry against each
  applied schema without restarting the server; it cannot see unapplied source
  edits; it reports every failing query, parse and type refusals together,
  each with its code, position, fix and byte edits. Each declaration of a
  stored file compiles on its own: one that fails under the server's release
  (after a binary upgrade, say) is unavailable with its own diagnostic, one
  that calls it is unavailable naming it, and the others in the file serve,
  while the graph serves: `queries list`, the MCP
  `queries` tool and `GET /graphs/{graph_id}/queries` list it under
  `unavailable` with its diagnostic, `GET /graphs` reports it per graph, and
  invoking it answers 409 `stored_query_unavailable` with the same diagnostic.
  `OMNIGRAPH_STRICT_STORED_QUERIES=1` instead quarantines the graph
  (`invalid_stored_queries`); startup is fatal when no healthy graph remains
  or `--require-all-graphs` is set. To upgrade across GQ 3: validate with the
  new binary, rewrite the failing sources from their fixes (`omnigraph lint
  --fix --config <dir>` applies them), and apply the rewritten registry in the
  same step as the binary switch.
- Both commands take a local cluster root `--target LOCAL_PATH`; `--graph` narrows the
  operation to one declared graph. `queries list` also takes a server or managed target,
  where `--graph` or the target's `default_graph` selects the graph and `--json`
  prints the route's body: its parameters carry `kind` (`string`, `int`, `list`
  with `item_kind`, `vector` with `vector_dim`, ...) rather than the root
  form's GQ `type`. Both forms list a declared `default` (the human form as
  `$k: I32 = 10`, JSON as `"default": 10`, and a declared `= null` as
  `"default": null`; with no declared default the key is absent); a
  parameter with neither `nullable` nor a `default` is required.
- `queries` is distinct from `lint` — `lint` validates a single `.gq` file you point it at; `queries validate` validates the registry the server will actually serve.

## HTTP surface

| Route | Gate | Purpose |
|-------|------|---------|
| `GET /graphs/{graph_id}/queries` | `read` | Typed tool catalog of the served queries. Graph-wide (branch-independent; `read` authorized against `main`). |
| `POST /graphs/{graph_id}/queries/{query_name}` | `invoke_query` (+ `change` for a stored mutation) | Invoke a named query. Body carries `params`, `branch`, `snapshot` and `expect_mutation` only — **never** `.gq` source; any other key is a `400 bad_request` naming it. A stored mutation cannot target a `snapshot` (`400`); a param type error is a structured `400` naming the param. |

The JSON body carries `params` and optionally `branch` (default `main`; the
write target for a stored mutation) or `snapshot` (reads only); `branch` and
`snapshot` are mutually exclusive. There are no `?branch=`/`?snapshot=` query
parameters. Branch/snapshot access stays enforced by the inner `read`/`change`
gate (`invoke_query` itself is graph-scoped, not branch-scoped). The
conditional form is `POST /graphs/{graph_id}/queries/{query_name}/if-graph-commit`; see
[`remote-ops.md`](remote-ops.md).

Stored reads share the [query result contract](queries.md#system-fields-and-result-values):
system identities use `@id`, bare node projections return objects, null fields
are omitted from JSON rows, and DateTime strings are UTC without a trailing
`Z`. A ranked stored read also carries the `ranking` object
([`search.md`](search.md#what-a-ranked-read-reports)). Check consumer expectations when upgrading the stored-query surface.

## Policy gating (`invoke_query`)

- **`invoke_query`** is a per-graph Cedar action gating the whole stored-query invocation surface. Grant it like any other action (see [`server-policy.md`](server-policy.md)).
- **Stored mutations are double-gated:** the caller needs `invoke_query` to reach the query **and** `change` for the write, plus `read` on the branch when the write has a `match`. An actor with `invoke_query` but not `change` gets `403` on a stored mutation.
- **Deny == unknown:** for a caller *lacking* `invoke_query`, a denial and an unknown query name return the **same 404** (identical body) — the catalog can't be probed. A caller who *holds* `invoke_query` may still get a `403` from the inner gate for a query it can't `read`/`change`, so existence is visible to grant-holders by design.
- **Default-deny mode** (bearer tokens, no policy bundle) permits nothing
  (`read` only under the server's `OMNIGRAPH_ALLOW_READS_WITHOUT_POLICY=1`), so
  every `/graphs/{graph_id}/queries/{query_name}` call returns `404` until an
  `invoke_query` rule is configured.

## MCP exposure

Every applied query is listed in `GET /graphs/{graph_id}/queries`, which the
MCP `queries` tool returns whole (`mutation: true` marks a stored mutation).
MCP's `query` tool takes either a stored read's `name` (gated `invoke_query`,
then `read`) or GQ source in `query` (gated `read`, like `POST /query`), and
refuses a stored mutation. `mutate` runs a stored mutation when given its
`name` and no `query` (`invoke_query`, then `change`), through the conditional
route when `if_graph_commit` is given. `/mcp` lists every tool whatever the
caller's policy; a denied call returns the route's refusal.
There is no per-query `mcp:` flag in cluster mode.

## Note on per-query authorization

The catalog is not Cedar-filtered per query: a caller with `read` but not
`invoke_query` can list a query it cannot invoke (invocation would 404).
The catalog is a discovery surface and `invoke_query` is the invocation gate.
