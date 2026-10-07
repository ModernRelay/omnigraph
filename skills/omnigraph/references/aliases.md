# Read Aliases

An operator alias binds a personal name and default parameters to a served
stored query. The team's query stays in the applied cluster registry; aliases
contain no `.gq` source and cannot invoke mutations.

```yaml
# ~/.omnigraph/config.yaml
servers:
  production: { url: https://graph.example.com }
aliases:
  person:
    server: production
    graph: knowledge
    query: get_person
    args: [slug]
    format: json
  recent-people:
    server: production
    graph: knowledge
    query: recent_people
    params: { since: "2026-10-01T00:00:00Z" }
    format: json
```

```bash
omnigraph alias person ada
omnigraph alias person --params '{"slug":"grace"}'
omnigraph alias recent-people --format jsonl
```

The server, graph and query name come from the binding. To choose a different
scope or branch, call `query` directly. The `alias` command accepts
`--params`/`--params-file` and `--format`/`--json`; it has its own namespace and
cannot shadow built-in commands. Credentials belong in `omnigraph login`
storage, never alias configuration.

## Parameters

`args` maps positional values to parameter names in order. Each argument is
parsed as JSON first, then as a String:

| Shell argument | Bound value |
|---|---|
| `29` | Integer |
| `'"29"'` | String `29` |
| `true` | Boolean |
| `Ada` | String |
| `'{"x":1}'` | Object |

Explicit `--params` wins over positional arguments, which win over configured
`params`. Parameter types must match the stored query.

## Results and policy

Use JSON when a consumer needs columns or the read's `graph_commit_id`; JSONL
omits those metadata fields. Null property values are omitted from rows and
DateTime output is UTC without a trailing `Z`. See [result values](queries.md#system-fields-and-result-values).

Alias invocation requires the stored-query `invoke_query` and `read` grants.
A stored mutation bound as an alias is refused. Invoke it explicitly instead:

```bash
omnigraph mutate update_person --server production --graph knowledge \
  --params '{"slug":"ada","name":"Ada"}' --json
```

See [stored queries](stored-queries.md) for registry declaration and live updates.
