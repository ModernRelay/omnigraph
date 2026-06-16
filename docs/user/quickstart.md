# Quickstart

This walks the core loop end to end: define a schema, initialize a graph, load
data, query it, and use a branch. It uses a local file-backed graph; swap the
path for an `s3://…` URI to run the same flow against object storage.

[Install](install.md) the `omnigraph` CLI first.

## 1. Write a schema

A schema (`.pg`) declares your node and edge types. Save this as `schema.pg`:

```
node Person {
  name: String
  title: String?
}
```

See the [schema language](schema/index.md) for types, constraints, and edges.

## 2. Initialize the graph

```bash
omnigraph init --schema schema.pg graph.omni
```

`init` creates an empty graph at the given URI with your schema applied.

## 3. Load data

`load` is the single bulk-write command. `--mode` is required
(`overwrite | append | merge`):

```bash
omnigraph load --data people.jsonl --mode overwrite graph.omni
```

`people.jsonl` is newline-delimited JSON, one record per line. For finer-grained
or inline writes, see [mutations](mutations/index.md).

## 4. Query

Write a query (`.gq`) — save as `queries.gq`:

```gq
query find_people($title: String) {
  match { $p: Person { title: $title } }
  return { $p.name }
}
```

Run it:

```bash
omnigraph query find_people --query queries.gq \
  --params '{"title":"Engineer"}' --format table --store graph.omni
```

The query name is positional; `--query` points at the `.gq` source and
`--store` addresses the graph's storage directly.

The [query language](queries/index.md) covers `match`/`return`/`order`, and
[search](search/index.md) covers vector and full-text search.

## 5. Work on a branch

Branches isolate changes until you merge them — Git-style, across the whole graph:

```bash
omnigraph branch create review/new-hires graph.omni
omnigraph load --data new-hires.jsonl --mode append --branch review/new-hires graph.omni
# inspect the branch, then integrate it
omnigraph branch merge review/new-hires --into main graph.omni
```

See [branches & commits](branching/index.md) and [merging](branching/merge.md).

## Next steps

- [CLI reference](cli/reference.md) — every command and flag.
- [Schema language](schema/index.md) and [query language](queries/index.md).
- [Operating a cluster](clusters/index.md) and [running the server](operations/server.md)
  for multi-graph, multi-user deployments.
