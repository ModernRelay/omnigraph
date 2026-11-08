# Quickstart

This walks the core loop end to end: define a schema, initialize a graph, load
data, query it, and use a branch. It uses a local file-backed graph. CLI direct
targets support local files only; access hosted graphs through a registered
server or managed target. See [operator configuration](cli/targets.md).

[Install](install.md) the `omnigraph` CLI first.

The example builds a small evidence graph in which sources support a claim.

## 1. Write a schema

A schema (`.pg`) declares your node and edge types. Save this as `schema.pg`:

```pg
node Source {
  slug: String @key
  title: String
}

node Claim {
  slug: String @key
  statement: String
}

edge Supports: Source -> Claim
```

See the [schema language](schema/index.md) for types, constraints, and edges.

## 2. Initialize the graph

```bash
omnigraph init --schema schema.pg graph.omni
```

`init` creates an empty graph at the given URI with your schema applied.

## 3. Load data

Data is newline-delimited JSON, one node or edge per line. Save this as
`evidence.jsonl`:

```jsonl
{"type":"Claim","data":{"slug":"lower-latency","statement":"The migration reduced request latency."}}
{"type":"Source","data":{"slug":"load-test","title":"Load test report"}}
{"type":"Source","data":{"slug":"production-metrics","title":"Production metrics"}}
{"edge":"Supports","from":"load-test","to":"lower-latency"}
{"edge":"Supports","from":"production-metrics","to":"lower-latency"}
```

The keyed `slug` values become node IDs, so the edges can refer to
`load-test`, `production-metrics`, and `lower-latency`. One load can contain
several node and edge types and publishes them as one graph commit. `--mode` is
required (`overwrite | append | merge`):

```bash
omnigraph load --data evidence.jsonl --mode overwrite --target ./graph.omni
```

For finer-grained writes and the node/edge JSONL shapes, see
[mutations and loading](mutations/index.md).

## 4. Query

Write a query (`.gq`) — save as `queries.gq`:

```gq
query sources_for_claim($claim: String) {
  match {
    $source: Source
    $claim_node: Claim { slug: $claim }
    $source supports $claim_node
  }
  return { $source.title as source }
  order { source asc }
}
```

Run it:

```bash
omnigraph query sources_for_claim --query queries.gq \
  --params '{"claim":"lower-latency"}' --format table --target ./graph.omni
```

This returns `Load test report` and `Production metrics`.

The query name is positional; `--query` points at the `.gq` source and
`--target` addresses the graph's storage directly.

The [query language](queries/index.md) covers `match`/`return`/`order`, and
[search](search/index.md) covers vector and full-text search.

## 5. Work on a branch

Branches isolate changes until you merge them — Git-style, across the whole
graph. Save an additional source and evidence link as `benchmark.jsonl`:

```jsonl
{"type":"Source","data":{"slug":"independent-benchmark","title":"Independent benchmark"}}
{"edge":"Supports","from":"independent-benchmark","to":"lower-latency"}
```

`branch` commands reserve their positional argument for the branch name, so
they address the graph with `--target` (the same flag `query` uses above):

```bash
omnigraph branch create review/add-benchmark --target ./graph.omni
omnigraph load --data benchmark.jsonl --mode append \
  --branch review/add-benchmark --target ./graph.omni
omnigraph query sources_for_claim --query queries.gq \
  --params '{"claim":"lower-latency"}' \
  --branch review/add-benchmark --target ./graph.omni
omnigraph branch merge review/add-benchmark --into main --target ./graph.omni
```

See [branches & commits](branching/index.md) and [merging](branching/merge.md).

## Next steps

- [CLI reference](cli/reference.md) — every command and flag.
- [Schema language](schema/index.md) and [query language](queries/index.md).
- [Operating a cluster](clusters/index.md) and [running the server](operations/server.md)
  for multi-graph, multi-user deployments.
