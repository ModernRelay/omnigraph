# Command Reference

Use `omnigraph <command> --help` for the complete flags. Examples below use
`graph.omni` for a standalone store and `production` for a configured server.
Read the linked workflow before an effectful operation.

## Addressing and credentials

| Scope | Selector |
|---|---|
| Served graph | `--server <name|url> --graph <id>` |
| Direct graph | `--store <path|file://|s3://|az://>`, or a positional storage URI where supported |
| Applied cluster | `--cluster <root> --graph <id>` for commands that support it |
| Operator profile | `--profile <name>` or `OMNIGRAPH_PROFILE` |
| Source bundle / managed context | `--config <dir>` on `cluster` and `use` |

`--store`, a positional storage URI and `--server` are mutually exclusive.
Query/mutate reserve their positional argument for the query name. An HTTP URL
is addressed with `--server`, never as a positional graph URI. Operator defaults
supply otherwise unspecified targets; explicit scopes override them.

```bash
echo "$TOKEN" | omnigraph login production
omnigraph logout production
omnigraph login --api https://control.example
omnigraph logout --api https://control.example
```

`OMNIGRAPH_HOME` relocates the auto-discovered operator directory (normally
`~/.omnigraph`). Named server tokens are separate from `config.yaml`; managed
API sessions/data credentials use the OS keychain. Managed folder routing and
`--direct` are described in [cluster](cluster.md#managed-clusters).

## Query and mutate

```bash
omnigraph query get_person --server production --graph knowledge \
  --params '{"slug":"ada"}' --json
omnigraph query get_person --query queries/people.gq --store graph.omni \
  --params '{"slug":"ada"}' --json
omnigraph mutate update_person --server production --graph knowledge \
  --params '{"slug":"ada","name":"Ada"}' --if-commit COMMIT_ID --json
omnigraph query -e 'query q() { match { $p: Person } return { $p.@id } limit 5 }' \
  --store graph.omni --json
```

Omit `--query`/`-e` only for a served stored query. `--params-file` can replace
inline JSON. Reads select `--branch` or `--snapshot`; writes select `--branch`.
`--as` attributes direct writes; served writes refuse it. For grammar and
conditional-write semantics, see [queries](queries.md) and [changes](changes.md).

## Inspect state

```bash
omnigraph snapshot --store graph.omni --branch main --json
omnigraph schema show --store graph.omni --json
omnigraph export --store graph.omni --branch main --type Person > people.jsonl
omnigraph graphs list --server production --json
```

Export streams JSONL; repeat `--type` to include several types, or omit it for
all entities. It is not query-result JSON. `/graphs` includes availability and
requires `graph_list`; `graphs list --discovery` uses an identity credential and
returns only IDs/names. See [server and policy](server-policy.md).

## Branches, commits and changes

```bash
omnigraph branch create review --from main --store graph.omni --json
omnigraph branch list --store graph.omni --json
omnigraph branch merge review --into main --delete-branch --store graph.omni --json
omnigraph branch delete review --store graph.omni --json
omnigraph commit list --store graph.omni --branch main --json
omnigraph commit show COMMIT_ID --store graph.omni --json
omnigraph commit changes COMMIT_ID --store graph.omni --json
omnigraph changes poll --start now --store graph.omni --json
omnigraph changes poll --cursor CURSOR --store graph.omni --json
omnigraph changes baseline --out snapshot.jsonl --store graph.omni --json
```

Merge receipts name the exact published commit. Feed cursors differ from page
tokens; save a baseline cursor only after consuming the complete snapshot.
See [data review](data.md) and [change feeds](changes.md).

## Blob reads

```bash
omnigraph blob stat node Document manual content --store graph.omni --json
omnigraph blob get node Document manual content --store graph.omni --out manual.bin
omnigraph blob get node Document manual content --store graph.omni --offset 0 --length 4096
```

The selector is `<node|edge> <TYPE> <ID> <PROPERTY>`, with either `--branch` or
`--snapshot`. Read [Blobs](blobs.md) for ranges and external-reference handling.

## Schema, load and embeddings

```bash
omnigraph lint --schema schema.pg --query queries/people.gq --json
omnigraph schema plan --schema next.pg --store graph.omni --json
omnigraph schema apply --schema next.pg --store graph.omni
omnigraph init --schema schema.pg graph.omni
omnigraph load --data seed.jsonl --mode merge --store graph.omni --json
omnigraph load --data delta.jsonl --mode merge --from main --branch review \
  --server production --graph knowledge --json
omnigraph embed --input raw.jsonl --output embedded.jsonl --spec embeddings.json
```

`schema plan`/`apply` and `init` are for standalone stores; cluster-managed
schemas use cluster apply. `init` has no `--json`; `--force` only replaces orphan
schema artifacts and never overwrites an initialized graph. Load requires an
explicit `--mode`; embeddings are a separate file transformation. See
[schema](schema.md), [data](data.md) and [search](search.md).

## Maintenance

Run direct maintenance with overlapping writers stopped. For a cluster, use
`--cluster ROOT --graph ID`, stop the server and transfer the exact writer lock
as described in [cluster](cluster.md#recovery).

### Optimize

```bash
omnigraph optimize --store graph.omni --json
```

Compacts fragments, including Blob-bearing data, and reconciles declared
scalar/vector indexes and derived traversal state. It preserves retained
versions. Existing full-text indexes remain usable; explicit rebuild owns
analyzer compatibility and full-text coverage changes.

### Rebuild full-text indexes — explicit analyzer upgrade

```bash
omnigraph rebuild-full-text-indexes --store graph.omni --branch main --json
```

Preserve a verified backup and run on every live branch that needs incompatible
full-text indexes rebuilt. The default English analyzer replaces custom tokenizer
settings. Inspect `branch`, `graph_commit_id`, `rebuilt_indexes` and `warnings`;
a no-op does not migrate other branches or old snapshots. See [upgrades](migrations.md).

### Cleanup

```bash
omnigraph cleanup --store graph.omni --keep 5 --older-than 7d --confirm
```

Prunes history and table versions no retained commit needs. At least one of
`--keep`/`--older-than` is required; with both, a commit must be outside both
windows. Units are `s`, `m`, `h`, `d`, `w`. Retention always protects live heads,
required merge bases and tagged snapshots. Quiesce readers that depend on
history about to be removed. Without `--confirm`, cleanup previews its work.

## Applied registries and policy

```bash
omnigraph queries validate --cluster .
omnigraph queries list --cluster . --graph knowledge
omnigraph policy validate --cluster . --graph knowledge
omnigraph policy test --cluster . --graph knowledge --tests policy.tests.yaml
```

These inspect applied state; validate desired files with `cluster validate`.
See [stored queries](stored-queries.md) and [policy](server-policy.md).

## Cluster commands

```bash
omnigraph cluster validate --config .
omnigraph cluster plan --server production --config . --json
omnigraph cluster apply --server production --config . --no-wait --json
omnigraph cluster status --server production --deployment-id ID --wait --json
omnigraph cluster observe --config . --json
```

Direct bootstrap, stopped recovery, ledger conversion and managed service
commands are owned by [cluster](cluster.md). Use its procedures rather than
substituting direct storage access for served apply.

## Output

Reads accept `--json` or `--format table|kv|csv|jsonl|json`. JSON contains
metadata, columns and rows; JSONL has a metadata line followed by rows, but does
not carry the full column list or read commit position. Use JSON for conditional
write workflows. The parser lists `arrow`, but it has no text renderer; do not
set it as an output default.

Mutation/load and other commands advertising it use `--json`, not `--format`.
Store the exact receipt with downstream state. For large schemas or exports,
redirect to a file and check command success before consuming it.

For non-local destructive commands, noninteractive execution needs explicit
`--yes` where advertised (`cleanup` additionally uses `--confirm`). Use `/readyz`
for rollout readiness and `/healthz` for process health.
