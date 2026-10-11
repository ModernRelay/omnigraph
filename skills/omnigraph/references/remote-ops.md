# Remote Graph Operations

Remote commands address an `omnigraph-server`; the server executes the graph
operation and the CLI only receives its HTTP response. Network failure can hide
a successful publication, so retry decisions must use graph evidence rather
than the transport status alone.

## Address the graph

Register a named server and keep the token outside project configuration:

```bash
echo "$TOKEN" | omnigraph login production
omnigraph query get_person --server production --graph knowledge \
  --params '{"email":"ada@example.com"}' --json
```

`--server <name>` resolves the URL from `~/.omnigraph/config.yaml`; `--graph`
selects a graph served by that cluster. Do not use the cluster control-plane
`--config` flag on data-plane commands.

## Successful write receipts

With `--json`, a successful effectful data `mutate` or `load` returns `commit` with
the exact `graph_commit_id` and metadata published by that attempt. A successful
mutation that matches nothing returns `"commit": null`.

GQ branch statements are different: inspect `outcome`. Creation/deletion return
null commits despite their branch effects. A merge returns the exact commit it
published, or `"commit": null` when the target was already up to date. See
[branch statements](changes.md#branch-statements).

Persist the receipt with downstream state when a workflow needs an audit or
resume position. Do not infer the published commit by listing history after the
write; another actor may commit in between.

## A 504 means the outcome is unknown

A gateway can time out after the server has published. A `504` therefore does
not prove success or failure:

1. Do not immediately repeat the write.
2. Inspect the target branch and the intended entity effect.
3. Retry only when that evidence proves the original attempt did not land.

Use server addressing for the checks too:

```bash
omnigraph commit list --server production --graph knowledge \
  --branch main --json
omnigraph export --server production --graph knowledge \
  --branch main --type Person > /tmp/people.jsonl
```

For `load --from <base> --branch <review>`, inspect the review branch rather
than assuming branch creation means the load landed. Strict inserts of unkeyed
nodes and edges can duplicate on a blind retry. Mutation `insert` and
`load --mode merge` upsert keyed nodes and keyed edges (`@key(@src, @dst, …)`)
by their derived logical IDs;
`load --mode append` remains strict and reports an ID collision. Verification
is still safer when the requested value matters.

## Conditional mutations

When a mutation is derived from returned rows, protect it with the commit pinned
to those rows:

```bash
omnigraph query find_person --server production --graph knowledge --json
omnigraph mutate update_person --server production --graph knowledge \
  --params '{"name":"Ada"}' --if-commit <graph_commit_id> --json
```

Any intervening branch commit makes the condition fail without effects. The CLI
exits `4`; HTTP returns `412` with `precondition_failure: {expected, actual?}`.
Re-read and decide again. Fetching a head id after the read does not close the
race.

Over HTTP, beside the `Omnigraph-Http-Api: 0.13` header every graph request
needs, send the raw id in the `Omnigraph-If-Graph-Commit` header to
`POST /graphs/{id}/mutate/if-graph-commit` or
`POST /graphs/{id}/queries/{name}/if-graph-commit`. The plain routes reject that
header; an older server answers `404`, so never fall back to the unconditional
route.

## Typed failures and recovery

- `429 Too Many Requests`: the write did not start. Honor `Retry-After`, then
  retry. The CLI marks this case with exit `75` and a `command_outcome` whose
  `action` is `retry`, only when no earlier step of the command took effect.
- Structured `read_set_conflict` means an input changed before publication.
  Refresh and reconsider the operation, including schema/table identity changes;
  an unchanged retry need not succeed. Commit conflicts use generic `conflict`,
  so HTTP `409` alone does not identify a retryable case.
- `key_conflict`: an append or strict insert found an existing id. Decide
  whether that entity is the intended one; do not silently turn the operation
  into an upsert.
- `recovery_required` (HTTP `503` with `recovery_required.operation_id`): no
  0.12 mutation, load or branch merge returns it, because a write that fails
  before its single publication leaves the graph unchanged. It remains for a
  graph that still holds a `__recovery/` sidecar from a pre-0.12 build (open
  it read-write with that build first) and for a schema apply or
  system-column upgrade that fails after its commit was published; the named
  commit already contains the change. Follow the error message's remedy and
  do not replay.

The effect-free conflict details are distinct from a lost response or a recovery
requirement. Neither HTTP `409`/`503` nor the CLI's generic failure exit `1` is
a universal retry signal; conditional precondition failure has its own exit `4`
on data-plane commands (managed `cluster plan`/`apply` use exit `4` for
recovery required instead).

## Read large output safely

Redirect large schemas and exports to a file before inspecting them so an agent
or terminal output cap cannot silently truncate the result:

```bash
omnigraph schema show --server production --graph knowledge > /tmp/schema.pg
wc -l /tmp/schema.pg
```

For ordered downstream consumption and durable cursors, use
[commit changes and change feeds](changes.md) instead of polling commit lists.
