# CLI guide

Upgrading an existing installation? Start with the
[0.13 → 0.14 checklist](../operations/upgrade-0.14.md) for before/after YAML
and command examples.

Select resources with `--target NAME|LOCAL_PATH`, or `defaults.target` when
omitted. Account operations use `--profile NAME` or `defaults.profile`.
These selectors are mutually exclusive. Source files select what to run; they
never choose where to run it.

```bash
# Open a graph directly; no registration needed.
omnigraph query sources_for_claim --query queries.gq \
  --params '{"claim":"lower-latency"}' --target ./graph.omni

# Run a stored query through a registered server target.
omnigraph query sources_for_claim --target prod --graph knowledge \
  --params '{"claim":"lower-latency"}'

# Remote endpoints require a registered target.
omnigraph graphs list --target prod
```

A bare word is a registered name. Write `./local` for a relative path named
`local`. A direct root or server needs `--graph ID` for graph operations unless
its registered target has `default_graph`. A direct graph already identifies
the graph and rejects `--graph`. Commands reject incompatible backend kinds
before making a request.

The CLI prints its resolved destination to stderr before mutations. An invalid
explicit target fails; it never falls back to the default. Folder context,
credentials and discovery responses cannot change the destination.

Run `omnigraph <command> --help` for available flags. The
[CLI reference](reference.md) covers the complete command surface, and
[operator configuration](targets.md) is the primary reference for profiles,
targets, authentication providers and the breaking format change.

## Create, load, and query a graph

```bash
omnigraph init ./graph.omni --schema schema.pg
omnigraph load --target ./graph.omni --data evidence.jsonl --mode overwrite
omnigraph query sources_for_claim --target ./graph.omni \
  --query queries.gq --params '{"claim":"lower-latency"}' --format table
omnigraph mutate add_source --target ./graph.omni --query mutations.gq \
  --params '{"slug":"incident-review","title":"Incident review"}'
```

`init URI` is the positional shorthand for an inline direct graph target.
Other graph commands take their destination through `--target`.

`load` requires a mode:

- `overwrite` replaces each node or edge type represented in the batch. Types
  absent from the batch are unchanged.
- `append` inserts new entities and rejects duplicate IDs.
- `merge` inserts new IDs and updates existing IDs.

For a stored query, omit `--query`; the positional name selects the query from
the selected backend's registry.

## Work with branches

```bash
omnigraph branch create review/new-data --from main --target ./graph.omni
omnigraph load --target ./graph.omni --data batch.jsonl --mode merge \
  --branch review/new-data
omnigraph query inspect --target ./graph.omni --query review.gq \
  --branch review/new-data
omnigraph branch merge review/new-data --into main --target ./graph.omni
```

Branch operations are also GQ statements. Statements name their branches
internally and take no `--branch`:

```bash
omnigraph mutate -e 'branch create "review/new-data" from main' --target ./graph.omni
omnigraph query -e 'branch list' --target ./graph.omni --format table
omnigraph mutate -e 'branch merge "review/new-data" into main' --target ./graph.omni
```

See [Branches and commits](../branching/index.md).

## Session settings

`--set NAME=VALUE`, repeatable, gives a [session
setting](../queries/index.md#session-settings) a value for one invocation. A
`set` line in source applies afterward, so the file wins.

```bash
omnigraph query reports --query queries.gq --set merge_lineage=off --target ./graph.omni
omnigraph branch merge review/new-data --into main --set merge_lineage=verify --target ./graph.omni
```

`query`, `mutate` and `branch merge` apply settings to their operation.
`commit changes` and `changes poll` validate settings but they currently have
no effect. Direct `load` applies settings to its staged write; served `load`
refuses `--set` because its HTTP contract has no settings field.

A direct invocation accepts process settings from `--set`, source and the
environment. A server or managed invocation refuses process settings in a
request; those belong to the serving process. A stored query without `-e` or
`--query` takes no `--set` and uses process defaults.

## Read and write Blob values

```bash
omnigraph blob get node Document doc-42 body --target ./graph.omni --out body.bin
omnigraph blob stat node Document doc-42 body --target ./graph.omni --json
omnigraph blob put node Document doc-42 body --target ./graph.omni --file body.bin
```

`blob get` writes bytes to stdout when `--out` is omitted; `--offset` and
`--length` select a range. The CLI reports external references without
following them. `blob put` reads stdin when `--file` is omitted; `blob clear`
sets a nullable cell to null. Both accept `--if-match`. See
[Blob values](../blobs.md).

## Inspect and maintain a graph

```bash
omnigraph snapshot --target ./graph.omni --json
omnigraph commit list --target ./graph.omni --json
omnigraph schema show --target ./graph.omni
omnigraph optimize --target ./graph.omni
omnigraph repair --target ./graph.omni
omnigraph cleanup --target ./graph.omni --keep 10 --older-than 7d
omnigraph cleanup --target ./graph.omni --keep 10 --older-than 7d --confirm
```

Maintenance uses direct storage. For a graph in a direct root, use
`--target ./cluster-root --graph knowledge`. Recovery effects that require an
explicit destination refuse a default-only selection. Read the
[maintenance guide](../operations/maintenance.md) before repair or cleanup.

## Use a server

Configure a server target with `auth: {provider: token, source: {store: {}}}` as shown
in [operator configuration](targets.md), then install its token explicitly:

```bash
printf '%s\n' "$SERVER_TOKEN" | omnigraph credentials set --target standalone --stdin
omnigraph graphs list --target standalone
omnigraph query sources_for_claim --target standalone --graph knowledge \
  --params '{"claim":"lower-latency"}'
omnigraph credentials status --target standalone --json
```

The server target's declared `auth` is the only credential source. There is no
implicit environment override or plaintext credential fallback. `login` and `logout`
manage platform sessions; static tokens use `credentials set/remove`.
The server resolves the actor from authentication; on a served write `--as`
only asserts it. Register remote endpoints rather than passing inline URLs.
### Use a server through a Unix socket

Operator version 1 does not accept `unix://` targets. Register an HTTPS server
endpoint instead; anonymous loopback HTTP is the only plain-HTTP exception.
Other clients with Unix socket support may use a
[TCP relay gateway](../deployment.md#unix-socket-access). This does not enable
Unix socket access in the current CLI.

## Manage a cluster

`--config DIR` selects a directory containing `cluster.yaml`; it defaults to
`.` for direct and server source operations. `.` means the current directory.
`--target` independently selects the direct root, server or managed cluster.

```bash
omnigraph cluster validate --config ./company-brain
omnigraph cluster plan --config ./company-brain --target ./cluster-root
omnigraph cluster apply --config ./company-brain --target ./cluster-root
omnigraph cluster status --target ./cluster-root
```

Validation is local and takes no target. Plan reads the selected destination
without acquiring writer admission. Apply owns exclusive admission for direct
storage or submits to the selected server. It manages graph definitions,
schemas, stored queries, policies, embedding providers and external Blob rules.
An optional `storage` in the bundle must agree with the target. See
[deployment boundaries](../clusters/index.md#deployment-boundaries).

An explicit source path distinguishes a fresh direct apply with a caller-chosen
ID from source-free observation of that ID:

```bash
omnigraph cluster apply --target ./cluster-root --config . --deployment-id ID
omnigraph cluster apply --target ./cluster-root --deployment-id ID
```

Reusing an existing ID only observes its original invocation. It never executes
it again. Recovery that needs proof of stopped writers additionally requires
`--writers-stopped` and an explicit target.

For managed operations, register a profile and then a target using
service-attested cluster identity:

```bash
omnigraph login --create-profile work
omnigraph cluster list --profile work --json
omnigraph target add cloud --profile work --cluster CLUSTER_ID
omnigraph cluster plan --target cloud --json > plan.json
omnigraph cluster apply --target cloud --plan "$(jq -r .data.preview_id plan.json)" --json
omnigraph cluster status --target cloud --json
```

Managed planning uses pushed configuration. It takes no local `--config`;
`cluster push --config DIR` uploads the declared sources. See
[managed data access](managed-data.md), [team administration](managed-team.md)
and the [managed command reference](reference.md#managed-cluster-commands).

## Output and errors

For `query` and `alias`, output selection follows `--json`, then `--format`,
then operator `defaults.output`, then `table`. Aliases have no format field.
Structured execution errors use the selected JSON, JSONL, or TOON format. Mutations
require explicit `--json` for structured errors. Target diagnostics go to
stderr and cannot be suppressed by a quiet flag.

Each invocation reads operator configuration once when needed. Edits cannot
change its destination or presentation defaults during execution. Unknown
fields, duplicate YAML keys and legacy routing sections are errors. Local
commands such as help, version, source validation and skill export do not load
operator configuration.

Output failures return nonzero without undoing completed work or automatically
repeating it. An accepted managed operation retains its recovery identity even
if its normal report cannot be delivered. See
[output formats](reference.md#query-inputs-and-output) and
[diagnostics](../queries/diagnostics.md).

## Give an agent the matching skill

```bash
omnigraph skill export ~/.claude/skills/omnigraph --force --json
omnigraph skill show references/queries.md
```

Running `omnigraph` or `omnigraph help` says to call that skill before a first schema or query. The binary carries the agent skill matching its commands and HTTP contract.
Export refuses existing files unless `--force`; a later forced export removes
obsolete recorded files only while their bytes remain unchanged. Neither
command reads operator configuration or contacts a server.
