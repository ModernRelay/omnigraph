# S3 cluster roots

A cluster whose server is stopped has no served route for deployment, recovery
or maintenance. For that case `cluster plan`, `cluster apply`, `cluster status`,
`cluster force-unlock`, `cluster upgrade-ledger` and the four
[maintenance commands](maintenance.md) (`optimize`, `index build`, `index rebuild`
and `cleanup`, and the deprecated `rebuild-full-text-indexes`) accept an
inline `s3://` cluster root. Maintenance selects its graph with `--graph`. The
direct reads and the writes that create and fill a graph take an inline `s3://`
graph as well ([below](#inline-s3-graphs)). Every other command, and any `az://`
address, refuses an inline cloud target.

```bash
omnigraph --target s3://bucket/company-brain cluster status --json
omnigraph optimize --target s3://bucket/company-brain --graph knowledge --as act-operator
```

A direct write or maintenance command on a graph with an applied policy is
judged by it, so `--as` names an actor the policy grants the command
(`maintain` for `optimize`, `reclaim` for `cleanup`); see
[direct writes](policy.md#actor-identity).

Every direct writer on a root takes the cluster's admission lock. A
maintenance command keeps it after it finishes. A `cluster apply` releases it
after convergence and settlement. If release is unconfirmed, follow
[direct apply recovery](../clusters/index.md#bootstrap-a-cluster).
A stopped server's lock blocks the first direct command,
and each kept lock blocks the next command and the server's boot. Read the held `lock_id` from `cluster status --json`,
establish that its owner has settled, and release that exact ID with
`cluster force-unlock` before the next writer or the server starts.

The destination is captured once from the environment: the region from
`AWS_REGION` or `AWS_DEFAULT_REGION` (required), the endpoint from
`AWS_ENDPOINT_URL_S3`, then `AWS_ENDPOINT_URL` or `AWS_ENDPOINT`, and the
addressing style from `AWS_S3_FORCE_PATH_STYLE` or
`AWS_VIRTUAL_HOSTED_STYLE_REQUEST`. With `AWS_PROFILE` set the endpoint must be
explicit. Unprefixed aliases such as `REGION` or `ENDPOINT`, conflicting
endpoint aliases and S3 Express refuse. Other environment settings supply
authentication only. The stderr target line reports the captured provider
before any I/O. `cleanup --confirm` against an S3 root also needs `--yes`, as
for any non-local target.

## Inline S3 graphs

A graph that no server serves, written by one batch loader and read by anyone
holding the bucket's credentials, needs no server between them. A benchmark
harness publishing its run records beside its store is the case. For it the
direct reads (`query`, `export`, `snapshot`, `diff`, `schema show`,
`branch list`, `branch diff`, `branch merge-base`, `commit list`,
`commit show`, `commit changes`, `blob get`, `blob stat`) and the three writes
that create and fill a graph (`init`, `schema apply`, `load`) accept an inline
`s3://` graph:

```bash
omnigraph init s3://bucket/bench/main/graph --schema schema.pg
omnigraph load --target s3://bucket/bench/main/graph --mode replace --data rows.jsonl
omnigraph query --target s3://bucket/bench/main/graph -e 'query q() { match { $r: BenchmarkRun } return { $r.p50_us } limit 5 }'
omnigraph query --target s3://bucket/cluster --graph results -e '…'
```

Without `--graph` the address is the graph itself. The reads and `load` also
take a cluster root with `--graph`, which selects the graph under it, as for
maintenance; `init` and `schema apply` take the graph's own address only. A
`load` on a graph inside a cluster root is a direct writer on that root: it
takes the cluster's admission lock and keeps it after the command, as
maintenance does. The destination is captured from the environment as
described above. A reader
takes no lock and writes nothing. `load --mode overwrite` against an S3 graph
needs `--yes`, as for any non-local target. Every other write on cloud storage
(`mutate`, `branch create`, `branch delete`, `branch merge`, `commit publish`,
`commit drop`, `blob put`, `blob clear`, `changes`) reaches it through a
server, whose lock and policy guard a served graph; for a graph a server
serves, write through that server.
