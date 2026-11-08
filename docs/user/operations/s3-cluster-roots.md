# S3 cluster roots

A cluster whose server is stopped has no served route for deployment, recovery
or maintenance. For that case `cluster plan`, `cluster apply`, `cluster status`,
`cluster force-unlock`, `cluster upgrade-ledger` and the five
[maintenance commands](maintenance.md) (`optimize`, `index build`, `index rebuild`,
`repair` and `cleanup`, and the deprecated `rebuild-full-text-indexes`) accept an
inline `s3://` cluster root. Maintenance selects its graph with `--graph`; a graph
URI is refused. Every other command, and any `az://`
address, refuses an inline cloud target.

```bash
omnigraph --target s3://bucket/company-brain cluster status --json
omnigraph optimize --target s3://bucket/company-brain --graph knowledge
```

Every direct writer on a root takes the cluster's admission lock and keeps it
after it finishes, as a direct `cluster apply` does. A stopped server's lock
blocks the first direct command, and each command's lock blocks the next one
and the server's boot. Read the held `lock_id` from `cluster status --json`,
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
