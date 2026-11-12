- A direct `cluster apply` that converges releases its lock once its writes settle, so a server can start without `cluster force-unlock`. Azure keeps it. See [bootstrap a cluster][direct-apply-lock].

[direct-apply-lock]: ../docs/user/clusters/index.md#bootstrap-a-cluster
