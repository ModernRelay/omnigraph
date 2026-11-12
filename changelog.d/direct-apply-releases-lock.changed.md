- A direct `cluster apply` that converges now releases its cluster lock once its writes settle, so a server can start next without `cluster force-unlock`. See [bootstrap a cluster][direct-apply-lock].

[direct-apply-lock]: ../docs/user/clusters/index.md#bootstrap-a-cluster
