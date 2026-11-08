- A fresh, empty S3 cluster initialized through `omnigraph-cluster::bootstrap_serving` can transfer its retained ownership to the first server with `--bootstrap-handoff FILE`. The bounded, exact receipt is checked before listening; failed or replayed claims never fall back to ordinary startup. See [first boot from a bootstrap receipt][bootstrap-serving-handoff].

[bootstrap-serving-handoff]: ../docs/user/clusters/index.md#first-boot-from-a-bootstrap-receipt
