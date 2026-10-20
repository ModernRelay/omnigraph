- On S3 cluster roots, the cluster lock now carries a lease renewed by
  heartbeat objects under `__cluster/lease/`. A server restarted after a crash
  takes over its predecessor's lock once the heartbeats have been silent for a
  lease and a half (45 seconds by default, `OMNIGRAPH_CLUSTER_LEASE_SECONDS`),
  so an orchestrator's restart-on-failure policy recovers without
  `cluster force-unlock`. An owner none of whose heartbeats is answered for two
  thirds of a lease stops writing and exits 2. Local and Azure roots keep the
  exact-ID unlock. Older binaries refuse the new version-3 lock record. See
  [restart after a failure][cluster-lock-lease].

[cluster-lock-lease]: ../docs/user/operations/storage-failures.md#restart-after-a-failure
