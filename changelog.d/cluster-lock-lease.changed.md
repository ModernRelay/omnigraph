- On local and S3 cluster roots, the cluster lock now carries a lease renewed by
  heartbeat objects under `__cluster/lease/`. A server restarted after a crash
  takes over its predecessor's lock once the heartbeats have been silent for a
  lease and a half (45 seconds by default, `OMNIGRAPH_CLUSTER_LEASE_SECONDS`),
  so an orchestrator's restart-on-failure policy recovers without
  `cluster force-unlock`. An owner whose heartbeats go unanswered stops writing
  and exits 2. Older binaries refuse the new version-3 lock record. See
  [writer topology][cluster-lock-lease].

[cluster-lock-lease]: ../docs/user/deployment.md#writer-topology
