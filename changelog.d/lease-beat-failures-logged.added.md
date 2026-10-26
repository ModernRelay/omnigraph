- **Failed cluster lease heartbeats are logged.** A server logs every
  heartbeat request that fails, every failed check for a successor's claim and
  a heartbeat left unanswered at its deadline, each with the failure's class and
  storage kind and before any fence. See the [storage failure
  logs][lease-beat-logs].

[lease-beat-logs]: ../docs/user/operations/storage-failures.md#logs
