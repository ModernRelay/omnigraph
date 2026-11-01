- A restarted server no longer takes the S3 cluster lock from a server that is still heartbeating, and a server whose lock was taken over or unlocked stops at its next heartbeat. See [lease restarts][lease-live-owner].

[lease-live-owner]: ../docs/user/operations/storage-failures.md#restart-after-a-failure
