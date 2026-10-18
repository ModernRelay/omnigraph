- A served write that fails after staging its detached effects and before its
  publication no longer stops the server. The engine reports the failure as
  definite (the graph did not move), the caller gets its 500, other writes,
  reads and other graphs keep serving, and a clean shutdown still releases the
  cluster lock. A storage request that fails on one of Lance's write-once
  table files or on a put-if-absent manifest version no longer leaves the
  server's storage scope uncertain, and the first request whose outcome is
  unknown on any other key is now logged with its key and error. See
  [shutdown and writer ownership][definite-storage-failures].

[definite-storage-failures]: ../docs/user/deployment.md#writer-topology
