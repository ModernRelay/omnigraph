- A served write that fails after staging its detached effects and before its
  publication no longer stops the server. The engine reports the failure as
  definite (the graph did not move), the caller gets its 500, other writes,
  reads and other graphs keep serving, and a clean shutdown still releases the
  cluster lock. A storage request that fails on one of Lance's write-once
  table files or on a put-if-absent manifest version no longer leaves the
  server's storage scope uncertain, including a multipart upload of a large
  data file; after a manifest put the store did not acknowledge, the scope
  keeps that dataset's transaction files from being deleted until cleanup
  runs from another process, so a manifest landing late stays complete. The
  first request whose outcome is unknown on any other key is now logged with
  its key and error. See
  [shutdown and writer ownership][definite-storage-failures].

[definite-storage-failures]: ../docs/user/deployment.md#writer-topology
