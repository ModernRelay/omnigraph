- On S3 and Azure, a conditional write of a cluster control object (the lock,
  cluster state, a deployment resource, a lease heartbeat or claim) whose answer
  was lost is re-issued and read back instead of failing on the first error. A
  request the store refused, such as a permission error, no longer leaves its
  key refused for the rest of the server's life. See
  [storage failures][control-object-reissue].

[control-object-reissue]: ../docs/user/operations/storage-failures.md#retries
