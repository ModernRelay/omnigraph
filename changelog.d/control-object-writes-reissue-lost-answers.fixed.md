- On S3 and Azure, a conditional write of a cluster control object (the lock,
  cluster state, a deployment resource, a lease heartbeat or claim) whose answer
  was lost is re-issued and read back instead of failing on the first error. A
  request the store refused, such as a permission error, no longer leaves its
  key refused for the rest of the server's life. See
  [storage failures][control-object-reissue].
- A graph commit whose manifest request was re-issued after a lost answer, and
  whose re-issue then met a conflict, is no longer reported as failed while the
  first request may still land on S3: its outcome stays unknown until a read
  shows it.

[control-object-reissue]: ../docs/user/operations/storage-failures.md#retries
