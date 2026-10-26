- Creating a branch whose branch record got no answer from S3 or Azure is no
  longer reported as failed, with its copied state deleted, while the record may
  still land: the outcome is unknown and the copied state is kept, so a late
  record names a whole branch. A create whose record the store refused cleans
  up and retries once. See [storage failures][branch-create-unknown].

[branch-create-unknown]: ../docs/user/operations/storage-failures.md#unknown-publications
