- External Blob bases may no longer overlap OmniGraph storage. A base inside
  or above a graph root or the cluster storage root let any authorized writer
  copy another graph's tables, its own manifest, or the cluster ledger into a
  Blob value readable through `GET /blob`. `cluster validate`, `plan` and
  `apply` now refuse such a base, in either scope, with
  `external_blob_base_overlaps_storage_root`. A server whose applied state
  already carries an overlapping `server_safe` base quarantines that graph and
  serves its healthy siblings; startup fails with `--require-all-graphs`, and
  with `cluster_no_healthy_graphs` when every applied graph is quarantined.
  `Omnigraph::with_external_blob_policy` refuses a policy whose base overlaps
  the handle's own graph root. A base is compared only with a storage root of
  its own kind (`s3://` with `s3://`, `file://` with a local root); a
  same-kind root spelled with an empty path component or a percent sign
  cannot be compared and is refused with
  `external_blob_storage_root_uncomparable` instead. To upgrade, move the base to a sibling prefix
  outside the storage root, run `cluster apply`, and restart. The new checks
  do not rewrite values written earlier under an overlapping base.
  Incremental writes copied those bytes into managed storage;
  `load --mode overwrite` kept the URI as an external reference, which still
  reads through the process's storage credentials. If the base covered graph
  or cluster data, audit both the copied managed values and the retained
  external references that name a URI under it.
