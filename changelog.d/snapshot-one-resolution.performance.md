- `omnigraph snapshot` and `GET /graphs/{graph_id}/snapshot` read the storage
  format version from the snapshot they return instead of resolving the branch
  again and opening its newest manifest version. A warm call now makes one
  storage request (the version probe) instead of four, and the reported
  version always describes the same graph version as the tables beside it.
