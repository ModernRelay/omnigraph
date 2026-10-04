- `omnigraph snapshot` and `GET /graphs/{graph_id}/snapshot` read the storage
  format version from the snapshot they return instead of resolving the branch
  again and opening its newest manifest version, so the reported version always
  describes the same graph version as the tables beside it. Reading the version
  itself now costs no storage request: a served snapshot of `main` with no new
  commit since the previous read does one version probe and opens no manifest
  version. How many storage requests one probe makes depends on the object
  store. A named branch also reads its branch reference, a handle that sees a
  new commit refreshes its state first, and the CLI opens the graph on every
  embedded command.
