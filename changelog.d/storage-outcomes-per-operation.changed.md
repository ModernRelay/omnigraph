- A served write whose storage request gets no definite answer no longer stops
  the server. A publication whose manifest request no re-issue settles answers
  500 (`outcome_unknown` on MCP) and the server keeps serving: the manifest
  version is created only if absent, so the request lands before the branch's
  next publication or never. A write to a reused key whose outcome is unknown
  blocks only that key. Reads and writes of fresh, uniquely named files now
  retry within Lance's own bounds, so a single throttled request no longer
  fails a write. A panic, a failed in-memory adoption or an opaque execution
  failure still contains the process. See
  [storage failures][storage-outcomes-per-operation].

[storage-outcomes-per-operation]: ../docs/user/operations/storage-failures.md
