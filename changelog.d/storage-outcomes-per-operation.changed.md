- A served write whose storage request gets no definite answer no longer stops
  the server. A publication whose manifest request no re-issue settles is
  pending on its branch: that write answers 500 (`outcome_unknown` on MCP),
  later writes to the branch answer 503 `service_unavailable` with nothing
  written, and reads, other branches and other graphs keep serving. The server
  re-issues the pending commit until storage answers, and then the write
  completes or is refused. A write to a reused key whose outcome is unknown
  blocks only that key. Reads and writes of fresh, uniquely named files now
  retry within Lance's own bounds, so a single throttled request no longer
  fails a write. A panic, a failed in-memory adoption or an opaque execution
  failure still contains the process. See
  [storage failures][storage-outcomes-per-operation].

[storage-outcomes-per-operation]: ../docs/user/operations/storage-failures.md
