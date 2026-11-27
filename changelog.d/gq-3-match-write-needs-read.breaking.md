- A mutation with a `match` now needs `read` on its branch as well as `change`,
  on every writer (embedded, CLI, HTTP and MCP), since its selection reads stored
  data and its affected counts and failures would reveal it. An actor holding
  `change` alone is refused with `403 forbidden` before any effect; grant it
  `read`, or write an `insert` with no `match`, which still needs `change` alone.
  See [policy][gq-3-match-write-read-guide].

[gq-3-match-write-read-guide]: ../docs/user/operations/policy.md#actions
