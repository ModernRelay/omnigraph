- `query --format arrow` writes an Arrow IPC stream from a direct target to a
  redirected stdout instead of always refusing; a terminal or a served target
  refuses before any work. See [Arrow output][query-arrow-output].

[query-arrow-output]: ../docs/user/cli/reference.md#query-inputs-and-output
