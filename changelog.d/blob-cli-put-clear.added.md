- `omnigraph blob put ENTITY TYPE ID PROPERTY` stores the raw bytes of
  `--file PATH` or stdin in one Blob cell, and `omnigraph blob clear` sets a
  nullable cell to null, on a `--store` graph or a served one. Both take
  `--branch`, `--if-match TAG` (`*` or entity tags) and `--json`, and print the
  receipt the server returns: the selector, branch, state, size and ETag of a
  managed value, the actor and the exact commit, which is `null` for a clear of
  a cell that was already null. Input over 32 MiB and a malformed `--if-match`
  are refused before the graph is addressed, and an interactive stdin is
  refused rather than waited on. `--as` names the actor of a `--store` write.
  A served Blob precondition failure exits 4, as a graph-commit one does; an
  embedded one exits 1. A write is sent once and never retried.
