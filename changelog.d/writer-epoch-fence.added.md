- An embedded program can claim a format-17 branch (`claim_branch_writer`) so no stale process lands a write on it; other writers, server and CLI too, get 409 `writer_fenced`. See [writer epoch][writer-epoch-guide].

[writer-epoch-guide]: ../docs/user/operations/troubleshooting.md#writer-epoch
