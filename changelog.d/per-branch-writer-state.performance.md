- Writes, merges and reads that alternate across named branches on one graph handle reuse each branch's catalog state, for up to 64 branches, instead of reopening it. See the [write path][per-branch-writer-state].

[per-branch-writer-state]: ../docs/dev/writes.md#captured-authority
