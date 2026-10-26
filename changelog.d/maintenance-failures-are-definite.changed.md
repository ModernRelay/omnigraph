- **A maintenance failure before its publication says the graph did not
  move.** Optimize, cleanup, index builds, the full-text rebuild and the repair
  report mark every such failure, so the direct CLI exits `1` (no effects) for
  a storage or internal failure where it exited `2` (outcome unknown), and a
  host serving the graph keeps serving through one. A publication in doubt is
  still reported as one. See the [write path][maintenance-failure-evidence].

[maintenance-failure-evidence]: ../docs/dev/writes.md#policy-and-attribution
