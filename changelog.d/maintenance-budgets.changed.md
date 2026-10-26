- **Maintenance runs one table at a time under declared memory caps.**
  Optimize, cleanup, index builds and the full-text rebuild now run one at a
  time per graph in a process, and a run rewrites or indexes one table at a
  time by default (`OMNIGRAPH_MAINTENANCE_CONCURRENCY`, which defaulted to 8,
  is now capped at 2). Compaction scans queue at most 64 MiB in Lance's I/O
  buffer and full-text builds use 512 MiB over two workers; each has its own
  variable. Expect a direct `optimize` on a graph with many tables to take
  longer. See [maintenance budgets][maintenance-budgets].

[maintenance-budgets]: ../docs/user/operations/maintenance.md#budgets
