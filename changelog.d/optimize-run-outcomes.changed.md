- `optimize` reports an outcome per table; one table's failure no longer fails
  the run, and runs back to back no longer keep one write waiting through many
  runs. See [bounding a run][optimize-run-outcomes].

[optimize-run-outcomes]: ../docs/user/operations/maintenance.md#bounding-a-run
