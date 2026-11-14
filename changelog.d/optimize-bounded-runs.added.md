- `optimize --max-gate-hold` stops starting tables once the limit passes, so writes to main wait about that long plus one table, not the whole run; `--tables` runs just those tables. See [bounding a run][optimize-bounded-runs].

[optimize-bounded-runs]: ../docs/user/operations/maintenance.md#bounding-a-run
