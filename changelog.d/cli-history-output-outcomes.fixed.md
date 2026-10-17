- `commit publish` and `commit drop` now report conservative write outcomes when a response or result output fails, matching their `mutate` forms. History commands propagate output failures, and baseline captures hold their output-file lock until the resume cursor is flushed. See [failed data-write commands][cli-history-write-outcomes] and [change baselines][cli-history-baselines].

[cli-history-write-outcomes]: ../docs/user/operations/troubleshooting.md#failed-data-write-commands
[cli-history-baselines]: ../docs/user/branching/changes.md#recover-from-a-retention-gap
