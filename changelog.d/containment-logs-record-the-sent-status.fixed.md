- A write the server cannot settle answers 500 `outcome_unknown`, and its
  containment warning and error lines now log that 500 instead of the status
  of the failure's own code, such as 400 for a query execution failure. See
  [storage failures][containment-logs-sent-status].

[containment-logs-sent-status]: ../docs/user/operations/storage-failures.md#logs
