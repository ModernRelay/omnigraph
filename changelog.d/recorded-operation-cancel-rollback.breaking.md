- Rolling a cluster server back from 0.17.1 to 0.17.0 or earlier: once a recorded-operation cancel (answered 202, or 500 `outcome_unknown`) wrote `cancellation_request` to `__cluster/operations.json`, the older server refuses the whole record (409 `operation_record_invalid`). Stop the server and check the record first. See [rolling back][cancel-rollback-guide].

[cancel-rollback-guide]: ../docs/user/operations/recorded-operations.md#rolling-back-from-0171
