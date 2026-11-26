- Rolling a cluster server back to 0.16 after it accepted a recorded-operation cancel: 0.16 refuses `__cluster/operations.json` once any entry has `cancellation_request` (409 `operation_record_invalid`). Let open operations finish, stop the server and move the record aside before starting 0.16. See [rolling back][cancel-rollback-guide].

[cancel-rollback-guide]: ../docs/user/operations/recorded-operations.md#rolling-back-to-016
