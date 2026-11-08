- Schema, initialization and lint commands now report output delivery failures without panicking. A lost schema-apply result retains reconciliation guidance; output failure does not undo an applied schema or initialized graph. See [failed data-write commands][cli-schema-failure].

[cli-schema-failure]: ../docs/user/operations/troubleshooting.md#failed-data-write-commands
