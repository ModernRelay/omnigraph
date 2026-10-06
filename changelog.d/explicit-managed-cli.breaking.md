- Managed service commands now use `omnigraph managed …`; self-hosted `cluster …` commands always ignore managed folder context. Use `managed operation ID` for service lifecycle observation and `managed status [RUN_ID]` for run status. The former managed `cluster` spellings are rejected; data-command context and credential behavior are unchanged. See the [CLI reference][explicit-managed-cli].

[explicit-managed-cli]: ../docs/user/cli/reference.md#managed-cluster-commands
