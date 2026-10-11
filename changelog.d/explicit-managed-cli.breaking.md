- Managed service commands now require `omnigraph cluster <command> --managed`; without that flag, cluster commands ignore managed folder context. Use `cluster operation --managed ID` for service lifecycle observation and `cluster status --managed [RUN_ID]` for run status. The top-level `managed` command is removed; data commands still use their existing folder context without `--managed`. See the [CLI reference][explicit-managed-cli].

[explicit-managed-cli]: ../docs/user/cli/reference.md#managed-cluster-commands
