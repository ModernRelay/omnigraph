- Managed profiles support unattended OAuth client credentials with an explicit token endpoint and env/file secret reference. Credentials renew before dispatch while preserving the verified workload identity; submitted operations are never replayed for authentication. Use existing cluster, graph and MCP commands after [configuring the workload profile][workload-client-credentials].

[workload-client-credentials]: ../docs/user/cli/managed-team.md#configure-an-unattended-workload-profile
