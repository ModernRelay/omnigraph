- Managed profiles support unattended OAuth client credentials with an explicit token endpoint and env/file secret reference. Credentials renew before dispatch while preserving the verified workload identity; submitted operations are never replayed for authentication. Administrators can list, enroll, update and revoke workload admission with `organization workloads`; see [workload setup][workload-client-credentials] and [administration][workload-administration].

[workload-client-credentials]: ../docs/user/cli/managed-team.md#configure-the-workload-profile
[workload-administration]: ../docs/user/cli/managed-team.md#enroll-unattended-workloads
