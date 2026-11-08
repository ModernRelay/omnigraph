- Organization commands follow provider roles: service permissions come from each member's organization roles, and graph and deployment access from applied cluster policy. Replace `organization members apply` with `members set --role ROLE`, repeating `--role` for every role the member keeps; add `--role` to new invitations; offboard by email or user ID, without `--expected-version`. See [team administration][organization-role-selection-guide].

[organization-role-selection-guide]: ../docs/user/cli/managed-team.md
