- Organization administration accepts email selectors for role assignment,
  offboarding and invitation resend/revoke. `organization members set --role`
  changes an existing member's service role; writes retain the exact resolved
  request locally so the same idempotency key cannot select a replacement
  membership or invitation. See [organization access][organization-member-selectors].

[organization-member-selectors]: ../docs/user/cli/managed-team.md
