# Sign in to a managed profile

`omnigraph login` signs a person in to a managed profile through the
identity provider's device flow: it prints a link and a code, waits while you
approve the sign-in in a browser, verifies the session with the API, and
saves it in the operating system's secure store. The commands and the profile
format are in [operator configuration](targets.md#login-and-registration).

```bash
omnigraph login --create-profile work
omnigraph login --create-profile research --organization org_example
omnigraph login --profile work
```

## Choose the organization

A profile belongs to one organization. With `--organization ID`, the sign-in is
exchanged for that organization whichever one the browser opened. Without it,
the CLI asks the API which organizations you are an active member of:

| Your memberships | Result |
|---|---|
| None | Refuses with `organization_required`. Join an organization before creating a profile. |
| One | Creates the profile in that organization, even if the browser opened a different one. |
| Several, at a terminal | Lists the organizations and asks for a number or an organization ID. |
| Several, with `--json` or without a terminal | Returns `organization_required` and the choices. Run again with `--organization ID`. |

For scripts, pass `--json`. If a choice is required, stdout contains one JSON
object with `type: "organization_required"` and an `organizations` array.
Each entry has an `organization_id` and an optional name. Use the selected ID
with `--organization` on the next invocation. For an existing profile, use
[`organization list`](managed-team.md#find-your-organizations).

## When a sign-in is refused

A sign-in that cannot become a session says which check failed and whose it
is. Each `detail` is a fixed sentence naming one check; none includes a token.

| Refusal | Meaning | What to do |
|---|---|---|
| `organization_required` | The API reported no memberships, the sign-in had no organization, or a choice could not be made | Join an organization if you have none. Otherwise pass its ID with `--organization ORG_ID`. |
| `provider_response_invalid` | The identity provider's new tokens do not fit the sign-in configuration the API publishes at `/v1/auth/config`: another issuer or client, an unreadable or out-of-range token, or a response that names another user or organization than its token | Nothing saved is at fault and signing in again does not help; report the `detail` to the service's administrator |
| `credential_invalid` | A session this machine saved, its index, or the API's verification of it fails a check | `logout` and `login` for that profile; the `detail` names the saved entry or the verified field |
| `session_identity_mismatch` | The sign-in is a session this or another profile already holds, or its identity changed during organization selection | Start a new sign-in; see the `detail` |
| `organization_mismatch` | The verified session belongs to another organization than the profile's | Sign in as a member of the profile's organization, or create a profile for the other |
