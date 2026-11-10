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
| One | The profile is created in it |
| Several, at a terminal | The CLI lists them and asks for a number or an organization ID; three answers that name none end the sign-in |
| Several, without a terminal | Refuses with `organization_required` and lists them; run the command again with `--organization ID` |

It never creates a profile in whichever of several organizations the browser
session happened to carry, and it asks the API to revoke a sign-in it does
not keep. The question
goes to stderr and the answer is read from stdin, so `--json` still prints one
document on stdout. [`organization list`](managed-team.md#find-your-organizations)
shows the same memberships for a profile that already exists.

## When a sign-in is refused

A sign-in that cannot become a session says which check failed and whose it
is. Each `detail` is a fixed sentence naming one check; none includes a token.

| Refusal | Meaning | What to do |
|---|---|---|
| `organization_required` | The sign-in selected no organization, you belong to several and none was named, or the API cannot list them | Run `login --create-profile NAME --organization ORG_ID` |
| `provider_response_invalid` | The identity provider's new tokens do not fit the sign-in configuration the API publishes at `/v1/auth/config`: another issuer or client, an unreadable or out-of-range token, or a response that names another user or organization than its token | Nothing saved is at fault and signing in again does not help; report the `detail` to the service's administrator |
| `credential_invalid` | A session this machine saved, its index, or the API's verification of it fails a check | `logout` and `login` for that profile; the `detail` names the saved entry or the verified field |
| `session_identity_mismatch` | The sign-in is a session this or another profile already holds, or its identity changed during organization selection | Start a new sign-in; see the `detail` |
| `organization_mismatch` | The verified session belongs to another organization than the profile's | Sign in as a member of the profile's organization, or create a profile for the other |
