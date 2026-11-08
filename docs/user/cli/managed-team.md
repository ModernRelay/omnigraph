# Onboard and remove organization members

Invite teammates and manage organization membership through the managed CLI.
The identity provider owns membership and organization-wide service permissions;
the applied cluster configuration owns graph, branch, schema and deployment
permissions.
No database or cloud credentials are needed. The managed service must support
organization administration, and an administrator must configure its provider
membership and service roles.

Sign in to the selected organization:

```sh
omnigraph login --profile work
```

Every command below uses `--profile work`, a managed profile with an explicit
API, organization ID and authentication provider. See
[operator registration](targets.md#login-and-registration). `org` aliases
`organization`. The caller needs the actual current `members_manage`
permission. Role names and permission claims in an older bearer do not confer
service authority. Role definitions and their service permissions remain owned
by the identity provider.

These commands never use folder context or change the selected target. A
cluster target cannot administer its organization implicitly. `--json` emits
the validated API envelope.

## Invite and find people

```sh
omnigraph organization --profile work invitations send avery@example.com \
  --role member --idempotency-key invite-avery-20261015
omnigraph organization --profile work invitations list --json
omnigraph organization members list --profile work --json
```

Choose an existing service-role slug for every new invitation; there is no
implicit default. The invitation email lets the recipient join the organization. After
acceptance, active membership establishes identity eligibility for its
clusters. There is no additional local enrollment step. Graph access still
requires permission in the applied cluster policy.

The complete organization directory shows names/emails, active, pending and
inactive membership states, assigned service roles, service permissions and
available `principal:ID` values for Cedar. A principal can be absent until the
person's first sign-in. `--json` also includes exact user and membership IDs
and the provider observation time and expiry. More than
1,000 memberships or an incomplete provider read refuses the whole directory.

Invitation lists are separate pages of up to 100. Pass the returned `data.after`
as `--after CURSOR` to fetch the next page. A null cursor means the last page.
To resend or cancel an invitation, use the recipient's email or its exact ID:

```sh
omnigraph org invitations resend avery@example.com --profile work \
  --idempotency-key resend-avery-20261015
omnigraph org invitations revoke avery@example.com --profile work \
  --idempotency-key revoke-avery-20261015
```

An email must match exactly one pending invitation in this organization. The
CLI scans at most ten pages and 1,000 invitations within 35 seconds, refusing
incomplete or ambiguous results. Use an exact invitation ID to address a
particular invitation. Output never includes acceptance URLs or invitation
tokens.

## Assign permissions and connect

Assign an existing active member one or more configured service roles:

```sh
omnigraph org members set avery@example.com --role member --profile work \
  --idempotency-key role-avery-20261027
omnigraph org members list --profile work
```

Replace `member` with a role slug configured for your organization. `members
list` shows current assignments and their permissions, not every available
role. Repeat `--role` to select multiple roles. The command replaces the complete
assigned set, so include every role the member should retain. Duplicate flags
are refused; changing their order does not change the request. Use an exact
user ID instead of email when needed. The command does not create a person,
membership or role.
Directory-managed memberships must be changed through their directory.

Service roles contain these organization-wide permissions:

| Permission | Scope |
|---|---|
| `members_manage` | Organization directory, invitations, role changes and offboarding |
| `create` | Create clusters in the organization |
| `status` | Discover and inspect the organization's clusters |
| `delete` | Delete or undo deletion of any cluster in the organization |
| `source_read` | Read configuration source for the organization's clusters |
| `source_write` | Update configuration source for the organization's clusters |

These permissions apply across the organization. They do not authorize native
configuration deployment or graph operations. `organization members apply`
and local grant patches are no longer supported.

The teammate registers their profile and signs in. An administrator can then
read their stable `principal_id` using `members list`. A configuration manager
adds `principal:PRINCIPAL_ID` to the appropriate Cedar policy through the
[source and deployment workflow](managed-lifecycle.md#prepare-and-apply-configuration).
Grant only the intended graph and configuration permissions, retaining another
manager.

With `status` permission, the teammate discovers a cluster and registers its
service-attested identity:

```sh
omnigraph login --create-profile work
omnigraph cluster list --profile work
omnigraph target add production --profile work --cluster CLUSTER_ID
omnigraph target inspect production --resolved
```

Verify a permitted graph read, a reversible write if granted, and a forbidden
operation. Cluster discovery and identity-credential issuance alone do not
prove graph permission. Service checks read current provider membership and
permissions; provider-side role propagation can take time.

## Remove access

First remove the person's stable principal from applicable graph and management
Cedar policies, deploy through the normal configuration workflow, and verify
denial using a previously issued graph credential. Retain another authorized
configuration manager.

Then offboard the person by email or exact user ID:

```sh
omnigraph organization members offboard avery@example.com --profile work \
  --idempotency-key offboard-avery-20261015 --json
```

The CLI resolves and retains the exact current membership before sending.
Email must match exactly one member in this organization. For a scripted
assertion, add `--expected-membership om_example`; it must match the observed
membership. Use an exact user ID with `--expected-membership absent` only when
membership is already absent and you need to finish session cleanup. A
different or recreated membership refuses; inspect the directory before
starting a new request. The service checks that another active member retains
`members_manage` before removing the last observed manager. This check does not
lock concurrent changes made through the identity provider.

Offboarding removes the selected organization's membership and revokes sessions
explicitly belonging to that organization and person. It preserves the shared
provider user and other organizations' sessions. The receipt reports provider
membership and session cleanup separately. A partial result exits 3; complete
cleanup exits 0. It never claims to change graph policy. Existing graph
credentials can last up to 86,400 seconds, so membership removal alone is not
immediate graph revocation.

## Recover an interrupted request

Every write requires an explicit `--idempotency-key`. Before sending, the CLI
durably retains the exact resolved request in private `OMNIGRAPH_HOME` custody.
Keep this home, the same profile/identity, arguments and key until the outcome
is known. Repeating the identical command uses the retained request; it does
not resolve the email again. Changed arguments or a different identity refuse.
Custody is bounded to 128 records and 1 MiB per stable profile identity, with no
automatic eviction. At the limit, new writes refuse while exact retries remain
available.

The CLI submits once with a 35-second request deadline. A timeout does not
cancel a provider action. Repeat the identical command with the same key to
retrieve its retained result. The service never resends an uncertain provider
write. A new key starts a new action, so do not change it merely to retry.

A role-change receipt distinguishes a verified update from an uncertain
provider outcome. An uncertain result exits 3 and shows the last verified
roles and permissions; it does not claim that the requested role took effect.

If an older CLI saved an invitation without a role, repeat its original
arguments and key to retrieve the saved result or uncertainty. The CLI preserves
that request unchanged; a new invitation always requires `--role`.

A partial offboard result stays partial on replay. Inspect the current provider
state and retained receipt before preparing a new request for remaining cleanup.
An uncertain offboard blocks other offboards in that organization until the
service operator reconciles the retained attempt; a new key cannot bypass it.
Human progress and the key go to stderr; `--json` keeps one response on stdout.
Permission refusals exit 2 and malformed/transport failures exit 1, with no
storage fallback.

## Configure an unattended workload profile

The managed service must support workload sessions. Ask its administrator to
configure an organization-owned client application and its service permissions.
Obtain and rotate its secret through your identity provider's administration
tools. Add a profile to
`~/.omnigraph/config.yaml` (or `$OMNIGRAPH_HOME/config.yaml`):

```yaml
version: 1
profiles:
  ops:
    api: https://control.example.com
    organization_id: org_example
    auth:
      provider: oauth_client_credentials
      token_endpoint: https://auth.example.com/oauth2/token
      client_id: client_example
      client_secret: {env: OG_OPS_CLIENT_SECRET}
      # Optional provider scope, not graph permissions:
      scope: "read write"
```

The secret may instead use `{file: /var/run/secrets/ops-client-secret}`.
Environment values are exact; files allow one final LF or CRLF and atomic
symlink rotation. Sources are bounded to 64 KiB. Literal YAML secrets and
secure-store client secrets are unsupported.

```sh
omnigraph cluster list --profile ops
omnigraph target add ops-cluster --profile ops --cluster CLUSTER_ID
omnigraph --target ops-cluster --graph knowledge query -e 'query q() { return { 1 as ok } }'
omnigraph mcp --target ops-cluster
```

Acquisition makes one form-encoded `client_credentials` request using
`client_secret_post` at the explicit HTTPS endpoint. It uses system trust,
direct networking and a 25-second total bound, without endpoint discovery,
redirects, browser login or refresh tokens. The verified service session must
identify the configured client as a verified workload in the selected
organization. Provider service permissions govern organization operations;
applied Cedar governs graph and deployment operations. Provider scopes do not
replace either authority.

The token response must include a positive `expires_in`; providers that omit it
are unsupported. The profile's proxy and custom-CA settings apply to managed
service requests, not this token exchange.

Tokens stay in invocation memory. Long-running commands renew expired control
credentials before their next request and keep the same verified organization,
principal, kind, subject, client ID and assurance. A typed native-credential
issuance preflight refusal may trigger one renewal and one new issuance request. A
submitted operation, HTTP 401 or uncertain delivery is never replayed for
authentication. Secret rotation takes effect on the next acquisition; a
different client or identity needs a separate profile.

OAuth `cluster token` reports issuance metadata without persisting the credential;
in a fresh invocation, `--clear` reports `local_credential_removed: false` because
there is no persisted OAuth credential to remove. It does not clear credentials
saved by a previous provider. Ordinary resource commands acquire their own
credential.
