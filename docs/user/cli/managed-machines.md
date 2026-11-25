# Manage machines for unattended workloads

A *machine* is an organization-owned client application: the identity an
unattended workload, such as CI, a sync job or an agent, signs in as. An
administrator with `members_manage` creates and retires machines and their
secrets with `organization machines`, from their own profile. The managed
console's Credentials page does the same. Machine and workload profiles cannot
run these commands.

```sh
omnigraph organization machines list --profile work
omnigraph organization machines create ci-sync --profile work \
  --scope status --secret-file ~/.omnigraph/ci-sync.secret \
  --create-profile ci-sync --idempotency-key machine-ci-sync-20261125
```

`machines list` follows every page and shows each machine's name, client ID,
service scopes, creation time and Cedar principal (`principal:ID`), plus the
token endpoint and the service scopes you may grant. `--json` emits one
envelope, `data.machines`, with a null `after`.

`machines create NAME` creates the machine, generates its first secret and
writes it to `--secret-file`. Repeat `--scope` for each organization service
permission the machine needs (see [service roles](managed-team.md#assign-permissions-and-connect)): at most five, each one you
hold yourself, never `members_manage`; none is valid. The command prints the
client ID and the principal; neither output stream, `--json`, a log or the
operator configuration ever contains the secret. The secret file is created
with owner-only permission (0600) by an atomic rename in its directory, which
must already exist. An existing file refuses unless you pass
`--replace-secret-file`; a symbolic link or a directory always refuses.

`--create-profile NAME` also adds an `oauth_client_credentials` profile for the
machine, with your profile's API, organization and transport, the token
endpoint `machines list` reports, the client ID and
`client_secret: {file: PATH}`. An existing profile refuses unless you pass
`--replace-profile`; your own profile is never replaced.

**Creating a machine grants no graph access.** Add its `principal:ID` to the
cluster's Cedar policy through the
[source and deployment workflow](managed-lifecycle.md#prepare-and-apply-configuration),
granting only what the workload needs.

## Rotate a secret

```sh
omnigraph organization machines secret rotate --profile work \
  --machine-profile ci-sync --idempotency-key rotate-ci-sync-20261201
```

`--machine-profile` names the machine's `oauth_client_credentials` profile,
whose `client_secret` must be a `{file: PATH}` source. Rotation reads the
machine's existing secrets, generates a new one, replaces the file atomically
(again owner-only), and exchanges the new secret for a token at the profile's
token endpoint. Only after that exchange succeeds does it revoke the earlier
secret.

The service never reveals a secret again, so the CLI cannot tell which secret
the old file held. When the machine had exactly one secret before rotation,
that secret is revoked. When it had none, nothing is. When it had more, the
command refuses before generating anything and lists each secret's ID, hint
and creation time: repeat it with `--revoke SECRET_ID` for every secret to
retire. `--revoke` also selects explicitly when there is one.

If the token exchange fails, nothing is revoked: the new secret stays in the
file and both secrets remain active. A new secret can take a moment to reach
the identity provider; repeat the same command with the same key to prove it
again and finish the revocation. Tokens issued with a revoked secret stay valid
until they expire.

## Retire a machine

```sh
omnigraph organization machines revoke ci-sync --profile work \
  --idempotency-key retire-ci-sync-20261201 --yes
```

Select the machine by exact name or client ID; a name several machines share
refuses, so use the client ID. Retirement deletes the application at the
identity provider. Without `--yes`, an interactive terminal asks for
confirmation, and `--json` or noninteractive input refuses. Tokens the machine
already holds stay valid until they expire, and Cedar policy naming its
principal is unchanged: remove it through the configuration workflow.

## Interrupted machine requests

Machine writes follow [Recover an interrupted request](managed-team.md#recover-an-interrupted-request):
each requires `--idempotency-key` (at most 200 characters, since the CLI derives
one key per write from it), retains its progress in the same private custody,
and repeats only the identical request. A retry continues where the request
stopped: it never creates a second machine or generates a second secret.

A generated secret is disclosed in one response only. If that response was
lost, or the file could not be written, the identical retry learns the secret's
ID but not its value, and the command says so: the secret cannot be recovered.
Revoke that secret ID and generate another. After `create --create-profile`,
`secret rotate --machine-profile NAME` with a new key does both, since the
profile is written before the secret. After a lost rotation, the message
prints the exact `--revoke` arguments for a new rotation.

## Use a machine

A machine signs in through an `oauth_client_credentials` profile. See
[configure an unattended workload profile](managed-team.md#configure-an-unattended-workload-profile)
for the profile's fields, token acquisition and renewal.
