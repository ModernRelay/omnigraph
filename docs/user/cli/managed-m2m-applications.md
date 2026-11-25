# Manage M2M applications for unattended workloads

An *M2M application* is an organization-owned client application: the
identity an unattended workload, such as CI, a sync job or an agent, signs in
as with a client ID and a client secret. An administrator with
`members_manage` creates and deletes M2M applications and their client secrets
with `organization m2m-applications`, from their own profile. The managed
console's Credentials page does the same. M2M application and workload
profiles cannot run these commands.

```sh
omnigraph organization m2m-applications list --profile work
omnigraph organization m2m-applications create ci-sync --profile work \
  --permission status --client-secret-file ~/.omnigraph/ci-sync.secret \
  --create-profile ci-sync --idempotency-key create-ci-sync-20261125
```

`m2m-applications list` follows every page and shows each application's name,
client ID, permissions, creation time and actor ID (`principal:ID`, what a
cluster's policy names), plus the token endpoint and the permissions you may
grant. `--json` emits one envelope, `data.applications`, with a null `after`.

`m2m-applications create NAME` creates the application, generates its first
client secret and writes it to `--client-secret-file`. Repeat `--permission`
for each organization permission the application needs (see
[service roles](managed-team.md#assign-permissions-and-connect)): at most
five, each one you hold yourself, never `members_manage`; none is valid. The
command prints the client ID and the actor ID; neither output stream,
`--json`, a log nor the operator configuration ever contains the client
secret. The file is created with owner-only permission (0600) by an atomic
rename in its directory, which must already exist. The path must end in a file
name: a trailing `/` or `.` refuses. An existing file refuses unless you pass
`--replace-client-secret-file`; a symbolic link or a directory always refuses.
A replaced file keeps its owner and group; when the account running the
command cannot give the new file that owner, the command refuses before
anything is created, so run it as that user or as root.

The client secret file must not be any other file the command writes: the
`--stats` file, the operator configuration, or the CLI's state under the
operator home (`organization/`, `lifecycle/`, `auth-state/`). Such a path
refuses before anything is written, statistics included.

Writing a client secret needs a platform that can create a file only its
owner can read: Linux or macOS. On other platforms, `create` and
`secret rotate` refuse before any request; `list` and `delete` work
everywhere. Use the managed console's Credentials page there.

`--create-profile NAME` also adds an `oauth_client_credentials` profile for the
application, with your profile's API, organization and transport, the token
endpoint `m2m-applications list` reports, the client ID and
`client_secret: {file: PATH}`. An existing profile refuses unless you pass
`--replace-profile`; your own profile is never replaced.

**Creating an M2M application grants no graph access.** Add its actor ID to
the cluster's Cedar policy through the
[source and deployment workflow](managed-lifecycle.md#prepare-and-apply-configuration),
granting only what the workload needs.

## Rotate a client secret

```sh
omnigraph organization m2m-applications secret rotate --profile work \
  --application-profile ci-sync --idempotency-key rotate-ci-sync-20261201
```

`--application-profile` names the application's `oauth_client_credentials`
profile, whose `client_secret` must be a `{file: PATH}` source. Rotation reads
the application's existing client secrets, generates a new one, replaces the
file atomically (again owner-only), and exchanges the new client secret for a
token at the profile's token endpoint. Only after that exchange succeeds does
it revoke the earlier client secret.

The service never reveals a client secret again, so the CLI cannot tell which
one the old file held. When the application had exactly one client secret
before rotation, that one is revoked. When it had none, nothing is. When it had
more, the command refuses before generating anything and lists each client
secret's ID, hint and creation time: repeat it with `--revoke SECRET_ID` for
every client secret to revoke. `--revoke` also selects explicitly when there is
one.

If the token exchange fails, nothing is revoked: the new client secret stays in
the file and both remain active. A new client secret can take a moment to reach
the identity provider; repeat the same command with the same key to prove it
again and finish the revocation. A repeat proves and revokes only while the
file still holds the client secret that rotation wrote: if you restored an
earlier client secret to roll back, the repeat refuses, revokes nothing, and
names the new client secret ID to revoke instead. Tokens issued with a revoked
client secret stay valid until they expire.

## Delete an M2M application

```sh
omnigraph organization m2m-applications delete ci-sync --profile work \
  --idempotency-key delete-ci-sync-20261201 --yes
```

Select the application by exact name or client ID; a name several applications
share refuses, so use the client ID. Deletion removes the application at the
identity provider. Without `--yes`, an interactive terminal asks for
confirmation, and `--json` or noninteractive input refuses. Tokens the
application already holds stay valid until they expire, and a cluster's policy
naming its actor ID is unchanged: remove it through the configuration
workflow.

## Interrupted requests

M2M application writes follow
[Recover an interrupted request](managed-team.md#recover-an-interrupted-request):
each requires `--idempotency-key` (at most 200 characters, since the CLI
derives one key per write from it), retains its progress in the same private
custody, and repeats only the identical request. A retry continues where the
request stopped: it never creates a second application or generates a second
client secret. A completed `create` returns its result without writing its
profile or file again, so it cannot undo a later change to either.

A generated client secret is disclosed in one response only. If that response
was lost, or the file could not be written, the identical retry learns the
client secret's ID but not its value, and the command says so: the value cannot
be recovered. Revoke that client secret ID and generate another. After
`create --create-profile`, `secret rotate --application-profile NAME` with a
new key does both, since the profile is written before the client secret. After
a lost rotation, the message prints the exact `--revoke` arguments for a new
rotation.

## Use an M2M application

An M2M application signs in through an `oauth_client_credentials` profile. See
[configure an unattended workload profile](managed-team.md#configure-an-unattended-workload-profile)
for the profile's fields, token acquisition and renewal.
