---
rfc: "2026-11-25-organization-m2m-applications"
title: "Organization M2M applications in the CLI"
track: maintainer
status: draft
implementation: in-progress
authors:
  - ragnorc
created: 2026-11-25
updated: 2026-11-26
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Organization M2M applications in the CLI

## Summary

`omnigraph organization m2m-applications` lets an organization administrator
manage *M2M applications*: organization-owned client applications that
unattended workloads sign in as, with a client ID and a client secret, through
an `oauth_client_credentials` profile. Four commands, run from the
administrator's own managed profile, call the managed service's existing Intent
API (the `/v1/organization/m2m-applications` collection) and add no Omnigraph
server endpoint:

- `m2m-applications list` shows every application with its client ID,
  permissions, creation time and actor ID (`principal:ID`, what a cluster's
  policy names).
- `m2m-applications create NAME [--permission P]... --client-secret-file PATH
  [--create-profile PROFILE]` creates the application, generates its first
  client secret, writes it only to `PATH`, and can add a profile for it.
- `m2m-applications secret rotate --application-profile PROFILE
  [--revoke SECRET_ID]...` writes a new client secret to the profile's file,
  proves a client-credentials token exchange with it, and only then revokes the
  earlier client secret.
- `m2m-applications delete NAME_OR_CLIENT_ID` deletes the application after
  confirmation.

An M2M application is an identity, not a grant: creating one changes no Cedar
policy, and deleting one leaves policy and already-issued tokens as they are.
A disclosed client secret reaches exactly one place, the named file; it never
reaches an output stream, `--json`, an error, a log, request custody or the
operator configuration.

The CLI uses one term per concept, the same nouns as the API: *M2M
application*, *client ID*, *client secret*, *permissions* (the organization
permissions an application may exercise) and *actor ID*. Customer-facing text
says "identity provider" and names no provider product.

Contract: keeps E6. The change adds no graph operation; an M2M application
reaches a graph only through the installed policy, as every caller does.

## Motivation

The workload-profile guide told operators to create the client application and
obtain and rotate its client secret through the identity provider's
administration tools. That leaves the steps that decide whether an unattended
workload is safe outside the CLI: which organization owns the application,
which permissions it carries, which actor ID to name in Cedar, and where the
one-time client secret goes. The managed service exposes M2M application
administration in its Intent API with the same authority model as member
administration (a human session with `members_manage`, permissions bounded by
the caller's own), so the CLI can close that gap without new server surface.

Two properties need a design rather than a thin wrapper. The client secret is
disclosed once and cannot be read back, so delivery and uncertain outcomes
decide whether a client secret is lost or leaked. Rotation must not cut a
workload off: the old client secret may be revoked only once the new one is in
place and proven, and the CLI cannot tell which client secret an old file held.

## User and operational behavior

All four commands take the administrator's profile from `--profile` or
`defaults.profile`, refuse `--target`, `--graph` and `--as` as the rest of
`organization` does, and print the validated result as text or, with `--json`,
one JSON document on stdout. Every write takes a required `--idempotency-key`;
progress and the key go to stderr. Refusals specific to M2M applications use the API's
`m2m_application_` problem-type family.

**List.** `m2m-applications list` follows `after` cursors for at most ten pages
and returns one envelope whose `data.applications` holds every application and
whose `after` is null; `grantable_permissions` and `token_endpoint` must agree
across pages. A foreign organization, an actor ID that is not `principal:<id>`,
a duplicate client ID or a repeated cursor refuses the whole listing.

**Create.** Permissions are canonical (sorted, distinct), at most five, and
never `members_manage`; the service further requires that the caller holds
each. Before any remote write the command refuses an existing client secret
file without `--replace-client-secret-file`, any symbolic link or directory at
that path, a missing directory, an existing profile name without
`--replace-profile`, and the administrator's own profile name. It then creates
the application, writes the profile if asked, generates the client secret and
writes the file. The profile holds the administrator profile's `api`,
`organization_id` and `transport`, the `token_endpoint` the application listing
reports, the client ID and `client_secret: {file: PATH}` with `PATH` absolute.
Output names the client ID, the actor ID, the client secret ID, the file and
the profile, and says that the application has no graph access until a
cluster's policy names its actor ID.

**Rotate.** `--application-profile` names an `oauth_client_credentials`
profile, other than the administrator's, with the same API and organization
and a `{file: PATH}` client secret. The global `--profile` keeps its meaning,
the administrator's profile, so the application profile has its own flag. The
command lists the application's client secrets, chooses what to revoke
(below), generates a client secret, replaces the file, exchanges the client
secret for a token at the profile's token endpoint, then revokes the chosen
client secrets. A failed exchange revokes nothing and reports
`m2m_application_secret_unproven` with the new client secret's ID.

**Delete.** The selector is an exact client ID or an exact name; a name that
several applications share refuses with `m2m_application_ambiguous`. The
command follows the CLI's destructive-write convention: global `--yes`
consents, an interactive terminal is asked, and `--json` or noninteractive
input refuses with `confirmation_required`. Output reminds the operator that
issued tokens last until expiry and that a cluster's policy still names the
actor ID.

## Design

### Why `organization`, not a new family

M2M applications belong to the organization exactly as members and invitations
do: the same Intent API resource tree (`/v1/organization/...`), the same
authority (`members_manage`, scoped to the profile's organization), the same
refusal of cluster targets, and the same request custody. A new top-level
family would duplicate the profile selection, scope refusal, organization
verification and custody, and split one concept, "who belongs to this
organization and with what permissions", across two command trees.
`organization` already says it manages identities and that graph authority
stays in Cedar; an M2M application is one more kind of identity. The
subcommand takes the API collection's noun, `m2m-applications`, and its
removal verb, `delete`, so one name holds from the API to the CLI.

### Client secret custody

The transport strips every `client_secret` field from every response. Client
secret generation is the one call that asks it to keep that field verbatim,
through the same per-call exemption that keeps exact source files in a source
export (`ResponseLimits::preserved`). The first thing the command does with
that body is remove the field into a value type that implements no `Debug`,
`Display` or `Serialize`, so the type system leaves one use: writing it to the
file. Every refusal after that point is a fixed message. Human and `--json`
output are built from validated, secret-free structures, never from the
response body.

The file is written as the CLI's other atomic writes are: a temporary file in
the destination's directory, created owner-only (0600) before the first remote
write, so an unwritable directory refuses while nothing exists remotely; then
write, `fsync`, rename (`create` uses a no-clobber rename unless replacement
was allowed), and `fsync` of the directory. The CLI writes regular files only;
it refuses to write through or over a symbolic link, so a link-rotated mount
stays under its own manager.

A destination is accepted in one place, `ClientSecretFile::new`, before
anything is staged or sent. The path as written must end in a file name; a
trailing separator, `.` or `..` refuses, since the rename would fail only after
the client secret existed. A replacement keeps the replaced file's owner and
group: the staged file is given them before the first remote write, and when
the running account cannot do that, the command refuses, naming the owner, so a
rotation run by another account never leaves the workload unable to read its
own file. Owner-only files need Unix permission bits; on other platforms
`create` and `secret rotate` refuse before any request, and `list` and `delete`
work as everywhere. Windows is a release target, but no CI job builds or tests
it, so an access-control implementation would first compile at release time; an
explicit refusal is the honest boundary until such a job exists.

The client secret file must not coincide with any other file the invocation or
the CLI writes: the `--stats` file, the operator configuration, the credential
binding state, or anything under the operator home's state directories
(`organization/`, `lifecycle/`, `auth-state/`). One check compares their
resolved locations before the statistics recorder starts, so a refusal is never
followed by another output writing over the secret. The codebase has no zeroization dependency, and
this change does not add one: the client secret lives in one owned string
dropped right after the write.

### Request identity and progress

M2M application writes follow the organization family's uncertain-write rule:
a write requires an explicit key, the exact request is retained before the
first remote effect, an identical retry repeats the identical writes, and a
different request under a retained key refuses. Create and rotate are several
writes, so the CLI derives one key per write from the request key
(`KEY:application`, `KEY:secret`, `KEY:revoke-N`, in the order the retained
request fixes); delete is one write under `KEY`. The request key is therefore
limited to 200 characters.

The record also holds the request's progress, written after each step and
before the next: the token endpoint and the created application for `create`;
the client ID, token endpoint, file and the chosen revocation targets for
`rotate`; the resolved client ID, actor ID and name for `delete`; and, for the
two commands that write a client secret, the ID of the client secret whose
value the request wrote to its file. A retry skips what is recorded and
resumes at the first unrecorded step, so it never creates a second application
or generates a second client secret, and never re-resolves a name after the
application it named has gone. The profile is a recorded step too: consent to
replace an existing profile covers the first attempt only, a retry after an
attempt accepts its own profile or none, and an installed profile is never
written again, so a completed create's replay changes nothing local and cannot
undo a later request's profile.

A delivery is recorded as a binding to the value written, not only its ID: a
random salt and the SHA-256 of salt and value. The value is a high-entropy
secret the service generated, so the digest identifies it without disclosing
it, and the salt keeps two records of one value from matching. Records live in `m2m-applications.json` beside
the organization custody file, under the same lock and the same bounds (128
records, 1 MiB, no eviction). A separate file keeps an older CLI, which
validates `requests.json` strictly, from refusing all organization writes after
a newer one ran an M2M application command.

### Uncertain and lost client secrets

The service discloses a client secret in the first successful response only;
its stored receipt, and so every exact replay, holds `client_secret: null`. A
client secret is therefore lost when that response does not arrive, when it
arrives and fails validation, or when the file cannot be written. In each case
the command reports what it knows, and a retry with the same key learns the
client secret's ID from the replay. When the replay names a client secret this
request did not record as delivered, the command refuses with
`m2m_application_secret_undelivered`, naming the client ID and client secret
ID, says the value cannot be recovered, and gives the recovery: revoke that ID
and generate another. It never generates a replacement on its own. A refusal
the service marks `request_outcome: "not_dispatched"` reached no provider
write; the CLI reports it as any other refusal.

`create` writes the profile before it generates the client secret. A create
whose client secret was lost thus leaves a profile, and
`secret rotate --application-profile PROFILE` with a new key is the recovery:
it finds that one client secret, writes a new one, proves it and revokes the
lost one. Without `--create-profile`, the recovery is the managed console's
Credentials page. A lost rotation names the exact `--revoke` arguments for a
new rotation that revokes both the lost client secret and the earlier ones it
was meant to replace.

### Rotation's revocation choice

The CLI cannot know which client secret the old file held: the service shows
only an ID and a short hint, and the file holds only the value. The command
lists the client secrets before it generates anything and decides once,
retaining the decision:

- with `--revoke`, exactly the named IDs, each of which must exist;
- otherwise, the one client secret if there was exactly one;
- nothing if there were none;
- with more than one and no `--revoke`, a refusal that lists every
  candidate's ID, hint and creation time, before any write.

Exactly one earlier client secret is the common case, an application that
this command or the console rotated before, and revoking it is what rotation
means. With several, guessing would cut off whichever workload holds the one
not guessed. The list is read before generation so the new client secret is
never a candidate. Revocation follows the exchange: a client secret that cannot
sign in never replaces a working one. If the exchange fails, both client
secrets stay active and the file keeps the new value; the same key repeats the
exchange and then revokes. Before proving, the command reads the file once and
requires it to hold the delivered value; it exchanges exactly the bytes it
checked. A file that holds anything else, such as an earlier client secret an
operator restored to roll back, refuses with
`m2m_application_secret_file_changed`: nothing is proven or revoked, and the
message names the new client secret to revoke instead.

The exchange reuses the `oauth_client_credentials` provider's acquisition,
split into its two halves: the provider's rule for reading a client secret
source and its exchange of a given value. It is one form-encoded request with
system trust and a 25-second bound, so it proves the file as written. The token
is dropped unused.

## Invariants

Invariant 10 (trust at the boundary, enforced at the engine) is untouched: an
M2M application authenticates to the managed service like any workload, and
every graph action it attempts is judged by applied Cedar. Invariant 11
(bounded, observable failures): listing is bounded to ten pages, every request
has the family's 35-second deadline, no write is retried automatically, and
every partial outcome (application created but client secret lost, client
secret written but unproven, revocation incomplete) is a typed result that
names what exists. No deny-list item applies; the custody file is local
operator custody, not a second source of truth for the service's state, and
the service remains authoritative.

## Compatibility and reversibility

The change adds commands, one custody file and one optional transport
exemption. Operator configuration gains no field: `--create-profile` writes the
existing `oauth_client_credentials` shape. An older CLI ignores
`m2m-applications.json`. Reverting removes the commands; profiles and client
secret files they wrote keep working, since they are ordinary configuration.
`cluster token --ttl` changes only its help and documentation: the value is a
requested lifetime, the service may issue less, and `expires_at` reports what
it issued.

## Alternatives

- **Print the client secret, or offer a stdout option.** Rejected: terminals,
  scrollback, CI logs and agent transcripts retain stdout, and the service
  refuses to disclose the client secret again. A file is the one destination
  whose permission and lifetime the operator controls.
- **Write the client secret into the operator configuration or the OS store.**
  The configuration is shared and copied; the profile grammar deliberately
  refuses literal secrets. The OS store is unavailable on the hosts unattended
  workloads usually run on.
- **Revoke every earlier client secret on rotation.** Simpler, but it cuts off
  any other host holding another one, which is the reason an application has
  several.
- **Revoke before proving.** Cheaper by one exchange, but a provider delay or a
  bad file would leave the workload with no working client secret.
- **A new top-level family.** See "Why `organization`".
- **Reuse `--profile` for the application profile in `rotate`.** The global
  flag already selects the administrator's profile; overloading it would make
  the same flag mean the caller in one command and the subject in the next.
- **Generate the key when absent.** The other organization writes require an
  explicit key, and a generated key printed on stderr is easily lost exactly
  when a retry needs it.

## Evidence and tests

Unit tests beside the code (`managed/organization/m2m_applications/tests.rs`)
cover the removal of the client secret before validation, with every refusal
free of it; the revocation choice; the file's owner-only mode, no-clobber and
replace renames, and refusal of links and directories; application validation,
including the actor ID; canonical permissions; derived keys; and the custody
record shape. Integration tests in
`crates/omnigraph-cli/tests/integration/cli_cluster.rs`, against the mock
Intent API and a mock token endpoint, cover each command's exact requests and
keys, paging, the profile written, idempotent retries, refusals before any
write, a client secret lost to an uncertain response and reported by ID,
rotation that proves before it revokes, a failed proof that revokes nothing
and its same-key recovery, the several-client-secret refusal, and deletion's
confirmation and selection. Every create and rotate test asserts that neither
stream, in human or `--json` form, nor any operator-home file but the client
secret file contains the client secret, and that the file's mode is 0600.

## Rollout

The commands ship in one change with their documentation
(`docs/user/cli/managed-m2m-applications.md`) and the agent skill. They work
against any managed service that implements the M2M application routes; one
without them answers the first request with its usual not-found refusal.

## Unresolved questions

- Should rotation offer an overlap mode that writes and proves the new client
  secret but revokes nothing, for one shared by several hosts that need time to
  pick it up? Today a rotation always revokes the one earlier client secret or
  the ones `--revoke` names, so keeping the old one means rotating in the
  console.
- Should the proof also open a managed session with the new token, to show the
  service, not only the identity provider, accepts it?

## Decision log

- 2026-11-25: Drafted with the implementation. Decided: commands extend
  `organization`; the client secret reaches only a named file; explicit keys
  with one derived key per write; profile written before the client secret so
  rotation recovers a lost one; rotation revokes the single earlier client
  secret, or the named ones, and refuses to guess among several; revocation
  only after a successful exchange; custody in its own file.
- 2026-11-25: Renamed from "Organization machine applications in the CLI"
  (`2026-11-25-organization-machine-applications`) to follow the API's final
  shape: the `m2m-applications` collection and its `delete` action, and one
  term per concept (M2M application, client ID, client secret, permissions,
  actor ID). `machines … revoke` became `m2m-applications … delete`,
  `--machine-profile` `--application-profile`, `--scope` `--permission` and
  `--secret-file` `--client-secret-file`. No other decision changed.
- 2026-11-26: Review of PR 1546. Decided, each with a regression test:
  a delivery binds the value written (a salted SHA-256), and a rotation proves
  and revokes only while the file holds that value, exchanging the bytes it
  checked, so a restored earlier client secret is never revoked; the profile
  is a recorded create step whose replacement consent covers the first attempt
  only, so a completed create never writes its profile again; a replacement
  keeps the replaced file's owner and group or refuses before any remote write;
  the client secret file may not coincide with `--stats`, the operator
  configuration, the credential binding state or the CLI's state directories,
  checked once before the statistics recorder starts; a destination must end in
  a file name; and writing a client secret is refused on platforms without
  owner-only files (all but Unix today), documented here.
