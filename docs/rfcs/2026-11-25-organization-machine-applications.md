---
rfc: "2026-11-25-organization-machine-applications"
title: "Organization machine applications in the CLI"
track: maintainer
status: draft
implementation: in-progress
authors:
  - ragnorc
created: 2026-11-25
updated: 2026-11-25
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Organization machine applications in the CLI

## Summary

`omnigraph organization machines` lets an organization administrator manage
*machines*: organization-owned client applications that unattended workloads
sign in as through an `oauth_client_credentials` profile. Four commands, run
from the administrator's own managed profile, call the managed service's
existing Intent API and add no Omnigraph server endpoint:

- `machines list` shows every machine with its client ID, service scopes,
  creation time and Cedar principal (`principal:ID`).
- `machines create NAME [--scope S]... --secret-file PATH [--create-profile P]`
  creates the application, generates its first secret, writes the secret only
  to `PATH`, and can add a profile `P` for it.
- `machines secret rotate --machine-profile P [--revoke SECRET_ID]...`
  writes a new secret to the profile's file, proves a client-credentials token
  exchange with it, and only then revokes the earlier secret.
- `machines revoke NAME_OR_CLIENT_ID` retires the application after
  confirmation.

A machine is an identity, not a permission: creating one changes no Cedar
policy, and retiring one leaves policy and already-issued tokens as they are.
A disclosed secret reaches exactly one place, the named file; it never reaches
an output stream, `--json`, an error, a log, request custody or the operator
configuration.

Contract: keeps E6. The change adds no graph operation; a machine principal
reaches a graph only through the installed policy, as every caller does.

## Motivation

The workload-profile guide told operators to create the client application
and obtain and rotate its secret "through your identity provider's
administration tools". That leaves the steps that decide whether an unattended
workload is safe outside the CLI: which organization owns the application, which
service scopes it carries, which principal to name in Cedar, and where the
one-time secret goes. The managed service now exposes machine administration
in its Intent API, with the same authority model as member administration (a
human session with `members_manage`, scopes bounded by the caller's own), so the
CLI can close that gap without new server surface.

Two properties need a design rather than a thin wrapper. The secret is
disclosed once and cannot be read back, so delivery and uncertain outcomes
decide whether a secret is lost or leaked. Rotation must not cut a workload off:
the old secret may be revoked only once the new one is in place and proven, and
the CLI cannot tell which secret an old file held.

## User and operational behavior

All four commands take the administrator's profile from `--profile` or
`defaults.profile`, refuse `--target`, `--graph` and `--as` as the rest of
`organization` does, and print the validated result as text or, with
`--json`, one JSON document on stdout. Every write takes a required
`--idempotency-key`; progress and the key go to stderr.

**List.** `machines list` follows `after` cursors for at most ten pages and
returns one envelope whose `data.machines` holds every machine and whose
`after` is null; `allowed_scopes` and `token_endpoint` must agree across pages.
A foreign organization, a machine whose `actor_id` is not
`principal:<principal_id>`, a duplicate client ID or a repeated cursor refuses
the whole listing.

**Create.** Scopes are canonical (sorted, distinct), at most five, and never
`members_manage`; the service further requires that the caller holds each.
Before any remote write the command refuses an existing secret file without
`--replace-secret-file`, any symbolic link or directory at that path, a missing
directory, an existing profile name without `--replace-profile`, and the
administrator's own profile name. It then creates the application, writes the
profile if asked, generates the secret and writes the file. The profile holds
the administrator profile's `api`, `organization_id` and `transport`, the
`token_endpoint` the machine listing reports, the client ID and
`client_secret: {file: PATH}` with `PATH` absolute. Output names the client ID,
the principal, the secret ID, the file and the profile, and says that the
machine has no graph access until Cedar names it.

**Rotate.** `--machine-profile` names an `oauth_client_credentials` profile,
other than the administrator's, with the same API and organization and a
`{file: PATH}` secret. The global `--profile` keeps its meaning, the
administrator's profile, so the machine profile has its own flag. The command
lists the machine's secrets, chooses what to revoke (below), generates a
secret, replaces the file, exchanges the secret for a token at the profile's
token endpoint, then revokes the chosen secrets. A failed exchange revokes
nothing and reports `machine_secret_unproven` with the new secret's ID.

**Retire.** The selector is an exact client ID or an exact name; a name that
several machines share refuses with `machine_ambiguous`. The command follows
the CLI's destructive-write convention: global `--yes` consents, an
interactive terminal is asked, and `--json` or noninteractive input refuses
with `confirmation_required`. Output reminds the operator that issued tokens
last until expiry and that Cedar still names the principal.

## Design

### Why `organization`, not a new family

Machines belong to the organization exactly as members and invitations do: the
same Intent API resource tree (`/v1/organization/...`), the same authority
(`members_manage`, scoped to the profile's organization), the same refusal of
cluster targets, and the same request custody. A new top-level family would
duplicate the profile selection, scope refusal, organization verification and
custody, and split one concept, "who belongs to this organization and with
what service permissions", across two command trees. `organization` already
says it manages identities and that graph authority stays in Cedar; a machine is
one more kind of identity.

### Secret custody

The transport strips every `client_secret` field from every response. Secret
generation is the one call that asks it to keep that field verbatim, through the
same per-call exemption that keeps exact source files in a source export
(`ResponseLimits::preserved`). The first thing the command does with that body is
remove the field into a value type that implements no `Debug`, `Display` or
`Serialize`, so the type system leaves one use: writing it to the file. Every
refusal after that point is a fixed message. Human and `--json` output are built
from validated, secret-free structures, never from the response body.

The file is written as the CLI's other atomic writes are: a temporary file in
the destination's directory, created owner-only (0600) before the first remote
write, so an unwritable directory refuses while nothing exists remotely; then
write, `fsync`, rename (`create` uses a no-clobber rename unless replacement was
allowed), and `fsync` of the directory. The CLI writes regular files only; it
refuses to write through or over a symbolic link, so a link-rotated mount stays
under its own manager. The codebase has no zeroization dependency, and this
change does not add one: the secret lives in one owned string dropped right
after the write.

### Request identity and progress

Machine writes follow the organization family's uncertain-write rule: a write
requires an explicit key, the exact request is retained before the first remote
effect, an identical retry repeats the identical writes, and a different request
under a retained key refuses. Create and rotate are several writes, so the CLI
derives one key per write from the request key (`KEY:machine`, `KEY:secret`,
`KEY:revoke-N`, in the order the retained request fixes); retire is one write
under `KEY`. The request key is therefore limited to 200 characters.

The record also holds the request's progress, written after each step and
before the next: the token endpoint and the created machine for `create`; the
client ID, token endpoint, file and the chosen revocation targets for `rotate`;
the resolved client ID, principal and name for `retire`; and, for the two
secret-writing commands, the ID of the secret whose value the request wrote to
its file. A retry skips what is recorded and resumes at the first unrecorded
step, so it never creates a second machine or generates a second secret, and
never re-resolves a name after the machine it named has gone. Records live in
`machines.json` beside the organization custody file, under the same lock and
the same bounds (128 records, 1 MiB, no eviction). A separate file keeps an
older CLI, which validates `requests.json` strictly, from refusing all
organization writes after a newer one ran a machine command.

### Uncertain and lost secrets

The service discloses a secret in the first successful response only; its
stored receipt, and so every exact replay, holds `client_secret: null` and
`secret_available: false`. A secret is therefore lost when that response does not
arrive, when it arrives and fails validation, or when the file cannot be
written. In each case the command reports what it knows, and a retry with the
same key learns the secret's ID from the replay. When the replay names a secret
this request did not record as delivered, the command refuses with
`machine_secret_undelivered`, naming the client and secret IDs, says the value
cannot be recovered, and gives the recovery: revoke that ID and generate
another. It never generates a replacement on its own.

`create` writes the profile before it generates the secret. A create whose
secret was lost thus leaves a profile, and
`secret rotate --machine-profile P` with a new key is the recovery: it finds
that one secret, writes a new one, proves it and revokes the lost one. Without
`--create-profile`, the recovery is the managed console's Credentials page. A
lost rotation names the exact `--revoke` arguments for a new rotation that
retires both the lost secret and the earlier ones it was meant to replace.

### Rotation's revocation choice

The CLI cannot know which secret ID the old file held: the service shows only an
ID and a short hint, and the file holds only the value. The command lists the
secrets before it generates anything and decides once, retaining the decision:

- with `--revoke`, exactly the named IDs, each of which must exist;
- otherwise, the one secret if there was exactly one;
- nothing if there were none;
- with more than one and no `--revoke`, a refusal that lists every candidate's
  ID, hint and creation time, before any write.

Exactly one earlier secret is the common case, a machine that this command or
the console rotated before, and revoking it is what rotation means. With
several, guessing would cut off whichever workload holds the one not guessed.
The list is read before generation so the new secret is never a candidate.
Revocation follows the exchange: a secret that cannot sign in never replaces a
working one. If the exchange fails, both secrets stay active and the file keeps
the new value; the same key repeats the exchange and then revokes.

The exchange reuses the `oauth_client_credentials` provider's acquisition: one
form-encoded request with system trust and a 25-second bound, reading the secret
back from the file, so it proves the file as written. The token is dropped
unused.

## Invariants

Invariant 10 (trust at the boundary, enforced at the engine) is untouched: a
machine authenticates to the managed service like any workload, and every graph
action it attempts is judged by applied Cedar. Invariant 11 (bounded, observable
failures): listing is bounded to ten pages, every request has the family's
35-second deadline, no write is retried automatically, and every partial
outcome (machine created but secret lost, secret written but unproven,
revocation incomplete) is a typed result that names what exists. No deny-list
item applies; the custody file is local operator custody, not a second source of
truth for the service's state, and the service remains authoritative.

## Compatibility and reversibility

The change adds commands, one custody file and one optional transport
exemption. Operator configuration gains no field: `--create-profile` writes the
existing `oauth_client_credentials` shape. An older CLI ignores
`machines.json`. Reverting removes the commands; profiles and secret files they
wrote keep working, since they are ordinary configuration. `cluster token --ttl`
changes only its help and documentation: the value is a requested lifetime, the
service may issue less, and `expires_at` reports what it issued.

## Alternatives

- **Print the secret, or offer `--secret-stdout`.** Rejected: terminals,
  scrollback, CI logs and agent transcripts retain stdout, and the service
  refuses to disclose the secret again. A file is the one destination whose
  permission and lifetime the operator controls.
- **Write the secret into the operator configuration or the OS store.** The
  configuration is shared and copied; the profile grammar deliberately refuses
  literal secrets. The OS store is unavailable on the hosts unattended workloads
  usually run on.
- **Revoke every earlier secret on rotation.** Simpler, but it cuts off any
  other host holding another secret, which is the reason a machine has several.
- **Revoke before proving.** Cheaper by one exchange, but a provider delay or a
  bad file would leave the workload with no working secret.
- **A new `machines` top-level family.** See "Why `organization`".
- **Reuse `--profile` for the machine profile in `rotate`.** The global flag
  already selects the administrator's profile; overloading it would make the
  same flag mean the caller in one command and the subject in the next.
- **Generate the key when absent.** The other organization writes require an
  explicit key, and a generated key printed on stderr is easily lost exactly
  when a retry needs it.

## Evidence and tests

Unit tests beside the code (`managed/organization/machines/tests.rs`) cover the
removal of the secret before validation, with every refusal free of it; the
revocation choice; the file's owner-only mode, no-clobber and replace renames,
and refusal of links and directories; machine validation, including the
principal Cedar names; canonical scopes; derived keys; and the custody record
shape. Integration tests in `crates/omnigraph-cli/tests/integration/cli_cluster.rs`,
against the mock Intent API and a mock token endpoint, cover each command's
exact requests and keys, paging, the profile written, idempotent retries,
refusals before any write, a secret lost to an uncertain response and reported
by ID, rotation that proves before it revokes, a failed proof that revokes
nothing and its same-key recovery, the several-secret refusal, and retirement's
confirmation and selection. Every create and rotate test asserts that neither
stream, in human or `--json` form, nor any operator-home file but the secret
file contains the secret, and that the file's mode is 0600.

## Rollout

The commands ship in one change with their documentation
(`docs/user/cli/managed-team.md`) and the agent skill. They work against any
managed service that implements the machine routes; one without them answers
the first request with its usual not-found refusal.

## Unresolved questions

- Should rotation offer an overlap mode that writes and proves the new secret
  but revokes nothing, for a secret shared by several hosts that need time to
  pick it up? Today a rotation always revokes the one earlier secret or the
  ones `--revoke` names, so keeping the old one means rotating in the console.
- Should the proof also open a managed session with the new token, to show the
  service, not only the identity provider, accepts it?

## Decision log

- 2026-11-25: Drafted with the implementation. Decided: commands extend
  `organization`; the secret reaches only a named file; explicit keys with one
  derived key per write; profile written before the secret so rotation recovers
  a lost one; rotation revokes the single earlier secret, or the named ones, and
  refuses to guess among several; revocation only after a successful exchange;
  machine custody in its own file.
