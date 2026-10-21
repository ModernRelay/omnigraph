# Onboard and remove organization members

An organization administrator can invite teammates, inspect membership, enroll
people and change cluster admission through the normal CLI. No database or cloud
credentials are needed. The managed service must support organization team
administration, and the organization must already be activated by its operator.

Sign in to the selected organization:

```sh
omnigraph login --profile work
```

Every command below uses `--profile work`, a managed profile with an
explicit API, organization ID and one authentication provider for one identity. See
[operator registration](targets.md#login-and-registration). `org` aliases
`organization`. The caller needs the exact organization control grant
`{cluster: null, verb: members_manage}`; a provider role alone does not imply it.
The service operator's enrollment procedure grants the initial administrator.

These commands never use folder context or change the target. A cluster target
cannot administer its organization implicitly. The organization in a member
patch must equal the profile before any credential or request is used.
`--json` emits the validated API envelope.

## Invite and find people

```sh
omnigraph organization --profile work invitations send avery@example.com \
  --idempotency-key invite-avery-20261015
omnigraph organization --profile work invitations list --json
```

The invitation email lets the recipient join the organization. Wait for
acceptance before enrolling them. Acceptance alone grants no cluster admission
or graph permissions. List the organization directory to find the exact user ID,
provider membership status and current local grant version:

```sh
omnigraph organization members list --profile work --json
```

This complete directory includes active, pending and inactive provider members,
plus retained local members whose provider membership is absent. A person not yet
enrolled locally has a null principal and grant version zero. Names and emails
may be unavailable; IDs remain authoritative. More than 1,000 members or a failed
provider read refuses instead of returning a partial directory.

Invitation lists are separate pages of up to 100. Pass the returned `data.after`
as `--after CURSOR` to fetch the next page. A null cursor means the last page.
To resend or cancel an invitation, use its exact ID:

```sh
omnigraph org invitations resend invitation_example --profile work \
  --idempotency-key resend-avery-20261015
omnigraph org invitations revoke invitation_example --profile work \
  --idempotency-key revoke-avery-20261015
```

These examples select the account explicitly. Invitation output never includes acceptance
URLs or invitation tokens.

## Enroll and grant cluster access

Write a YAML patch using the exact organization, user and cluster IDs. This
example admits an accepted person to one cluster and permits discovery and
configuration-source reading:

```yaml
version: 1
organization_id: org_customer
user_id: user_avery
action: add
expected_grant_version: 0
add:
  cluster_admissions: [CLUSTER_ID]
  control_grants:
    - {cluster: CLUSTER_ID, verb: status}
    - {cluster: CLUSTER_ID, verb: source_read}
```

```sh
omnigraph organization members apply member.yaml --profile work \
  --idempotency-key enroll-avery-20261015 --json > member-receipt.json
```

Use `action: add` with version zero only for someone not yet enrolled locally.
For an enrolled person, use `action: update` and the current `grant_version` from
`members list`. `add:` and `remove:` alter only the listed grants/admissions;
unrelated access remains intact. A stale version refuses the complete edit.
List again and review the current state before preparing a new patch.

Organization grants use `cluster: null` with `create` or `members_manage`. Exact
cluster grants support `status`, `delete`, `source_read` and `source_write`.
For example, delegate team administration by adding
`{cluster: null, verb: members_manage}`. The service prevents removing its last
locally delegated organization administrator. There are no wildcard grants or
graph permissions in member patches. YAML files are regular files of at most
256 KiB; unknown fields, symlinks and conflicting changes refuse.

The receipt contains the person's stable `principal_id`. Cluster admission
permits identity-credential issuance; it does not authorize graph operations.
An existing configuration manager must add `principal:PRINCIPAL_ID` to the
appropriate Cedar policy through the normal [source and deployment
workflow](managed-lifecycle.md#prepare-and-apply-configuration). Grant only the
intended graph and configuration permissions, retaining another manager.

The teammate registers their own profile, discovers the cluster and registers
its service-attested resource identity:

```sh
omnigraph login --register work
omnigraph cluster list --profile work
omnigraph target add production --profile work --cluster CLUSTER_ID
omnigraph target inspect production --resolved
```

Verify a permitted graph read, a reversible write if granted, and a forbidden
operation. Successful cluster discovery alone is not proof of graph permission.

## Remove access

First remove the person's stable principal from applicable graph and management
Cedar policies, deploy through the normal configuration workflow, and verify
denial using a previously issued graph credential. Retain another authorized
configuration manager.

Then read the current grant version and offboard the exact user:

```sh
omnigraph organization members list --profile work --json
omnigraph organization members offboard user_avery --profile work \
  --expected-version 4 --idempotency-key offboard-avery-20261015 --json
```

Offboarding clears local grants and admissions, removes the selected organization
membership, and revokes sessions explicitly belonging to that organization and
person. It preserves the shared provider user and other organizations' sessions.
Use version zero for a provider member who has never been enrolled locally.
The receipt reports local access, provider membership and session cleanup
separately. A partial result exits 3; complete cleanup exits 0. It never claims
to change graph policy. Existing graph credentials can last up to 86,400 seconds,
so membership removal alone is not immediate graph revocation.

For local access removal only, use `action: remove` in a member patch with the
current version and no `add` or `remove` changes. This preserves provider
membership and sessions; it also leaves graph policy unchanged.

## Recover an interrupted request

Every write requires an explicit `--idempotency-key`. Keep the exact YAML,
arguments and key until the outcome is known. The CLI submits once and uses a
35-second request deadline. A timeout does not cancel a provider action.
Repeat the identical request with the same key to retrieve its retained result;
conflicting content refuses. The service never resends an uncertain provider
write. A new key starts a new action, so do not change it merely to retry.

A partial offboard result stays partial on replay. Inspect the current directory
and retained receipt before preparing a new versioned request for the remaining
cleanup. Human progress and the key go to stderr; `--json` keeps one response on
stdout. Ordinary permission refusals exit 2 and malformed/transport failures
exit 1, with no storage fallback.
