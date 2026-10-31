---
rfc: "2026-10-31-provider-role-policy-membership"
title: "Provider-role policy membership"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-31
updated: 2026-10-31
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Provider-role policy membership

## Summary

Let a policy group name identity-provider role slugs as well as literal
actors. A server configured with a membership snapshot treats an actor as a
member of every group whose roles the snapshot assigns to that actor.
Credentials stay identity-only. Policy still decides what each group may do;
the provider decides who holds each role.

## Motivation

`PolicyActorSelector` names one group, and a group is a literal list of actor
ids. Managed deployments allocate `principal:<id>` per environment database,
so one person has unrelated ids in staging and production, and a policy file
cannot move between them. Every team change is a policy deployment per
cluster (and per policy file where management and graph policies both name
the person), and removing someone from the provider does not
remove their policy entries. Enterprise customers expect access to follow
directory and SSO role assignments.

[Identity credentials and applied policy authorization](2026-09-09-identity-credentials-and-applied-policy.md)
deliberately refuses membership claims in credentials, so membership must not
travel in the token. That RFC already admits OIDC subjects from a bounded,
revisioned snapshot. This RFC adds role facts through the same kind of
snapshot.

## User and operational behavior

```yaml
version: 1
groups:
  owners: ["principal:01JD8Y3T2Z"]          # existing literal form, unchanged
  analysts:
    actors: ["principal:01JE4K9M1Q"]        # optional literal members
    provider_roles: [analyst, data-steward]  # role slugs from the snapshot
rules:
  - id: analysts-read
    allow:
      actors: { group: analysts }
      actions: [read, invoke_query]
      branch_scope: any
```

The list form keeps its meaning. The map form takes `actors` and
`provider_roles`; at least one must be non-empty. Unknown keys refuse.

`omnigraph-server --membership-snapshot FILE` loads public JSON:

```json
{
  "version": 1,
  "binding": {"issuer": "...", "organization": "...", "account": "...",
              "cluster": "...", "incarnation": "...", "root": "..."},
  "revision": 42,
  "generated_at": 1793420000,
  "expires_at": 1793420300,
  "members": [{"actor": "principal:01JE4K9M1Q", "roles": ["analyst"]}]
}
```

`GET /graphs/{graph_id}/access` and `policy explain` report role-derived
groups separately from literal membership. Because `policy explain` and
`policy test` read the applied bundle offline, they take `--roles <slug>...`
(and test cases a `roles:` list) to simulate snapshot membership. Preview
reports a referenced role that no current snapshot assigns, without failing
validation.

## Design

- Membership is evaluated per request: the actor's Cedar entity gets parents
  for literal groups plus groups whose `provider_roles` intersect the actor's
  snapshot roles. Cedar policies and actions are unchanged.
- Snapshot validation mirrors OIDC admission: exact binding to the served
  root and identity, positive monotonic revision, `expires_at - generated_at`
  at most 300 seconds, 30 seconds of clock skew, local reload every five
  seconds, at most 256 KiB, 1,000 members, 32 roles per member and 128-byte
  slugs with the provider's slug alphabet.
- An invalid refresh leaves the previous snapshot usable only until its own
  expiry. After expiry, role-derived membership is empty. Literal
  membership is unaffected.
- The publisher lists only actors holding a role that applied policy
  references, so the member bound counts actors with role-derived access. A
  server never truncates: an over-bound snapshot is refused like any invalid
  one.
- No network call is made during authorization.
- Direct CLI and embedded writers have no snapshot, so role-derived permits do
  not apply to them; invariant 10 gates still run.
- Group names and literal ids keep their existing validation. A role slug
  never matches a literal actor id.

## Invariants

[Invariant 10](../dev/invariants.md) holds: the actor still comes only from
the verified credential, and the engine `_as` gate still runs for every
writer. Writers without a snapshot fail closed for role-derived permits.
Invariant 11 holds through the fixed size, count and staleness bounds and
the absence of network calls during authorization. The snapshot is not the
deny-listed "maintained parallel truth": the provider is the authority for
role membership, the server never edits or rebuilds the snapshot, and an
expired copy stops counting rather than being served as current.

- Credentials remain identity-only; no role, group or permission claim is
  parsed from a token.
- Applied policy remains the only source of actions. A snapshot can only
  make an actor a member of groups that applied policy declares.
- Fail closed: an absent, expired or invalid snapshot yields no role-derived
  membership. Maximum staleness is 330 seconds.
- A candidate policy still cannot authorize its own installation.

## Compatibility and reversibility

Existing policies parse and evaluate identically. A server without
`--membership-snapshot` treats `provider_roles` as matching nobody, which
preview reports. Removing the flag or the map form reverts to literal
membership with no stored-state migration.

## Alternatives

- Roles in credential claims: contradicts the identity-only credential RFC,
  ties revocation to credential lifetime, and AuthKit carries a role only for
  the organization selected at sign-in.
- Rewriting policy from provider events: makes provider events deploy
  configuration and loses the reviewed-change property.
- Raw directory groups: the provider does not expose them in tokens or
  memberships; roles already absorb directory and SSO mappings.
- Longer snapshot lifetime: improves outage tolerance but weakens revocation;
  left as an unresolved question rather than a default.

## Evidence and tests

- Parser: list and map forms, unknown keys, empty map, slug bounds.
- Evaluation: literal-only, role-only, mixed and absent-snapshot cases; an
  actor with a role but no literal entry; a role referenced by no rule.
- Snapshot lifecycle: binding mismatch, revision regression, oversized file,
  expiry during a request stream, invalid refresh before and after expiry.
- Explain and access routes distinguish literal and role-derived groups.
- Engine-direct writers ignore role-derived membership.

## Rollout

Ship parsing and evaluation behind the absent flag. A deployment enables it
only after it can publish snapshots within the bound and has qualified
provider observation, pagination and outage behavior.

## Unresolved questions

- Whether the snapshot lifetime should be configurable above 300 seconds.
- Whether groups may be declared once per cluster and referenced from graph
  policies. A policy file bound through `applies_to` to several graphs
  already shares its groups across them; cluster management policy and graph
  policy still declare groups separately.
- Whether the snapshot should merge with the OIDC admission document.
- A paged or role-indexed format for clusters above 1,000 role-holding
  actors.

## Decision log

- 2026-10-31: drafted. The publisher lists only actors holding a referenced
  role, and over-bound snapshots refuse rather than truncate.
