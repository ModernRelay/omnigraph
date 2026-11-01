---
rfc: "2026-11-01-provider-role-policy-membership"
title: "Provider-role policy membership"
track: public
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-11-01
updated: 2026-11-01
discussion: https://github.com/ModernRelay/omnigraph/pull/1223
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Provider-role policy membership

## Summary

A policy group may name identity-provider role slugs as well as literal
actor ids. The server learns which actor holds which role from a bounded,
revisioned, short-lived public document, the membership snapshot, published
beside the existing OIDC admission snapshot and read the same way. At
request time an actor belongs to a group when the group lists the actor or
lists a role the current snapshot assigns to the actor.

Nothing else changes. Applied policy remains the only source of actions.
Credentials stay identity-only. The snapshot can place an actor only in a
group that applied policy declares, and only through a role that group
names. Without a snapshot, or after it expires, role-derived membership is
empty and literal membership is unaffected.

## Motivation

### How membership works today

A policy bundle declares `groups`, each a literal list of actor ids, and
every rule selects one group (`actors: { group: analysts }`). The group map
is compiled into Cedar entities when the bundle is activated, and a request
from an actor that no group lists is denied before Cedar runs
(`crates/omnigraph-policy`, `PolicyEngine::authorize`). Cluster management
policy and graph policy use the same format and are activated atomically
with the rest of the configuration.

The identity side is separate by design.
[Identity credentials and applied policy authorization](2026-09-09-identity-credentials-and-applied-policy.md)
made native credentials identity-only and admits external OIDC subjects
from a public snapshot that maps `subject` to `principal_id` and nothing
else: at most 1,000 principals, 256 KiB, a 300-second lifetime, re-read
every five seconds, with a 330-second maximum stale allowance. The server
makes no network call during authorization.

In the managed service, a person's actor id is `principal:<id>`, allocated
per customer account when the person first signs in. Their WorkOS
organization membership carries one or more role slugs, which the identity
provider's administrators assign directly, through SCIM group mappings or
through SSO group mappings. The managed service reads those roles to decide
organization-wide service permissions; it never reads them into native
policy, and every managed-service document marks that integration as
deferred until a native trust, freshness and outage contract exists.

### What goes wrong

- **A team change is a configuration deployment.** Giving a new analyst
  read access means an existing configuration manager pulls the source,
  adds `principal:01JE…` to the right group in the right bundle, pushes,
  previews and applies. Removing or moving a person is the same work in
  reverse. For a cluster with graph policies and a management policy, it
  is the same edit in several files.
- **Policy files are not portable.** Actor ids are allocated per account,
  so the same person has unrelated ids in a staging and a production
  account. A policy reviewed in one cannot be applied to the other without
  rewriting every group.
- **Role assignment already exists and is already enforced elsewhere.**
  Enterprise customers assign roles in their identity provider, WorkOS maps
  them to role slugs, and the managed service honors them for service
  permissions. Graph permissions are the one place where a role assignment
  does nothing and a separate, hand-maintained list has to agree with it.

What does not go wrong, and what this RFC therefore does not claim to fix:
removing a person's organization membership already stops them obtaining
new credentials. Their literal policy entries become dead weight, not live
access, once their last credential expires. This RFC removes the cleanup
and the churn; it does not change credential lifetimes.

### Why not the obvious fixes

- **Roles in the credential.** Refused by the identity RFC and still
  wrong: it ties revocation to credential lifetime, and a token lists roles
  only for the organization selected at sign-in.
- **Rewrite the `groups` lists from provider events.** Turns an identity
  provider event into a configuration deployment, loses the reviewed-change
  property of the source, and needs an actor with `config_manage` on every
  cluster that is nobody's identity.
- **Ask the provider at request time.** Breaks the no-network rule the
  identity RFC established and makes authorization availability depend on
  a third party.

### Why a snapshot fits

The server already consumes exactly this kind of fact for identity: a
public, bounded, revisioned document with a short lifetime, bound to the
exact cluster and serving root, re-read from a local file, failing closed
on expiry. Membership is a second fact of the same shape about the same
population, and a second document lets the two keep different bounds and
different publishers without changing the admission contract.

### Goals and non-goals

Goals: a group may name provider roles; role facts arrive through a
bounded local document; authorization stays offline; outage, overflow and
activation behavior are specified before any deployment enables it.

Non-goals: narrowing a client below its user's permissions, agent or
delegated identities, provider-group synchronization into policy source,
provider-side freshness guarantees, and resource-scoped (per-cluster)
provider roles. Each is a separate proposal.

## Vocabulary

| Term | Meaning |
|---|---|
| provider role | A role slug owned and assigned by the identity provider. The server treats it as an opaque string. |
| membership snapshot | The public document that assigns provider roles to actors for one cluster. |
| literal membership | An actor listed in a group's `actors`. |
| role-derived membership | An actor placed in a group because the group names a role the current snapshot assigns to the actor. |
| publisher | Whatever writes the membership snapshot. In the managed service this is the control plane; a self-hosted operator can write it by hand or from any directory. |

## User and operational behavior

### Policy format

```yaml
version: 1
groups:
  owners: ["principal:01JD8Y3T2Z"]           # list form, unchanged
  analysts:
    actors: ["principal:01JE4K9M1Q"]         # optional literal members
    provider_roles: [analyst, data-steward]  # optional provider roles
  admins:
    provider_roles: [platform-admin]
rules:
  - id: analysts-read
    allow:
      actors: { group: analysts }
      actions: [read, invoke_query]
      branch_scope: any
```

A group is either the existing list of actor ids or a map with `actors`
and `provider_roles`. At least one of the two must be non-empty. Unknown
keys refuse, as elsewhere in the bundle. A role slug is 1 to 128 bytes of
printable ASCII without whitespace. A group names at most 64 roles. A role
slug is never compared with an actor id; the two are different namespaces.

### Server flag

```text
omnigraph-server --membership-snapshot /membership/membership.json
```

The file is public JSON. It carries no secret and no permission; it says
which actor holds which role.

```json
{
  "version": 1,
  "revision": 42,
  "generated_at": 1793420000,
  "expires_at": 1793420300,
  "issuer": "https://auth.example.com",
  "organization_id": "org_01H…",
  "account_id": "acct_01H…",
  "cluster_id": "team-cluster",
  "cluster_incarnation": "01JE…",
  "canonical_root": "s3://…/clusters/team-cluster",
  "members": [
    { "actor": "principal:01JE4K9M1Q", "roles": ["analyst"] },
    { "actor": "principal:01JD8Y3T2Z", "roles": ["platform-admin", "member"] }
  ]
}
```

The publisher lists every actor it can map that holds at least one role.
It does not filter by what policy references; see "Policy activation".

### What a user sees

- An actor with role `analyst` and no literal entry can do whatever the
  `analysts` group permits, on every graph whose applied policy declares
  that group with that role.
- `GET /graphs/{graph_id}/access` gains a `membership` object:
  `{"status": "current" | "expired" | "absent", "roles": [...]}`. `roles`
  lists the caller's own roles from the snapshot and is empty unless
  `status` is `current`. The route still names actions, never rules, and
  still reveals nothing about other actors.
- A denial while the snapshot is expired or absent, on a server whose
  applied policy names any provider role, returns the ordinary 403
  `forbidden` with `detail: {"membership": "unavailable"}`. The caller can
  tell an outage from a revocation. A denial with a current snapshot
  carries no detail.
- `omnigraph policy explain` takes `--roles <slug>...`, and `policy test`
  cases take an optional `roles:` list, so an operator can answer "what
  could someone with these roles do" offline. `policy validate` reports the
  number of provider roles the bundle names.
- Direct (`file://`, `s3://`) and embedded writers have no snapshot. For
  them role-derived membership is empty and literal membership applies.
  Policy still covers them; it is only narrower than over HTTP.

## Design

### Evaluation

At load, the policy engine builds, in addition to today's entities, an
index from role slug to the groups that name it. At request time the server
supplies the actor's current roles (empty when the snapshot is expired or
absent). The engine computes the actor's groups as literal groups plus
every group whose `provider_roles` intersect the actor's roles, builds the
actor entity with those parents, and evaluates the same Cedar policies as
today. The unknown-actor check becomes: the actor is listed literally or
derives at least one group. The policy crate performs no I/O; the server
resolves roles from an in-memory snapshot exactly as it resolves OIDC
admissions. Policy evaluation remains per request and offline.

The three bundle kinds (cluster, graph, catalogue) accept the map form.
`config_manage` and `graph_list` may be granted to a role-derived member.

### Membership snapshot

All fields are required and unknown fields refuse.

| Field | Rule |
|---|---|
| `version` | 1 |
| `revision` | positive, strictly increasing across installs |
| `generated_at`, `expires_at` | Unix seconds; `expires_at - generated_at` at most 300; `generated_at` at most 30 seconds in the future |
| `issuer` | the identity provider, an https URI as in the admission snapshot |
| `organization_id`, `account_id`, `cluster_id`, `cluster_incarnation`, `canonical_root` | the exact serving binding, validated against the admitted serving snapshot before graphs open, as `--oidc-identity-trust` is |
| `members[].actor` | a full actor id, 1 to 256 bytes, unique within the file |
| `members[].roles` | 1 to 256 distinct slugs, each 1 to 128 bytes |
| bounds | at most 1,000 members, at most 8,000 role assignments in total, at most 2 MiB |

The member bound equals the admission bound because the two documents
describe the same population: an OIDC-admitted principal is one of at most
1,000, and every WorkOS member holds at least one role. The role bound is
per file, not per member, so one member may legitimately hold many roles
without forcing the file bound up; 8,000 assignments at the maximum slug
and actor size fit in 2 MiB with room for syntax, so a file within the
count bounds is always within the byte bound. Overflow is therefore a
count the publisher can state exactly, not a size it discovers after
serialization.

Refresh follows the admission snapshot: the server re-reads the file every
five seconds (comparing size and modification time before parsing), and
installs the new bytes only when the binding is unchanged, the revision is
higher, and `generated_at` has not moved backwards. An invalid refresh is
logged and leaves the current snapshot usable until its own `expires_at`.
Every request checks that expiry; a snapshot is never used more than 300
seconds after its `generated_at`, 330 with clock skew. There is no
network verification and no fallback.

The flag is optional. A server started without it treats every
`provider_roles` entry as matching nobody. A server started with it refuses
to boot when the file is missing, unparsable or bound to a different
cluster or root. A file that is correctly bound but already expired does
not refuse boot: the server serves with literal membership only and logs
the expiry, because that is the same state the server enters during any
publisher outage, and a restart must not be worse than staying up.

### Publisher contract

The server trusts whoever can write the file, as it does for admission.
That party holds membership authority bounded by applied policy: it can
place an admitted actor in any group whose `provider_roles` it can match,
and in no other. A publisher that can also write the admission snapshot
can map any subject to any principal already; this document adds no new
authority to that party and should be protected like the admission
snapshot (in the managed service: written by the control plane into the
cluster resource, projected by the operator, mounted read-only).

The publisher must set `generated_at` to the earliest observation time of
any fact in the file. The native bound then means "facts observed at time
T are never used after T + 330 s". How quickly the publisher observes a
provider change is the publisher's contract, not this one. For the record,
WorkOS applies Directory Sync role changes immediately, applies SSO group
role changes at the user's next sign-in, deletes roles asynchronously, and
emits `organization_membership.updated` on every role change through an
ordered, replayable Events API.

The publisher must refuse to publish a set that exceeds a count bound and
report which bound, rather than truncate or publish a subset. The server
refuses an over-bound file like any invalid file. Both are deliberate: see
"Overflow".

### Policy activation

Membership facts are not part of a deployment, and a deployment does not
wait for them. Activation stays atomic and unchanged. A group's
role-derived members at any instant are the actors the current snapshot
assigns one of its roles, whichever policy revision is active.

Because the publisher publishes every role every mapped actor holds, there
is no ordering problem between a policy change and the snapshot: a policy
that switches its administrators from role `a` to role `b` activates
against a snapshot that already lists the holders of `b`, if there are
any. The remaining risk is a role nobody holds, and the preview surfaces
it. `POST /cluster/plan` reports, per candidate bundle, without failing
the preview:

- `role_unassigned`: a named provider role that the current snapshot
  assigns to no actor.
- `membership_unavailable`: the bundle names provider roles and the
  snapshot is expired or absent, so every role-derived group is empty.
- `management_roles_only`: a cluster-management group that grants
  `config_manage` and has no literal `actors`. Such a policy has no
  administrator during a publisher outage. The report is a warning, not a
  refusal, because an operator may accept that for a cluster whose
  management is itself provider-driven.

The existing rule stands: a candidate policy never authorizes its own
installation, and a role-derived administrator is an administrator only
under the applied policy that names the role.

### Overflow

A snapshot over a count bound is refused in full, by the publisher before
publication and by the server if published anyway. The consequence is
that, 330 seconds after the last valid snapshot's `generated_at`, every
role-derived member of every group on that cluster loses that membership
at once, while literal members keep theirs. This RFC chooses that over
partial publication for three reasons: a partial file would silently
revoke an unpredictable subset; the bounds are large enough that
exceeding them is an operational event worth stopping on; and the failure
is visible, through the publisher's report, the server log, the `/access`
`membership.status`, and the `membership: unavailable` detail on denials.
Deployments that cannot tolerate it keep literal `actors` in the groups
that matter and preview warns them when they do not.

### Outage and recovery

| State | Role-derived membership | Literal membership |
|---|---|---|
| no flag | none | normal |
| current snapshot | per snapshot | normal |
| expired snapshot, no valid refresh | none | normal |
| file absent at boot, flag set | server refuses to boot | — |
| file expired at boot, flag set | none until a valid file arrives | normal |
| binding mismatch | server refuses to boot or refresh | normal if refresh |

Recovery is a valid file with a higher revision; it is in effect within
five seconds. Nothing is restarted and no deployment is needed.

## Invariants

- Applied policy remains the only source of actions. A snapshot can make
  an actor a member of a declared group; it cannot create a group, a rule
  or an action.
- Credentials remain identity-only. No role, group or permission claim is
  parsed from a native or OIDC token; the OIDC verifier keeps ignoring
  `roles` and `permissions` claims as it does today.
- Fail closed: an absent, expired or invalid snapshot yields no
  role-derived membership. Maximum staleness of a used fact is 330
  seconds after its `generated_at`.
- No network call during authorization.
- A candidate policy cannot authorize its own installation.
- Policy still covers non-HTTP writers (invariant 10): direct and embedded
  writers evaluate literal membership, which is a subset of what HTTP
  allows, never a superset.
- Failures are loud (invariant 8): overflow, binding drift and refresh
  refusal are refusals with reasons, never truncation or silent reuse.

## Compatibility and reversibility

Every existing bundle parses and evaluates identically; the list form is
unchanged and the map form is additive. A server without the flag matches
no provider role, which the preview reports as `membership_unavailable`.
Removing the flag, the map form or the snapshot reverts to literal
membership. No stored state changes: the snapshot is a runtime input, not
part of the ledger.

The admission snapshot is untouched. Its contract that admissions carry
"no Cedar roles, groups, actions or graph grants" remains true; role facts
live in a different file with a different bound.

## Alternatives

- **Extend the admission snapshot with `roles` per principal.** One file,
  one publisher, one refresh. Rejected because it couples an identity fact
  to a membership fact that has different bounds (roles multiply the file
  size), may have a different publisher in self-hosted deployments, and
  would require re-opening an accepted, complete RFC's contract.
- **A `role` actor selector on rules** (`actors: { role: analyst }`).
  Shorter, but it makes roles a second way to address principals and
  bypasses the group abstraction that explain, test and access already
  report in terms of. Groups naming roles keep one selector.
- **Filter the snapshot to roles the applied policy references.** Smaller
  files, but a policy that introduces a new role activates before the
  snapshot lists its holders, and the publisher must watch applied policy
  to know what to publish. Publishing every role removes both problems.
- **Longer snapshot lifetime.** Better outage tolerance, weaker
  revocation; the admission precedent is 300 seconds and this RFC keeps it.
  Left as an unresolved question.
- **Treat resource-scoped (per-cluster) provider roles as the fact.**
  WorkOS fine-grained authorization can assign a role on a specific
  resource and list who holds a permission on it, which would let one
  organization give a person `analyst` on one cluster only. It is not
  reachable through identity-provider group mapping today and emits no
  events, so a publisher would poll. Deferred; see unresolved questions.

## Evidence and tests

- Parser: list and map forms; empty map; unknown key; role slug bounds;
  more than 64 roles in a group; a role slug equal to a literal actor id
  stays distinct.
- Evaluation matrix: literal only, role only, both, neither; an actor with
  a role but no literal entry is known; a role no rule references grants
  nothing; `config_manage` through a role; catalogue bundle through a role.
- Snapshot lifecycle: binding mismatch at boot and at refresh; revision
  regression; same revision with different bytes; `generated_at` moving
  backwards; oversize file; member, assignment and slug bounds at and past
  the limit; expiry between two requests in one connection; invalid refresh
  before and after expiry; recovery with a higher revision.
- Boot: flag without file refuses; expired file serves with literal
  membership and logs.
- `/access` `membership` object in each status; denial detail present only
  when unavailable and only when policy names a role.
- Preview: `role_unassigned`, `membership_unavailable` and
  `management_roles_only` each reported and none failing the preview.
- `policy explain --roles` and `policy test` `roles:` on a direct root.
- Direct-root and embedded writers ignore the snapshot.
- Audit records are unchanged: the actor is the actor; no role is recorded.

## Rollout

1. Parser and evaluation land behind the absent flag; no behavior changes
   for any existing server.
2. A deployment enables the flag only after its publisher proves, against
   a test cluster: full pagination of the provider population, correct
   `generated_at` under partial observation, refusal and reporting at each
   count bound, behavior through a publisher outage longer than 330
   seconds, and recovery. For the managed service that proof and its
   control-plane contract are a separate decision; this RFC supplies the
   native half.
3. Operators move groups to roles one bundle at a time, keeping literal
   `actors` on management groups until they have watched one outage.

## Related RFCs

- [Identity credentials and applied policy authorization](2026-09-09-identity-credentials-and-applied-policy.md)
  (accepted): this RFC reuses its admission-document model and keeps its
  rule that credentials carry no membership. Its admission contract is
  unchanged; no disposition note is needed.
- [HTTP API conventions](2026-10-19-http-api-conventions.md) (accepted):
  the `membership: unavailable` detail rides on the existing 403
  `forbidden` body.
- [Graph access serving and cross-process writes](2026-10-16-graph-access-serving-and-cross-process-writes.md)
  (accepted): describes what `GET /graphs/{graph_id}/access` reports; the
  `membership` object is an additive field there.
- RFC 0059 (config CLI coherency): `organization members set --role`
  already treats roles as provider-owned and accepts a repeated `--role`;
  the managed service bounds that set at 256 per member. This RFC's
  per-member bound is the same 256, and its total-assignment bound is what
  keeps the file small.
- Draft [#951](https://github.com/ModernRelay/omnigraph/pull/951) and
  [#1210](https://github.com/ModernRelay/omnigraph/pull/1210) model actor
  views as rows in the graph. They limit what a permitted actor can see;
  this RFC decides who is a member. They compose; neither widens the
  other.

## Unresolved questions

- Whether the snapshot lifetime should be configurable above 300 seconds
  for deployments that prefer availability over revocation speed.
- Whether a role should be addressable as `issuer/slug` so one cluster can
  trust two providers. Today the binding names one issuer.
- Whether resource-scoped provider roles (a role held on one cluster, not
  the organization) should become a second fact source once the provider
  maps identity-provider groups to them and emits events for them.
- Whether `policy explain` over HTTP should accept `--roles` for the
  caller's own actor only, so a user can ask what a role change would do
  without a direct root.

## Decision log

- 2026-11-01: drafted. Replaces the membership half of the closed draft
  [#1218](https://github.com/ModernRelay/omnigraph/pull/1218) after
  design review of ModernRelay/og-control-plane#125, which found that the
  earlier draft bundled three features, filtered the snapshot by applied
  policy (creating an activation gap) and left overflow, freshness and
  outage behavior unspecified. Delegated ceilings and agent identities are
  out of scope and would be separate RFCs.
