---
rfc: "0064"
title: "Identity credentials and applied policy authorization"
track: maintainer
status: draft
implementation: in-progress
authors:
  - andrew
created: 2026-09-09
updated: 2026-09-09
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0064: Identity credentials and applied policy authorization

## Summary

Add an identity-only version 2 signed credential alongside the restricted
version 1 profile in [RFC 0053](0053-offline-data-token-verification.md).
The credential authenticates a principal for one cluster; the cluster's
applied policy configuration defines graph and schema permissions.
Authenticated users can discover every graph's existence without obtaining
permission to read its data or schema.

Add an identity-authorized cluster planning/apply API that checks the same
policy engine against the current applied configuration before effects.
Existing direct storage-holder APIs and version 1 restrictions remain intact.
This proposal and its implementation are for review; neither is accepted by
being present in an unmerged pull request.

## Motivation

An explicit token ceiling is useful for restricted delegation, but requiring
every ordinary credential to duplicate graph/action grants creates another
permission list to maintain. Policy changes cannot grant an action missing
from that token, even when the same principal is already authenticated.
The version 1 action vocabulary also categorically excludes schema changes.

Removing that ceiling alone is insufficient. Schema changes through cluster
apply must authorize their initiating principal without bypassing declarative
ownership. A candidate policy must not authorize its own installation. Graph
discovery must expose names without exposing the richer operational metadata
returned by the existing graph catalog.

## User and operational behavior

Normal credential acquisition requests version 2 without graph/actions:

```text
omnigraph cluster token --ttl 3600
omnigraph --graph knowledge query list_people
omnigraph graphs list
```

Explicit legacy `--actions` requests retain version 1 restrictions. Existing
cached restricted credentials never become identity-only credentials through
renewal, permission denial, or omission of a field. Unsupported profiles
refuse without switching credentials or storage addressing.

`GET /graphs/discovery` returns only
`{"graphs":[{"graph_id":"knowledge","display_name":"knowledge"}]}`
for a valid version 2 identity. It uses the effective inventory, including
unavailable graphs. No policy group or `graph_list` permit is required. Graph
data, schema, query bodies, roots, diagnostics and topology are absent.
The existing `/graphs` response and authorization retain their contract.
Version 1 credentials cannot use discovery to evade their existing filters.

Protected operations still require applied policy. Missing policy or missing
principal membership denies them. An activated policy change governs the
next request with the same valid token; editing files alone does not change
permissions. Expiry does not cancel an already admitted request.

## Design

### Credential profiles

Version 2 retains the exact signature, key, issuer, audience, principal,
account, cluster/incarnation, assurance and temporal checks of version 1.
Its claims contain `version: 2`, `iss`, `aud`, `sub`, `account_id`,
`cluster_id`, `cluster_incarnation`, `principal_kind`, `assurance`, `iat`,
`exp`, and `jti`. It contains no `grants`, roles or policy membership.
Unknown or duplicate fields refuse; removing `grants` from a version 1
credential does not change its profile. The verifier privately represents
the profiles distinctly and preserves public version 1 construction APIs.

The token remains bounded to 8,192 bytes, lifetime 60–86,400 seconds, default
3,600 seconds, no expiry leeway and at most 30 seconds future issue time.
Public trust and canonical-root binding retain RFC 0053's bounds. Verification
and policy evaluation have no synchronous issuer dependency. `principal:<sub>`
remains the immutable authenticated actor; caller overrides cannot replace it.

Credential acquisition explicitly requests `version: 2`; an omitted version
retains the version 1 request contract. Version 2 responses explicitly report
their version, identity, endpoint and expiry metadata without a grant list.
The client checks the response profile and keeps separate versioned cache
metadata. The issuer must check current identity and exact cluster admission;
it must not exchange a restricted credential for a broader one. Issuer policy
enrollment and lifecycle remain outside this library's trust boundary.

### Schema and configuration authorization

Remote schema reads use `read`; schema application uses `schema_apply` on
the affected graph and main branch. Add cluster-scoped `config_manage` for
configuration administration, including policy/group changes and creation of
a graph with its initial schema and policy. The existing graph-scoped `admin`
action is not silently reinterpreted as this new cluster permission.

Identity-authorized planning binds the trusted principal and checks protected
schema reads. Its authorization evidence binds the accepted base revision and
CAS, desired configuration digest, policy digests, required actions and exact
resource effects. Identity-authorized apply revalidates that base and the
whole effect set under the existing cluster lock before any graph or
configuration effect. Missing policy, denied scope, changed base or uncertain
recovery authority refuses without an authorized subset being applied.

The current applied policy is read from its accepted catalog references.
Policy bytes from the proposed directory never authorize that proposal.
Installing a policy and exercising a newly added permit therefore requires
two separately authorized changes. Successful preflight does not bypass
schema sidecars, cluster state CAS, recovery ownership or writer exclusion.
The server continues to refuse direct schema application to cluster-owned
graphs; those schemas change through configuration and cluster apply.

First initialization needs explicit trusted bootstrap authority bound to
the initial desired configuration and effect set. The Core also verifies
the pristine imported base. Missing policy on an initialized cluster,
including one with zero graphs, never reopens bootstrap. Adding a graph
later requires the previously applied cluster management policy.

Stored plan and history details containing protected schema information are
not transferable read grants. Consumers must authorize the current requesting
principal against current applied policy before exposing those details.
An authenticated execution result may support a non-sensitive summary without
exposing schema contents. Source-file possession retains its separate contract.

## Invariants

This extends [invariants](../dev/invariants.md) 3, 8, 10, 11, 12 and 13.
One accepted state supplies the authorization basis; stale or missing authority
fails closed. Shared policy code owns decisions, and a credential or external
permission mirror cannot override it. All checks precede graph effects.
No new graph/storage format, publication door, writer fence, background queue
or Lance behavior is introduced. Direct storage possession remains its
documented trust boundary, never an automatic fallback from identity denial.

## Compatibility and reversibility

Version 1 tokens retain their ceilings, filtered catalog and schema exclusion.
Existing static/unauthenticated modes, direct APIs and public version 1 data
types retain their behavior. New authorized entry points are additive. Policy
configuration using `config_manage` requires a supporting binary; old binaries
refuse unknown actions rather than granting permission.

Existing clusters must explicitly install a reviewed management policy and
intended memberships through their existing authorized migration path before
activating the new gate. Do not derive permissions from legacy token grants.
Downgrade requires stopping version 2 issuance, allowing existing credentials
to expire, and explicitly reverting unsupported policy configuration.

## Alternatives

Keep mandatory token grants: preserves attenuation but leaves two ordinary
permission lists and the categorical schema exclusion. Silently ignore legacy
grants: widens credentials and breaks delegation. Hide unauthorized graphs:
prevents basic discovery and confuses absence with lack of data access.
Let a proposed policy authorize apply: lets a caller grant itself permission.
Teach each caller to evaluate policy: duplicates semantics and permits drift.

## Evidence and tests

Extend the server token/auth-policy and catalog suites, CLI token/cache and
dispatch suites, policy tests, and cluster plan/apply tests. Required cases:

- Version 1 remains restricted and unchanged; malformed cross-profile claims
  and invalid signature/time/root/identity fail.
- An authenticated unenrolled principal sees every graph ID/name, including
  an unavailable graph, and cannot read data/schema or private catalog fields.
- Allowed and denied schema/config effects follow the applied policy; a
  candidate self-grant, wrong actor or stale base fails before effects.
- A policy activation changes permissions for the same identity credential.
- Bootstrap cannot be repeated on an initialized or policy-free cluster.
- Old CLI restriction flags, cache entries and automation exchanges cannot
  silently gain authority; explicit addressing stays compatible.
- OpenAPI accurately describes the additive discovery response.

The implementation changes authentication and authorization around existing
operations, not Lance semantics. No new substrate behavior is assumed.

## Rollout

1. Review this contract and qualify dual-profile verification, discovery,
   shared policy enforcement and client compatibility.
2. Apply reviewed policy/principal configuration to existing clusters through
   their existing authorized path; verify management access before activation.
3. Activate the new execution boundary and compatible server/client versions,
   then enable version 2 issuance explicitly.
4. Migrate restricted workloads to dedicated policy-configured identities
   only through an explicit change. Keep the legacy path during migration.
5. Retire version 1 only after all issuers stop producing it and at least
   86,430 seconds pass, with old client migration accounted for.

## Unresolved questions

No alternative authority model is left open. Acceptance requires review of
the additive public APIs and the evidence above; an implementation PR remains
unmerged until that review is complete.

## Decision log

- 2026-09-09: Proposed the separate identity profile, authenticated minimal
  discovery, and authorization against applied policy, retaining legacy
  restrictions and direct storage-holder behavior.
