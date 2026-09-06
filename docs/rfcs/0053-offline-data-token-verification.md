---
rfc: "0053"
title: "Offline data-token verification"
track: maintainer
status: accepted
implementation: complete
authors:
  - andrew
created: 2026-09-05
updated: 2026-09-05
discussion: https://github.com/ModernRelay/omnigraph/pull/633
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0053: Offline data-token verification

## Summary

Accept bounded, short-lived ES256 data credentials at the existing HTTP
boundary. An operator supplies public trust at boot; the server verifies every
credential offline, derives its actor from the authenticated principal, and
intersects its graph/action grants with the existing Cedar policy. An issuer
cannot use this credential to bypass graph policy or authorize schema changes.

The contract is generic OSS behavior. No managed-service library, network
discovery, token database, engine fork, or graph-storage format is introduced.

## Motivation

The server currently authenticates configured static bearer tokens. Managed
login establishes a different credential for the control service, which must
never be forwarded to graph endpoints. Short-lived data credentials let an
authorized caller query and mutate an existing cluster while serving remains
independent of the issuer's availability.

Identity, attenuation, expiry, trust custody, and root binding are public
security contracts. They require a decision before implementation rather than
an incidental alternative authentication branch.

## User and operational behavior

An operator starts the cluster server with public trust:

```sh
omnigraph-server --cluster s3://customer/root --data-token-trust trust.json
```

The flag is optional. Without it, existing static-token, policy, and explicit
unauthenticated behavior is unchanged. With it, protected routes require a
credential even when no static tokens are configured. Public health, readiness,
and OpenAPI endpoints retain their current behavior.

Malformed, expired, wrongly scoped, or untrusted credentials never become
anonymous requests. Authentication failures are 401; action/graph denials are
403, except existing resource-hiding conventions such as stored-query denials
remaining indistinguishable from missing queries. Denial occurs before graph
lookup or effect wherever that contract applies.

Token validity is checked at request admission. Expiry does not cancel an
already admitted mutation or stream. Revocation of a grant or login session
prevents future issuance; an issued token remains usable until its expiry or
the serving process restarts with trust that excludes its signing key. There
is no immediate online revocation promise.

## Design

### Signed credential

The credential is a compact JWT of at most 8,192 bytes. Its protected header is
exactly `alg: ES256`, `typ: JWT`, and `kid`, with no duplicate or unknown fields.
The signature uses JOSE's fixed-width P-256 encoding; issuers using a signer
that returns ASN.1 DER must convert it before serialization. `kid` is the
64-character lowercase hexadecimal SHA-256 of the signing public key's SPKI
DER representation.

The version 1 claims are a JSON object with these fields, without duplicates
or unknown fields:

```json
{
  "version": 1,
  "iss": "https://control.example",
  "aud": "urn:omnigraph:data:cluster-1",
  "sub": "principal-id",
  "account_id": "account-1",
  "cluster_id": "cluster-1",
  "cluster_incarnation": "incarnation-1",
  "principal_kind": "human",
  "assurance": "verified_human",
  "iat": 1788612000,
  "exp": 1788612900,
  "jti": "token-id",
  "grants": [{"graph_id": "knowledge", "actions": ["read", "change"]}]
}
```

`iss` is the canonical control API origin, at most 2,048 bytes, without a trailing slash, userinfo,
path, query, or fragment. HTTPS is required except exact loopback HTTP origins
for local qualification. `aud` is the exact string
`urn:omnigraph:data:<cluster_id>`, not an array. The account, cluster id, and
cluster incarnation must all equal the operator's boot trust.

`sub` is the issuer's immutable local principal id, never an email or a caller
selected actor. The server derives the Cedar actor as `principal:<sub>`.
The only valid kind/assurance pairs are `human`/`verified_human` and
`automation`/`verified_workload`. Development, self-asserted, and impersonated
credentials cannot use this profile. Principal/account/cluster/incarnation
strings and `jti` contain 1–128 ASCII letters, digits, hyphens, or underscores.
The latter is provenance,
not a persisted replay ledger: data credentials are reusable until expiry.

`iat` and `exp` are integer Unix seconds. Lifetime `exp - iat` is 60–86,400
seconds inclusive. Admission requires `now < exp`, with zero expiry leeway,
and `iat <= now + 30`. A maximum-lifetime credential issued by a clock 30
seconds ahead can therefore have 86,430 seconds remaining at admission.
Time arithmetic is checked. Other temporal claims,
including `nbf`, are unsupported in this fixed version and refuse rather than
being silently ignored. The issuer may issue a shorter token than the caller
requests according to its current permissions and configured limit.

`grants` contains 1–64 unique graph ids. Each id passes the server's existing
graph-id validation: 1–64 ASCII letters, digits, or hyphens, excluding
`policies`, `healthz`, `openapi`, and `graphs`. The managed issuer additionally
requires Core configuration validity, so it issues only for the intersection
of those grammars. Core graph names unsupported by existing server routing
remain unsupported; this increment does not widen routing. Each action set is nonempty,
contains no duplicate, and uses only these existing exact policy action names:
`read`, `export`, `change`, `branch_create`, `branch_delete`, `branch_merge`,
`invoke_query`, `graph_list`. Wildcards, `schema_apply`, `admin`, and unknown
actions refuse the whole token. There are no implicit action implications or
branch grants; Cedar retains branch and target-branch policy ownership.

### Public trust and root binding

The trust file is machine-written JSON, read once at boot, at most 64 KiB,
with no duplicate or unknown fields:

```json
{
  "version": 1,
  "canonical_root": "s3://customer/root",
  "account_id": "account-1",
  "cluster_id": "cluster-1",
  "cluster_incarnation": "incarnation-1",
  "issuer": "https://control.example",
  "audience": "urn:omnigraph:data:cluster-1",
  "keys": [{"kid": "<64 lowercase hex characters>", "public_key_pem": "<SPKI PEM>"}]
}
```

There are 1–4 unique keys. Each is a valid ECDSA P-256 public key and its
declared `kid` must match the SPKI digest. Identity and origin bounds match
the credential. An invalid file or any invalid key refuses startup; an
unknown request `kid` refuses without fetching anything.

`canonical_root` is nonempty and at most 4,096 bytes. Issuer and endpoint
origins are at most 2,048 bytes in addition to their containing object bounds.
The server compares `canonical_root` with the actual root selected by
`--cluster`. The Core exposes `ServingSnapshot.canonical_root` from the same
store and snapshot resolution; it does not reparse configuration or reread
mutable state for this binding. Local file roots are canonical absolute
paths encoded as file URIs. A mismatched root refuses before graph engines
open. The server does not read or reinterpret a private managed identity
marker. The authority supplying trust owns verification of that identity and
the account/cluster/incarnation binding before deployment. In the managed
realization the operator uses its validated `Cluster.status.identity`; the
mount is provisioning authority, not independent evidence of storage identity.

Trust is immutable for the process lifetime. Rotation installs both old and
new public keys before the issuer starts using the new key. Keep an old key
installed for at least 86,430 seconds after its final issuance; retiring it
requires another controlled restart. A control-plane outage does not cause a
server-side call or change already loaded trust. Its offline bound is each
credential's actual remaining lifetime, bounded by 86,430 seconds including
the permitted issuance-clock skew. Expiry still has zero leeway.

### Authorization and compatibility

The bearer boundary produces an authenticated principal and immutable grant
set. Graph routing narrows it to the selected graph before registry lookup.
The common policy gate checks the token action ceiling before the existing
Cedar decision. Signed credentials require an applied policy and explicit
permit; they never inherit the static-token no-policy read default. An allowed token action is insufficient
if Cedar denies it or has no corresponding actor. No policy entity is created
from a request or token. Policy bundles explicitly enroll `principal:<sub>`
through the ordinary cluster configuration and apply path.

| Existing route family | Required action checks |
|---|---|
| Query/read, snapshot, Blob GET/HEAD, schema/catalog, branch/commit/change-feed reads | `read` |
| Export and change-feed baseline | `export` |
| Mutate/change, conditional mutation, load/ingest | `change` |
| Stored query or mutation | `invoke_query`, then its existing inner `read` or `change` |
| Load creating a branch | Existing `branch_create` check, then `change` |
| Branch create/delete/merge | Corresponding `branch_*` action |
| Schema apply | Refused by this token profile |
| Graph registry | Server Cedar `graph_list`, plus per-result `graph_list` grant |

The graph registry filters both served and quarantined graph ids. A grant on
one graph never exposes another. Deprecated route aliases and conditional
variants use the same checks. Merge's optional source deletion keeps its
existing separate check: a successful merge can report a denied deletion
through `branch_deleted: false` without falsely failing the durable merge.

Explicit static break-glass credentials may coexist. The existing exact
constant-time hash match runs first; a configured static credential retains
its actor mapping, including when it contains dots. An unmatched credential
then goes to the signed-token verifier. Verification failure never re-enters
static matching or becomes anonymous. Static credentials retain existing
Cedar/default-deny semantics.

### Managed CLI data access

The companion issuance API supplies a data endpoint and signed credential.
`omnigraph cluster token --config DIR --graph ID --actions read,change --ttl 1h`
requests an explicit graph/action subset of the caller's current data grant.
TTL defaults to 3,600 seconds and must be 60–86,400 seconds; requested authority
never comes from control-plane admin or apply permission. `--clear` instead
forgets the selected cluster's local data credential. Token command output is
metadata only, never the credential.

The CLI stores the result in a separate OS-keychain service under canonical
API origin plus cluster id. The entry contains the endpoint, credential,
expiry, key id, and exact grants and is validated before use. It is separate
from the control-plane session, so ordinary data requests need no API call.
Control-plane logout remains unchanged; an issued data credential survives
until expiry or trust retirement, and local clearing is not server revocation.
Automation may use the explicit origin-bound control credential to mint into
the same keychain; unattended raw-token consumers use the issuance API.

With managed context in the exact current directory, `query` and `mutate`
require `--graph` and use this cached data endpoint/credential. Other data
verbs refuse as unsupported in this increment. Missing, malformed, expired,
or under-scoped credentials never fall through to static credentials, a
profile, or direct storage. Managed data access rejects explicit `--server`,
`--profile`, `--store`, `--cluster`, and `--as` and ignores inherited legacy
profiles and token settings. `--direct` becomes a global explicit override;
existing `cluster --direct` remains compatible. Without context, legacy data
commands keep their behavior. Managed requests refuse redirects and have a
10-second deadline and 8 MiB response bound.

## Invariants

Invariant 10 keeps actor identity at the authenticated server boundary and
retains every engine policy check. Invariant 11 bounds token parsing, trust,
key search, scope search, and time. Invariant 12 adds no authoritative token
database or mutable cache. Graph publication, accepted snapshots, recovery,
and direct-engine policy behavior are unchanged. No Lance-shaped behavior is
changed or relied on, and no Lance documentation domain applies to this HTTP
authentication work. No deny-list exception is requested.

## Compatibility and reversibility

Existing deployments without trust are unchanged. New credentials do not work
on old servers; they cannot degrade into static credentials. Removing the
trust flag disables signed credentials deliberately. The trust supplier must
not leave a token-only deployment in unauthenticated mode after rollback.
No graph storage, ledger, manifest, policy-file, or managed-marker format
changes. The public API gains authentication semantics, not a new graph route.

## Alternatives

- Online introspection or JWKS fetching adds a serving dependency and cannot
  preserve the required offline contract.
- Putting credentials in each graph's static-token store adds rotation and
  expiry state when signed, expiring capabilities already express the grant.
- Treating a signature as complete authorization bypasses Cedar and would
  grant actions or branches the graph owner never allowed.
- Importing private managed identity formats into the server couples OSS
  serving to a specific provisioning implementation; explicit boot trust
  keeps that authority outside the data plane.

## Evidence and tests

The existing `auth_policy`, `multi_graph`, `stored_queries`, `boot_settings`,
`data_routes`, and `openapi` owners plus server unit tests passed 359 checks.
The eight verifier cases cover the issuer's shared golden signature,
tampering, wrong algorithms/keys/bindings, duplicate and oversized fields,
invalid curves, key fingerprints, exact time boundaries, unsupported versions,
and kind/assurance mismatches. A focused Core regression verifies that directory
and storage-URI snapshots report the same canonical root and state CAS.

Protected-route tests prove the grant ceiling despite permissive Cedar policy,
Cedar denial despite a valid credential, actor-header spoofing failure,
stored-read/write double gating, cross-graph and registry isolation, and
unchanged static/unauthenticated behavior. Denied mutations leave the published
graph head unchanged. Boot coverage includes refusal before recovery opens a
graph, trust without static credentials, invalid trust, and overlapping keys.
The CLI passed 99 unit and 133 process tests; strict lint and documentation
checks passed for the implemented server and client surfaces.

The 2026-09-05 managed pilot used the server implementation at `463290a9` and
native CLI binary with SHA-256
`cb464cb72bb0d48ec885896e2361bee46b860b995c457cee9d33f9ca17eace0a`.
Generated evidence records real WorkOS session issuance through the API and a
per-cluster AWS KMS key, with a request for an ungranted export action refused.
The actual operator retained kubelet's final container exit on a Ready node,
completed its ordinary drain and apply, and verified that serving reported the
finalized receipt's ledger after trust and Cedar policy were installed.

Seven real-KMS verifier cases produced the expected results: a valid read
returned 200; expired, wrong-cluster, and wrong-incarnation credentials returned
401; missing read authority, another graph's grant, and an absent Cedar actor
returned 403. With the Intent API at zero Pods, the actual CLI inserted one
node and read it back at the same graph commit with the immutable principal
actor. Its 60-second credential then produced `data_credential_expired`, and
the API was restored. This separately exercises server expiry and client cache
expiry; no credential was included in the generated CLI output.

This qualification covers one existing cluster and one enrolled human
principal. It does not complete the companion control-plane G4 identity, auth,
and credential-transport gate or G5 managed-product gate, provide cell
infrastructure, or qualify backup/restore, distributed writer fencing, or the
full tenant adversarial matrix. Those boundaries remain outside this RFC.

## Rollout

Record this contract and the companion control-plane decision before code.
Implement and qualify the offline verifier, then deploy prepared trust and
policy to an existing cluster. Only then enable issuance and client access.
Keep a deliberate static break-glass path during the controlled transition.
User docs and release notes must describe the final flag, grant semantics,
expiry, and controlled key rotation. New engine or storage behavior is out of
scope, as are full cloud provisioning, online refresh, and dynamic discovery.

## Unresolved questions

None for the bounded wire and authorization contract. Implementation and
qualification remain separate from acceptance.

## Decision log

2026-09-05: Recorded the bounded offline verifier before implementation,
following the existing server policy/action audit. Managed root identity is
validated by the trust supplier; the OSS server binds that supplied trust to
the actual resolved serving root and does not import private marker formats.

2026-09-05: Accepted by the maintainer for implementation with the explicit
wire bounds, static-first exact credential matching, required Cedar permit,
and same-snapshot canonical root accessor. Companion control-plane DEC-08-23
owns issuance, permissions, KMS custody, and operator preparation. Qualification
remains required before claiming the implementation complete.

2026-09-05: Retained the existing server graph-id grammar and reserved names.
This replaces the proposed 512-byte graph-id bound; the grant-count bound
remains 64. Managed issuance uses the intersection with Core configuration
identifiers rather than changing the independent server routing contract.

2026-09-05: Completed the bounded server and CLI implementation after the
focused regressions and real WorkOS/KMS qualification above. The live pilot
used the ordinary managed drain, receipt, and readiness path, preserving
existing engine and storage behavior. The
later CLI diagnostic-redaction fix is covered by its focused regression and
does not change the successfully qualified data path.
