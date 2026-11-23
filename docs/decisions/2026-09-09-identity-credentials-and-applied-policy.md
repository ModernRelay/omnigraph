# Identity credentials and applied policy

**Decides:** a signed data credential authenticates a principal for one cluster and carries no permissions; the cluster's applied policy alone decides what that principal may do.
**Contract:** keeps E6
**Code:** `crates/omnigraph-server/src/data_tokens.rs`, `crates/omnigraph-server/src/handlers.rs`, `crates/omnigraph-cluster/src/authorization.rs`, `crates/omnigraph-cluster/src/lib.rs`, `crates/omnigraph-policy/src/lib.rs`

## Context

A credential that lists its own grants is a second permission list beside the
policy. A policy change cannot give a principal an action its token lacks, and
the token vocabulary excluded schema changes outright. Removing the grants is
not enough on its own: schema changes through cluster apply must be authorized
for the caller without letting the proposed configuration authorize itself,
and callers need to learn which graphs exist without gaining access to their
data or schema.

## Decision

- Native signed credentials are version 2 only. The claims are exactly
  `version`, `iss`, `aud`, `sub`, `account_id`, `cluster_id`,
  `cluster_incarnation`, `principal_kind`, `assurance`, `iat`, `exp` and
  `jti`. Grants, roles, unknown fields and other versions refuse the whole
  token; removing grants from a version-1 token does not make it valid.
- The actor is `principal:<sub>`. Every protected operation is checked
  against the applied Cedar policy for that actor. A policy activated after
  admission governs the next request made with the same token.
- `GET /graphs/discovery` returns each graph's id and display name, the
  unavailable ones included, to any admitted identity. It needs no policy
  grant and returns no data, schema, roots or diagnostics. `/graphs` keeps
  its own authorization.
- Schema reads need `read`; schema application needs `schema_apply` on the
  graph's main. Configuration administration, including policy and group
  changes and adding a graph, is the cluster-scoped `config_manage`. The
  graph-scoped `admin` action is not reinterpreted as it.
- Identity-authorized planning and apply read policy from the applied
  ledger. Policy files in the proposed configuration never authorize that
  proposal, so installing a permit and using it are two applies.
- Apply revalidates the base, the policy and the complete effect set under
  the cluster lock before any effect. A denial applies no subset.
- First initialization uses explicit bootstrap authority bound to the
  initial configuration.
- Offline verification of the credential is
  [Offline data-token verification](0053-offline-data-token-verification.md).

## Rejected

- **Mandatory grants in the token:** keeps two ordinary permission lists and
  the categorical exclusion of schema changes.
- **Ignoring grants found in old tokens:** silently widens restricted
  credentials and breaks delegation.
- **Hiding graphs a caller cannot read:** blocks basic discovery and makes
  absence indistinguishable from lack of access.
- **Letting the proposed policy authorize its own apply:** a caller could
  grant itself the permission it needs.
- **Each caller evaluating policy itself:** duplicates the semantics, which
  then drift.
