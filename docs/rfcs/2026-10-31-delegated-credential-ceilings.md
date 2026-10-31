---
rfc: "2026-10-31-delegated-credential-ceilings"
title: "Delegated credential ceilings"
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

# RFC: Delegated credential ceilings

## Summary

Add an optional, attenuation-only ceiling to native credentials. A request is
allowed only when applied policy permits the actor and the ceiling, when
present, covers the action. An absent ceiling keeps today's behavior. The
ceiling lets a user hand an MCP client, AI agent or third-party application
less than their own permissions. A delegated credential also names the client
acting for the user, so audit can tell the two apart.

## Motivation

[Identity credentials and applied policy authorization](2026-09-09-identity-credentials-and-applied-policy.md)
removed mandatory token grants because every ordinary credential duplicated
a second permission list. It notes that a token ceiling "is useful for
restricted delegation" and leaves delegated profiles disabled.

Delegation is now needed. The MCP surface exposes `mutate`, `load`, branch,
commit and blob write tools that run with the caller's full applied
permissions. A user who connects an agent to read a graph also lets it write,
merge, publish and drop commits wherever they personally can. OAuth scopes
exist to limit what a client may do on a user's behalf; nothing in the native
credential can express that today.

## User and operational behavior

- A credential without a ceiling behaves exactly as now.
- A credential with `ceiling: ["graph.read"]` can read and invoke stored
  reads on any graph its actor may read, and nothing else.
- `GET /graphs/{graph_id}/access` reports the intersection, and states that a
  ceiling narrowed the result.
- A refusal caused by the ceiling uses the existing permission-denied shape
  with a distinct reason, so a client can ask the user for more scope instead
  of reporting a policy gap.
- Graph discovery stays independent of policy and ceiling, as now.
- A change made through a delegated credential records the user as actor and
  the client as delegate.

## Design

### Vocabulary

A fixed, versioned set. Each scope names native actions:

| Scope | Actions |
|---|---|
| `graph.read` | `read`, `invoke_query` |
| `graph.export` | `export` |
| `graph.write` | `change`, `branch_create`, `branch_delete`, `branch_merge`, `commit_publish`, `commit_drop` |
| `graph.schema` | `schema_apply` |
| `graph.admin` | `admin` |
| `cluster.list` | `graph_list` |
| `cluster.config` | `config_manage` |

Scopes do not imply each other. A writer that must also read lists both.
Bulk `export` is separate from `graph.read` so a read-only agent cannot take
a full copy of a graph. The table covers every action in the current policy
vocabulary; a new action is uncovered by any ceiling until the vocabulary
version that names it.

### Signed credentials

Signed credentials gain version 3: version 2's claims plus an optional
`ceiling` array of at most 16 distinct vocabulary entries and an optional
`delegate` string of at most 256 bytes naming the client. A version-3
credential carrying either field lives at most 300 seconds. Version 2 stays
identity-only and unchanged; its strict parser still refuses a ceiling.
Unknown scopes, duplicates, an empty array or a version-3 credential at a
server that does not support it refuse the credential. Nothing is ignored.

### External OIDC tokens

The OIDC trust document may declare:

- `scope_prefix`, for example `omnigraph:`: token `scope` entries with this
  prefix are ceiling scopes, and an unknown prefixed entry refuses the token.
  Other entries such as `openid` are ignored.
- `default_ceiling`: applied when a token carries no prefixed entry. Absent
  means no ceiling, preserving today's behavior.

This lets a deployment give dynamically registered MCP clients, which cannot
hold per-client provider scopes, a read-only default.

A trusted token's `client_id` or `azp`, when present, is the delegate.

### Delegated OIDC tokens

The trust document may set `delegation: act`. A token with an `act` claim
(RFC 8693) then authorizes as `act.sub`, admitted through the same subject
snapshot as an ordinary token, and records the token's `sub` as the
delegate. Its ceiling comes from `scope` as above. Nested `act` claims, an
`act` without `sub`, or `act` at a trust document without `delegation`
refuse the token. A delegated token's `exp - iat` must be at most 300
seconds. Without `act`, the token's `sub` is the actor, so an autonomous
agent is its own principal and needs its own permits. This is the shape
WorkOS Agent Registration and Agent Auth issue.

### WorkOS example

A deployment using WorkOS defines permissions `omnigraph:graph.read` and
so on, grants them to agents through Agent Registration trust levels or
Agent Auth blueprints, and sets `scope_prefix: "omnigraph:"` and
`delegation: act`. A claimed agent's token then authorizes as the user named
in `act`, narrowed to the agent's granted permissions, and records the agent
as delegate. Other providers that issue RFC 8693 `act` claims use the same
fields.

### Audit

Commit, branch and configuration audit records gain an optional `delegate`
beside the actor. A delegate is attribution only: it is never matched by
policy and never authorizes.

### Evaluation

The ceiling is checked after authentication and beside the Cedar decision on
every HTTP route, MCP tool and engine `_as` entry point that receives the
actor. Both must allow. A candidate configuration is still authorized by the
applied policy, and the ceiling must include `cluster.config` (and
`graph.schema` for schema effects) for an identity-authorized apply.

## Invariants

[Invariant 10](../dev/invariants.md) is extended, not weakened: HTTP still
resolves the actor from a verified token, and the client never supplies it. A
delegate is likewise read only from the verified credential. The ceiling is
checked at the same engine `_as` gate as policy, so embedded and CLI-direct
writers holding a ceilinged credential cannot bypass it. Invariant 11 holds
because the new fields have fixed bounds and refusals are typed. No deny-list
shape is introduced: the ceiling is a property of one credential, not a
second permission store.

- A ceiling never widens access; applied policy remains the only grant.
- Absent ceiling equals current behavior; no ordinary credential needs a
  second permission list.
- Unknown or malformed ceiling input fails closed.
- Actor identity is unchanged by a ceiling or delegate; a delegate is
  recorded, never authorized.
- A delegated credential lives at most 300 seconds.

## Compatibility and reversibility

Existing version-2 credentials and OIDC trust documents without the new
fields are unaffected. Issuers adopt version 3 per client class. Removing
support means refusing version 3 again; no stored state changes.

## Alternatives

- Reinstate mandatory grants: brings back the duplicated permission list the
  earlier RFC removed.
- Map provider roles to ceilings: conflates who a person is with what a client
  may do for them.
- Restrict only in the MCP layer: misses HTTP and engine paths and makes MCP a
  second authorization system, which [RFC 0003](0003-mcp-server-surface.md)
  rules out.
- Per-tool allow lists: unstable as tools change; actions are the stable
  authorization vocabulary.

## Evidence and tests

- Parser: version 2 with a ceiling refuses; version 3 with none, valid, empty,
  duplicate and unknown entries.
- Evaluation matrix: every action against every scope, with policy allowing
  and denying.
- Every MCP write tool refuses under `graph.read`; reads still succeed.
- Identity-authorized apply requires `cluster.config` when a ceiling is
  present.
- OIDC: prefix parsing, unknown prefixed scope, default ceiling, no default.
- Delegation: `act` maps the actor and delegate; nested, `sub`-less or
  unexpected `act` refuses; lifetime over 300 seconds refuses; a token
  without `act` authorizes as its own `sub`.
- Audit records carry the delegate and policy ignores it.
- Access route reports the intersection and the narrowing.

## Rollout

Ship parsing and evaluation first. Issuers emit version 3 only for client
classes they choose; deployments set an OIDC `default_ceiling` only after
qualifying their MCP clients' consent and scope flows.

## Unresolved questions

- Whether scopes should be graph-qualified (for example
  `graph.read:knowledge`) in a later version.
- Whether `graph.write` should split merge and publish from ordinary changes.
- Whether policy should later be able to name delegates, for example to
  deny a specific client.

## Decision log

- 2026-10-31: drafted. Delegation through `act`, delegate attribution and
  the 300-second bound for delegated credentials added after reviewing WorkOS
  Agent Registration and Agent Auth token shapes.
