---
rfc: "2026-10-31-delegated-credential-ceilings"
title: "Delegated credential ceilings"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-31
updated: 2026-11-01
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

### How a client acts for a user today

A request carries exactly one identity. A native signed credential is
version 2 and identity-only: it names `principal:<id>` and nothing about what
that principal may do. An OIDC token admitted through `--oidc-identity-trust`
maps to the same kind of principal. Applied Cedar policy then decides every
action for that principal. There is no second input to the decision.

That design came from
[Identity credentials and applied policy authorization](2026-09-09-identity-credentials-and-applied-policy.md).
Version-1 credentials carried mandatory grants, so every ordinary credential
duplicated a permission list that policy changes could not widen. The RFC
removed them and kept two notes for later: an explicit ceiling "is useful for
restricted delegation", and "unqualified machine or delegated profiles remain
disabled". Today the OIDC profile refuses OAuth-client tokens and delegated
or impersonated credentials outright.

### What goes wrong

MCP made delegation the ordinary case. The `/mcp` endpoint runs every tool
through the same handler and policy action as its HTTP route, and the tool
list includes `mutate`, `load`, branch create, delete and merge, commit
publish and drop, and `blobs_clear`. The same token also works on every HTTP
route, including `POST /cluster/deployments`. When a person connects Claude, an IDE
agent or a scheduled assistant to a graph, the client signs in as that
person, so it receives their complete applied permissions:

- An analyst who can write in production connects an agent to answer
  questions. The agent can also merge branches and publish commits.
- An administrator with `config_manage` tries a third-party MCP client. That
  client's token can now submit cluster deployments, including policy
  changes.
- A prompt injection in data the agent reads can steer it to any write the
  person could make.

The [MCP guide](../user/operations/mcp.md) says to restrict an agent by
narrowing "the policy for the agent's credential". That works only when the
agent has its own principal. A client signed in as a person has no separate
principal to narrow. Removing the person's write access to protect them from
their agent also removes it from the person.

Two partial guards exist, and neither is an authorization boundary:

- `omnigraph mcp` without `--write` lists and admits only tools annotated
  `readOnlyHint`. The bridge enforces this on the client side; a client
  that calls `/mcp` directly bypasses it.
- MCP hosts let a user approve tools one by one. That is host behavior the
  server cannot see or rely on.

### Attribution

Commit and audit records name the actor. When an agent writes as a person,
the record cannot tell the person's own change from the agent's. Reviewers,
incident responders and regulated customers need both: who was authorized
and which client acted.

### Why now

Identity providers now issue the delegation shape directly. RFC 8693 defines
the `act` claim for "this token's subject acts for that party". WorkOS Agent
Registration (third-party agents) and Agent Auth (first-party agents) issue
short-lived tokens in which `sub` names the agent, `act.sub` names the user,
and `scope` names the permissions granted to that agent. Other OAuth
providers express client delegation through `scope`, and the MCP
authorization specification relies on OAuth scopes for consent. Without
native support, a deployment must either refuse these tokens, as today, or
treat the agent as the user and give it everything.

### Goals and non-goals

Goals:

- A user can give a client less than their own access, enforced by the
  server on every path.
- Applied policy stays the only source of grants. A ceiling only narrows.
- Credentials without a ceiling behave exactly as today, so ordinary users
  keep one permission list.
- Audit names the client acting for the user.
- Standard provider tokens, including RFC 8693 `act` tokens, work without a
  provider-specific code path.

Non-goals:

- Data-level narrowing (rows or columns). The scope here is actions.
- Policy rules that name or match a client.
- Per-graph or per-branch scopes. They are an unresolved question.
- Online token introspection. Verification stays offline and bounded.

## User and operational behavior

- A credential without a ceiling behaves exactly as now.
- A credential with `ceiling: ["graph.read"]` can read and invoke stored
  reads on any graph its actor may read, and nothing else.
- `GET /graphs/{graph_id}/access` reports the intersection, and states that a
  ceiling narrowed the result.
- A refusal caused by the ceiling answers 403 with code `ceiling_exceeded`
  in the ordinary error body, and a typed detail naming the scope the action
  needs. A policy denial keeps 403 `forbidden`, so a client can ask the user
  for more scope instead of reporting a policy gap. When OIDC is configured,
  the response also carries `WWW-Authenticate: Bearer
  error="insufficient_scope"` with that scope (RFC 6750), which MCP clients
  use to ask for consent to a wider scope.
- `/mcp` keeps listing every tool, as [RFC 0003](0003-mcp-server-surface.md)
  requires; a call outside the ceiling refuses.
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
version that names it. The open `maintain` and `reclaim` actions
([#1175](https://github.com/ModernRelay/omnigraph/pull/1175))
would get `graph.maintain` and `graph.reclaim` in the version that follows
their release.

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

## Related RFCs

- [Identity credentials and applied policy authorization](2026-09-09-identity-credentials-and-applied-policy.md):
  this RFC is the restricted delegation that RFC deferred. It keeps version 2
  identity-only and enables the disabled delegated OIDC profile only through
  `delegation: act`. On acceptance, that RFC gets a disposition note naming
  the sentence that delegated profiles remain disabled.
- [RFC 0003](0003-mcp-server-surface.md): says an operator who wants an
  agent narrower than a person "grants the agent's credential less". That
  holds for agents with their own principal; a ceiling covers agents signed
  in as a person. On acceptance, RFC 0003 gets a disposition note. Its rule
  that every tool is listed and visibility is not permission is unchanged.
- [HTTP API conventions](2026-10-19-http-api-conventions.md): the refusal
  uses its single error body; `ceiling_exceeded` is a new code beside
  `forbidden`.
- [Operator profiles and credential providers](2026-10-20-operator-connections-and-credential-providers.md):
  the managed exchange keeps issuing identity credentials without a ceiling.
  A profile that wants a delegated credential is a later extension.
- Open drafts on finer authority, which narrow data rather than actions:
  - [#1210](https://github.com/ModernRelay/omnigraph/pull/1210) (views and
    reach) also defines delegated credentials, with the agent as actor, the
    person as subject, Cedar checked for both, and a data `over`. That
    conflicts with this RFC's identity rule (the person is the actor and the
    agent a delegate that policy never matches). One must be chosen before
    either is accepted; the action ceiling and a data `over` can share one
    credential.
  - [#951](https://github.com/ModernRelay/omnigraph/pull/951) (actor views)
    and [#1197](https://github.com/ModernRelay/omnigraph/pull/1197) (query
    contracts and Cedar grants) restrict what an actor's queries can see or
    run. A ceiling composes with either: it is checked before their filters
    and never widens them.

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
- Which identity rule wins against #1210: the person as actor with the agent
  as delegate, or the agent as actor with the person as subject.

## Decision log

- 2026-11-01: related RFCs reviewed: refusal code aligned with the HTTP
  conventions, maintenance actions noted, the identity conflict with #1210
  recorded as unresolved.
- 2026-10-31: drafted. Delegation through `act`, delegate attribution and
  the 300-second bound for delegated credentials added after reviewing WorkOS
  Agent Registration and Agent Auth token shapes.
