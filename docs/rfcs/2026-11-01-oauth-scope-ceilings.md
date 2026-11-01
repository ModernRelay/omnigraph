---
rfc: "2026-11-01-oauth-scope-ceilings"
title: "OAuth scope ceilings"
track: public
status: draft
implementation: not-started
authors:
  - Ragnor Comerford
created: 2026-11-01
updated: 2026-11-01
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: OAuth scope ceilings

## Summary

Let an admitted OIDC access token carry an attenuation-only ceiling, read
from its standard `scope` claim. A request through such a token succeeds only
when applied policy permits the actor **and** the ceiling covers the action.
Nothing else changes: the actor is still the person's principal, applied
policy is still the only grant, a token without a ceiling behaves exactly as
today, and native signed credentials are untouched.

The vocabulary is fixed in the engine: six scope names, each a set of
existing policy actions. An OIDC trust document opts in by declaring the
scope prefix its issuer uses; without that declaration the `scope` claim stays
ignored, as it is now.

## Motivation

### What happens today

An OIDC access token admits a person and nothing more. The server reads
`sub`, `aud`, `iss`, `org_id`, `client_id`, `iat` and `exp`; it refuses
`act`, `impersonator` and any `sub_profile` other than `user`; and it never
reads `scope`, `scp` or `azp` (`crates/omnigraph-server/src/oidc_identity.rs`,
`accepted_claims`). Every request then authorizes as `principal:<id>` against
applied policy. The `client_id` is kept beside the actor, logged with each
policy decision and reported by `GET /graphs/{graph_id}/access`, but it never
reaches Cedar and never narrows anything.

So every client a person connects acts with that person's full applied
permissions. The MCP surface (RFC 0003) makes this concrete: `/mcp`
authenticates through the same bearer path, and when a server enables
`--mcp-allow-writes`, the `mutate`, `load`, `branches_*` and `commits_*`
tools reach the same `_as` entry points as HTTP writes. A person who connects
an agent to read a graph also lets it change, merge, publish and drop commits
wherever that person can. RFC 0003 names the intended remedy: "An operator
who wants an agent narrower than a person grants the agent's credential
less." Today there is no way to grant a credential less than its actor.

### What the identity RFC decided, and did not decide

[Identity credentials and applied policy authorization](2026-09-09-identity-credentials-and-applied-policy.md)
removed mandatory token grants because every ordinary credential duplicated
a second permission list that policy changes could not widen. It kept the
door open in the same paragraph: "An explicit token ceiling is useful for
restricted delegation." Its alternatives section rejected only the two
extremes, mandatory grants and silently ignored grants. An *optional*
ceiling that only narrows is neither.

That RFC also left "unqualified machine or delegated profiles" disabled. A
scoped token is not a delegated profile: its `sub` is still the person, the
actor is still that person's principal, and `act` still refuses. The person
simply handed one client a credential that can do less than they can.

### Why OAuth scopes are the right carrier

OAuth scopes exist for exactly this: a client requests less than the user's
authority, the authorization server shows the user what was requested, and
the issued token says what was granted. The pieces are already in place:

- WorkOS Connect user tokens carry `scope` ("granted scopes", space-delimited)
  and `client_id` on every token issued through the authorization-code,
  refresh and device-code grants, to pre-registered applications and to
  clients registered through Dynamic Client Registration (DCR) or Client ID
  Metadata Documents (CIMD) alike. The consent screen reappears when "the
  application requests new scopes", and existing consent covers a request for
  "the same as, or a subset of, scopes previously granted"
  ([Access token claims](https://workos.com/docs/authkit/connect/token-claims),
  [Authorize](https://workos.com/docs/reference/workos-connect/authorize)).
- The server already serves RFC 9728 protected-resource metadata and an RFC
  6750 `WWW-Authenticate` challenge on `/mcp` when OIDC trust is configured.
  Both have standard places for scopes (`scopes_supported`, and the
  `insufficient_scope` error with a `scope` parameter) that clients already
  understand.
- The Intent API already parses and intersects a `scope` claim for M2M
  tokens, with exact bounds (256 entries, 128 bytes, the RFC 6749 scope-token
  alphabet). This RFC reuses those bounds unchanged.

### What this RFC is not

It is not membership, roles, agents or delegation.

- It does not read provider roles or permissions. WorkOS says plainly that
  "scopes do not enforce the user's role-based permissions" and that Connect
  tokens "do not include a permissions claim". Here, who may do what is
  decided by applied policy alone, exactly as today.
- It does not create agent identities, accept `act`, or change `sub_profile`
  handling. Autonomous automation stays an M2M principal with its own policy
  entries. Delegated, branch-scoped credentials remain
  [#978](https://github.com/ModernRelay/omnigraph/issues/978).
- It does not add a ceiling to native signed credentials (version 2 stays
  identity-only), and it does not record the client on commits
  ([#1173](https://github.com/ModernRelay/omnigraph/issues/1173)).
- It does not make WorkOS a second permission source. A scope can only remove
  an action the policy would have allowed; it can never add one.

## Vocabulary

| Term | Meaning |
|---|---|
| Scope | One entry of a token's `scope` claim, as RFC 6749 §3.3 defines it. |
| Scope prefix | The string an OIDC trust document declares; a scope that starts with it is a ceiling scope, every other scope is ignored. |
| Ceiling | The set of policy actions a token's ceiling scopes name. A token without ceiling scopes, at a trust document without a prefix, has no ceiling. |
| Ceiling scope vocabulary | The fixed table below, versioned with the server. |

The existing `Scope` enum in `crates/omnigraph-server/src/identity.rs`
(`Full` or `IdentityToken`) is a credential class, not an OAuth scope; the
implementation renames or namespaces it so the two never share a word.

## User and operational behavior

### The vocabulary

| Scope name | Policy actions |
|---|---|
| `graph.read` | `read`, `invoke_query`, `export` |
| `graph.write` | `change`, `branch_create`, `branch_delete`, `branch_merge`, `commit_publish`, `commit_drop` |
| `graph.schema` | `schema_apply` |
| `graph.admin` | `admin` |
| `cluster.list` | `graph_list` |
| `cluster.config` | `config_manage` |

Scopes do not imply one another: a writer that also reads holds both. The
table covers every one of the thirteen current policy actions.

`export` sits inside `graph.read` deliberately. The `read` action already
covers "queries, snapshots, branches, and commits", so a client holding
`graph.read` can run any query over HTTP and see everything its user can;
snapshot export is a resumable bulk transport for the same data, not a
confidentiality boundary. A scope that withheld it would protect only
convenience. Policy keeps `export` as its own action for people, where
denying bulk export while allowing queries is a reasonable operator choice; a
client ceiling needs no such distinction. A policy action added later belongs to no scope until the vocabulary
version that names it, so a ceilinged token can never perform it. This is the
fail-closed direction.

Graph discovery (`GET /graphs` and the `graphs` tool for an identity
credential) names graphs by ID and display name and checks no policy action
today; a ceiling does not change it.

### Trust document

The OIDC trust document (the `--oidc-identity-trust` file) gains one optional
field:

```json
{
  "version": 1,
  "issuer": "https://your-environment.authkit.app",
  "audience": "https://g-01jd8y-01je4k.graphs.example.com/mcp",
  "scope_prefix": "omnigraph:",
  "...": "revision, dates, binding, keys and principals as today"
}
```

`scope_prefix` is 1 to 64 bytes of the RFC 6749 scope-token alphabet
(`%x21 / %x23-5B / %x5D-7E`). It is part of the frozen binding: the first
validated file fixes it, and a later file that changes, adds or removes it is
refused like any other binding change. Changing the prefix means restarting
the server, which keeps "which tokens carry a ceiling" a boot-time fact.

The document still contains no actions, groups or grants. The prefix is a
parsing instruction, not a permission.

### Token handling

At a trust document **without** `scope_prefix`, `scope` stays ignored. Nothing
in this RFC applies.

At a trust document **with** `scope_prefix`, admission reads the `scope`
claim:

- Absent `scope`, or an empty string, means the token was granted no scopes:
  the ceiling is empty. The token is still admitted; every policy action is
  then refused with `insufficient_scope` (below). This matches WorkOS, which
  "omits scope when no scopes were granted".
- `scope` must be a string. It is split on single spaces into at most 256
  entries, each 1 to 128 bytes of the scope-token alphabet, with no
  duplicates. Anything else refuses the token with 401 `invalid_token`.
- An entry that starts with the prefix must be the prefix followed by a
  vocabulary name. An unknown name refuses the token with 401 `invalid_token`:
  the authorization server and this server disagree about the vocabulary, and
  guessing is how a ceiling widens.
- Every entry that does not start with the prefix (`openid`, `profile`,
  `email`, `offline_access`, a control-plane service scope) is ignored.
- The ceiling is the union of the actions of the named scopes.

Nothing about the actor changes: `sub` still maps through the admitted
principals to `principal:<id>`, and `client_id` is still kept beside it.

### Evaluation

Every authorization decision over HTTP and MCP already passes through one
function, `handlers::authorize`, which holds the full `AuthenticatedActor`.
The ceiling is checked there, after the policy decision:

1. Applied policy decides as today. A denial is 403 `forbidden`, unchanged.
2. If policy allows and the actor's token has a ceiling, the requested policy
   action must be in it. Otherwise the request is refused with 403
   `insufficient_scope`.

Policy goes first so `insufficient_scope` is only ever returned for an action
the person could perform with a wider scope. A client that receives it may ask
the user for that scope and will then succeed; a client that receives
`forbidden` must stop, as UIS-style client guidance already requires. The
response reveals to the client only that its own user holds a permission,
which the user can read from `/access` anyway.

The engine's second gate (`Omnigraph::enforce`, the `PolicyChecker` trait) and
the cluster path (`AppliedPolicies::check_cluster` / `check_graph`) see only an
actor string and stay as they are. A ceiling is a property of the credential,
and the credential exists only at the HTTP layer; it is checked once, where it
exists, before any handler runs. The two routes that build an
`IdentityAuthorization` from an OIDC actor (`POST /cluster/plan` and
`POST /cluster/deployments`) check the ceiling for `config_manage` through the
same function before constructing it.

A request that performs several actions (a `mutate` with branch statements, a
`load` with `from`, a stored mutation's `invoke_query` then `change`) checks
each action as it does today; the first action outside the ceiling refuses
the request before any effect, exactly where a policy denial would.

### Refusal shape

`insufficient_scope` is a new `ErrorCode` with status 403, serialized as the
HTTP conventions RFC requires:

```json
{
  "error": "the credential's scope does not cover change",
  "code": "insufficient_scope",
  "scope": "omnigraph:graph.write"
}
```

`scope` is the typed detail: the one prefixed scope that would have covered
the refused action. The response also carries

```
WWW-Authenticate: Bearer error="insufficient_scope", scope="omnigraph:graph.write", resource_metadata="<url>"
```

as RFC 6750 §3.1 specifies for this error. RFC 0003 currently says a 403
carries no challenge; this RFC amends that for `insufficient_scope` only.
`forbidden` and `not_admitted` keep no challenge, so a client still cannot be
driven to re-authenticate for a permission its user lacks.

MCP tool refusals carry the same `error`, `code` and `scope` in their
structured content, as every handler refusal does.

### Discovery

When the trust document declares a prefix, the protected-resource metadata
adds the RFC 9728 field:

```json
{
  "resource": "https://g-01jd8y-01je4k.graphs.example.com/mcp",
  "authorization_servers": ["https://your-environment.authkit.app"],
  "bearer_methods_supported": ["header"],
  "scopes_supported": [
    "omnigraph:graph.read", "omnigraph:graph.write", "omnigraph:graph.schema",
    "omnigraph:graph.admin", "omnigraph:cluster.list", "omnigraph:cluster.config"
  ]
}
```

Without a prefix the field is absent and the body is byte-identical to
today's.

### Access report

`GET /graphs/{graph_id}/access` and the `access` tool report the intersection.
`AccessOutput` gains one optional field, `scopes`, present only when the
credential has a ceiling:

```json
{
  "actor_id": "principal:01JE4K9M1Q",
  "client_id": "client_01JP8BD0CZ401TDF9X54NT5ZEK",
  "graph_id": "knowledge",
  "scopes": ["omnigraph:graph.read"],
  "graph_wide": ["read", "invoke_query"],
  "branch": null,
  "target_branch": null,
  "actions": ["read", "invoke_query"]
}
```

`graph_wide` and `actions` list what this credential can do here: policy
permits intersected with the ceiling. An empty `scopes` array is a token
granted no ceiling scopes; an absent field is a token without a ceiling.

### What a deployment must know about its authorization server

The server consumes scopes; it does not decide which client gets which. That
is the authorization server's job, and with WorkOS Connect the facts are:

- An OAuth application created in the dashboard has scopes assigned from the
  environment's permission catalog. A client requesting an unassigned scope
  fails at the authorization endpoint with `invalid_scope`.
- Clients registered through DCR or CIMD, which is how MCP clients usually
  arrive, "cannot have scopes assigned per client" and receive only `openid`,
  `profile`, `email` and `offline_access`. An environment-wide default set for
  such clients exists but is enabled by WorkOS support.
- Revoking a client's consent (deleting the authorized application) ends
  refresh. Access tokens already issued remain valid until `exp`, at most 300
  seconds under the admitted profile.

Consequence: at a deployment that declares `scope_prefix`, a dynamically
registered client without an environment default is admitted with an empty
ceiling and can perform no policy action. That is the intended fail-closed
behavior, and it is why the prefix is opt-in per trust document rather than a
default. A deployment declares it only after its clients can obtain scopes.

## Design

### Where the ceiling lives

`VerifiedDataToken::Oidc` currently holds `client_id`. It gains the parsed
ceiling, an immutable set of `PolicyAction`, computed once at admission and
owned by the actor for the request. `AuthenticatedActor` exposes
`ceiling() -> Option<&ActionSet>`: `None` for static tokens, native signed
tokens and OIDC tokens at a trust document without a prefix; `Some` for the
rest, including `Some(empty)`.

### Where it is checked

`handlers::authorize(actor, policy, request)` gains the second step described
above. It is the only place; no handler, MCP tool, resource or route checks a
scope on its own. This preserves RFC 0003's invariant that the MCP layer adds
no authorization path.

### Vocabulary as code

The table is a `const` in `crates/omnigraph-server` beside the error codes:
six names, each mapping to a slice of `PolicyAction`. A test asserts that the
union of all slices equals every `PolicyAction` variant, so adding a policy
action without placing it in the vocabulary fails to compile the test suite
rather than silently leaving the action outside every ceiling. A deliberate
omission is recorded by adding the action to the test's exclusion list, with
the RFC that decided it.

### Bounds

| Item | Bound |
|---|---|
| `scope_prefix` | 1–64 bytes, scope-token alphabet |
| `scope` claim entries | ≤ 256, each 1–128 bytes, distinct |
| Admitted token lifetime | unchanged, `exp - iat` ≤ 300 s, no leeway |
| Vocabulary size | 6 names; the whole prefixed list fits in 512 bytes of metadata |

The entry bounds equal the Intent API's `validate_scopes`, so the two
components refuse the same inputs.

## Invariants

- A ceiling never widens access. For every request, allowed ⊆ policy allowed.
- Absent ceiling equals current behavior, byte for byte, in every response.
- Unknown prefixed scopes, malformed `scope` claims and a changed prefix fail
  closed: the token or the file is refused, never partially honored.
- The actor is unchanged by a ceiling. Commits, policy logs and `/access`
  attribute to the same `principal:<id>` with or without one.
- `insufficient_scope` is only ever returned where policy would have allowed
  the action.
- The vocabulary is owned by the server, is versioned with it, and is not
  configurable. A deployment chooses a prefix, never a mapping.

## Compatibility and reversibility

Trust documents without `scope_prefix` are unaffected, and existing documents
have none. Native signed credentials, static tokens, the policy crate, the
engine gate and the cluster authorization path are unchanged. The
`AccessOutput` and metadata additions are optional fields that older clients
ignore; the new error code follows the conventions RFC's rule that a client
treats an unknown code by its HTTP status.

Removing the feature means refusing `scope_prefix` again; no stored state is
touched.

## Alternatives

- **Mandatory token grants.** Rejected by the identity RFC: two ordinary
  permission lists, and policy cannot widen a token.
- **Read `roles` or a permission claim from the token.** Contradicts the
  identity RFC ("admission has no Cedar roles, groups, actions or graph
  grants"), and Connect tokens carry no permissions claim anyway. Roles say
  who a person is; a scope says what one client may do for them.
- **A server-side default ceiling for clients without scopes.** Rejected. It
  would make the trust document a second place where client authority is
  configured. OAuth already has a place for client defaults, the authorization
  server; a deployment that wants dynamically registered clients narrowed
  configures the environment default there.
- **A configurable scope-to-action mapping.** Rejected for the same reason:
  the vocabulary is part of the server's contract, like the action names.
- **Filter in the MCP layer only** (hide write tools from scoped clients).
  Misses HTTP, and makes MCP "a second authorization system", which RFC 0003
  rules out. Tool visibility can follow the ceiling later as a convenience; it
  is never the check.
- **Per-tool or per-route allow lists.** Unstable as tools change; policy
  actions are the stable authorization vocabulary.
- **A separate `graph.export` scope.** Rejected: `read` already permits
  arbitrary queries, so the data is visible either way; the split would
  withhold a transport, not information, while adding a scope every read
  client must know to request.
- **Delegated credentials** (`act`, `delegate:<actor>/<id>`). A different
  feature with its own identity questions; see #978.

## Evidence and tests

- Trust document: prefix accepted, refused when outside the alphabet or over
  64 bytes, refused when changed on refresh, metadata gains `scopes_supported`
  only with a prefix.
- Admission: absent, empty, single, multiple, unknown-prefixed, malformed,
  duplicate and over-bound `scope` claims; non-prefixed entries ignored; a
  trust document without a prefix ignores `scope` entirely.
- Evaluation matrix: every policy action against every scope, with policy
  allowing and denying, asserting `forbidden` when policy denies and
  `insufficient_scope` only when policy allows.
- The vocabulary test covers every `PolicyAction`.
- MCP: every write tool refuses under `graph.read` with the structured
  `insufficient_scope`; every read tool succeeds; `access` reports the
  intersection and `scopes`.
- Challenge: `insufficient_scope` carries `WWW-Authenticate` with `scope`;
  `forbidden` and `not_admitted` carry none.
- Multi-action requests refuse before any effect when a later action is
  outside the ceiling.
- Cluster routes refuse plan and deployment without `cluster.config`.
- Attribution: a scoped write records the same `actor_id` as an unscoped one.
- No regression: a trust document without a prefix produces byte-identical
  responses on the existing OIDC fixtures.

## Rollout

1. Ship parsing, evaluation, metadata and the error code behind the absent
   `scope_prefix`. Nothing changes for any deployment.
2. A deployment assigns the prefixed scopes to its registered OAuth
   applications at the authorization server, and arranges the environment
   default for dynamically registered clients if it wants them narrowed.
3. Only then does it add `scope_prefix` to the trust document and restart.
   The deployment qualifies a real client requesting a subset, receiving
   `insufficient_scope` on a write, re-consenting to a wider scope, and
   succeeding, before calling the feature live.

## Related RFCs and issues

- [Identity credentials and applied policy authorization](2026-09-09-identity-credentials-and-applied-policy.md):
  this RFC adds the optional ceiling that RFC called useful and left out;
  admission, actor identity and the refusal of `act` are unchanged.
- [MCP server surface](0003-mcp-server-surface.md): the check sits in the
  one shared `authorize`; the 403 challenge rule is amended for
  `insufficient_scope` only.
- [HTTP API conventions](2026-10-19-http-api-conventions.md): error shape.
- [Graph access and cross-process writes](2026-10-16-graph-access-serving-and-cross-process-writes.md):
  describes `/access` (its field list predates `actor_id` and `client_id`)
  and observes that "an OIDC server can scope a token per client". This RFC
  is the server side of that observation.
- RFC 0055 still refers to the RFC 0053 action ceiling, which no longer
  exists; it is unrelated to this one and is not corrected here.
- #978 (delegated, branch-scoped credentials), #1173 (client beside actor;
  step 1 landed in #1196), #1025 (OAuth for standard clients), #991 (one MCP
  surface).
- Closed #1218 bundled a version of this with provider-role membership and
  WorkOS Agent Auth; the membership half is #1223, and Agent Auth is out of
  scope here.

## Unresolved questions

- Whether scopes should later be graph-qualified (`graph.read:knowledge`) in
  a vocabulary version 2, and how that composes with `applies_to`.
- Whether `graph.write` should split merge/publish/drop from ordinary change
  once there is a client that needs the distinction.
- Whether identity-credential graph discovery should require `cluster.list`
  under a ceiling, or stay free as now.
- Whether a native signed credential should carry an equivalent field so a
  CLI can mint a narrowed credential for a local agent without an
  authorization server; today that is #978's territory.

## Decision log

- 2026-11-01: drafted after review of og-control-plane#125 and the closure of
  #1218. Scope reduced to the OAuth `scope` claim on admitted OIDC tokens;
  provider-role membership is the separate #1223; Agent Auth, `act`
  delegation and signed-credential ceilings are out of scope.
