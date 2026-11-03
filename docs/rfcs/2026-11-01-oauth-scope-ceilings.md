---
rfc: "2026-11-01-oauth-scope-ceilings"
title: "OAuth scope ceilings"
track: public
status: draft
implementation: not-started
authors:
  - Ragnor Comerford
created: 2026-11-01
updated: 2026-11-03
discussion: https://github.com/ModernRelay/omnigraph/pull/1224
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

The vocabulary is fixed in the engine: seven scope names, each a set of
existing policy actions, one of them limited to the branches applied policy
does not protect. An OIDC trust document opts in by declaring the
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
authenticates through the same bearer path, every tool is listed, and each
tool runs under the policy action of the HTTP route it projects; the
`--mcp-allow-writes` and `--mcp-allow-adhoc` settings that once hid tools
were removed on 2026-10-29. A person who connects an agent to read a graph
also lets it change, merge, publish and drop commits wherever that person
can, and no server setting keeps the agent narrower. RFC 0003 names the
intended remedy: "An operator
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
  tokens. The entry bounds this RFC fixes for the server (256 entries, 128
  bytes, the RFC 6749 scope-token alphabet) are the same numbers, so the two
  components refuse the same inputs.

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
- It does not separate stored-query invocation from ordinary reads. Invoking a
  stored query still requires `read` on the branch or snapshot it runs
  against, checked by the runner's inner gate, so a scope that named
  `invoke_query` alone could run nothing; `invoke_query` therefore sits inside
  `graph.read`. A client limited to deployed queries is the query-contract
  design of [#1197](https://github.com/ModernRelay/omnigraph/issues/1197),
  not a scope.

## Vocabulary

| Term | Meaning |
|---|---|
| Scope | One entry of a token's `scope` claim, as RFC 6749 §3.3 defines it. |
| Scope prefix | The string an OIDC trust document declares; a scope that starts with it is a ceiling scope, every other scope is ignored. |
| Ceiling | The set of policy actions a token's ceiling scopes name, and for the write actions whether they are limited to unprotected branches. A token without ceiling scopes, at a trust document without a prefix, has no ceiling. |
| Protected branch | A branch the applied policy bundle's `protected_branches` list names, exactly as policy's `branch_scope: protected` means it. The ceiling adds no definition of its own. |
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
| `graph.write.unprotected` | the `graph.write` actions, only where the branch acted on is not a protected branch |
| `graph.schema` | `schema_apply` |
| `graph.admin` | `admin` |
| `cluster.list` | `graph_list` |
| `cluster.config` | `config_manage` |

Scopes do not imply one another, with one exception: `graph.write` covers
everything `graph.write.unprotected` covers, so a token granted both simply
has `graph.write`. A writer that also reads holds `graph.read` as well. The
table covers every one of the thirteen current policy actions.

`graph.write.unprotected` is the ceiling for an agent that may work on
branches but not land on the ones that matter. The branch it is judged by is
the slot of the policy request that the action already uses: the `branch`
slot for `change`, which policy judges by `branch_scope`, and the
`target_branch` slot for every other write action, which policy judges by
`target_branch_scope`: the deleted branch for `branch_delete`, the new branch
for `branch_create`, and the destination for `branch_merge`, `commit_publish`
and `commit_drop`. The access report classifies the actions the same way
(`access_slot` in `omnigraph-api-types`), so the ceiling adds no third list.
A merge from `main` into a feature branch is inside the ceiling; a merge into
`main` is not, and neither is deleting `main`. Schema apply targets `main`
only and is not a write action here, so this scope never covers it; a client
that applies schema holds `graph.schema`. Which branches are protected is
decided by the applied bundle's `protected_branches`, the same list that
gives `branch_scope: protected` its meaning, so a deployment has one
definition of a protected branch and changes it in one place. A bundle that
names no protected branches protects nothing, for policy and for this scope
alike. A server whose trust document declares a prefix names every graph
whose applied bundle lists none in its boot log, and again whenever such a
bundle is applied, so the gap is visible where an operator reads.

This follows the shape GitHub uses for the same problem. Its credentials say
`contents: write` for a repository and never name a branch; rulesets on the
repository decide who may bypass branch protection, and an installed app is
not a bypass actor unless the ruleset lists it, so integrations land on
unprotected branches by default and a person merges. Here the policy bundle
is the ruleset, and the scope is the fixed credential-side word for "not a
bypass actor". Letting policy rules name a client directly, as a ruleset
names an app, is a complement rather than an alternative; see the
alternatives below.

`export` sits inside `graph.read` deliberately. The `read` action already
covers "queries, snapshots, branches, and commits", so a client holding
`graph.read` can run any query over HTTP and see everything its user can;
snapshot export is a resumable bulk transport for the same data, not a
confidentiality boundary. A scope that withheld it would protect only
convenience. Policy keeps `export` as its own action for people, where
denying bulk export while allowing queries is a reasonable operator choice; a
client ceiling needs no such distinction.

A policy action added later belongs to no scope until the vocabulary version
that names it, so a ceilinged token can never perform it. This is the
fail-closed direction.

A scope names a kind of action, not a graph. `graph.read` lets the client
read on every graph of the cluster where applied policy lets the person read;
policy still decides graph by graph through `applies_to`, so a client never
reaches a graph its user cannot. Narrowing a client to one of the user's
graphs is a different question, answered in OAuth by the resource indicator
(RFC 8707) rather than by the scope; see the unresolved questions.

Graph discovery for an identity credential (`GET /graphs/discovery`, and
the identity path of the `graphs` tool) names graphs by ID and display name
and checks no policy action today; a ceiling does not change it. `GET
/graphs`, which every other credential uses, checks `graph_list` and so sits
under `cluster.list`.

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
  then refused with `insufficient_scope` (below).
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
function, `handlers::authorize`, which holds the full `AuthenticatedActor`
and today returns one of two outcomes, `Allowed` or `Denied`. It gains a
third, outside the ceiling, and the ceiling is checked there after the
policy decision:

1. Applied policy decides as today. A denial is `Denied`, answered 403
   `forbidden` or hidden as 404 exactly where it is today.
2. If policy allows and the actor's token has a ceiling, the requested policy
   action must be in it. A write action covered only by
   `graph.write.unprotected` is in the ceiling only when the branch in the
   slot the request is judged by is not protected in the applied bundle. A
   miss is the third outcome, answered 403 `insufficient_scope`.

The second step sees the same `PolicyRequest` the first did, branch and target
branch included, and asks the same `PolicyEngine` whether the judged branch
is protected, through one new predicate beside `uses_target_branch_scope`; it
reads nothing the policy decision did not. The step also runs where
`authorize` allows without an engine: a server in default-deny mode (bearer
tokens configured, no applied bundle) permits `read` alone, and a ceilinged
token without `graph.read` is refused there too. An open server
(`--unauthenticated`) resolves no actor and so has no ceiling.

The third outcome exists because the routes that hide a policy denial as 404
must not hide a ceiling miss. Invoking a stored query, changing an
unattached branch from its base commit, creating a branch from a snapshot and
reading a commit's changes all answer `Denied` with "not found", so an actor
without the grant cannot learn whether the query or commit exists. A ceiling
miss is reached only after policy confirmed that the actor holds the grant,
so on every one of those routes it is answered 403 `insufficient_scope`: the
client learns only that its own user may act there, which that user reads
from `/access` anyway. Where a route folds a per-branch `Denied` into a
listing (the branch listing and the readable change diff omit branches the
actor cannot read), a ceiling miss is not a per-branch fact and refuses the
whole listing with 403 rather than emptying it.

Policy goes first so `insufficient_scope` is only ever returned for an action
the person could perform with a wider scope. A client that receives it may ask
the user for that scope and will then succeed; a client that receives
`forbidden` must stop, as UIS-style client guidance already requires. The
response reveals to the client only that its own user holds a permission,
which the user can read from `/access` anyway.

The engine's second gate (`Omnigraph::enforce`, the `PolicyChecker` trait)
sees only an actor string and stays as it is: it runs after `authorize` on
the same request, and a request outside the ceiling never reaches it. The
cluster path is different and is covered next.

A request that performs several actions (a `mutate` with branch statements, a
`load` with `from`, a stored mutation's `invoke_query` then `change`) checks
each action as it does today; the first action outside the ceiling refuses
the request before any effect, exactly where a policy denial would.

### Deployment routes

The deployment routes are the one place where a policy action is checked
without passing through `handlers::authorize`. `POST /cluster/plan` and
`POST /cluster/deployments` check `config_manage` through
`authorize_request`, then hand the cluster crate an `IdentityAuthorization`
that carries only the actor string. The crate itself checks `read` on `main`
and `schema_apply` on `main` for every graph the deployment touches
(`AppliedPolicies::check_graph`, in the preview and again during apply when a
graph's schema digest changes), and `schema_apply` is checked nowhere else.
A ceiling applied only in `authorize` would cover `config_manage` and leave
`graph.schema` meaningless: a token holding only `cluster.config` could
still apply schema.

So the ceiling travels with the identity. `IdentityAuthorization` gains the
same optional ceiling the actor holds, set by the server when it constructs
the deployment caller, and `check_cluster` and `check_graph` apply it after
their policy check, in the same order as the HTTP layer: `config_manage`
under `cluster.config`, `read` under `graph.read`, `schema_apply` under
`graph.schema`. A miss is a refusal diagnostic with code `insufficient_scope`
that the server answers as the 403 below. Every one of these checks runs
before the deployment is accepted. The ledger records the actor, as today,
and never the ceiling; a settlement resumed after acceptance (a severed
caller, a restarted server) is an accepted operation that carries no token,
so no ceiling applies to it.

`GET /cluster/deployments/{id}` (`lookup`) builds its identity the same way
but without the `config_manage` check, so that a receipt can be read back by
its original identity. It carries the ceiling the same way; what it checks
is not changed by this RFC.

### Refusal shape

`insufficient_scope` is a new `ErrorCode` with status 403, serialized as the
HTTP conventions RFC requires:

```json
{
  "error": "the credential's scope does not cover change",
  "code": "insufficient_scope",
  "disposition": {"effects": "none", "action": "fix"},
  "scope": "omnigraph:graph.write"
}
```

`scope` is the typed detail: the one prefixed scope that would have covered
the refused action. A write to a protected branch under
`graph.write.unprotected` names `graph.write`, the scope that would have
covered it. `ErrorOutput` gains the optional field, and so does
`ChangeErrorOutput`, which the change routes serialize instead. The response
also carries

```
WWW-Authenticate: Bearer error="insufficient_scope", scope="omnigraph:graph.write", resource_metadata="<url>"
```

as RFC 6750 §3.1 specifies for this error. The `challenge` middleware sets
`WWW-Authenticate` only on a 401 and stays that way; the `insufficient_scope`
error carries its own header, built from the same protected-resource metadata
URL the 401 challenge names. RFC 0003 currently says a 403 carries no
challenge; this RFC amends that for `insufficient_scope` only. `forbidden`
and `not_admitted` keep no challenge, so a client still cannot be driven to
re-authenticate for a permission its user lacks.

MCP tool refusals carry the same `error`, `code`, `disposition` and `scope`
in their structured content, as every handler refusal does.

### Discovery

When the trust document declares a prefix, the protected-resource metadata
adds the RFC 9728 field:

```json
{
  "resource": "https://g-01jd8y-01je4k.graphs.example.com/mcp",
  "authorization_servers": ["https://your-environment.authkit.app"],
  "bearer_methods_supported": ["header"],
  "scopes_supported": [
    "omnigraph:graph.read", "omnigraph:graph.write",
    "omnigraph:graph.write.unprotected", "omnigraph:graph.schema",
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
permits intersected with the ceiling, a policy denial and a ceiling miss both
reading as absent. An empty `scopes` array is a token granted no ceiling
scopes, and its report lists no action while still naming the scopes; an
absent field is a token without a ceiling.

Under `graph.write.unprotected` the report takes the shape policy already
produces for a rule with `branch_scope: unprotected`: the write actions leave
`graph_wide`, and `actions` lists them for an unprotected `branch` and omits
them for a protected one. No new field is needed.

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
ceiling, an immutable value computed once at admission and owned by the actor
for the request: the set of `PolicyAction` the scopes name, and whether the
write actions are limited to unprotected branches (true when
`graph.write.unprotected` is present and `graph.write` is not).
`AuthenticatedActor` exposes `ceiling() -> Option<&Ceiling>`: `None` for
static tokens, native signed tokens and OIDC tokens at a trust document
without a prefix; `Some` for the rest, including `Some(empty)`. The
deployment routes copy that value onto the `IdentityAuthorization` they hand
the cluster crate, which is the only other place it lives.

### Where it is checked

`handlers::authorize(actor, policy, request)` gains the third outcome and the
second step described above, and `AppliedPolicies::check_cluster` and
`check_graph` apply the ceiling that `IdentityAuthorization` carries. Those
are the only two places; no handler, MCP tool, resource or route checks a
scope on its own. This preserves RFC 0003's invariant that the MCP layer adds
no authorization path. The branch test for `graph.write.unprotected` is one
predicate on `PolicyEngine`: given a `PolicyRequest`, whether the branch in
the slot the action is judged by (`target_branch` for the actions
`uses_target_branch_scope` names, `branch` otherwise) is in
`protected_branches`. The server keeps no list of actions or slots of its own,
so the ceiling and policy cannot disagree about which branch an action
touches.

### Vocabulary as code

The table is a `const` in `crates/omnigraph-server` beside the error codes:
seven names, each mapping to a slice of `PolicyAction`, the unprotected
variant sharing `graph.write`'s slice and carrying the branch limit as a
flag. A test asserts that the
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
| Admitted token lifetime | unchanged: `exp - iat` ≤ 300 s, zero validation leeway, `iat` at most 30 s in the future |
| Vocabulary size | 7 names; the whole prefixed list fits in 512 bytes of metadata |

The entry bounds are this server's contract. They coincide with the Intent
API's `validate_scopes`, so the two components refuse the same inputs.

## Invariants

- A ceiling never widens access. For every request, allowed ⊆ policy allowed.
- Absent ceiling equals current behavior, byte for byte, in every response.
- Unknown prefixed scopes, malformed `scope` claims and a changed prefix fail
  closed: the token or the file is refused, never partially honored.
- The actor is unchanged by a ceiling. Commits, policy logs and `/access`
  attribute to the same `principal:<id>` with or without one.
- `insufficient_scope` is only ever returned where policy would have allowed
  the action, and a ceiling miss is never hidden as 404: the existence it
  could disclose is already the user's to read.
- The cluster crate applies the ceiling it is handed before acceptance and
  records none of it; a settlement resumed after acceptance has no ceiling
  to apply.
- The vocabulary is owned by the server, is versioned with it, and is not
  configurable. A deployment chooses a prefix, never a mapping.
- A branch is protected for the ceiling exactly when applied policy's
  `protected_branches` names it. The ceiling never defines, defaults or
  implies a protected branch of its own.

## Compatibility and reversibility

Trust documents without `scope_prefix` are unaffected, and existing documents
have none. Native signed credentials, static tokens and the engine gate are
unchanged. The policy crate gains one predicate and the cluster authorization
path gains an optional ceiling on `IdentityAuthorization`; with no ceiling
both behave as today. The
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
- **A separate scope for `invoke_query`**, so an agent could run deployed
  queries without ad-hoc reads. Not possible yet: the stored-query runner
  checks `read` on the branch or snapshot under its inner gate, so a token
  without `graph.read` could invoke nothing. The split is #1197's query
  contracts, where a deployed query's own contract authorizes its reads;
  once that lands, a scope for it is one table row.
- **A separate `graph.export` scope.** Rejected: `read` already permits
  arbitrary queries, so the data is visible either way; the split would
  withhold a transport, not information, while adding a scope every read
  client must know to request.
- **Branch-limited writes through policy rules that name the client**, the
  way a GitHub ruleset names an app as a bypass actor. This is the fuller
  model and is where #1173 points, but it is not this RFC: `client_id` would
  have to reach Cedar as a selector, and the clients that most need
  narrowing, those registered through DCR or CIMD, receive a new `client_id`
  per registration, so no policy could list them. A fixed scope in the
  authorization server's environment default is the one lever that reaches
  them. The two compose: a later client selector can widen or narrow what a
  scoped client may do on protected branches without changing the scope.
- **A parametric scope** such as `graph.write:branch=feature/*`. WorkOS
  assigns scopes from a fixed environment-wide catalog and cannot assign any
  to dynamically registered clients, so a scope that embeds a branch pattern
  cannot be issued. Rich Authorization Requests (RFC 9396) are the OAuth home
  for such parameters and are not available here.
- **Treating `main` as protected for the scope even when
  `protected_branches` omits it.** Rejected: it would give a deployment two
  definitions of a protected branch, one for people and one for clients. A
  bundle that protects nothing is a policy author's choice, and preview says
  so.
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
- `graph.write.unprotected`: each write action on a protected and an
  unprotected branch, judged by the `branch` slot for `change` and the
  `target_branch` slot for `branch_create`, `branch_delete`, `branch_merge`,
  `commit_publish` and `commit_drop`; deleting a protected branch refuses; a
  merge from a protected source into an unprotected target succeeds and the
  reverse refuses with `scope: "<prefix>graph.write"`; `schema_apply`
  refuses; a bundle with no `protected_branches` permits every branch and the
  boot log names the graph; a token holding both write scopes behaves as
  `graph.write`; `/access` matches the shape of a `branch_scope: unprotected`
  rule.
- Hidden-404 routes: on stored-query invoke, change from a base commit,
  branch create from a snapshot and commit changes, a policy denial keeps its
  404 and a ceiling miss answers 403 `insufficient_scope`; the branch listing
  and readable change diff refuse under a ceiling without `graph.read` rather
  than returning empty.
- Default-deny mode (tokens, no bundle): `read` is refused for a ceiling
  without `graph.read` and allowed with it.
- MCP: every write tool refuses under `graph.read` with the structured
  `insufficient_scope`; every read tool succeeds; `access` reports the
  intersection and `scopes`.
- Challenge: `insufficient_scope` carries `WWW-Authenticate` with `scope`,
  set by the error rather than the 401 middleware, and a body with
  `disposition` and `scope` on both `ErrorOutput` and `ChangeErrorOutput`;
  `forbidden` and `not_admitted` carry no challenge.
- Multi-action requests refuse before any effect when a later action is
  outside the ceiling.
- Deployment routes: plan and apply refuse without `cluster.config`; with
  `cluster.config` alone, a deployment that changes a graph's schema refuses
  in preview and in apply with `insufficient_scope` naming `graph.schema`,
  and one that reads a graph refuses naming `graph.read`; the ledger of an
  accepted deployment holds no scope, and a settlement resumed after
  acceptance completes without a token.
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
  one shared `authorize`, and since every tool is listed and runs under its
  HTTP action the ceiling reaches MCP without a second path; the 403
  challenge rule is amended for `insufficient_scope` only.
- [HTTP API conventions](2026-10-19-http-api-conventions.md): error shape.
- [Graph access and cross-process writes](2026-10-16-graph-access-serving-and-cross-process-writes.md):
  describes `/access` (its field list predates `actor_id` and `client_id`)
  and observes that "an OIDC server can scope a token per client". This RFC
  is the server side of that observation.
- RFC 0055 still refers to the RFC 0053 action ceiling, which no longer
  exists; it is unrelated to this one and is not corrected here.
- #978 (delegated, branch-scoped credentials; `graph.write.unprotected`
  gives OAuth clients the protected-branch half of that ask without a
  delegated identity), #1173 (client beside actor; step 1 landed in #1196;
  a client selector in policy would be its continuation), #1197 (query
  contracts; the stored-query-only client this RFC cannot express), #1025
  (OAuth for standard clients), #991 (one MCP surface).
- Closed #1218 bundled a version of this with provider-role membership and
  WorkOS Agent Auth; the membership half is #1223, and Agent Auth is out of
  scope here.

## Unresolved questions

- How a client is narrowed to one graph of a multi-graph cluster. Scope
  strings are the wrong carrier: WorkOS assigns scopes from an
  environment-wide permission catalog, which cannot enumerate every tenant's
  graph IDs, and dynamically registered clients cannot be assigned scopes at
  all. The OAuth answer is the resource indicator: WorkOS issues `aud` as the
  concrete URI a client requested under a path-wildcard resource indicator
  (`https://<graph host>/mcp/*`), so `…/mcp/<graph_id>` could name the graph
  in the audience and the server could bind the token to that graph. That
  changes audience admission, resource registration and the served metadata
  paths, and is its own RFC.
- Whether `graph.write` should split merge/publish/drop from ordinary change
  once there is a client that needs the distinction. The protected-branch
  limit covers the common case (an agent may merge and publish on its own
  branches but not onto `main`) without that split.
- Whether a scope-enabled deployment with a bundle that lists no
  `protected_branches` should be a boot-log report only, as proposed, or a
  validation failure for that bundle.
- Whether identity-credential graph discovery (`GET /graphs/discovery` and
  the identity path of the `graphs` tool) should require `cluster.list`
  under a ceiling, or stay free as now.
- Whether `GET /cluster/deployments/{id}` should check `config_manage` as
  the other deployment routes do. It does not today, and this RFC leaves
  that as it is.
- Whether a native signed credential should carry an equivalent field so a
  CLI can mint a narrowed credential for a local agent without an
  authorization server; today that is #978's territory.

## Decision log

- 2026-11-01: drafted after review of og-control-plane#125 and the closure of
  #1218. Scope reduced to the OAuth `scope` claim on admitted OIDC tokens;
  provider-role membership is the separate #1223; Agent Auth, `act`
  delegation and signed-credential ceilings are out of scope.
- 2026-11-01: added `graph.write.unprotected`, judged by policy's own
  `protected_branches`, after the question of how a client is kept off
  `main`; the GitHub ruleset model is recorded as the policy-side complement.
- 2026-11-03: revised after the review on #1224, verified against main
  36d3ea08. Write actions are judged by the request slot the action already
  uses, so `branch_delete` is judged by the deleted branch; the ceiling
  travels on `IdentityAuthorization` so the cluster crate's `read` and
  `schema_apply` checks honor `graph.read` and `graph.schema`; `authorize`
  gains a third outcome so a ceiling miss is never hidden as 404 and the 403
  carries its own challenge and `disposition`; the removed
  `--mcp-allow-writes` is no longer cited and discovery names
  `GET /graphs/discovery`; `invoke_query` stays in `graph.read` as a stated
  non-goal pending #1197; the empty `protected_branches` report moves to the
  boot log.
