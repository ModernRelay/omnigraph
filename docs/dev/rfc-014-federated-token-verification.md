# RFC: Federated Token Verification — OIDC/WorkOS JWTs → Server-Resolved Actors → Cedar

**Status:** Proposed
**Date:** 2026-07-12
**Tickets:** MR-956 (federated auth / WorkOS OAuth — this RFC), MR-971 (per-server credential resolver — the CLI companion), MR-969/RFC-003 (MCP surface — the first consumer of the `TokenVerifier` seam)
**Builds on:** the shipped bearer-token auth path (`crates/omnigraph-server/src/{auth,handlers,identity}.rs`), the Cedar policy engine (`crates/omnigraph-policy`), cluster-boot policy binding ([rfc-005](rfc-005-server-cluster-boot.md)), and the identity design reference in the Tower repo (`docs/design/identity-workos-cedar.md`, validated against the live WorkOS docs 2026-07-04).
**Naming note:** `identity.rs` doc-comments reference this design as draft "RFC 0001" (and Cloud mode as "RFC 0003", OAuth scopes as "RFC 0004") — a *plans-space* numbering that collides with this repo's `docs/dev/rfc-001/003/004`. Per [rfc-003](rfc-003-mcp-server-surface.md)'s numbering note, cite this work as **MR-956 / RFC-014**; the `identity.rs` comments should be updated to point here when step 1 lands.

## Summary

Add a **federated token-verification path** to `omnigraph-server`: verify an OIDC JWT (WorkOS
AuthKit today; any customer OIDC issuer by the same mechanism) against the issuer's JWKS, resolve
the **actor server-side from the verified claims**, map claim-derived roles/permissions onto the
**existing Cedar group vocabulary**, and let the existing Cedar engine decide — so people, CI, and
agents reach OmniGraph *as themselves* instead of through a shared static token
(`specs 002-sso-oauth-access` in the Tower repo, FR-001…003/007/008/013).

The architectural position, decided in the identity design doc and preserved here:

> **OmniGraph never builds, hosts, or terminates an OAuth authorization server.** WorkOS
> AuthKit/Connect (or the customer's IdP) is the Authorization Server. OmniGraph is purely an
> **OAuth 2.0 Protected Resource**: it verifies JWTs offline against a cached JWKS, resolves the
> actor, and advertises RFC 9728 discovery metadata. Every caller (console BFF, CLI device flow,
> M2M client-credentials, MCP agents) differs only in how it *obtains* a token — never in how the
> server verifies one.

Concretely, four additions behind one seam:

1. A **`TokenVerifier` seam** in `omnigraph-server`: the existing static-hash path is extracted as
   `StaticHashTokenVerifier` (behavior-identical), and `OidcJwtVerifier` is added beside it. Both
   produce the already-shipped `ResolvedActor`; everything downstream (Cedar `authorize`, the `_as`
   writers, MCP per RFC-003 §5.8) is untouched.
2. A **reviewed trust definition** in `cluster.yaml` (`auth:`): trusted issuers, JWKS source,
   optional audience, org binding, and the claim→Cedar-group mapping. It contains **no secrets**
   (JWKS is public-key material), so it belongs in the reviewed control-plane config, not env vars.
3. A **federated principal path in `omnigraph-policy`**: Cedar policy *files* stay byte-identical
   (rules still grant to groups); federated actors get their group parents from verified claims
   instead of the static `groups:` roster, via per-request principal injection.
4. **RFC 9728 discovery** (`/.well-known/oauth-protected-resource` + `WWW-Authenticate` on 401) so
   compliant clients (MCP included) find the authorization server with zero configuration.

The static bearer-token path is **retained unchanged** as bootstrap/break-glass/air-gapped/simple-CI
auth (spec 002 FR-012/SC-007 — zero regressions for existing callers).

## Reconciliation with shipped code (verified against this branch's HEAD, 2026-07-12)

Every claim below was checked against source, not docs:

- ✅ **`ResolvedActor` is ready.** `identity.rs` ships
  `ResolvedActor { actor_id: Arc<str>, tenant_id: Option<TenantId>, scopes: Vec<Scope>, source: AuthSource }`
  with `Scope`/`AuthSource` both `#[non_exhaustive]`, and doc-comments explicitly reserving
  `AuthSource::Oidc`, `tenant_id` from the OAuth `org_id` claim, and OAuth-style scopes. This RFC
  fills those reserved seams; no shape change.
- ✅ **Static path.** `require_bearer_auth` (handlers.rs) → `authenticate_bearer_token` (lib.rs):
  SHA-256 at startup (`hash_bearer_tokens`), constant-time compare (`subtle::ConstantTimeEq`,
  iterating all entries unconditionally), actor from the `{actor_id: token}` map. Token sources
  (auth.rs `TokenSource`): `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` (feature `aws`) →
  `…_TOKENS_FILE` → `…_TOKENS_JSON` → `OMNIGRAPH_SERVER_BEARER_TOKEN` (single, actor `default`).
- ✅ **Cedar engine.** `PolicyEngine::authorize(actor_id, PolicyRequest { action, branch,
  target_branch })`; policies compile to `permit (principal in Omnigraph::Group::"…", action ==
  …, resource == Graph/Server)`; schema is `entity Actor in [Group]`. Policy bundles bind via
  `cluster.yaml policies: { applies_to: [cluster | <graph_id>] }` (settings.rs splits
  `server_policy` vs `graph_policies`).
- ⚠️ **The `known_actors` gate is the load-bearing gap.** `PolicyEngine` pre-compiles `Entities`
  from the static `groups:` roster and **early-denies any actor id not in the union of group
  members** (lib.rs `known_actors`, checked first in `authorize`). A federated `user_01…` can
  never appear in that roster — §5.4 is the designed bridge.
- ⚠️ **`TenantId` rejects WorkOS org ids.** Its charset is `^[a-zA-Z0-9-]{1,64}$`;
  WorkOS ids are underscore-delimited (`org_01…`). Mapping `org_id → tenant_id` requires widening
  the charset to include `_` (validation catch; §5.5). The same holds if anyone reuses the regex
  for actor-shaped ids (`user_01…`) — Cedar itself has no such restriction (`entity_uid` accepts
  arbitrary strings).
- ✅ **Deps.** Workspace already carries `reqwest 0.12` (rustls) for JWKS fetch, `sha2`/`subtle`
  for the static path. **No JWT library present** — §5.3 adds `jsonwebtoken` v10 (latest 10.4.0;
  ships a `jwk` module for public JWK sets, `DecodingKey::from_jwk`, and `Validation` for
  `iss`/`aud`/`exp`; see [crates.io](https://crates.io/crates/jsonwebtoken),
  [docs.rs jwk module](https://docs.rs/jsonwebtoken/latest/jsonwebtoken/jwk/index.html)).
- ✅ **401 today** is `ApiError::unauthorized` → status 401 + JSON `ErrorOutput`, with **no**
  `WWW-Authenticate` header; the single attach point is `impl IntoResponse for ApiError` (lib.rs),
  which already conditionally adds `Retry-After` for 429 — the same pattern extends to 401.
- ✅ **`cluster.yaml` has no auth section** (`RawClusterConfig { version, metadata, storage, state,
  providers, graphs, policies }`) — `auth:` slots in beside `providers:`/`policies:` and flows to
  the server through the same applied-revision snapshot the policies already ride.
- ✅ **WorkOS facts** (from the identity design doc's 2026-07-04 validation log, plus one
  *live-production* confirmation): the access token is an RS256 JWT verified against JWKS at
  `https://api.workos.com/sso/jwks/<client_id>`; default claims are `sub`, `sid`, `org_id`,
  `role` (**singular**), `permissions` (array), `exp`, `iat`; **user tokens carry no default
  `aud`** (agent tokens do); agent tokens carry `sub = agent_reg_…` plus an **`act.sub`
  delegation claim** (RFC 8693) naming the authorizing person. **Live correction to the design
  doc:** a working AuthKit deployment (Tower's Convex integration) validates against **two**
  issuers — `https://api.workos.com/` *and* `https://api.workos.com/user_management/<client_id>`
  — so issuer config must be a **list per provider**, not a single string.
- ❓ **Still unverified before implementation** (carried from the design doc's log): the exact
  WorkOS JWT-template syntax/context variables (only needed for the optional `omnigraph_actor`
  claim) and the precise permission-slug charset (`-.:_*` claimed). Neither blocks the design:
  actor falls back to `sub`, and Cedar group names are arbitrary strings.

## Motivation

- **Shared static tokens are the biggest operational risk** (spec 002): no attribution, no
  role-following access, revocation = rotating a secret everyone shares.
- **Attribution is the product.** Commits, audit rows, and branch reviews should name the person
  (or the agent *and* its authorizing person), not `default` (Tower Constitution VI/VII).
- **One verification seam serves every access path.** Console BFF, `omnigraph login` (device
  flow), third-party apps, M2M/CI, and MCP agents all present the same kind of verified JWT;
  RFC-003 already builds the MCP surface against "an already-resolved `ResolvedActor`" and leaves
  the RFC 9728 hook to this RFC.
- **On-prem and cloud must not fork.** A customer IdP (issuer = theirs) and our managed cloud
  (issuer = WorkOS) differ only in `auth:` config, never in code paths.

## Goals

- Verify RS256 JWTs from configured trusted issuers offline (cached JWKS); reject on signature,
  `iss`, `exp`, or (when configured) `aud`/org mismatch — fail closed (spec 002 FR-001/FR-013).
- Resolve the actor server-side from verified claims (`omnigraph_actor` else `sub`); clients can
  never assert identity (FR-002; preserves the MR-731 invariant and engine invariants 11/12).
- Map claim roles/permissions onto the **existing** Cedar group vocabulary — one policy engine,
  one rule shape, no parallel authorization path (FR-006/007).
- Keep static bearer tokens working byte-for-byte (FR-012; SC-007).
- Advertise RFC 9728 protected-resource metadata + `WWW-Authenticate` so discovery needs zero
  client configuration (FR-009).
- Record agent delegation (`act.sub`) alongside the agent actor (FR-011) — phased; see §5.7.

## Non-Goals

- **Building or hosting an OAuth authorization server** — WorkOS/customer IdP owns login UI, token
  minting, MFA, consent. The server never holds an OAuth client secret.
- **The CLI device flow** (`omnigraph login` SSO upgrade) — consumes this seam; designed in the
  identity doc §11; tracked as MR-971's `CredentialSource` sibling key. Engine change: none.
- **Capability introspection** ("what may actor A do on graph G/branch B") — required by the
  console (spec 002 US-3) but a separate additive endpoint on top of this seam; own ticket.
- **Cloud multi-tenant routing** (`GraphKey::cloud`, per-tenant registries) — this RFC *binds* a
  cluster to one org (§5.5); tenant-routing stays the reserved Cloud-mode seam.
- **WorkOS FGA** — declined for graph resources (two resource-authz engines = two owners for one
  fact); recorded in the identity design doc §2.
- **Per-query `invoke_query` scope** — RFC-003 PR 0b, orthogonal.

## Design

### 5.1 The `TokenVerifier` seam

```rust
// crates/omnigraph-server/src/auth.rs (new trait, same module as TokenSource)
#[async_trait]
pub(crate) trait TokenVerifier: Send + Sync {
    /// Verify a bearer credential and resolve the actor, or None if this
    /// verifier does not recognize/accept it. Must not panic; must not block
    /// on network I/O in the request path (see JWKS cache, §5.3).
    async fn verify(&self, token: &str) -> Option<ResolvedActor>;
    fn name(&self) -> &'static str;
}
```

- **`StaticHashTokenVerifier`** — pure extraction of today's `authenticate_bearer_token`
  (constant-time semantics preserved verbatim). PR 1 lands this refactor with zero behavior
  change, satisfying RFC-003's "modular auth" requirement on its own.
- **`OidcJwtVerifier`** — one instance per configured provider (§5.2); holds the JWKS cache.
- **Dispatch:** a compact-JWS shape probe (`two '.' separators with base64url segments`) routes to
  the OIDC verifiers; anything else goes to the static verifier. The probe inspects only public
  structure, not secret content, so it adds no timing side-channel to the static path; a JWT that
  fails OIDC verification is **rejected**, never retried against the static table (a signed
  credential must not alias an opaque one).
- `require_bearer_auth` keeps its exact contract: header parse → verifier chain →
  `request.extensions_mut().insert(actor)` → downstream `authorize_request` / `_as` writers see
  the same `ResolvedActor` they see today.

### 5.2 Trust configuration — a reviewed definition in `cluster.yaml`

JWT verification needs *public* trust anchors only, so the config is a definition (Constitution
I/II of the Tower constitution; same reasoning as `policies:`) and rides the cluster snapshot the
server already boots from:

```yaml
# cluster.yaml
auth:
  oidc:
    workos:                                   # provider name (free-form)
      issuers:                                # LIST — WorkOS uses two in practice
        - https://api.workos.com/
        - https://api.workos.com/user_management/client_01ABC…
      jwks: https://api.workos.com/sso/jwks/client_01ABC…   # url | file:// | inline
      audience: https://graph.example.com     # optional; REQUIRED to enforce RFC 8707 binding
      expected_org: org_01XYZ…                 # optional; reject tokens for other orgs
      actor_claim: omnigraph_actor             # optional; default resolution is `sub`
      groups:
        from: [role, permissions]              # which claims contribute Cedar groups
        map:                                   # optional alias map, claim value → group
          ontology-architect: architects
          "read:graph": readers
```

- **The schema is issuer-agnostic by construction.** `groups.from` takes arbitrary claim names —
  `[role, permissions]` for WorkOS, `[groups]` for an Entra/Okta token carrying AD group claims —
  and `map` normalizes foreign claim values onto the cluster's Cedar group vocabulary. Nothing in
  the config (or the verifier) is WorkOS-specific; WorkOS is one set of values (§5.9).

- **`issuers` is a list** (live-validated: AuthKit deployments present both the root and the
  `user_management/<client_id>` issuer).
- **`jwks`** accepts an HTTPS URL (normal), a `file://` path, or inline JWKS JSON — the latter two
  make **air-gapped** deployments work with zero request-path *and* zero boot-path network.
- **`audience`** should be set to the server's public URL and registered as an RFC 8707 Resource
  Indicator in WorkOS — this is the standardized form of the CLI's existing "a keyed token is only
  sent to the server it is keyed to" rule. Without it, user tokens (no default `aud`) are accepted
  on `iss`+signature alone; the RFC recommends configuring it for any internet-reachable server.
- **No secrets appear**; static-token env vars are untouched. `cluster apply` treats `auth:` as a
  Tier-0 (reversible, additive) definition change.
- Boot behavior is **fail-closed** (spec 002 FR-013): if `auth.oidc` is configured and the JWKS
  cannot be loaded at boot (URL unreachable and no cache/file), the server refuses to serve rather
  than silently degrading to static-only.

### 5.3 Verification mechanics

- **Library:** `jsonwebtoken = "10"` (RS256; `jwk::JwkSet` parsing; `Validation` handles
  `iss`/`aud`/`exp`/`nbf`; algorithm allow-list pinned to `[RS256]` so `alg=none`/HS256-confusion
  attacks are structurally rejected).
- **JWKS cache:** fetched at boot (via the workspace `reqwest`), held in memory keyed by `kid`.
  Request-path verification is **offline** — no per-request network. Refresh triggers: an unknown
  `kid` (debounced — at most one refresh in flight, with a floor interval so a garbage-`kid` flood
  cannot become an SSRF/DoS vector) and a periodic TTL (default 15 min). `file://`/inline sources
  never refresh over the network.
- **Checks, in order:** compact-JWS parse → `kid` → signature (RS256, JWKS) → `iss` ∈ configured
  list → `exp`/`nbf` (default leeway 60 s, configurable — spec 002's documented clock-skew
  tolerance) → `aud` if configured → `org_id == expected_org` if configured. Any failure →
  `ApiError::unauthorized` with a reason that does **not** echo token contents.
- **No plaintext retention** (engine invariant 12's spirit): the JWT is verified per request and
  dropped; nothing token-derived is stored beyond the `ResolvedActor`. The SHA-256/constant-time
  requirements remain scoped to the static path (signature verification is not a
  secret-equality comparison).

### 5.4 Claims → Cedar: federated principals without a second authz engine

The invariant to preserve: **Cedar stays the sole enforcement point, and policy files keep their
exact shape** — rules grant actions to *groups*. What changes is only where a principal's group
parents come from:

- **Static actors** (unchanged): parents from the `groups:` roster in the policy file.
- **Federated actors:** parents derived from **verified claims** at request time — each value of
  `role` and each entry of `permissions[]` (post `groups.map` aliasing) names a Cedar group.

Engine change (`omnigraph-policy`):

```rust
/// Existing entry point — static actors; behavior unchanged.
pub fn authorize(&self, actor_id: &str, request: &PolicyRequest) -> Result<PolicyDecision>;

/// Federated principals: the caller (the server's auth layer, never the client)
/// supplies claim-derived group names. The engine injects a per-request
/// `Actor` entity whose parents are the intersection of `groups` with the
/// groups the policy actually declares/uses, layered over the cached static
/// entity set. An empty intersection short-circuits to the same deny shape as
/// an unknown actor — a verified identity with no mapped group has no access.
pub fn authorize_federated(
    &self,
    actor_id: &str,
    groups: &[String],
    request: &PolicyRequest,
) -> Result<PolicyDecision>;
```

- The `known_actors` early-deny stays for the static path. Federated actors are *known by
  verification*: their admission ticket is a valid signature from a trusted issuer, and their
  privileges are exactly their mapped groups' — an unmapped federated identity gets the same deny
  as an unknown static actor (default-deny floor, RFC-003 §5.9).
- Per-request entity construction is one `Entity` + one `Entities` overlay on the pre-compiled
  static set — O(groups) per request, no policy recompilation. (If profiling ever shows the
  overlay matters, an LRU keyed by the sorted group set is a drop-in; not built until measured.)
- Group names are arbitrary Cedar strings, so permission slugs like `read:graph` are legal group
  ids as-is; the optional `map` exists for human-friendly policy files, not as a requirement.
- `omnigraph policy test` / `policy explain` gain an optional `--groups` input so operators can
  exercise federated principals exactly like static ones.

### 5.5 Actor identity, tenancy, and `ResolvedActor` population

| Field | Static (today) | OIDC human | OIDC M2M | OIDC agent |
|---|---|---|---|---|
| `actor_id` | config map | `omnigraph_actor` claim else `sub` (`user_01…`) | `sub` (client/service id) | `sub` (`agent_reg_…`) |
| `tenant_id` | `None` | `Some(org_id)` | `Some(org_id)` | `Some(org_id)` |
| `scopes` | `[Full]` | `[Full]` (Cedar is the gate) | `[Full]` | `[Full]` |
| `source` | `Static` | `Oidc` (new variant) | `Oidc` | `Oidc` |

- **`TenantId` charset must widen** from `^[a-zA-Z0-9-]{1,64}$` to admit `_` (WorkOS ids are
  underscore-delimited). This is a validation-found prerequisite; the regex is also mirrored in
  `GraphId` — only `TenantId` widens, `GraphId` is untouched.
- **Org binding:** with `expected_org` configured, a token whose `org_id` differs is rejected at
  verification — a cluster serves one organization. `tenant_id` is *recorded* on the actor (the
  reserved Cloud seam) but does not route anything in cluster mode.
- Actor ids flow to commits/audit exactly as today (the `_as` writer surface takes the
  `actor_id` string) — a federated write is attributed to `user_01…`/the pinned
  `omnigraph_actor` with zero writer changes.

### 5.6 Discovery — RFC 9728 + `WWW-Authenticate`

- New unauthenticated route `GET /.well-known/oauth-protected-resource` returning
  `{ "resource": "<server base URL>", "authorization_servers": ["<issuer(s)>"],
  "bearer_methods_supported": ["header"] }`, derived from `auth:` config; served only when
  `auth.oidc` is configured (a static-only server advertises nothing).
- `impl IntoResponse for ApiError` attaches
  `WWW-Authenticate: Bearer resource_metadata="<base>/.well-known/oauth-protected-resource"` to
  every 401 when OIDC is configured — the exact hook RFC-003 §5.8 reserved so a compliant MCP
  client auto-negotiates OAuth on the same endpoint URL with zero client-side branching.
- Both live in `omnigraph-server` (transport boundary — engine invariant 11); the compiler/engine
  kernel stays transport-free.

### 5.7 Agents and delegation (phased)

Agent tokens verify through the same path (`aud` present by default; `sub = agent_reg_…`). The
**`act.sub`** delegation claim names the authorizing person. Phasing, honestly:

- **This RFC:** verify agent tokens; actor = agent `sub`; claim-derived groups gate them like any
  principal (map *untrusted* pre-claim agents to read-only groups via `groups.map`). The `act.sub`
  value is parsed and carried on an extended `ResolvedActor` field (`delegated_by:
  Option<Arc<str>>`, `None` everywhere else).
- **Deferred (own ticket):** dual attribution in commits/audit. The engine's `_as` surface carries
  a single actor string today; recording "agent X authorized by person Y" as a first-class audit
  fact is a commit-metadata change that must not ride an auth RFC. Until then, delegation is
  visible in server logs only — flagged as a known gap, not silently claimed.

### 5.9 Deployment topologies — one verifier, three value-sets

The same `OidcJwtVerifier` + config schema serves every deployment model; only the *values*
change — whose issuer is trusted, where JWKS comes from, and what fences the tenant. No forks, no
deployment-specific code (the RFC-003 requirement "on-prem and cloud from one endpoint, switched
by configuration only", made concrete):

| | Managed cloud | BYOC (customer VPC, our software) | On-prem / air-gapped |
|---|---|---|---|
| Authorization server | **Our WorkOS environment** (one per stage) | Ours by default; optionally theirs | **Customer IdP directly** — or none |
| Customer IdP's role | Federates *into* our WorkOS (per-org SSO/SCIM via Admin Portal) | Same, or is the issuer itself | Is the issuer itself |
| Tenant fence | `expected_org` = the customer's WorkOS org | Same | None needed |
| `aud` binding | Per-cluster Resource Indicator | Same | Optional (their IdP's audience) |
| JWKS reachability | Outbound HTTPS (cached; request path offline) | Outbound-only egress, or `file://` snapshot | LAN-internal IdP, `file://`, or static-only |
| WorkOS account | Ours | Ours (default) / theirs | **None** |

- **Managed cloud.** One WorkOS environment per stage (separate `client_id` per stage — so a
  staging token cannot verify against prod even before the `aud` check). Every customer is a
  WorkOS **Organization** in that environment; their enterprise IdP connects into *their org*
  (self-serve SSO/SCIM). Role/permission slugs are defined once, environment-wide (they are the
  product's capability vocabulary); orgs assign members via IdP group→role mapping. Each customer
  cluster gets the same fleet-templated `auth:` block with only `audience` (its URL) and
  `expected_org` (its org) varying. `org_id → tenant_id` is recorded on the actor (the reserved
  Cloud seam); true multi-tenant serving (one process, many orgs, `GraphKey::cloud` routing)
  remains deferred Cloud-mode work — cluster-per-customer + `expected_org` is this RFC's model.
- **BYOC.** Identity is control-plane: it stays on our WorkOS by default, with the data plane in
  the customer's VPC — the `auth:` block is byte-identical to managed cloud, and the only
  infrastructure requirement is outbound HTTPS for JWKS refresh (verification is offline per
  request, so there is no availability coupling). Egress-forbidden postures use a `file://` JWKS
  snapshot plus a rotation runbook. Customers who require identity to never leave their tenancy
  point the same verifier at *their* issuer (their own WorkOS account, or their IdP directly) and
  use `groups.from`/`map` to normalize their claims — Cedar rules do not change.
- **On-prem / air-gapped.** The customer's IdP is the issuer; JWKS is LAN-internal or `file://`.
  Zero ModernRelay dependency and no phone-home. Fully air-gapped with no IdP → the static-token
  path alone, unchanged.

One adjacent flag (not this RFC's scope): the **engine** is issuer-agnostic, but the **Tower
console** is WorkOS-coupled today (AuthKit SDK). An on-prem customer wanting the console against
their own IdP needs Tower to grow generic-OIDC support or run their own WorkOS environment — a
console roadmap item recorded in Open Questions.

### 5.10 Invariant check (docs/dev/invariants.md)

- **Inv 11 (transport/auth at the boundary):** all additions live in `omnigraph-server` (+ one
  additive `omnigraph-policy` entry point). ✅
- **Inv 12 (no plaintext retention; server-resolved actor):** JWTs verified-and-dropped;
  actor from signed claims; client-supplied identity ignored (unchanged chokepoint). ✅
- **Deny-list "silent fallback":** JWKS failure at boot = refuse; unknown issuer/bad signature =
  401; unmapped federated identity = Cedar deny. Nothing falls open. ✅
- **Deny-list "state that drifts":** the JWKS cache is derived, refreshable state with the source
  (the issuer) authoritative — probe-and-refresh, never a maintained parallel copy. ✅
- **Hyrum's law:** the 401 body shape is unchanged; the new header and well-known route are
  additive and gated on config.

## Testing

Boundary-matched (inv 14), no live WorkOS in CI — mint test JWTs with a locally generated RSA key
and serve the JWKS from a `file://` fixture:

- **Verifier unit tests** (`omnigraph-server`): accept (valid RS256, right iss/exp), reject —
  wrong issuer, expired (± leeway), wrong signature, `alg` confusion (`none`, HS256), wrong `aud`
  when configured, wrong `org_id` when bound, unknown `kid` (triggers one debounced refresh),
  malformed compact JWS falls through to the static verifier, and a *valid-looking* JWT that
  fails verification is NOT retried against the static table.
- **Policy tests** (`omnigraph-policy`): `authorize_federated` parity — a federated actor with
  groups `[analysts]` gets byte-identical decisions to a static actor rostered in `analysts`
  (allow + deny + branch-scope cases); empty/unmapped groups deny with the unknown-actor shape;
  static-path regression suite unchanged.
- **Server integration** (`crates/omnigraph-server/tests/auth_policy.rs` — the existing suite owns
  this area): boot with `auth.oidc` from a cluster fixture; end-to-end request with a minted JWT →
  200 with the actor attributed in the commit (reuse the existing actor-attribution assertions);
  static token still works in the same boot (SC-007); fail-closed boot when JWKS is unreadable.
- **Discovery:** well-known route content; `WWW-Authenticate` present on 401 iff OIDC configured.
- **OpenAPI:** the well-known route is registered → `openapi.json` regenerates
  (`OMNIGRAPH_UPDATE_OPENAPI=1`, per the repo's drift gate).

## Rollout — phased by risk

1. **PR 1 — `TokenVerifier` extraction** (zero behavior change): trait + `StaticHashTokenVerifier`,
   `require_bearer_auth` re-routed through the chain. Unblocks RFC-003's modular-auth requirement
   independently. Update the `identity.rs` "RFC 0001" doc-comments to cite MR-956/RFC-014.
2. **PR 2 — `OidcJwtVerifier` + `auth:` cluster config + `TenantId` widening** (`jsonwebtoken`
   dep, JWKS cache, fail-closed boot, fixtures-based tests). Verification only — federated actors
   still deny at Cedar (`known_actors`) unless rostered, which is safe/harmless.
3. **PR 3 — `authorize_federated`** (claims→groups, per-request principal injection, `policy
   test/explain --groups`). This is the moment federated identities become usable.
4. **PR 4 — discovery** (well-known route + `WWW-Authenticate` + OpenAPI regen).
5. **PR 5 — agent `act` parsing + `delegated_by`** on `ResolvedActor` (logging-level attribution;
   the audit-surface change is ticketed separately).

Docs land with each PR per the maintenance contract: `docs/user/operations/server.md` (auth
model), `policy.md` (federated principals + `--groups`), `docs/user/clusters/index.md`
(`auth:` section), and the capability matrix row.

## Migration / backwards compatibility

- Static-token deployments: nothing changes — no new env vars, same 401 body, same boot.
- `cluster.yaml` without `auth:`: parses as today (the section is optional).
- Enabling OIDC is purely additive: existing static tokens keep working beside it.
- Rollback = remove the `auth:` section; no on-disk or wire format is touched.

## Open Questions

1. **Dual attribution surface** (agent + authorizing person) — where does `act.sub` land in commit
   metadata/audit rows? Needs its own design; §5.7 phases it out of this RFC.
2. **Capability-introspection endpoint** — required by the console (spec 002 US-3); builds on
   `authorize_federated` (same inputs) but is its own additive route + ticket.
3. **`omnigraph_actor` JWT template** — optional actor pinning; template syntax flagged unverified
   in the design doc's validation log. Ship with `sub` default; verify before documenting the
   template path.
4. **JWKS cache tunables** — TTL (15 min default) and unknown-`kid` refresh floor; expose in
   `auth:` config or keep as constants until an operator needs them? Lean: constants first
   (Hyrum's law — every exposed knob becomes a contract).
5. **Refresh-on-401 UX for long-lived server processes** whose clock drifts — rely on `exp` leeway
   or add an explicit skew metric? Lean: leeway only, observable via a rejected-token counter.
6. **Console (Tower) issuer coupling** — the engine accepts any OIDC issuer (§5.9), but the Tower
   console authenticates exclusively through WorkOS AuthKit today. Generic-OIDC console sign-in
   (for on-prem customers who bring their own IdP and want the console) is a Tower roadmap item,
   tracked outside this RFC.
