# RFC 0001 — Federated Authentication (Cloud Control Plane + VPC/On-Prem)

**Type:** design proposal
**Status:** draft — not accepted, not implemented
**Audience:** maintainers reviewing the auth substrate and the OSS/Cloud boundary
**Date:** 2026-05-16
**Supersedes:** nothing — extends the current bearer-token model

> This is a proposal, not current truth. Until accepted and implemented, the
> authoritative description of auth remains [docs/user/server.md](../../user/server.md)
> and [docs/user/policy.md](../../user/policy.md).

## Summary

Add OIDC-based federated authentication to `omnigraph-server` so that a
managed cloud offering can issue identity tokens, while VPC and air-gapped
on-prem deployments keep working with **no request-time dependency** on the
cloud. The mechanism is a new `TokenVerifier` seam in `omnigraph-server` with
two OSS implementations: today's static bearer tokens, and an OIDC JWT
verifier. The cloud offering is configuration of the OSS verifier plus an
additive, optional control-plane sync component — not a fork.

## Motivation

The current model (`omnigraph-server/src/auth.rs`, `lib.rs`) hashes a static
set of bearer tokens at startup and compares per request. It is correct and
on-prem-friendly, but it cannot:

- accept identities issued by an enterprise IdP (Okta, Entra, Google) or by an
  OmniGraph cloud control plane;
- support short-lived, rotating credentials;
- feed an actor allowlist from an external source (e.g. SCIM).

We want a cloud offering with managed identity **without** sacrificing the VPC
/ on-prem deployment story or violating the OSS/Cloud invariants in
[invariants.md](../invariants.md).

## Goals

- One engine binary; deployment mode is configuration only.
- Token **validation** is fully OSS and works offline (no control-plane call
  on the request path).
- Cloud control plane *issues* tokens and *distributes* config; it never
  becomes a correctness dependency of the data plane.
- Static bearer tokens remain a first-class, default-capable path for
  machine-to-machine (M2M), CI, and air-gapped use.
- Cedar authorization (`policy.rs`) is unchanged — it still operates on a
  server-resolved actor.

## Non-goals

- Building an OmniGraph identity provider. The cloud control plane may wrap a
  third party (e.g. WorkOS) for human SSO; that is out of scope here.
- Browser login / session UX. This RFC covers API/CLI credential verification.
- Changing the engine crates. `omnigraph` and `omnigraph-compiler` stay free
  of transport/auth code (Invariant 11).

## Background — current state

| Concern | Today | File |
|---|---|---|
| Token store | SHA-256 hashes of static tokens | `omnigraph-server/src/lib.rs:58` |
| Comparison | constant-time over all entries | `lib.rs:286` |
| Token sources | env / file / AWS Secrets Manager | `auth.rs` (`TokenSource` trait) |
| Actor | `AuthenticatedActor(Arc<str>)`, server-resolved | `lib.rs:135` |
| Authz | Cedar, 8 actions, branch scopes | `policy.rs` |

`TokenSource` yields a *static set to hash*. OIDC needs a per-request
*validation* step instead, so a new seam is required rather than a new
`TokenSource` impl.

## Design

### Control plane / data plane split

The decoupling that makes VPC + on-prem work: **token validation never makes a
network call.** OIDC tokens are signed by the issuer; the verifier needs only
the issuer's public keys (JWKS). The data plane validates offline against
cached JWKS.

```
                 CLOUD CONTROL PLANE (SaaS only, optional)
                 - issues tokens (own IdP or WorkOS front)
                 - publishes signed config bundle:
                     { jwks, cedar_policy, actor_allowlist }
                          │  (pull, periodic)
                          ▼
   ┌─────────────────────────────────────────────────────────┐
   │  DATA PLANE  — omnigraph-server + engine                 │
   │  (identical binary in SaaS / VPC / on-prem)              │
   │                                                          │
   │  request ─▶ TokenVerifier ─▶ AuthenticatedActor ─▶ Cedar │
   │             reads ONLY local cached state                │
   └─────────────────────────────────────────────────────────┘
```

In air-gapped mode the bundle is supplied as static files; the control-plane
sync component is simply not configured. The request path is byte-identical in
all three modes.

### The `TokenVerifier` seam

Lives entirely in `omnigraph-server` (Invariant 11).

```rust
/// Verifies an inbound bearer credential and resolves it to an actor.
trait TokenVerifier: Send + Sync {
    async fn verify(&self, presented: &str) -> Result<ResolvedActor, AuthError>;
}

struct ResolvedActor { actor_id: Arc<str>, source: AuthSource }
```

OSS implementations:

- `StaticHashTokenVerifier` — current behavior, refactored behind the trait.
  Constant-time hash compare. Default.
- `OidcJwtVerifier` — validates JWT signature against cached JWKS, checks
  `iss` / `aud` / `exp` (with bounded clock skew), maps a configured claim to
  `actor_id`, optionally checks an allowlist.

`require_bearer_auth()` dispatches to the configured verifier(s) and produces
the same `AuthenticatedActor` the rest of the stack already consumes. Nothing
downstream — Cedar, `mutate_as`, the commit actor map — changes.

The "cloud verifier" is **not new code**: it is `OidcJwtVerifier` pointed at
the OmniGraph cloud issuer.

### Configuration

```toml
[auth]
mode = "static"        # "static" | "oidc" | "hybrid"

[auth.oidc]
issuer        = "https://auth.omnigraph.cloud/"  # or a customer IdP
audience      = "omnigraph"
actor_claim   = "sub"                            # claim -> actor_id
jwks_uri      = ""                               # blank = OIDC discovery
jwks_cache_ttl   = "1h"
jwks_offline_path = "/etc/omnigraph/jwks.json"   # air-gapped pre-seed
jwks_stale_max    = "24h"                        # see degraded mode
clock_skew        = "60s"
allowed_actors_path = ""                         # optional; SCIM-fed in cloud

[auth.control_plane]                              # SaaS only; omit elsewhere
bundle_url      = "https://cp.omnigraph.cloud/v1/bundle"
sync_interval   = "5m"
```

`hybrid` mode runs both verifiers (static tried first, then OIDC), so M2M
service tokens and human/federated identities coexist during and after
migration.

### Control-plane sync (additive, cloud-only)

An optional `ControlPlaneSync` task periodically pulls a **signed** bundle
(`jwks`, `cedar_policy`, `actor_allowlist`), verifies its signature, and writes
it to the same local paths the verifier and `policy.rs` already read. It is a
distribution mechanism, not a code path the request touches — preserving the
"no fork for Cloud" invariant.

### Degraded-mode behavior

VPC deployments must tolerate brief control-plane / IdP unreachability:

- **JWKS refresh failure** — keep serving on cached keys; emit a loud
  `WARN` + metric. Past `jwks_stale_max`, fail closed. Cached JWKS is safe
  because signing keys rotate slowly.
- **Revocation** — JWT revocation is inherently weak. Mitigate with short token
  TTL (Databricks Lakebase uses 1h; we recommend ≤1h for cloud-issued tokens)
  rather than a request-time denylist lookup. Optional opaque-token
  introspection is left as future work, not a default.
- **Control-plane bundle staleness** — last-good bundle stays in effect; loud
  warning. Never silently fail open or drop to a weaker policy.

This satisfies the deny-list "no silent failures" and Invariant 6 (any
degraded mode is explicit, bounded, observable).

### M2M for VPC

In-VPC service-to-service and CI clients must not depend on the cloud at all.
`StaticHashTokenVerifier` remains the supported M2M path (analogous to
Lakebase's indefinitely-lived service principals). `hybrid` mode lets a
deployment serve static service tokens and OIDC human tokens simultaneously.

### Authorization interaction

Unchanged. Cedar (`policy.rs`) receives the resolved actor regardless of which
verifier produced it. The control-plane bundle may *distribute* the Cedar
policy and actor allowlist, but enforcement, scopes, and the 8 actions are
exactly as they are today.

## Invariant analysis

| Invariant / deny-list item | Outcome |
|---|---|
| 11 — transport/auth at the boundary | ✅ `TokenVerifier` is server-only; engine untouched |
| 12 — bearer plaintext not retained | ✅ JWT verified per request, not stored; static path keeps constant-time compare |
| 6 — strong consistency default | ✅ degraded mode is explicit, bounded, non-default |
| Deny: cloud-only correctness fix | ✅ verification is OSS; cloud only issues + distributes |
| Deny: fork the codebase for Cloud | ✅ cloud verifier = config; `ControlPlaneSync` is additive/optional |
| Deny: silent failure | ✅ JWKS/bundle staleness is loud + metered, fails closed at a bound |
| Deny: side-channel for semantics | ✅ actor stays a first-class server-resolved value |

## Open questions — decisions needed before acceptance

1. **Degraded grace window.** Is `jwks_stale_max = 24h` the right default, and
   is fail-closed-after-bound acceptable for VPC SLAs?
2. **Revocation.** Short TTL only, or do we also ship optional introspection /
   a denylist for high-security tenants?
3. **Policy authority for VPC.** Can a VPC customer override a cloud-pushed
   Cedar policy locally, or is the cloud bundle authoritative? Security and
   product implications.
4. **Unknown-actor handling.** When `actor_claim` resolves to an actor absent
   from the allowlist: reject at `verify()`, or pass through and let Cedar deny
   (with a default-deny rule)?
5. **Multi-issuer.** Does `hybrid` need to validate against more than one OIDC
   issuer simultaneously (cloud issuer + customer IdP)?
6. **Bundle signing.** What signs the control-plane bundle, and how is that
   root of trust provisioned to an air-gapped install?

## Rollout

1. Land `TokenVerifier` + `StaticHashTokenVerifier` as a pure refactor —
   `mode = "static"` is the default, behavior identical. (Separate commit, per
   AGENTS.md rule 11.)
2. Add `OidcJwtVerifier` + JWKS cache; `mode = "oidc"` / `"hybrid"` opt-in.
3. Add `ControlPlaneSync` as an optional component.
4. Update [docs/user/server.md](../../user/server.md),
   [docs/user/policy.md](../../user/policy.md), and
   [docs/user/deployment.md](../../user/deployment.md) in the same changes.

No breaking change: existing static-token deployments keep working untouched.

## Alternatives considered

- **Mandatory cloud control plane (Lakebase model).** Rejected — Lakebase
  requires the Databricks control plane reachable, which kills the air-gapped
  on-prem story and would put correctness behind a cloud service (deny-list:
  cloud-only correctness).
- **Opaque tokens + request-time introspection.** Rejected as the default —
  adds request-path egress to the issuer, defeating the VPC decoupling. Kept as
  possible future opt-in for revocation-sensitive tenants.
- **Build an OmniGraph IdP.** Rejected — not our domain; delegate issuance to
  an OIDC provider or a thin cloud control plane that may wrap WorkOS.

## Testing

- Extend `crates/omnigraph-server/tests/server.rs` with `TokenVerifier`
  coverage; add a focused auth test module.
- OIDC path: test harness signs JWTs with a local key; assert accept / expired
  / bad-audience / bad-signature / unknown-actor.
- Degraded mode: exercise JWKS-unreachable via the `failpoints` feature; assert
  cached-key serving, the loud warning, and fail-closed past `jwks_stale_max`.
- Confirm `omnigraph-server` remains the only crate with auth dependencies.
