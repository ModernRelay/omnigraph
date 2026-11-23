# Offline data-token verification

**Decides:** the server verifies short-lived ES256 data credentials offline against public trust fixed at boot, and derives the actor from the verified principal.
**Contract:** keeps E6
**Code:** `crates/omnigraph-server/src/data_tokens.rs`, `crates/omnigraph-server/src/identity.rs`, `crates/omnigraph-server/src/lib.rs`, `crates/omnigraph-server/src/main.rs`
**Cited as:** RFC 0053

## Context

Static bearer tokens are configured per server and do not expire. A managed
issuer can mint short-lived credentials, but serving must not depend on that
issuer being reachable, and a control-plane session must never be forwarded
to a graph endpoint. Identity, expiry, key custody and the binding to one
storage root are security contracts, so they are fixed in one bounded wire
format instead of an incidental authentication branch.

## Decision

- `omnigraph-server --data-token-trust FILE` reads public trust once at
  boot: at most 64 KiB of JSON with no unknown or duplicate fields, naming
  the canonical root, account, cluster, incarnation, issuer, audience and
  one to four P-256 keys. Each `kid` is the lowercase hex SHA-256 of its
  SPKI DER. An invalid file or key refuses startup.
- `canonical_root` must equal the root the server serves, checked before
  any graph engine opens. The server reads no private managed identity
  marker; the trust supplier owns that binding.
- A credential is a compact JWT of at most 8,192 bytes with header exactly
  `alg: ES256`, `typ: JWT` and `kid`. Its claims must match the trust,
  `aud` is `urn:omnigraph:data:<cluster_id>`, lifetime is 60 to 86,400
  seconds, `now < exp` with no leeway, and `iat <= now + 30`. Kind and
  assurance pair as `human`/`verified_human` or
  `automation`/`verified_workload`.
- Verification does no network I/O and keeps no token database. `jti` is
  provenance, not a replay ledger.
- A configured static token is matched first, by constant-time hash. An
  unmatched token goes to the signed verifier. A failure never re-enters
  static matching and never becomes anonymous.
- Validity is checked at admission; expiry does not cancel an admitted
  request. There is no online revocation. Keys rotate by restarting with
  old and new keys installed together.
- A signed identity never inherits the static-token read default for a
  graph with no policy; it needs an explicit permit
  ([Identity credentials and applied policy](2026-09-09-identity-credentials-and-applied-policy.md)).

## Rejected

- **Online introspection or JWKS fetching:** adds a serving dependency on
  the issuer and cannot keep the offline contract.
- **Signed credentials in each graph's static-token store:** adds rotation
  and expiry state when the credential already binds identity and lifetime.
- **Treating a valid signature as authorization:** bypasses Cedar and grants
  actions and branches the graph owner never allowed.
- **Importing the managed service's identity format into the server:**
  couples the open-source server to one provisioning implementation.
