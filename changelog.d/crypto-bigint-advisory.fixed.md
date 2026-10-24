- Remove `crypto-bigint` 0.5, which RUSTSEC-2026-0387 reports for timing side
  channels. Data-token verification now uses `p256` 0.14, and OIDC tokens are
  verified through `aws-lc-rs`. The server only verifies these signatures, and
  the tokens it accepts are unchanged.
