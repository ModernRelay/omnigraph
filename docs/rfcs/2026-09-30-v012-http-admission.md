---
rfc: "2026-09-30-v012-http-admission"
title: "v0.12 HTTP admission"
track: maintainer
status: accepted
implementation: complete
authors:
  - OmniGraph maintainers
created: 2026-09-30
updated: 2026-09-30
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: v0.12 HTTP admission

## Decision

Implement A1 of [Server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md#rollout): one explicit HTTP contract, checked before graph access. Its remaining outcome and lifecycle increments stay separate proposals. This decision changes neither engine publication nor storage formats.

The request and response header is `Omnigraph-Http-Api: 0.12`. Header names are case-insensitive; the value must be exactly `0.12`, occurring once. Missing, duplicate, combined or unsupported values refuse. This identifies the v0.12 HTTP contract, independently of the package version and internal manifest stamp. Deploy CLI, server and integrations from a qualified build together; the identifier does not attest that other server increments are implemented.

## Admission and discovery

Protected OmniGraph graph and registry HTTP routes authenticate, then validate the header before graph resolution, request-body decoding or execution. Refusal is HTTP 400 with the existing `ErrorOutput` and `code: api_contract_mismatch`. The diagnostic names the supported header without echoing untrusted values. Authentication retains its existing refusal precedence.

Every ordinary HTTP response carries the contract header, including refusals, streamed results and public `GET`/`HEAD /healthz`, `/readyz` and `/openapi.json` responses. These public routes do not require the header. `/healthz` responses use `Cache-Control: no-store` for discovery; no additional endpoint or compatibility registry is introduced. MCP and OAuth metadata retain their separately negotiated standard protocols and are outside this header contract.

Before each graph or registry request, the CLI probes the configured server's public `HEAD /healthz` without bearer credentials, preserving any reverse-proxy path prefix. Discovery has a five-second bound, does not consume the response body, and requires a successful response with the exact single contract header. Failure prevents data dispatch. The request then carries the header, and the CLI checks the response header before decoding or emitting any JSON, Arrow, export, feed or Blob body. This adds one discovery round trip per data request and no persistent capability cache.

Graph HTTP clients disable redirects and automatic retries. This includes raw streaming and NDJSON paths; managed control-plane and OAuth clients retain their independent protocols. Data-request deadlines and response limits remain unchanged; the discovery bound is additional. The server admission check is still necessary: discovery is not a fence against a changed backend or a proxy dropping a header.

A missing or incompatible data response header is a protocol failure with the HTTP status retained and effects classified as unknown. It does not prove non-execution or authorize replay. Preflight failures instead report that the data request was not sent. Errors preserve that distinction in human, JSON and JSONL output. A transport failure after dispatch keeps the existing uncertain-outcome rules. No automatic fallback, retry, data idempotency or exact merge receipt is added.

## Compatibility and invariants

This is an intentional v0.12 breaking boundary. Old clients without the header are refused by new servers; new CLI builds refuse old servers during discovery. Operators upgrade consumers together and configure proxies to preserve the header in both directions. Reverting requires reverting clients and server together; no stored state needs conversion.

Configured server URLs identify the service root, including any proxy prefix. Replace graph-qualified server URLs with the root plus `--graph`, a profile's `default_graph` or an alias's `graph`. The CLI never guesses the root by stripping a `/graphs` segment, which may belong to the proxy prefix.

Bearer identity and engine authorization remain authoritative. The header is a protocol discriminator, never authentication or permission. Admission touches no graph state, and response mismatch cannot manufacture a no-effect claim. Shared wire constants, the existing error envelope and the existing OpenAPI owner are sufficient; no surface hashes, version negotiation or compatibility aliases are needed. No Lance behavior changes or new substrate assumption is introduced.

## Evidence and rollout

Extend the existing server `auth_policy`, `data_routes` and `openapi` owners, and CLI client/helper tests plus `cli_data` and `system_remote`. Rust tests are required because these assertions concern HTTP headers, raw streams and dispatch counts, not GQ row semantics.

Required evidence: missing/mismatched/duplicate headers refuse before graph effects; authentication precedence and public discovery hold; valid requests work; ordinary error and streaming responses carry the header; failed CLI discovery sends no data request; a bad response after dispatch emits no success body and reports unknown effects; redirects/retries do not create extra submissions; proxy prefixes and managed data routes remain correct. Regenerate OpenAPI and keep existing server/CLI suites green.

A1 lands with coordinated client/server code and user migration guidance. It does not complete increment A, enable online deployment or make admitted writes survive disconnect. Those remain with the umbrella RFC. Qualified tests close this RFC's implementation gate.

Qualification passed in the server/API/CLI owners, all 14 loopback system cases, the AWS-feature server suite and generated OpenAPI checks. Live S3/Azure fixtures were not configured; this evidence qualifies the HTTP boundary, not a cloud deployment.

## Decision log

- 2026-09-30: Accepted the narrowly scoped A1 implementation on the maintainer's instruction to build the first slice. Fixed the header, refusal, discovery and transport rules; the broader server runtime RFC remains draft. Chose explicit refusal over package-version inference or old-client fallback.
