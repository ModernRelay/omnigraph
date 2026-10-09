# 01 · Server integration

Supporting material for [Studio: a local graph UI served by the CLI](../../../2026-10-02-studio.md#4-design). This is a proposed implementation, subject to the RFC's acceptance and boundaries.

## 1. Scope

Allow boot mode to mount static UI routes on the existing server runtime and learn its actual listening address. Preserve the standalone server's startup, authorization, request limits, readiness, and bounded shutdown behavior. Studio reads and conditional mutations both use existing protected graph routes.

**Source checked:** `crates/omnigraph-server/src/lib.rs` on upstream `main` at `29ad30dd`, 2026.10.02. These are source observations, not executed integration tests:

- `build_app(state)` is already public and returns an Axum router. App construction does not itself provide the full boot and shutdown lifecycle.
- `serve(config)` and `serve_with_data_token_trust(config)` delegate to private `serve_config`.
- That function binds the listener, obtains `local_addr`, prints a listen-address line, and runs the app with the existing shutdown machinery.
- Protected graph routes enforce bearer authentication and the HTTP API contract before graph resolution. The default request body limit is private to the server crate.

## 2. Proposed implementation

Add a serving entry point with two explicit inputs beyond configuration: the additional router and a one-shot sender for the bound `SocketAddr`. Call it `serve_with_routes`; its final Rust signature is settled with the implementation PR.

The entry point follows the existing `serve_config` lifecycle. Construct the combined app, bind the listener, and send the actual address before entering the serving loop. The address notification means the socket is bound, not that the graph is ready. The CLI separately checks readiness. Startup failure closes the notification channel and is also returned by the serving task.

Existing serving entry points supply an empty extra router and no address notification. Preserve their existing listen-address output and behavior. Extend the common internal path rather than copying startup or shutdown into Studio.

Merge the extra router while retaining the server's body-limit and trace layers. Studio owns `/`, its client-navigation paths under `/g/`, and assets under `/_studio/`. Protected graph routes retain their existing middleware; static routes do not receive graph actor authority.

Conflicting method/path registrations must fail during construction. Axum merge provides no server-route precedence guarantee. Exercise its collision behavior in the integration test and surface failure before advertising a usable URL.

The Studio router has no engine or server dependency. `Router<()>` is an interface choice, not a sandbox: application composition and crate dependencies must establish that the UI uses only HTTP for graph access.

## 3. Assumptions to verify

- Choose the exact insertion point without losing shared layers, fallback behavior, managed-token trust, boot witnesses, or shutdown coverage.
- Verify that graph startup failure, cancellation, and address notification cannot leave an orphan serving task.
- Confirm loopback validation for every supported address family. The CLI must reject a non-loopback address before opening the cluster.
- The RFC's operator responsibility for already-served folders is still a maintainer decision. This plan claims no cross-process serving lock.

## 4. Validation

Extend the existing server test owners and `tests/support` fixtures. Demonstrate static serving, bound-address notification with port zero, protected graph reads and conditional writes with the required contract header, unchanged authentication, explicit route collisions, startup failure, and bounded shutdown.

Run the existing OpenAPI drift check unchanged. The route extension is a Rust integration API; Studio routes do not become documented graph endpoints. Check that boot-mode writes inherit the existing server actor attribution and policy behavior; do not add a browser-supplied trusted actor or invent a Studio actor. Record executed results in the implementation PR. No line-count estimate or claim of unchanged behavior is established by this plan alone.
