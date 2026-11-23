# Self-contained server testing

**Decides:** a GQT case that declares the `omnigraph-server` environment runs through a real HTTP server the harness provisions itself, under the case's own expectations, and its served verdict must equal its in-process verdict.
**Contract:** none; serves invariant 13
**Code:** `crates/omnigraph-gqt-core/src/runner_config.rs`, `crates/omnigraph-gqt/src/dst_runner.rs`, `crates/omnigraph-gqt-served/tests/gqt_served_conformance.rs`, `crates/omnigraph-gqt-served/tests/gqt_served_count.rs`, `crates/omnigraph-server/tests`

## Context

A test through the engine alone misses the server's admission,
authorization, request bodies, rendering and shutdown. Separate Rust server
suites repeat the scenario setup each time and diverge from the query corpus.
A second server implementation, or a separate scenario runner, would make
server and engine tests disagree about what they test.

## Decision

- A case's environment names its route: `omnigraph-engine`,
  `omnigraph-engine-dst`, `omnigraph-server` or `omnigraph-server-dst`. One
  file keeps the same sections and expectations on every route.
- The served conformance test initializes a fresh graph with the case's
  schema, boots the production router (`build_app`) under `axum::serve` on a
  loopback socket, unauthenticated and with no policy, and loads the seed
  through `/load`. Each step goes to the route that already serves it.
- The in-process verdict must be green and the served verdict must equal it,
  so a failure on both sides is a failure, never an agreement. A case with no
  server environment is skipped by name, and an empty served set is refused.
- The server calls the real engine in process. There is no scripted engine,
  no test-control endpoint and no feature-gated server build.
- What the wire cannot carry is refused by name before any request:
  `--- restart`, `--- concurrent`, seam directives, settings steps,
  `--- expect plan`, `--- expect same as v1`, the traversal and writer pins,
  and index-backed searches. `--- expect shape` is not judged, since it
  compares the executor's Arrow schema.
- `omnigraph-server-dst` is refused at admission; deterministic server
  execution is a separate design.
- `--server <URL> --graph <ID>` runs the declared server environments against
  an existing server. It excludes `--store`, never blesses and does not
  replay, since the server's state is not frozen in the report.

## Rejected

- **Only Rust server suites:** they repeat scenario setup and give no common
  authored test interface; they stay as owners until a case detects the same
  defect.
- **A manually started server as the default:** it cannot guarantee fresh
  configuration, exclusive state or teardown.
- **The router alone as server evidence:** `Router::oneshot` skips the
  connection loop, HTTP framing and cancellation; it stays component
  evidence in the server route suites.
- **A substitute engine by default:** it can pass a drain test while real
  storage work survives.

## Not built

- Step headers that name the API a step takes (`via http`, `via mcp`,
  `--- cli`) against one owned server.
- The server as a separate process owned by the GQT supervisor from spawn,
  with process-group containment.
- Lifecycle steps: kill, process restart, requested shutdown, and a
  read-only `via engine` physical read after containment.
- An executed result-type witness on the HTTP read response, so
  `--- expect shape` is judged over the wire.
