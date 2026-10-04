# 02 · CLI lifecycle and proxy

Supporting material for [Studio: a local graph UI served by the CLI](../../../2026-10-02-studio.md#3-usage). This is a proposed implementation, subject to the RFC's acceptance and boundaries.

## 1. Scope

Add a `studio` subcommand with inspection and editing in local boot and remote attach modes. The CLI owns the listener lifecycle and remote credential. Attach-mode writes use the actor resolved by the remote server; boot-mode writes inherit the existing server runtime attribution.

**Source checked:** server serving code, CLI `helpers.rs`, and `openapi.json` on upstream `main` at `29ad30dd`, 2026.10.02. No Studio command has been implemented or executed.

## 2. Proposed implementation

```text
omnigraph studio --cluster <dir> [--graph <id>] [--bind <addr>] [--no-open]
omnigraph studio --server <name|url> [--graph <id>] [--bind <addr>] [--no-open]
```

| Input | Proposed behavior |
|---|---|
| `--cluster` / `--server` | Exactly one is required. Boot an applied local cluster folder or attach to a server. |
| `--graph` | Select the graph opened first; otherwise display the graph list. |
| `--bind` | Default `127.0.0.1:0`; refuse any non-loopback address before startup. |
| `--no-open` | Print the URL without launching a browser. The URL is always printed. |

Do not add `--as`. Confirm how the new options interact with global addressing flags in Clap; reject conflicting inputs instead of silently ignoring them.

### Boot mode

1. Validate arguments and loopback binding.
2. Load configuration through the existing server settings path, explicitly selecting its unauthenticated local runtime. Preserve errors for missing applied state and graph availability.
3. Build the static Studio router and spawn the proposed server entry point from [plan 1](01-server-seam.md#2-proposed-implementation).
4. Receive the bound address from its one-shot notification. Monitor the serving task concurrently for startup failure.
5. Check readiness with a bounded deadline, then print and optionally open the URL. Proposed deadline: five seconds, subject to startup measurements; cancel startup cleanly when it expires.
6. Keep the process alive until shutdown and use the server's existing bounded shutdown path. Stop any booted task on failure as well as normal exit.

The banner names the folder and states that attach mode is required when another server already serves it. Local configuration directories can reference remote storage; validate effective roots before advertising a local-only support boundary.

### Attach mode

Resolve the server root with `resolve_server_flag(Some(server), None)`. Passing `--graph` into this helper appends `/graphs/{id}` and would produce the wrong proxy root. Keep graph selection in browser navigation. Resolve the token against the server root using `resolve_remote_bearer_token`.

Serve UI routes and an explicit API allowlist. Initial allowed operations are `GET /graphs`, `/healthz`, `/readyz`, `/openapi.json`; graph-scoped `GET /schema`, `/snapshot`, `/branches`, `/commits`; and graph-scoped `POST /query` and `POST /mutate/if-graph-commit`. Preserve the edit's `Omnigraph-If-Graph-Commit` header and branch body unchanged. Extend this list explicitly for later read routes. Reject unconditional `/mutate`, its aliases, and other unmatched requests locally. Validate the decoded path and upstream origin; disable redirects so credentials cannot follow a response to another host.

Preserve allowed query strings, request bodies, upstream status, and useful response headers. Strip incoming authorization and hop-by-hop headers in both directions, including headers named by `Connection`. Add the configured credential and the supported HTTP contract header. Upstream at the surveyed commit requires `omnigraph-http-api: 0.12` for graph API requests; use the project's contract authority rather than independently maintaining this value.

Proposed proxy limits are a 1 MiB request body and a 30-second total request deadline, including response streaming. Do not claim access to the server's private limit constant. Stream bodies with limits enforced, perform no automatic retry, and show transport failures explicitly. An upstream error response retains its status and body, including conditional-mutation `412` responses. A timeout or dropped connection after write submission may leave the outcome unknown: surface that state and require a fresh read and deliberate reconciliation, never automatic resubmission.

## 3. Assumptions to verify

- Check readiness response shape, partial graph availability, and startup cancellation against the current server settings path.
- Establish typed handling of HTTP contract mismatch; do not silently negotiate or fall back to another API.
- With no configured credential, omit authorization and display the server's response. Unknown server names fail before serving; browser-launch failure leaves the printed URL usable.
- Review and implement loopback-origin protections for the credential-bearing proxy and boot listener, including Host and Origin validation on writes. Loopback binding alone does not establish which browser page initiated a request.
- Select a browser-opening dependency under the existing dependency and license checks.

## 4. Validation

Extend existing CLI fixtures and test owners. Cover both modes, graph selection without duplicating the upstream path, ephemeral-port startup, errors, shutdown, explicit allowlisting, contract headers, token handling, redirect refusal, body bounds, deadlines, and origin checks. Cover a successful conditional edit, a stale edit returning `412` with no effect, preservation of the branch and precondition, refusal of unconditional writes, and an uncertain transport outcome without replay.

Verify that remote errors remain visible and credentials do not appear in URLs, startup output, static assets, or browser-facing responses. These are required outcomes, not tests already run.
