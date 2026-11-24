# Owned server operations

**Decides:** an admitted HTTP request belongs to the server until its operation returns, whether or not its caller is still waiting; admission bounds how many such operations and how many input bytes the server holds; shutdown waits for them under one absolute deadline; and the CLI reports what a failed command established instead of repeating it.
**Contract:** keeps E4, E5, write:7
**Code:** `crates/omnigraph-server/src/operations.rs`, `crates/omnigraph-server/src/workload.rs`, `crates/omnigraph-server/src/ingress.rs`, `crates/omnigraph-cli/src/command_outcome.rs`

## Context

A write whose future is dropped when its socket closes can stop halfway through its effects, and the caller cannot tell whether it landed. A spawned task with no accounting leaks capacity and lets shutdown exit while work still runs. A caller that retries after any failure can apply a write twice. Joining a task proves only that the task returned, not that the storage requests it sent have settled.

## Decision

- Admission reserves capacity, then registers the operation under one synchronous ordering boundary before spawning it. If admission closed in between, the request refuses and runs nothing.
- The owned future runs once, with immutable inputs, the trusted actor, the session settings and the request's tracing span. Disconnect drops the response waiter, never the operation or its reservations. Every admitted write takes this path, and a merge with optional source deletion has one owner for both effects.
- After body collection, read handlers run in owned tasks too; MCP tool execution keeps its slot past the caller's deadline. A completed result has one bounded delivery slot.
- No timeout wraps the owned engine future. There is no post-disconnect result lookup, so a caller that left can hold an unknown outcome.
- Read and write ingress are reserved separately before a body is collected, and read and write response observers are separate lanes, so slow readers cannot block writes. Registered route identity selects body limits. Operation admission bounds aggregate count and retained bytes, per actor too, with a finite number of actor records. Every acquisition succeeds at once or refuses; there is no request queue. Defaults live in `workload.rs`.
- Admission closes permanently under the same ordering boundary. Shutdown waits for registered writes, read bodies and producers, then exits; the signal and any fatal failure share one deadline and watchdog, and a cutoff exits non-zero.
- The engine supplies completion evidence (`BeforeEffect`, `Unpublished`, `Uncertain`) apart from the typed cause. Only an audited owning boundary mints a pre-effect proof, and earlier effects strip it from a later sub-operation. Validation refusals stay non-fatal; status codes and error text never prove anything.
- An unknown storage outcome answers `outcome_unknown` and the server keeps serving, as [storage outcomes per operation](2026-10-19-storage-outcomes-and-restartable-ownership.md) decides. A write panic, a lost owner, `RecoveryRequired`, an opaque DataFusion failure or an uncertain activation closes admission for every graph and exits 2 once the remaining known owners finish, keeping unresolved reservations charged until exit.
- A failed CLI data write carries `command_outcome { execution, effects, action }` for the whole invocation, so a final refusal cannot hide earlier effects. Exit 75 means a verified typed 429 before admission; exit 4 a verified 412 with no earlier effect; exit 1 anything else, including unknown delivery. The CLI never retries automatically.

## Rejected

- **Keep the request future alive only while its socket is connected:** preserves the partial-effect bug.
- **Spawn the operation without bounded ownership:** leaks admission accounting and lets shutdown exit early.
- **Treat a task join as a storage drain:** would permit unsafe runtime replacement, reclamation or retry on a false completion claim.
- **Infer retry safety from HTTP status or error text:** neither proves the whole command had no effect.
