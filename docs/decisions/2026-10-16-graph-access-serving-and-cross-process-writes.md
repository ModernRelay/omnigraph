# Graph access and cross-process writes

**Decides:** a caller names a target and an actor, every surface shares one operation list and one failure shape, the cluster-backed server is the only serving model, and cross-process correctness comes from control facts on the `__manifest` version CAS, never from a process that routes writes.
**Contract:** keeps E4, E6
**Code:** `crates/omnigraph-api-types/src/lib.rs`, `crates/omnigraph-api-types/src/operations.rs`, `crates/omnigraph-server/src/http_contract.rs`, `crates/omnigraph-cli/src/command_outcome.rs`, `crates/omnigraph-cli/src/commands/mcp`, `crates/omnigraph-cli/src/commands/studio`, `crates/omnigraph-catalog/src/branch_rows.rs`, `crates/omnigraph-catalog/src/publisher.rs`

## Context

People, agents and Studio reach one graph through the CLI, HTTP and MCP. Without one contract, each surface grows its own addressing, error shapes and exit codes, a caller cannot state which actor it expects a server to use, and a client learns its permissions from refusals. Lance refs have no conditional update or delete, so a branch or proposal control step decided by a ref write or a process-local gate holds only inside one process. A second serving mode for standalone graphs would double the boot, policy and admission contract.

## Decision

- The ordinary cluster-backed `omnigraph-server` is the only HTTP serving model, on localhost as anywhere. No command starts, discovers or hosts a server. `omnigraph studio` and `omnigraph mcp` connect to an existing server target and refuse direct targets with `target_kind_unsupported`.
- On direct access the caller declares its actor (`--as`, else `operator.actor`). On served access the server resolves the actor from the credential, and `--as` sends `Omnigraph-If-Actor`; a mismatch is `412 actor_mismatch` before graph lookup or effects. The header only narrows.
- Every failure is `ErrorOutput` with a `code` from the one `ErrorCode` registry and a `disposition`: `effects` (`none`, `partial`, `unknown`) and `action` (`fix`, `refresh`, `retry`, `recover`, `reconcile`). A write whose publication has no definite answer is `outcome_unknown`, never `internal`, and is never replayed.
- CLI exits follow the disposition: 0 success, 1 no effects, 2 effects unknown or partial, 4 a caller-set condition failed, 64 usage, 75 retry later.
- One table in `operations.rs` binds each operation to its route, MCP tool and command, or names the reason it has none.
- `omnigraph access` reports the caller's actor and the actions policy grants, from the server's access route or, for a direct target, from the installed policy.
- The MCP bridge is bound to one graph and admits only `readOnlyHint` tools without `--write`; the server's policy stays the ceiling.
- A control fact is a `__manifest` row that changes no table row and no schema and records no graph commit. Each carries the attempt id its operation minted once, so a lost acknowledgement reads back as landed, lost or unknown, and a retry treats its own fact as landed.
- A branch exists when main's branch row names it, and a branch delete first publishes a tombstone on the branch's own chain. Publishers refuse `branch_exists` and `branch_deleted` on every attempt. The row format is [branch existence in the catalog](2026-11-19-branch-row-in-the-catalog.md).

## Rejected

- **A second, local serving model for standalone graphs:** generated policies and tokens, separate boot and shutdown, and process discovery are another contract to keep, and a localhost cluster already serves.
- **A daemon every CLI write goes through:** correctness across machines still belongs to the engine, and a daemon adds lifecycle and version skew.
- **A cooperative machine-local writer lock:** it protects only cooperating writers on one machine.
- **A CLI-only refusal field beside the HTTP `code`:** two code namespaces for one failure.
- **Learning access from a 403:** a failed write is too late to learn a limit.

## Not built

- The drop record. `commit drop` against `commit publish` is still decided under the process's branch gate, so two processes can both acknowledge; `commit_dropped` and `commit_published` do not exist.
- `disposition.check`, the branch, head and publication id that settle an unknown write by a later read.
- Branch create still takes the exclusive schema gate, and schema apply, branch control, proposal drop and cleanup keep the one-writer-process boundary of [invariants](../dev/invariants.md#support-boundaries). Lifting it would close G2.
