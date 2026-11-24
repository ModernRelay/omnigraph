# Bulk load as a query plan

**Decides:** a `load csv` statement lowers to a physical plan (source, validation, fragment write) that the engine's executor runs and `explain` prints, with each validation check's access path chosen by cost and the whole statement's tracked memory bounded by `query_memory_bytes`.
**Contract:** keeps E2, E3, E5, write:5, write:6
**Code:** `crates/omnigraph-compiler/src/ir/load.rs`, `crates/omnigraph/src/loader/mod.rs` (`plan_load`, `load_csv_overwrite_plan`), `crates/omnigraph/src/engine/operators/csv_source.rs`, `crates/omnigraph/src/engine/operators/validate.rs`, `crates/omnigraph/src/engine/operators/fragment_sink.rs`, `crates/omnigraph/src/validate/plan.rs`, `crates/omnigraph/src/loader/read_reuse.rs`, `docs/user/queries/explain-load.md`

## Context

A bulk load was one fixed pipeline that parsed and validated on one core, and its behavior could only be inferred from scaling measurements. Each load call re-read the tables it validated against, so request counts grew with fragment count. A query's access path is pinned by a plan expectation; a load had no plan to pin. Making the load a plan gives one artifact that execution consumes and that `explain`, GQT expectations and measurement all read.

## Decision

- The compiler lowers a parsed `load csv` to a load IR (`lower_load`). The planner builds a plan of `CsvSource`, `Validate` and `FragmentWrite` nodes from the catalog, the pinned snapshot, the source's byte size and index facts.
- `explain load csv …` returns that plan in the query explain document. It reads source metadata and no payload, and publishes nothing. HTTP and MCP require `change` on the target branch before the probe.
- `Validate` lists every check in the order it runs, which is the refusal precedence. Each check names its kind, identity fields and the committed table it consults.
- Each check's access path is `delta`, `probe` or `scan`. Cost compares estimated input rows, the consulted table's row count and complete BTREE coverage. Unknown counts and equal costs keep `probe`. The plan records the statistics and both costs.
- An overwrite runs as the plan. The source is parsed by several partitions, one per 8 MiB and at most `stage_write_concurrency`, each cutting its range on record ends; slices are reordered into source order. The sink stages fragments of one overwrite transaction as rows drain, never commits them, and deletes them on a refusal.
- A partitioned run that refuses reruns as one partition, so the reported refusal is the one source order selects, whatever the worker timing.
- Validation settles before the staged transaction commits. The load publishes once, through the ordinary publisher, in one `__manifest` publication.
- `query_memory_bytes` is the allowance of one statement's tracked memory: retained source, decoding, typed batches, validation state and the sink's chunks. Read-reuse caches (a statement-scoped session, held handles and a range memo) are admitted against a share of it, and the statement reads uncached when the share is refused.
- Append and merge keep their fenced keyed adapters and their existing bounds, and take their checks and access paths from the accepted plan. The source is probed once per statement, and append and merge capture its bytes once and reuse them on every reprepare.

## Rejected

- **Optimize the pipeline in place:** parallel parsing could be added without a plan, but access-path choice, worker bounds, explanation and expectations would not share one contract.
- **Defer the checks to index rebuild:** validation would follow visibility, and a load must not publish rows it has not checked.
- **Per-key probes against the store:** at bulk scale that is about a billion requests to object storage; the batched check is already the better shape.
- **An external converter or importer:** parsing moves outside policy and audit, and the engine still pays the same validation and write path.

## Not built

- Append and merge do not execute as the operator tree: their source is parsed in one stream outside it.
