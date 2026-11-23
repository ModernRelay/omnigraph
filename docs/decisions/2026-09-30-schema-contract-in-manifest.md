# Schema contract in the manifest

**Decides:** the accepted schema source, IR and identity are one `schema_contract` row of `__manifest`, replaced in the same publication as the table registrations and graph head that use it.
**Contract:** keeps E1, E2, schema.changes
**Code:** `crates/omnigraph-catalog/src/state.rs`, `crates/omnigraph-catalog/src/record.rs`, `crates/omnigraph-catalog/src/migrations.rs`, `crates/omnigraph/src/db/omnigraph/schema_apply.rs`, `crates/omnigraph/src/table_store.rs`, `crates/omnigraph/src/db/upgrade/root_schema.rs`

## Context

Schema apply published its tables first and then installed three schema files
at the graph root. A crash between the two left a published graph whose
contract still had to be installed, so open, refresh and write entry had to
decide whether staged files belonged to a finished or an unfinished apply. A
sentinel branch announced an apply in progress, but a process-local gate
cannot fence another process's decisions about those files. The manifest
compare-and-swap already decides one publication; putting the contract in it
removes the second decision.

## Decision

- The row has `object_id` and `object_type` `schema_contract`. The exact
  source and IR texts are the nullable `schema_source` and `schema_ir`
  columns beside the packed record; `metadata` holds `schema_ir_hash`,
  `schema_identity_version` and `schema_identity_domain`. Each converted
  branch has exactly one live row.
- Schema apply and the system-column upgrade put the replacement row in
  their one publication with registrations, tombstones and lineage. Nothing
  is installed after it; a new in-memory view is cache maintenance.
- Admission parses the IR, recomputes its hash, checks the identity
  envelope and proves the source compiles to the IR's shape. A missing,
  duplicate, malformed or mismatched row refuses. Serving has no root-file
  fallback.
- Ordinary state scans omit the content columns.
- Genesis includes the row in the manifest Create. `__init_claim.json`
  still decides competing initializers.
- An added type's table path is derived from the allocator, so a retry
  meets the same path. An original empty version-one table with the desired
  schema is reused. One with another schema gets an empty detached
  Overwrite, published as logical version 2 with `last_linear_version` 1.
  Any other state refuses. A retry never deletes the path, because another
  apply may already have registered it.
- The root files `_schema.pg`, `_schema.ir.json` and `__schema_state.json`
  are input only to the offline upgrade of a stamp 8 or 9 root, which reads
  them and leaves them in place.
- The process-local schema gate stays; see
  [Shared schema gate](2026-09-18-shared-schema-gate.md).

## Rejected

- **Keep the root files, drop only the sentinel:** a crash after publication
  still needs a durable way to find and finish the installation.
- **Identity in the manifest, text in objects addressed by hash:** the
  objects must exist before publication and live as long as every version
  that names them, which adds lookup and reclamation rules.
- **A separate contract dataset:** it does not share the manifest
  compare-and-swap, and a pointer to it adds another version reference and
  retention dependency.
- **Delete an unregistered added-type path on retry:** another apply can
  register that path after the retry looked, and the delete then destroys a
  published table.
