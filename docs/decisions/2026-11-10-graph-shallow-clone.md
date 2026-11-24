# Graph shallow clone over read-through storage

**Decides:** a graph shallow clone is an object store below Lance that answers from a clone root and falls through to an unchanged base, with deletes of base objects recorded as durable whiteouts in a reserved `__clone` directory.
**Contract:** keeps E1, E2, E4
**Code:** `crates/omnigraph-storage/src/read_through.rs`, `crates/omnigraph-storage/src/read_through_contract.rs`, `crates/omnigraph-core/src/lance_access/read_through.rs`, `crates/omnigraph-cli/src/admission.rs`, `crates/omnigraph/tests/integration/shallow_clone.rs`, `crates/omnigraph-gqt/tests/fixtures/runner/clone`, `crates/omnigraph-bench/src/gqt_tests.rs`

## Context

A caller wants a private, writable copy of a whole graph (every branch, commit and index) without copying its bytes. The benchmark harness needs one per repetition, and a byte copy costs the fixture's size. Lance's `Dataset::shallow_clone` clones one dataset at one version; a graph is many datasets, a `__manifest` whose pins name exact table versions, and `__history` objects. A clone built from per-table Lance clones needs graph metadata Lance does not own. A clone placed below Lance, at the object-store interface, carries the whole graph by construction and adds no dataset, manifest or ref.

## Decision

- `Omnigraph::open_clone(base, clone_root)` builds read-through storage (`storage_read_through`) and returns an ordinary handle. The CLI's `--base` does the same for reads and writes; maintenance, schema apply and deployment do not take it.
- A read looks in the clone root, then in the base unless a whiteout hides the key. A listing is the byte-ordered union; on equal keys the clone root wins. The base's object metadata is indexed once, when the storage is built.
- Put-if-absent on a key only the base holds is refused as existing. Put-if-match compares the base object's ETag and lands in the clone root. Unconditional puts and multipart uploads land in the clone root.
- A delete of a base key puts `__clone/deleted/<key>` first, then hides the key in memory, then deletes the clone root's own object. An acknowledged delete survives a reopen.
- A rename whose source the base holds is refused: its copy would take no turn against a concurrent write.
- `__clone` is hidden from reads and listings, and writes under it are refused. The first build writes `__clone/created`. A base holding `__clone` is refused, so a clone of a clone is refused. A non-empty clone root without `__clone/created` is refused. The CLI refuses to open a clone root without `--base`.
- Base and clone root are one kind, local or `s3://`, and neither contains the other. A local base holding a symbolic link is refused.
- The mount travels with the handle's `StorageAdapter` (`read_through_mount()`) into a per-handle Lance object-store registry. A wrapper adapter must forward it. `delete_prefix` deletes key by key through the store, so it whiteouts base objects.
- The base is never written. Dropping a clone is deleting its clone root.
- The caller keeps the base unchanged while a clone exists, opens a clone root only over the base it came from, and opens one storage per clone root at a time. Nothing enforces these: a clone root records no identity of its base, and a `cleanup` of the base removes objects a clone reads.

## Rejected

- **A URI scheme for clone roots:** a clone has two locations, and every function that parses a root URI would learn the pair.
- **A process-wide table of mounts:** global state keyed by path; Lance registries are already built per handle.
- **Deletes masked in memory only:** a later open reads deleted objects from the base again, so a branch delete is undone by a restart.
- **A filesystem clone per repetition (clonefile, reflink):** one filesystem at a time, and nothing on object storage.
- **A scratch branch per repetition:** it writes the fixture's own root, so damage is detected afterwards instead of prevented, and branch listings see it.

