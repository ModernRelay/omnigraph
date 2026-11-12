---
rfc: "2026-11-10-graph-shallow-clone"
title: "Graph shallow clone"
track: maintainer
status: draft
implementation: not-started
authors:
  - azimafroozeh
created: 2026-11-10
updated: 2026-11-11
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Route agreement for every engine-target scenario of `benchmarks/benchmarks.yaml`: the three thresholds of Evidence and tests hold on every one of them, and the Route agreement table lists each with the date of its run."
  - "The route comparison of Evidence and tests repeated on Linux with XFS, where the harness's restore is a verified byte copy."
---

# RFC: Graph shallow clone

> A term set in ***bold italics*** is defined at that spot; it is plain text afterwards.

## Summary

A ***graph shallow clone*** is a new graph that starts with everything another graph holds and shares that graph's objects instead of copying them. It starts with every branch, every published version, the commit history and every index. The graph it starts from is its ***base***, and a clone never writes its base. What the clone writes, and the record of what it deletes, stays in the ***clone root***, a root of the clone's own reached through the same endpoint and credentials as the base: a directory on a local filesystem, a key prefix on object storage, in the base's bucket or another. Two clones of one base therefore do not see each other's changes, and dropping a clone is removing its clone root. A bare "clone" below means a graph shallow clone; Lance's per-dataset operation is always named with Lance or as `Dataset::shallow_clone`.

This RFC fixes that contract and accepts one implementation of it: ***read-through storage***, an implementation of the object-store API placed below the engine, built from two stores of that API, the base and the clone root. A read is answered from the clone root when the object is there and from the base otherwise. A delete of a base object is recorded as a ***whiteout***, an empty object under a reserved directory of the clone root, written before the delete takes effect. The contract names no backend: the local filesystem and S3 are the two ***instances*** this RFC proves, an instance being the pair of stores a backend provides to the durable form, the local one in phase 1 and S3 in phase 2 (Rollout), and the calls are the same on both. The first user is `omnigraph-bench`, the harness of the end-to-end benchmark ([RFC 0039](0039-end-to-end-benchmark.md)). Its engine target, where the harness opens the graph in its own process instead of through a server, can run a repetition on an empty clone root whose base is the scenario's ***frozen fixture***, the never-opened copy of the validated fixture the harness keeps for restores (RFC 0039 calls a validated fixture frozen), instead of a restored working copy.

An ordinary root is unchanged: no storage format change, no new storage stamp, and an ordinary open reaches its objects as it does today. Outside this RFC, each with its gate in Rollout: a command or HTTP operation that creates and drops clones; `cleanup`, `optimize` and a storage upgrade on a clone; a native form built on `Dataset::shallow_clone`; and replacing the harness's restore. A served graph on a clone is also outside; the product operation's RFC owns it.

## Motivation

The harness gives every repetition of an engine-target scenario a fresh working copy of its fixture, reads included. `DatasetLease::restore` recreates the copy at the path the fixture was built at: on APFS by forced `clonefileat(2)` calls, on a qualified XFS volume by a byte copy that is verified in full. On XFS the restore's own source comment states what it does: it "verifies every source and destination byte". The cost of a repetition's setup therefore follows the fixture's file count on APFS and its byte size where the restore is a byte copy, not the size of what the repetition changes.

A prototype of read-through storage ran four scenarios on APFS, where the restore copies no bytes. On the fixture of `very-long-history-1000-write` the whole process took 1.77 s per repetition with the restore and 1.01 s on a clone; on APFS the restore issues one `clonefileat(2)` per file and digests the restored tree's metadata, so its cost follows the fixture's file count, and that fixture holds the longest history of the four. On the other three scenarios' fixtures it took 0.65 to 0.68 s either way. The measured step is the one operation a scenario times; its p50 on a clone was 0.92 to 0.98 of its p50 on a restored copy opened as the harness opens it today. Evidence and tests has the table. The platform where the restore is a byte copy is not measured yet, which is why it is an acceptance gate.

Three things make this more than a harness change. A clone root gains a reserved directory, a shape in its layout that other code must leave alone. The mechanism sits beside a primitive Lance owns, a shape the deny-list rejects until an RFC argues the case. And the contract outlives its first implementation: a later product operation and a native form must both meet it. The need is not the harness's alone: private, repeatable work on a frozen graph without copying it is what that product operation serves (phase 4).

## User and operational behavior

### Opening a clone

This phase adds no command, HTTP route or GQ statement, and advertises no API: `storage_read_through` is exported for the harness and stays unadvertised until the product operation's RFC. The harness builds the storage and opens the graph with it:

```rust
let storage = omnigraph::storage::storage_read_through(base, clone_root).await?;
let graph = Omnigraph::open_with_storage(clone_root, storage).await?;
```

The contract names a base root and a clone root on one backend, and one function takes both as root URIs and refuses a pair of different kinds. Phase 1 admits local paths: `base` names a local directory that holds a graph, and `clone_root` a directory that does not exist yet or is empty for a new clone, the same directory again for a later open. Phase 2 admits the same two roots as `s3://` URIs through the same function. A non-empty root that is not a clone root of this base is not detected; its objects shadow the base's key by key. Nothing checks that `base` holds a graph; the open then fails as it does on an empty root. On the local instance, links under the clone root are followed and links under the base are refused. The result is an ordinary `Omnigraph` handle: no query, mutation or branch statement changes.

### What a caller observes

- **A new clone answers as its base does.** The scenarios in Evidence and tests read rows on `main` and on four other branches, list branches and reopen, and their verification is the same on a clone as on a restored copy of the base.
- **Writes behave as on an ordinary root.** Published mutations, loads, merges, branch creation and branch deletion ran in a clone in the prototype.
- **A later open finds the clone's own state.** Opening the same base and clone root again shows the clone's writes and its deletes; scenarios restart after a write, a merge and a branch delete.
- **Two clones of one base are independent.** Neither sees the other's writes or deletes.
- **The base's bytes do not change.** The harness digests the base after the last repetition of a successful run and fails the run on any difference.
- **Dropping a clone is deleting its clone root.** Nothing on the base's side records a clone.

### Obligations in this phase

- **The base must not change while a clone of it exists.** Nothing enforces this. Read-through storage indexes the base's object metadata once, when the storage is built, so a later change to the base is not detected. Nothing keeps the base's objects alive either: a `cleanup` of the base while a clone exists removes objects the clone still reads.
- **At most one storage may be open over a clone root at a time.** Whiteouts are read once, when the storage is built. A second storage built over the same clone root sees every object the clone root holds at the time of each read and the whiteouts present when it was built, and not the deletes the first storage makes afterwards.
- **The caller must open a clone root over the base it was created from.** A clone root does not record its base in this phase, and opening it over another base is not refused. The record is the product operation's to define (phase 4); it names the base by an identity the base carries, never by its path.
- **The clone root's URI must be the one the base's absolute references name.** Datasets Lance made by `Dataset::shallow_clone` can keep absolute paths to their source's files (Lance's `base_paths`), and only keys under the clone root are read through the base. The harness's clone root is the path its fixture was built at, so such references resolve through the base. Nothing checks this for another caller.

### Refusals

| Condition | Outcome |
|---|---|
| The clone root is on a backend this phase does not wire (phase 1: anything but a local path; phase 2 admits `s3://` and refuses the rest with the backend named) | Error: "read-through storage needs a local clone root" |
| The base cannot be listed | Error |
| The base holds a symbolic link (local instance) | Error that names the link: a local base is read from plain files and directories only |
| The base holds the reserved directory | Error: "it is a clone root, and reading through a clone root is not supported" |
| A write or a delete under the reserved directory | Refused: the key "is inside the store's reserved directory" |
| A read or a listing under the reserved directory | Not found; listings omit it |

Not verified in phase 1: the S3 instance (phase 2); a served graph; and `cleanup`, `optimize`, `repair` or a storage upgrade on a clone. Nothing refuses the last four: a clone's handle is an ordinary handle, so they run and write their effects into the clone root, and what they leave behind is not defined by this RFC.

### The harness's route option

`omnigraph-bench run <scenario> --config benchmarks/benchmarks.yaml --dataset-cache <cache> --local-route direct|object-store|read-through` selects how an engine-target repetition reaches its fixture. The option takes no base: on `read-through` the base is the cache entry's frozen `template` directory and the clone root is its `active` sibling, created empty before each repetition and removed after it; the worker refuses a template that is not a sibling of the repetition root. The worker's proof that nothing wrote to the repetition root before the measured step compares the root's shape with the witness taken before the open; on `read-through` that comparison leaves out the reserved directory, the one entry the storage itself writes when it is first built over the empty clone root.

| Route | Repetition root | I/O |
|---|---|---|
| `direct`, the default | A restored working copy | Lance's direct local file path |
| `object-store` | A restored working copy | Every read and write through the object store, as for a handle opened with an I/O scope (`StorageIoScope`) |
| `read-through` | An empty clone root over the frozen fixture | The object-store route, through read-through storage, under the same I/O scope |

The argument parser refuses `--archive` together with the option, even as `--local-route direct`, before anything runs (exit code 2): a run on a route other than `direct` is a comparison and writes no run record, and the default run without the option is the recorded one. Its `--json` output carries `local_route` with the route's name when the route is not `direct`; the samples are in that output's `runs`, each with `durable_record: false`. No other output names the route. On `read-through` the harness reads every byte of the frozen fixture after the last repetition of a run whose repetitions all passed, and fails the run with `template bytes changed: …` (exit code 1) if its digest changed. The `object-store` route exists because a clone changes two things at once for a local root: it leaves Lance's direct local file path, and it adds read-through storage. Comparing `read-through` with `object-store` isolates read-through storage. On an S3 root Lance already reads and writes every object through the store, so there the clone changes one thing.

## Design

Read-through storage has two forms. The ***in-memory form***, on the main branch, keeps every write and delete in memory and is used by the deterministic simulation harness. The ***durable form***, new in this RFC, keeps them in the clone root and adds the whiteout and the reserved directory.

This section specifies phases 1 and 2. Except for the in-memory form, none of it is on the main branch yet; the prototype that produced the measurements in Evidence and tests differs from it where that section says so.

### The contract is on the object-store API

Read-through storage implements the object-store API the engine and Lance call, the `ObjectStore` trait of the `object_store` crate: get, head, list with and without a delimiter, put in its three modes (overwrite, put-if-absent, put-if-match on an ETag), multipart upload, delete, copy and rename. It is built from two stores of that API, the base and the clone root, and every rule below is stated in those calls. No rule names a backend. The two instances are the local filesystem (`LocalFileSystem`, phase 1) and S3 (this RFC writes S3 for the `s3://` scheme, which covers the S3-compatible services the engine already opens; phase 2). The in-memory form runs the same calls over two in-memory stores and keeps its deletes in memory; it is a form of read-through storage, not an instance this RFC proves. Azure and GCS are the same calls and are not proven by this RFC.

### A clone root is an ordinary root's layout plus one reserved directory

A clone root holds the objects the clone wrote, at the keys an ordinary root would use for them. Beside them it holds the reserved directory `__clone`: `__clone/created`, an empty object written when the storage is first built over a clone root that has none, and `__clone/deleted/<key>`, one empty object per whiteout, where `<key>` is the deleted base object's key relative to the root. Nothing else is recorded: no copy of base state and, in this phase, no pointer to the base. The names are the prototype's; Unresolved questions lists them.

A key is the object's path on its backend, the same key Lance's store and the engine's `StorageAdapter` use for the object: on the local instance the object's absolute path, made absolute lexically (no symlink is resolved), as Lance's `file-object-store` store keys it; on S3 the object key below the bucket. The base's keys are taken relative to the base root and appear below the clone root at the same relative key; the base may be another bucket of the same endpoint. The reserved directory is a key prefix, `__clone/`, which the local instance holds as a directory. The clone root's prefix, its path on the local instance, is the prefix under which the base's objects appear; a key outside it is served by the clone root's own store, which is the local filesystem at that absolute path on the local instance and the clone root's bucket on S3, so reads and writes of such a key never see the base and are not confined to the clone root. The promise that a clone never writes its base holds for keys under the clone root; a base reached by an absolute reference outside it (Obligations, the last item) is written like any other path. A whiteout's key is the reserved directory followed by the deleted key's segments below the root. A read or head of a whiteout key is not found. A rename onto a base key behaves as the put it ends in; the source's delete follows the delete rule. A delete of a key whose base object is already hidden writes no second whiteout; it removes the clone root's own object at that key if there is one, and succeeds whether or not there is.

A clone root nobody wrote to holds only `__clone/created`, after any number of reads. After a branch delete of a branch the base holds, with nothing else written, it holds one whiteout for the branch's ref object and one for each object under the branch's prefix, since Lance removes a prefix object by object. A published mutation adds the new version's objects and writes no whiteout. On the local instance it also shadows the base's `latest_version_hint.json` by an unconditional put; Lance writes that hint only on a store whose listing is not lexically ordered, so on S3 it is never written.

### Reads and listings see the base through the clone root

A read looks in the clone root first and then in the base, unless a whiteout hides the base object. A listing is the union of both, sorted by key in the byte order of the key, the order S3 lists in; Lance relies on it on an `s3` root, where it takes the first entry of a dataset's `_versions/` listing as the newest manifest. On equal keys the clone root's object wins, and base objects hidden by a whiteout are left out. A listing with a delimiter returns the union's objects and common prefixes at that depth, with the same hiding. The base's object metadata is indexed once, when the storage is built. A head, an existence check or a listing of a base object then does no I/O on the base; only reading an object's bytes does.

### Conditional writes keep their meaning over a base object

| Write | On a key only the base holds |
|---|---|
| Create (put-if-absent) | Refused as already existing, until the object is deleted in the clone |
| Update (put-if-match) | The caller's ETag is compared with the base object's; a match lands in the clone root as a create, so of racing updates exactly one wins |
| Unconditional put, multipart upload | Lands in the clone root and shadows the base object |
| Copy, rename | Reads the source through the union and writes the destination in the clone root; a rename then deletes the source |

Lance commits a dataset version by a put-if-absent of its manifest (on Windows, by a rename-if-absent of a staged manifest, which this store serves as a copy-if-absent and a delete). For a version the base already holds, that put is refused by the first row. For a new version, the clone root's own put-if-absent decides. No engine path issues a put-if-match on either instance: Lance commits with put-if-absent, and the objects the engine keeps outside Lance datasets are written by put-if-absent (`__history`) or by unconditional put. The row exists for the cluster's lock protocol, which updates its objects by put-if-match on the ETag the store reported, and which no clone opens. It is held by the storage tests of both forms and exercised under the deterministic simulation harness; on S3 its test runs against the clone root's real `If-None-Match` put, and the ETag the base index holds is the one S3 reports in a listing, a head and a get alike. On S3 the Create row is answered from the base index without a request; after a delete, the clone root's own put-if-absent is S3's `If-None-Match: *`. The Update row is one head of the clone root, the ETag comparison against the index, then that same put-if-absent. Both need conditional put enabled on the clone root's store, which is the `object_store` default and which the engine does not change; the copy and rename rows never reach S3's copy API.

### A whiteout is durable before the delete takes effect

A delete of a key the base holds runs in this order: put the empty whiteout object, hide the base object in memory, then delete the clone root's own object at that key if there is one. A delete of a key only the clone root holds writes no whiteout. The whiteout is the only durable step that concerns the base, and it comes first. An acknowledged delete therefore stays deleted across a reopen; this holds across a process stop. A machine crash is outside phase 1 on the local instance, since the local store does not sync the whiteout; on S3 an acknowledged put is durable, so the whiteout is durable before the delete is issued. Until the base object is hidden in memory, a concurrent reader of the same storage still sees it; from that step on a reader sees the clone root's own object or nothing, never the base's. The caller of the delete sees none of this: the three steps complete before the delete returns. A stop after the whiteout leaves the key as it was when the clone root held its own object there, and deleted otherwise. A create after a delete lands in the clone root; the whiteout stays, and later opens read the clone's object.

### The reserved directory never shows and refuses writes

Reads under the reserved directory answer not found and listings omit it. Puts, multipart uploads and deletes under it are refused. Building the storage over a clone root that lacks the reserved directory writes `__clone/created`, so every clone root carries the directory from its first build. A base that holds the reserved directory is refused when the storage is built: it is another clone's root, and reading through it would lose that clone's base and whiteouts. A clone of a clone is therefore not supported, and refused.

### A clone is a property of the handle's storage, not of the URI

A ***mount*** is one base paired with one clone root: it holds the index of the base's objects and the set of whiteouts. `storage_read_through` builds one and returns a `StorageAdapter` over it, the interface through which the engine reads and writes the objects it keeps outside Lance datasets (its ***control objects***, such as `__history` objects). The adapter hands the mount on through `StorageAdapter::read_through_mount()`. `Omnigraph::open_with_storage` gives the adapter to `LanceAccessContext::with_storage`. When the adapter carries a mount, that function builds a Lance object-store registry for this handle alone, whose providers for the clone root's scheme make the mount's store the store Lance uses, with the store Lance built as the clone root's store: on the local instance the `file` and `file-object-store` providers, with `file:` rewritten to `file-object-store:`; on S3 the `s3` provider, whose store becomes the clone root's store with no scheme rewrite (phase 2); `s3+ddb`, whose commits go through DynamoDB, is not admitted. The wrapper replaces the store Lance reads and writes through and clears Lance's paginated lister, a second handle to the bare store that Lance keeps for pushed-down directory pages, so that no listing walks the clone root past the mount. Lance's stores and the adapter then share one index and one set of whiteouts, so a delete through either is hidden from the other at once.

- **The clone root's URI is an ordinary root URI of its backend:** a local path, or an `s3://` prefix in phase 2. No function that branches on a root's scheme changes, and no process-wide table maps roots to mounts.
- **A wrapper around the adapter must forward `read_through_mount()`.** A wrapper that drops it leaves Lance on the bare clone root. `CountingStorageAdapter`, the engine's storage-call counter, and the harness's `PreparationStorageAdapter` forward it.
- **`delete_prefix` on such an adapter lists and deletes through the store, never by the local shortcut that removes a directory tree.** It empties the clone's view of the prefix: one whiteout per base object under it, kept by the clone root. On S3 it ends with the exact-prefix delete object stores need; on the local instance that step is skipped, since the prefix is a directory and the empty directories the loop leaves are not objects. The shortcut would act on the clone root alone and leave the base's objects visible. Dropping a clone is removing its clone root, by the caller and never through the adapter.

### Lance owns everything inside a dataset; Omnigraph adds the mount, the registry and the reserved directory

Three behaviors are Lance 13.0.0's own. On a `file` root, Lance reads and writes data files by direct local paths that bypass the object store. `file-object-store` is Lance's local store whose object reads and writes go through the ObjectStore API; Lance's documentation describes it as used for testing with ObjectStore wrappers, and this RFC relies on it outside tests. Its two directory helpers, `remove_dir_all` and `remove_empty_dirs`, still act on the local directory tree directly. On an `s3` root Lance reads and writes every object through the store already, with no direct path and no directory helper. And everything inside a dataset stays Lance's: manifests, versions, refs, transactions, indexes and the conditional manifest put. As Lance sees them, a clone's datasets are the base's datasets, and read-through storage adds no Lance metadata.

Omnigraph adds the mount, the per-handle registry that installs it, the `read_through_mount()` accessor and the reserved directory. The engine already rewrites `file:` to `file-object-store:` for a handle opened with an I/O scope (`ScopedProvider`); the mount's provider has the same shape and composes with it, so a handle on the read-through route carries the scope too and the two comparison routes differ in read-through storage alone.

A clone therefore reads and writes every object through the store; on the local instance, Lance's two direct directory removals act on the clone root's tree and never name the base. In the twelve scenarios measured, the measured step's counted storage calls equal the `object-store` route's (Evidence and tests), and its timings compare with that route first.

### Building the storage lists the base once

Building the storage lists every object of the base and every whiteout of the clone root. The cost follows the number of objects in the base, and the index stays in memory while the storage lives.

## Invariants

- **1, respect the substrate:** read-through storage sits under the object-store interface Lance calls. It implements no manifest, version, ref, transaction or index logic; Lance owns every dataset in a clone as it does in an ordinary root.
- **2, one publication door:** a clone publishes as every graph does, through one `__manifest` publication, written in the clone root. The base's `__manifest` is never written.
- **3, one coherent view:** unchanged inside a clone. It rests on the obligation that the base does not change, which nothing enforces in this phase.
- **5, crash convergence:** the whiteout is written before the delete it records, so an acknowledged delete survives a reopen. `a_stop_between_the_whiteout_and_the_delete_leaves_the_key_deleted_or_untouched` stops between the two steps and reopens: the key is then deleted or untouched, never half of either.
- **8, loud integrity failures:** a symbolic link in a local base, a base that is a clone root, and a write under the reserved directory are refused with errors that name the object. One gap is named and not closed here: a clone root opened over the wrong base is not detected. A storage wrapper that drops the mount leaves the handle's Lance stores on the bare clone root, where a base-only key is not found; `a_wrapper_that_drops_the_mount_leaves_lance_on_the_bare_root` pins that failure.
- **11, bounded resource use:** building the storage costs one listing of the base (paged on S3, one request per thousand objects) and one of the clone root's whiteouts, proportional to the base's object count, its history included. The harness will build the storage before its clock starts, for a measured restart too, so the listing is outside every measured step. A bound on that listing cost is a gate for any served use (phase 4).
- **12, one source of truth:** a clone's state is its base plus its clone root minus its whiteouts, derived when the storage is built. The clone root holds no copy of base state, and the in-memory index caches state that the first obligation makes immutable.
- **13, evidence matches the boundary:** the storage contract is tested in `omnigraph-storage`, the engine's use of it in `omnigraph-core`, route agreement in `omnigraph-bench`, and the S3 instance in the `s3_storage` integration tests of `omnigraph-engine`, run over RustFS in the RustFS S3 integration job (phase 2). A cost claim needs a checked-in instrument, which is the `--local-route` option and the comparison loop that drives it over four passes per route, both landing with phase 1; until they land, the timings in this RFC are prototype measurements. Rebuild evidence does not apply: a clone root is disposable, and the rebuild is deleting it and building a new storage over the base.

### Why read-through storage is not a deny-list shape

The deny-list item is "a custom WAL, transaction manager, buffer pool, or storage primitive already owned by Lance". Lance owns the shallow clone of one dataset at one version: `Dataset::shallow_clone(target_path, version, store_params)` resolves one ref and commits one `Operation::Clone`, and the new dataset's manifest points at the source's files through `base_paths`. A graph is more than one dataset at one version. It is many table datasets, a `__manifest` dataset whose Lance refs are the branch registry and whose table pins (the table versions a `__manifest` version names) witness exact manifest objects, and `__history` objects, each one Lance file written by a put-if-absent of its own. A graph clone composed of per-table Lance clones therefore needs graph metadata that Lance does not own, such as a `__manifest` whose pins name the cloned manifests, a `__history`, and a rule for branches and older versions. That design is the native form, and it is deferred to its own RFC. Read-through storage is a different kind of thing. The object store is not Lance's to own: Lance takes it from a registry the embedder fills, and the engine already supplies a per-handle provider there for scoped handles. Read-through storage is one more implementation of that interface, installed the same way on any backend the interface has, two of them proven by this RFC. It creates no dataset, no manifest and no reference; it changes where objects are found, below the level at which Lance defines a clone, and it carries the whole graph by construction. When the native form lands, its RFC decides whether read-through storage stays as the oracle the native form is tested against or retires. Acceptance appends to the argued cases listed under the deny-list in the [architectural invariants](../dev/invariants.md): "; and read-through storage, an object store below Lance that serves a clone root over its base, in the [graph shallow clone RFC](../rfcs/2026-11-10-graph-shallow-clone.md#why-read-through-storage-is-not-a-deny-list-shape)".

## Compatibility and reversibility

An ordinary root is untouched: its storage format, its stamp, every wire contract and the `omnigraph` CLI stay as they are.

A clone root is not an ordinary root. Opened as one, without its base, it holds manifests whose data files and earlier versions are in the base: Lance reports the missing objects, and nothing names the root as a clone root. A new clone root holds only the reserved directory and no graph. No tool turns a clone into an ordinary root in this phase. Clone roots are disposable, and no upgrade path is promised for them.

The reserved directory and the whiteout key shape are the one new commitment to a root's layout, and they exist only inside clone roots. Acceptance records the reserved directory where the root layout is documented (the versioning guide, beside `__history/`) and the deny-list case on the invariants page; phase 1 adds the route tests to the testing map's `omnigraph-bench` row and `--local-route` to the harness's README.

In the harness, `--local-route` defaults to `direct`, which keeps today's repetition and the run record and point id of [RFC 0039](0039-end-to-end-benchmark.md). The worker's input frame will omit the route when it is `direct`, so existing frames are unchanged.

Reverting removes the durable form of read-through storage, `storage_read_through`, the per-handle registry, the `read_through_mount()` accessor and its two forwarders, the `delete_prefix` branch, and the harness's `--local-route` option with its worker-frame field. The in-memory form stays; the deterministic simulation harness ([RFC 0037](0037-deterministic-simulation-harness.md)) uses it. No persisted state outside clone roots refers to the facility.

## Alternatives

| Alternative | What it buys | Decision for this scope |
|---|---|---|
| Keep the restore | No new code; today's records stay comparable | Stays the default and the baseline. Its cost follows the fixture's byte size where the restore is a byte copy and its file count on APFS; Motivation has the measurement. |
| No reset for a scenario whose steps only read | No copy and no clone for the scenarios whose steps read | A read-write open writes a probe object into the root before the first read, and restart scenarios reopen, so every repetition would open the frozen fixture for writing and the digest check would guard it after the fact instead of isolating it. One route for every repetition keeps read and write scenarios comparable. |
| A filesystem clone per repetition (APFS `clonefileat`, a reflink elsewhere) | No engine code | A feature of one filesystem at a time, and nothing on object storage. It cannot be the one mechanism for every backend. |
| No reset, `as commit` writes only | A write that moves no branch head, so the next repetition starts from the same heads | Leaves a detached `__manifest` version and its tag in the fixture's root, and does not express a published write. 35 of the catalog's 79 engine-target scenarios publish or change the branch registry in their measured step: 19 mutations, 9 merges, 4 branch creations, 2 loads and 1 branch deletion. |
| A scratch branch per repetition | Existing branch operations on every backend | Writes the fixture's own root, so damage is detected afterwards instead of prevented. A scenario that lists branches or names them, as `e2e_branch_delete.gqt` does, sees the scratch branch or must be rewritten. |
| Undo what a repetition wrote | No copy and no clone root | Opens the fixture for writing, and a correct undo is a second implementation of every write's effects. |
| The native form: per-table `Dataset::shallow_clone` composed into a graph clone | The substrate's own primitive | Not rejected, deferred to its own RFC. One call clones one version of one dataset; the graph metadata around it is undesigned (Invariants). Read-through storage is the oracle it will be tested against. |
| A URI scheme for clone roots | The URI alone identifies a clone | A clone has two locations, and every function that parses a root URI would learn the pair. The handle's storage already carries what an open needs. |
| A process-wide table of mounts | Any store built in the process finds its mount | Process-global state keyed by path. The registries of a bound and of a scoped handle are built per handle, and the mount follows them. |
| Read-through storage without durable whiteouts | No reserved directory and no commitment to a root's layout | Fails inside the supported boundary: see the next paragraph. |
| A contract for the local instance alone, object storage deferred to the product operation | Nothing beyond phase 1 to prove | RFC 0039 runs the end-to-end benchmark on MinIO and real S3, so the one mechanism it adopts must hold there; the S3 instance is phase 2 (Rollout). |

**The design without the whiteout object loses deletes at a later open.** With deletes masked in memory only, a delete hides a base object for the life of the storage and no longer. `e2e_branch_delete.gqt` deletes a branch, restarts and lists branches again; the restart builds a new storage over the same clone root, so the reopened clone would read the deleted objects from the base again. A table of masks shared by every storage of one process would carry that scenario and nothing else: a later process is the case the contract names (a later open finds the clone's own state), and the mount is per handle by the previous row. One empty object per whiteout, rather than one sidecar file rewritten per delete, keeps every durable write of the clone a put of a new key.

**The nearest in-repo precedents are `ScopedProvider` and the in-memory form of the same store.** `ScopedProvider` is a per-handle provider that rewrites `file:` to `file-object-store:` and wraps the store Lance uses; the mount's provider does the same and adds the mount. The in-memory form of read-through storage already runs prepared stores under the simulation harness, where a delete needs to last only for the run; it reaches Lance through a process-wide table keyed by the in-memory store's authority. The durable form diverges from it in two places: the whiteout object, for the reason above, and a mount installed by the handle's own registry instead of a process-wide table.

## Evidence and tests

Everything in this section comes from a prototype of phase 1 and arrives with it. Only the eleven storage tests of the in-memory form are on the main branch today. Phase 1 closes five places where the prototype differed from the Design, and the measurements below were taken on the prototype before those changes: the reserved directory written at first build, the refusal of a non-local clone root, the in-memory mask set before the clone root's delete, the I/O scope composed over the mount, and the exact-prefix delete skipped on a local clone root.

### The storage contract

Owner: the in-source tests of `omnigraph-storage`. Seven tests of the store and two of the adapter are new for the durable form. The test names below are the ones phase 1 lands with, and the refusal messages name the base, the clone root and the reserved directory in these words.

| Contract | Test |
|---|---|
| A read falls through to the base, a write stays in the clone root, the base's files are unchanged | `reads_fall_through_and_writes_stay_in_the_clone_root` |
| A base that holds a symbolic link is refused by name | `open_refuses_a_symlink_anywhere_under_the_root` |
| Heads and ranged reads of a base object agree with its file | `ranges_and_heads_read_the_base` |
| A delete hides a base object; a later put and delete behave as on any store | `delete_masks_the_base_object` |
| A listing is the sorted union and the clone root's object wins | `listings_are_the_sorted_union` |
| A create on a base object is refused until the object is deleted | `create_refuses_a_base_object_until_it_is_deleted` |
| An update against a base object is a compare-and-swap on its ETag | `update_against_a_base_object_is_a_cas` |
| Of eight racing updates of one base object, one wins | `racing_updates_of_a_base_object_admit_one` |
| Copies and renames read the base and write the clone root | `copies_and_renames_read_the_base_and_write_the_clone_root` |
| A multipart upload lands in the clone root | `multipart_uploads_land_in_the_clone_root` |
| Two stores built from one mount (Lance's store and the control adapter's) share its deletes | `two_mounts_share_one_deletion_view` |
| A delete survives a reopen and stays out of another clone root over the same base (new) | `durable_deletes_survive_a_reopen_and_stay_in_their_own_clone_root` |
| The reserved directory is hidden and refuses writes; only a delete of a base object leaves a whiteout (new) | `a_durable_store_keeps_its_whiteouts_out_of_sight` |
| A base that holds the reserved directory is refused (new) | `durable_refuses_a_base_that_is_itself_a_clone_root` |
| A create after a delete holds across reopens (new) | `create_after_a_durable_delete_holds_across_reopens` |
| A clone root carries the reserved directory from its first build, and is refused as a base (new) | `a_clone_root_carries_the_reserved_directory_from_its_first_build` |
| A stop between the whiteout and the delete leaves the key deleted or untouched after a reopen (new) | `a_stop_between_the_whiteout_and_the_delete_leaves_the_key_deleted_or_untouched` |
| Of eight racing updates of one base object over a local clone root, one wins (new) | `racing_updates_of_a_base_object_admit_one_over_a_local_clone_root` |
| A clone root on a backend the phase does not wire is refused (new, adapter; phase 2 moves its input to `az://`) | `read_through_storage_refuses_a_remote_clone_root` |
| A prefix delete hides the base's objects under it, deletes the clone's, keeps the base's files and the reserved directory (new, adapter) | `delete_prefix_over_a_clone_root_masks_the_base_and_keeps_it` |

The eleven tests not marked new exercise the in-memory form over two in-memory stores; the durable form shares their code paths. Four of the store's new tests and both adapter tests run over a local clone root.

In `omnigraph-core`, `lance_and_control_stores_of_one_root_share_the_mount` builds a Lance store and a control adapter over one mount. It checks that the Lance store has no direct local paths, that a delete through either store hides the object from the other, that a put lands in the clone root, that the base's files are unchanged, and that a storage built later over the same clone root finds the same deletes and writes. `a_wrapper_that_drops_the_mount_leaves_lance_on_the_bare_root` opens a handle's Lance access through a storage wrapper that forwards every call but the mount: the control reads still fall through, the Lance store is the bare clone root with direct local paths, and a base-only key is not found there. `a_scoped_read_through_registry_closes_with_its_scope` opens the access through a scoped read-through storage: the store reads through the mount, a put lands in the clone root, and a put after the scope closed is refused and leaves nothing behind.

### Route agreement

Owner: the in-source tests of `omnigraph-bench`. `routes_agree` builds the scenario's fixture and freezes it once per route, then runs the scenario once on `direct`, once on `object-store` and twice on `read-through`, preparing each repetition the way the run loop does: a verified copy for the first two routes, an empty clone root for `read-through`. It asserts five things:

1. Every repetition passes the workload's expectations.
2. The verification record is the same on every route.
3. The frozen fixture's physical digest is unchanged after every repetition.
4. The measured step's control-adapter calls (`control_store_calls`) are equal on the three routes.
5. The measured step's Lance calls (`logical_store_calls`) are equal on `read-through` and `object-store`, in both `read-through` repetitions.

Six tests run it over ten scenarios: `tiny-read`, `tiny-restart`, `tiny-post-reopen`, `tiny-mutate`, `tiny-branch`, `tiny-load`, `e2e-merge-all-changed`, `e2e-branch-delete`, `very-long-history-32-write` and `identity-lifecycle`.

### Timing and storage calls

This is a measurement of the prototype, taken by the loop that `scripts/compare-local-routes.py` checks in with phase 1. One release binary of `omnigraph-bench`, built from commit `2c4b2f12` plus the prototype, ran four scenarios on the three routes. After one discarded run per scenario that built its fixture, each route ran four passes of ten repetitions, the passes alternating the order of the routes, so each row has 40 samples. No other build ran on the machine during the run. Machine: Apple M5 Max, 18 cores, macOS 26.5, APFS on NVMe, `rustc 1.97.1`. "Process" is the wall time of one `run` process divided by its ten repetitions, as the median of the four passes; it includes the restore or the empty clone root, the worker and the checks.

| Scenario | Route | Measured step p50 µs | × direct | × object-store | Process s per repetition |
|---|---|---|---|---|---|
| `e2e-query-scan` | direct | 476 | 1.00× | 1.07× | 0.66 |
| | object-store | 445 | 0.93× | 1.00× | 0.66 |
| | read-through | 462 | 0.97× | 1.04× | 0.65 |
| `tiny-mutate` | direct | 6653 | 1.00× | 1.00× | 0.67 |
| | object-store | 6660 | 1.00× | 1.00× | 0.67 |
| | read-through | 6460 | 0.97× | 0.97× | 0.67 |
| `e2e-merge-all-changed` | direct | 19368 | 1.00× | 0.99× | 0.68 |
| | object-store | 19578 | 1.01× | 1.00× | 0.68 |
| | read-through | 17908 | 0.92× | 0.91× | 0.68 |
| `very-long-history-1000-write` | direct | 12153 | 1.00× | 1.02× | 1.77 |
| | object-store | 11885 | 0.98× | 1.00× | 1.83 |
| | read-through | 11886 | 0.98× | 1.00× | 1.01 |

The Lance calls of the measured step were identical in all 40 samples of a route, and equal on `read-through` and `object-store`. The control adapter saw no call in any sample. The `direct` route shows fewer Lance calls because its data files bypass the store that counts them.

| Scenario | `__manifest` calls on `object-store` and `read-through` | Table calls on both |
|---|---|---|
| `e2e-query-scan` | head 2 | get 1 |
| `tiny-mutate` | get 5, put 4, head 6 | get 2, put 3, head 1 |
| `e2e-merge-all-changed` | get 26, put 8, head 25, list 1, delete 3 | get 30, put 3, head 14 |
| `very-long-history-1000-write` | get 8, put 4, head 6 | get 4, put 3, head 2 |

### What the evidence covers and what it does not

The catalog has 97 scenarios: 79 on the engine target and 18 served. Twelve of the 79 have run on a clone, the ten above plus `e2e-query-scan` and `very-long-history-1000-write` in the timing run. The other 67 have not. The 18 served scenarios are outside this phase.

Not shown:

- Linux with XFS, where the restore is a byte copy.
- The S3 instance (phase 2), and a served graph.
- `cleanup`, `optimize` and a storage upgrade on a clone.
- A process stop between the whiteout and the delete, and a machine crash after an acknowledged delete.
- A clone root opened over the wrong base, and a handle whose storage wrapper drops the mount.
- A base with absolute references opened at another path.
- The cost of building the storage over a large base.
- Why the merge's measured step is faster on a clone. The instrument times the step and counts calls above the store; it does not attribute time below it.

### Required evidence and thresholds

Before acceptance, the two gates in `blocked_on`. The first is met when `routes_agree` in `omnigraph-bench`'s in-source tests runs over every scenario of `benchmarks/benchmarks.yaml` that has no `target` key (the engine target; 79 today) and passes in the workspace test job, and when the Route agreement table above lists each scenario with the date of that run. Phase 1 lands the three tests The storage contract names for them: the fault test that stops between the whiteout and the delete and reopens, the test that pins the failure of a handle whose storage wrapper drops the mount, and the test that a clone root carries the reserved directory from its first build and is refused as a base. Phase 2 lands the S3 instance's evidence in the RustFS S3 integration job (`rustfs_integration` in `ci.yml`), the job that runs every configured S3 test: the durable-form rows of The storage contract, except the two that name a local clone root, run a second time with a bucket prefix as base and another as clone root, as tests of `omnigraph-engine`'s `integration` binary under `s3_storage::`, the module that job selects by name, each skipping without `OMNIGRAPH_S3_TEST_BUCKET`, the store's own test bodies shared with them through a public test helper of `omnigraph-storage` so the rows are written once; and the GQT runner gains `--base <uri>`, admitted only beside `--store` on the direct engine, which builds the case's storage with the same function and opens `--store` as the clone root, so `.gqt` cases of the first four observations under What a caller observes run with both options against a bucket. The last two observations are not visible in rows and are held by the engine's `s3_storage` tests: the base prefix's listing and ETags are unchanged after the cases, and deleting the clone root's prefix leaves the base's listing as it was. Its table is added to this section with its date. Phase 2's evidence is not an acceptance gate: it gates `implementation: complete` (Rollout) and the harness's adoption of clones.

Three thresholds apply to every scenario the gates cover; they are the local instance's route comparison, and phase 2 carries no timing threshold:

1. The verification record is the same on the three routes, and the frozen fixture's digest is unchanged.
2. The `logical_store_calls` and `control_store_calls` of the measured step on `read-through` equal those on `object-store` in every sample.
3. A pass is one `omnigraph-bench run` invocation of ten repetitions; a comparison is four passes per route, the route order alternating, each p50 computed from the `elapsed_us` of the invocation's `--json` samples. The p50 of the measured step over the four `read-through` passes is no higher than 1.05× the p50 over the four `object-store` passes.

Each gate passes when its named check exits 0: the first gate and thresholds 1 and 2 are the `routes_agree` tests; threshold 3 and the second gate are the comparison loop, run once on this RFC's machine and once on Linux with XFS, its two tables added to this section with their dates.

### Upstream surfaces surveyed

Lance 13.0.0, the pinned crates.io release. In source: `Dataset::shallow_clone` (`lance`, `src/dataset.rs`), `Operation::Clone` (`lance-table`, `src/transaction/operation.rs`), `Manifest::shallow_clone` and `Manifest::base_paths` (`lance-table`, `src/format/manifest.rs`), and the `file` and `file-object-store` providers with `ObjectStore::is_local` and `has_direct_local_paths` (`lance-io`, `src/object_store/providers.rs` and `src/object_store.rs`). In the documentation, read in full on 2026-11-11: the table layout page for base paths and shallow clones, the transaction page for the Clone operation, the branch and tag format page, the tags and branches guide and the object store guide. The documentation does not say whether cleanup of a source dataset keeps files that a Lance shallow clone at another root still references; its source says that dropping a dataset root cannot protect files another dataset references through `base_paths` ([lance#7514](https://github.com/lance-format/lance/issues/7514)). The native form's RFC owns that question.

## Rollout

1. **Read-through storage on the local instance and the route comparison in the harness.** Ships the engine API, the storage and route tests, and `--local-route`. No product surface, no record on a clone route, no change to the default repetition. Phase 1 lands while this RFC is a draft, since the first gate in `blocked_on` needs its route tests and `--local-route`; the API it adds claims no authority until acceptance. `implementation` moves to `in-progress` when phase 1 starts and to `complete` when phase 2 lands with the evidence above. Phases 3 to 5 advance the `implementation` fields of their own RFCs.
2. **The S3 instance.** The same contract over an `s3://` base and clone root, through the same function, which for an `s3://` pair reads the standard `AWS_*` environment as the engine's S3 storage does and carries no captured binding; the `s3` provider wrapped in the per-handle registry; and the evidence in the RustFS S3 integration job named under Required evidence. That job reports on pull requests and does not gate `main`; a configured test that skips is a failure there, and a test needs `OMNIGRAPH_S3_TEST_BUCKET` and skips without it, as the engine's `s3_storage` module does. It lands before phase 3, because RFC 0039 names MinIO as the end-to-end benchmark's repeatable rig and real S3 as its truth, so the one mechanism that benchmark adopts must hold on S3.
3. **The harness's engine target adopts a clone per repetition.** Not decided here. Repetition reset is a Protocol factor in RFC 0039 and enters the run spec, so a clone is a new level with new point ids. It needs an amendment to RFC 0039 and the XFS comparison.
4. **A product operation that creates and drops clones.** Its own RFC. Its gates are the gaps this RFC names: a clone root that records its base's identity and an open that refuses a mismatch, an enforced rule that keeps a base unchanged and its files alive while a clone exists, more than one process on a clone root, `cleanup`, `optimize` and storage upgrades on a clone, a bound on the cost of building the storage, a clone of a clone, a served clone, a base opened at a path other than the one its absolute references name, and `repair`.
5. **The native form over `Dataset::shallow_clone`.** Its own RFC. It is tested against read-through storage: the same scenarios, the same verification.

## Unresolved questions

- The reserved names. The prototype uses `__clone/deleted/`, and this RFC adds `__clone/created`. They are the one commitment to a root's layout and are chosen once; the storage tests and the refusal messages carry them and change with them. The name must be one no table or system object can take; the engine's root already reserves `__manifest` and `__history`. Decided by the maintainer at acceptance; the phase 1 PR, whose tests and messages carry the name, forces it.

## Decision log

- 2026-11-10: Created. The decisions, each with what it rejects: **The name.** Graph shallow clone, the term Lance and Delta use, here carrying a whole graph where Lance's carries one version of one dataset. Rejected: fork, a repository's word for a copy with its own history; overlay, a filesystem's name for a mechanism and not a name for the feature. **Where a clone is handed to the engine.** The handle's storage (Alternatives: a URI scheme, a process-wide table). **The whiteout object.** Durable before the delete (Alternatives: the design without it).
- 2026-11-11: **The contract on the object-store API.** The contract names no backend; the local filesystem and S3 are instances, local in phase 1 and S3 in phase 2 over an S3-compatible service, RustFS in CI and MinIO where the end-to-end benchmark runs, before the harness adopts clones. Rejected: a contract stated for the local instance alone, with object storage deferred to the product operation, since RFC 0039 runs the end-to-end benchmark on MinIO and real S3 and one mechanism must hold on every backend that benchmark runs on.
