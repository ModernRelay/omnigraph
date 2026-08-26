// The crate is `#![cfg(tokio_unstable)]`-gated (tokio's seeded scheduler
// RNG); without the flag the lib compiles EMPTY, so this file must vanish
// with it or the workspace gate fails on unresolved imports. CI sets
// RUSTFLAGS in .github/workflows/dst.yml.
#![cfg(tokio_unstable)]

//! Single-commit-birth pin (#487): a CRASH during `init` immediately after
//! the `__manifest` Create commit must leave an OPENABLE store — the
//! manifest's entire birth (entries, genesis lineage, internal-schema
//! stamp) rides that one commit, so there is no torn half-born state.
//! (The predecessor of this test pinned #483: a separate stamp commit
//! meant a crash in the gap left an unstamped manifest that every open
//! misdiagnosed as a pre-0.4 store. #487 removed the gap; this pin keeps
//! the class dead.)
//!
//! Crash vs cleanup: a PANIC at the window unwinds past
//! `cleanup_failed_init` — true crash semantics, store survives complete.
//! The error-RETURN path at the same window still runs cleanup on the
//! born-complete store (the cleanup-brick class); that arm lives in the
//! birth-contract enumeration in `scenarios.rs`.

use std::sync::Arc;

use serial_test::serial;

use omnigraph::db::{InitOptions, Omnigraph};
use omnigraph::failpoints::{ScopedFailPoint, names};
use omnigraph::storage::{ObjectStorageAdapter, StorageAdapter};
use omnigraph_dst::fixtures::TEST_SCHEMA;

#[test]
#[serial]
fn crash_after_manifest_create_leaves_openable_store() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build_local(Default::default())
        .expect("current-thread runtime");
    runtime.block_on(async {
        let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
        let root = "shared-memory://torn-init";

        // ---- Phase 1: init CRASHES right after the manifest Create commit.
        // Panic action = unwind past cleanup_failed_init, like a process
        // death; the durable state is the Create commit and everything
        // written before it.
        {
            let _fp = ScopedFailPoint::new(names::INIT_POST_MANIFEST_CREATE, "panic");
            let died = std::panic::AssertUnwindSafe(Omnigraph::init_with_storage(
                root,
                TEST_SCHEMA,
                storage.clone(),
                InitOptions::default(),
            ));
            let result = futures::FutureExt::catch_unwind(died).await;
            assert!(result.is_err(), "init must die at the injected crash");
        }
        println!(
            "[phase 1] init crashed at {}",
            names::INIT_POST_MANIFEST_CREATE
        );

        // ---- Phase 2: read-write reopen SUCCEEDS — the store was born
        // complete in the one commit.
        let db = Omnigraph::open_with_storage(root, storage.clone())
            .await
            .expect("RW open must succeed on a post-Create store (#487)");
        drop(db);
        println!("[phase 2] RW open: OK");

        // ---- Phase 3: read-only reopen succeeds too.
        let ro = Omnigraph::open_read_only_with_storage(root, storage.clone())
            .await
            .expect("RO open must succeed on a post-Create store (#487)");
        drop(ro);
        println!("[phase 3] RO open: OK");

        // ---- Phase 4: re-init still refuses (an existing `__manifest` is
        // the store-existence authority; init never clobbers one).
        let reinit = Omnigraph::init_with_storage(
            root,
            TEST_SCHEMA,
            storage.clone(),
            InitOptions::default(),
        )
        .await;
        match reinit {
            Ok(_) => panic!("re-init over an existing __manifest must refuse"),
            Err(e) => println!("[phase 4] re-init refused: {e}"),
        }
    });
}
