//! A `limit` with no `order` cuts the tie rule's order (the RFC's `Cut`):
//! the plan sorts an unfiltered scan's rows by `$p`'s id under the `limit`,
//! so it reads every fragment and keeps the smallest ids, deleted rows
//! skipped. The scan's own early stop and Lance's `Scanner::limit` apply only
//! to a `Limit` directly over a scan's projection, which no read plan has.
//!
//! Measured at the table stores' own trackers, which also see Lance's direct
//! local reader (a `file://` read never reaches a `WrappingObjectStore`).

use std::collections::BTreeSet;
use std::sync::Arc;

use lance_io::utils::tracking_store::IOTracker;
use omnigraph::Session;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};

use crate::helpers::{
    self, MUTATION_QUERIES, first_column_sorted, init_and_load, mixed_params, mutate_main, params,
};

/// Single-row inserts, one Lance fragment each: more than the local store's
/// fragment read-ahead, so a read that stopped early would visibly read fewer.
const INSERTS: usize = 24;

/// Run `query` on a fresh handle (so its table open carries the probes) and
/// return its row count with the distinct data files it read (the query
/// reads one table, `Person`).
async fn person_data_files_read(uri: &str, query: &str, name: &str) -> (usize, BTreeSet<String>) {
    let db: Session = helpers::session(Omnigraph::open(uri).await.unwrap());
    let probes = QueryIoProbes {
        table_wrapper: Some(Arc::new(IOTracker::default())),
        ..Default::default()
    };
    let stores = probes.table_stores.clone();
    let rows = with_query_io_probes(
        probes,
        db.query(ReadTarget::branch("main"), query, name, &params(&[])),
    )
    .await
    .unwrap();
    let mut files = BTreeSet::new();
    for store in stores.stores() {
        for request in store.io_stats_incremental().requests {
            let path = request.path.to_string();
            if path.contains("/data/") && path.ends_with(".lance") {
                files.insert(path);
            }
        }
    }
    (rows.num_rows(), files)
}

#[tokio::test]
async fn limit_with_no_order_reads_every_fragment_for_its_tie_rule() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = init_and_load(&dir).await;
    for i in 0..INSERTS {
        let name = format!("limit_pushdown_{i}");
        let person = mixed_params(&[("$name", &name)], &[("$age", 30)]);
        mutate_main(&db, MUTATION_QUERIES, "insert_person", &person)
            .await
            .unwrap();
    }
    drop(db);

    let (all_rows, all_files) = person_data_files_read(
        &uri,
        "query everyone() { match { $p: Person } return { $p.name } }",
        "everyone",
    )
    .await;
    assert!(
        all_files.len() > INSERTS,
        "the meter must see every fragment of a full scan (the loaded rows plus one per insert), saw {}: {all_files:?}",
        all_files.len()
    );
    assert!(all_rows > INSERTS);

    let (rows, files) = person_data_files_read(
        &uri,
        "query one() { match { $p: Person } return { $p.name } limit 1 }",
        "one",
    )
    .await;
    assert_eq!(rows, 1);
    assert_eq!(
        files, all_files,
        "a `limit 1` with no `order` reads every fragment to find the smallest id"
    );
}

/// The limit counts live rows only and spans fragments: deleted rows among
/// the smallest ids are skipped, and a filtered limit finds a row in the last
/// fragment.
#[tokio::test]
async fn limit_skips_deleted_rows_and_spans_fragments_issue_1335() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    let db = init_and_load(&dir).await;
    for i in 0..INSERTS {
        let name = format!("limit_pushdown_{i}");
        let person = mixed_params(&[("$name", &name)], &[("$age", 30)]);
        mutate_main(&db, MUTATION_QUERIES, "insert_person", &person)
            .await
            .unwrap();
    }
    let deleted = ["limit_pushdown_0", "limit_pushdown_1"];
    for name in deleted {
        mutate_main(
            &db,
            MUTATION_QUERIES,
            "remove_person",
            &params(&[("$name", name)]),
        )
        .await
        .unwrap();
    }
    drop(db);

    let db: Session = helpers::session(Omnigraph::open(&uri).await.unwrap());
    // The four loaded people, then the two smallest live inserts by id.
    let six = db
        .query(
            ReadTarget::branch("main"),
            "query six() { match { $p: Person } return { $p.name } limit 6 }",
            "six",
            &params(&[]),
        )
        .await
        .unwrap();
    let names = first_column_sorted(&six);
    assert_eq!(
        names.len(),
        6,
        "a limit across fragments returns its rows: {names:?}"
    );
    assert!(
        names.iter().all(|name| !deleted.contains(&name.as_str())),
        "a deleted row never fills the limit: {names:?}"
    );

    let last = format!("limit_pushdown_{}", INSERTS - 1);
    let found = db
        .query(
            ReadTarget::branch("main"),
            "query late($name: String) { match { $p: Person } where $p.name = $name return { $p.name } limit 1 }",
            "late",
            &params(&[("$name", &last)]),
        )
        .await
        .unwrap();
    assert_eq!(
        first_column_sorted(&found),
        vec![last],
        "the filtered limit finds the row in the last fragment"
    );
}
