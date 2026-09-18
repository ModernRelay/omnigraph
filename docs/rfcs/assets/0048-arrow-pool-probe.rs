// RFC 0048 qualification only. Run through 0048-arrow-pool-probe.py so the
// optional upstream features do not change OmniGraph's production feature set.
use std::sync::Arc;

use arrow_array::{Array, Int32Array};
use arrow_buffer::MemoryPool as ArrowPool;
use datafusion_execution::memory_pool::arrow::ArrowMemoryPool;
use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};

fn main() {
    let first: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
    let second: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
    let first_arrow = ArrowMemoryPool::new(first.clone(), MemoryConsumer::new("query-a"));
    let second_arrow = ArrowMemoryPool::new(second.clone(), MemoryConsumer::new("query-b"));
    let original = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let bytes = original.get_buffer_memory_size();
    assert!(bytes > 1);

    // Positive refusal control: DataFusion's fallible operator reservation.
    let admission = MemoryConsumer::new("fallible-control").register(&first);
    assert!(admission.try_grow(bytes).is_err());
    assert_eq!(first.reserved(), 0);

    // Arrow's claim API tracks an already allocated buffer, infallibly.
    original.claim(&first_arrow);
    assert_eq!(first.reserved(), bytes);
    assert!(first_arrow.available() < 0);
    let over_limit_available = first_arrow.available();
    let shared = original.slice(0, 2);
    shared.claim(&first_arrow);
    assert_eq!(
        first.reserved(),
        bytes,
        "same-owner slices are not double charged"
    );

    // The buffer has one reservation. Claiming it for another query transfers
    // that reservation even though the first query still retains the array.
    shared.claim(&second_arrow);
    assert_eq!(first.reserved(), 0);
    assert_eq!(second.reserved(), bytes);
    drop(shared);
    assert_eq!(
        second.reserved(),
        bytes,
        "the first query still owns an alias"
    );
    assert_eq!(original.value(4), 5);
    drop(original);
    assert_eq!(first.reserved(), 0);
    assert_eq!(second.reserved(), 0);
    println!(
        "{{\"buffer_bytes\":{bytes},\"pool_capacity\":1,\"available_after_claim\":{over_limit_available},\"fallible_admission_refused\":true,\"same_owner_shared_once\":true,\"cross_owner_reservation_transferred\":true,\"released_after_last_alias\":true}}"
    );
}
