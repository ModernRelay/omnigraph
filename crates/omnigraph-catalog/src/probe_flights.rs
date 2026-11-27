//! Concurrent callers of one `__manifest` freshness probe share it.
//!
//! A warm read pays one probe to learn whether the head it holds is still the
//! branch's latest (`read:2`). Reads that overlap on one held view ask the same
//! question, so they share one answer, under one rule: a caller only takes an
//! answer from a probe that started after it arrived. A probe already running
//! when a caller arrives may have read the store before a commit the caller
//! saw acknowledged, so the caller waits for the next probe instead, which
//! starts once the running one finishes. Every caller who arrives while the
//! running probe is in flight joins that same next probe. A lone caller starts
//! its probe at once. Under load this is about one probe per probe round trip,
//! however many readers there are.
//!
//! A flight runs while some caller polls it: a caller that stops waiting never
//! cancels it for the others, and a flight nobody waits for is dropped, so the
//! next caller starts a fresh one. An error is not shared: the first caller to
//! see a failed flight takes its error, and every other caller of that flight
//! probes again on its own, a probe that also started after it arrived, so each
//! caller receives its own typed outcome.
//!
//! Nothing here is a lock across processes or a clock: the table holds at most
//! one waiting flight and one running flight for each held view that callers
//! are probing now, and forgets a view once its flights end.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use futures::FutureExt;
use futures::future::{BoxFuture, Shared, WeakShared};

/// A flight's error, taken by the first caller that sees it.
type ErrorSlot<E> = Arc<Mutex<Option<E>>>;

type Flight<T, E> = Shared<BoxFuture<'static, Result<T, ErrorSlot<E>>>>;

/// The newest flight for one key: the one a caller arriving now may join,
/// while it has not started its probe.
struct Tail<T, E> {
    id: u64,
    flight: WeakShared<BoxFuture<'static, Result<T, ErrorSlot<E>>>>,
    started: bool,
}

struct Table<K, T, E> {
    next_id: u64,
    tails: HashMap<K, Tail<T, E>>,
}

/// The flights of the probes callers are waiting on, by the view they probe.
pub(crate) struct ProbeFlights<K, T, E> {
    table: Arc<Mutex<Table<K, T, E>>>,
}

impl<K, T, E> Default for ProbeFlights<K, T, E> {
    fn default() -> Self {
        Self {
            table: Arc::new(Mutex::new(Table {
                next_id: 0,
                tails: HashMap::new(),
            })),
        }
    }
}

impl<K, T, E> std::fmt::Debug for ProbeFlights<K, T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeFlights").finish_non_exhaustive()
    }
}

fn lock<K, T, E>(table: &Mutex<Table<K, T, E>>) -> MutexGuard<'_, Table<K, T, E>> {
    table.lock().unwrap_or_else(PoisonError::into_inner)
}

impl<K, T, E> ProbeFlights<K, T, E>
where
    K: Hash + Eq + Clone + Send + 'static,
    T: Clone + Send + Sync + 'static,
    E: Send + 'static,
{
    /// The answer of a `probe` of `key` that started after this call: the
    /// next flight of `key` that has not started, joined, or a new one.
    pub(crate) async fn probe<P, Fut>(&self, key: K, probe: P) -> Result<T, E>
    where
        P: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        let probe = Arc::new(probe);
        let flight = self.join_or_start(key, Arc::clone(&probe));
        match flight.await {
            Ok(answer) => Ok(answer),
            Err(slot) => {
                let taken = slot.lock().unwrap_or_else(PoisonError::into_inner).take();
                match taken {
                    Some(error) => Err(error),
                    None => probe().await,
                }
            }
        }
    }

    /// The flight this caller waits on, chosen in one critical section, so
    /// whether a flight has started is decided against this arrival.
    fn join_or_start<P, Fut>(&self, key: K, probe: Arc<P>) -> Flight<T, E>
    where
        P: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        let mut table = lock(&self.table);
        // A flight every caller stopped waiting for is gone; forget its key.
        table
            .tails
            .retain(|_, tail| tail.flight.upgrade().is_some());
        let previous = match table.tails.get(&key) {
            Some(tail) if !tail.started => {
                return tail
                    .flight
                    .upgrade()
                    .expect("a live tail survives the retain above");
            }
            Some(tail) => tail.flight.upgrade(),
            None => None,
        };
        let id = table.next_id;
        table.next_id += 1;
        let flight = Self::fly(Arc::clone(&self.table), key.clone(), id, previous, probe)
            .boxed()
            .shared();
        let weak = flight
            .downgrade()
            .expect("a flight that was never polled has not completed");
        table.tails.insert(
            key,
            Tail {
                id,
                flight: weak,
                started: false,
            },
        );
        flight
    }

    /// One flight: wait out the running flight, close this one to new
    /// callers, probe, and forget the key once no newer flight replaced it.
    async fn fly<P, Fut>(
        table: Arc<Mutex<Table<K, T, E>>>,
        key: K,
        id: u64,
        previous: Option<Flight<T, E>>,
        probe: Arc<P>,
    ) -> Result<T, ErrorSlot<E>>
    where
        P: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        if let Some(previous) = previous {
            let _ = previous.await;
        }
        if let Some(tail) = lock(&table).tails.get_mut(&key)
            && tail.id == id
        {
            tail.started = true;
        }
        let answer = probe().await;
        {
            let mut table = lock(&table);
            if table.tails.get(&key).is_some_and(|tail| tail.id == id) {
                table.tails.remove(&key);
            }
        }
        answer.map_err(|error| Arc::new(Mutex::new(Some(error))))
    }

    /// Keys with a flight some caller still waits on.
    #[cfg(test)]
    fn live_keys(&self) -> usize {
        let mut table = lock(&self.table);
        table
            .tails
            .retain(|_, tail| tail.flight.upgrade().is_some());
        table.tails.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU64, Ordering};
    use std::task::Poll;

    use futures::poll;
    use tokio::sync::Semaphore;

    /// A probe whose runs are counted and each held until the test releases
    /// one permit; its answer is the store's value when it started.
    #[derive(Clone)]
    struct Gated {
        started: Arc<AtomicU64>,
        gate: Arc<Semaphore>,
        store: Arc<AtomicU64>,
    }

    impl Gated {
        fn new() -> Self {
            Self {
                started: Arc::new(AtomicU64::new(0)),
                gate: Arc::new(Semaphore::new(0)),
                store: Arc::new(AtomicU64::new(0)),
            }
        }

        fn probe(
            &self,
        ) -> impl Fn() -> BoxFuture<'static, Result<u64, String>> + Send + Sync + use<> {
            let this = self.clone();
            move || {
                let this = this.clone();
                async move {
                    this.started.fetch_add(1, Ordering::SeqCst);
                    let seen = this.store.load(Ordering::SeqCst);
                    this.gate.acquire().await.unwrap().forget();
                    if seen == u64::MAX {
                        return Err("store unavailable".to_string());
                    }
                    Ok(seen)
                }
                .boxed()
            }
        }

        fn started(&self) -> u64 {
            self.started.load(Ordering::SeqCst)
        }
    }

    // contract: read:2
    #[tokio::test]
    async fn a_lone_caller_probes_at_once() {
        let flights = ProbeFlights::<&str, u64, String>::default();
        let gated = Gated::new();
        gated.gate.add_permits(1);
        assert_eq!(flights.probe("main", gated.probe()).await, Ok(0));
        assert_eq!(gated.started(), 1);
        assert_eq!(flights.live_keys(), 0);
        // Sequential callers never share: each starts its own probe.
        gated.gate.add_permits(1);
        assert_eq!(flights.probe("main", gated.probe()).await, Ok(0));
        assert_eq!(gated.started(), 2);
    }

    // contract: read:2
    #[tokio::test]
    async fn callers_during_a_probe_share_the_next_one_and_see_what_preceded_them() {
        let flights = ProbeFlights::<&str, u64, String>::default();
        let gated = Gated::new();

        // The first caller starts a probe, which reads the store at 0.
        let mut first = Box::pin(flights.probe("main", gated.probe()));
        assert!(poll!(&mut first).is_pending());
        assert_eq!(gated.started(), 1);

        // A commit lands and is acknowledged; then many callers arrive while
        // the first probe is still in flight.
        gated.store.store(1, Ordering::SeqCst);
        let mut later: Vec<_> = (0..16)
            .map(|_| Box::pin(flights.probe("main", gated.probe())))
            .collect();
        for caller in &mut later {
            assert!(poll!(caller).is_pending());
        }
        // None of them started a probe or joined the running one.
        assert_eq!(gated.started(), 1);

        // The first probe answers what it read; the next one starts only now.
        gated.gate.add_permits(1);
        assert_eq!(first.await, Ok(0));
        for caller in &mut later {
            assert!(poll!(caller).is_pending());
        }
        assert_eq!(gated.started(), 2);

        // Every later caller sees the commit that preceded its arrival, from
        // one shared probe.
        gated.gate.add_permits(1);
        for caller in later {
            assert_eq!(caller.await, Ok(1));
        }
        assert_eq!(gated.started(), 2);
        assert_eq!(flights.live_keys(), 0);
    }

    #[tokio::test]
    async fn a_caller_after_the_next_probe_started_waits_for_a_third() {
        let flights = ProbeFlights::<&str, u64, String>::default();
        let gated = Gated::new();
        let mut first = Box::pin(flights.probe("main", gated.probe()));
        assert!(poll!(&mut first).is_pending());
        let mut second = Box::pin(flights.probe("main", gated.probe()));
        assert!(poll!(&mut second).is_pending());
        gated.gate.add_permits(1);
        assert_eq!(first.await, Ok(0));
        assert!(poll!(&mut second).is_pending());
        assert_eq!(gated.started(), 2);

        // The second probe is running; a commit then a third caller arrive.
        gated.store.store(7, Ordering::SeqCst);
        let mut third = Box::pin(flights.probe("main", gated.probe()));
        assert!(poll!(&mut third).is_pending());
        assert_eq!(gated.started(), 2);
        gated.gate.add_permits(1);
        assert_eq!(second.await, Ok(0));
        assert!(poll!(&mut third).is_pending());
        assert_eq!(gated.started(), 3);
        gated.gate.add_permits(1);
        assert_eq!(third.await, Ok(7));
    }

    #[tokio::test]
    async fn distinct_keys_never_share() {
        let flights = ProbeFlights::<&str, u64, String>::default();
        let gated = Gated::new();
        let mut main = Box::pin(flights.probe("main", gated.probe()));
        let mut feature = Box::pin(flights.probe("feature", gated.probe()));
        assert!(poll!(&mut main).is_pending());
        assert!(poll!(&mut feature).is_pending());
        assert_eq!(gated.started(), 2);
        gated.gate.add_permits(2);
        assert_eq!(main.await, Ok(0));
        assert_eq!(feature.await, Ok(0));
    }

    #[tokio::test]
    async fn a_dropped_caller_does_not_cancel_the_flight_for_the_others() {
        let flights = ProbeFlights::<&str, u64, String>::default();
        let gated = Gated::new();
        let mut first = Box::pin(flights.probe("main", gated.probe()));
        assert!(poll!(&mut first).is_pending());
        let mut joined: Vec<_> = (0..2)
            .map(|_| Box::pin(flights.probe("main", gated.probe())))
            .collect();
        for caller in &mut joined {
            assert!(poll!(caller).is_pending());
        }
        gated.gate.add_permits(1);
        assert_eq!(first.await, Ok(0));
        // Both wait on the next flight, which has started; one gives up.
        for caller in &mut joined {
            assert!(poll!(caller).is_pending());
        }
        assert_eq!(gated.started(), 2);
        let survivor = joined.pop().unwrap();
        drop(joined);
        gated.store.store(3, Ordering::SeqCst);
        gated.gate.add_permits(1);
        // The survivor gets the shared flight's answer, which read the store
        // before the drop; no new probe ran for it.
        assert_eq!(survivor.await, Ok(0));
        assert_eq!(gated.started(), 2);
        assert_eq!(flights.live_keys(), 0);
    }

    #[tokio::test]
    async fn a_flight_every_caller_abandoned_is_forgotten() {
        let flights = ProbeFlights::<&str, u64, String>::default();
        let gated = Gated::new();
        let mut first = Box::pin(flights.probe("main", gated.probe()));
        assert!(poll!(&mut first).is_pending());
        drop(first);
        assert_eq!(flights.live_keys(), 0);
        // The next caller starts a fresh probe instead of waiting on it.
        gated.store.store(5, Ordering::SeqCst);
        gated.gate.add_permits(1);
        assert_eq!(flights.probe("main", gated.probe()).await, Ok(5));
        assert_eq!(gated.started(), 2);
    }

    #[tokio::test]
    async fn every_caller_of_a_failed_flight_receives_an_error_of_its_own() {
        let flights = ProbeFlights::<&str, u64, String>::default();
        let gated = Gated::new();
        let mut first = Box::pin(flights.probe("main", gated.probe()));
        assert!(poll!(&mut first).is_pending());
        let mut joined: Vec<_> = (0..3)
            .map(|_| Box::pin(flights.probe("main", gated.probe())))
            .collect();
        for caller in &mut joined {
            assert!(poll!(caller).is_pending());
        }
        // The store fails from now on: the running probe already read it, and
        // the shared one the joined callers wait for reads it after this.
        gated.store.store(u64::MAX, Ordering::SeqCst);
        gated.gate.add_permits(1);
        assert_eq!(first.await, Ok(0));
        for caller in &mut joined {
            assert!(poll!(caller).is_pending());
        }
        // The shared flight fails; one caller takes its error and the other
        // two probe again on their own, failing too.
        gated.gate.add_permits(1);
        let mut outcomes = Vec::new();
        for caller in &mut joined {
            let outcome = loop {
                match poll!(&mut *caller) {
                    Poll::Ready(outcome) => break outcome,
                    Poll::Pending => {
                        gated.gate.add_permits(1);
                        tokio::task::yield_now().await;
                    }
                }
            };
            outcomes.push(outcome);
        }
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.as_ref().map_err(String::as_str) == Err("store unavailable"))
        );
        // The shared flight, then one probe each for the two that did not
        // take its error.
        assert_eq!(gated.started(), 2 + 2);
    }
}
