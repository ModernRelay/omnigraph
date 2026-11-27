//! Concurrent callers of one `__manifest` freshness probe share it.
//!
//! A warm read pays one probe to learn whether the head it holds is still the
//! branch's latest (`read:2`). Reads that overlap on one held view ask the same
//! question, so they may share one answer, under one rule: a caller only takes
//! the answer of a probe that asks the store after it arrived, and it never
//! waits on a probe that started before it arrived.
//!
//! The caller that creates a flight yields to the scheduler once, in its own
//! future, before the flight asks the store; a caller polling the shared
//! flight meanwhile cannot start it early. A caller that arrives in that
//! window, while a flight for its view has not started, joins it; a caller
//! that arrives after the flight started starts a flight of its own at once,
//! so probes of one view may run concurrently and a stalled probe holds up
//! only the callers that joined it before it started. Reads that a server
//! admits together, or that one task issues together, share one probe; a lone
//! caller pays one scheduler yield. If the creator stops waiting, the flight
//! starts for the callers that joined it.
//!
//! A flight runs while some caller polls it: a caller that stops waiting never
//! cancels it for the others, and a flight nobody waits for is dropped, so the
//! next caller starts a fresh one. An error is not shared: the first caller to
//! see a failed flight takes its error, and every other caller of that flight
//! probes again on its own, a probe that also starts after it arrived, so each
//! caller receives its own typed outcome.
//!
//! Nothing here is a lock across processes or a clock: the table holds at most
//! one flight that has not started for each view, and a flight leaves the table
//! when it starts.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use futures::FutureExt;
use futures::future::{BoxFuture, Shared, WeakShared};
use tokio::sync::Notify;

/// A flight's error, taken by the first caller that sees it.
type ErrorSlot<E> = Arc<Mutex<Option<E>>>;

type FlightFuture<T, E> = BoxFuture<'static, Result<T, ErrorSlot<E>>>;

/// A flight that has not asked the store yet, which a caller may still join.
struct Unstarted<T, E> {
    id: u64,
    flight: WeakShared<FlightFuture<T, E>>,
}

/// Held by the caller that created a flight; dropping it lets the flight
/// start. `notify_one` stores a permit, so a start is never lost.
struct Starter(Arc<Notify>);

impl Drop for Starter {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

struct Table<K, T, E> {
    next_id: u64,
    unstarted: HashMap<K, Unstarted<T, E>>,
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
                unstarted: HashMap::new(),
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
    /// The answer of a `probe` of `key` that asks the store after this call:
    /// the flight of `key` that has not started, joined, or a new one.
    pub(crate) async fn probe<P, Fut>(&self, key: K, probe: P) -> Result<T, E>
    where
        P: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        let probe = Arc::new(probe);
        let (flight, starter) = self.join_or_start(key, Arc::clone(&probe));
        if let Some(starter) = starter {
            // The caller that created the flight yields once in its own future,
            // so a joined caller polling the flight cannot end the yield early,
            // then lets the flight start (or does so by leaving).
            tokio::task::yield_now().await;
            drop(starter);
        }
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

    /// The flight this caller waits on, chosen in one critical section with
    /// the flight's start, so a caller joins only a flight that asks the store
    /// after it arrived. A caller that creates the flight also receives the
    /// [`Starter`] that lets it start.
    fn join_or_start<P, Fut>(
        &self,
        key: K,
        probe: Arc<P>,
    ) -> (Shared<FlightFuture<T, E>>, Option<Starter>)
    where
        P: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        let mut table = lock(&self.table);
        // One upgrade decides: a flight every caller dropped is gone, and its
        // entry is replaced below.
        if let Some(flight) = table
            .unstarted
            .get(&key)
            .and_then(|unstarted| unstarted.flight.upgrade())
        {
            return (flight, None);
        }
        let id = table.next_id;
        table.next_id += 1;
        let go = Arc::new(Notify::new());
        let flight = Self::fly(
            Arc::clone(&self.table),
            key.clone(),
            id,
            Arc::clone(&go),
            probe,
        )
        .boxed()
        .shared();
        let weak = flight
            .downgrade()
            .expect("a flight that was never polled has not completed");
        table.unstarted.insert(key, Unstarted { id, flight: weak });
        (flight, Some(Starter(go)))
    }

    /// One flight: wait until its creator lets it start, so callers arriving
    /// meanwhile join, then leave the table (later callers start their own)
    /// and probe.
    async fn fly<P, Fut>(
        table: Arc<Mutex<Table<K, T, E>>>,
        key: K,
        id: u64,
        go: Arc<Notify>,
        probe: Arc<P>,
    ) -> Result<T, ErrorSlot<E>>
    where
        P: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        go.notified().await;
        {
            let mut table = lock(&table);
            if table
                .unstarted
                .get(&key)
                .is_some_and(|unstarted| unstarted.id == id)
            {
                table.unstarted.remove(&key);
            }
        }
        probe()
            .await
            .map_err(|error| Arc::new(Mutex::new(Some(error))))
    }

    /// Keys with an unstarted flight some caller still waits on.
    #[cfg(test)]
    fn joinable_keys(&self) -> usize {
        lock(&self.table)
            .unstarted
            .values()
            .filter(|unstarted| unstarted.flight.upgrade().is_some())
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU64, Ordering};

    use futures::poll;
    use tokio::sync::Semaphore;

    /// A probe whose runs are counted and whose answer is the store's value
    /// when it asked. The run numbered `park`, if any, waits for a permit
    /// after reading the store, as a probe stalled in its request does.
    #[derive(Clone)]
    struct Probe {
        started: Arc<AtomicU64>,
        park: Option<u64>,
        gate: Arc<Semaphore>,
        store: Arc<AtomicU64>,
    }

    impl Probe {
        fn new(park: Option<u64>) -> Self {
            Self {
                started: Arc::new(AtomicU64::new(0)),
                park,
                gate: Arc::new(Semaphore::new(0)),
                store: Arc::new(AtomicU64::new(0)),
            }
        }

        fn run(
            &self,
        ) -> impl Fn() -> BoxFuture<'static, Result<u64, String>> + Send + Sync + use<> {
            let this = self.clone();
            move || {
                let this = this.clone();
                async move {
                    let run = this.started.fetch_add(1, Ordering::SeqCst);
                    let seen = this.store.load(Ordering::SeqCst);
                    if this.park == Some(run) {
                        this.gate.acquire().await.unwrap().forget();
                    }
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
    async fn a_lone_caller_probes_and_sequential_callers_never_share() {
        let flights = ProbeFlights::<&str, u64, String>::default();
        let probe = Probe::new(None);
        assert_eq!(flights.probe("main", probe.run()).await, Ok(0));
        probe.store.store(1, Ordering::SeqCst);
        assert_eq!(flights.probe("main", probe.run()).await, Ok(1));
        assert_eq!(probe.started(), 2);
        assert_eq!(flights.joinable_keys(), 0);
    }

    // contract: read:2
    #[tokio::test]
    async fn callers_arriving_before_a_probe_starts_share_it() {
        let flights = ProbeFlights::<&str, u64, String>::default();
        let probe = Probe::new(None);
        let mut callers: Vec<_> = (0..16)
            .map(|_| Box::pin(flights.probe("main", probe.run())))
            .collect();
        // The first caller's flight yields once before it asks the store;
        // every caller polled in that window joins it.
        for caller in &mut callers {
            assert!(poll!(caller).is_pending());
        }
        assert_eq!(probe.started(), 0);
        // A commit lands before the shared probe asks: every caller sees it.
        probe.store.store(1, Ordering::SeqCst);
        for caller in callers {
            assert_eq!(caller.await, Ok(1));
        }
        assert_eq!(probe.started(), 1);
        assert_eq!(flights.joinable_keys(), 0);
    }

    // contract: read:2
    #[tokio::test]
    async fn a_caller_never_waits_on_a_probe_that_started_before_it() {
        let flights = ProbeFlights::<&str, u64, String>::default();
        // The first probe stalls in its request after reading the store.
        let probe = Probe::new(Some(0));
        let mut first = Box::pin(flights.probe("main", probe.run()));
        while probe.started() == 0 {
            assert!(poll!(&mut first).is_pending());
        }
        assert!(poll!(&mut first).is_pending());

        // A commit is acknowledged; later callers neither join the stalled
        // probe nor wait for it: one probe of their own answers them.
        probe.store.store(1, Ordering::SeqCst);
        let later: Vec<_> = (0..4).map(|_| flights.probe("main", probe.run())).collect();
        assert_eq!(
            futures::future::join_all(later).await,
            vec![Ok(1); 4],
            "the later callers finish while the first probe is still stalled"
        );
        assert_eq!(probe.started(), 2);
        assert!(poll!(&mut first).is_pending());

        probe.gate.add_permits(1);
        assert_eq!(first.await, Ok(0), "the stalled probe read the store first");
        assert_eq!(flights.joinable_keys(), 0);
    }

    #[tokio::test]
    async fn distinct_keys_never_share() {
        let flights = ProbeFlights::<&str, u64, String>::default();
        let probe = Probe::new(None);
        let main = flights.probe("main", probe.run());
        let feature = flights.probe("feature", probe.run());
        assert_eq!(futures::join!(main, feature), (Ok(0), Ok(0)));
        assert_eq!(probe.started(), 2);
    }

    #[tokio::test]
    async fn a_dropped_caller_does_not_cancel_the_flight_for_the_others() {
        let flights = ProbeFlights::<&str, u64, String>::default();
        let probe = Probe::new(None);
        let mut first = Box::pin(flights.probe("main", probe.run()));
        let mut second = Box::pin(flights.probe("main", probe.run()));
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut second).is_pending());
        // The caller that created the flight gives up; the joined one drives
        // the same flight to its answer.
        drop(first);
        assert_eq!(second.await, Ok(0));
        assert_eq!(probe.started(), 1);
    }

    /// The entry of a flight every caller dropped before it started is dead:
    /// the next caller's one upgrade fails and it starts a fresh flight. (The
    /// table never upgrades the entry twice, so a drop between two upgrades
    /// cannot make the second fail.)
    #[tokio::test]
    async fn a_flight_every_caller_abandoned_is_replaced() {
        let flights = ProbeFlights::<&str, u64, String>::default();
        let probe = Probe::new(None);
        let mut first = Box::pin(flights.probe("main", probe.run()));
        assert!(poll!(&mut first).is_pending());
        assert_eq!(flights.joinable_keys(), 1);
        drop(first);
        assert_eq!(flights.joinable_keys(), 0);
        probe.store.store(5, Ordering::SeqCst);
        assert_eq!(flights.probe("main", probe.run()).await, Ok(5));
        assert_eq!(probe.started(), 1, "the abandoned flight never asked");
        assert_eq!(flights.joinable_keys(), 0);
    }

    #[tokio::test]
    async fn every_caller_of_a_failed_flight_receives_an_error_of_its_own() {
        let flights = ProbeFlights::<&str, u64, String>::default();
        let probe = Probe::new(None);
        probe.store.store(u64::MAX, Ordering::SeqCst);
        let callers: Vec<_> = (0..3).map(|_| flights.probe("main", probe.run())).collect();
        let outcomes = futures::future::join_all(callers).await;
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.as_ref().map_err(String::as_str) == Err("store unavailable"))
        );
        // The shared flight, then one probe each for the two callers that did
        // not take its error.
        assert_eq!(probe.started(), 1 + 2);
    }
}
