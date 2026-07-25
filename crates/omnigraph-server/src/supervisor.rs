//! RFC-029 unified graph supervision (W3 boot retry + W2(b) supervised
//! reopen).
//!
//! One server-owned task with per-graph retry state, fed by two triggers:
//! quarantine entries seeded at boot (a graph whose open failed), and
//! `RecoveryRequired` notifications from the shielded write path (a served
//! graph carrying an unresolved rollback-class recovery residual). In both
//! cases the action is identical: re-drive the full `open_single_graph` —
//! whose read-write open runs the engine's Full recovery sweep under the
//! shared root-scoped write queue — and RCU-publish the healed handle into
//! the registry. In-flight requests on a replaced handle finish on their own
//! `Arc` clone (the registry's engine-survival contract).
//!
//! Deliberately ONE task, not one per graph: a single loop dedups
//! notification storms for the same graph and guarantees at most one
//! in-flight open per root (two concurrent read-write opens of one root
//! would run redundant Full sweeps). Shutdown is free: the only sender
//! lives in `AppState`; when the state drops, `recv()` yields `None` and
//! the loop exits.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::identity::GraphKey;
use crate::registry::GraphRegistry;
use crate::GraphStartupConfig;

/// Retry pacing for the supervision loop. Production uses capped
/// exponential backoff with jitter; tests inject millisecond-scale
/// timings so convergence tests complete in bounded wall-clock.
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    pub initial_backoff: Duration,
    pub multiplier: u32,
    pub max_backoff: Duration,
    /// Jitter as a fraction of the computed delay (0.0 disables).
    pub jitter: f64,
}

impl SupervisorConfig {
    pub fn production() -> Self {
        Self {
            initial_backoff: Duration::from_secs(5),
            multiplier: 2,
            max_backoff: Duration::from_secs(600),
            jitter: 0.1,
        }
    }

    /// Millisecond-scale pacing for in-process tests.
    pub fn fast_for_tests() -> Self {
        Self {
            initial_backoff: Duration::from_millis(10),
            multiplier: 2,
            max_backoff: Duration::from_millis(200),
            jitter: 0.0,
        }
    }
}

/// Per-graph retry bookkeeping.
struct RetryState {
    attempts: u32,
    next_at: tokio::time::Instant,
}

pub(crate) struct GraphSupervisor {
    pub(crate) registry: Arc<GraphRegistry>,
    pub(crate) configs: Arc<HashMap<GraphKey, GraphStartupConfig>>,
    pub(crate) config: SupervisorConfig,
    pub(crate) rx: mpsc::UnboundedReceiver<GraphKey>,
}

impl GraphSupervisor {
    /// The RFC-029 supervision loop. Seeds retry state from the registry's
    /// boot-time quarantine entries (trigger A), then selects over reopen
    /// notifications (trigger B — retried immediately, deduped while
    /// pending) and the earliest scheduled retry. A due graph gets one full
    /// `open_single_graph`; success RCU-publishes the healed handle
    /// (clearing quarantine or swapping the serving handle), failure
    /// reschedules with capped exponential backoff and refreshes the
    /// quarantine record (a no-op while the graph still serves, so a failed
    /// W2(b) reopen never takes a healthy graph down).
    pub(crate) async fn run(mut self) {
        use tokio::time::{Instant, sleep_until};
        use tracing::{info, warn};

        let mut pending: HashMap<GraphKey, RetryState> = HashMap::new();
        for key in self.registry.snapshot_ref().quarantined.keys() {
            pending.insert(
                key.clone(),
                RetryState {
                    attempts: 1,
                    next_at: Instant::now(),
                },
            );
        }
        loop {
            // With nothing pending, park solely on the channel; the
            // far-future fallback only exists to give select! a future.
            let next_due = pending.values().map(|r| r.next_at).min();
            let deadline =
                next_due.unwrap_or_else(|| Instant::now() + Duration::from_secs(24 * 3600));
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        // The only sender lives in AppState — its drop is
                        // server shutdown.
                        None => return,
                        Some(key) => {
                            // Dedup: an already-pending graph keeps its
                            // schedule; a fresh trigger-B request retries
                            // immediately.
                            pending.entry(key).or_insert(RetryState {
                                attempts: 0,
                                next_at: Instant::now(),
                            });
                        }
                    }
                }
                _ = sleep_until(deadline), if next_due.is_some() => {
                    let now = Instant::now();
                    let due: Vec<GraphKey> = pending
                        .iter()
                        .filter(|(_, r)| r.next_at <= now)
                        .map(|(k, _)| k.clone())
                        .collect();
                    for key in due {
                        let Some(cfg) = self.configs.get(&key) else {
                            // No retained config (single mode, or a key from
                            // a foreign source) — nothing to reopen from.
                            warn!(graph_id = %key.graph_id, "no startup config retained; dropping supervision");
                            pending.remove(&key);
                            continue;
                        };
                        match crate::open_single_graph(cfg.clone()).await {
                            Ok(handle) => match self.registry.publish(handle).await {
                                Ok(()) => {
                                    info!(graph_id = %key.graph_id, "supervised reopen healed graph");
                                    pending.remove(&key);
                                }
                                Err(err) => {
                                    warn!(graph_id = %key.graph_id, error = %err, "supervised reopen could not publish");
                                    self.reschedule(&key, cfg, err.to_string(), &mut pending).await;
                                }
                            },
                            Err(err) => {
                                warn!(graph_id = %key.graph_id, error = %err, "supervised reopen failed");
                                self.reschedule(&key, cfg, err.to_string(), &mut pending).await;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Backoff-reschedule `key` and refresh its quarantine record.
    async fn reschedule(
        &self,
        key: &GraphKey,
        cfg: &GraphStartupConfig,
        last_error: String,
        pending: &mut HashMap<GraphKey, RetryState>,
    ) {
        let Some(state) = pending.get_mut(key) else {
            return;
        };
        state.attempts = state.attempts.saturating_add(1);
        let exp = state.attempts.saturating_sub(1).min(20);
        let base = self
            .config
            .initial_backoff
            .saturating_mul(self.config.multiplier.saturating_pow(exp))
            .min(self.config.max_backoff);
        // Cheap deterministic-enough jitter without a rand dependency:
        // subsecond nanos spread retries within ±jitter of the base delay.
        let frac = f64::from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos())
                .unwrap_or(0)
                % 1000,
        ) / 500.0
            - 1.0;
        let delay = base.mul_f64((1.0 + self.config.jitter * frac).max(0.0));
        state.next_at = tokio::time::Instant::now() + delay;

        let now_sys = std::time::SystemTime::now();
        let since = self
            .registry
            .snapshot_ref()
            .quarantined
            .get(key)
            .map(|q| q.since)
            .unwrap_or(now_sys);
        self.registry
            .set_quarantined(
                key.clone(),
                crate::QuarantineInfo {
                    uri: cfg.uri.clone(),
                    since,
                    attempts: state.attempts,
                    last_error,
                    retry_at: now_sys + delay,
                    policy_configured: cfg.policy.is_some(),
                },
            )
            .await;
    }
}
