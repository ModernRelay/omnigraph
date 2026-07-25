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
#[allow(dead_code)]
struct RetryState {
    attempts: u32,
    next_at: tokio::time::Instant,
}

pub(crate) struct GraphSupervisor {
    #[allow(dead_code)]
    pub(crate) registry: Arc<GraphRegistry>,
    #[allow(dead_code)]
    pub(crate) configs: Arc<HashMap<GraphKey, GraphStartupConfig>>,
    #[allow(dead_code)]
    pub(crate) config: SupervisorConfig,
    pub(crate) rx: mpsc::UnboundedReceiver<GraphKey>,
}

impl GraphSupervisor {
    /// Scaffolding: drain-only. Receives (and discards) reopen requests so
    /// the notify chokepoint has a live receiver; performs no reopen yet.
    /// The RFC-029 W3+W2(b) loop replaces this body behind its red tests.
    pub(crate) async fn run(mut self) {
        while self.rx.recv().await.is_some() {}
    }
}
