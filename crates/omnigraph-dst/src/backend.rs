//! The `Backend` abstraction that lets the seeded walk drive any execution
//! context (embedded SDK, CLI subprocess) through one op surface, plus the two
//! concrete backends.
//!
//! `Backend` is **black-box** by construction: `load`/`mutate`/`query`/
//! `optimize`/`repair` plus a normalized `query` row shape — no engine handle.
//! That is the only surface a non-embedded context (the CLI) can satisfy. The
//! white-box invariant battery (`invariants.rs`) needs the real `Omnigraph`
//! handle, so it stays embedded-only via `Embedded::db()`; a cross-backend walk
//! runs the black-box oracles only.

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

use async_trait::async_trait;
use omnigraph::db::{Omnigraph, ReadTarget, RepairOptions};
use omnigraph::error::OmniError;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;
use serde_json::Value;

use crate::op::SCHEMA;

/// An error from any backend op. Embedded ops carry the raw `OmniError`
/// (variant preserved for structured classification); CLI ops carry the exit
/// code + stderr (which surfaces the same engine error text out-of-process).
#[derive(Debug)]
pub enum BackendError {
    Engine(OmniError),
    Cli { code: Option<i32>, stderr: String },
}

impl BackendError {
    pub fn message(&self) -> String {
        match self {
            BackendError::Engine(e) => e.to_string(),
            BackendError::Cli { code, stderr } => format!("cli exit {code:?}: {stderr}"),
        }
    }
}

/// All `query`/`mutate` go through a single query named `q`, so the embedded
/// backend can name it and the CLI can infer it from a single-query `-e` source.
const QNAME: &str = "q";

/// The black-box op surface the walk drives. Every gq passed to `mutate`/`query`
/// must contain exactly one query named `q` (see `QNAME`). `query` returns
/// NORMALIZED rows: embedded via `to_rust_json`, CLI via parsed `--json`.
#[async_trait]
pub trait Backend: Send + Sync {
    /// Bulk-load JSONL in merge (upsert) mode — the only mode the walk uses.
    async fn load(&self, jsonl: &str) -> Result<(), BackendError>;
    /// Run a single-statement write (`query q() { insert|update|delete … }`).
    async fn mutate(&self, branch: &str, gq: &str) -> Result<(), BackendError>;
    /// Run a read (`query q() { match … return … }`) → normalized rows.
    async fn query(&self, branch: &str, gq: &str) -> Result<Vec<Value>, BackendError>;
    async fn optimize(&self) -> Result<(), BackendError>;
    async fn repair(&self) -> Result<(), BackendError>;
}

// ───────────────────────────── Embedded ─────────────────────────────

/// The in-process engine backend. Wraps a real `Omnigraph` (optionally over a
/// `FaultAdapter`) and additionally exposes the handle via `db()` so the
/// white-box battery can run on top of it.
pub struct Embedded {
    db: Omnigraph,
}

impl Embedded {
    /// A clean graph (no fault injection).
    pub async fn open_clean(uri: &str) -> Self {
        Embedded {
            db: Omnigraph::init(uri, SCHEMA).await.expect("init"),
        }
    }

    /// A graph whose storage injects seeded manifest-layer faults (CAS-lost).
    /// Created cleanly first, then reopened through the `FaultAdapter` so only
    /// the op workload runs under faults.
    pub async fn open_faulted(uri: &str, seed: u64, cas_pct: u8) -> Self {
        Omnigraph::init(uri, SCHEMA).await.expect("init");
        let base = omnigraph::storage::storage_for_uri(uri).expect("storage_for_uri");
        let faulted = crate::fault::FaultAdapter::new(base, seed, cas_pct);
        Embedded {
            db: Omnigraph::open_with_storage(uri, faulted)
                .await
                .expect("open_with_storage"),
        }
    }

    /// Reopen an EXISTING graph from disk (runs the open-time recovery sweep).
    /// Backs the reopen==pre_state durability oracle.
    pub async fn reopen(uri: &str) -> Self {
        Embedded {
            db: Omnigraph::open(uri).await.expect("reopen"),
        }
    }

    /// White-box access to the underlying handle (embedded-only; the invariant
    /// battery reaches `Snapshot::open`→`Dataset`, `_rowid` scans through it).
    pub fn db(&self) -> &Omnigraph {
        &self.db
    }
}

#[async_trait]
impl Backend for Embedded {
    async fn load(&self, jsonl: &str) -> Result<(), BackendError> {
        load_jsonl(&self.db, jsonl, LoadMode::Merge)
            .await
            .map(|_| ())
            .map_err(BackendError::Engine)
    }
    async fn mutate(&self, branch: &str, gq: &str) -> Result<(), BackendError> {
        self.db
            .mutate(branch, gq, QNAME, &ParamMap::new())
            .await
            .map(|_| ())
            .map_err(BackendError::Engine)
    }
    async fn query(&self, branch: &str, gq: &str) -> Result<Vec<Value>, BackendError> {
        let res = self
            .db
            .query(ReadTarget::branch(branch), gq, QNAME, &ParamMap::new())
            .await
            .map_err(BackendError::Engine)?;
        let json = res.to_rust_json();
        Ok(json.as_array().cloned().unwrap_or_default())
    }
    async fn optimize(&self) -> Result<(), BackendError> {
        self.db.optimize().await.map(|_| ()).map_err(BackendError::Engine)
    }
    async fn repair(&self) -> Result<(), BackendError> {
        // confirm (not force): heals VERIFIED maintenance drift, leaves
        // suspicious/semantic drift (RC-1's) for head_eq_manifest to catch.
        let opts = RepairOptions {
            confirm: true,
            force: false,
        };
        self.db.repair(opts).await.map(|_| ()).map_err(BackendError::Engine)
    }
}

// ─────────────────────────────── Cli ────────────────────────────────

/// The CLI subprocess backend. Shells out to a built `omnigraph` binary against
/// a local `--store` graph. The binary path is injected by the consumer (e.g.
/// `env!("CARGO_BIN_EXE_omnigraph")`) — no `assert_cmd` in this library.
pub struct Cli {
    bin: PathBuf,
    store_uri: String,
    actor: String,
}

impl Cli {
    pub fn new(bin: PathBuf, store_uri: impl Into<String>) -> Self {
        Cli {
            bin,
            store_uri: store_uri.into(),
            actor: "act-dst".to_string(),
        }
    }

    fn io_err(e: std::io::Error) -> BackendError {
        BackendError::Cli {
            code: None,
            stderr: e.to_string(),
        }
    }

    /// Run the binary, returning its `Output` (does NOT check the exit status).
    fn run(&self, args: &[&str]) -> Result<std::process::Output, BackendError> {
        Command::new(&self.bin).args(args).output().map_err(|e| BackendError::Cli {
            code: None,
            stderr: format!("spawn {}: {e}", self.bin.display()),
        })
    }

    /// Run and require success, else map to a `Cli` error with stderr.
    fn run_ok(&self, args: &[&str]) -> Result<std::process::Output, BackendError> {
        let out = self.run(args)?;
        if !out.status.success() {
            return Err(BackendError::Cli {
                code: out.status.code(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            });
        }
        Ok(out)
    }

    fn temp(&self, suffix: &str, contents: &str) -> Result<tempfile::NamedTempFile, BackendError> {
        let mut f = tempfile::Builder::new()
            .suffix(suffix)
            .tempfile()
            .map_err(Self::io_err)?;
        f.write_all(contents.as_bytes()).map_err(Self::io_err)?;
        f.flush().map_err(Self::io_err)?;
        Ok(f)
    }

    /// `init --schema <SCHEMA> <store_uri>` — call once before the walk.
    pub async fn init(&self) -> Result<(), BackendError> {
        let f = self.temp(".pg", SCHEMA)?;
        let path = f.path().to_string_lossy().into_owned();
        self.run_ok(&["init", "--schema", &path, &self.store_uri]).map(|_| ())
    }
}

#[async_trait]
impl Backend for Cli {
    async fn load(&self, jsonl: &str) -> Result<(), BackendError> {
        let f = self.temp(".jsonl", jsonl)?;
        let path = f.path().to_string_lossy().into_owned();
        self.run_ok(&[
            "load", "--data", &path, "--mode", "merge", "--store", &self.store_uri, "--as",
            &self.actor,
        ])
        .map(|_| ())
    }
    async fn mutate(&self, _branch: &str, gq: &str) -> Result<(), BackendError> {
        // Local store is always `main`; the CLI infers the single query from -e.
        self.run_ok(&["mutate", "-e", gq, "--store", &self.store_uri, "--as", &self.actor])
            .map(|_| ())
    }
    async fn query(&self, _branch: &str, gq: &str) -> Result<Vec<Value>, BackendError> {
        let out = self.run_ok(&[
            "query", "-e", gq, "--json", "--store", &self.store_uri, "--as", &self.actor,
        ])?;
        let stdout = String::from_utf8_lossy(&out.stdout);
        let trimmed = stdout.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }
        let v: Value = serde_json::from_str(trimmed).map_err(|e| BackendError::Cli {
            code: out.status.code(),
            stderr: format!("json parse: {e}: {stdout}"),
        })?;
        let rows = match &v {
            Value::Array(a) => a.clone(),
            Value::Object(o) => o
                .get("rows")
                .and_then(|r| r.as_array())
                .cloned()
                .unwrap_or_else(|| vec![v.clone()]),
            _ => Vec::new(),
        };
        Ok(rows)
    }
    async fn optimize(&self) -> Result<(), BackendError> {
        self.run_ok(&["optimize", "--store", &self.store_uri]).map(|_| ())
    }
    async fn repair(&self) -> Result<(), BackendError> {
        self.run_ok(&["repair", "--confirm", "--store", &self.store_uri]).map(|_| ())
    }
}
