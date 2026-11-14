//! The one owner of a write that prepares again.
//!
//! A mutation, a load and a Blob write prepare outside the writer gates and
//! revalidate under them. A publication that moved the branch in between
//! sends the attempt back with `ReadSetChanged` before any effect, unless
//! every publication since the capture changed no row (optimize, an index
//! build): revalidation then carries the attempt onto the current head
//! ([`Omnigraph::revalidate_write_txn_carrying`], reached by every one of
//! these writers through `StagedMutation::commit_all`), and the attempt goes
//! on. Lance's conflict rules still refuse to carry an update of a row in a
//! fragment a rewrite replaced; that attempt is sent back too.
//!
//! A write that was sent back prepares again from its request, whatever its
//! kind: it reruns its query, or rereads its input, on the current head, so
//! it is the write a caller resubmitting the same request would make, and an
//! update or delete reads the rows as they are when it publishes. A caller
//! whose write must apply to the head it read names that head
//! (`expected_head`), and a moved head is then its terminal precondition
//! failure. Which rows a write that selects existing rows (an update, a
//! delete, an overwrite load, a Blob put) touches depends on the branch
//! incarnation and the accepted schema it was compiled against, so such an
//! attempt that sees either differ from the first attempt's returns
//! `ReadSetChanged` instead ([`WriteAttempt::admit`]). A write that only adds
//! or upserts rows by key goes on, onto a branch created again under its name
//! or a schema an apply accepted meanwhile.
//!
//! The next attempt prepares with the branch's writer gates held, from its
//! capture through its publication ([`WriteAttempt`]). Every publisher on the
//! branch in this process, optimize included, takes the same branch gate, so
//! nothing can move the branch under that attempt: a write finishes in at
//! most two attempts, whatever sent it back and however often other writers
//! or maintenance run. The cost, one preparation under the gates, is paid only
//! after a send-back. A publication by another process is outside that bound
//! (one mutation-capable writer process is the supported topology); the
//! publisher's CAS refuses it, and the write prepares again within
//! [`MAX_PRE_EFFECT_REPREPARES`]. Taking all of this from
//! [`PreEffectReprepares`] keeps the next writer of this kind from forgetting
//! any of it.
//!
//! Preparation must take no writer gate of its own: the gates are not
//! reentrant, so a gated attempt that took one would wait on its own gate
//! forever. Each attempt therefore runs inside a preparation scope
//! ([`crate::db::write_queue::preparing`]), where every writer-gate
//! acquisition refuses with an internal error; [`WriteAttempt::admit`]
//! refuses an attempt outside one, and [`WriteAttempt::admitted_gates`], the
//! commit point, ends it.

use super::omnigraph::{Omnigraph, WriteTxn};
use super::write_queue::{self, BranchWriteGates};
use crate::error::{OmniError, Result};

/// How many times a write prepares again before its `ReadSetChanged` is the
/// caller's.
const MAX_PRE_EFFECT_REPREPARES: usize = 32;

/// What the first attempt of a write was compiled against, which every later
/// attempt must still see: the native branch incarnation and the accepted
/// schema.
#[derive(PartialEq, Eq)]
struct AttemptIdentity {
    branch_identifier: lance::dataset::refs::BranchIdentifier,
    schema_ir_hash: String,
    schema_identity_domain: String,
    schema_identity_version: u32,
}

impl AttemptIdentity {
    fn of(txn: &WriteTxn) -> Self {
        Self {
            branch_identifier: txn.authority.branch_identifier.clone(),
            schema_ir_hash: txn.authority.schema_ir_hash.clone(),
            schema_identity_domain: txn.authority.schema_identity_domain.clone(),
            schema_identity_version: txn.authority.schema_identity_version,
        }
    }

    /// The first member in which `self`, a later attempt's identity, differs
    /// from `first`, as the `ReadSetChanged` the caller receives.
    fn changed_since(&self, first: &Self, branch: Option<&str>) -> Option<OmniError> {
        let identifier = |identifier: &lance::dataset::refs::BranchIdentifier| {
            serde_json::to_string(identifier).ok()
        };
        if self.branch_identifier != first.branch_identifier {
            return Some(OmniError::manifest_read_set_changed(
                format!("branch_identifier:{}", branch.unwrap_or("main")),
                identifier(&first.branch_identifier),
                identifier(&self.branch_identifier),
            ));
        }
        if self.schema_ir_hash != first.schema_ir_hash {
            return Some(OmniError::manifest_read_set_changed(
                "schema_ir_hash",
                Some(first.schema_ir_hash.clone()),
                Some(self.schema_ir_hash.clone()),
            ));
        }
        if self.schema_identity_domain != first.schema_identity_domain {
            return Some(OmniError::manifest_read_set_changed(
                "schema_identity_domain",
                Some(first.schema_identity_domain.clone()),
                Some(self.schema_identity_domain.clone()),
            ));
        }
        if self.schema_identity_version != first.schema_identity_version {
            return Some(OmniError::manifest_read_set_changed(
                "schema_identity_version",
                Some(first.schema_identity_version.to_string()),
                Some(self.schema_identity_version.to_string()),
            ));
        }
        None
    }
}

/// What one attempt of a write carries: the writer gates it prepared under,
/// if any, the identity of the write's first attempt, whether this attempt
/// was admitted, and whether its admission found that identity changed.
#[derive(Default)]
pub(crate) struct WriteAttempt {
    gates: Option<BranchWriteGates>,
    first: Option<AttemptIdentity>,
    admitted: bool,
    identity_changed: bool,
}

impl WriteAttempt {
    /// WAL research prototype: whether this attempt was prepared under the
    /// writer gates (a re-preparation), which group commit leaves on the
    /// ordinary path.
    pub(crate) fn holds_gates(&self) -> bool {
        self.gates.is_some()
    }

    /// The schema permit and branch gate this attempt prepared under, for
    /// its commit point to complete with the table gates; `None` on a first
    /// attempt, which takes them at its revalidation. An attempt that never
    /// passed [`WriteAttempt::admit`] cannot commit: a writer that forgot to
    /// admit its capture would otherwise rerun across a recreated branch or a
    /// schema change unchecked. This is the attempt's commit point: its
    /// preparation ends here, and the writer gates may be taken from here on.
    pub(crate) fn admitted_gates(&mut self) -> Result<Option<BranchWriteGates>> {
        if !self.admitted {
            return Err(OmniError::manifest_internal(
                "a write attempt reached its commit without admitting its capture",
            ));
        }
        write_queue::end_preparation();
        Ok(self.gates.take())
    }

    /// Admit the authority this attempt captured. The first attempt records
    /// its branch incarnation and accepted schema. A later attempt of a write
    /// that `selects_rows` and sees either differ returns `ReadSetChanged`
    /// for the member that did: the rows its request selects are no longer
    /// the rows it was compiled to select.
    ///
    /// An attempt must prepare inside a preparation scope
    /// ([`write_queue::preparing`]), where a writer-gate acquisition refuses
    /// instead of waiting on the gates a gated attempt holds; one admitted
    /// outside it is refused, so a writer cannot skip the check.
    pub(crate) fn admit(&mut self, txn: &WriteTxn, selects_rows: bool) -> Result<()> {
        if !write_queue::is_preparing() {
            return Err(OmniError::manifest_internal(
                "a write attempt admitted its capture outside a preparation scope",
            ));
        }
        let identity = AttemptIdentity::of(txn);
        match &self.first {
            None => self.first = Some(identity),
            Some(_) if !selects_rows => {}
            Some(first) => {
                if let Some(changed) = identity.changed_since(first, txn.branch.as_deref()) {
                    // An earlier attempt may have staged detached effects,
                    // so this refusal claims no proof that nothing was.
                    self.identity_changed = true;
                    return Err(changed.without_pre_effect_evidence());
                }
            }
        }
        self.admitted = true;
        Ok(())
    }
}

/// One write that may prepare again: its attempt count and the state of its
/// current attempt.
pub(crate) struct PreEffectReprepares<'a> {
    db: &'a Omnigraph,
    branch: Option<&'a str>,
    what: &'static str,
    attempt: usize,
    current: WriteAttempt,
}

impl<'a> PreEffectReprepares<'a> {
    /// A write of `what` on `branch` starts. Main is one gate whether the
    /// caller names it or not.
    pub(crate) fn begin(db: &'a Omnigraph, branch: Option<&'a str>, what: &'static str) -> Self {
        Self {
            db,
            branch: branch.filter(|name| *name != "main"),
            what,
            attempt: 0,
            current: WriteAttempt::default(),
        }
    }

    /// Whether the attempt about to run is the write's first.
    pub(crate) fn is_first(&self) -> bool {
        self.attempt == 0
    }

    /// The attempt about to run, for its writer to admit its capture with and
    /// to pass to `commit_all`.
    pub(crate) fn attempt(&mut self) -> &mut WriteAttempt {
        &mut self.current
    }

    /// Whether `error` from an attempt sends the write back to prepare
    /// again: a `ReadSetChanged` within the bound. One that
    /// [`WriteAttempt::admit`] raised is not: the write's identity changed,
    /// and only the first attempt could have recorded it.
    pub(crate) fn may_prepare_again(&self, error: &OmniError) -> bool {
        error.is_read_set_changed()
            && self.attempt < MAX_PRE_EFFECT_REPREPARES
            && !self.current.identity_changed
    }

    /// Go round again: take the branch's writer gates for the next attempt,
    /// then refresh the coordinator it prepares from under them.
    pub(crate) async fn prepare_again(&mut self) -> Result<()> {
        self.attempt += 1;
        tracing::debug!(
            attempt = self.attempt,
            branch = self.branch.unwrap_or("main"),
            write = self.what,
            "prepared write authority changed before effects; preparing again under the writer gates"
        );
        crate::instrumentation::record_mutation_reprepare();
        // The last attempt's gates, if it never reached `commit_all`, go
        // first: the gates are not reentrant.
        let first = self.current.first.take();
        self.current = WriteAttempt::default();
        let gates = self
            .db
            .write_queue()
            .acquire_branch_write_gates(self.branch)
            .await?;
        self.current = WriteAttempt {
            gates: Some(gates),
            first,
            admitted: false,
            identity_changed: false,
        };
        self.db.refresh_coordinator_only().await
    }
}
