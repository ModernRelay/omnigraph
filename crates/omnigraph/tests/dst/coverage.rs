//! Coverage capture: which matrix cells the generative walk actually touched.
//! Turns "comprehensive" from a claim into a number (hit / total).

use std::collections::HashSet;

use crate::op::OpKind;

#[derive(Default)]
pub struct Coverage {
    ops: HashSet<OpKind>,
    invariants: HashSet<&'static str>,
    /// op-error and invariant-violation signatures actually exercised.
    findings: HashSet<&'static str>,
}

impl Coverage {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn op(&mut self, k: OpKind) {
        self.ops.insert(k);
    }
    pub fn invariant(&mut self, name: &'static str) {
        self.invariants.insert(name);
    }
    pub fn finding(&mut self, name: &'static str) {
        self.findings.insert(name);
    }
    pub fn report(&self) -> String {
        format!(
            "ops {}/{}, invariants {}/5, known-bugs exercised {}",
            self.ops.len(),
            OpKind::ALL.len(),
            self.invariants.len(),
            self.findings.len()
        )
    }
}
