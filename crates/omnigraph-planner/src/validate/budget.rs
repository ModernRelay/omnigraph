//! The finite limits acceptance runs under (RFC 0047, "Evidence has bounded
//! size and checking work"). Every check charges its work before doing it,
//! so the same query, values, facts, evidence and limits always produce the
//! same accept, violation or exhaustion; wall time decides nothing here.

use serde::{Deserialize, Serialize};

use super::ValidationError;

/// The validation inputs that bound its cost, captured with the evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationLimits {
    /// Bytes of serialized evidence a replay may supply, checked before any
    /// decoding.
    pub evidence_bytes: u64,
    /// Typed nodes a derivation may hold.
    pub nodes: u64,
    /// Rule applications a derivation may record.
    pub steps: u64,
    /// Expression and plan nodes the checks may visit, decoding,
    /// reconstruction and property derivation included.
    pub work: u64,
}

impl ValidationLimits {
    /// Limits far above what any accepted plan needs: a derivation of the
    /// exact fragment holds a handful of nodes and steps.
    pub const DEFAULT: Self = Self {
        evidence_bytes: 1 << 20,
        nodes: 4096,
        steps: 1024,
        work: 1 << 20,
    };
}

impl Default for ValidationLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The running charge against [`ValidationLimits`].
#[derive(Debug)]
pub struct Budget {
    limits: ValidationLimits,
    nodes: u64,
    steps: u64,
    work: u64,
}

impl Budget {
    pub fn new(limits: ValidationLimits) -> Self {
        Self {
            limits,
            nodes: 0,
            steps: 0,
            work: 0,
        }
    }

    pub fn limits(&self) -> ValidationLimits {
        self.limits
    }

    /// Charge `count` visits before making them.
    pub fn visit(&mut self, count: u64) -> Result<(), ValidationError> {
        charge(&mut self.work, count, self.limits.work, "work")
    }

    /// Charge one retained derivation node before allocating it.
    pub fn node(&mut self) -> Result<(), ValidationError> {
        charge(&mut self.nodes, 1, self.limits.nodes, "nodes")?;
        self.visit(1)
    }

    /// Charge one rule application before reconstructing its successor.
    pub fn step(&mut self) -> Result<(), ValidationError> {
        charge(&mut self.steps, 1, self.limits.steps, "steps")?;
        self.visit(1)
    }

    /// Refuse `bytes` of serialized evidence above the byte limit, before it
    /// is decoded.
    pub fn evidence_bytes(&self, bytes: u64) -> Result<(), ValidationError> {
        if bytes > self.limits.evidence_bytes {
            return Err(ValidationError::Exhausted {
                limit: "evidence_bytes",
                value: self.limits.evidence_bytes,
            });
        }
        Ok(())
    }
}

fn charge(
    used: &mut u64,
    count: u64,
    limit: u64,
    name: &'static str,
) -> Result<(), ValidationError> {
    let next = used.saturating_add(count);
    if next > limit {
        return Err(ValidationError::Exhausted {
            limit: name,
            value: limit,
        });
    }
    *used = next;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_charge_past_its_limit_exhausts_before_counting() {
        let mut budget = Budget::new(ValidationLimits {
            evidence_bytes: 8,
            nodes: 1,
            steps: 1,
            work: 3,
        });
        budget.node().unwrap();
        assert_eq!(
            budget.node(),
            Err(ValidationError::Exhausted {
                limit: "nodes",
                value: 1
            })
        );
        budget.step().unwrap();
        assert_eq!(
            budget.visit(2),
            Err(ValidationError::Exhausted {
                limit: "work",
                value: 3
            })
        );
        budget.visit(1).unwrap();
        assert!(budget.evidence_bytes(8).is_ok());
        assert_eq!(
            budget.evidence_bytes(9),
            Err(ValidationError::Exhausted {
                limit: "evidence_bytes",
                value: 8
            })
        );
    }
}
