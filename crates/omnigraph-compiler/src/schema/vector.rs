//! The distance a vector property declares:
//! `Vector(n, distance = "l2" | "cosine" | "dot")`
//! (the GQ 3 RFC, section 4, Schema: Vectors).
//!
//! `distance(…)` and `nearest(…)` both read the field's declared distance;
//! an omitted distance is `l2`, edition 2's meaning, so no accepted schema
//! changes meaning. Every value is ascending: a smaller distance is nearer.
//! A property that declares `l2` or omits the option carries no
//! [`VectorDistance`] in its shape or IR, so an edition 2 schema keeps its
//! bytes and hashes; the absent value resolves to [`VectorDistance::L2`].

use std::fmt;

use serde::{Deserialize, Serialize};

/// The option `Vector(n, …)` reads.
pub const DISTANCE_OPTION: &str = "distance";

/// The distances a vector property can declare.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub enum VectorDistance {
    /// The sum of squared component differences, no square root.
    #[default]
    #[serde(rename = "l2")]
    L2,
    /// `1 - dot(x, q) / (norm(x) · norm(q))`; undefined at a zero vector.
    #[serde(rename = "cosine")]
    Cosine,
    /// `1 - dot(x, q)`; can be negative.
    #[serde(rename = "dot")]
    Dot,
}

impl VectorDistance {
    /// Every name a schema can declare, in the order a refusal lists them.
    pub const NAMES: [&'static str; 3] = ["l2", "cosine", "dot"];

    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "l2" => Some(Self::L2),
            "cosine" => Some(Self::Cosine),
            "dot" => Some(Self::Dot),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::L2 => "l2",
            Self::Cosine => "cosine",
            Self::Dot => "dot",
        }
    }

    /// The distance as a shape or IR records it: `None` for `l2`, the value
    /// an absent declaration resolves to.
    pub fn declared(self) -> Option<Self> {
        (self != Self::L2).then_some(self)
    }

    /// Whether a zero vector is outside this distance's domain: `cosine`
    /// divides by the vector's norm, so a write refuses one, a zero query is
    /// refused before the read runs, and a zero vector stored earlier drops
    /// its row as a null does.
    pub fn refuses_zero_vector(self) -> bool {
        self == Self::Cosine
    }

    /// The distance of `x` to `q` under this distance, in `F32` as a score
    /// reports it: `None` where it is undefined (a `cosine` operand that is
    /// a zero vector). The operands have one length; the caller has refused
    /// a non-finite component.
    pub fn evaluate(self, x: &[f32], q: &[f32]) -> Option<f32> {
        match self {
            Self::L2 => Some(
                x.iter()
                    .zip(q)
                    .map(|(x, q)| {
                        let difference = x - q;
                        difference * difference
                    })
                    .sum(),
            ),
            Self::Cosine => {
                let (mut dot, mut xx, mut qq) = (0.0f32, 0.0f32, 0.0f32);
                for (x, q) in x.iter().zip(q) {
                    dot += x * q;
                    xx += x * x;
                    qq += q * q;
                }
                if is_zero_vector(x) || is_zero_vector(q) {
                    return None;
                }
                Some(1.0 - dot / (xx.sqrt() * qq.sqrt()))
            }
            Self::Dot => Some(1.0 - x.iter().zip(q).map(|(x, q)| x * q).sum::<f32>()),
        }
    }
}

impl fmt::Display for VectorDistance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether `values` is a zero vector to `cosine`: its squared norm, summed
/// in `F32` as the distance kernels sum it, is zero. Every zero component
/// (`-0.0` included) makes one, and so does a vector whose components are
/// all so small (below about 3.7e-23) that each square underflows: its norm
/// is zero too, and its cosine would divide by it.
pub fn is_zero_vector(values: &[f32]) -> bool {
    values.iter().map(|value| value * value).sum::<f32>() == 0.0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The RFC's table: `l2` the sum of squared differences with no square
    /// root, `cosine` one minus the cosine, `dot` one minus the dot product,
    /// which can be negative; `cosine` is undefined at a zero vector.
    #[test]
    fn each_distance_evaluates_as_the_rfc_states_it() {
        let q = [1.0, 1.0, 1.0, 1.0];
        assert_eq!(
            VectorDistance::L2.evaluate(&[2.0, 0.0, 1.0, 1.0], &q),
            Some(2.0)
        );
        assert_eq!(
            VectorDistance::Cosine.evaluate(&[1.0, 0.0, 0.0, 0.0], &q),
            Some(0.5)
        );
        assert_eq!(
            VectorDistance::Cosine.evaluate(&[2.0, 2.0, 2.0, 2.0], &q),
            Some(0.0)
        );
        assert_eq!(
            VectorDistance::Cosine.evaluate(&[-1.0, -1.0, -1.0, -1.0], &q),
            Some(2.0)
        );
        assert_eq!(
            VectorDistance::Dot.evaluate(&[2.0, 2.0, 2.0, 2.0], &q),
            Some(-7.0)
        );
        assert_eq!(
            VectorDistance::Cosine.evaluate(&[0.0, -0.0, 0.0, 0.0], &q),
            None
        );
        assert_eq!(VectorDistance::Cosine.evaluate(&q, &[0.0; 4]), None);
        assert_eq!(VectorDistance::L2.evaluate(&[0.0; 4], &q), Some(4.0));
        assert_eq!(VectorDistance::Dot.evaluate(&[0.0; 4], &q), Some(1.0));
        assert!(VectorDistance::Cosine.refuses_zero_vector());
        assert!(!VectorDistance::L2.refuses_zero_vector());
        assert!(!VectorDistance::Dot.refuses_zero_vector());
    }

    #[test]
    fn names_round_trip_and_l2_is_recorded_absent() {
        for name in VectorDistance::NAMES {
            assert_eq!(VectorDistance::parse(name).unwrap().as_str(), name);
        }
        assert_eq!(VectorDistance::parse("euclidean"), None);
        assert_eq!(VectorDistance::L2.declared(), None);
        assert_eq!(
            VectorDistance::Cosine.declared(),
            Some(VectorDistance::Cosine)
        );
        assert_eq!(VectorDistance::default(), VectorDistance::L2);
    }
}
