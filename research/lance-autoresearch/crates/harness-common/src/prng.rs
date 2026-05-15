//! Deterministic SplitMix64 PRNG. Same seed produces the same sequence on
//! every machine; no platform-specific RNG / no `rand` crate. Reproducibility
//! across trials is the whole point.

pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)` with 24 bits of mantissa precision.
    pub fn next_f32(&mut self) -> f32 {
        let bits = (self.next_u64() >> 40) as u32;
        bits as f32 / ((1u32 << 24) as f32)
    }

    /// Standard normal via Box–Muller. Cheap and sufficient for fixture
    /// generation; not cryptographically anything.
    pub fn next_normal(&mut self) -> f32 {
        let mut u1 = self.next_f32();
        if u1 < 1e-7 {
            u1 = 1e-7;
        }
        let u2 = self.next_f32();
        (-2.0 * u1.ln()).sqrt() * (std::f32::consts::TAU * u2).cos()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_across_calls() {
        let mut a = SplitMix64::new(0x1234_5678);
        let mut b = SplitMix64::new(0x1234_5678);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }
}
