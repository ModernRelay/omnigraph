//! Geometric mean of u64 timings. Robust to outliers; the right aggregator for
//! latency distributions because halving one query and doubling another cancels.

pub fn geomean(xs: &[u64]) -> u64 {
    if xs.is_empty() {
        return 0;
    }
    let mut sum_ln = 0.0f64;
    for &x in xs {
        sum_ln += (x.max(1) as f64).ln();
    }
    (sum_ln / xs.len() as f64).exp() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_yields_zero() {
        assert_eq!(geomean(&[]), 0);
    }

    #[test]
    fn single_value_round_trips() {
        assert_eq!(geomean(&[100]), 100);
    }

    #[test]
    fn geomean_is_below_arithmetic_mean() {
        let xs = [1, 10, 100, 1000];
        let g = geomean(&xs);
        let am: u64 = xs.iter().sum::<u64>() / xs.len() as u64;
        assert!(g < am);
    }
}
