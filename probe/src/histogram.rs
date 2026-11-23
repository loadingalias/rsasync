//! A log-linear histogram of nanoseconds: one bucket for each value below 256, then 128 buckets
//! for each power of two. A percentile is the upper end of its bucket, so it is at most 1/128
//! above the true value, and never below it.

/// The buckets for each power of two, as a power of two.
const SUB_BITS: u32 = 7;
/// The values with one bucket each.
const EXACT: u64 = 2 << SUB_BITS;
/// The buckets: the exact values, then 128 for each power of two from 2^8 to 2^63.
const BUCKETS: usize = 256 + 56 * 128;

/// The counts of the recorded values.
#[derive(Debug)]
pub(crate) struct Histogram {
    counts: Vec<u64>,
    total: u64,
    max: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            counts: vec![0; BUCKETS],
            total: 0,
            max: 0,
        }
    }
}

impl Histogram {
    /// Records `value`.
    pub(crate) fn record(&mut self, value: u64) {
        if let Some(count) = self.counts.get_mut(index(value)) {
            *count = count.saturating_add(1);
        }
        self.total = self.total.saturating_add(1);
        self.max = self.max.max(value);
    }

    /// Adds the counts of `other`.
    pub(crate) fn merge(&mut self, other: &Self) {
        for (count, more) in self.counts.iter_mut().zip(&other.counts) {
            *count = count.saturating_add(*more);
        }
        self.total = self.total.saturating_add(other.total);
        self.max = self.max.max(other.max);
    }

    /// The recorded values.
    pub(crate) const fn total(&self) -> u64 {
        self.total
    }

    /// The largest recorded value.
    pub(crate) const fn max(&self) -> u64 {
        self.max
    }

    /// The value at or below which `numerator / denominator` of the values are, or 0 with no value.
    pub(crate) fn percentile(&self, numerator: u64, denominator: u64) -> u64 {
        let target = self.total.saturating_mul(numerator).div_ceil(denominator).max(1);
        let mut seen = 0_u64;
        for (index, count) in self.counts.iter().enumerate() {
            seen = seen.saturating_add(*count);
            if seen >= target {
                return upper(index).min(self.max);
            }
        }
        0
    }
}

/// The bucket of `value`.
fn index(value: u64) -> usize {
    if value < EXACT {
        return usize::try_from(value).unwrap_or(0);
    }
    // `value` has at least 9 bits, so the shift keeps its top 8 bits: 128 to 255.
    let shift = 63 - value.leading_zeros() - SUB_BITS;
    let sub = (value >> shift) - (1 << SUB_BITS);
    usize::try_from(EXACT + u64::from(shift - 1) * (1 << SUB_BITS) + sub).unwrap_or(BUCKETS - 1)
}

/// The largest value of the bucket `index`.
fn upper(index: usize) -> u64 {
    let index = u64::try_from(index).unwrap_or(u64::MAX);
    if index < EXACT {
        return index;
    }
    let shift = (index - EXACT) / (1 << SUB_BITS) + 1;
    let sub = (index - EXACT) % (1 << SUB_BITS) + (1 << SUB_BITS);
    // The last bucket ends at 2^64 - 1: its shift moves the top bit out, and the result wraps to it.
    ((sub + 1) << shift).wrapping_sub(1)
}

#[cfg(test)]
mod tests {
    use super::Histogram;

    /// Each percentile is the true value of its rank or at most 1/128 above it, from the exact
    /// values below 256 to `u64::MAX`.
    #[test]
    fn a_percentile_is_never_below_its_true_value_and_at_most_1_128_above_it() {
        let mut values: Vec<u64> = (0..20_000_u64).map(|i| i * i * 37 + i).collect();
        values.push(u64::MAX);
        let mut histogram = Histogram::default();
        for &value in &values {
            histogram.record(value);
        }
        values.sort_unstable();
        let count = u64::try_from(values.len()).unwrap();
        for (numerator, denominator) in [(1, 20_001), (1, 2), (99, 100), (999, 1000), (1, 1)] {
            let rank = usize::try_from((count * numerator).div_ceil(denominator)).unwrap();
            let truth = values[rank - 1];
            let given = histogram.percentile(numerator, denominator);
            assert!(
                given >= truth && given - truth <= truth / 128,
                "{numerator}/{denominator}: {given} for {truth}"
            );
        }
    }
}
