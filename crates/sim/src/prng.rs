//! The simulator's PRNG. It lives in this crate so a recorded seed replays
//! the same run after any dependency upgrade.

use std::ops::RangeInclusive;
use xxhash_rust::xxh3::xxh3_64_with_seed;

/// xoshiro256**, seeded through SplitMix64.
#[derive(Clone, Debug)]
pub struct Prng {
    state: [u64; 4],
}

impl Prng {
    pub fn new(seed: u64) -> Prng {
        let mut mix = seed;
        Prng {
            state: std::array::from_fn(|_| splitmix64(&mut mix)),
        }
    }

    /// An independent stream for one purpose, so drawing more numbers for
    /// one purpose leaves the others' draws unchanged.
    pub fn stream(seed: u64, purpose: &str) -> Prng {
        Prng::new(xxh3_64_with_seed(purpose.as_bytes(), seed))
    }

    pub fn next_u64(&mut self) -> u64 {
        let [s0, s1, s2, s3] = &mut self.state;
        let result = s1.wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = *s1 << 17;
        *s2 ^= *s0;
        *s3 ^= *s1;
        *s1 ^= *s2;
        *s0 ^= *s3;
        *s2 ^= t;
        *s3 = s3.rotate_left(45);
        result
    }

    /// A uniform value in `0..bound`, which must be nonempty.
    pub fn below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "below(0)");
        // Lemire's method: reject the few products that would bias the result.
        let threshold = bound.wrapping_neg() % bound;
        loop {
            let product = u128::from(self.next_u64()) * u128::from(bound);
            if product as u64 >= threshold {
                return (product >> 64) as u64;
            }
        }
    }

    pub fn range(&mut self, range: RangeInclusive<u64>) -> u64 {
        range.start() + self.below(range.end() - range.start() + 1)
    }

    pub fn index(&mut self, len: usize) -> usize {
        self.below(len as u64) as usize
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.index(items.len())]
    }

    /// True `percent` times in a hundred.
    pub fn percent(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    /// True `per_mille` times in a thousand.
    pub fn per_mille(&mut self, per_mille: u64) -> bool {
        self.below(1000) < per_mille
    }

    pub fn bytes(&mut self, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len + 8);
        while out.len() < len {
            out.extend_from_slice(&self.next_u64().to_le_bytes());
        }
        out.truncate(len);
        out
    }
}

pub fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_is_stable() {
        let mut prng = Prng::new(7);
        let draws: Vec<u64> = (0..3).map(|_| prng.below(1_000)).collect();
        assert_eq!(draws, [700, 278, 839]);
    }

    #[test]
    fn below_stays_in_bounds() {
        let mut prng = Prng::new(1);
        for bound in 1..200 {
            assert!(prng.below(bound) < bound);
        }
    }
}
