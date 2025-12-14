use rand::{
    RngCore,
    distr::{Distribution, Uniform},
};

use crate::shortcode::GenerateShortcode;

/// Base58 alphabet (no 0,O,I,l)
const BASE58_ALPHABET_LEN: usize = 58;
const BASE58_ALPHABET: &[u8; BASE58_ALPHABET_LEN] =
    b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
/// Length of the Base58 shortcode, change this to 8-10 if many millions of shortcodes are needed
const LEN: usize = 7;

#[derive(Debug, Clone, Copy, Default)]
pub struct Base58Shortcode;

impl Base58Shortcode {
    /// Call at startup so any OS entropy blocking happens before serving.
    /// Note: warms only the current thread; each thread initializes its own TLS RNG on first use.
    pub fn warm_up() {
        let mut rng = rand::rng();
        // Consume a value so seeding happens now.
        std::hint::black_box(rng.next_u32());
    }
}

impl GenerateShortcode for Base58Shortcode {
    fn generate_shortcode(&self) -> String {
        let mut rng = rand::rng();
        let dist = Uniform::new(0, BASE58_ALPHABET_LEN).expect("failed to create distribution");

        (0..LEN)
            .map(|_| BASE58_ALPHABET[dist.sample(&mut rng)] as char)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base58_shortcode_length_and_charset() {
        let shortcode_gen = Base58Shortcode;
        for _ in 0..10_000 {
            let s = shortcode_gen.generate_shortcode();
            assert_eq!(s.len(), LEN, "wrong length: {}", s);
            assert!(
                s.bytes().all(|b| BASE58_ALPHABET.contains(&b)),
                "non-Base58 char in {}",
                s
            );
        }
    }
    /// Quick uniformity sanity check (not a formal test, but catches obvious bugs).
    /// Generates 50k codes (350k chars) and asserts each symbol is within ±5% of expected.
    #[test]
    fn base58_shortcode_uniformity_sanity() {
        const CODES: usize = 50_000;
        let shortcode_gen = Base58Shortcode;

        let mut map = [usize::MAX; 128];
        for (i, &b) in BASE58_ALPHABET.iter().enumerate() {
            map[b as usize] = i;
        }

        let mut counts = [0usize; BASE58_ALPHABET_LEN];
        for _ in 0..CODES {
            let s = shortcode_gen.generate_shortcode();
            for b in s.bytes() {
                let idx = map[b as usize];
                assert!(idx != usize::MAX, "byte not in BASE58");
                counts[idx] += 1;
            }
        }

        let total = CODES * LEN;
        let expected = total as f64 / BASE58_ALPHABET_LEN as f64;
        let tol = expected * 0.05; // ±5%

        for (i, &c) in counts.iter().enumerate() {
            let diff = (c as f64 - expected).abs();
            assert!(
                diff <= tol,
                "symbol '{}' freq={} outside ±5% (expected {:.1} ± {:.1})",
                BASE58_ALPHABET[i] as char,
                c,
                expected,
                tol
            );
        }
    }

    /// Collision smoke test.
    #[test]
    fn base58_shortcode_collision_smoke_50k() {
        use std::collections::HashSet;
        const N: usize = 50_000;
        let shortcode_gen = Base58Shortcode;

        let mut seen = HashSet::with_capacity(N);
        let mut dups = 0usize;

        for _ in 0..N {
            let s = shortcode_gen.generate_shortcode();
            if !seen.insert(s) {
                dups += 1;
            }
        }

        assert!(dups <= 5, "too many duplicates: {}", dups);
    }
}
