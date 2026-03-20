//! Bloom filter used by the scheduler for cheap duplicate checks.

/// Bloom filter implementation backed by a bitset.
pub struct BloomFilter {
    bit_set: Vec<u64>,
    num_bits: u64,
    hash_functions: usize,
}

impl BloomFilter {
    /// Creates a new BloomFilter with the specified capacity and number of hash functions.
    pub fn new(num_bits: u64, hash_functions: usize) -> Self {
        let size = ((num_bits as f64 / 64.0).ceil() as usize).max(1);
        Self {
            bit_set: vec![0; size],
            num_bits,
            hash_functions,
        }
    }

    /// Adds an item to the BloomFilter.
    pub fn add(&mut self, item: &str) {
        // Pre-compute hash of the item once
        let item_bytes = item.as_bytes();
        let hash1 = seahash::hash(item_bytes);

        for i in 0..self.hash_functions {
            // Use double hashing: h(i) = hash1 + i * hash2
            // Optimized: use XOR and rotation instead of hash(&i.to_ne_bytes()) to avoid allocation
            let hash2 = hash1 ^ (i as u64).rotate_left(13);
            let combined_hash = hash1.wrapping_add((i as u64).wrapping_mul(hash2));
            let index = combined_hash % self.num_bits;

            let bucket_idx = (index / 64) as usize;
            let bit_idx = (index % 64) as usize;

            if bucket_idx < self.bit_set.len() {
                self.bit_set[bucket_idx] |= 1u64 << bit_idx;
            }
        }
    }

    /// Checks if an item might be in the BloomFilter.
    /// Returns true if the item might be in the set, false if it definitely isn't.
    pub fn might_contain(&self, item: &str) -> bool {
        let item_bytes = item.as_bytes();
        let hash1 = seahash::hash(item_bytes);

        for i in 0..self.hash_functions {
            // Optimized: use XOR and rotation instead of hash(&i.to_ne_bytes())
            let hash2 = hash1 ^ (i as u64).rotate_left(13);
            let combined_hash = hash1.wrapping_add((i as u64).wrapping_mul(hash2));
            let index = combined_hash % self.num_bits;

            let bucket_idx = (index / 64) as usize;
            let bit_idx = (index % 64) as usize;

            if bucket_idx >= self.bit_set.len() {
                return false;
            }

            if (self.bit_set[bucket_idx] & (1u64 << bit_idx)) == 0 {
                return false;
            }
        }
        true
    }
}
