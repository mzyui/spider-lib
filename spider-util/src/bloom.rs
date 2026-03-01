//! # Bloom Filter Module
//!
//! Implements a memory-efficient Bloom Filter for duplicate URL detection.
//!
//! ## Overview
//!
//! The Bloom Filter module provides an efficient probabilistic data structure
//! for testing whether an element is a member of a set. In the context of web
//! crawling, it's used to quickly check if a URL has potentially been visited
//! before, significantly reducing the need for expensive lookups in the main
//! visited URLs cache. The filter trades a small probability of false positives
//! for substantial memory savings and performance gains.
//!
//! ## Key Components
//!
//! - **BloomFilter**: Main struct implementing the Bloom Filter algorithm
//! - **Bit Vector**: Memory-efficient storage using a vector of 64-bit integers
//! - **Hash Functions**: Multiple hash functions using double hashing technique
//! - **Might Contain**: Probabilistic membership testing method
//!
//! ## Algorithm Details
//!
//! The implementation uses a bit vector for memory efficiency and applies double
//! hashing to generate multiple hash values from two initial hash functions.
//! This approach reduces the computational overhead of calculating multiple
//! independent hash functions while maintaining good distribution properties.
//! The filter supports configurable size and number of hash functions.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_util::bloom_filter::BloomFilter;
//!
//! // Create a Bloom Filter with capacity for ~1M items and 5 hash functions
//! let mut bloom_filter = BloomFilter::new(5_000_000, 5);
//!
//! // Add items to the filter
//! bloom_filter.add("https://example.com/page1");
//! bloom_filter.add("https://example.com/page2");
//!
//! // Check if items might be in the set (with possibility of false positives)
//! assert_eq!(bloom_filter.might_contain("https://example.com/page1"), true);
//! assert_eq!(bloom_filter.might_contain("https://example.com/nonexistent"), false); // Likely, but not guaranteed
//! ```

/// A proper Bloom Filter implementation using a bit vector for memory efficiency.
/// This is used for efficiently checking if a URL has potentially been visited before,
/// reducing the need for expensive lookups in the main visited URLs cache.
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
