//! Formatting utilities for the spider framework.
//!
//! This module provides traits and implementations for formatting
//! durations, byte sizes, and rates in a consistent manner.

use std::time::Duration;

/// Trait for formatting duration values.
pub trait DurationFormatter {
    /// Formats a duration in a human-readable format.
    fn formatted_duration(&self, duration: Duration) -> String;

    /// Formats a request time, showing milliseconds or seconds as appropriate.
    fn formatted_request_time(&self, duration: Option<Duration>) -> String;
}

/// Default implementation for duration formatting.
pub struct DefaultDurationFormatter;

impl DurationFormatter for DefaultDurationFormatter {
    fn formatted_duration(&self, duration: Duration) -> String {
        format!("{:?}", duration)
    }

    fn formatted_request_time(&self, duration: Option<Duration>) -> String {
        match duration {
            Some(d) => {
                if d.as_millis() < 1000 {
                    format!("{} ms", d.as_millis())
                } else {
                    format!("{:.2} s", d.as_secs_f64())
                }
            }
            None => "N/A".to_string(),
        }
    }
}

/// Trait for formatting byte values.
pub trait ByteFormatter {
    /// Formats a byte count in a human-readable format (B, KB, MB, GB).
    fn formatted_bytes(&self, bytes: usize) -> String;
}

/// Default implementation for byte formatting.
pub struct DefaultByteFormatter;

impl ByteFormatter for DefaultByteFormatter {
    fn formatted_bytes(&self, bytes: usize) -> String {
        const KB: usize = 1024;
        const MB: usize = 1024 * KB;
        const GB: usize = 1024 * MB;

        if bytes >= GB {
            format!("{:.2} GB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.2} MB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.2} KB", bytes as f64 / KB as f64)
        } else {
            format!("{} B", bytes)
        }
    }
}

/// Trait for calculating rates.
pub trait RateCalculator {
    /// Calculates a rate given a count and elapsed time.
    fn calculate_rate(&self, count: usize, elapsed: Duration) -> f64;
}

/// Default implementation for rate calculation.
pub struct DefaultRateCalculator;

impl RateCalculator for DefaultRateCalculator {
    fn calculate_rate(&self, count: usize, elapsed: Duration) -> f64 {
        let elapsed = elapsed.as_secs_f64();
        if elapsed > 0.0 {
            count as f64 / elapsed
        } else {
            0.0
        }
    }
}

// ============================================================================
// Convenience Functions
// ============================================================================

/// Formats a duration in a human-readable format.
pub fn format_duration(duration: Duration) -> String {
    DefaultDurationFormatter.formatted_duration(duration)
}

/// Formats a request time, showing milliseconds or seconds as appropriate.
pub fn format_request_time(duration: Option<Duration>) -> String {
    DefaultDurationFormatter.formatted_request_time(duration)
}

/// Formats a byte count in a human-readable format.
pub fn format_bytes(bytes: usize) -> String {
    DefaultByteFormatter.formatted_bytes(bytes)
}

/// Calculates a rate given a count and elapsed time.
pub fn calculate_rate(count: usize, elapsed: Duration) -> f64 {
    DefaultRateCalculator.calculate_rate(count, elapsed)
}
