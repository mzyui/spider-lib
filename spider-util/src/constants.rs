//! Global constants used across the spider-lib workspace.
//!
//! This module is a facade that re-exports domain-specific constant groups.
//! Keep cross-crate and publicly tunable values here; keep local implementation
//! details near their usage sites.

mod crawler;
mod downloader;
mod middleware;
mod pipeline;
mod scheduler;

pub use crawler::*;
pub use downloader::*;
pub use middleware::*;
pub use pipeline::*;
pub use scheduler::*;
