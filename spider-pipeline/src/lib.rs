//! # spider-pipeline
//!
//! Item pipelines for cleanup, validation, deduplication, and output.
//!
//! Pipelines run after parsing. This crate contains both in-memory stages such
//! as transforms and validators, and output backends such as CSV, JSON, SQLite,
//! and streaming JSON.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_pipeline::json::JsonPipeline;
//! use spider_pipeline::console::ConsolePipeline;
//!
//! let crawler = CrawlerBuilder::new(MySpider)
//!     .add_pipeline(JsonPipeline::new("output.json")?)
//!     .add_pipeline(ConsolePipeline::new())
//!     .build()
//!     .await?;
//! ```

// Core pipelines (always available)
pub mod console;
pub mod dedup;
pub mod pipeline;
pub mod transform;
pub mod validation;

// Optional pipelines (feature-gated)
#[cfg(feature = "pipeline-csv")]
pub mod csv;

#[cfg(feature = "pipeline-json")]
pub mod json;

#[cfg(feature = "pipeline-jsonl")]
pub mod jsonl;

#[cfg(feature = "pipeline-sqlite")]
pub mod sqlite;

#[cfg(feature = "pipeline-stream-json")]
pub mod stream_json;
