//! Internal engine pieces used by [`crate::Crawler`].
//!
//! Most users will not work with this module directly. It holds the task-level
//! implementation details that connect scheduling, downloading, parsing,
//! middleware, and item processing into one running crawler.

mod context;
mod crawler;
mod handler;
mod middleware;
mod parser;
mod processor;

pub use context::CrawlerContext;
pub use crawler::Crawler;
pub(crate) use handler::spawn_downloader_task;
pub(crate) use middleware::SharedMiddlewareManager;
pub(crate) use parser::spawn_parser_task;
pub(crate) use processor::spawn_item_processor_task;
