//! Pipeline that logs items as they pass through.
use crate::pipeline::Pipeline;
use async_trait::async_trait;
use log::info;
use spider_util::{error::PipelineError, item::ScrapedItem};

/// Pipeline that logs each scraped item with `log::info!`.
pub struct ConsolePipeline;

impl ConsolePipeline {
    /// Creates a new `ConsolePipeline`.
    pub fn new() -> Self {
        Self
    }
}

impl Default for ConsolePipeline {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<I: ScrapedItem> Pipeline<I> for ConsolePipeline {
    fn name(&self) -> &str {
        "ConsolePipeline"
    }

    async fn process_item(&self, item: I) -> Result<Option<I>, PipelineError> {
        info!("Pipeline processing item: {:?}", item);
        Ok(Some(item))
    }
}
