//! Shared task context used inside the crawler engine.

use crate::{Scheduler, spider::Spider, stats::StatCollector};
use spider_pipeline::pipeline::Pipeline;
use spider_util::item::ScrapedItem;
use std::sync::Arc;

/// Inner data shared across crawler tasks.
pub struct CrawlerContextInner<S, I>
where
    S: Spider<Item = I>,
    I: ScrapedItem,
{
    pub scheduler: Arc<Scheduler>,
    pub stats: Arc<StatCollector>,
    pub spider: Arc<S>,
    pub spider_state: Arc<S::State>,
    pub pipelines: Arc<Vec<Box<dyn Pipeline<I>>>>,
}

/// Cheaply cloneable wrapper around the engine's shared context payload.
pub struct CrawlerContext<S, I>(pub Arc<CrawlerContextInner<S, I>>)
where
    S: Spider<Item = I>,
    I: ScrapedItem;

impl<S, I> Clone for CrawlerContext<S, I>
where
    S: Spider<Item = I>,
    I: ScrapedItem,
{
    fn clone(&self) -> Self {
        CrawlerContext(Arc::clone(&self.0))
    }
}

impl<S, I> CrawlerContext<S, I>
where
    S: Spider<Item = I>,
    I: ScrapedItem,
{
    /// Creates a new CrawlerContext with the given components.
    pub fn new(
        scheduler: Arc<Scheduler>,
        stats: Arc<StatCollector>,
        spider: Arc<S>,
        spider_state: Arc<S::State>,
        pipelines: Arc<Vec<Box<dyn Pipeline<I>>>>,
    ) -> Self {
        CrawlerContext(Arc::new(CrawlerContextInner {
            scheduler,
            stats,
            spider,
            spider_state,
            pipelines,
        }))
    }

    /// Creates a CrawlerContext from a Crawler instance.
    pub fn from_crawler(
        scheduler: Arc<Scheduler>,
        stats: Arc<StatCollector>,
        spider: Arc<S>,
        spider_state: Arc<S::State>,
        pipelines: Arc<Vec<Box<dyn Pipeline<I>>>>,
    ) -> Self {
        Self::new(scheduler, stats, spider, spider_state, pipelines)
    }
}

// Implement Deref for convenient access to inner fields
impl<S, I> std::ops::Deref for CrawlerContext<S, I>
where
    S: Spider<Item = I>,
    I: ScrapedItem,
{
    type Target = CrawlerContextInner<S, I>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
