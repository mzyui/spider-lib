#[path = "showcase/support.rs"]
mod showcase;

use showcase::{ShowcaseSpider, prepare_output_dir};
use spider_lib::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), SpiderError> {
    prepare_output_dir()?;

    let crawler = CrawlerBuilder::new(ShowcaseSpider)
        .limit(1)
        .crawl_shape_preset(CrawlShapePreset::ApiHeavy)
        .max_concurrent_downloads(2)
        .max_parser_workers(2)
        .max_concurrent_pipelines(2)
        .channel_capacity(32)
        .output_batch_size(8)
        .response_backpressure_threshold(16)
        .item_backpressure_threshold(16)
        .retry_release_permit(true)
        .shutdown_grace_period(Duration::from_secs(2))
        .live_stats(true)
        .live_stats_interval(Duration::from_millis(250))
        .live_stats_preview_fields(["title", "url", "status"])
        .with_checkpoint_path("output/showcase.checkpoint")
        .with_checkpoint_interval(Duration::from_secs(30))
        .log_level(log::LevelFilter::Info)
        .build()
        .await?;

    let state = crawler.state_arc();
    crawler.start_crawl().await?;

    println!("showcase runtime summary: {}", state.summary());

    Ok(())
}
