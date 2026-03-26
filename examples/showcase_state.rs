#[path = "showcase/support.rs"]
mod showcase;

use showcase::{ShowcaseSpider, prepare_output_dir};
use spider_lib::prelude::*;

#[tokio::main]
async fn main() -> Result<(), SpiderError> {
    prepare_output_dir()?;

    let crawler = CrawlerBuilder::new(ShowcaseSpider)
        .limit(1)
        .log_level(log::LevelFilter::Info)
        .build()
        .await?;

    let state = crawler.state_arc();
    crawler.start_crawl().await?;

    println!("showcase state summary: {}", state.summary());

    Ok(())
}
