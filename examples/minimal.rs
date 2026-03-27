use spider_lib::prelude::*;

#[scraped_item]
struct MinimalItem {
    title: String,
    url: String,
    status: u16,
    has_heading: bool,
}

struct MinimalSpider;

#[async_trait]
impl Spider for MinimalSpider {
    type Item = MinimalItem;
    type State = ();

    fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
        Ok(StartRequests::Urls(vec!["https://example.com/"]))
    }

    async fn parse(&self, cx: ParseContext<'_, Self>) -> Result<(), SpiderError> {
        let heading = cx
            .css("h1::text")?
            .get()
            .unwrap_or_else(|| "Example Domain".to_string())
            .trim()
            .to_string();

        cx.add_item(MinimalItem {
            title: heading.clone(),
            url: cx.url.to_string(),
            status: cx.status.as_u16(),
            has_heading: !heading.is_empty(),
        })
        .await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), SpiderError> {
    let crawler = CrawlerBuilder::new(MinimalSpider)
        .limit(1)
        .log_level(log::LevelFilter::Info)
        .build()
        .await?;

    crawler.start_crawl().await
}
