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

    async fn parse(
        &self,
        response: Response,
        _state: &Self::State,
    ) -> Result<ParseOutput<Self::Item>, SpiderError> {
        let mut output = ParseOutput::new();

        let heading = response
            .css("h1::text")?
            .get()
            .unwrap_or_else(|| "Example Domain".to_string())
            .trim()
            .to_string();

        output.add_item(MinimalItem {
            title: heading.clone(),
            url: response.url.to_string(),
            status: response.status.as_u16(),
            has_heading: !heading.is_empty(),
        });

        Ok(output)
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
