use spider_lib::prelude::*;

#[scraped_item]
struct SitemapItem {
    url: String,
    title: String,
    description: String,
}

struct SitemapSpider;

#[async_trait]
impl Spider for SitemapSpider {
    type Item = SitemapItem;
    type State = ();

    fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
        Ok(StartRequests::Urls(vec![
            "https://www.rust-lang.org/sitemap.xml",
        ]))
    }

    async fn parse(&self, cx: ParseContext<'_, Self>) -> Result<(), SpiderError> {
        let is_xml = cx
            .headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("xml"))
            || cx.url.path().ends_with(".xml");

        if is_xml {
            return Ok(());
        }

        let metadata = cx.page_metadata()?;
        cx.add_item(SitemapItem {
            url: cx.url.to_string(),
            title: metadata.title.unwrap_or_default(),
            description: metadata.description.unwrap_or_default(),
        })
        .await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), SpiderError> {
    let crawler = CrawlerBuilder::new(SitemapSpider)
        .discovery_mode(DiscoveryMode::SitemapOnly)
        .enable_sitemaps(true)
        .extract_page_metadata(true)
        .max_sitemap_depth(2)
        .limit(3)
        .log_level(log::LevelFilter::Info)
        .build()
        .await?;

    crawler.start_crawl().await
}
