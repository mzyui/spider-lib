use spider_lib::prelude::*;
use spider_lib::route_by_rule;

#[scraped_item]
struct RuleRoutedItem {
    page_kind: String,
    title: String,
    url: String,
}

struct RuleSpider;

impl RuleSpider {
    async fn parse_listing(&self, cx: &ParseContext<'_, Self>) -> Result<(), SpiderError> {
        let title = cx
            .css("title::text")?
            .get()
            .unwrap_or_default()
            .trim()
            .to_string();

        cx.add_item(RuleRoutedItem {
            page_kind: "listing".to_string(),
            title,
            url: cx.url.to_string(),
        })
        .await?;

        Ok(())
    }

    async fn parse_book(&self, cx: &ParseContext<'_, Self>) -> Result<(), SpiderError> {
        let title = cx
            .css(".product_main h1::text")?
            .get()
            .unwrap_or_default()
            .trim()
            .to_string();

        cx.add_item(RuleRoutedItem {
            page_kind: "book".to_string(),
            title,
            url: cx.url.to_string(),
        })
        .await?;

        Ok(())
    }

    async fn parse_default(&self, cx: &ParseContext<'_, Self>) -> Result<(), SpiderError> {
        cx.add_item(RuleRoutedItem {
            page_kind: "default".to_string(),
            title: cx
                .discovery_rule_name()
                .unwrap_or_else(|| "unmatched".to_string()),
            url: cx.url.to_string(),
        })
        .await?;
        Ok(())
    }
}

#[async_trait]
impl Spider for RuleSpider {
    type Item = RuleRoutedItem;
    type State = ();

    fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
        Ok(StartRequests::Urls(vec!["https://books.toscrape.com"]))
    }

    async fn parse(&self, cx: ParseContext<'_, Self>) -> Result<(), SpiderError> {
        route_by_rule!(
            cx,
            "listing" => self.parse_listing(&cx).await,
            "book" => self.parse_book(&cx).await,
            _ => self.parse_default(&cx).await,
        )
    }
}

#[tokio::main]
async fn main() -> Result<(), SpiderError> {
    let listing_rule = DiscoveryRule::new("listing")
        .with_allow_patterns(["https://books.toscrape.com/"])
        .with_allowed_tags(["a"])
        .with_allowed_attributes(["href"])
        .with_follow_allow_patterns(["*catalogue/*.html"])
        .with_follow_deny_patterns(["*/page-*.html"])
        .with_denied_link_types([LinkType::Image, LinkType::Script, LinkType::Stylesheet]);

    let book_rule = DiscoveryRule::new("book").with_allow_patterns(["*catalogue/*.html"]);

    let crawler = CrawlerBuilder::new(RuleSpider)
        .discovery_mode(DiscoveryMode::HtmlLinks)
        .discover_same_site_only(true)
        .add_discovery_rule(book_rule)
        .add_discovery_rule(listing_rule)
        .limit(6)
        .log_level(log::LevelFilter::Info)
        .build()
        .await?;

    crawler.start_crawl().await
}
