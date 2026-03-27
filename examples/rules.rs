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
    async fn parse_listing(
        &self,
        response: Response,
        _state: &(),
    ) -> Result<ParseOutput<RuleRoutedItem>, SpiderError> {
        let mut output = ParseOutput::new();

        let title = response
            .css("title::text")?
            .get()
            .unwrap_or_default()
            .trim()
            .to_string();

        output.add_item(RuleRoutedItem {
            page_kind: "listing".to_string(),
            title,
            url: response.url.to_string(),
        });

        Ok(output)
    }

    async fn parse_book(
        &self,
        response: Response,
        _state: &(),
    ) -> Result<ParseOutput<RuleRoutedItem>, SpiderError> {
        let mut output = ParseOutput::new();

        let title = response
            .css(".product_main h1::text")?
            .get()
            .unwrap_or_default()
            .trim()
            .to_string();

        output.add_item(RuleRoutedItem {
            page_kind: "book".to_string(),
            title,
            url: response.url.to_string(),
        });

        Ok(output)
    }

    async fn parse_default(
        &self,
        response: Response,
        _state: &(),
    ) -> Result<ParseOutput<RuleRoutedItem>, SpiderError> {
        let mut output = ParseOutput::new();
        output.add_item(RuleRoutedItem {
            page_kind: "default".to_string(),
            title: response
                .discovery_rule_name()
                .unwrap_or_else(|| "unmatched".to_string()),
            url: response.url.to_string(),
        });
        Ok(output)
    }
}

#[async_trait]
impl Spider for RuleSpider {
    type Item = RuleRoutedItem;
    type State = ();

    fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
        Ok(StartRequests::Urls(vec!["https://books.toscrape.com"]))
    }

    async fn parse(
        &self,
        response: Response,
        state: &Self::State,
    ) -> Result<ParseOutput<Self::Item>, SpiderError> {
        route_by_rule!(
            response,
            "listing" => self.parse_listing(response, state).await,
            "book" => self.parse_book(response, state).await,
            _ => self.parse_default(response, state).await,
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
