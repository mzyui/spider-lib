use spider_lib::prelude::*;

/// Scraped item model for a book detail page.
#[scraped_item]
pub struct BookItem {
    pub title: String,
    pub price: String,
    pub rating: String,
    pub availability: String,
    pub upc: String,
    pub tax: String,
    pub reviews: String,
    pub stock: String,
}

/// Example spider for https://books.toscrape.com/.
pub struct BooksSpider;

#[async_trait]
impl Spider for BooksSpider {
    type Item = BookItem;
    type State = ();

    fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
        Ok(StartRequests::Urls(vec!["https://books.toscrape.com/"]))
    }

    async fn parse(
        &self,
        response: Response,
        _state: &Self::State,
    ) -> Result<ParseOutput<Self::Item>, SpiderError> {
        let html = response.to_html()?;
        let mut output = ParseOutput::new();

        if html
            .select(&".product_main".to_selector()?)
            .next()
            .is_some()
        {
            let title = html
                .select(&".product_main h1".to_selector()?)
                .next()
                .map(|e| e.text().collect::<String>())
                .unwrap_or_default()
                .trim()
                .to_string();

            let price = html
                .select(&".price_color".to_selector()?)
                .next()
                .map(|e| e.text().collect::<String>())
                .unwrap_or_default()
                .trim()
                .to_string();

            let rating = html
                .select(&".star-rating".to_selector()?)
                .next()
                .and_then(|e| e.attr("class"))
                .map(|class| {
                    class
                        .split_whitespace()
                        .find(|&c| c != "star-rating")
                        .unwrap_or_default()
                        .to_string()
                })
                .unwrap_or_default();

            let mut upc = String::new();
            let mut tax = String::new();
            let mut reviews = String::new();
            let mut availability = String::new();

            for row in html.select(&".table.table-striped tr".to_selector()?) {
                if let (Some(label_elem), Some(value_elem)) = (
                    row.select(&"th".to_selector()?).next(),
                    row.select(&"td".to_selector()?).next(),
                ) {
                    let label = label_elem.text().collect::<String>().trim().to_lowercase();
                    let value = value_elem.text().collect::<String>().trim().to_string();

                    match label.as_str() {
                        "upc" => upc = value,
                        "tax" => tax = value,
                        "number of reviews" => reviews = value,
                        "availability" => availability = value,
                        _ => {}
                    }
                }
            }

            output.add_item(BookItem {
                title,
                price,
                rating,
                availability,
                upc,
                tax,
                reviews,
                stock: String::new(),
            });
        } else {
            for book in html.select(&"article.product_pod".to_selector()?) {
                if let Some(book_link) = book
                    .select(&"h3 a".to_selector()?)
                    .next()
                    .and_then(|a| a.attr("href"))
                {
                    let book_url = response.url.join(book_link)?;

                    // Create a request to the book detail page
                    output.add_request(Request::new(book_url));
                }
            }

            if let Some(next_href) = html
                .select(&".next > a[href]".to_selector()?)
                .next()
                .and_then(|a| a.attr("href"))
            {
                let next_url = response.url.join(next_href)?;
                output.add_request(Request::new(next_url));
            }
        }

        Ok(output)
    }
}

#[tokio::main]
async fn main() -> Result<(), SpiderError> {
    let crawler = CrawlerBuilder::new(BooksSpider)
        .live_stats(true)
        .add_pipeline(CsvPipeline::new("output/books_live.csv")?)
        .build()
        .await?;
    crawler.start_crawl().await?;

    Ok(())
}
