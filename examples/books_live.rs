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
        let mut output = ParseOutput::new();

        if !response.css(".product_main")?.is_empty() {
            let title = response
                .css(".product_main h1::text")?
                .get()
                .unwrap_or_default()
                .trim()
                .to_string();

            let price = response
                .css(".price_color::text")?
                .get()
                .unwrap_or_default()
                .trim()
                .to_string();

            let rating = response
                .css(".star-rating")?
                .attrib("class")
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

            for row in response.css(".table.table-striped tr")? {
                let label = row
                    .css("th::text")?
                    .get()
                    .unwrap_or_default()
                    .trim()
                    .to_lowercase();
                let value = row
                    .css("td::text")?
                    .get()
                    .unwrap_or_default()
                    .trim()
                    .to_string();

                match label.as_str() {
                    "upc" => upc = value,
                    "tax" => tax = value,
                    "number of reviews" => reviews = value,
                    "availability" => availability = value,
                    _ => {}
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
            for book in response.css("article.product_pod")? {
                if let Some(book_link) = book.css("h3 a::attr(href)")?.get() {
                    let book_url = response.url.join(&book_link)?;

                    // Create a request to the book detail page
                    output.add_request(Request::new(book_url));
                }
            }

            if let Some(next_href) = response.css(".next > a::attr(href)")?.get() {
                let next_url = response.url.join(&next_href)?;
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
        .limit(50)
        .build()
        .await?;
    crawler.start_crawl().await?;

    Ok(())
}
