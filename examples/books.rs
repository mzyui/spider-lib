use dashmap::DashMap;
use spider_lib::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

/// Shared runtime state for the books spider example.
#[derive(Clone, Default)]
pub struct BooksSpiderState {
    page_count: Arc<AtomicUsize>,
    book_count: Arc<AtomicUsize>,
    visited_urls: Arc<DashMap<String, bool>>,
}

impl BooksSpiderState {
    /// Increments the number of processed pages.
    pub fn increment_page_count(&self) {
        self.page_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Increments the number of discovered books.
    pub fn increment_book_count(&self) {
        self.book_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Returns the current processed page count.
    pub fn get_page_count(&self) -> usize {
        self.page_count.load(Ordering::SeqCst)
    }

    /// Returns the current discovered book count.
    pub fn get_book_count(&self) -> usize {
        self.book_count.load(Ordering::SeqCst)
    }

    /// Marks a URL as visited in the local state map.
    pub fn mark_url_visited(&self, url: String) {
        self.visited_urls.insert(url, true);
    }
}

/// Example spider for https://books.toscrape.com/.
pub struct BooksSpider;

#[async_trait]
impl Spider for BooksSpider {
    type Item = BookItem;
    type State = BooksSpiderState;

    fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
        Ok(StartRequests::Urls(vec!["https://books.toscrape.com/"]))
    }

    async fn parse(&self, cx: ParseContext<'_, Self>) -> Result<(), SpiderError> {
        cx.state().increment_page_count();
        cx.state().mark_url_visited(cx.url.to_string());

        if !cx.css(".product_main")?.is_empty() {
            let title = cx
                .css(".product_main h1::text")?
                .get()
                .unwrap_or_default()
                .trim()
                .to_string();

            let price = cx
                .css(".price_color::text")?
                .get()
                .unwrap_or_default()
                .trim()
                .to_string();

            let rating = cx
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

            for row in cx.css(".table.table-striped tr")? {
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

            cx.add_item(BookItem {
                title,
                price,
                rating,
                availability,
                upc,
                tax,
                reviews,
                stock: String::new(),
            })
            .await?;

            cx.state().increment_book_count();
        } else {
            for book in cx.css("article.product_pod")? {
                if let Some(book_link) = book.css("h3 a::attr(href)")?.get() {
                    let book_url = cx.url.join(&book_link)?;

                    // Create a request to the book detail page
                    cx.add_request(Request::new(book_url)).await?;
                }

                cx.state().increment_book_count();
            }

            if let Some(next_href) = cx.css(".next > a::attr(href)")?.get() {
                let next_url = cx.url.join(&next_href)?;
                cx.add_request(Request::new(next_url)).await?;
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), SpiderError> {
    let crawler = CrawlerBuilder::new(BooksSpider)
        .log_level(log::LevelFilter::Info)
        .build()
        .await?;

    let state = crawler.state_arc();
    crawler.start_crawl().await?;

    println!("=== Final Results ===");
    println!("Total pages crawled: {}", state.get_page_count());
    println!("Total books scraped: {}", state.get_book_count());

    Ok(())
}
