# spider-util

Provides utility types, traits, and implementations for the `spider-lib` framework.

## Overview

The `spider-util` crate contains fundamental data structures, error types, and utility functions that are shared across all components of the spider framework. This crate serves as the common foundation for all other spider crates, providing the basic building blocks for web scraping operations.

## Key Components

- **Request**: Represents an HTTP request with URL, method, headers, and body
- **Response**: Represents an HTTP response with status, headers, and body
- **ScrapedItem**: Trait and derive macro for defining data structures to hold scraped data
- **Error Handling**: Comprehensive error types for all operations
- **Bloom Filter**: Efficient probabilistic data structure for duplicate detection
- **Utilities**: Helper functions and extensions for common operations

## Architecture

This crate is designed to be lightweight and reusable, containing only the essential types and utilities needed by other spider components. It has minimal external dependencies to ensure stability and compatibility.

## Usage

```rust
use spider_util::{request::Request, response::Response, item::ScrapedItem};
use url::Url;

// Create a request
let url = Url::parse("https://example.com").unwrap();
let request = Request::new(url);

// Define a scraped item
#[spider_macro::scraped_item]
struct Article {
    title: String,
    content: String,
}
```

## Components

### Request

Represents an HTTP request with URL, method, headers, and body. Provides methods for constructing and manipulating requests.

**Usage:**
```rust
use spider_util::request::Request;
use url::Url;

let url = Url::parse("https://example.com").unwrap();
let mut request = Request::new(url);

// Add headers
request.headers.insert(
    reqwest::header::USER_AGENT,
    reqwest::header::HeaderValue::from_static("MyBot/1.0")
);

// Add metadata
request.meta.insert("custom_field".into(), "custom_value".into());
```

### Response

Represents an HTTP response with status, headers, and body. Contains methods for extracting content and metadata from responses.

**Usage:**
```rust
use spider_util::response::Response;

// Access response properties
println!("Status: {}", response.status);
println!("URL: {}", response.url);
println!("Body: {}", String::from_utf8_lossy(&response.body));

// Extract content with scraper
use scraper::{Html, Selector};
let document = Html::parse_document(&response.text()?);
let selector = Selector::parse("h1").unwrap();
if let Some(element) = document.select(&selector).next() {
    println!("Title: {}", element.inner_html());
}
```

### ScrapedItem

Defines the trait and associated functionality for data structures that hold scraped data. Used by spiders to define the structure of the data they extract.

**Usage:**
```rust
use spider_macro::scraped_item;

#[scraped_item]
struct Product {
    name: String,
    price: f64,
    in_stock: bool,
}

// The macro automatically implements necessary traits
let product = Product {
    name: "Widget".to_string(),
    price: 19.99,
    in_stock: true,
};

// Convert to JSON
let json = product.to_json_value();
```

### Error Handling

Comprehensive error types for all operations within the spider framework, providing detailed information about failures.

**Error Types:**
- `SpiderError`: General error type for spider operations
- `PipelineError`: Errors specific to pipeline operations
- `SerializationError`: Errors during serialization/deserialization
- `IoError`: Input/output errors
- `ConfigurationError`: Errors in configuration

**Usage:**
```rust
use spider_util::error::SpiderError;

match some_operation() {
    Ok(result) => println!("Success: {:?}", result),
    Err(SpiderError::ReqwestError(e)) => {
        eprintln!("HTTP error: {}", e);
    }
    Err(SpiderError::SerializationError(e)) => {
        eprintln!("Serialization error: {}", e);
    }
    Err(e) => {
        eprintln!("Other error: {}", e);
    }
}
```

### Bloom Filter

Efficient probabilistic data structure for duplicate detection, useful for identifying URLs that have already been crawled.

**Usage:**
```rust
use spider_util::bloom::BloomFilter;

let mut filter = BloomFilter::new(1000, 0.1); // 1000 expected items, 10% false positive rate

let url = "https://example.com/page";
if !filter.contains(url) {
    filter.insert(url);
    // Process the URL since it hasn't been seen before
} else {
    // Skip, as this URL has already been processed
}
```

### Utilities

Helper functions and extensions for common operations such as URL manipulation, content extraction, and data processing.

**Utility Functions:**
- `validate_output_dir`: Validates that an output directory exists and is writable
- `normalize_origin`: Normalizes URL origins for consistent comparison
- `extract_links`: Extracts links from HTML content
- `calculate_fingerprint`: Calculates request fingerprints for caching

**Usage:**
```rust
use spider_util::util;

// Validate output directory
util::validate_output_dir("/path/to/output")?;

// Normalize URL origin
let normalized = util::normalize_origin(&request);

// Calculate request fingerprint
let fingerprint = request.fingerprint();
```

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.
