# spider-pipeline

Item pipelines for processing, filtering, and exporting scraped data in `spider-lib`.

Use this crate directly when you need pipeline functionality without taking the full facade crate.

## Installation

```toml
[dependencies]
spider-pipeline = "0.3.4"
```

## Pipeline Catalog

### Core (always available)

- `TransformPipeline`: normalize/transform item fields.
- `ValidationPipeline`: enforce field and type rules.
- `DeduplicationPipeline`: drop duplicate items by key fields.
- `ConsolePipeline`: print processed items for visibility/debugging.

### Optional output pipelines (feature-gated)

- `pipeline-json` -> `JsonPipeline`
- `pipeline-jsonl` -> `JsonlPipeline`
- `pipeline-csv` -> `CsvPipeline`
- `pipeline-sqlite` -> `SqlitePipeline`
- `pipeline-stream-json` -> `StreamJsonPipeline`

## Core Composition Example

```rust,ignore
use spider_pipeline::{
    console::ConsolePipeline,
    dedup::DeduplicationPipeline,
    transform::{TransformOperation, TransformPipeline},
    validation::{ValidationPipeline, ValidationRule},
};

let crawler = spider_core::CrawlerBuilder::new(MySpider)
    .add_pipeline(
        TransformPipeline::new()
            .with_operation(TransformOperation::Trim { field: "title".into() }),
    )
    .add_pipeline(
        ValidationPipeline::new()
            .with_rule("title", ValidationRule::Required)
            .with_rule("title", ValidationRule::NonEmptyString),
    )
    .add_pipeline(DeduplicationPipeline::new(&["url"]))
    .add_pipeline(ConsolePipeline::new())
    .build()
    .await?;
```

## Build a Custom Pipeline

Use a custom pipeline when your processing logic is domain-specific (custom scoring, external API enrichment, bespoke filtering, etc.).

```rust,ignore
use async_trait::async_trait;
use spider_pipeline::pipeline::Pipeline;
use spider_util::{error::PipelineError, item::ScrapedItem};

struct EnrichPipeline;

#[async_trait]
impl<I: ScrapedItem> Pipeline<I> for EnrichPipeline {
    fn name(&self) -> &str {
        "enrich_pipeline"
    }

    async fn process_item(&self, item: I) -> Result<Option<I>, PipelineError> {
        // Enrich, validate, or drop item by returning Ok(None).
        Ok(Some(item))
    }
}
```

Runtime integration:

```rust,ignore
let crawler = spider_core::CrawlerBuilder::new(MySpider)
    .add_pipeline(EnrichPipeline)
    .build()
    .await?;
```

## Optional Output Pipelines (One by One)

### `pipeline-json` (`JsonPipeline`)

```toml
[dependencies]
spider-lib = { version = "2.0.4", features = ["pipeline-json"] }
```

```rust,ignore
use spider_lib::prelude::*;

let crawler = CrawlerBuilder::new(MySpider)
    .add_pipeline(JsonPipeline::new("output/items.json"))
    .build()
    .await?;
```

### `pipeline-jsonl` (`JsonlPipeline`)

```toml
[dependencies]
spider-lib = { version = "2.0.4", features = ["pipeline-jsonl"] }
```

```rust,ignore
use spider_lib::prelude::*;

let crawler = CrawlerBuilder::new(MySpider)
    .add_pipeline(JsonlPipeline::new("output/items.jsonl"))
    .build()
    .await?;
```

### `pipeline-csv` (`CsvPipeline`)

```toml
[dependencies]
spider-lib = { version = "2.0.4", features = ["pipeline-csv"] }
```

```rust,ignore
use spider_lib::prelude::*;

let crawler = CrawlerBuilder::new(MySpider)
    .add_pipeline(CsvPipeline::new("output/items.csv"))
    .build()
    .await?;
```

### `pipeline-sqlite` (`SqlitePipeline`)

```toml
[dependencies]
spider-lib = { version = "2.0.4", features = ["pipeline-sqlite"] }
```

```rust,ignore
use spider_lib::prelude::*;

let crawler = CrawlerBuilder::new(MySpider)
    .add_pipeline(SqlitePipeline::new("output/items.db", "items"))
    .build()
    .await?;
```

### `pipeline-stream-json` (`StreamJsonPipeline`)

```toml
[dependencies]
spider-lib = { version = "2.0.4", features = ["pipeline-stream-json"] }
```

```rust,ignore
use spider_lib::prelude::*;

let crawler = CrawlerBuilder::new(MySpider)
    .add_pipeline(StreamJsonPipeline::new("output/items-stream.json"))
    .build()
    .await?;
```

## Pipeline Strategy

A common production sequence:

1. `TransformPipeline` for cleanup.
2. `ValidationPipeline` for schema checks.
3. `DeduplicationPipeline` to control duplicates.
4. One or more output pipelines (`JsonlPipeline`, `CsvPipeline`, etc.).

## Feature Flags

- `core` (default)
- `pipeline-csv`
- `pipeline-json`
- `pipeline-jsonl`
- `pipeline-sqlite`
- `pipeline-stream-json`

```toml
[dependencies]
spider-pipeline = { version = "0.3.4", features = ["pipeline-jsonl", "pipeline-csv"] }
```

When using via `spider-lib`, enable root features with the same names.

## Related Crates

- [`spider-lib`](../README.md)
- [`spider-core`](../spider-core/README.md)
- [`spider-util`](../spider-util/README.md)

## License

MIT. See [LICENSE](../LICENSE).
