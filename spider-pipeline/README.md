# spider-pipeline

Item pipelines for processing, filtering, and exporting scraped data in `spider-lib`.

Use this crate directly when you need pipeline functionality without taking the full facade crate, or when you want to build custom item processing stages against the lower-level API.

## When to Use This Crate Directly

Use `spider-pipeline` if you want to:

- build or publish custom pipelines
- work directly with pipeline composition primitives
- use export pipelines without depending on the facade crate

If you are building a normal application spider, `spider-lib` is usually the easiest entry point.

## Installation

```toml
[dependencies]
spider-pipeline = "0.3.6"
```

## Pipeline Catalog

### Core pipelines

| Type | Purpose |
| --- | --- |
| `TransformPipeline` | Normalize or transform item fields. |
| `ValidationPipeline` | Enforce field and type rules. |
| `DeduplicationPipeline` | Drop duplicate items by key fields. |
| `ConsolePipeline` | Print processed items for visibility and debugging. |

### Optional output pipelines

| Feature | Type | Output |
| --- | --- | --- |
| `pipeline-json` | `JsonPipeline` | JSON array file |
| `pipeline-jsonl` | `JsonlPipeline` | One JSON object per line |
| `pipeline-csv` | `CsvPipeline` | CSV file |
| `pipeline-sqlite` | `SqlitePipeline` | SQLite database |
| `pipeline-stream-json` | `StreamJsonPipeline` | Streaming JSON output |

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

## Output Examples

`JsonlPipeline` writes one item per line:

```json
{"title":"Example","url":"https://example.com"}
{"title":"Another","url":"https://example.com/2"}
```

`CsvPipeline` produces standard tabular output:

```csv
title,url
Example,https://example.com
Another,https://example.com/2
```

## Feature Flags

```toml
[dependencies]
spider-pipeline = { version = "0.3.6", features = ["pipeline-jsonl", "pipeline-csv"] }
```

When used through `spider-lib`, enable the same feature names on the root crate.

## Pipeline Strategy

A common production sequence:

1. `TransformPipeline` for cleanup.
2. `ValidationPipeline` for schema checks.
3. `DeduplicationPipeline` to control duplicates.
4. One or more output pipelines such as `JsonlPipeline` or `CsvPipeline`.

## Common Gotchas

- Export pipelines are feature-gated.
- Pipeline order matters: transform before validate, validate before export is a common default.
- If you need framework integration plus middleware and runtime setup, starting from `spider-lib` is simpler.

## Related Crates

- [`spider-lib`](../README.md)
- [`spider-core`](../spider-core/README.md)
- [`spider-util`](../spider-util/README.md)

## License

MIT. See [LICENSE](../LICENSE).
