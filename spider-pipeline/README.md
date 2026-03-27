# spider-pipeline

`spider-pipeline` is where scraped items get cleaned up, validated, filtered, and written out. Pipelines sit after parsing, so this crate is a good fit for data-shaping work that should stay separate from page extraction logic.

For normal app code you can enable pipelines through [`spider-lib`](../README.md). This crate is more useful when you want to compose pipelines directly or ship reusable item-processing stages.

## When to use it directly

Use `spider-pipeline` if you want to:

- build or publish custom pipelines
- compose processing stages without the root facade crate
- depend on output backends directly from lower-level runtime code

## Installation

```toml
[dependencies]
spider-pipeline = "0.4.0"
```

## Built-in pipelines

Always available:

| Type | Purpose |
| --- | --- |
| `TransformPipeline` | Apply field-level transformations. |
| `ValidationPipeline` | Enforce field presence, type, and value rules. |
| `DeduplicationPipeline` | Drop duplicate items by selected fields. |
| `ConsolePipeline` | Log items for visibility and debugging. |

Feature-gated output pipelines:

| Feature | Type | Output |
| --- | --- | --- |
| `pipeline-json` | `JsonPipeline` | JSON array file |
| `pipeline-jsonl` | `JsonlPipeline` | One JSON object per line |
| `pipeline-csv` | `CsvPipeline` | CSV file |
| `pipeline-sqlite` | `SqlitePipeline` | SQLite database |
| `pipeline-stream-json` | `StreamJsonPipeline` | Streaming JSON output |

## Composition example

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

That ordering is a good default: clean first, validate second, deduplicate next, then export or log.

## Pipeline contract in one sentence

Each pipeline receives an item, optionally mutates it, either forwards it with `Ok(Some(item))` or drops it with `Ok(None)`, and may persist side effects along the way.

## Custom pipeline example

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

## Runnable example in this repo

If you want to see a real pipeline setup instead of a skeleton, run:

```bash
cargo run --example books_live --features "live-stats pipeline-csv"
```

That example uses `CsvPipeline` and writes output to `output/books_live.csv`.

## When to choose a pipeline instead of middleware

Choose a pipeline when the concern is about scraped items after parsing.
Choose middleware when the concern is about requests, responses, retries, or other HTTP lifecycle behavior.

## Feature flags

```toml
[dependencies]
spider-pipeline = { version = "0.4.0", features = ["pipeline-jsonl", "pipeline-csv"] }
```

When used through the root crate, enable the same feature names on `spider-lib`.

## Related crates

- [`spider-lib`](../README.md)
- [`spider-core`](../spider-core/README.md)
- [`spider-util`](../spider-util/README.md)

## License

MIT. See [LICENSE](../LICENSE).
