# spider-pipeline

Item pipelines for processing, filtering, and exporting scraped data in `spider-lib`.

## Install

```toml
[dependencies]
spider-pipeline = "0.3.4"
```

## Built-in Pipelines

Core (always available):

- `ConsolePipeline`
- `DeduplicationPipeline`

Optional (feature-gated):

- `pipeline-json` -> `JsonPipeline`
- `pipeline-jsonl` -> `JsonlPipeline`
- `pipeline-csv` -> `CsvPipeline`
- `pipeline-sqlite` -> `SqlitePipeline`
- `pipeline-stream-json` -> `StreamJsonPipeline`

## Usage

```rust,no_run
use spider_pipeline::{console::ConsolePipeline, dedup::DeduplicationPipeline};

let crawler = spider_core::CrawlerBuilder::new(MySpider)
    .add_pipeline(DeduplicationPipeline::new(&["url"]))
    .add_pipeline(ConsolePipeline::new())
    .build()
    .await?;
```

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

## License

MIT. See [LICENSE](./LICENSE).
