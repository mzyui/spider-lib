use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use spider_pipeline::pipeline::Pipeline;
#[cfg(feature = "pipeline-stream-json")]
use spider_pipeline::stream_json::StreamJsonPipeline;
use spider_pipeline::transform::{TransformOperation, TransformPipeline};
use spider_pipeline::validation::{JsonType, ValidationPipeline, ValidationRule};
use spider_util::item::ScrapedItem;
use std::any::Any;
#[cfg(feature = "pipeline-stream-json")]
use std::fs;
#[cfg(feature = "pipeline-stream-json")]
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProductItem {
    title: String,
    slug: String,
    stock: i32,
}

impl ScrapedItem for ProductItem {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn box_clone(&self) -> Box<dyn ScrapedItem + Send + Sync> {
        Box::new(self.clone())
    }

    fn to_json_value(&self) -> Value {
        serde_json::to_value(self).expect("serialize test item")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TitleOnlyItem {
    title: String,
}

impl ScrapedItem for TitleOnlyItem {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn box_clone(&self) -> Box<dyn ScrapedItem + Send + Sync> {
        Box::new(self.clone())
    }

    fn to_json_value(&self) -> Value {
        serde_json::to_value(self).expect("serialize test item")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValidationItem {
    title: String,
    price: f64,
}

impl ScrapedItem for ValidationItem {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn box_clone(&self) -> Box<dyn ScrapedItem + Send + Sync> {
        Box::new(self.clone())
    }

    fn to_json_value(&self) -> Value {
        serde_json::to_value(self).expect("serialize test item")
    }
}

#[tokio::test]
async fn applies_string_operations() {
    let pipeline = TransformPipeline::<ProductItem>::new()
        .with_operation(TransformOperation::Trim {
            field: "title".to_string(),
        })
        .with_operation(TransformOperation::Lowercase {
            field: "slug".to_string(),
        });

    let out = pipeline
        .process_item(ProductItem {
            title: "  Book  ".to_string(),
            slug: "HELLO-WORLD".to_string(),
            stock: 1,
        })
        .await
        .expect("pipeline should not fail")
        .expect("item should pass");

    assert_eq!(out.title, "Book");
    assert_eq!(out.slug, "hello-world");
}

#[tokio::test]
async fn applies_rename_remove_set_and_default() {
    let pipeline = TransformPipeline::<TitleOnlyItem>::new()
        .with_operation(TransformOperation::Rename {
            from: "title".to_string(),
            to: "title".to_string(),
        })
        .with_operation(TransformOperation::SetDefault {
            field: "title".to_string(),
            value: json!("fallback"),
        })
        .with_operation(TransformOperation::Set {
            field: "title".to_string(),
            value: json!("final"),
        })
        .with_operation(TransformOperation::Remove {
            field: "missing".to_string(),
        });

    let out = pipeline
        .process_item(TitleOnlyItem {
            title: "old".to_string(),
        })
        .await
        .expect("pipeline should not fail")
        .expect("item should pass");

    assert_eq!(out.title, "final");
}

#[tokio::test]
async fn applies_custom_transform() {
    let pipeline = TransformPipeline::<ProductItem>::new().with_transform(|json| {
        let map = json
            .as_object_mut()
            .ok_or_else(|| "object expected".to_string())?;
        map.insert("stock".to_string(), json!(42));
        Ok(())
    });

    let out = pipeline
        .process_item(ProductItem {
            title: "A".to_string(),
            slug: "b".to_string(),
            stock: 0,
        })
        .await
        .expect("pipeline should not fail")
        .expect("item should pass");

    assert_eq!(out.stock, 42);
}

#[tokio::test]
async fn drops_on_deserialize_failure_after_transform() {
    let pipeline = TransformPipeline::<ProductItem>::new().with_transform(|json| {
        let map = json
            .as_object_mut()
            .ok_or_else(|| "object expected".to_string())?;
        map.insert("stock".to_string(), json!("not_a_number"));
        Ok(())
    });

    let out = pipeline
        .process_item(ProductItem {
            title: "A".to_string(),
            slug: "b".to_string(),
            stock: 0,
        })
        .await
        .expect("pipeline should not fail");

    assert!(out.is_none());
}

#[tokio::test]
async fn missing_field_operation_is_noop() {
    let pipeline =
        TransformPipeline::<ProductItem>::new().with_operation(TransformOperation::Uppercase {
            field: "missing".to_string(),
        });

    let out = pipeline
        .process_item(ProductItem {
            title: "A".to_string(),
            slug: "b".to_string(),
            stock: 1,
        })
        .await
        .expect("pipeline should not fail")
        .expect("item should pass");

    assert_eq!(out.title, "A");
    assert_eq!(out.slug, "b");
}

#[tokio::test]
async fn passes_valid_item() {
    let pipeline = ValidationPipeline::<ValidationItem>::new()
        .with_rule("title", ValidationRule::Required)
        .with_rule("title", ValidationRule::NonEmptyString)
        .with_rule("price", ValidationRule::MinNumber(1.0))
        .with_rule("price", ValidationRule::MaxNumber(100.0));

    let item = ValidationItem {
        title: "Book".to_string(),
        price: 20.0,
    };

    let out = pipeline
        .process_item(item)
        .await
        .expect("pipeline should not fail");
    assert!(out.is_some());
}

#[tokio::test]
async fn drops_missing_required_field() {
    let pipeline =
        ValidationPipeline::<ValidationItem>::new().with_rule("missing", ValidationRule::Required);
    let item = ValidationItem {
        title: "Book".to_string(),
        price: 20.0,
    };

    let out = pipeline
        .process_item(item)
        .await
        .expect("pipeline should not fail");
    assert!(out.is_none());
}

#[tokio::test]
async fn drops_on_custom_validator_error() {
    let pipeline = ValidationPipeline::<ValidationItem>::new().with_validator(|_item, json| {
        match json.get("title").and_then(Value::as_str) {
            Some("Book") => Ok(()),
            _ => Err("title mismatch".to_string()),
        }
    });

    let item = ValidationItem {
        title: "Other".to_string(),
        price: 20.0,
    };

    let out = pipeline
        .process_item(item)
        .await
        .expect("pipeline should not fail");
    assert!(out.is_none());
}

#[tokio::test]
async fn drops_on_invalid_type_rule() {
    let pipeline = ValidationPipeline::<ValidationItem>::new()
        .with_rule("title", ValidationRule::Type(JsonType::Number));
    let item = ValidationItem {
        title: "Book".to_string(),
        price: 20.0,
    };

    let out = pipeline
        .process_item(item)
        .await
        .expect("pipeline should not fail");
    assert!(out.is_none());
}

#[cfg(feature = "pipeline-stream-json")]
fn temp_json_output_path() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time before epoch")
        .as_nanos();
    std::env::temp_dir()
        .join(format!(
            "spider_stream_pipeline_{}_{}.json",
            std::process::id(),
            nanos
        ))
        .display()
        .to_string()
}

#[cfg(feature = "pipeline-stream-json")]
#[tokio::test]
async fn stream_json_pipeline_writes_valid_json_array() {
    let path = temp_json_output_path();
    let pipeline = StreamJsonPipeline::<ProductItem>::with_batch_size(&path, 2)
        .expect("pipeline should initialize");

    pipeline
        .process_item(ProductItem {
            title: "Book One".to_string(),
            slug: "book-one".to_string(),
            stock: 3,
        })
        .await
        .expect("first item should be accepted");
    pipeline
        .process_item(ProductItem {
            title: "Book Two".to_string(),
            slug: "book-two".to_string(),
            stock: 8,
        })
        .await
        .expect("second item should be accepted");
    pipeline
        .process_item(ProductItem {
            title: "Book Three".to_string(),
            slug: "book-three".to_string(),
            stock: 13,
        })
        .await
        .expect("third item should be accepted");

    pipeline
        .close()
        .await
        .expect("pipeline should close cleanly");

    let contents = fs::read_to_string(&path).expect("output file should exist");
    let value: Value = serde_json::from_str(&contents).expect("output should be valid JSON");
    let items = value.as_array().expect("output should be a JSON array");

    assert_eq!(items.len(), 3);
    assert_eq!(items[0]["title"], json!("Book One"));
    assert_eq!(items[2]["slug"], json!("book-three"));

    let _ = fs::remove_file(path);
}

#[tokio::test]
async fn handles_multiple_rules() {
    let pipeline = ValidationPipeline::<ValidationItem>::new()
        .with_rule("title", ValidationRule::MinLen(2))
        .with_rule("title", ValidationRule::MaxLen(10))
        .with_validator(|_, _| Ok(()));
    let item = ValidationItem {
        title: "ok".to_string(),
        price: 5.0,
    };
    let out = pipeline
        .process_item(item)
        .await
        .expect("pipeline should not fail");
    assert_eq!(
        out.expect("item should pass").to_json_value(),
        json!({"title":"ok","price":5.0})
    );
}

#[cfg(feature = "pipeline-csv")]
#[derive(Clone, Debug)]
struct CsvItem {
    id: u32,
    title: &'static str,
}

#[cfg(feature = "pipeline-csv")]
impl ScrapedItem for CsvItem {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn box_clone(&self) -> Box<dyn ScrapedItem + Send + Sync> {
        Box::new(self.clone())
    }

    fn to_json_value(&self) -> Value {
        json!({
            "id": self.id,
            "title": self.title,
        })
    }
}

#[cfg(feature = "pipeline-csv")]
fn temp_csv_path() -> std::path::PathBuf {
    use std::time::{SystemTime, UNIX_EPOCH};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("spider_csv_restore_{nanos}.csv"))
}

#[cfg(feature = "pipeline-csv")]
#[tokio::test]
async fn restore_state_writes_header_for_empty_file() {
    use spider_pipeline::csv::CsvPipeline;
    use std::fs;

    let path = temp_csv_path();
    fs::write(&path, "").expect("should create empty csv file");

    let initial = CsvPipeline::<CsvItem>::new(&path).expect("csv pipeline should build");
    initial
        .process_item(CsvItem {
            id: 1,
            title: "first",
        })
        .await
        .expect("initial item should write");
    let state = initial
        .get_state()
        .await
        .expect("state fetch should succeed")
        .expect("csv state should exist");
    initial.close().await.expect("initial close should succeed");

    fs::write(&path, "").expect("should reset csv file to empty");

    let restored = CsvPipeline::<CsvItem>::new(&path).expect("csv pipeline should rebuild");
    restored
        .restore_state(state)
        .await
        .expect("restore state should succeed");
    restored
        .process_item(CsvItem {
            id: 2,
            title: "second",
        })
        .await
        .expect("restored item should write");
    restored
        .close()
        .await
        .expect("restored close should succeed");

    let written = fs::read_to_string(&path).expect("csv output should be readable");
    assert_eq!(written.lines().next(), Some("id,title"));
    assert!(written.contains("2,second"));

    let _ = fs::remove_file(path);
}
