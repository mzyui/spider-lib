//! Pipeline/output constants.

/// Buffer size for CSV export pipeline.
pub const CSV_BUFFER_SIZE: usize = 8192;

/// Channel capacity for SQLite pipeline.
pub const SQLITE_CHANNEL_CAPACITY: usize = 100;

/// Default batch size for stream JSON pipeline.
pub const STREAM_JSON_DEFAULT_BATCH_SIZE: usize = 100;
