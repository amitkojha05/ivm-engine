use prometheus::{
    register_counter, register_gauge, register_histogram, Counter, Gauge, Histogram, HistogramOpts,
    Opts,
};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref ROWS_PROCESSED: Counter = register_counter!(Opts::new(
        "ivm_rows_processed_total",
        "Total rows processed across all pipelines"
    ))
    .unwrap();

    pub static ref ROWS_PER_SECOND: Gauge = register_gauge!(Opts::new(
        "ivm_rows_per_second",
        "Current rows/sec throughput"
    ))
    .unwrap();

    pub static ref CHECKPOINT_DURATION: Histogram = register_histogram!(HistogramOpts::new(
        "ivm_checkpoint_duration_seconds",
        "Time taken to write a Parquet checkpoint"
    )
    .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]))
    .unwrap();

    pub static ref KAFKA_LAG: Gauge = register_gauge!(Opts::new(
        "ivm_kafka_lag_messages",
        "Kafka consumer lag (messages behind)"
    ))
    .unwrap();

    pub static ref WAL_LAG: Gauge =
        register_gauge!(Opts::new("ivm_wal_lag_bytes", "Postgres WAL replication lag (bytes)"))
            .unwrap();

    pub static ref PIPELINE_STATUS: Gauge = register_gauge!(Opts::new(
        "ivm_pipelines_running",
        "Number of currently running pipelines"
    ))
    .unwrap();

    pub static ref BATCH_SIZE: Histogram = register_histogram!(HistogramOpts::new(
        "ivm_batch_size_rows",
        "Number of rows in each processed batch"
    )
    .buckets(vec![1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0]))
    .unwrap();
}

pub fn update_kafka_lag(lag: i64) {
    KAFKA_LAG.set(lag as f64);
}

pub fn update_wal_lag(bytes: i64) {
    WAL_LAG.set(bytes as f64);
}

pub fn set_pipelines_running(count: i64) {
    PIPELINE_STATUS.set(count as f64);
}
