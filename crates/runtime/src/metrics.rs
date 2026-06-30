use prometheus::{
    register_counter_vec, register_gauge, register_gauge_vec, register_histogram,
    CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, Opts,
};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref ROWS_PROCESSED: CounterVec = register_counter_vec!(
        "ivm_rows_processed_total",
        "Rows processed per pipeline and connector",
        &["pipeline", "connector"]
    )
    .unwrap();

    pub static ref DEAD_LETTER_TOTAL: CounterVec = register_counter_vec!(
        "ivm_dead_letters_total",
        "Malformed payloads sent to dead-letter queue",
        &["connector"]
    )
    .unwrap();

    pub static ref CONSUMER_LAG: GaugeVec = register_gauge_vec!(
        "ivm_consumer_lag",
        "Source consumer lag (messages for Kafka, bytes for WAL)",
        &["connector", "unit"]
    )
    .unwrap();

    pub static ref CHECKPOINT_AGE_SECS: GaugeVec = register_gauge_vec!(
        "ivm_checkpoint_age_seconds",
        "Seconds since last successful checkpoint per pipeline",
        &["pipeline"]
    )
    .unwrap();

    pub static ref BACKPRESSURE_EVENTS: CounterVec = register_counter_vec!(
        "ivm_backpressure_events_total",
        "Times consumer was paused due to backpressure",
        &["pipeline", "connector"]
    )
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

pub fn update_kafka_lag(connector: &str, lag: i64) {
    CONSUMER_LAG
        .with_label_values(&[connector, "messages"])
        .set(lag as f64);
}

pub fn update_wal_lag(connector: &str, bytes: i64) {
    CONSUMER_LAG
        .with_label_values(&[connector, "bytes"])
        .set(bytes as f64);
}

pub fn set_pipelines_running(count: i64) {
    PIPELINE_STATUS.set(count as f64);
}
