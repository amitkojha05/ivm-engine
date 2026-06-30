//! Shared connector abstractions for ivm-engine.
//! All connectors — Kafka, Postgres WAL, Delta Lake, Iceberg — implement
//! `InputConnector` or `OutputConnector` and are driven by the PipelineScheduler.

mod connector_state;

pub use connector_state::ConnectorState;

use async_trait::async_trait;
use ivm_core::{Batch, Row};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterRecord {
    pub source: String,
    pub epoch: u64,
    pub payload_hex: String,
    pub error: String,
    pub timestamp_ms: u64,
}

impl DeadLetterRecord {
    pub fn new(source: &str, epoch: u64, payload: &[u8], error: &str) -> Self {
        Self {
            source: source.into(),
            epoch,
            payload_hex: hex::encode(payload),
            error: error.into(),
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }
}

/// A source that produces batches of Z-set changes.
#[async_trait]
pub trait InputConnector: Send + Sync {
    async fn poll_batch(&self, max_rows: usize) -> anyhow::Result<Batch<Row>>;

    async fn commit(&self, epoch: u64) -> anyhow::Result<()>;

    async fn pause(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn resume(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn connector_id(&self) -> &str;

    fn delivery_semantics(&self) -> DeliverySemantics;

    fn connector_state(&self, checkpoint_epoch: u64) -> ConnectorState {
        let _ = checkpoint_epoch;
        ConnectorState::default()
    }
}

/// A sink that consumes Z-set change batches.
#[async_trait]
pub trait OutputConnector: Send + Sync {
    async fn write_batch(&self, batch: &Batch<Row>) -> anyhow::Result<()>;
    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }
    fn connector_id(&self) -> &str;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliverySemantics {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConnectorConfig {
    Kafka {
        brokers: String,
        topic: String,
        group_id: String,
        #[serde(default = "default_max_poll")]
        max_poll_records: usize,
    },
    PgWal {
        conn_str: String,
        slot: String,
        publication: String,
    },
    DeltaLake {
        table_uri: String,
        start_version: Option<i64>,
    },
    Iceberg {
        catalog_uri: String,
        namespace: String,
        table_name: String,
        start_snapshot_id: Option<i64>,
    },
}

fn default_max_poll() -> usize {
    1000
}
