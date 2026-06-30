use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// The durable state of a connector at a given checkpoint epoch.
/// Serialised alongside the Z-set checkpoint in Parquet.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConnectorState {
    /// Kafka: highest committed offset per partition.
    #[serde(default)]
    pub kafka_offsets: HashMap<i32, i64>,

    /// Postgres WAL: highest confirmed LSN.
    pub postgres_lsn: Option<u64>,

    /// Delta Lake: highest committed table version.
    pub delta_version: Option<i64>,

    /// Apache Iceberg: snapshot ID.
    pub iceberg_snapshot_id: Option<i64>,

    /// The epoch at which this state was saved.
    #[serde(default)]
    pub checkpoint_epoch: u64,
}
