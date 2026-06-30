//! Delta Lake input connector (v0 skeleton).
//!
//! Version-polling connector implementing `InputConnector`. Production
//! optimization: diff add/remove actions from `_delta_log` JSON per version
//! instead of full file rescan (see README capability matrix).

use ivm_connectors::{ConnectorState, DeliverySemantics, InputConnector};
use ivm_core::{Batch, Row, ZSet};
use std::sync::Mutex;

pub struct DeltaLakeConnector {
    table_uri: String,
    current_version: Mutex<i64>,
    /// Latest known table version (simulated until real Delta log polling is wired).
    latest_version: Mutex<i64>,
}

impl DeltaLakeConnector {
    pub async fn new(table_uri: &str, start_version: Option<i64>) -> anyhow::Result<Self> {
        let version = start_version.unwrap_or(0);
        tracing::info!(table_uri, version, "Delta Lake connector initialised (v0 skeleton)");
        Ok(Self {
            table_uri: table_uri.into(),
            current_version: Mutex::new(version),
            latest_version: Mutex::new(version),
        })
    }

    /// Simulate discovering a new table version (for tests and demos).
    pub fn set_latest_version(&self, version: i64) {
        *self.latest_version.lock().unwrap() = version;
    }

    async fn poll_next_version(&self) -> anyhow::Result<Option<Batch<Row>>> {
        let latest = *self.latest_version.lock().unwrap();
        let mut current = self.current_version.lock().unwrap();

        if *current >= latest {
            return Ok(None);
        }
        let next = *current + 1;

        // v0: empty delta placeholder — real impl reads _delta_log/{version}.json
        // add/remove actions and loads only changed Parquet files via ivm_parquet.
        let delta = ZSet::<Row>::default();
        *current = next;
        tracing::info!(version = next, rows = delta.len(), "Delta version ingested (skeleton)");

        Ok(Some(Batch {
            epoch: next as u64,
            delta,
            watermark: None,
        }))
    }
}

#[async_trait::async_trait]
impl InputConnector for DeltaLakeConnector {
    async fn poll_batch(&self, _max_rows: usize) -> anyhow::Result<Batch<Row>> {
        match self.poll_next_version().await? {
            Some(batch) => Ok(batch),
            None => {
                let v = *self.current_version.lock().unwrap();
                Ok(Batch::empty(v as u64))
            }
        }
    }

    async fn commit(&self, _epoch: u64) -> anyhow::Result<()> {
        Ok(())
    }

    fn connector_id(&self) -> &str {
        &self.table_uri
    }

    fn delivery_semantics(&self) -> DeliverySemantics {
        DeliverySemantics::ExactlyOnce
    }

    fn connector_state(&self, checkpoint_epoch: u64) -> ConnectorState {
        ConnectorState {
            delta_version: Some(*self.current_version.lock().unwrap()),
            checkpoint_epoch,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn polls_new_versions() {
        let c = DeltaLakeConnector::new("s3://bucket/table", Some(0))
            .await
            .unwrap();
        c.set_latest_version(2);
        let b1 = c.poll_batch(100).await.unwrap();
        assert_eq!(b1.epoch, 1);
        let b2 = c.poll_batch(100).await.unwrap();
        assert_eq!(b2.epoch, 2);
        let b3 = c.poll_batch(100).await.unwrap();
        assert!(b3.delta.is_empty());
    }
}
