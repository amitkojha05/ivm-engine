use std::mem;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use futures::StreamExt;
use ivm_core::Batch;
use ivm_pg_wal::WalStreamConnector;
use tokio::sync::RwLock;
use tracing::{error, info};

use crate::checkpoint::CheckpointManager;
use crate::metrics::{BATCH_SIZE, ROWS_PER_SECOND, ROWS_PROCESSED};
use crate::pipeline::{Pipeline, SourceKind};

pub struct PipelineScheduler {
    pipeline: Arc<RwLock<Pipeline>>,
    checkpoint_mgr: Arc<RwLock<CheckpointManager>>,
    running: Arc<RwLock<bool>>,
}

impl PipelineScheduler {
    pub fn new(pipeline: Pipeline, checkpoint_dir: PathBuf) -> Self {
        Self {
            pipeline: Arc::new(RwLock::new(pipeline)),
            checkpoint_mgr: Arc::new(RwLock::new(CheckpointManager::new(checkpoint_dir))),
            running: Arc::new(RwLock::new(false)),
        }
    }

    pub async fn restore_checkpoint(&self) -> anyhow::Result<()> {
        let mut mgr = self.checkpoint_mgr.write().await;
        if let Some(zset) = mgr.restore()? {
            let mut pipeline = self.pipeline.write().await;
            pipeline.accumulated = zset;
        }
        Ok(())
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        *self.running.write().await = true;
        self.restore_checkpoint().await?;

        let pipeline = self.pipeline.read().await;
        let config = pipeline.config.clone();
        drop(pipeline);

        match &config.source {
            SourceKind::Kafka {
                brokers,
                topic,
                group_id,
            } => {
                self.run_kafka_loop(brokers, topic, group_id, config.checkpoint_interval_epochs)
                    .await
            }
            SourceKind::PgWal {
                conn_str,
                slot,
                publication,
            } => {
                self.run_pg_loop(conn_str, slot, publication, config.checkpoint_interval_epochs)
                    .await
            }
        }
    }

    pub async fn stop(&self) {
        *self.running.write().await = false;
    }

    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }

    async fn run_kafka_loop(
        &self,
        brokers: &str,
        topic: &str,
        group_id: &str,
        checkpoint_interval: u64,
    ) -> anyhow::Result<()> {
        #[cfg(feature = "kafka")]
        {
            let connector = ivm_kafka_cdc::KafkaCdcConnector::new(brokers, group_id, topic)
                .context("Kafka connector init")?;
            info!(topic, "Started Kafka source loop");

            while *self.running.read().await {
                match connector.poll_batch(100).await {
                    Ok(batch) if !batch.delta.is_empty() => {
                        self.process_batch(batch, checkpoint_interval).await?;
                    }
                    Ok(_) => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(e) => {
                        error!("Kafka poll error: {e}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
            Ok(())
        }
        #[cfg(not(feature = "kafka"))]
        {
            let _ = (brokers, topic, group_id, checkpoint_interval);
            anyhow::bail!("Kafka support not enabled; rebuild ivm-runtime with `--features kafka`")
        }
    }

    async fn run_pg_loop(
        &self,
        conn_str: &str,
        slot: &str,
        publication: &str,
        checkpoint_interval: u64,
    ) -> anyhow::Result<()> {
        let connector = WalStreamConnector::new(conn_str, slot, publication)
            .await
            .context("Postgres WAL stream connector init")?;
        info!(slot, "Started Postgres WAL real-time stream");

        let mut stream = connector
            .stream_events()
            .await
            .context("WAL stream init failed")?;
        let mut pending_delta = ivm_core::ZSet::default();

        while *self.running.read().await {
            match tokio::time::timeout(Duration::from_millis(200), stream.next()).await {
                Ok(Some(Ok(event))) => match event {
                    ivm_pg_wal::WalEvent::Insert { row, .. } => {
                        pending_delta.insert(row, 1);
                    }
                    ivm_pg_wal::WalEvent::Delete { row, .. } => {
                        pending_delta.insert(row, -1);
                    }
                    ivm_pg_wal::WalEvent::Update {
                        old_row,
                        new_row,
                        ..
                    } => {
                        pending_delta.insert(old_row, -1);
                        pending_delta.insert(new_row, 1);
                    }
                    ivm_pg_wal::WalEvent::Commit { lsn } => {
                        if !pending_delta.is_empty() {
                            let batch = Batch {
                                epoch: lsn,
                                delta: mem::take(&mut pending_delta),
                            };
                            self.process_batch(batch, checkpoint_interval).await?;
                        }
                        connector.acknowledge_lsn(lsn).await?;
                    }
                    ivm_pg_wal::WalEvent::Relation { .. } => {}
                },
                Ok(Some(Err(e))) => {
                    error!("WAL stream error: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    match connector.stream_events().await {
                        Ok(new_stream) => {
                            stream = new_stream;
                            info!("WAL stream reconnected");
                        }
                        Err(e) => {
                            error!("WAL reconnect failed: {e}");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
                Ok(None) => {
                    info!("WAL stream ended, reconnecting");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    stream = connector
                        .stream_events()
                        .await
                        .context("WAL stream reconnect")?;
                }
                Err(_timeout) => continue,
            }
        }

        info!("WAL stream loop stopped (running=false)");
        Ok(())
    }

    async fn process_batch(
        &self,
        batch: Batch<ivm_core::Row>,
        checkpoint_interval: u64,
    ) -> anyhow::Result<()> {
        let batch_size = batch.delta.inner.len() as f64;
        let start = Instant::now();

        let epoch = batch.epoch;
        let mut pipeline = self.pipeline.write().await;
        let _output = pipeline.apply_batch(batch);
        let accumulated = pipeline.accumulated.clone();
        drop(pipeline);

        let elapsed = start.elapsed().as_secs_f64();
        ROWS_PROCESSED.inc_by(batch_size);
        BATCH_SIZE.observe(batch_size);
        if elapsed > 0.0 {
            ROWS_PER_SECOND.set(batch_size / elapsed);
        }

        let mgr = self.checkpoint_mgr.read().await;
        if mgr.should_checkpoint(epoch, checkpoint_interval) {
            drop(mgr);
            self.checkpoint_mgr
                .write()
                .await
                .save(&accumulated, epoch)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::{PipelineConfig, SourceKind};

    #[tokio::test]
    async fn scheduler_not_running_by_default() {
        let config = PipelineConfig {
            name: "s".into(),
            source: SourceKind::Kafka {
                brokers: "localhost:9092".into(),
                topic: "t".into(),
                group_id: "g".into(),
            },
            operators: vec![],
            sql: None,
            checkpoint_interval_epochs: 10,
        };
        let scheduler = PipelineScheduler::new(
            Pipeline::new(config),
            PathBuf::from("/tmp/ivm_test_scheduler"),
        );
        assert!(!scheduler.is_running().await);
    }
}
