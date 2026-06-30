use std::mem;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use futures::StreamExt;
use ivm_connectors::InputConnector;
use ivm_core::{Batch, Watermark};
use ivm_pg_wal::WalStreamConnector;
use tokio::sync::RwLock;
use tracing::{error, info};

use crate::checkpoint::CheckpointManager;
use crate::metrics::{BATCH_SIZE, ROWS_PER_SECOND, ROWS_PROCESSED};
#[cfg(feature = "kafka")]
use crate::metrics::BACKPRESSURE_EVENTS;
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

    async fn pipeline_name_async(&self) -> String {
        self.pipeline.read().await.config.name.clone()
    }

    pub async fn restore_checkpoint(&self) -> anyhow::Result<bool> {
        let mut mgr = self.checkpoint_mgr.write().await;
        if let Some(zset) = mgr.restore()? {
            let mut pipeline = self.pipeline.write().await;
            pipeline.accumulated = zset;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        *self.running.write().await = true;
        self.checkpoint_mgr.read().await.cleanup_uncommitted()?;

        let restored = self.restore_checkpoint().await?;

        let pipeline = self.pipeline.read().await;
        let config = pipeline.config.clone();
        drop(pipeline);

        if let SourceKind::PgWal { conn_str, slot, .. } = &config.source {
            if !restored {
                let snap = ivm_snapshot::SnapshotConnector::new(
                    conn_str,
                    &config.bootstrap_table(),
                    slot,
                )
                .bootstrap()
                .await
                .context("Snapshot bootstrap failed")?;

                let mut pipeline = self.pipeline.write().await;
                pipeline.accumulated.merge(snap.snapshot);
                drop(pipeline);

                let accumulated = self.pipeline.read().await.accumulated.clone();
                self.checkpoint_mgr.write().await.save(&accumulated, 0)?;

                info!(
                    resume_lsn = snap.resume_lsn,
                    "Bootstrap checkpointed at epoch 0"
                );
            }
        }

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

            const HIGH_WATERMARK: usize = 50_000;
            const LOW_WATERMARK: usize = 10_000;
            let mut paused = false;

            while *self.running.read().await {
                let queue_depth = {
                    let pipeline = self.pipeline.read().await;
                    pipeline.accumulated.len()
                };
                let pipeline_name = self.pipeline_name_async().await;

                if !paused && queue_depth > HIGH_WATERMARK {
                    connector.pause().await.ok();
                    paused = true;
                    BACKPRESSURE_EVENTS
                        .with_label_values(&[&pipeline_name, topic])
                        .inc();
                    tracing::warn!(queue_depth, "Backpressure: pausing Kafka consumer");
                } else if paused && queue_depth < LOW_WATERMARK {
                    connector.resume().await.ok();
                    paused = false;
                }

                match connector.poll_batch(100).await {
                    Ok(batch) if !batch.delta.is_empty() => {
                        self.process_batch(batch, checkpoint_interval, &connector)
                            .await?;
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
                    ivm_pg_wal::WalEvent::Commit {
                        lsn,
                        commit_time_ms,
                    } => {
                        if !pending_delta.is_empty() {
                            let batch = Batch {
                                epoch: lsn,
                                delta: mem::take(&mut pending_delta),
                                watermark: Some(Watermark {
                                    event_time_ms: commit_time_ms,
                                    source_id: format!("pg:{}", connector.slot()),
                                }),
                            };
                            self.process_batch(batch, checkpoint_interval, &connector)
                                .await?;
                        }
                        connector.commit(lsn).await?;
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
        connector: &dyn InputConnector,
    ) -> anyhow::Result<()> {
        let epoch = batch.epoch;
        let batch_size = batch.delta.inner.len() as f64;
        let start = Instant::now();
        let pipeline_name = self.pipeline_name_async().await;

        let mut pipeline = self.pipeline.write().await;
        let _output = pipeline.apply_batch(batch);
        let accumulated = pipeline.accumulated.clone();
        drop(pipeline);

        let elapsed = start.elapsed().as_secs_f64();
        ROWS_PROCESSED
            .with_label_values(&[&pipeline_name, connector.connector_id()])
            .inc_by(batch_size);
        BATCH_SIZE.observe(batch_size);
        if elapsed > 0.0 {
            ROWS_PER_SECOND.set(batch_size / elapsed);
        }

        let should_ckpt = {
            let mgr = self.checkpoint_mgr.read().await;
            mgr.should_checkpoint(epoch, checkpoint_interval)
        };

        if should_ckpt {
            let connector_state = connector.connector_state(epoch);
            let tmp_path = {
                let mgr = self.checkpoint_mgr.read().await;
                mgr.save_tmp(&accumulated, epoch, &connector_state)?
            };

            connector
                .commit(epoch)
                .await
                .context("Source commit failed during checkpoint")?;

            self.checkpoint_mgr
                .write()
                .await
                .confirm(tmp_path, epoch, connector_state)?;
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
