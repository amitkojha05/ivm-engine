use anyhow::Context;
use futures::StreamExt;
use ivm_connectors::{ConnectorState, DeliverySemantics, InputConnector};
use ivm_core::{Batch, Row, Watermark, ZSet};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Message, Timestamp};
use rdkafka::ClientConfig;

pub struct KafkaCdcConnector {
    consumer: StreamConsumer,
    topic: String,
}

impl KafkaCdcConnector {
    pub fn new(brokers: &str, group_id: &str, topic: &str) -> anyhow::Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .context("Kafka consumer creation failed")?;

        consumer
            .subscribe(&[topic])
            .context("Failed to subscribe to topic")?;

        Ok(Self {
            consumer,
            topic: topic.into(),
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub async fn poll_batch(&self, max_messages: usize) -> anyhow::Result<Batch<Row>> {
        let mut delta = ZSet::default();
        let mut epoch = 0u64;
        let mut count = 0;
        let mut max_event_time_ms: u64 = 0;
        let mut dead_letters = Vec::new();

        let mut stream = self.consumer.stream();
        while count < max_messages {
            match tokio::time::timeout(std::time::Duration::from_millis(100), stream.next()).await
            {
                Ok(Some(Ok(msg))) => {
                    epoch = msg.offset().unwrap_or(0) as u64;
                    if let Timestamp::CreateTime(ts) = msg.timestamp() {
                        max_event_time_ms = max_event_time_ms.max(ts.max(0) as u64);
                    }
                    if let Some(payload) = msg.payload() {
                        super::apply_cdc_payload(
                            payload,
                            &mut delta,
                            &mut dead_letters,
                            &self.topic,
                            epoch,
                        );
                    }
                    count += 1;
                }
                Ok(Some(Err(e))) => return Err(e.into()),
                Ok(None) | Err(_) => break,
            }
        }

        const ALLOWED_LATENESS_MS: u64 = 5_000;
        let watermark = if max_event_time_ms > 0 {
            Some(Watermark {
                event_time_ms: max_event_time_ms.saturating_sub(ALLOWED_LATENESS_MS),
                source_id: self.topic.clone(),
            })
        } else {
            None
        };

        Ok(Batch {
            epoch,
            delta,
            watermark,
        })
    }

    pub fn pause_partitions(&self) -> anyhow::Result<()> {
        let assignment = self
            .consumer
            .assignment()
            .context("Failed to read Kafka assignment")?;
        self.consumer
            .pause(&assignment)
            .context("Failed to pause Kafka partitions")?;
        tracing::warn!(topic = %self.topic, "Kafka consumption PAUSED (backpressure)");
        Ok(())
    }

    pub fn resume_partitions(&self) -> anyhow::Result<()> {
        let assignment = self
            .consumer
            .assignment()
            .context("Failed to read Kafka assignment")?;
        self.consumer
            .resume(&assignment)
            .context("Failed to resume Kafka partitions")?;
        tracing::info!(topic = %self.topic, "Kafka consumption RESUMED");
        Ok(())
    }

    /// Commit current consumer offsets to Kafka.
    /// Call this ONLY after apply_batch() and checkpoint both succeed.
    pub fn commit_offsets(&self) -> anyhow::Result<()> {
        self.consumer
            .commit_consumer_state(CommitMode::Sync)
            .context("Failed to commit Kafka offsets")
    }
}

#[async_trait::async_trait]
impl InputConnector for KafkaCdcConnector {
    async fn poll_batch(&self, max_rows: usize) -> anyhow::Result<Batch<Row>> {
        self.poll_batch(max_rows).await
    }

    async fn commit(&self, _epoch: u64) -> anyhow::Result<()> {
        self.commit_offsets()
    }

    async fn pause(&self) -> anyhow::Result<()> {
        self.pause_partitions()
    }

    async fn resume(&self) -> anyhow::Result<()> {
        self.resume_partitions()
    }

    fn connector_id(&self) -> &str {
        &self.topic
    }

    fn delivery_semantics(&self) -> DeliverySemantics {
        DeliverySemantics::AtLeastOnce
    }

    fn connector_state(&self, checkpoint_epoch: u64) -> ConnectorState {
        let mut kafka_offsets = std::collections::HashMap::new();
        if let Ok(assignment) = self.consumer.assignment() {
            for elem in assignment.elements() {
                if let Some(offset) = elem.offset().to_raw() {
                    kafka_offsets.insert(elem.partition(), offset);
                }
            }
        }
        ConnectorState {
            kafka_offsets,
            checkpoint_epoch,
            ..Default::default()
        }
    }
}
