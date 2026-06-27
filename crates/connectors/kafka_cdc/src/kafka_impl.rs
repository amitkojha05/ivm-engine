use anyhow::Context;
use futures::StreamExt;
use ivm_core::{Batch, ZSet};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
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

        let mut stream = self.consumer.stream();
        while count < max_messages {
            match tokio::time::timeout(std::time::Duration::from_millis(100), stream.next()).await
            {
                Ok(Some(Ok(msg))) => {
                    epoch = msg.offset().unwrap_or(0) as u64;
                    if let Some(payload) = msg.payload() {
                        super::apply_cdc_payload(payload, &mut delta)?;
                    }
                    count += 1;
                }
                Ok(Some(Err(e))) => return Err(e.into()),
                Ok(None) | Err(_) => break,
            }
        }

        self.consumer
            .commit_consumer_state(CommitMode::Async)
            .ok();

        Ok(Batch { epoch, delta })
    }

    pub fn commit_sync(&self) -> anyhow::Result<()> {
        self.consumer
            .commit_consumer_state(CommitMode::Sync)
            .context("Failed to commit Kafka offsets")
    }
}
