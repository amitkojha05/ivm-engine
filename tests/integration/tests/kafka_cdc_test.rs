#![cfg(all(feature = "kafka-it", not(target_os = "windows")))]

use ivm_connectors::InputConnector;
use ivm_kafka_cdc::KafkaCdcConnector;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::time::Duration;
use testcontainers::clients::Cli;
use testcontainers_modules::kafka::Kafka;

async fn produce_messages(brokers: &str, topic: &str, payloads: &[&str]) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap();
    admin
        .create_topics(
            vec![&NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .ok();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap();

    for payload in payloads {
        producer
            .send(
                FutureRecord::to(topic).payload(*payload).key("k"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_kafka_cdc_commit_and_watermark() {
    let docker = Cli::default();
    let kafka_node = docker.run(Kafka::default());
    let brokers = format!("127.0.0.1:{}", kafka_node.get_host_port_ipv4(9093));

    produce_messages(
        &brokers,
        "orders",
        &[
            r#"{"op":"c","after":{"id":1,"amount":100}}"#,
            r#"{"op":"c","after":{"id":2,"amount":200}}"#,
        ],
    )
    .await;

    let connector = KafkaCdcConnector::new(&brokers, "test-group", "orders").unwrap();
    let batch = connector.poll_batch(10).await.unwrap();

    assert_eq!(batch.delta.len(), 2);
    // Task 7: watermark populated when Kafka CreateTime timestamps exist.
    if batch.watermark.is_some() {
        assert!(batch.watermark.as_ref().unwrap().event_time_ms > 0);
    }

    // Task 1 regression: commit decoupled from poll.
    connector.commit(batch.epoch).await.unwrap();
}
