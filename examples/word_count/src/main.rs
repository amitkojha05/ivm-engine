//! Hello-world pipeline: Kafka → filter → count → Parquet checkpoint.
//!
//! ```bash
//! cargo run -p word_count -- \
//!   --brokers localhost:9092 \
//!   --topic words \
//!   --group-id ivm-word-count
//! ```

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Context;
use ivm_core::{Batch, Row, Value, ZSet};
use ivm_kafka_cdc::KafkaCdcConnector;
use ivm_operators::{filter, AggregateState};
use ivm_runtime::CheckpointManager;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let brokers = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "localhost:9092".into());
    let topic = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "words".into());
    let group_id = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "ivm-word-count".into());
    let checkpoint_dir = PathBuf::from("./checkpoints/word_count");

    let connector = KafkaCdcConnector::new(&brokers, &group_id, &topic)
        .context("Failed to create Kafka connector")?;

    let mut agg = AggregateState::count("word");
    let mut checkpoint_mgr = CheckpointManager::new(&checkpoint_dir);
    if let Some(restored) = checkpoint_mgr.restore()? {
        agg.accumulated = restored;
        tracing::info!(epoch = checkpoint_mgr.last_epoch(), "Restored from checkpoint");
    }

    tracing::info!(%brokers, %topic, "Word count pipeline running");

    loop {
        let batch = connector.poll_batch(50).await?;
        if batch.delta.is_empty() {
            continue;
        }

        let filtered = filter(batch, |row| {
            row.get_str("word")
                .map(|w| !w.is_empty())
                .unwrap_or(false)
        });

        let output_delta = agg.apply_delta(&filtered.delta);
        let epoch = filtered.epoch;

        if !output_delta.is_empty() {
            for (row, weight) in &output_delta.inner {
                if *weight > 0 {
                    tracing::info!(
                        word = row.get_str("key").unwrap_or("?"),
                        count = row.get_int("value"),
                        "count update"
                    );
                }
            }
        }

        if checkpoint_mgr.should_checkpoint(epoch, 100) {
            checkpoint_mgr.save(&agg.accumulated, epoch)?;
        }
    }
}

#[allow(dead_code)]
fn demo_offline() -> Batch<Row> {
    let mut delta = ZSet::new();
    for word in ["hello", "world", "hello"] {
        delta.insert(
            Row(HashMap::from([("word".into(), Value::Str(word.into()))])),
            1,
        );
    }
    Batch::new(1, delta)
}
