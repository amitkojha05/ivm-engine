use std::collections::HashMap;

use ivm_connectors::DeadLetterRecord;
use ivm_core::{Batch, Row, Value, ZSet};

#[cfg(feature = "kafka")]
mod kafka_impl;

#[cfg(feature = "kafka")]
pub use kafka_impl::KafkaCdcConnector;

/// Stub when built without the `kafka` feature (e.g. local Windows dev without CMake).
#[cfg(not(feature = "kafka"))]
pub struct KafkaCdcConnector;

#[cfg(not(feature = "kafka"))]
impl KafkaCdcConnector {
    pub fn new(_brokers: &str, _group_id: &str, _topic: &str) -> anyhow::Result<Self> {
        anyhow::bail!("Kafka support not enabled; rebuild with `--features kafka`")
    }

    pub async fn poll_batch(&self, _max_messages: usize) -> anyhow::Result<Batch<Row>> {
        anyhow::bail!("Kafka support not enabled")
    }

    pub fn commit_sync(&self) -> anyhow::Result<()> {
        anyhow::bail!("Kafka support not enabled")
    }

    pub fn commit_offsets(&self) -> anyhow::Result<()> {
        anyhow::bail!("Kafka support not enabled")
    }
}

fn try_apply_cdc_payload(payload: &[u8], delta: &mut ZSet<Row>) -> anyhow::Result<()> {
    let envelope: serde_json::Value = serde_json::from_slice(payload)?;
    let op = envelope["op"].as_str().unwrap_or("c");
    match op {
        "c" | "r" => {
            let row = json_to_row(&envelope["after"]);
            delta.insert(row, 1);
        }
        "d" => {
            let row = json_to_row(&envelope["before"]);
            delta.insert(row, -1);
        }
        "u" => {
            let before = json_to_row(&envelope["before"]);
            let after = json_to_row(&envelope["after"]);
            delta.insert(before, -1);
            delta.insert(after, 1);
        }
        _ => {}
    }
    Ok(())
}

pub fn apply_cdc_payload(
    payload: &[u8],
    delta: &mut ZSet<Row>,
    dead_letters: &mut Vec<DeadLetterRecord>,
    source: &str,
    epoch: u64,
) {
    if let Err(e) = try_apply_cdc_payload(payload, delta) {
        dead_letters.push(DeadLetterRecord::new(source, epoch, payload, &e.to_string()));
        tracing::warn!(source, epoch, error = %e, "Dead-lettered malformed CDC payload");
    }
}

pub fn json_to_row(v: &serde_json::Value) -> Row {
    let mut map = HashMap::new();
    if let Some(obj) = v.as_object() {
        for (k, val) in obj {
            map.insert(k.clone(), json_value_to_typed(val));
        }
    }
    Row(map)
}

fn json_value_to_typed(val: &serde_json::Value) -> Value {
    match val {
        serde_json::Value::Number(n) => Value::Int(n.as_i64().unwrap_or(0)),
        serde_json::Value::String(s) => Value::Str(s.clone()),
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Null => Value::Null,
        other => Value::Str(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ivm_core::ZSet;

    #[test]
    fn decode_insert_envelope() {
        let payload = br#"{"op":"c","after":{"id":1,"name":"alice"}}"#;
        let mut delta = ZSet::new();
        let mut dead_letters = Vec::new();
        apply_cdc_payload(payload, &mut delta, &mut dead_letters, "test", 0);
        assert_eq!(delta.len(), 1);
        assert!(dead_letters.is_empty());
    }

    #[test]
    fn decode_update_envelope() {
        let payload = br#"{"op":"u","before":{"id":1,"name":"alice"},"after":{"id":1,"name":"bob"}}"#;
        let mut delta = ZSet::new();
        let mut dead_letters = Vec::new();
        apply_cdc_payload(payload, &mut delta, &mut dead_letters, "test", 0);
        assert_eq!(delta.len(), 2);
    }

    #[test]
    fn decode_delete_envelope() {
        let payload = br#"{"op":"d","before":{"id":1,"name":"alice"}}"#;
        let mut delta = ZSet::new();
        let mut dead_letters = Vec::new();
        apply_cdc_payload(payload, &mut delta, &mut dead_letters, "test", 0);
        assert_eq!(delta.inner.values().next(), Some(&-1));
    }

    #[test]
    fn malformed_payload_dead_letters() {
        let payload = b"not json";
        let mut delta = ZSet::new();
        let mut dead_letters = Vec::new();
        apply_cdc_payload(payload, &mut delta, &mut dead_letters, "test", 1);
        assert_eq!(dead_letters.len(), 1);
        assert!(delta.is_empty());
    }
}
