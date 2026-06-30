mod stream_connector;
mod streaming;

pub use stream_connector::WalStreamConnector;
pub use streaming::{parse_pgoutput_message, events_to_zset, WalEvent};

use std::collections::HashMap;

use anyhow::Context;
use ivm_core::{Batch, Row, Value, ZSet};
use tokio_postgres::{Client, NoTls};

pub struct PgWalConnector {
    client: Client,
    slot: String,
    publication: String,
}

impl PgWalConnector {
    pub async fn new(
        conn_str: &str,
        slot: &str,
        publication: &str,
    ) -> anyhow::Result<Self> {
        let (client, connection) = tokio_postgres::connect(conn_str, NoTls)
            .await
            .context("Failed to connect to Postgres")?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Postgres connection error: {e}");
            }
        });

        let slot_exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
                &[&slot],
            )
            .await
            .context("Failed to check replication slot")?
            .get(0);

        if !slot_exists {
            client
                .batch_execute(&format!(
                    "SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput')"
                ))
                .await
                .context("Failed to create replication slot")?;
        }

        Ok(Self {
            client,
            slot: slot.into(),
            publication: publication.into(),
        })
    }

    pub fn slot(&self) -> &str {
        &self.slot
    }

    /// Peek at WAL changes and decode into Z-set delta.
    pub async fn poll_batch(&self, max_changes: i32) -> anyhow::Result<Batch<Row>> {
        let query = format!(
            "SELECT data FROM pg_logical_slot_get_changes('{}', NULL, {}, \
             'proto_version', '1', 'publication_names', '{}')",
            self.slot, max_changes, self.publication
        );

        let rows = self
            .client
            .query(&query, &[])
            .await
            .context("Failed to read WAL changes")?;

        let mut delta = ZSet::default();
        let mut epoch = 0u64;

        for (i, row) in rows.iter().enumerate() {
            let data: &str = row.get(0);
            apply_wal_change(data, &mut delta);
            epoch = i as u64 + 1;
        }

        Ok(Batch {
            epoch,
            delta,
            watermark: None,
        })
    }

    pub async fn ensure_publication(&self, tables: &[&str]) -> anyhow::Result<()> {
        let table_list = tables.join(", ");
        let sql = format!(
            "CREATE PUBLICATION IF NOT EXISTS {} FOR TABLE {}",
            self.publication, table_list
        );
        self.client
            .batch_execute(&sql)
            .await
            .context("Failed to create publication")?;
        Ok(())
    }
}

pub fn apply_wal_change(data: &str, delta: &mut ZSet<Row>) {
    for event in crate::streaming::parse_wal_data(data) {
        match event {
            WalEvent::Insert { row, .. } => delta.insert(row, 1),
            WalEvent::Delete { row, .. } => delta.insert(row, -1),
            WalEvent::Update { old_row, new_row, .. } => {
                delta.insert(old_row, -1);
                delta.insert(new_row, 1);
            }
            _ => {}
        }
    }
}

pub fn json_to_row(v: &serde_json::Value) -> Row {
    let mut map = HashMap::new();
    if let Some(obj) = v.as_object() {
        for (k, val) in obj {
            map.insert(k.clone(), json_value_to_typed(val));
        }
    } else if let Some(arr) = v.as_array() {
        for col in arr {
            if let (Some(name), Some(value)) = (col.get("name"), col.get("value")) {
                if let Some(n) = name.as_str() {
                    map.insert(n.to_string(), json_value_to_typed(value));
                }
            }
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
    fn decode_insert_wal_json() {
        let data = r#"{"action":"I","columns":{"id":1,"name":"alice"}}"#;
        let mut delta = ZSet::new();
        apply_wal_change(data, &mut delta);
        assert_eq!(delta.len(), 1);
    }

    #[test]
    fn decode_delete_wal_json() {
        let data = r#"{"action":"D","identity":{"id":1}}"#;
        let mut delta = ZSet::new();
        apply_wal_change(data, &mut delta);
        assert_eq!(delta.inner.values().next(), Some(&-1));
    }
}
