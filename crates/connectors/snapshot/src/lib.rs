use std::collections::HashMap;

use anyhow::Context;
use ivm_core::{Row, Value, ZSet};
use tokio_postgres::{Client, NoTls};

pub struct SnapshotConnector {
    conn_str: String,
    table: String,
    slot_name: String,
}

pub struct BootstrapResult {
    pub snapshot: ZSet<Row>,
    /// LSN at the moment the replication slot was created.
    pub resume_lsn: u64,
}

impl SnapshotConnector {
    pub fn new(conn_str: &str, table: &str, slot_name: &str) -> Self {
        Self {
            conn_str: conn_str.into(),
            table: table.into(),
            slot_name: slot_name.into(),
        }
    }

    pub async fn bootstrap(&self) -> anyhow::Result<BootstrapResult> {
        let (client, conn) = tokio_postgres::connect(&self.conn_str, NoTls)
            .await
            .context("Failed to connect to Postgres for snapshot")?;
        tokio::spawn(async move {
            conn.await.ok();
        });

        let resume_lsn = self.create_slot_or_get_existing_lsn(&client).await?;
        let snapshot = self.copy_table_snapshot(&client).await?;

        tracing::info!(
            table = %self.table,
            rows = snapshot.len(),
            resume_lsn,
            "Snapshot + CDC bootstrap complete"
        );

        Ok(BootstrapResult {
            snapshot,
            resume_lsn,
        })
    }

    async fn create_slot_or_get_existing_lsn(&self, client: &Client) -> anyhow::Result<u64> {
        let exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
                &[&self.slot_name],
            )
            .await?
            .get(0);

        if exists {
            let lsn_text: String = client
                .query_one(
                    "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
                    &[&self.slot_name],
                )
                .await?
                .get(0);
            return Ok(parse_lsn(&lsn_text));
        }

        let row = client
            .query_one(
                "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&self.slot_name],
            )
            .await
            .context("Failed to create replication slot for bootstrap")?;
        let lsn_text: String = row.get(0);
        Ok(parse_lsn(&lsn_text))
    }

    async fn copy_table_snapshot(&self, client: &Client) -> anyhow::Result<ZSet<Row>> {
        let rows = client
            .query(&format!("SELECT * FROM {}", self.table), &[])
            .await
            .context("Failed to snapshot table")?;

        let mut snapshot = ZSet::default();
        for pg_row in &rows {
            snapshot.insert(pg_row_to_ivm_row(pg_row), 1);
        }
        Ok(snapshot)
    }
}

fn pg_row_to_ivm_row(pg_row: &tokio_postgres::Row) -> Row {
    let mut map = HashMap::new();
    for (i, col) in pg_row.columns().iter().enumerate() {
        let value = match col.type_().name() {
            "int4" | "int8" | "bigint" | "integer" => pg_row
                .try_get::<_, i64>(i)
                .map(Value::Int)
                .unwrap_or(Value::Null),
            "bool" => pg_row
                .try_get::<_, bool>(i)
                .map(Value::Bool)
                .unwrap_or(Value::Null),
            _ => pg_row
                .try_get::<_, String>(i)
                .map(Value::Str)
                .unwrap_or(Value::Null),
        };
        map.insert(col.name().to_string(), value);
    }
    Row(map)
}

fn parse_lsn(s: &str) -> u64 {
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() != 2 {
        return 0;
    }
    let hi = u64::from_str_radix(parts[0], 16).unwrap_or(0);
    let lo = u64::from_str_radix(parts[1], 16).unwrap_or(0);
    (hi << 32) | lo
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_parsing() {
        assert_eq!(parse_lsn("0/1A2B3C"), 0x1A2B3C);
        assert_eq!(parse_lsn("1/0"), 1u64 << 32);
    }
}
