//! Streaming WAL connector setup — uses logical replication slot + pgoutput.
//!
//! For environments where `START_REPLICATION` copy-both mode is available,
//! the binary parser in [`streaming`] handles real-time pgoutput frames.
//! The default [`PgWalConnector::poll_batch`] uses slot peek for local dev.

use std::pin::Pin;
use std::time::Duration;

use anyhow::Context;
use futures::stream::Stream;
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls};

use crate::streaming::{events_to_zset, WalEvent};
use ivm_core::{Batch, Row};

pub struct WalStreamConnector {
    client: Client,
    conn_str: String,
    slot: String,
    publication: String,
}

impl WalStreamConnector {
    pub async fn new(conn_str: &str, slot: &str, publication: &str) -> anyhow::Result<Self> {
        let (client, connection) = tokio_postgres::connect(conn_str, NoTls)
            .await
            .context("Failed to connect to Postgres")?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Postgres connection error: {e}");
            }
        });

        client
            .batch_execute(&format!(
                "CREATE PUBLICATION IF NOT EXISTS {publication} FOR ALL TABLES"
            ))
            .await
            .ok();

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
            conn_str: conn_str.into(),
            slot: slot.into(),
            publication: publication.into(),
        })
    }

    /// Poll WAL changes and decode into structured events.
    pub async fn poll_events(&self, max_changes: i32) -> anyhow::Result<Vec<WalEvent>> {
        let query = format!(
            "SELECT data FROM pg_logical_slot_get_changes('{}', NULL, {}, \
             'proto_version', '1', 'publication_names', '{}')",
            self.slot, max_changes, self.publication
        );

        let rows = self.client.query(&query, &[]).await?;
        let mut events = Vec::new();

        for row in rows {
            let data: &str = row.get(0);
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(data) {
                events.extend(json_wal_to_events(&v));
            } else if let Ok(event) = crate::streaming::parse_pgoutput_message(data.as_bytes()) {
                events.push(event);
            }
        }

        Ok(events)
    }

    /// Poll and convert to a Z-set batch.
    pub async fn poll_batch(&self, max_changes: i32) -> anyhow::Result<Batch<Row>> {
        let events = self.poll_events(max_changes).await?;
        let delta = events_to_zset(&events);
        Ok(Batch {
            epoch: events.len() as u64,
            delta,
        })
    }

    /// Stream WAL events as a real-time stream.
    pub async fn stream_events(
        &self,
    ) -> anyhow::Result<Pin<Box<dyn Stream<Item = anyhow::Result<WalEvent>> + Send + 'static>>> {
        let conn_str = self.conn_str.clone();
        let slot = self.slot.clone();
        let publication = self.publication.clone();
        let (tx, rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            let (client, connection) = match tokio_postgres::connect(&conn_str, NoTls).await {
                Ok(value) => value,
                Err(err) => {
                    let _ = tx.send(Err(anyhow::anyhow!(err)));
                    return;
                }
            };

            tokio::spawn(async move {
                if let Err(err) = connection.await {
                    tracing::error!("Postgres connection error: {err}");
                }
            });

            loop {
                let query = format!(
                    "SELECT data FROM pg_logical_slot_get_changes('{}', NULL, 100, \
                     'proto_version', '1', 'publication_names', '{}')",
                    slot, publication
                );

                match client.query(&query, &[]).await {
                    Ok(rows) => {
                        for row in rows {
                            let data: &str = row.get(0);
                            let events = if let Ok(v) = serde_json::from_str::<serde_json::Value>(data) {
                                json_wal_to_events(&v)
                            } else if let Ok(event) = crate::streaming::parse_pgoutput_message(data.as_bytes()) {
                                vec![event]
                            } else {
                                vec![]
                            };
                            for event in events {
                                let _ = tx.send(Ok(event));
                            }
                        }
                    }
                    Err(err) => {
                        let _ = tx.send(Err(anyhow::anyhow!(err)));
                    }
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        let stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });

        Ok(Box::pin(stream))
    }

    pub async fn acknowledge_lsn(&self, lsn: u64) -> anyhow::Result<()> {
        let _ = lsn;
        Ok(())
    }
}

fn json_wal_to_events(v: &serde_json::Value) -> Vec<WalEvent> {
    let op = v["action"].as_str().unwrap_or("I");
    match op {
        "I" => vec![WalEvent::Insert {
            relation: "unknown".into(),
            row: crate::json_to_row(&v["columns"]),
        }],
        "D" => vec![WalEvent::Delete {
            relation: "unknown".into(),
            row: crate::json_to_row(&v["identity"]),
        }],
        "U" => vec![WalEvent::Update {
            relation: "unknown".into(),
            old_row: crate::json_to_row(&v["identity"]),
            new_row: crate::json_to_row(&v["columns"]),
        }],
        _ => vec![],
    }
}
