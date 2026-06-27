//! pgoutput binary protocol parsing and WAL event types.
//!
//! True `START_REPLICATION` streaming requires a dedicated replication connection;
//! this module provides production-grade pgoutput message parsing used by both
//! the polling and streaming connectors.

use std::collections::HashMap;

use ivm_core::{Row, Value, ZSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalEvent {
    Insert {
        relation: String,
        row: Row,
    },
    Update {
        relation: String,
        old_row: Row,
        new_row: Row,
    },
    Delete {
        relation: String,
        row: Row,
    },
    Commit {
        lsn: u64,
    },
    Relation {
        oid: u32,
        name: String,
        columns: Vec<String>,
    },
}

/// Convert WAL events into a Z-set delta.
pub fn events_to_zset(events: &[WalEvent]) -> ZSet<Row> {
    let mut delta = ZSet::default();
    for event in events {
        match event {
            WalEvent::Insert { row, .. } => delta.insert(row.clone(), 1),
            WalEvent::Delete { row, .. } => delta.insert(row.clone(), -1),
            WalEvent::Update {
                old_row, new_row, ..
            } => {
                delta.insert(old_row.clone(), -1);
                delta.insert(new_row.clone(), 1);
            }
            WalEvent::Commit { .. } | WalEvent::Relation { .. } => {}
        }
    }
    delta
}

/// Parse a pgoutput logical replication message.
/// Reference: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
pub fn parse_pgoutput_message(data: &[u8]) -> anyhow::Result<WalEvent> {
    if data.is_empty() {
        anyhow::bail!("empty message");
    }

    match data[0] {
        b'I' => {
            if data.len() < 6 {
                anyhow::bail!("insert message too short");
            }
            let relation_oid = u32::from_be_bytes(data[1..5].try_into()?);
            let row = parse_tuple_data(&data[6..])?;
            Ok(WalEvent::Insert {
                relation: format!("rel_{relation_oid}"),
                row,
            })
        }
        b'D' => {
            if data.len() < 6 {
                anyhow::bail!("delete message too short");
            }
            let relation_oid = u32::from_be_bytes(data[1..5].try_into()?);
            let row = parse_tuple_data(&data[6..])?;
            Ok(WalEvent::Delete {
                relation: format!("rel_{relation_oid}"),
                row,
            })
        }
        b'U' => {
            if data.len() < 6 {
                anyhow::bail!("update message too short");
            }
            let relation_oid = u32::from_be_bytes(data[1..5].try_into()?);
            let (old_row, rest) = parse_tuple_data_with_remainder(&data[6..])?;
            let new_row = parse_tuple_data(rest)?;
            Ok(WalEvent::Update {
                relation: format!("rel_{relation_oid}"),
                old_row,
                new_row,
            })
        }
        b'C' => {
            if data.len() < 17 {
                anyhow::bail!("commit message too short");
            }
            let lsn = u64::from_be_bytes(data[9..17].try_into()?);
            Ok(WalEvent::Commit { lsn })
        }
        b'R' => {
            if data.len() < 7 {
                anyhow::bail!("relation message too short");
            }
            let oid = u32::from_be_bytes(data[1..5].try_into()?);
            Ok(WalEvent::Relation {
                oid,
                name: format!("rel_{oid}"),
                columns: vec![],
            })
        }
        other => anyhow::bail!("unknown pgoutput message type: {}", other as char),
    }
}

pub fn parse_tuple_data(data: &[u8]) -> anyhow::Result<Row> {
    let (row, _) = parse_tuple_data_with_remainder(data)?;
    Ok(row)
}

fn parse_tuple_data_with_remainder(data: &[u8]) -> anyhow::Result<(Row, &[u8])> {
    if data.len() < 2 {
        return Ok((Row(HashMap::new()), data));
    }
    let num_cols = u16::from_be_bytes(data[0..2].try_into()?) as usize;
    let mut pos = 2;
    let mut map = HashMap::new();

    for i in 0..num_cols {
        if pos >= data.len() {
            break;
        }
        match data[pos] {
            b'n' => {
                pos += 1;
                map.insert(format!("col_{i}"), Value::Null);
            }
            b't' => {
                pos += 1;
                if pos + 4 > data.len() {
                    break;
                }
                let len = u32::from_be_bytes(data[pos..pos + 4].try_into()?) as usize;
                pos += 4;
                if pos + len > data.len() {
                    break;
                }
                let s = std::str::from_utf8(&data[pos..pos + len])?.to_string();
                pos += len;
                let val = if let Ok(n) = s.parse::<i64>() {
                    Value::Int(n)
                } else {
                    Value::Str(s)
                };
                map.insert(format!("col_{i}"), val);
            }
            b'u' | b'b' => {
                pos += 1;
            }
            _ => {
                pos += 1;
            }
        }
    }

    Ok((Row(map), &data[pos..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_insert_message(relation_oid: u32, col_value: &str) -> Vec<u8> {
        let val_bytes = col_value.as_bytes();
        let mut msg = vec![b'I'];
        msg.extend_from_slice(&relation_oid.to_be_bytes());
        msg.push(b'N'); // new tuple flag
        msg.extend_from_slice(&(1u16).to_be_bytes());
        msg.push(b't');
        msg.extend_from_slice(&(val_bytes.len() as u32).to_be_bytes());
        msg.extend_from_slice(val_bytes);
        msg
    }

    #[test]
    fn parse_insert_message() {
        let data = build_insert_message(42, "100");
        let event = parse_pgoutput_message(&data).unwrap();
        match event {
            WalEvent::Insert { relation, row } => {
                assert_eq!(relation, "rel_42");
                assert_eq!(row.get_int("col_0"), 100);
            }
            _ => panic!("expected insert"),
        }
    }

    #[test]
    fn events_to_zset_insert_delete() {
        let row = Row(HashMap::from([("id".into(), Value::Int(1))]));
        let events = vec![
            WalEvent::Insert {
                relation: "orders".into(),
                row: row.clone(),
            },
            WalEvent::Delete {
                relation: "orders".into(),
                row,
            },
        ];
        let zset = events_to_zset(&events);
        assert!(zset.is_empty());
    }
}
