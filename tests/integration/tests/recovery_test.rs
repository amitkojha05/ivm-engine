use ivm_core::{Row, Value, ZSet};
use ivm_parquet::{restore_zset_checkpoint, write_zset_checkpoint};
use std::collections::HashMap;
use tempfile::TempDir;

/// Simulates: ingest data → checkpoint → "crash" → restore → verify state.
#[test]
fn test_crash_and_recovery() {
    let tmp = TempDir::new().unwrap();
    let checkpoint_dir = tmp.path();

    let mut delta = ZSet::default();
    delta.insert(make_row("customer_id", 1, "amount", 500), 1);
    delta.insert(make_row("customer_id", 2, "amount", 250), 1);
    delta.insert(make_row("customer_id", 3, "amount", 750), 1);

    write_zset_checkpoint(checkpoint_dir, &delta, 42).expect("checkpoint write failed");

    let (restored_zset, restored_epoch) =
        restore_zset_checkpoint(checkpoint_dir).expect("checkpoint restore failed");

    assert_eq!(restored_epoch, 42, "epoch must be recovered");
    assert_eq!(restored_zset.inner.len(), 3, "all 3 rows must be recovered");

    let mut new_delta = ZSet::default();
    new_delta.insert(make_row("customer_id", 4, "amount", 100), 1);
    new_delta.insert(make_row("customer_id", 1, "amount", 500), -1);

    let mut merged = restored_zset;
    merged.merge(new_delta);

    assert_eq!(merged.inner.len(), 3);
}

fn make_row(k1: &str, v1: i64, k2: &str, v2: i64) -> Row {
    Row(HashMap::from([
        (k1.to_string(), Value::Int(v1)),
        (k2.to_string(), Value::Int(v2)),
    ]))
}
