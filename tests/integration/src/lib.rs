//! End-to-end tests for exactly-once semantics via checkpoint + replay.

use std::collections::HashMap;

use ivm_core::{Batch, Row, Value, ZSet};
use ivm_kafka_cdc::apply_cdc_payload;
use ivm_operators::{filter, AggregateState};
use ivm_parquet::{read_zset_checkpoint, write_zset_checkpoint};
use ivm_runtime::CheckpointManager;

#[test]
fn cdc_to_aggregate_pipeline() {
    let messages = [
        br#"{"op":"c","after":{"word":"hello"}}"#.as_slice(),
        br#"{"op":"c","after":{"word":"world"}}"#.as_slice(),
        br#"{"op":"c","after":{"word":"hello"}}"#.as_slice(),
    ];

    let mut delta = ZSet::new();
    for msg in messages {
        apply_cdc_payload(msg, &mut delta).unwrap();
    }

    let batch = Batch::new(100, delta);
    let filtered = filter(batch, |r| r.get_str("word").is_some());

    let mut agg = AggregateState::count("word");
    agg.apply_delta(&filtered.delta);

    assert_eq!(agg.result[&Value::Str("hello".into())], 2);
    assert_eq!(agg.result[&Value::Str("world".into())], 1);
}

#[test]
fn checkpoint_survives_crash_and_restore() {
    let mut agg = AggregateState::count("word");
    let mut delta = ZSet::new();
    delta.insert(
        Row(HashMap::from([("word".into(), Value::Str("hello".into()))])),
        1,
    );
    agg.apply_delta(&delta);

    let dir = tempfile::tempdir().unwrap();
    write_zset_checkpoint(dir.path(), &agg.accumulated, 42).unwrap();

    let (restored, epoch) =
        read_zset_checkpoint(&dir.path().join("checkpoint_epoch_42.parquet")).unwrap();
    assert_eq!(epoch, 42);
    assert_eq!(restored.len(), 1);
}

#[test]
fn checkpoint_manager_interval() {
    let dir = tempfile::tempdir().unwrap();
    let mut mgr = CheckpointManager::new(dir.path());
    assert!(!mgr.should_checkpoint(50, 100));
    mgr.save(&ZSet::new(), 100).unwrap();
    assert!(mgr.should_checkpoint(200, 100));
}

#[test]
fn update_produces_correct_zset_delta() {
    let insert = br#"{"op":"c","after":{"word":"foo"}}"#;
    let update = br#"{"op":"u","before":{"word":"foo"},"after":{"word":"bar"}}"#;

    let mut delta = ZSet::new();
    apply_cdc_payload(insert, &mut delta).unwrap();
    apply_cdc_payload(update, &mut delta).unwrap();

    let mut agg = AggregateState::count("word");
    agg.apply_delta(&delta);
    assert_eq!(agg.result.get(&Value::Str("foo".into())), None);
    assert_eq!(agg.result[&Value::Str("bar".into())], 1);
}

#[test]
fn delete_removes_from_aggregate() {
    let insert = br#"{"op":"c","after":{"word":"temp"}}"#;
    let delete = br#"{"op":"d","before":{"word":"temp"}}"#;

    let mut delta = ZSet::new();
    apply_cdc_payload(insert, &mut delta).unwrap();
    apply_cdc_payload(delete, &mut delta).unwrap();

    let mut agg = AggregateState::count("word");
    agg.apply_delta(&delta);
    assert!(agg.result.is_empty());
}
