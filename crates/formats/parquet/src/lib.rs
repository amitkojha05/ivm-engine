use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use ivm_connectors::ConnectorState;
use ivm_core::{Row, Value, ZSet};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub fn write_zset_checkpoint(path: &Path, zset: &ZSet<Row>, epoch: u64) -> anyhow::Result<PathBuf> {
    std::fs::create_dir_all(path).context("Failed to create checkpoint directory")?;

    let file_path = path.join(format!("checkpoint_epoch_{epoch}.parquet"));
    write_zset_checkpoint_to_path(&file_path, zset, epoch, None)?;
    Ok(file_path)
}

/// Write a checkpoint to an explicit file path (used for two-phase `.tmp` writes).
pub fn write_zset_checkpoint_to_path(
    file_path: &Path,
    zset: &ZSet<Row>,
    epoch: u64,
    connector_state: Option<&ConnectorState>,
) -> anyhow::Result<PathBuf> {
    if let Some(parent) = file_path.parent() {
        std::fs::create_dir_all(parent).context("Failed to create checkpoint directory")?;
    }

    let mut keys: Vec<String> = Vec::new();
    let mut weights: Vec<i64> = Vec::new();

    for (row, weight) in &zset.inner {
        keys.push(serde_json::to_string(&row.0)?);
        weights.push(*weight);
    }

    let row_count = keys.len().max(1);
    let schema = Arc::new(Schema::new(vec![
        Field::new("epoch", DataType::Int64, false),
        Field::new("row_json", DataType::Utf8, false),
        Field::new("weight", DataType::Int64, false),
    ]));

    let mut props_builder = WriterProperties::builder().set_compression(Compression::SNAPPY);
    if let Some(state) = connector_state {
        let state_json = serde_json::to_string(state)?;
        props_builder = props_builder.set_key_value_metadata(Some(vec![
            parquet::file::metadata::KeyValue::new(
                "connector_state".to_string(),
                Some(state_json),
            ),
            parquet::file::metadata::KeyValue::new(
                "ivm_version".to_string(),
                Some("1".to_string()),
            ),
        ]));
    }
    let props = props_builder.build();

    let file = File::create(file_path).context("Failed to create parquet checkpoint file")?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .context("Failed to create parquet checkpoint writer")?;

    let epoch_values: Vec<i64> = if keys.is_empty() {
        vec![epoch as i64]
    } else {
        vec![epoch as i64; row_count]
    };
    let row_values: Vec<String> = if keys.is_empty() {
        vec!["".into()]
    } else {
        keys
    };
    let weight_values: Vec<i64> = if weights.is_empty() {
        vec![0i64]
    } else {
        weights
    };

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(epoch_values)),
            Arc::new(StringArray::from(row_values)),
            Arc::new(Int64Array::from(weight_values)),
        ],
    )?;

    writer.write(&batch)?;
    writer.close()?;

    Ok(file_path.to_path_buf())
}

/// Write checkpoint with connector state embedded in Parquet metadata.
pub fn write_checkpoint_with_state(
    path: &Path,
    zset: &ZSet<Row>,
    epoch: u64,
    state: &ConnectorState,
) -> anyhow::Result<PathBuf> {
    std::fs::create_dir_all(path).context("Failed to create checkpoint directory")?;
    let file_path = path.join(format!("checkpoint_epoch_{epoch}.parquet"));
    write_zset_checkpoint_to_path(&file_path, zset, epoch, Some(state))?;
    Ok(file_path)
}

pub fn read_connector_state(path: &Path) -> anyhow::Result<Option<ConnectorState>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let meta = builder.metadata().file_metadata();
    if let Some(kv_list) = meta.key_value_metadata() {
        for kv in kv_list {
            if kv.key == "connector_state" {
                if let Some(ref val) = kv.value {
                    let state: ConnectorState = serde_json::from_str(val)?;
                    return Ok(Some(state));
                }
            }
        }
    }
    Ok(None)
}

pub fn read_zset_checkpoint(path: &Path) -> anyhow::Result<(ZSet<Row>, u64)> {
    let file = File::open(path).context("Failed to open checkpoint file")?;
    let mut batch_reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("Failed to build parquet record batch reader")?
        .build()
        .context("Failed to build parquet record batch reader")?;

    let mut zset = ZSet::default();
    let mut epoch = 0u64;

    while let Some(batch) = batch_reader.next() {
        let batch = batch.context("Failed to read parquet batch")?;
        let epoch_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("Expected epoch column")?;
        let row_arr = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("Expected row_json column")?;
        let weight_arr = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("Expected weight column")?;

        for i in 0..batch.num_rows() {
            if epoch_arr.is_valid(i) {
                epoch = epoch_arr.value(i) as u64;
            }
            if row_arr.is_valid(i) && weight_arr.is_valid(i) {
                let row_json = row_arr.value(i);
                if row_json.is_empty() {
                    continue;
                }
                let weight = weight_arr.value(i);
                let fields: HashMap<String, Value> = serde_json::from_str(row_json)?;
                zset.insert(Row(fields), weight);
            }
        }
    }

    Ok((zset, epoch))
}

pub fn latest_checkpoint(checkpoint_dir: &Path) -> anyhow::Result<Option<PathBuf>> {
    if !checkpoint_dir.exists() {
        return Ok(None);
    }
    let mut best: Option<(u64, PathBuf)> = None;
    for entry in std::fs::read_dir(checkpoint_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(epoch_str) = name
            .strip_prefix("checkpoint_epoch_")
            .and_then(|s| s.strip_suffix(".parquet"))
        {
            if let Ok(epoch) = epoch_str.parse::<u64>() {
                if best.as_ref().map(|(e, _)| epoch > *e).unwrap_or(true) {
                    best = Some((epoch, entry.path()));
                }
            }
        }
    }
    Ok(best.map(|(_, p)| p))
}

/// Restore the most recent checkpoint from a directory.
pub fn restore_zset_checkpoint(dir: impl AsRef<Path>) -> anyhow::Result<(ZSet<Row>, u64)> {
    let dir = dir.as_ref();
    let path = latest_checkpoint(dir)?.ok_or_else(|| {
        anyhow::anyhow!("no checkpoint files found in {}", dir.display())
    })?;
    read_zset_checkpoint(&path)
}

/// Read rows from an arbitrary Parquet data file (Delta Lake data files, etc.).
pub async fn read_arbitrary_parquet_as_rows(path: &Path) -> anyhow::Result<Vec<Row>> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || read_parquet_rows_sync(&path))
        .await
        .context("Parquet read task failed")?
}

fn read_parquet_rows_sync(path: &Path) -> anyhow::Result<Vec<Row>> {
    if !path.exists() {
        return Ok(vec![]);
    }
    let file = File::open(path).context("Failed to open parquet file")?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .build()
        .context("Failed to build parquet reader")?;

    let mut rows = Vec::new();
    while let Some(batch) = reader.next() {
        let batch = batch.context("Failed to read parquet batch")?;
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            let mut map = HashMap::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let value = if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    if arr.is_valid(row_idx) {
                        Value::Str(arr.value(row_idx).to_string())
                    } else {
                        Value::Null
                    }
                } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    if arr.is_valid(row_idx) {
                        Value::Int(arr.value(row_idx))
                    } else {
                        Value::Null
                    }
                } else {
                    Value::Null
                };
                map.insert(field.name().clone(), value);
            }
            rows.push(Row(map));
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ivm_core::ZSet;
    use std::collections::HashMap;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("ivm_parquet_{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn checkpoint_roundtrip_single_row() {
        let dir = temp_dir("single_row");

        let mut zset = ZSet::new();
        zset.insert(
            Row(HashMap::from([("id".into(), Value::Int(1))])),
            1,
        );

        let path = write_zset_checkpoint(&dir, &zset, 99).unwrap();
        let (restored, epoch) = read_zset_checkpoint(&path).unwrap();
        assert_eq!(epoch, 99);
        assert_eq!(restored.len(), 1);
        let row = restored.inner.keys().next().unwrap();
        assert_eq!(row.get_int("id"), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn checkpoint_roundtrip_multiple_rows() {
        let dir = temp_dir("multiple_rows");
        let mut zset = ZSet::new();
        zset.insert(Row(HashMap::from([("id".into(), Value::Int(1))])), 1);
        zset.insert(Row(HashMap::from([("id".into(), Value::Int(2))])), 1);

        let path = write_zset_checkpoint(&dir, &zset, 42).unwrap();
        let (restored, epoch) = read_zset_checkpoint(&path).unwrap();
        assert_eq!(epoch, 42);
        assert_eq!(restored.len(), 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn checkpoint_roundtrip_empty_zset() {
        let dir = temp_dir("empty_zset");
        let zset = ZSet::new();

        let path = write_zset_checkpoint(&dir, &zset, 7).unwrap();
        let (restored, epoch) = read_zset_checkpoint(&path).unwrap();
        assert_eq!(epoch, 7);
        assert!(restored.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn latest_checkpoint_selects_highest_epoch() {
        let dir = temp_dir("latest_epoch");
        let _ = write_zset_checkpoint(&dir, &ZSet::new(), 1).unwrap();
        let _ = write_zset_checkpoint(&dir, &ZSet::new(), 9).unwrap();
        let latest = latest_checkpoint(&dir).unwrap().unwrap();
        assert!(latest.ends_with("checkpoint_epoch_9.parquet"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn checkpoint_roundtrip_negative_weights() {
        let dir = temp_dir("negative_weights");
        let mut zset = ZSet::new();
        zset.insert(Row(HashMap::from([("id".into(), Value::Int(1))])), -1);

        let path = write_zset_checkpoint(&dir, &zset, 3).unwrap();
        let (restored, epoch) = read_zset_checkpoint(&path).unwrap();
        assert_eq!(epoch, 3);
        let restored_weight = restored.inner.values().next().copied().unwrap();
        assert_eq!(restored_weight, -1);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
