use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use ivm_core::{Row, Value, ZSet};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub fn write_zset_checkpoint(path: &Path, zset: &ZSet<Row>, epoch: u64) -> anyhow::Result<PathBuf> {
    std::fs::create_dir_all(path).context("Failed to create checkpoint directory")?;

    let file_path = path.join(format!("checkpoint_epoch_{epoch}.parquet"));
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

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let file = File::create(&file_path).context("Failed to create parquet checkpoint file")?;
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

    Ok(file_path)
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
