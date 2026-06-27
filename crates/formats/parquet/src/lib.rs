use std::collections::HashMap;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::Context;
use arrow2::array::{Array, Int64Array, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::parquet::read::{infer_schema, read_metadata, FileReader};
use arrow2::io::parquet::write::{
    transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version, WriteOptions,
};
use ivm_core::{Row, Value, ZSet};

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
    let schema = Schema::from(vec![
        Field::new("epoch", DataType::Int64, false),
        Field::new("row_json", DataType::Utf8, false),
        Field::new("weight", DataType::Int64, false),
    ]);

    let epoch_arr = if keys.is_empty() {
        Int64Array::from_vec(vec![epoch as i64])
    } else {
        Int64Array::from_vec(vec![epoch as i64; row_count])
    };
    let row_arr = if keys.is_empty() {
        Utf8Array::<i32>::from_iter_values(std::iter::once(""))
    } else {
        Utf8Array::<i32>::from_iter_values(keys.iter().map(String::as_str))
    };
    let weight_arr = if weights.is_empty() {
        Int64Array::from_vec(vec![0i64])
    } else {
        Int64Array::from_vec(weights)
    };

    let chunk = Chunk::new(vec![
        epoch_arr.boxed(),
        row_arr.boxed(),
        weight_arr.boxed(),
    ]);

    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Snappy,
        version: Version::V2,
        data_pagesize_limit: None,
    };

    let encodings: Vec<Vec<Encoding>> = schema
        .fields
        .iter()
        .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
        .collect();

    let file = File::create(&file_path)?;
    let mut writer = FileWriter::try_new(file, schema.clone(), options)?;
    let mut row_groups = RowGroupIterator::try_new(
        vec![Ok(chunk)].into_iter(),
        &schema,
        options,
        encodings,
    )?;

    for group in &mut row_groups {
        writer.write(group?)?;
    }
    writer.end(None)?;

    Ok(file_path)
}

pub fn read_zset_checkpoint(path: &Path) -> anyhow::Result<(ZSet<Row>, u64)> {
    let mut file = File::open(path).context("Failed to open checkpoint file")?;
    let metadata = read_metadata(&mut file).context("Failed to read parquet metadata")?;
    let schema = infer_schema(&metadata).context("Failed to infer schema")?;
    let row_groups = metadata.row_groups.clone();

    file.seek(SeekFrom::Start(0))?;

    let reader = FileReader::new(file, row_groups, schema, None, None, None);
    let mut zset = ZSet::default();
    let mut epoch = 0u64;

    for maybe_chunk in reader {
        let chunk = maybe_chunk.context("Failed to read parquet chunk")?;
        if chunk.len() == 0 {
            continue;
        }

        let epoch_arr = chunk.arrays()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("Expected epoch column")?;
        let row_arr = chunk.arrays()[1]
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .context("Expected row_json column")?;
        let weight_arr = chunk.arrays()[2]
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("Expected weight column")?;

        for i in 0..chunk.len() {
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

    #[test]
    fn checkpoint_roundtrip() {
        let dir = std::env::temp_dir().join("ivm_parquet_test");
        let _ = std::fs::remove_dir_all(&dir);

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
}
