use std::path::{Path, PathBuf};

use anyhow::Context;
use ivm_core::{Row, ZSet};
use ivm_parquet::{latest_checkpoint, read_zset_checkpoint, write_zset_checkpoint};
use tracing::info;

use crate::metrics::CHECKPOINT_DURATION;

pub struct CheckpointManager {
    dir: PathBuf,
    last_epoch: u64,
}

impl CheckpointManager {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            last_epoch: 0,
        }
    }

    pub fn last_epoch(&self) -> u64 {
        self.last_epoch
    }

    pub fn checkpoint_dir(&self) -> &Path {
        &self.dir
    }

    pub fn restore(&mut self) -> anyhow::Result<Option<ZSet<Row>>> {
        if let Some(path) = latest_checkpoint(&self.dir)? {
            let (zset, epoch) = read_zset_checkpoint(&path)?;
            self.last_epoch = epoch;
            info!(epoch, path = %path.display(), "Restored checkpoint");
            Ok(Some(zset))
        } else {
            Ok(None)
        }
    }

    pub fn save(&mut self, zset: &ZSet<Row>, epoch: u64) -> anyhow::Result<PathBuf> {
        let timer = CHECKPOINT_DURATION.start_timer();
        let path = write_zset_checkpoint(&self.dir, zset, epoch)
            .context("Failed to write checkpoint")?;
        timer.observe_duration();
        self.last_epoch = epoch;
        info!(epoch, path = %path.display(), "Wrote checkpoint");
        Ok(path)
    }

    pub fn should_checkpoint(&self, epoch: u64, interval_epochs: u64) -> bool {
        interval_epochs > 0 && epoch.saturating_sub(self.last_epoch) >= interval_epochs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ivm_core::{Value, ZSet};
    use std::collections::HashMap;

    #[test]
    fn save_and_restore() {
        let dir = std::env::temp_dir().join("ivm_checkpoint_mgr_test");
        let _ = std::fs::remove_dir_all(&dir);

        let mut mgr = CheckpointManager::new(&dir);
        let mut zset = ZSet::new();
        zset.insert(
            Row(HashMap::from([("k".into(), Value::Str("v".into()))])),
            1,
        );
        mgr.save(&zset, 10).unwrap();

        let mut mgr2 = CheckpointManager::new(&dir);
        let restored = mgr2.restore().unwrap().unwrap();
        assert_eq!(restored.len(), 1);
        assert_eq!(mgr2.last_epoch(), 10);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
