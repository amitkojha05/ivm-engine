use std::path::{Path, PathBuf};

use anyhow::Context;
use ivm_connectors::ConnectorState;
use ivm_core::{Row, ZSet};
use ivm_parquet::{
    latest_checkpoint, read_connector_state, read_zset_checkpoint, write_zset_checkpoint_to_path,
};
use tracing::info;

use crate::metrics::CHECKPOINT_DURATION;

pub struct CheckpointManager {
    dir: PathBuf,
    last_epoch: u64,
    connector_state: Option<ConnectorState>,
}

impl CheckpointManager {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            last_epoch: 0,
            connector_state: None,
        }
    }

    pub fn last_epoch(&self) -> u64 {
        self.last_epoch
    }

    pub fn connector_state(&self) -> Option<&ConnectorState> {
        self.connector_state.as_ref()
    }

    pub fn checkpoint_dir(&self) -> &Path {
        &self.dir
    }

    pub fn restore(&mut self) -> anyhow::Result<Option<ZSet<Row>>> {
        if let Some(path) = latest_checkpoint(&self.dir)? {
            let (zset, epoch) = read_zset_checkpoint(&path)?;
            self.last_epoch = epoch;
            self.connector_state = read_connector_state(&path)?;
            info!(epoch, path = %path.display(), "Restored checkpoint");
            Ok(Some(zset))
        } else {
            Ok(None)
        }
    }

    pub fn save(&mut self, zset: &ZSet<Row>, epoch: u64) -> anyhow::Result<PathBuf> {
        let timer = CHECKPOINT_DURATION.start_timer();
        let path = self
            .dir
            .join(format!("checkpoint_epoch_{epoch}.parquet"));
        write_zset_checkpoint_to_path(&path, zset, epoch, self.connector_state.as_ref())
            .context("Failed to write checkpoint")?;
        timer.observe_duration();
        self.last_epoch = epoch;
        info!(epoch, path = %path.display(), "Wrote checkpoint");
        Ok(path)
    }

    /// Phase 1: write checkpoint to a `.tmp` file.
    /// Does NOT update last_epoch — this is not yet confirmed.
    pub fn save_tmp(
        &self,
        zset: &ZSet<Row>,
        epoch: u64,
        connector_state: &ConnectorState,
    ) -> anyhow::Result<PathBuf> {
        let tmp = self
            .dir
            .join(format!("checkpoint_epoch_{epoch}.parquet.tmp"));
        write_zset_checkpoint_to_path(&tmp, zset, epoch, Some(connector_state))
            .context("Failed to write tmp checkpoint")?;
        tracing::debug!(epoch, "Phase 1: tmp checkpoint written");
        Ok(tmp)
    }

    /// Phase 2: atomically promote the `.tmp` file to final.
    /// Call this ONLY after the source has confirmed the epoch (Kafka commit / LSN advance).
    pub fn confirm(&mut self, tmp_path: PathBuf, epoch: u64, connector_state: ConnectorState) -> anyhow::Result<()> {
        let final_path = self
            .dir
            .join(format!("checkpoint_epoch_{epoch}.parquet"));
        std::fs::rename(&tmp_path, &final_path).context("Atomic checkpoint rename failed")?;
        self.last_epoch = epoch;
        self.connector_state = Some(connector_state);
        tracing::info!(epoch, "Phase 2: checkpoint confirmed");
        Ok(())
    }

    /// On startup: delete any orphaned `.tmp` files (uncommitted checkpoints).
    pub fn cleanup_uncommitted(&self) -> anyhow::Result<()> {
        if !self.dir.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".parquet.tmp") {
                std::fs::remove_file(entry.path())
                    .context("Failed to remove uncommitted checkpoint")?;
                tracing::warn!(file = %name, "Removed uncommitted checkpoint from previous run");
            }
        }
        Ok(())
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

    #[test]
    fn two_phase_checkpoint() {
        let dir = std::env::temp_dir().join("ivm_checkpoint_two_phase_test");
        let _ = std::fs::remove_dir_all(&dir);

        let mut mgr = CheckpointManager::new(&dir);
        let mut zset = ZSet::new();
        zset.insert(
            Row(HashMap::from([("k".into(), Value::Str("v".into()))])),
            1,
        );
        let state = ConnectorState {
            checkpoint_epoch: 5,
            ..Default::default()
        };
        let tmp = mgr.save_tmp(&zset, 5, &state).unwrap();
        assert!(tmp.exists());

        mgr.confirm(tmp, 5, state).unwrap();
        assert_eq!(mgr.last_epoch(), 5);
        assert!(mgr.connector_state().is_some());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cleanup_uncommitted_removes_tmp_files() {
        let dir = std::env::temp_dir().join("ivm_checkpoint_cleanup_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let tmp = dir.join("checkpoint_epoch_3.parquet.tmp");
        std::fs::write(&tmp, b"orphan").unwrap();

        let mgr = CheckpointManager::new(&dir);
        mgr.cleanup_uncommitted().unwrap();
        assert!(!tmp.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
