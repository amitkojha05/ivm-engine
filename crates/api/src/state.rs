use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use ivm_runtime::{Pipeline, PipelineConfig, PipelineScheduler};
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineStatus {
    Stopped,
    Running,
    Failed(String),
}

pub struct PipelineEntry {
    pub config: PipelineConfig,
    pub status: PipelineStatus,
    pub scheduler: Option<Arc<PipelineScheduler>>,
}

#[derive(Clone)]
pub struct AppState {
    pub pipelines: Arc<RwLock<HashMap<String, PipelineEntry>>>,
    pub checkpoint_dir: PathBuf,
}

impl AppState {
    pub fn new(checkpoint_dir: PathBuf) -> Self {
        Self {
            pipelines: Arc::new(RwLock::new(HashMap::new())),
            checkpoint_dir,
        }
    }
}

impl PipelineEntry {
    pub fn new(config: PipelineConfig, checkpoint_dir: PathBuf) -> Self {
        let scheduler = Arc::new(PipelineScheduler::new(
            Pipeline::new(config.clone()),
            checkpoint_dir.join(&config.name),
        ));
        Self {
            config,
            status: PipelineStatus::Stopped,
            scheduler: Some(scheduler),
        }
    }
}
