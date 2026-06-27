use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use ivm_runtime::{set_pipelines_running, PipelineConfig};
use serde::{Deserialize, Serialize};

use crate::state::{AppState, PipelineEntry, PipelineStatus};

#[derive(Debug, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub name: String,
    pub source: ivm_runtime::SourceKind,
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub operators: Vec<ivm_runtime::OperatorKind>,
    pub checkpoint_interval_secs: u64,
}

impl From<PipelineSpec> for PipelineConfig {
    fn from(spec: PipelineSpec) -> Self {
        PipelineConfig {
            name: spec.name,
            source: spec.source,
            sql: spec.sql,
            operators: spec.operators,
            checkpoint_interval_epochs: spec.checkpoint_interval_secs.max(1),
        }
    }
}

pub async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

pub async fn create_pipeline(
    State(state): State<AppState>,
    Json(spec): Json<PipelineSpec>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut map = state.pipelines.write().await;
    if map.contains_key(&spec.name) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "pipeline already exists"})),
        );
    }
    let config: PipelineConfig = spec.into();
    let name = config.name.clone();
    map.insert(
        name.clone(),
        PipelineEntry::new(config, state.checkpoint_dir.clone()),
    );
    (
        StatusCode::CREATED,
        Json(serde_json::json!({"name": name, "status": "created"})),
    )
}

pub async fn list_pipelines(State(state): State<AppState>) -> Json<serde_json::Value> {
    let map = state.pipelines.read().await;
    let out: Vec<_> = map
        .iter()
        .map(|(k, v)| {
            serde_json::json!({
                "name": k,
                "status": status_str(&v.status),
                "source": v.config.source,
                "operators": v.config.operators,
            })
        })
        .collect();
    Json(serde_json::json!(out))
}

pub async fn get_pipeline(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let map = state.pipelines.read().await;
    let entry = map.get(&name).ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(serde_json::json!({
        "name": name,
        "status": status_str(&entry.status),
        "config": entry.config,
    })))
}

pub async fn delete_pipeline(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> StatusCode {
    let mut map = state.pipelines.write().await;
    if let Some(entry) = map.get(&name) {
        if entry.status == PipelineStatus::Running {
            return StatusCode::CONFLICT;
        }
    }
    if map.remove(&name).is_some() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

pub async fn start_pipeline(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> StatusCode {
    let mut map = state.pipelines.write().await;
    let Some(entry) = map.get_mut(&name) else {
        return StatusCode::NOT_FOUND;
    };
    if entry.status == PipelineStatus::Running {
        return StatusCode::OK;
    }
    let scheduler = entry.scheduler.clone().unwrap();
    entry.status = PipelineStatus::Running;
    update_running_gauge(&map);
    drop(map);
    tokio::spawn(async move {
        if let Err(e) = scheduler.start().await {
            tracing::error!("Pipeline failed: {e}");
        }
    });
    StatusCode::OK
}

pub async fn stop_pipeline(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> StatusCode {
    let mut map = state.pipelines.write().await;
    let Some(entry) = map.get_mut(&name) else {
        return StatusCode::NOT_FOUND;
    };
    if let Some(scheduler) = &entry.scheduler {
        scheduler.stop().await;
    }
    entry.status = PipelineStatus::Stopped;
    update_running_gauge(&map);
    StatusCode::OK
}

fn update_running_gauge(map: &std::collections::HashMap<String, PipelineEntry>) {
    let running = map
        .values()
        .filter(|e| e.status == PipelineStatus::Running)
        .count() as i64;
    set_pipelines_running(running);
}

fn status_str(status: &PipelineStatus) -> &'static str {
    match status {
        PipelineStatus::Stopped => "stopped",
        PipelineStatus::Running => "running",
        PipelineStatus::Failed(_) => "failed",
    }
}
