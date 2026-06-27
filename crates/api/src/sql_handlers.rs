use std::collections::HashMap;

use axum::{extract::State, http::StatusCode, Json};
use ivm_core::{Batch, Row, Value, ZSet};
use ivm_operators::AggregateState;
use ivm_planner::{display_plan, execute, sql_to_plan};
use serde::{Deserialize, Serialize};

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct SqlPlanRequest {
    pub sql: String,
}

#[derive(Debug, Deserialize)]
pub struct SqlExecuteRequest {
    pub sql: String,
    pub sources: HashMap<String, SourceBatch>,
}

#[derive(Debug, Deserialize)]
pub struct SourceBatch {
    pub epoch: u64,
    pub rows: Vec<SourceRow>,
}

#[derive(Debug, Deserialize)]
pub struct SourceRow {
    pub fields: HashMap<String, JsonValue>,
    pub weight: i64,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum JsonValue {
    Int(i64),
    Str(String),
    Bool(bool),
    Null,
}

impl From<JsonValue> for Value {
    fn from(v: JsonValue) -> Self {
        match v {
            JsonValue::Int(n) => Value::Int(n),
            JsonValue::Str(s) => Value::Str(s),
            JsonValue::Bool(b) => Value::Bool(b),
            JsonValue::Null => Value::Null,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct SqlPlanResponse {
    pub plan: String,
}

#[derive(Debug, Serialize)]
pub struct SqlExecuteResponse {
    pub epoch: u64,
    pub output_rows: usize,
    pub rows: Vec<HashMap<String, Value>>,
}

pub async fn sql_plan(
    Json(req): Json<SqlPlanRequest>,
) -> Result<Json<SqlPlanResponse>, (StatusCode, Json<serde_json::Value>)> {
    match sql_to_plan(&req.sql) {
        Ok(plan) => Ok(Json(SqlPlanResponse {
            plan: display_plan(&plan, 0),
        })),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        )),
    }
}

pub async fn sql_execute(
    State(_state): State<AppState>,
    Json(req): Json<SqlExecuteRequest>,
) -> Result<Json<SqlExecuteResponse>, (StatusCode, Json<serde_json::Value>)> {
    let plan = sql_to_plan(&req.sql).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        )
    })?;

    let sources: HashMap<String, Batch<Row>> = req
        .sources
        .into_iter()
        .map(|(name, batch)| {
            let mut delta = ZSet::new();
            for row in batch.rows {
                let fields = row
                    .fields
                    .into_iter()
                    .map(|(k, v)| (k, Value::from(v)))
                    .collect();
                delta.insert(Row(fields), row.weight);
            }
            (name, Batch { epoch: batch.epoch, delta })
        })
        .collect();

    let mut agg_state: HashMap<String, AggregateState> = HashMap::new();
    let out = execute(&plan, &sources, &mut agg_state);

    let rows: Vec<HashMap<String, Value>> = out
        .delta
        .inner
        .keys()
        .map(|r| r.0.clone())
        .collect();

    Ok(Json(SqlExecuteResponse {
        epoch: out.epoch,
        output_rows: rows.len(),
        rows,
    }))
}
