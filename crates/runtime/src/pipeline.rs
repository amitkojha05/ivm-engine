use std::collections::HashMap;

use ivm_core::{Batch, Row, ZSet};
use ivm_operators::{AggregateState, JoinState};
use ivm_planner::{execute, sql_to_plan, LogicalPlan};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub name: String,
    pub source: SourceKind,
    pub operators: Vec<OperatorKind>,
    #[serde(default)]
    pub sql: Option<String>,
    pub checkpoint_interval_epochs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceKind {
    Kafka {
        brokers: String,
        topic: String,
        group_id: String,
    },
    PgWal {
        conn_str: String,
        slot: String,
        publication: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OperatorKind {
    Filter { column: String, value: String },
    AggregateCount { key_column: String },
    AggregateSum {
        key_column: String,
        value_column: String,
    },
    Join { key_column: String },
}

pub struct Pipeline {
    pub config: PipelineConfig,
    pub aggregate: Option<AggregateState>,
    pub join: Option<JoinState>,
    pub accumulated: ZSet<Row>,
    sql_plan: Option<LogicalPlan>,
    sql_agg_state: HashMap<String, AggregateState>,
    source_table: String,
}

impl Pipeline {
    pub fn new(config: PipelineConfig) -> Self {
        let source_table = match &config.source {
            SourceKind::Kafka { topic, .. } => topic.clone(),
            SourceKind::PgWal { .. } => "default".into(),
        };

        let sql_plan = config
            .sql
            .as_ref()
            .and_then(|sql| sql_to_plan(sql).ok());

        let aggregate = if sql_plan.is_some() {
            None
        } else {
            config.operators.iter().find_map(|op| match op {
                OperatorKind::AggregateCount { key_column } => {
                    Some(AggregateState::count(key_column.clone()))
                }
                OperatorKind::AggregateSum {
                    key_column,
                    value_column,
                } => Some(AggregateState::sum(key_column.clone(), value_column.clone())),
                _ => None,
            })
        };

        let join = if sql_plan.is_some() {
            None
        } else {
            config
                .operators
                .iter()
                .any(|op| matches!(op, OperatorKind::Join { .. }))
                .then(|| JoinState::new(join_key_fn()))
        };

        Self {
            config,
            aggregate,
            join,
            accumulated: ZSet::default(),
            sql_plan,
            sql_agg_state: HashMap::new(),
            source_table,
        }
    }

    pub fn uses_sql(&self) -> bool {
        self.sql_plan.is_some()
    }

    pub fn apply_batch(&mut self, input: Batch<Row>) -> Batch<Row> {
        if let Some(ref plan) = self.sql_plan {
            let mut sources = HashMap::new();
            sources.insert(self.source_table.clone(), input);
            let out = execute(plan, &sources, &mut self.sql_agg_state);
            self.accumulated.merge(out.delta.clone());
            return out;
        }

        let mut current = input;

        for op in &self.config.operators {
            current = match op {
                OperatorKind::Filter { column, value } => {
                    let col = column.clone();
                    let val = value.clone();
                    ivm_operators::filter(current, move |row| {
                        row.get_str(&col).map(|s| s == val.as_str()).unwrap_or(false)
                            || row.get(&col)
                                .and_then(|v| v.as_int())
                                .map(|i| i.to_string() == val)
                                .unwrap_or(false)
                    })
                }
                OperatorKind::AggregateCount { .. } | OperatorKind::AggregateSum { .. } => {
                    if let Some(ref mut agg) = self.aggregate {
                        let out_delta = agg.apply_delta(&current.delta);
                        Batch {
                            epoch: current.epoch,
                            delta: out_delta,
                        }
                    } else {
                        current
                    }
                }
                OperatorKind::Join { .. } => current,
            };
        }

        self.accumulated.merge(current.delta.clone());
        current
    }

    pub fn apply_join_delta(
        &mut self,
        left: &ZSet<Row>,
        right: &ZSet<Row>,
        epoch: u64,
    ) -> Batch<Row> {
        if let Some(ref mut join) = self.join {
            let delta = join.apply_delta(left, right);
            Batch { epoch, delta }
        } else {
            Batch::empty(epoch)
        }
    }
}

fn join_key_fn() -> fn(&Row) -> ivm_core::Value {
    |row| row.get("id").cloned().unwrap_or(ivm_core::Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ivm_core::{Value, ZSet};
    use std::collections::HashMap;

    #[test]
    fn filter_pipeline() {
        let config = PipelineConfig {
            name: "test".into(),
            source: SourceKind::Kafka {
                brokers: "localhost:9092".into(),
                topic: "t".into(),
                group_id: "g".into(),
            },
            operators: vec![OperatorKind::Filter {
                column: "status".into(),
                value: "active".into(),
            }],
            sql: None,
            checkpoint_interval_epochs: 100,
        };
        let mut pipeline = Pipeline::new(config);

        let mut delta = ZSet::new();
        delta.insert(
            Row(HashMap::from([("status".into(), Value::Str("active".into()))])),
            1,
        );
        delta.insert(
            Row(HashMap::from([("status".into(), Value::Str("inactive".into()))])),
            1,
        );
        let out = pipeline.apply_batch(Batch::new(1, delta));
        assert_eq!(out.delta.len(), 1);
    }

    #[test]
    fn sql_pipeline_filter() {
        let config = PipelineConfig {
            name: "sql-test".into(),
            source: SourceKind::Kafka {
                brokers: "localhost:9092".into(),
                topic: "orders".into(),
                group_id: "g".into(),
            },
            operators: vec![],
            sql: Some("SELECT * FROM orders WHERE amount > 50".into()),
            checkpoint_interval_epochs: 100,
        };
        let mut pipeline = Pipeline::new(config);
        assert!(pipeline.uses_sql());

        let mut delta = ZSet::new();
        delta.insert(
            Row(HashMap::from([("amount".into(), Value::Int(100))])),
            1,
        );
        delta.insert(
            Row(HashMap::from([("amount".into(), Value::Int(10))])),
            1,
        );
        let out = pipeline.apply_batch(Batch::new(1, delta));
        assert_eq!(out.delta.len(), 1);
    }
}
