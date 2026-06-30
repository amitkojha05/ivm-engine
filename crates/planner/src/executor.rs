use std::collections::HashMap;

use ivm_core::{Batch, Row, Value, ZSet};
use ivm_operators::{filter, incremental_join, map, AggregateState};

use crate::logical_plan::*;

/// Execute a logical plan against a map of named source batches.
pub fn execute(
    plan: &LogicalPlan,
    sources: &HashMap<String, Batch<Row>>,
    agg_state: &mut HashMap<String, AggregateState>,
) -> Batch<Row> {
    match plan {
        LogicalPlan::Scan { table } => sources.get(table).cloned().unwrap_or_else(|| Batch {
            epoch: 0,
            delta: ZSet::default(),
            watermark: None,
        }),

        LogicalPlan::Filter { input, predicate } => {
            let batch = execute(input, sources, agg_state);
            let pred = predicate.clone();
            filter(batch, move |row| eval_predicate(&pred, row))
        }

        LogicalPlan::Project { input, columns } => {
            let batch = execute(input, sources, agg_state);
            let cols = columns.clone();
            map(batch, move |row| {
                let mut out = HashMap::new();
                for col in &cols {
                    if let Some(v) = row.0.get(col) {
                        out.insert(col.clone(), v.clone());
                    }
                }
                Row(out)
            })
        }

        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let batch = execute(input, sources, agg_state);
            let key = format!("{:?}{:?}", group_by, aggregates);
            let state = agg_state.entry(key).or_insert_with(|| {
                let gb = group_by.clone();
                let aggs = aggregates.clone();
                AggregateState::custom(
                    move |row| {
                        if gb.is_empty() {
                            Value::Str("*".into())
                        } else if gb.len() == 1 {
                            row.get(&gb[0]).cloned().unwrap_or(Value::Null)
                        } else {
                            Value::Str(
                                gb.iter()
                                    .map(|k| format!("{:?}", row.0.get(k)))
                                    .collect::<Vec<_>>()
                                    .join("|"),
                            )
                        }
                    },
                    move |row| match aggs.first() {
                        Some(AggExpr {
                            func: AggFunc::Count,
                            ..
                        }) => 1,
                        Some(AggExpr {
                            func: AggFunc::Sum,
                            column,
                            ..
                        }) => row.get_int(column),
                        _ => 1,
                    },
                )
            });
            let out_delta = state.apply_delta(&batch.delta);
            Batch {
                epoch: batch.epoch,
                delta: out_delta,
                watermark: batch.watermark,
            }
        }

        LogicalPlan::Join {
            left,
            right,
            left_key,
            right_key,
        } => {
            let left_batch = execute(left, sources, agg_state);
            let right_batch = execute(right, sources, agg_state);
            incremental_join(&left_batch, &right_batch, left_key, right_key)
        }
    }
}

fn eval_predicate(pred: &Predicate, row: &Row) -> bool {
    match pred {
        Predicate::Eq { column, value } => row.0.get(column) == Some(value),
        Predicate::Gt { column, value } => {
            matches!(row.0.get(column), Some(Value::Int(n)) if n > value)
        }
        Predicate::Lt { column, value } => {
            matches!(row.0.get(column), Some(Value::Int(n)) if n < value)
        }
        Predicate::And(a, b) => eval_predicate(a, row) && eval_predicate(b, row),
    }
}
