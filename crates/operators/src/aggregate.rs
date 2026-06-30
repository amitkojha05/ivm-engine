use std::collections::HashMap;

use ivm_core::{Batch, Row, Value, ZSet};

pub struct AggregateState {
    pub accumulated: ZSet<Row>,
    pub result: HashMap<Value, i64>,
    key_fn: Box<dyn Fn(&Row) -> Value + Send + Sync>,
    agg_fn: Box<dyn Fn(&Row) -> i64 + Send + Sync>,
}

impl AggregateState {
    pub fn count(key_column: impl Into<String>) -> Self {
        let col = key_column.into();
        Self {
            accumulated: ZSet::default(),
            result: HashMap::new(),
            key_fn: Box::new(move |row| row.get(&col).cloned().unwrap_or(Value::Null)),
            agg_fn: Box::new(|_| 1),
        }
    }

    pub fn sum(key_column: impl Into<String>, value_column: impl Into<String>) -> Self {
        let key_col = key_column.into();
        let val_col = value_column.into();
        Self {
            accumulated: ZSet::default(),
            result: HashMap::new(),
            key_fn: Box::new(move |row| row.get(&key_col).cloned().unwrap_or(Value::Null)),
            agg_fn: Box::new(move |row| row.get_int(&val_col)),
        }
    }

    pub fn custom(
        key_fn: impl Fn(&Row) -> Value + Send + Sync + 'static,
        agg_fn: impl Fn(&Row) -> i64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            accumulated: ZSet::default(),
            result: HashMap::new(),
            key_fn: Box::new(key_fn),
            agg_fn: Box::new(agg_fn),
        }
    }

    pub fn apply_delta(&mut self, delta: &ZSet<Row>) -> ZSet<Row> {
        let mut output_delta = ZSet::default();

        for (row, weight) in &delta.inner {
            let key = (self.key_fn)(row);
            let contribution = (self.agg_fn)(row) * weight;

            let old_val = *self.result.get(&key).unwrap_or(&0);
            *self.result.entry(key.clone()).or_insert(0) += contribution;
            let new_val = self.result[&key];

            if old_val != 0 {
                output_delta.insert(output_row(&key, old_val), -1);
            }
            if new_val != 0 {
                output_delta.insert(output_row(&key, new_val), 1);
            }

            self.accumulated.insert(row.clone(), *weight);
        }

        output_delta
    }
}

pub struct WindowedAggregateState {
    inner: AggregateState,
    window_size_ms: u64,
    pending: Vec<(u64, Row, i64)>,
    watermark_ms: u64,
}

impl WindowedAggregateState {
    pub fn new(inner: AggregateState, window_size_ms: u64) -> Self {
        Self {
            inner,
            window_size_ms,
            pending: Vec::new(),
            watermark_ms: 0,
        }
    }

    pub fn apply_batch(
        &mut self,
        batch: &Batch<Row>,
        event_time_fn: impl Fn(&Row) -> u64,
    ) -> ZSet<Row> {
        for (row, weight) in &batch.delta.inner {
            let ts = event_time_fn(row);
            self.pending.push((ts, row.clone(), *weight));
        }

        if let Some(ref wm) = batch.watermark {
            self.watermark_ms = self.watermark_ms.max(wm.event_time_ms);
        }

        let cutoff = self.watermark_ms;
        let window_size = self.window_size_ms;
        let mut ready = Vec::new();
        self.pending.retain(|(ts, row, w)| {
            if ts + window_size <= cutoff {
                ready.push((row.clone(), *w));
                false
            } else {
                true
            }
        });

        let mut flush_delta = ZSet::default();
        for (row, w) in ready {
            flush_delta.insert(row, w);
        }
        self.inner.apply_delta(&flush_delta)
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

fn output_row(key: &Value, value: i64) -> Row {
    Row(HashMap::from([
        ("key".into(), key.clone()),
        ("value".into(), Value::Int(value)),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ivm_core::Watermark;
    use std::collections::HashMap;

    fn input_row(key: &str, val: i64) -> Row {
        Row(HashMap::from([
            ("category".into(), Value::Str(key.into())),
            ("amount".into(), Value::Int(val)),
        ]))
    }

    #[test]
    fn incremental_count() {
        let mut state = AggregateState::count("category");

        let mut delta = ZSet::new();
        delta.insert(input_row("a", 10), 1);
        delta.insert(input_row("a", 20), 1);
        let out = state.apply_delta(&delta);
        assert_eq!(out.len(), 1);
        assert_eq!(state.result[&Value::Str("a".into())], 2);

        let mut delta2 = ZSet::new();
        delta2.insert(input_row("a", 10), -1);
        let out2 = state.apply_delta(&delta2);
        assert!(!out2.is_empty());
        assert_eq!(state.result[&Value::Str("a".into())], 1);
    }

    #[test]
    fn incremental_sum() {
        let mut state = AggregateState::sum("category", "amount");

        let mut delta = ZSet::new();
        delta.insert(input_row("a", 10), 1);
        delta.insert(input_row("a", 20), 1);
        state.apply_delta(&delta);
        assert_eq!(state.result[&Value::Str("a".into())], 30);
    }

    #[test]
    fn windowed_aggregate_closes_on_watermark() {
        let inner = AggregateState::count("category");
        let mut state = WindowedAggregateState::new(inner, 60_000);

        let mut delta = ZSet::new();
        delta.insert(input_row("a", 10), 1);
        let batch = Batch {
            epoch: 1,
            delta,
            watermark: Some(Watermark {
                event_time_ms: 120_000,
                source_id: "test".into(),
            }),
        };

        let out = state.apply_batch(&batch, |_| 0);
        assert_eq!(out.len(), 1);
        assert_eq!(state.pending_count(), 0);
    }
}
