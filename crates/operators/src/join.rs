use std::collections::HashMap;

use ivm_core::{Row, Value, ZSet};

pub struct JoinState {
    pub left_history: ZSet<Row>,
    pub right_history: ZSet<Row>,
    pub join_key: fn(&Row) -> Value,
}

impl JoinState {
    pub fn new(join_key: fn(&Row) -> Value) -> Self {
        Self {
            left_history: ZSet::default(),
            right_history: ZSet::default(),
            join_key,
        }
    }

    /// Incremental join: Δ(A ⋈ B) = ΔA ⋈ B_old + A_old ⋈ ΔB + ΔA ⋈ ΔB
    pub fn apply_delta(&mut self, left_delta: &ZSet<Row>, right_delta: &ZSet<Row>) -> ZSet<Row> {
        let mut out = ZSet::default();

        self.join_sets(left_delta, &self.right_history, &mut out);
        self.join_sets(&self.left_history, right_delta, &mut out);
        self.join_sets(left_delta, right_delta, &mut out);

        self.left_history.merge(left_delta.clone());
        self.right_history.merge(right_delta.clone());

        out
    }

    fn join_sets(&self, left: &ZSet<Row>, right: &ZSet<Row>, out: &mut ZSet<Row>) {
        let mut right_idx: HashMap<Value, Vec<(Row, i64)>> = HashMap::new();
        for (row, w) in &right.inner {
            right_idx
                .entry((self.join_key)(row))
                .or_default()
                .push((row.clone(), *w));
        }

        for (lrow, lw) in &left.inner {
            let key = (self.join_key)(lrow);
            if let Some(matches) = right_idx.get(&key) {
                for (rrow, rw) in matches {
                    let mut merged = lrow.0.clone();
                    merged.extend(rrow.0.clone());
                    out.insert(Row(merged), lw * rw);
                }
            }
        }
    }
}

/// Stateless join of two batch deltas on equality keys (no history).
pub fn incremental_join(
    left: &ivm_core::Batch<Row>,
    right: &ivm_core::Batch<Row>,
    left_key: &str,
    right_key: &str,
) -> ivm_core::Batch<Row> {
    let mut out = ZSet::default();
    let mut right_idx: HashMap<Value, Vec<(Row, i64)>> = HashMap::new();

    for (row, w) in &right.delta.inner {
        let key = row.get(right_key).cloned().unwrap_or(Value::Null);
        right_idx.entry(key).or_default().push((row.clone(), *w));
    }

    for (lrow, lw) in &left.delta.inner {
        let key = lrow.get(left_key).cloned().unwrap_or(Value::Null);
        if let Some(matches) = right_idx.get(&key) {
            for (rrow, rw) in matches {
                let mut merged = lrow.0.clone();
                merged.extend(rrow.0.clone());
                out.insert(Row(merged), lw * rw);
            }
        }
    }

    ivm_core::Batch {
        epoch: left.epoch.max(right.epoch),
        delta: out,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn left_row(id: i64) -> Row {
        Row(HashMap::from([("id".into(), Value::Int(id))]))
    }

    fn right_row(id: i64, name: &str) -> Row {
        Row(HashMap::from([
            ("id".into(), Value::Int(id)),
            ("name".into(), Value::Str(name.into())),
        ]))
    }

    fn join_key(row: &Row) -> Value {
        row.get("id").cloned().unwrap_or(Value::Null)
    }

    #[test]
    fn incremental_join() {
        let mut state = JoinState::new(join_key);

        let mut left = ZSet::new();
        left.insert(left_row(1), 1);
        let mut right = ZSet::new();
        right.insert(right_row(1, "alice"), 1);

        let out = state.apply_delta(&left, &right);
        assert_eq!(out.len(), 1);
        let joined = out.inner.keys().next().unwrap();
        assert_eq!(joined.get_str("name"), Some("alice"));
    }

    #[test]
    fn join_new_right_row() {
        let mut state = JoinState::new(join_key);

        let mut left = ZSet::new();
        left.insert(left_row(1), 1);
        state.apply_delta(&left, &ZSet::new());

        let mut right = ZSet::new();
        right.insert(right_row(1, "bob"), 1);
        let out = state.apply_delta(&ZSet::new(), &right);
        assert_eq!(out.len(), 1);
    }
}
