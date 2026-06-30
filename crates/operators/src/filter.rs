use ivm_core::{Batch, Row, ZSet};

pub fn filter<F>(input: Batch<Row>, predicate: F) -> Batch<Row>
where
    F: Fn(&Row) -> bool,
{
    let mut out = ZSet::default();
    for (row, weight) in input.delta.inner {
        if predicate(&row) {
            out.insert(row, weight);
        }
    }
    Batch {
        epoch: input.epoch,
        delta: out,
        watermark: input.watermark,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ivm_core::{Value, ZSet};
    use std::collections::HashMap;

    fn row(id: i64) -> Row {
        Row(HashMap::from([("id".into(), Value::Int(id))]))
    }

    #[test]
    fn filter_keeps_matching_rows() {
        let mut delta = ZSet::new();
        delta.insert(row(1), 1);
        delta.insert(row(2), 1);
        let batch = Batch::new(1, delta);
        let out = filter(batch, |r| r.get_int("id") > 1);
        assert_eq!(out.delta.len(), 1);
    }
}
