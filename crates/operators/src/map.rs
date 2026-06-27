use ivm_core::{Batch, Row, ZSet};

pub fn map<F>(input: Batch<Row>, f: F) -> Batch<Row>
where
    F: Fn(Row) -> Row,
{
    let mut out = ZSet::default();
    for (row, weight) in input.delta.inner {
        out.insert(f(row), weight);
    }
    Batch {
        epoch: input.epoch,
        delta: out,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ivm_core::{Value, ZSet};
    use std::collections::HashMap;

    #[test]
    fn map_transforms_rows() {
        let mut delta = ZSet::new();
        delta.insert(
            Row(HashMap::from([("x".into(), Value::Int(1))])),
            1,
        );
        let batch = Batch::new(1, delta);
        let out = map(batch, |mut r| {
            if let Some(Value::Int(v)) = r.0.get_mut("x") {
                *v *= 2;
            }
            r
        });
        assert_eq!(out.delta.len(), 1);
        let row = out.delta.inner.keys().next().unwrap();
        assert_eq!(row.get_int("x"), 2);
    }
}
