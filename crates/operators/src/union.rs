use ivm_core::{Batch, Row};

pub fn union(a: Batch<Row>, b: Batch<Row>) -> Batch<Row> {
    let epoch = a.epoch.max(b.epoch);
    let mut out = a.delta;
    out.merge(b.delta);
    Batch {
        epoch,
        delta: out,
        watermark: a.watermark.or(b.watermark),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ivm_core::{Value, ZSet};
    use std::collections::HashMap;

    #[test]
    fn union_merges_deltas() {
        let mut d1 = ZSet::new();
        d1.insert(
            Row(HashMap::from([("id".into(), Value::Int(1))])),
            1,
        );
        let mut d2 = ZSet::new();
        d2.insert(
            Row(HashMap::from([("id".into(), Value::Int(2))])),
            1,
        );
        let out = union(Batch::new(1, d1), Batch::new(2, d2));
        assert_eq!(out.delta.len(), 2);
        assert_eq!(out.epoch, 2);
    }
}
