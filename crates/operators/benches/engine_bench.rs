use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use ivm_core::{Batch, Row, Value, ZSet};
use ivm_operators::{filter, incremental_join};
use std::collections::HashMap;

fn make_batch(n: usize) -> Batch<Row> {
    let mut delta = ZSet::default();
    for i in 0..n {
        delta.insert(
            Row(HashMap::from([
                ("id".into(), Value::Int(i as i64)),
                ("amount".into(), Value::Int((i * 10) as i64)),
                ("status".into(), Value::Str("active".into())),
            ])),
            1,
        );
    }
    Batch { epoch: 1, delta }
}

fn bench_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_operator");
    for size in [100, 1_000, 10_000, 100_000] {
        let batch = make_batch(size);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &batch, |b, batch| {
            b.iter_batched(
                || batch.clone(),
                |batch| {
                    filter(black_box(batch), |row| {
                        matches!(row.0.get("amount"), Some(Value::Int(n)) if *n > 500)
                    })
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_operator");
    for size in [100, 1_000, 10_000] {
        let left = make_batch(size);
        let right = make_batch(size / 2);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &(left, right), |b, (l, r)| {
            b.iter(|| incremental_join(black_box(l), black_box(r), "id", "id"));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_filter, bench_join);
criterion_main!(benches);
