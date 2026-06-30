use ivm_planner::{display_plan, execute, sql_to_plan};
use ivm_core::{Batch, Row, Value, ZSet};
use std::collections::HashMap;

#[test]
fn test_simple_filter_plan() {
    let sql = "SELECT customer_id, amount FROM orders WHERE amount > 100";
    let plan = sql_to_plan(sql).unwrap();
    let output = display_plan(&plan, 0);
    assert!(output.contains("Filter"));
    assert!(output.contains("Scan(orders)"));
}

#[test]
fn test_group_by_plan() {
    let sql = "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id";
    let plan = sql_to_plan(sql).unwrap();
    let output = display_plan(&plan, 0);
    assert!(output.contains("Aggregate"));
    assert!(output.contains("group=[customer_id]"));
}

#[test]
fn test_join_plan() {
    let sql =
        "SELECT o.customer_id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id";
    let plan = sql_to_plan(sql).unwrap();
    let output = display_plan(&plan, 0);
    assert!(output.contains("Join(customer_id = id)"));
}

#[test]
fn test_execute_filter() {
    let sql = "SELECT * FROM orders WHERE amount > 50";
    let plan = sql_to_plan(sql).unwrap();

    let mut delta = ZSet::default();
    delta.insert(
        Row(HashMap::from([
            ("amount".into(), Value::Int(100)),
            ("id".into(), Value::Int(1)),
        ])),
        1,
    );
    delta.insert(
        Row(HashMap::from([
            ("amount".into(), Value::Int(10)),
            ("id".into(), Value::Int(2)),
        ])),
        1,
    );

    let sources = HashMap::from([(
        "orders".into(),
        Batch {
            epoch: 1,
            delta,
            watermark: None,
        },
    )]);
    let mut agg_state = HashMap::new();
    let out = execute(&plan, &sources, &mut agg_state);

    assert_eq!(out.delta.inner.len(), 1);
}
