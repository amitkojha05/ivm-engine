//! Compile and display a SQL logical plan, optionally execute against sample data.
//!
//! ```bash
//! cargo run -p sql_demo -- "SELECT customer_id, SUM(amount) FROM orders WHERE amount > 100 GROUP BY customer_id"
//! cargo run -p sql_demo -- --execute "SELECT * FROM orders WHERE amount > 50"
//! ```

use std::collections::HashMap;

use anyhow::Context;
use ivm_core::{Batch, Row, Value, ZSet};
use ivm_operators::AggregateState;
use ivm_planner::{display_plan, execute, sql_to_plan};

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let execute_mode = args.iter().any(|a| a == "--execute");
    let sql = args
        .iter()
        .skip(1)
        .filter(|a| *a != "--execute")
        .cloned()
        .collect::<Vec<_>>()
        .join(" ");

    if sql.is_empty() {
        anyhow::bail!("usage: sql_demo [--execute] \"SELECT ...\"");
    }

    let plan = sql_to_plan(&sql).context("failed to parse SQL")?;
    println!("Logical plan:\n{}", display_plan(&plan, 0));

    if execute_mode {
        let mut delta = ZSet::new();
        delta.insert(
            Row(HashMap::from([
                ("customer_id".into(), Value::Int(1)),
                ("amount".into(), Value::Int(200)),
            ])),
            1,
        );
        delta.insert(
            Row(HashMap::from([
                ("customer_id".into(), Value::Int(2)),
                ("amount".into(), Value::Int(30)),
            ])),
            1,
        );

        let sources = HashMap::from([(
            "orders".into(),
            Batch {
                epoch: 1,
                delta,
            },
        )]);

        let mut agg_state: HashMap<String, AggregateState> = HashMap::new();
        let out = execute(&plan, &sources, &mut agg_state);
        println!("\nOutput rows: {}", out.delta.len());
        for (row, weight) in &out.delta.inner {
            println!("  {:?} (w={weight})", row.0);
        }
    }

    Ok(())
}
