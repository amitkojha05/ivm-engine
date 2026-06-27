use crate::logical_plan::{AggFunc, LogicalPlan};

pub fn display_plan(plan: &LogicalPlan, indent: usize) -> String {
    let pad = "  ".repeat(indent);
    match plan {
        LogicalPlan::Scan { table } => format!("{}Scan({})\n", pad, table),
        LogicalPlan::Filter { input, predicate } => {
            format!(
                "{}Filter({:?})\n{}",
                pad,
                predicate,
                display_plan(input, indent + 1)
            )
        }
        LogicalPlan::Project { input, columns } => {
            format!(
                "{}Project({})\n{}",
                pad,
                columns.join(", "),
                display_plan(input, indent + 1)
            )
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let aggs: Vec<_> = aggregates
                .iter()
                .map(|a| format!("{:?}({})", a.func, a.column))
                .collect();
            format!(
                "{}Aggregate(group=[{}], aggs=[{}])\n{}",
                pad,
                group_by.join(", "),
                aggs.join(", "),
                display_plan(input, indent + 1)
            )
        }
        LogicalPlan::Join {
            left,
            right,
            left_key,
            right_key,
        } => {
            format!(
                "{}Join({} = {})\n{}{}",
                pad,
                left_key,
                right_key,
                display_plan(left, indent + 1),
                display_plan(right, indent + 1)
            )
        }
    }
}

impl std::fmt::Display for AggFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggFunc::Count => write!(f, "Count"),
            AggFunc::Sum => write!(f, "Sum"),
            AggFunc::Min => write!(f, "Min"),
            AggFunc::Max => write!(f, "Max"),
        }
    }
}
