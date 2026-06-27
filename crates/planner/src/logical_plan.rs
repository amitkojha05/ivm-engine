use ivm_core::Value;

/// A node in the logical plan tree.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Table scan — source of rows
    Scan { table: String },

    /// Filter rows by predicate
    Filter {
        input: Box<LogicalPlan>,
        predicate: Predicate,
    },

    /// Project / rename columns
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<String>,
    },

    /// Aggregate with optional GROUP BY
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<String>,
        aggregates: Vec<AggExpr>,
    },

    /// Inner join on equality key
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        left_key: String,
        right_key: String,
    },
}

#[derive(Debug, Clone)]
pub enum Predicate {
    /// column = literal
    Eq { column: String, value: Value },
    /// column > literal (Int only)
    Gt { column: String, value: i64 },
    /// column < literal
    Lt { column: String, value: i64 },
    /// AND of two predicates
    And(Box<Predicate>, Box<Predicate>),
}

#[derive(Debug, Clone)]
pub struct AggExpr {
    pub func: AggFunc,
    pub column: String,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
}
