use anyhow::{bail, Result};
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, GroupByExpr, JoinConstraint, JoinOperator,
    ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::logical_plan::*;
use ivm_core::Value;

pub fn sql_to_plan(sql: &str) -> Result<LogicalPlan> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    let stmt = statements
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty SQL"))?;

    match stmt {
        Statement::Query(query) => plan_query(*query),
        other => bail!("unsupported statement: {:?}", other),
    }
}

fn plan_query(query: Query) -> Result<LogicalPlan> {
    let body = *query.body;
    match body {
        SetExpr::Select(select) => plan_select(*select),
        other => bail!("unsupported query body: {:?}", other),
    }
}

fn plan_select(select: Select) -> Result<LogicalPlan> {
    let from = select
        .from
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("no FROM clause"))?;

    let mut plan = plan_table_factor(from.relation)?;

    for join in from.joins {
        let right = plan_table_factor(join.relation)?;
        let (left_key, right_key) = extract_join_keys(join.join_operator)?;
        plan = LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(right),
            left_key,
            right_key,
        };
    }

    if let Some(expr) = select.selection {
        let predicate = expr_to_predicate(expr)?;
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }

    let has_agg = select
        .projection
        .iter()
        .any(|p| matches!(p, SelectItem::UnnamedExpr(Expr::Function(_))));
    let has_group_by = match &select.group_by {
        GroupByExpr::Expressions(exprs) => !exprs.is_empty(),
        GroupByExpr::All => true,
    };

    if has_agg || has_group_by {
        let group_by = match &select.group_by {
            GroupByExpr::Expressions(exprs) => exprs
                .iter()
                .map(expr_to_column_name)
                .collect::<Result<Vec<_>>>()?,
            GroupByExpr::All => vec![],
        };

        let aggregates = select
            .projection
            .iter()
            .filter_map(|p| match p {
                SelectItem::UnnamedExpr(e) => parse_agg_expr(e).ok(),
                SelectItem::ExprWithAlias { expr, alias } => parse_agg_expr(expr)
                    .ok()
                    .map(|mut a| {
                        a.alias = alias.value.clone();
                        a
                    }),
                _ => None,
            })
            .collect();

        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_by,
            aggregates,
        };
    } else {
        let columns: Vec<String> = select
            .projection
            .iter()
            .filter_map(|p| match p {
                SelectItem::UnnamedExpr(e) => expr_to_column_name(e).ok(),
                SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
                SelectItem::Wildcard(_) => None,
                _ => None,
            })
            .collect();

        if !columns.is_empty() {
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                columns,
            };
        }
    }

    Ok(plan)
}

fn plan_table_factor(factor: TableFactor) -> Result<LogicalPlan> {
    match factor {
        TableFactor::Table { name, .. } => Ok(LogicalPlan::Scan {
            table: object_name_to_string(&name),
        }),
        other => bail!("unsupported table factor: {:?}", other),
    }
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|i| i.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

fn extract_join_keys(op: JoinOperator) -> Result<(String, String)> {
    match op {
        JoinOperator::Inner(JoinConstraint::On(expr)) => match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => Ok((
                expr_to_column_name(&left)?,
                expr_to_column_name(&right)?,
            )),
            _ => bail!("only equality join supported"),
        },
        _ => bail!("only INNER JOIN ON supported"),
    }
}

fn expr_to_predicate(expr: Expr) -> Result<Predicate> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Eq => {
                let col = expr_to_column_name(&left)?;
                let val = expr_to_sql_value(&right)?;
                Ok(Predicate::Eq { column: col, value: val })
            }
            BinaryOperator::Gt => {
                let col = expr_to_column_name(&left)?;
                let val = expr_to_i64(&right)?;
                Ok(Predicate::Gt {
                    column: col,
                    value: val,
                })
            }
            BinaryOperator::Lt => {
                let col = expr_to_column_name(&left)?;
                let val = expr_to_i64(&right)?;
                Ok(Predicate::Lt {
                    column: col,
                    value: val,
                })
            }
            BinaryOperator::And => Ok(Predicate::And(
                Box::new(expr_to_predicate(*left)?),
                Box::new(expr_to_predicate(*right)?),
            )),
            _ => bail!("unsupported operator in WHERE: {:?}", op),
        },
        _ => bail!("unsupported WHERE expression: {:?}", expr),
    }
}

fn expr_to_column_name(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => Ok(parts.last().unwrap().value.clone()),
        _ => bail!("expected column name, got {:?}", expr),
    }
}

fn expr_to_sql_value(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Value(SqlValue::Number(n, _)) => Ok(Value::Int(n.parse()?)),
        Expr::Value(SqlValue::SingleQuotedString(s)) => Ok(Value::Str(s.clone())),
        Expr::Value(SqlValue::Boolean(b)) => Ok(Value::Bool(*b)),
        _ => bail!("unsupported literal: {:?}", expr),
    }
}

fn expr_to_i64(expr: &Expr) -> Result<i64> {
    match expr_to_sql_value(expr)? {
        Value::Int(n) => Ok(n),
        _ => bail!("expected integer literal"),
    }
}

fn parse_agg_expr(expr: &Expr) -> Result<AggExpr> {
    match expr {
        Expr::Function(f) => {
            let name = f.name.to_string().to_uppercase();
            let func = match name.as_str() {
                "COUNT" => AggFunc::Count,
                "SUM" => AggFunc::Sum,
                "MIN" => AggFunc::Min,
                "MAX" => AggFunc::Max,
                _ => bail!("unsupported aggregate: {}", name),
            };
            let column = match f.args.first() {
                Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) => "*".into(),
                Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => expr_to_column_name(e)?,
                _ => "*".into(),
            };
            Ok(AggExpr {
                func,
                column: column.clone(),
                alias: format!("{}_{}", name.to_lowercase(), column),
            })
        }
        _ => bail!("not an aggregate expression"),
    }
}
