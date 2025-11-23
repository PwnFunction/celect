use crate::binder::{BoundAggregateExpression, BoundExpression, BoundQuery, Column};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalOperator {
    Get(LogicalGet),
    Filter(LogicalFilter),
    Projection(LogicalProjection),
    Limit(LogicalLimit),
    Aggregate(LogicalAggregate),
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalGet {
    pub file_path: PathBuf,
    pub columns: Vec<Column>,    // schema of the file
    pub max_rows: Option<usize>, // pushed down from LIMIT for early termination
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalFilter {
    pub expression: BoundExpression,
    pub child: Box<LogicalOperator>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalProjection {
    pub expressions: Vec<BoundExpression>,
    pub child: Box<LogicalOperator>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalLimit {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub child: Box<LogicalOperator>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalAggregate {
    pub aggregates: Vec<BoundAggregateExpression>,
    pub child: Box<LogicalOperator>,
}

pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Self
    }

    pub fn plan(&self, query: BoundQuery) -> LogicalOperator {
        // 1. Create Source (LogicalGet)
        let mut root = LogicalOperator::Get(LogicalGet {
            file_path: query.file_path,
            columns: query.schema.columns,
            max_rows: None, // will be set by optimizer if LIMIT can be pushed down
        });

        // 2. Apply Filter (if present)
        if let Some(where_clause) = query.where_clause {
            root = LogicalOperator::Filter(LogicalFilter {
                expression: where_clause,
                child: Box::new(root),
            });
        }

        // 3. Apply Aggregate (if present)
        // aggregates consume all rows and produce single result row
        if !query.aggregates.is_empty() {
            root = LogicalOperator::Aggregate(LogicalAggregate {
                aggregates: query.aggregates,
                child: Box::new(root),
            });
            // note: For aggregates, we don't add projection - aggregate itself returns the result
        } else {
            // 3b. Apply Projection (only if no aggregates)
            // convert selected columns into BoundExpression::ColumnRef
            let projection_expressions = query
                .select_columns
                .into_iter()
                .map(|col| BoundExpression::ColumnRef {
                    name: col.name,
                    index: col.index,
                    type_: col.type_,
                })
                .collect();

            root = LogicalOperator::Projection(LogicalProjection {
                expressions: projection_expressions,
                child: Box::new(root),
            });
        }

        // 4. Apply Limit/Offset (if present)
        if query.limit.is_some() || query.offset.is_some() {
            root = LogicalOperator::Limit(LogicalLimit {
                limit: query.limit,
                offset: query.offset,
                child: Box::new(root),
            });
        }

        root
    }
}
