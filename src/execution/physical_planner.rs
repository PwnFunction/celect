use super::operators::{
    PhysicalFilter, PhysicalLimit, PhysicalOperator, PhysicalProjection, PhysicalScan,
    PhysicalUngroupedAggregate,
};
use crate::binder::ColumnType;
use crate::planner::{LogicalGet, LogicalOperator};

/// physical plan generator
/// converts logical operators into physical operators
pub struct PhysicalPlanner;

impl PhysicalPlanner {
    pub fn new() -> Self {
        Self
    }

    /// convert a logical plan into a physical plan
    /// returns a vector of operators in execution order (source first, sink last)
    /// and the schema for each operator's output
    pub fn plan(
        &self,
        logical_plan: LogicalOperator,
    ) -> (Vec<Box<dyn PhysicalOperator>>, Vec<Vec<ColumnType>>) {
        let mut operators: Vec<Box<dyn PhysicalOperator>> = Vec::new();
        let mut schemas: Vec<Vec<ColumnType>> = Vec::new();

        self.build_pipeline(logical_plan, &mut operators, &mut schemas);

        (operators, schemas)
    }

    /// recursively build the pipeline from the logical plan
    fn build_pipeline(
        &self,
        operator: LogicalOperator,
        operators: &mut Vec<Box<dyn PhysicalOperator>>,
        schemas: &mut Vec<Vec<ColumnType>>,
    ) {
        match operator {
            LogicalOperator::Get(get) => {
                self.build_get(get, operators, schemas);
            }
            LogicalOperator::Filter(filter) => {
                // recurse to child first (build bottom-up)
                let child = *filter.child;
                let expression = filter.expression;
                self.build_pipeline(child, operators, schemas);

                // then add filter
                self.build_filter_with_expr(expression, operators, schemas);
            }
            LogicalOperator::Projection(projection) => {
                // recurse to child first (build bottom-up)
                let child = *projection.child;
                let expressions = projection.expressions;
                self.build_pipeline(child, operators, schemas);

                // then add projection
                self.build_projection_with_exprs(expressions, operators, schemas);
            }
            LogicalOperator::Limit(limit) => {
                // recurse to child first (build bottom-up)
                let child = *limit.child;
                let limit_value = limit.limit;
                let offset_value = limit.offset;
                self.build_pipeline(child, operators, schemas);

                // then add limit
                self.build_limit(limit_value, offset_value, operators, schemas);
            }
            LogicalOperator::Aggregate(agg_op) => {
                // recurse to child first (build bottom-up)
                let child = *agg_op.child;
                let aggregates = agg_op.aggregates;
                self.build_pipeline(child, operators, schemas);

                // then add aggregate
                self.build_aggregate(aggregates, operators, schemas);
            }
        }
    }

    fn build_get(
        &self,
        get: LogicalGet,
        operators: &mut Vec<Box<dyn PhysicalOperator>>,
        schemas: &mut Vec<Vec<ColumnType>>,
    ) {
        // the optimizer has already filtered the columns list
        // extract the original indices for projection pushdown
        let projected_columns: Vec<usize> = get.columns.iter().map(|col| col.index).collect();

        // output schema matches the projected columns
        let output_schema: Vec<ColumnType> =
            get.columns.iter().map(|col| col.type_.clone()).collect();

        // create schema object for PhysicalScan
        // note: We need the FULL schema here (all columns from the file)
        // but we only have the projected ones. For now, use projected as schema.
        // todo: Pass full schema from binder
        let schema = crate::binder::Schema {
            columns: get.columns.clone(),
        };

        let scan = PhysicalScan::new(get.file_path, schema, Some(projected_columns), get.max_rows);
        operators.push(Box::new(scan));
        schemas.push(output_schema);
    }

    fn build_filter_with_expr(
        &self,
        expression: crate::binder::BoundExpression,
        operators: &mut Vec<Box<dyn PhysicalOperator>>,
        schemas: &mut Vec<Vec<ColumnType>>,
    ) {
        // filter doesn't change the schema - output schema is same as input
        let input_schema = schemas.last().unwrap().clone();

        let physical_filter = PhysicalFilter::new(expression);
        operators.push(Box::new(physical_filter));
        schemas.push(input_schema);
    }

    fn build_projection_with_exprs(
        &self,
        expressions: Vec<crate::binder::BoundExpression>,
        operators: &mut Vec<Box<dyn PhysicalOperator>>,
        schemas: &mut Vec<Vec<ColumnType>>,
    ) {
        // projection output schema is determined by the expressions
        let output_schema: Vec<ColumnType> = expressions
            .iter()
            .map(|expr| {
                // for now, we only support ColumnRef
                if let crate::binder::BoundExpression::ColumnRef { type_, .. } = expr {
                    type_.clone()
                } else {
                    ColumnType::Null // fallback for unsupported expressions
                }
            })
            .collect();

        let physical_projection = PhysicalProjection::new(expressions);
        operators.push(Box::new(physical_projection));
        schemas.push(output_schema);
    }

    fn build_limit(
        &self,
        limit: Option<usize>,
        offset: Option<usize>,
        operators: &mut Vec<Box<dyn PhysicalOperator>>,
        schemas: &mut Vec<Vec<ColumnType>>,
    ) {
        // limit doesn't change the schema - output schema is same as input
        let input_schema = schemas.last().unwrap().clone();

        let physical_limit = PhysicalLimit::new(limit, offset);
        operators.push(Box::new(physical_limit));
        schemas.push(input_schema);
    }

    fn build_aggregate(
        &self,
        aggregates: Vec<crate::binder::BoundAggregateExpression>,
        operators: &mut Vec<Box<dyn PhysicalOperator>>,
        schemas: &mut Vec<Vec<ColumnType>>,
    ) {
        // aggregate produces one INTEGER column per aggregate function
        let output_schema = vec![ColumnType::Integer; aggregates.len()];

        let physical_aggregate = PhysicalUngroupedAggregate::new(aggregates);
        operators.push(Box::new(physical_aggregate));
        schemas.push(output_schema);
    }
}
