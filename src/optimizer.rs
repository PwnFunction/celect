use crate::binder::{BoundExpression, ColumnType};
use crate::parser::LiteralValue;
use crate::planner::{LogicalFilter, LogicalGet, LogicalLimit, LogicalOperator, LogicalProjection};
use std::collections::{HashMap, HashSet};

pub struct Optimizer;

impl Optimizer {
    pub fn new() -> Self {
        Self
    }

    /// optimize a logical plan by applying multiple optimization passes:
    /// 1. Dead Code Elimination - simplify boolean literals in expressions
    /// 2. Projection Pushdown - prune unnecessary columns
    /// 3. Limit Pushdown - push LIMIT down to scan for early termination
    pub fn optimize(&self, plan: LogicalOperator) -> LogicalOperator {
        // first: Eliminate dead code (simplify boolean literals)
        let plan = self.eliminate_dead_code(plan);

        // second: Collect required columns and apply projection pushdown
        let required_columns = self.collect_required_columns(&plan);
        let plan = self.apply_projection_pushdown(plan, &required_columns);

        // third: Push down LIMIT to scan for early termination
        self.push_down_limit(plan)
    }

    /// eliminate dead code by simplifying boolean literals in expressions.
    /// examples:
    /// - true AND x → x
    /// - false OR x → x
    /// - NOT true → false
    /// - Filter with true condition → removed
    fn eliminate_dead_code(&self, plan: LogicalOperator) -> LogicalOperator {
        match plan {
            LogicalOperator::Projection(proj) => {
                let optimized_child = self.eliminate_dead_code(*proj.child);
                LogicalOperator::Projection(LogicalProjection {
                    expressions: proj.expressions,
                    child: Box::new(optimized_child),
                })
            }
            LogicalOperator::Filter(filter) => {
                // simplify the filter expression
                let simplified_expr = self.simplify_expression(filter.expression);

                // optimize child first
                let optimized_child = self.eliminate_dead_code(*filter.child);

                // check if the simplified expression is a constant
                if self.is_constant_true(&simplified_expr) {
                    // filter always passes - remove it!
                    optimized_child
                } else {
                    // keep the filter (even if constant false - executor handles it)
                    LogicalOperator::Filter(LogicalFilter {
                        expression: simplified_expr,
                        child: Box::new(optimized_child),
                    })
                }
            }
            LogicalOperator::Get(get) => {
                // base case - no optimization needed
                LogicalOperator::Get(get)
            }
            LogicalOperator::Limit(limit) => {
                // optimize child first
                let optimized_child = self.eliminate_dead_code(*limit.child);
                LogicalOperator::Limit(LogicalLimit {
                    limit: limit.limit,
                    offset: limit.offset,
                    child: Box::new(optimized_child),
                })
            }
            LogicalOperator::Aggregate(agg) => {
                // optimize child first
                let optimized_child = self.eliminate_dead_code(*agg.child);
                LogicalOperator::Aggregate(crate::planner::LogicalAggregate {
                    aggregates: agg.aggregates,
                    child: Box::new(optimized_child),
                })
            }
        }
    }

    /// recursively simplify boolean expressions containing literal true/false.
    fn simplify_expression(&self, expr: BoundExpression) -> BoundExpression {
        match expr {
            // and: true AND x → x, false AND x → false
            BoundExpression::And(left, right) => {
                let left = self.simplify_expression(*left);
                let right = self.simplify_expression(*right);

                // check for constant true/false on left
                if self.is_constant_true(&left) {
                    return right; // true AND x → x
                }
                if self.is_constant_false(&left) {
                    return left; // false AND x → false
                }

                // check for constant true/false on right
                if self.is_constant_true(&right) {
                    return left; // x AND true → x
                }
                if self.is_constant_false(&right) {
                    return right; // x AND false → false
                }

                BoundExpression::And(Box::new(left), Box::new(right))
            }

            // or: true OR x → true, false OR x → x
            BoundExpression::Or(left, right) => {
                let left = self.simplify_expression(*left);
                let right = self.simplify_expression(*right);

                // check for constant true/false on left
                if self.is_constant_true(&left) {
                    return left; // true OR x → true
                }
                if self.is_constant_false(&left) {
                    return right; // false OR x → x
                }

                // check for constant true/false on right
                if self.is_constant_true(&right) {
                    return right; // x OR true → true
                }
                if self.is_constant_false(&right) {
                    return left; // x OR false → x
                }

                BoundExpression::Or(Box::new(left), Box::new(right))
            }

            // not: NOT NOT x → x, NOT true → false, NOT false → true
            BoundExpression::Not(inner) => {
                let inner = self.simplify_expression(*inner);

                // not NOT x → x (double negation elimination)
                if let BoundExpression::Not(double_inner) = inner {
                    return *double_inner;
                }

                // not true → false
                if self.is_constant_true(&inner) {
                    return BoundExpression::Literal {
                        value: LiteralValue::Boolean(false),
                        type_: ColumnType::Boolean,
                    };
                }
                // not false → true
                if self.is_constant_false(&inner) {
                    return BoundExpression::Literal {
                        value: LiteralValue::Boolean(true),
                        type_: ColumnType::Boolean,
                    };
                }

                BoundExpression::Not(Box::new(inner))
            }

            // comparison operators - simplify children, then evaluate if both are literals
            BoundExpression::Equal(left, right) => {
                let left = self.simplify_expression(*left);
                let right = self.simplify_expression(*right);

                // try to evaluate at compile time
                if let (Some(left_val), Some(right_val)) =
                    (self.extract_literal(&left), self.extract_literal(&right))
                {
                    if let Some(result) = self.evaluate_equal(left_val, right_val) {
                        return self.make_bool_literal(result);
                    }
                }

                BoundExpression::Equal(Box::new(left), Box::new(right))
            }
            BoundExpression::NotEqual(left, right) => {
                let left = self.simplify_expression(*left);
                let right = self.simplify_expression(*right);

                if let (Some(left_val), Some(right_val)) =
                    (self.extract_literal(&left), self.extract_literal(&right))
                {
                    if let Some(result) = self.evaluate_not_equal(left_val, right_val) {
                        return self.make_bool_literal(result);
                    }
                }

                BoundExpression::NotEqual(Box::new(left), Box::new(right))
            }
            BoundExpression::GreaterThan(left, right) => {
                let left = self.simplify_expression(*left);
                let right = self.simplify_expression(*right);

                if let (Some(left_val), Some(right_val)) =
                    (self.extract_literal(&left), self.extract_literal(&right))
                {
                    if let Some(result) = self.evaluate_greater_than(left_val, right_val) {
                        return self.make_bool_literal(result);
                    }
                }

                BoundExpression::GreaterThan(Box::new(left), Box::new(right))
            }
            BoundExpression::GreaterThanOrEqual(left, right) => {
                let left = self.simplify_expression(*left);
                let right = self.simplify_expression(*right);

                if let (Some(left_val), Some(right_val)) =
                    (self.extract_literal(&left), self.extract_literal(&right))
                {
                    if let Some(result) = self.evaluate_greater_than_or_equal(left_val, right_val) {
                        return self.make_bool_literal(result);
                    }
                }

                BoundExpression::GreaterThanOrEqual(Box::new(left), Box::new(right))
            }
            BoundExpression::LessThan(left, right) => {
                let left = self.simplify_expression(*left);
                let right = self.simplify_expression(*right);

                if let (Some(left_val), Some(right_val)) =
                    (self.extract_literal(&left), self.extract_literal(&right))
                {
                    if let Some(result) = self.evaluate_less_than(left_val, right_val) {
                        return self.make_bool_literal(result);
                    }
                }

                BoundExpression::LessThan(Box::new(left), Box::new(right))
            }
            BoundExpression::LessThanOrEqual(left, right) => {
                let left = self.simplify_expression(*left);
                let right = self.simplify_expression(*right);

                if let (Some(left_val), Some(right_val)) =
                    (self.extract_literal(&left), self.extract_literal(&right))
                {
                    if let Some(result) = self.evaluate_less_than_or_equal(left_val, right_val) {
                        return self.make_bool_literal(result);
                    }
                }

                BoundExpression::LessThanOrEqual(Box::new(left), Box::new(right))
            }

            // leaf nodes - no simplification needed
            BoundExpression::ColumnRef { .. } | BoundExpression::Literal { .. } => expr,
        }
    }

    /// check if expression is constant true
    fn is_constant_true(&self, expr: &BoundExpression) -> bool {
        matches!(
            expr,
            BoundExpression::Literal {
                value: LiteralValue::Boolean(true),
                ..
            }
        )
    }

    /// check if expression is constant false
    fn is_constant_false(&self, expr: &BoundExpression) -> bool {
        matches!(
            expr,
            BoundExpression::Literal {
                value: LiteralValue::Boolean(false),
                ..
            }
        )
    }

    /// extract literal value from expression if it is a literal
    fn extract_literal<'a>(&self, expr: &'a BoundExpression) -> Option<&'a LiteralValue> {
        match expr {
            BoundExpression::Literal { value, .. } => Some(value),
            _ => None,
        }
    }

    /// evaluate a comparison between two literal values at compile time
    fn evaluate_equal(&self, left: &LiteralValue, right: &LiteralValue) -> Option<bool> {
        match (left, right) {
            (LiteralValue::Integer(a), LiteralValue::Integer(b)) => Some(a == b),
            (LiteralValue::Float(a), LiteralValue::Float(b)) => Some(a == b),
            (LiteralValue::String(a), LiteralValue::String(b)) => Some(a == b),
            (LiteralValue::Boolean(a), LiteralValue::Boolean(b)) => Some(a == b),
            (LiteralValue::Null, LiteralValue::Null) => Some(false), // null = NULL is false in SQL
            _ => None, // different types - can't evaluate
        }
    }

    fn evaluate_not_equal(&self, left: &LiteralValue, right: &LiteralValue) -> Option<bool> {
        self.evaluate_equal(left, right).map(|v| !v)
    }

    fn evaluate_greater_than(&self, left: &LiteralValue, right: &LiteralValue) -> Option<bool> {
        match (left, right) {
            (LiteralValue::Integer(a), LiteralValue::Integer(b)) => Some(a > b),
            (LiteralValue::Float(a), LiteralValue::Float(b)) => Some(a > b),
            (LiteralValue::String(a), LiteralValue::String(b)) => Some(a > b),
            _ => None,
        }
    }

    fn evaluate_greater_than_or_equal(
        &self,
        left: &LiteralValue,
        right: &LiteralValue,
    ) -> Option<bool> {
        match (left, right) {
            (LiteralValue::Integer(a), LiteralValue::Integer(b)) => Some(a >= b),
            (LiteralValue::Float(a), LiteralValue::Float(b)) => Some(a >= b),
            (LiteralValue::String(a), LiteralValue::String(b)) => Some(a >= b),
            _ => None,
        }
    }

    fn evaluate_less_than(&self, left: &LiteralValue, right: &LiteralValue) -> Option<bool> {
        match (left, right) {
            (LiteralValue::Integer(a), LiteralValue::Integer(b)) => Some(a < b),
            (LiteralValue::Float(a), LiteralValue::Float(b)) => Some(a < b),
            (LiteralValue::String(a), LiteralValue::String(b)) => Some(a < b),
            _ => None,
        }
    }

    fn evaluate_less_than_or_equal(
        &self,
        left: &LiteralValue,
        right: &LiteralValue,
    ) -> Option<bool> {
        match (left, right) {
            (LiteralValue::Integer(a), LiteralValue::Integer(b)) => Some(a <= b),
            (LiteralValue::Float(a), LiteralValue::Float(b)) => Some(a <= b),
            (LiteralValue::String(a), LiteralValue::String(b)) => Some(a <= b),
            _ => None,
        }
    }

    /// create a boolean literal expression
    fn make_bool_literal(&self, value: bool) -> BoundExpression {
        BoundExpression::Literal {
            value: LiteralValue::Boolean(value),
            type_: ColumnType::Boolean,
        }
    }

    /// recursively collect all column indices referenced in the plan.
    fn collect_required_columns(&self, plan: &LogicalOperator) -> HashSet<usize> {
        let mut columns = HashSet::new();

        match plan {
            LogicalOperator::Projection(proj) => {
                // collect columns from projection expressions
                for expr in &proj.expressions {
                    columns.extend(self.collect_columns_from_expression(expr));
                }
                // recurse into child
                columns.extend(self.collect_required_columns(&proj.child));
            }
            LogicalOperator::Filter(filter) => {
                // collect columns from filter expression
                columns.extend(self.collect_columns_from_expression(&filter.expression));
                // recurse into child
                columns.extend(self.collect_required_columns(&filter.child));
            }
            LogicalOperator::Get(_get) => {
                // base case: Get doesn't contribute any column requirements
                // the columns it needs to read will be determined by the operators above it
            }
            LogicalOperator::Limit(limit) => {
                // limit doesn't use any columns itself, just pass through to child
                columns.extend(self.collect_required_columns(&limit.child));
            }
            LogicalOperator::Aggregate(agg) => {
                // aggregates read all columns they need (columns from COUNT(col), etc.)
                // for now, collect columns from the child (scan needs to read them)
                for aggregate in &agg.aggregates {
                    if let crate::binder::BoundAggregateExpression::Count { column } = aggregate {
                        columns.insert(column.index);
                    }
                }
                // also collect from child
                columns.extend(self.collect_required_columns(&agg.child));
            }
        }

        columns
    }

    /// recursively traverse a BoundExpression tree to find all ColumnRef nodes.
    /// this handles complex expressions with AND/OR/NOT.
    fn collect_columns_from_expression(&self, expr: &BoundExpression) -> HashSet<usize> {
        let mut columns = HashSet::new();

        match expr {
            // logical operators (recurse on both sides)
            BoundExpression::Or(left, right) | BoundExpression::And(left, right) => {
                columns.extend(self.collect_columns_from_expression(left));
                columns.extend(self.collect_columns_from_expression(right));
            }

            // unary logical operator (recurse on child)
            BoundExpression::Not(inner) => {
                columns.extend(self.collect_columns_from_expression(inner));
            }

            // comparison operators (recurse on both sides)
            BoundExpression::Equal(left, right)
            | BoundExpression::NotEqual(left, right)
            | BoundExpression::GreaterThan(left, right)
            | BoundExpression::GreaterThanOrEqual(left, right)
            | BoundExpression::LessThan(left, right)
            | BoundExpression::LessThanOrEqual(left, right) => {
                columns.extend(self.collect_columns_from_expression(left));
                columns.extend(self.collect_columns_from_expression(right));
            }

            // column reference (this is what we're looking for!)
            BoundExpression::ColumnRef { index, .. } => {
                columns.insert(*index);
            }

            // literals don't reference columns
            BoundExpression::Literal { .. } => {
                // no columns
            }
        }

        columns
    }

    /// apply projection pushdown by updating LogicalGet operators with the
    /// set of required columns.
    fn apply_projection_pushdown(
        &self,
        plan: LogicalOperator,
        required_columns: &HashSet<usize>,
    ) -> LogicalOperator {
        match plan {
            LogicalOperator::Projection(proj) => {
                // recurse into child first
                let optimized_child = self.apply_projection_pushdown(*proj.child, required_columns);

                // get the index mapping from the optimized child
                let index_mapping = self.build_index_mapping(&optimized_child);

                // remap column indices in projection expressions
                let remapped_expressions: Vec<_> = proj
                    .expressions
                    .into_iter()
                    .map(|expr| self.remap_expression(expr, &index_mapping))
                    .collect();

                LogicalOperator::Projection(LogicalProjection {
                    expressions: remapped_expressions,
                    child: Box::new(optimized_child),
                })
            }
            LogicalOperator::Filter(filter) => {
                // recurse into child first
                let optimized_child =
                    self.apply_projection_pushdown(*filter.child, required_columns);

                // get the index mapping from the optimized child
                let index_mapping = self.build_index_mapping(&optimized_child);

                // remap column indices in filter expression
                let remapped_expression = self.remap_expression(filter.expression, &index_mapping);

                LogicalOperator::Filter(LogicalFilter {
                    expression: remapped_expression,
                    child: Box::new(optimized_child),
                })
            }
            LogicalOperator::Get(get) => {
                // this is where we apply the optimization!
                // filter the schema to only include required columns
                // keep the original index in col.index for mapping purposes
                let projected_columns: Vec<_> = get
                    .columns
                    .into_iter()
                    .filter(|col| required_columns.contains(&col.index))
                    .collect();

                LogicalOperator::Get(LogicalGet {
                    file_path: get.file_path,
                    columns: projected_columns,
                    max_rows: get.max_rows, // preserve max_rows from limit pushdown
                })
            }
            LogicalOperator::Limit(limit) => {
                // limit just passes through, optimize child
                let optimized_child =
                    self.apply_projection_pushdown(*limit.child, required_columns);
                LogicalOperator::Limit(LogicalLimit {
                    limit: limit.limit,

                    offset: limit.offset,
                    child: Box::new(optimized_child),
                })
            }
            LogicalOperator::Aggregate(agg) => {
                // aggregate passes through, optimize child
                let optimized_child = self.apply_projection_pushdown(*agg.child, required_columns);

                // remap column indices in aggregates after projection pushdown
                let mapping = self.build_index_mapping(&optimized_child);
                let remapped_aggregates = agg
                    .aggregates
                    .into_iter()
                    .map(|agg_expr| self.remap_aggregate(agg_expr, &mapping))
                    .collect();

                LogicalOperator::Aggregate(crate::planner::LogicalAggregate {
                    aggregates: remapped_aggregates,
                    child: Box::new(optimized_child),
                })
            }
        }
    }

    /// build a mapping from old column indices to new column indices
    /// the Get operator's columns now only contain the projected columns,
    /// and their .index field contains the ORIGINAL index from the file.
    /// we build a mapping: original_index → new_position
    fn build_index_mapping(&self, plan: &LogicalOperator) -> HashMap<usize, usize> {
        match plan {
            LogicalOperator::Get(get) => {
                // build mapping: original index → new position
                get.columns
                    .iter()
                    .enumerate()
                    .map(|(new_pos, col)| (col.index, new_pos))
                    .collect()
            }
            LogicalOperator::Filter(filter) => self.build_index_mapping(&filter.child),
            LogicalOperator::Projection(proj) => self.build_index_mapping(&proj.child),
            LogicalOperator::Limit(limit) => self.build_index_mapping(&limit.child),
            LogicalOperator::Aggregate(agg) => self.build_index_mapping(&agg.child),
        }
    }

    /// remap column indices in a BoundAggregateExpression using the provided mapping
    fn remap_aggregate(
        &self,
        agg: crate::binder::BoundAggregateExpression,
        mapping: &HashMap<usize, usize>,
    ) -> crate::binder::BoundAggregateExpression {
        match agg {
            crate::binder::BoundAggregateExpression::CountStar => {
                crate::binder::BoundAggregateExpression::CountStar
            }
            crate::binder::BoundAggregateExpression::Count { mut column } => {
                // remap the column index
                if let Some(&new_index) = mapping.get(&column.index) {
                    column.index = new_index;
                }
                crate::binder::BoundAggregateExpression::Count { column }
            }
        }
    }

    /// remap column indices in a BoundExpression using the provided mapping
    fn remap_expression(
        &self,
        expr: BoundExpression,
        mapping: &HashMap<usize, usize>,
    ) -> BoundExpression {
        match expr {
            BoundExpression::ColumnRef { name, index, type_ } => {
                // remap the column index
                let new_index = *mapping.get(&index).unwrap_or(&index);
                BoundExpression::ColumnRef {
                    name,
                    index: new_index,
                    type_,
                }
            }
            BoundExpression::Literal { value, type_ } => BoundExpression::Literal { value, type_ },
            BoundExpression::Equal(left, right) => BoundExpression::Equal(
                Box::new(self.remap_expression(*left, mapping)),
                Box::new(self.remap_expression(*right, mapping)),
            ),
            BoundExpression::NotEqual(left, right) => BoundExpression::NotEqual(
                Box::new(self.remap_expression(*left, mapping)),
                Box::new(self.remap_expression(*right, mapping)),
            ),
            BoundExpression::GreaterThan(left, right) => BoundExpression::GreaterThan(
                Box::new(self.remap_expression(*left, mapping)),
                Box::new(self.remap_expression(*right, mapping)),
            ),
            BoundExpression::GreaterThanOrEqual(left, right) => {
                BoundExpression::GreaterThanOrEqual(
                    Box::new(self.remap_expression(*left, mapping)),
                    Box::new(self.remap_expression(*right, mapping)),
                )
            }
            BoundExpression::LessThan(left, right) => BoundExpression::LessThan(
                Box::new(self.remap_expression(*left, mapping)),
                Box::new(self.remap_expression(*right, mapping)),
            ),
            BoundExpression::LessThanOrEqual(left, right) => BoundExpression::LessThanOrEqual(
                Box::new(self.remap_expression(*left, mapping)),
                Box::new(self.remap_expression(*right, mapping)),
            ),
            BoundExpression::And(left, right) => BoundExpression::And(
                Box::new(self.remap_expression(*left, mapping)),
                Box::new(self.remap_expression(*right, mapping)),
            ),
            BoundExpression::Or(left, right) => BoundExpression::Or(
                Box::new(self.remap_expression(*left, mapping)),
                Box::new(self.remap_expression(*right, mapping)),
            ),
            BoundExpression::Not(inner) => {
                BoundExpression::Not(Box::new(self.remap_expression(*inner, mapping)))
            }
        }
    }

    /// push down LIMIT to the scan operator for early termination.
    /// pattern: Limit → [Projection] → [Filter] → Get
    /// only applies when child chain is simple (no joins, aggregations, etc.)
    fn push_down_limit(&self, plan: LogicalOperator) -> LogicalOperator {
        match plan {
            LogicalOperator::Limit(limit_op) => {
                // check if we can push down the limit
                if let Some(max_rows) = self.calculate_max_rows(&limit_op) {
                    // walk down and try to set max_rows on the Get operator
                    let optimized_child = self.set_max_rows_on_get(*limit_op.child, max_rows);
                    LogicalOperator::Limit(LogicalLimit {
                        limit: limit_op.limit,
                        offset: limit_op.offset,
                        child: Box::new(optimized_child),
                    })
                } else {
                    // can't push down, just recurse
                    let optimized_child = self.push_down_limit(*limit_op.child);
                    LogicalOperator::Limit(LogicalLimit {
                        limit: limit_op.limit,
                        offset: limit_op.offset,
                        child: Box::new(optimized_child),
                    })
                }
            }
            LogicalOperator::Projection(proj) => {
                let optimized_child = self.push_down_limit(*proj.child);
                LogicalOperator::Projection(LogicalProjection {
                    expressions: proj.expressions,
                    child: Box::new(optimized_child),
                })
            }
            LogicalOperator::Filter(filter) => {
                let optimized_child = self.push_down_limit(*filter.child);
                LogicalOperator::Filter(LogicalFilter {
                    expression: filter.expression,
                    child: Box::new(optimized_child),
                })
            }
            LogicalOperator::Get(get) => {
                // base case - no recursion needed
                LogicalOperator::Get(get)
            }
            LogicalOperator::Aggregate(agg) => {
                // aggregate should not have limit pushed through it
                let optimized_child = self.push_down_limit(*agg.child);
                LogicalOperator::Aggregate(crate::planner::LogicalAggregate {
                    aggregates: agg.aggregates,
                    child: Box::new(optimized_child),
                })
            }
        }
    }

    /// calculate max_rows = (limit + offset) * safety_factor
    /// safety factor accounts for filter selectivity
    fn calculate_max_rows(&self, limit_op: &LogicalLimit) -> Option<usize> {
        // check if the child chain is simple enough for limit pushdown
        if !self.is_simple_scan_chain(&limit_op.child) {
            return None;
        }

        // calculate total rows needed: limit + offset
        let limit_val = limit_op.limit.unwrap_or(usize::MAX);
        let offset_val = limit_op.offset.unwrap_or(0);

        // avoid overflow
        if limit_val == usize::MAX {
            return None;
        }

        let base_rows = limit_val.saturating_add(offset_val);

        // apply safety factor if there are filters in the chain
        // this accounts for unknown filter selectivity
        let has_filters = self.has_filters_in_chain(&limit_op.child);
        let safety_factor = if has_filters {
            // assume ~10% selectivity (read 10x more rows than needed)
            // still much better than reading entire file
            10
        } else {
            1 // no filters, read exact amount
        };

        Some(base_rows.saturating_mul(safety_factor))
    }

    /// check if the operator chain is simple (only Get, Filter, Projection)
    /// this ensures limit pushdown is safe and beneficial
    fn is_simple_scan_chain(&self, op: &LogicalOperator) -> bool {
        match op {
            LogicalOperator::Get(_) => true,
            LogicalOperator::Filter(filter) => self.is_simple_scan_chain(&filter.child),
            LogicalOperator::Projection(proj) => self.is_simple_scan_chain(&proj.child),
            LogicalOperator::Limit(_) => false, // nested limits - don't optimize
            LogicalOperator::Aggregate(_) => false, // don't push limit through aggregates
        }
    }

    /// check if there are any filters in the operator chain
    fn has_filters_in_chain(&self, op: &LogicalOperator) -> bool {
        match op {
            LogicalOperator::Get(_) => false,
            LogicalOperator::Filter(_) => true,
            LogicalOperator::Projection(proj) => self.has_filters_in_chain(&proj.child),
            LogicalOperator::Limit(_) => false,
            LogicalOperator::Aggregate(_) => false,
        }
    }

    /// set max_rows on the Get operator at the bottom of the chain
    fn set_max_rows_on_get(&self, plan: LogicalOperator, max_rows: usize) -> LogicalOperator {
        match plan {
            LogicalOperator::Get(mut get) => {
                get.max_rows = Some(max_rows);
                LogicalOperator::Get(get)
            }
            LogicalOperator::Filter(filter) => {
                let optimized_child = self.set_max_rows_on_get(*filter.child, max_rows);
                LogicalOperator::Filter(LogicalFilter {
                    expression: filter.expression,
                    child: Box::new(optimized_child),
                })
            }
            LogicalOperator::Projection(proj) => {
                let optimized_child = self.set_max_rows_on_get(*proj.child, max_rows);
                LogicalOperator::Projection(LogicalProjection {
                    expressions: proj.expressions,
                    child: Box::new(optimized_child),
                })
            }
            LogicalOperator::Limit(limit) => {
                // shouldn't happen if is_simple_scan_chain works correctly
                LogicalOperator::Limit(limit)
            }
            LogicalOperator::Aggregate(agg) => {
                // shouldn't happen if is_simple_scan_chain works correctly
                LogicalOperator::Aggregate(agg)
            }
        }
    }
}
