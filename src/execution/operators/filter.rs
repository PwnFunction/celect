use super::{ExecuteResult, PhysicalOperator};
use crate::binder::BoundExpression;
use crate::execution::data_chunk::{DataChunk, SelectionVector, Value};

/// physical operator for filtering rows based on a predicate
/// evaluates the predicate on each row and only outputs matching rows
pub struct PhysicalFilter {
    predicate: BoundExpression,
}

impl PhysicalFilter {
    pub fn new(predicate: BoundExpression) -> Self {
        Self { predicate }
    }

    /// evaluate the predicate on a specific row
    fn evaluate_predicate(&self, chunk: &DataChunk, row_idx: usize) -> bool {
        match self.evaluate_expression(&self.predicate, chunk, row_idx) {
            Some(Value::Boolean(b)) => b,
            _ => false, // null or non-boolean -> false
        }
    }

    /// recursively evaluate an expression on a specific row
    fn evaluate_expression(
        &self,
        expr: &BoundExpression,
        chunk: &DataChunk,
        row_idx: usize,
    ) -> Option<Value> {
        match expr {
            BoundExpression::ColumnRef { index, .. } => chunk.get_value(*index, row_idx),
            BoundExpression::Literal { value, .. } => Some(match value {
                crate::parser::LiteralValue::Integer(i) => Value::Integer(*i),
                crate::parser::LiteralValue::Float(f) => Value::Float(*f),
                crate::parser::LiteralValue::String(s) => Value::Varchar(s.clone()),
                crate::parser::LiteralValue::Boolean(b) => Value::Boolean(*b),
                crate::parser::LiteralValue::Null => Value::Null,
            }),
            BoundExpression::Equal(left, right) => {
                let left_val = self.evaluate_expression(left, chunk, row_idx)?;
                let right_val = self.evaluate_expression(right, chunk, row_idx)?;
                Some(Value::Boolean(self.compare_equal(&left_val, &right_val)))
            }
            BoundExpression::NotEqual(left, right) => {
                let left_val = self.evaluate_expression(left, chunk, row_idx)?;
                let right_val = self.evaluate_expression(right, chunk, row_idx)?;
                Some(Value::Boolean(!self.compare_equal(&left_val, &right_val)))
            }
            BoundExpression::GreaterThan(left, right) => {
                let left_val = self.evaluate_expression(left, chunk, row_idx)?;
                let right_val = self.evaluate_expression(right, chunk, row_idx)?;
                Some(Value::Boolean(self.compare_greater(&left_val, &right_val)))
            }
            BoundExpression::GreaterThanOrEqual(left, right) => {
                let left_val = self.evaluate_expression(left, chunk, row_idx)?;
                let right_val = self.evaluate_expression(right, chunk, row_idx)?;
                Some(Value::Boolean(
                    self.compare_greater_equal(&left_val, &right_val),
                ))
            }
            BoundExpression::LessThan(left, right) => {
                let left_val = self.evaluate_expression(left, chunk, row_idx)?;
                let right_val = self.evaluate_expression(right, chunk, row_idx)?;
                Some(Value::Boolean(self.compare_less(&left_val, &right_val)))
            }
            BoundExpression::LessThanOrEqual(left, right) => {
                let left_val = self.evaluate_expression(left, chunk, row_idx)?;
                let right_val = self.evaluate_expression(right, chunk, row_idx)?;
                Some(Value::Boolean(
                    self.compare_less_equal(&left_val, &right_val),
                ))
            }
            BoundExpression::And(left, right) => {
                let left_val = self.evaluate_expression(left, chunk, row_idx)?;
                let right_val = self.evaluate_expression(right, chunk, row_idx)?;
                match (left_val, right_val) {
                    (Value::Boolean(l), Value::Boolean(r)) => Some(Value::Boolean(l && r)),
                    _ => None,
                }
            }
            BoundExpression::Or(left, right) => {
                let left_val = self.evaluate_expression(left, chunk, row_idx)?;
                let right_val = self.evaluate_expression(right, chunk, row_idx)?;
                match (left_val, right_val) {
                    (Value::Boolean(l), Value::Boolean(r)) => Some(Value::Boolean(l || r)),
                    _ => None,
                }
            }
            BoundExpression::Not(inner) => {
                let val = self.evaluate_expression(inner, chunk, row_idx)?;
                match val {
                    Value::Boolean(b) => Some(Value::Boolean(!b)),
                    _ => None,
                }
            }
        }
    }

    fn compare_equal(&self, left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => l == r,
            (Value::Float(l), Value::Float(r)) => l == r,
            (Value::Integer(l), Value::Float(r)) => (*l as f64) == *r,
            (Value::Float(l), Value::Integer(r)) => *l == (*r as f64),
            (Value::Boolean(l), Value::Boolean(r)) => l == r,
            (Value::Varchar(l), Value::Varchar(r)) => l == r,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }

    fn compare_greater(&self, left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => l > r,
            (Value::Float(l), Value::Float(r)) => l > r,
            (Value::Integer(l), Value::Float(r)) => (*l as f64) > *r,
            (Value::Float(l), Value::Integer(r)) => *l > (*r as f64),
            (Value::Varchar(l), Value::Varchar(r)) => l > r,
            _ => false,
        }
    }

    fn compare_greater_equal(&self, left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => l >= r,
            (Value::Float(l), Value::Float(r)) => l >= r,
            (Value::Integer(l), Value::Float(r)) => (*l as f64) >= *r,
            (Value::Float(l), Value::Integer(r)) => *l >= (*r as f64),
            (Value::Varchar(l), Value::Varchar(r)) => l >= r,
            _ => false,
        }
    }

    fn compare_less(&self, left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => l < r,
            (Value::Float(l), Value::Float(r)) => l < r,
            (Value::Integer(l), Value::Float(r)) => (*l as f64) < *r,
            (Value::Float(l), Value::Integer(r)) => *l < (*r as f64),
            (Value::Varchar(l), Value::Varchar(r)) => l < r,
            _ => false,
        }
    }

    fn compare_less_equal(&self, left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => l <= r,
            (Value::Float(l), Value::Float(r)) => l <= r,
            (Value::Integer(l), Value::Float(r)) => (*l as f64) <= *r,
            (Value::Float(l), Value::Integer(r)) => *l <= (*r as f64),
            (Value::Varchar(l), Value::Varchar(r)) => l <= r,
            _ => false,
        }
    }
}

impl PhysicalOperator for PhysicalFilter {
    fn execute(&mut self, input: &DataChunk, output: &mut DataChunk) -> ExecuteResult {
        output.reset();

        // build selection vector instead of copying rows (zero-copy filtering)
        let mut selection = SelectionVector::new(input.count);

        // evaluate predicate for each row
        for row_idx in 0..input.count {
            if self.evaluate_predicate(input, row_idx) {
                selection.push(row_idx as u16);
            }
        }

        // clone input chunk but with selection vector
        // this is zero-copy: we just reference the same data with different indices
        output.columns = input.columns.clone();
        output.count = input.count;
        output.capacity = input.capacity;
        output.set_selection(selection);

        ExecuteResult::NeedMoreInput
    }

    fn reset(&mut self) {
        // no state to reset
    }
}
