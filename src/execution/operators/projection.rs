use super::{ExecuteResult, PhysicalOperator};
use crate::binder::BoundExpression;
use crate::execution::data_chunk::{DataChunk, Value, Vector};

/// physical operator for projecting columns
/// selects specific columns from input and outputs them
pub struct PhysicalProjection {
    expressions: Vec<BoundExpression>,
}

impl PhysicalProjection {
    pub fn new(expressions: Vec<BoundExpression>) -> Self {
        Self { expressions }
    }
}

impl PhysicalOperator for PhysicalProjection {
    fn execute(&mut self, input: &DataChunk, output: &mut DataChunk) -> ExecuteResult {
        output.reset();

        // build new column vectors for projected columns
        // if input has selection, we materialize only selected rows
        let row_count = input.selected_count();

        let mut projected_columns = Vec::with_capacity(self.expressions.len());

        for expr in &self.expressions {
            match expr {
                BoundExpression::ColumnRef { index, type_, .. } => {
                    // create a new vector for this projected column
                    let mut new_col = Vector::new(type_, row_count);

                    // copy data respecting selection vector
                    for row_idx in 0..row_count {
                        if let Some(value) = input.get_value(*index, row_idx) {
                            new_col.push(value);
                        } else {
                            new_col.push(Value::Null);
                        }
                    }

                    projected_columns.push(new_col);
                }
                // todo: Support computed expressions (e.g., SELECT age + 1)
                _ => {
                    // unsupported expression type for now - create null column
                    let mut new_col = Vector::new(&crate::binder::ColumnType::Varchar, row_count);
                    for _ in 0..row_count {
                        new_col.push(Value::Null);
                    }
                    projected_columns.push(new_col);
                }
            }
        }

        output.columns = projected_columns;
        output.count = row_count;
        output.capacity = row_count;
        // projection materializes the selection, so clear it
        output.clear_selection();

        ExecuteResult::NeedMoreInput
    }

    fn reset(&mut self) {
        // no state to reset
    }
}
