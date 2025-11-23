use super::{ExecuteResult, PhysicalOperator};
use crate::binder::{BoundAggregateExpression, ColumnType};
use crate::execution::data_chunk::{DataChunk, Value};

/// physical operator for ungrouped aggregation (e.g., SELECT COUNT(*) FROM table)
/// consumes all input rows and produces a single output row with aggregate results
pub struct PhysicalUngroupedAggregate {
    aggregates: Vec<BoundAggregateExpression>,
    states: Vec<i64>, // one counter per aggregate
    finished: bool,
    has_emitted: bool, // track if we've already emitted the result
}

impl PhysicalUngroupedAggregate {
    pub fn new(aggregates: Vec<BoundAggregateExpression>) -> Self {
        let num_aggregates = aggregates.len();
        Self {
            aggregates,
            states: vec![0; num_aggregates],
            finished: false,
            has_emitted: false,
        }
    }

    /// update aggregate states with a new chunk of data
    fn update_states(&mut self, chunk: &DataChunk) {
        for (i, aggregate) in self.aggregates.iter().enumerate() {
            match aggregate {
                BoundAggregateExpression::CountStar => {
                    // count(*): just count selected rows
                    self.states[i] += chunk.selected_count() as i64;
                }
                BoundAggregateExpression::Count { column } => {
                    // count(column): count non-NULL values
                    let column_idx = column.index;

                    // get the vector for this column
                    if column_idx >= chunk.column_count() {
                        continue; // column not in chunk, skip
                    }

                    let vector = &chunk.columns[column_idx];
                    let validity = vector.validity();

                    // count non-NULL values using the validity bitmap
                    let count = validity.count_valid(chunk.selected_count());

                    self.states[i] += count as i64;
                }
            }
        }
    }

    /// emit the final aggregate results as a single-row DataChunk
    fn emit_result(&self) -> DataChunk {
        // create output schema: one INTEGER column per aggregate
        let output_types = vec![ColumnType::Integer; self.aggregates.len()];
        let mut output_chunk = DataChunk::new(output_types, 1);

        // create a single row with all aggregate results
        let mut row = Vec::new();
        for &state in &self.states {
            row.push(Value::Integer(state));
        }

        output_chunk.append_row(row);
        output_chunk
    }
}

impl PhysicalOperator for PhysicalUngroupedAggregate {
    fn execute(&mut self, input: &DataChunk, output: &mut DataChunk) -> ExecuteResult {
        if self.finished {
            output.reset();
            return ExecuteResult::Finished;
        }

        // if we've already emitted the result, we're done
        if self.has_emitted {
            output.reset();
            self.finished = true;
            return ExecuteResult::Finished;
        }

        // if input is empty, we're at the end of data - emit result
        if input.is_empty() {
            let result = self.emit_result();
            *output = result;
            self.has_emitted = true;
            self.finished = true; // mark as finished after emitting
            return ExecuteResult::Finished;
        }

        // update aggregate states with this chunk
        self.update_states(input);

        // keep consuming input (don't emit yet)
        output.reset();
        ExecuteResult::NeedMoreInput
    }

    fn reset(&mut self) {
        self.states.fill(0);
        self.finished = false;
        self.has_emitted = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::Column;

    fn create_test_chunk(rows: usize, has_nulls: bool) -> DataChunk {
        let mut chunk = DataChunk::new(vec![ColumnType::Integer], DataChunk::STANDARD_VECTOR_SIZE);
        for i in 0..rows {
            if has_nulls && i % 2 == 0 {
                chunk.append_row(vec![Value::Null]);
            } else {
                chunk.append_row(vec![Value::Integer(i as i64)]);
            }
        }
        chunk
    }

    #[test]
    fn test_count_star() {
        let aggregates = vec![BoundAggregateExpression::CountStar];
        let mut agg_op = PhysicalUngroupedAggregate::new(aggregates);

        // process first chunk
        let chunk1 = create_test_chunk(10, false);
        let mut output = DataChunk::new(vec![ColumnType::Integer], 1);
        let result = agg_op.execute(&chunk1, &mut output);
        assert_eq!(result, ExecuteResult::NeedMoreInput);
        assert!(output.is_empty());

        // process second chunk
        let chunk2 = create_test_chunk(15, false);
        let result = agg_op.execute(&chunk2, &mut output);
        assert_eq!(result, ExecuteResult::NeedMoreInput);
        assert!(output.is_empty());

        // signal end of input
        let empty_chunk = DataChunk::empty();
        let result = agg_op.execute(&empty_chunk, &mut output);
        assert_eq!(result, ExecuteResult::Finished);
        assert_eq!(output.selected_count(), 1);
        assert_eq!(output.get_value(0, 0), Some(Value::Integer(25))); // 10 + 15
    }

    #[test]
    fn test_count_column_with_nulls() {
        let column = Column {
            name: "test".to_string(),
            type_: ColumnType::Integer,
            index: 0,
        };
        let aggregates = vec![BoundAggregateExpression::Count { column }];
        let mut agg_op = PhysicalUngroupedAggregate::new(aggregates);

        // process chunk with NULL values (every other row is NULL)
        let chunk = create_test_chunk(10, true);
        let mut output = DataChunk::new(vec![ColumnType::Integer], 1);
        let result = agg_op.execute(&chunk, &mut output);
        assert_eq!(result, ExecuteResult::NeedMoreInput);

        // signal end of input
        let empty_chunk = DataChunk::empty();
        let result = agg_op.execute(&empty_chunk, &mut output);
        assert_eq!(result, ExecuteResult::Finished);
        assert_eq!(output.selected_count(), 1);
        assert_eq!(output.get_value(0, 0), Some(Value::Integer(5))); // 10 rows, 5 non-NULL
    }

    #[test]
    fn test_multiple_aggregates() {
        let column = Column {
            name: "test".to_string(),
            type_: ColumnType::Integer,
            index: 0,
        };
        let aggregates = vec![
            BoundAggregateExpression::CountStar,
            BoundAggregateExpression::Count { column },
        ];
        let mut agg_op = PhysicalUngroupedAggregate::new(aggregates);

        // process chunk with NULL values
        let chunk = create_test_chunk(10, true);
        let mut output = DataChunk::new(vec![ColumnType::Integer, ColumnType::Integer], 1);
        agg_op.execute(&chunk, &mut output);

        // signal end of input
        let empty_chunk = DataChunk::empty();
        let result = agg_op.execute(&empty_chunk, &mut output);
        assert_eq!(result, ExecuteResult::Finished);
        assert_eq!(output.selected_count(), 1);
        assert_eq!(output.get_value(0, 0), Some(Value::Integer(10))); // count(*) = 10
        assert_eq!(output.get_value(1, 0), Some(Value::Integer(5))); // count(col) = 5
    }
}
