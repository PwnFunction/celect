use super::{ExecuteResult, PhysicalOperator};
use crate::execution::data_chunk::DataChunk;

/// physical operator for LIMIT and OFFSET
/// skips the first N rows (OFFSET) and returns at most M rows (LIMIT)
pub struct PhysicalLimit {
    limit: Option<usize>,
    offset: Option<usize>,

    // runtime state
    offset_remaining: usize, // how many rows we still need to skip
    rows_emitted: usize,     // how many rows we've returned so far
}

impl PhysicalLimit {
    pub fn new(limit: Option<usize>, offset: Option<usize>) -> Self {
        Self {
            limit,
            offset_remaining: offset.unwrap_or(0),
            offset,
            rows_emitted: 0,
        }
    }
}

impl PhysicalOperator for PhysicalLimit {
    fn execute(&mut self, input: &DataChunk, output: &mut DataChunk) -> ExecuteResult {
        // if we've already hit the limit, we're done
        if let Some(limit) = self.limit {
            if self.rows_emitted >= limit {
                return ExecuteResult::Finished;
            }
        }

        // if input is empty, we're done
        if input.is_empty() {
            return ExecuteResult::Finished;
        }

        // clone input to output first (we'll apply offset/limit via selection vector)
        *output = input.clone();

        // apply OFFSET (skip rows)
        if self.offset_remaining > 0 {
            let available_rows = output.selected_count();
            if available_rows <= self.offset_remaining {
                // skip entire chunk
                self.offset_remaining -= available_rows;
                output.reset(); // clear output
                return ExecuteResult::NeedMoreInput;
            } else {
                // skip partial chunk
                let skip = self.offset_remaining;
                output.apply_offset(skip);
                self.offset_remaining = 0;
            }
        }

        // apply LIMIT (truncate rows)
        if let Some(limit) = self.limit {
            let remaining_quota = limit - self.rows_emitted;
            let available_rows = output.selected_count();

            if available_rows > remaining_quota {
                output.truncate(remaining_quota);
            }

            self.rows_emitted += output.selected_count();

            // if we've hit the limit, signal that we're done
            if self.rows_emitted >= limit {
                return ExecuteResult::Finished;
            }
        }

        ExecuteResult::NeedMoreInput
    }

    fn reset(&mut self) {
        self.offset_remaining = self.offset.unwrap_or(0);
        self.rows_emitted = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::ColumnType;
    use crate::execution::data_chunk::{DataChunk, Value};

    fn create_test_chunk(rows: Vec<i64>) -> DataChunk {
        let mut chunk = DataChunk::new(vec![ColumnType::Integer], DataChunk::STANDARD_VECTOR_SIZE);
        for val in rows {
            chunk.append_row(vec![Value::Integer(val)]);
        }
        chunk
    }

    #[test]
    fn test_limit_only() {
        let input = create_test_chunk(vec![1, 2, 3, 4, 5]);
        let mut output = DataChunk::new(vec![ColumnType::Integer], DataChunk::STANDARD_VECTOR_SIZE);

        let mut limit_op = PhysicalLimit::new(Some(3), None);

        let result = limit_op.execute(&input, &mut output);
        assert_eq!(output.selected_count(), 3);
        assert_eq!(result, ExecuteResult::Finished); // hit limit
    }

    #[test]
    fn test_offset_only() {
        let input = create_test_chunk(vec![1, 2, 3, 4, 5]);
        let mut output = DataChunk::new(vec![ColumnType::Integer], DataChunk::STANDARD_VECTOR_SIZE);

        let mut limit_op = PhysicalLimit::new(None, Some(2));

        let result = limit_op.execute(&input, &mut output);
        assert_eq!(output.selected_count(), 3); // skipped first 2, got 3 remaining
        assert_eq!(result, ExecuteResult::NeedMoreInput);
    }

    #[test]
    fn test_limit_and_offset() {
        let input = create_test_chunk(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let mut output = DataChunk::new(vec![ColumnType::Integer], DataChunk::STANDARD_VECTOR_SIZE);

        let mut limit_op = PhysicalLimit::new(Some(3), Some(5));

        let result = limit_op.execute(&input, &mut output);
        assert_eq!(output.selected_count(), 3); // skip 5, return 3
        assert_eq!(result, ExecuteResult::Finished); // hit limit
    }

    #[test]
    fn test_offset_larger_than_chunk() {
        let input = create_test_chunk(vec![1, 2, 3]);
        let mut output = DataChunk::new(vec![ColumnType::Integer], DataChunk::STANDARD_VECTOR_SIZE);

        let mut limit_op = PhysicalLimit::new(None, Some(10));

        let result = limit_op.execute(&input, &mut output);
        assert_eq!(output.selected_count(), 0); // all rows skipped
        assert_eq!(result, ExecuteResult::NeedMoreInput); // still waiting for more data
    }

    #[test]
    fn test_limit_larger_than_chunk() {
        let input = create_test_chunk(vec![1, 2, 3]);
        let mut output = DataChunk::new(vec![ColumnType::Integer], DataChunk::STANDARD_VECTOR_SIZE);

        let mut limit_op = PhysicalLimit::new(Some(10), None);

        let result = limit_op.execute(&input, &mut output);
        assert_eq!(output.selected_count(), 3); // returns all 3 rows
        assert_eq!(result, ExecuteResult::NeedMoreInput); // still below limit
    }

    #[test]
    fn test_multi_chunk_with_offset() {
        let mut limit_op = PhysicalLimit::new(Some(5), Some(8));

        // first chunk: 5 rows, all skipped (offset = 8)
        let input1 = create_test_chunk(vec![1, 2, 3, 4, 5]);
        let mut output1 =
            DataChunk::new(vec![ColumnType::Integer], DataChunk::STANDARD_VECTOR_SIZE);
        let result1 = limit_op.execute(&input1, &mut output1);
        assert_eq!(output1.selected_count(), 0); // all skipped
        assert_eq!(result1, ExecuteResult::NeedMoreInput);

        // second chunk: 5 rows, skip 3, return 2 (offset_remaining = 3 after first chunk)
        let input2 = create_test_chunk(vec![6, 7, 8, 9, 10]);
        let mut output2 =
            DataChunk::new(vec![ColumnType::Integer], DataChunk::STANDARD_VECTOR_SIZE);
        let result2 = limit_op.execute(&input2, &mut output2);
        assert_eq!(output2.selected_count(), 2); // skip 3, return 2
        assert_eq!(result2, ExecuteResult::NeedMoreInput); // still below limit

        // third chunk: need 3 more rows to hit limit of 5
        let input3 = create_test_chunk(vec![11, 12, 13, 14, 15]);
        let mut output3 =
            DataChunk::new(vec![ColumnType::Integer], DataChunk::STANDARD_VECTOR_SIZE);
        let result3 = limit_op.execute(&input3, &mut output3);
        assert_eq!(output3.selected_count(), 3); // return 3 to hit limit
        assert_eq!(result3, ExecuteResult::Finished); // hit limit of 5
    }
}
