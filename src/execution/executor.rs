use super::buffer_pool::BufferPool;
use super::data_chunk::DataChunk;
use super::operators::{ExecuteResult, PhysicalOperator};
use crate::binder::ColumnType;
use std::sync::Arc;

/// pipeline executor that drives push-based execution
/// coordinates data flow between physical operators
pub struct PipelineExecutor {
    operators: Vec<Box<dyn PhysicalOperator>>,
    schemas: Vec<Vec<ColumnType>>,
    buffer_pool: Arc<BufferPool>,
}

impl PipelineExecutor {
    /// create a new pipeline executor with the given operators
    /// operators are in execution order: [Source, Filter, Projection, ...]
    pub fn new(operators: Vec<Box<dyn PhysicalOperator>>, schema: Vec<Vec<ColumnType>>) -> Self {
        // create buffer pool for reusing chunks during execution
        let buffer_pool = Arc::new(BufferPool::new(100, DataChunk::STANDARD_VECTOR_SIZE));

        Self {
            operators,
            schemas: schema,
            buffer_pool,
        }
    }

    /// execute the entire pipeline and collect results
    pub fn execute(&mut self) -> Vec<DataChunk> {
        let mut results = Vec::new();
        let mut source_finished = false;

        loop {
            // get buffers from pool for this iteration
            let mut buffers: Vec<DataChunk> = self
                .schemas
                .iter()
                .map(|schema| self.buffer_pool.get_chunk_with_schema(schema.clone()))
                .collect();

            // source operator produces data into buffer[0]
            let result = self.operators[0].execute(&DataChunk::empty(), &mut buffers[0]);

            if buffers[0].is_empty() {
                if source_finished {
                    // already did finalization pass, break
                    for buffer in buffers {
                        self.buffer_pool.return_chunk(buffer);
                    }
                    break;
                }
                // source finished, but we need to pass empty chunk through pipeline
                // to let aggregates finalize
                source_finished = true;
            }

            // push through the pipeline
            for i in 1..self.operators.len() {
                let (left, right) = buffers.split_at_mut(i);
                let input = &left[i - 1];
                let output = &mut right[0];
                self.operators[i].execute(input, output);
            }

            // collect final output (last buffer)
            if let Some(last_buffer) = buffers.last() {
                if !last_buffer.is_empty() {
                    results.push(last_buffer.clone());
                }
            }

            // return buffers to pool
            for buffer in buffers {
                self.buffer_pool.return_chunk(buffer);
            }

            if result == ExecuteResult::Finished && source_finished {
                break;
            }
        }

        results
    }

    /// reset all operators (for re-execution)
    pub fn reset(&mut self) {
        for op in &mut self.operators {
            op.reset();
        }
    }
}
