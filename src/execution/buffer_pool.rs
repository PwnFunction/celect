use crate::binder::ColumnType;
use crate::execution::data_chunk::DataChunk;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

/// buffer pool for reusing DataChunk instances to avoid allocation overhead
/// thread-safe pool that stores pre-allocated chunks for reuse
pub struct BufferPool {
    /// pool of free chunks (thread-safe)
    pool: Mutex<Vec<DataChunk>>,
    /// maximum capacity of the pool
    capacity: usize,
    /// standard chunk size (number of rows per chunk)
    chunk_size: usize,
}

impl BufferPool {
    /// create a new buffer pool with specified capacity and chunk size
    pub fn new(capacity: usize, chunk_size: usize) -> Self {
        Self {
            pool: Mutex::new(Vec::with_capacity(capacity)),
            capacity,
            chunk_size,
        }
    }

    /// get a chunk from the pool, or create a new one if pool is empty
    pub fn get_chunk(&self) -> DataChunk {
        let mut pool = self.pool.lock().unwrap();

        if let Some(mut chunk) = pool.pop() {
            // reuse existing chunk - reset it but keep capacity
            chunk.reset();
            chunk
        } else {
            // pool is empty - create empty chunk
            // note: Schema will be set by the caller
            DataChunk::empty()
        }
    }

    /// get a chunk with the specified schema from the pool
    pub fn get_chunk_with_schema(&self, column_types: Vec<ColumnType>) -> DataChunk {
        let mut pool = self.pool.lock().unwrap();

        if let Some(mut chunk) = pool.pop() {
            // reuse existing chunk - reset and set up new schema
            chunk.reset();
            chunk.columns = column_types
                .iter()
                .map(|col_type| {
                    crate::execution::data_chunk::Vector::new(col_type, self.chunk_size)
                })
                .collect();
            chunk.capacity = self.chunk_size;
            chunk
        } else {
            // pool is empty - create new chunk with schema
            DataChunk::new(column_types, self.chunk_size)
        }
    }

    /// return a chunk to the pool (if there's room)
    pub fn return_chunk(&self, mut chunk: DataChunk) {
        let mut pool = self.pool.lock().unwrap();

        // only return if pool isn't full
        if pool.len() < self.capacity {
            // reset the chunk before returning
            chunk.reset();
            pool.push(chunk);
        }
        // if pool is full, chunk is dropped here (freed)
    }
}

/// raii wrapper around DataChunk that automatically returns it to the pool when dropped
/// allows using DataChunk like a normal value while ensuring it's returned to the pool
pub struct PooledDataChunk {
    /// the actual chunk (Option to allow taking ownership in Drop)
    chunk: Option<DataChunk>,
    /// reference to the pool to return the chunk to
    pool: Arc<BufferPool>,
}

impl PooledDataChunk {
    /// create a new PooledDataChunk from a pool
    pub fn new(pool: Arc<BufferPool>) -> Self {
        let chunk = pool.get_chunk();
        Self {
            chunk: Some(chunk),
            pool,
        }
    }
}

impl Drop for PooledDataChunk {
    fn drop(&mut self) {
        if let Some(chunk) = self.chunk.take() {
            self.pool.return_chunk(chunk);
        }
    }
}

impl Deref for PooledDataChunk {
    type Target = DataChunk;

    fn deref(&self) -> &Self::Target {
        self.chunk.as_ref().unwrap()
    }
}

impl DerefMut for PooledDataChunk {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.chunk.as_mut().unwrap()
    }
}
