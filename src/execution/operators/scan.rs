use super::{ExecuteResult, PhysicalOperator};
use crate::binder::{ColumnType, Schema};
use crate::execution::data_chunk::{DataChunk, Value};
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, channel};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::thread::{JoinHandle, spawn};

/// physical operator for scanning CSV files
/// reads CSV file and produces DataChunks in columnar format
/// uses parallel workers with csv crate for robust parsing
pub struct PhysicalScan {
    file_path: PathBuf,
    schema: Schema,
    finished: bool,
    max_rows: Option<usize>, // maximum rows to read (from LIMIT pushdown)
    rows_read: usize,        // track rows read so far
    // parallel CSV scanning fields
    receiver: Option<Receiver<DataChunk>>,
    handles: Option<Vec<JoinHandle<()>>>,
    // single-threaded CSV scanning fields
    csv_reader: Option<csv::Reader<File>>,
}

impl PhysicalScan {
    pub fn new(
        file_path: PathBuf,
        schema: Schema,
        _projected_columns: Option<Vec<usize>>,
        max_rows: Option<usize>,
    ) -> Self {
        Self {
            file_path,
            schema,
            finished: false,
            max_rows,
            rows_read: 0,
            receiver: None,
            handles: None,
            csv_reader: None,
        }
    }

    /// determine if we should use single-threaded scan
    fn should_use_single_threaded(&self) -> bool {
        // use single-threaded for small limits (< 5000 rows)
        // this allows immediate early termination with no coordination overhead
        if let Some(max_rows) = self.max_rows {
            max_rows < 5000
        } else {
            false
        }
    }

    /// parse a CSV value and convert it to the appropriate type
    fn parse_value(value: &str, column_type: &ColumnType) -> Value {
        let trimmed = value.trim();

        if trimmed.is_empty() {
            return Value::Null;
        }

        match column_type {
            ColumnType::Integer => trimmed
                .parse::<i64>()
                .map(Value::Integer)
                .unwrap_or(Value::Null),
            ColumnType::Float => trimmed
                .parse::<f64>()
                .map(Value::Float)
                .unwrap_or(Value::Null),
            ColumnType::Boolean => {
                if trimmed.eq_ignore_ascii_case("true") {
                    Value::Boolean(true)
                } else if trimmed.eq_ignore_ascii_case("false") {
                    Value::Boolean(false)
                } else {
                    Value::Null
                }
            }
            ColumnType::Varchar => Value::Varchar(trimmed.to_string()),
            ColumnType::Null => Value::Null,
        }
    }

    /// single-threaded CSV scan with early termination
    /// used for small LIMIT values to minimize overhead
    fn execute_single_threaded(&mut self, output: &mut DataChunk) -> ExecuteResult {
        // initialize CSV reader on first call
        if self.csv_reader.is_none() {
            match csv::Reader::from_path(&self.file_path) {
                Ok(reader) => self.csv_reader = Some(reader),
                Err(_) => {
                    self.finished = true;
                    output.reset();
                    return ExecuteResult::Finished;
                }
            }
        }

        let reader = self.csv_reader.as_mut().unwrap();
        let column_types: Vec<ColumnType> = self
            .schema
            .columns
            .iter()
            .map(|c| c.type_.clone())
            .collect();
        let mut chunk = DataChunk::new(column_types.clone(), DataChunk::STANDARD_VECTOR_SIZE);

        // read rows until chunk is full or limit is reached
        for result in reader.records() {
            // check if we've hit the limit
            if let Some(max_rows) = self.max_rows {
                if self.rows_read >= max_rows {
                    self.finished = true;
                    if chunk.count > 0 {
                        *output = chunk;
                        return ExecuteResult::Finished;
                    } else {
                        output.reset();
                        return ExecuteResult::Finished;
                    }
                }
            }

            match result {
                Ok(record) => {
                    let mut row = Vec::new();
                    for col in self.schema.columns.iter() {
                        let file_index = col.index;
                        if let Some(field) = record.get(file_index) {
                            let value = Self::parse_value(field, &col.type_);
                            row.push(value);
                        } else {
                            row.push(Value::Null);
                        }
                    }

                    chunk.append_row(row);
                    self.rows_read += 1;

                    // chunk is full, send it back
                    if chunk.count >= DataChunk::STANDARD_VECTOR_SIZE {
                        *output = chunk;
                        return ExecuteResult::NeedMoreInput;
                    }
                }
                Err(_) => {
                    // error reading, stop
                    self.finished = true;
                    if chunk.count > 0 {
                        *output = chunk;
                        return ExecuteResult::Finished;
                    } else {
                        output.reset();
                        return ExecuteResult::Finished;
                    }
                }
            }
        }

        // eof reached
        self.finished = true;
        if chunk.count > 0 {
            *output = chunk;
            ExecuteResult::Finished
        } else {
            output.reset();
            ExecuteResult::Finished
        }
    }

    /// parallel CSV worker that reads a specific byte range
    /// now supports early termination via shared atomic counter
    fn parallel_csv_worker(
        path: PathBuf,
        start: u64,
        end: u64,
        sender: std::sync::mpsc::Sender<DataChunk>,
        schema: Schema,
        is_first: bool,
        rows_counter: Option<Arc<AtomicUsize>>,
        max_rows: Option<usize>,
    ) {
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(_) => return,
        };

        let mut reader = BufReader::new(file);

        // seek to start position
        if reader.seek(SeekFrom::Start(start)).is_err() {
            return;
        }

        // if not first thread, skip partial line to align to row boundary
        if !is_first {
            let mut line = String::new();
            if reader.read_line(&mut line).is_err() {
                return;
            }
            // discarded partial line
        }

        // if first thread, skip header
        if is_first {
            let mut header = String::new();
            if reader.read_line(&mut header).is_err() {
                return;
            }
        }

        // track actual position
        let mut current_pos = reader.stream_position().unwrap_or(start);

        let column_types: Vec<ColumnType> =
            schema.columns.iter().map(|c| c.type_.clone()).collect();
        let mut chunk = DataChunk::new(column_types.clone(), DataChunk::STANDARD_VECTOR_SIZE);

        // read lines until we exceed our byte range
        let mut line = String::new();
        loop {
            // early termination check for LIMIT
            if let (Some(counter), Some(limit)) = (&rows_counter, max_rows) {
                if counter.load(Ordering::Relaxed) >= limit {
                    // limit reached, stop reading
                    break;
                }
            }

            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break, // eof
                Ok(bytes_read) => {
                    current_pos += bytes_read as u64;

                    if line.trim().is_empty() {
                        continue;
                    }

                    // simple CSV parsing (split by comma)
                    let fields: Vec<&str> = line.trim().split(',').collect();

                    let mut row = Vec::new();
                    for col in schema.columns.iter() {
                        let file_index = col.index;
                        if file_index < fields.len() {
                            let value = Self::parse_value(fields[file_index], &col.type_);
                            row.push(value);
                        } else {
                            row.push(Value::Null);
                        }
                    }

                    chunk.append_row(row);

                    // update global counter
                    if let Some(counter) = &rows_counter {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }

                    if chunk.count >= DataChunk::STANDARD_VECTOR_SIZE {
                        if sender.send(chunk).is_err() {
                            return;
                        }
                        chunk =
                            DataChunk::new(column_types.clone(), DataChunk::STANDARD_VECTOR_SIZE);
                    }

                    // stop when we've exceeded our range
                    if current_pos >= end {
                        break;
                    }
                }
                Err(_) => break,
            }
        }

        if chunk.count > 0 {
            let _ = sender.send(chunk);
        }
    }

    /// spawn parallel worker threads for CSV processing
    fn spawn_workers(&mut self) -> std::io::Result<()> {
        let file_size = std::fs::metadata(&self.file_path)?.len();

        // use single-threaded mode for small files (< 1MB) to avoid boundary issues
        let num_threads = if file_size < 1_000_000 {
            1
        } else {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        };

        let chunk_size = file_size / num_threads as u64;

        let (chunk_tx, chunk_rx) = channel();

        // create shared atomic counter for LIMIT pushdown
        let rows_counter = if self.max_rows.is_some() {
            Some(Arc::new(AtomicUsize::new(0)))
        } else {
            None
        };

        let mut handles = Vec::new();

        for i in 0..num_threads {
            let start = i as u64 * chunk_size;
            let end = if i == num_threads - 1 {
                file_size
            } else {
                (i + 1) as u64 * chunk_size
            };

            let path = self.file_path.clone();
            let schema = self.schema.clone();
            let sender = chunk_tx.clone();
            let is_first = i == 0;
            let counter = rows_counter.clone();
            let max_rows = self.max_rows;

            let handle = spawn(move || {
                Self::parallel_csv_worker(
                    path, start, end, sender, schema, is_first, counter, max_rows,
                );
            });

            handles.push(handle);
        }

        drop(chunk_tx);

        self.receiver = Some(chunk_rx);
        self.handles = Some(handles);

        Ok(())
    }
}

impl PhysicalOperator for PhysicalScan {
    fn execute(&mut self, _input: &DataChunk, output: &mut DataChunk) -> ExecuteResult {
        if self.finished {
            output.reset();
            return ExecuteResult::Finished;
        }

        // choose execution strategy based on max_rows
        if self.should_use_single_threaded() {
            // single-threaded scan for small limits
            return self.execute_single_threaded(output);
        }

        // parallel scan for large limits or no limit
        // spawn workers on first call if not already done
        if self.receiver.is_none() {
            if let Err(_) = self.spawn_workers() {
                self.finished = true;
                return ExecuteResult::Finished;
            }
        }

        // receive a chunk from the channel (blocks until chunk available or channel closed)
        let receiver = self.receiver.as_ref().unwrap();
        match receiver.recv() {
            Ok(chunk) => {
                // copy chunk to output
                *output = chunk;
                ExecuteResult::NeedMoreInput
            }
            Err(_) => {
                // channel closed - all workers finished
                self.finished = true;
                output.reset();
                ExecuteResult::Finished
            }
        }
    }

    fn reset(&mut self) {
        self.finished = false;
        self.rows_read = 0;
        // clean up parallel resources
        self.receiver = None;
        // join worker threads if they exist
        if let Some(handles) = self.handles.take() {
            for handle in handles {
                let _ = handle.join(); // wait for threads to finish
            }
        }
        // clean up single-threaded resources
        self.csv_reader = None;
    }
}
