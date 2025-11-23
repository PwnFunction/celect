use super::bitmap::Bitmap;
use crate::binder::ColumnType;

/// selection vector: stores indices of selected rows for zero-copy filtering
/// uses u16 to save memory (max 65K rows per chunk, our standard is 2048)
#[derive(Debug, Clone, PartialEq)]
pub struct SelectionVector {
    indices: Vec<u16>,
}

impl SelectionVector {
    /// create a new empty selection vector with given capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            indices: Vec::with_capacity(capacity),
        }
    }

    /// add an index to the selection
    pub fn push(&mut self, index: u16) {
        self.indices.push(index);
    }

    /// get the index at position i in the selection
    pub fn get(&self, i: usize) -> usize {
        self.indices[i] as usize
    }

    /// number of selected rows
    pub fn count(&self) -> usize {
        self.indices.len()
    }

    /// check if empty
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// create a selection vector with all indices 0..count
    pub fn all(count: usize) -> Self {
        Self {
            indices: (0..count as u16).collect(),
        }
    }
}

/// represents a single value in the database
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Varchar(String),
    Null,
}

/// a columnar vector storing values for a single column
/// uses separate data array + validity bitmap for efficient NULL handling
#[derive(Debug, Clone)]
pub enum Vector {
    Integer { data: Vec<i64>, validity: Bitmap },
    Float { data: Vec<f64>, validity: Bitmap },
    Boolean { data: Vec<bool>, validity: Bitmap },
    Varchar { data: Vec<String>, validity: Bitmap },
}

impl Vector {
    /// create a new empty vector with given capacity and type
    pub fn new(column_type: &ColumnType, capacity: usize) -> Self {
        match column_type {
            ColumnType::Integer => Vector::Integer {
                data: Vec::with_capacity(capacity),
                validity: Bitmap::new(0), // start with 0 length, grows as we push
            },
            ColumnType::Float => Vector::Float {
                data: Vec::with_capacity(capacity),
                validity: Bitmap::new(0),
            },
            ColumnType::Boolean => Vector::Boolean {
                data: Vec::with_capacity(capacity),
                validity: Bitmap::new(0),
            },
            ColumnType::Varchar => Vector::Varchar {
                data: Vec::with_capacity(capacity),
                validity: Bitmap::new(0),
            },
            ColumnType::Null => Vector::Integer {
                data: Vec::with_capacity(capacity),
                validity: Bitmap::new(0),
            },
        }
    }

    /// get the number of values in the vector
    pub fn len(&self) -> usize {
        match self {
            Vector::Integer { data, .. } => data.len(),
            Vector::Float { data, .. } => data.len(),
            Vector::Boolean { data, .. } => data.len(),
            Vector::Varchar { data, .. } => data.len(),
        }
    }

    /// check if the vector is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// get a reference to the validity bitmap
    pub fn validity(&self) -> &Bitmap {
        match self {
            Vector::Integer { validity, .. } => validity,
            Vector::Float { validity, .. } => validity,
            Vector::Boolean { validity, .. } => validity,
            Vector::Varchar { validity, .. } => validity,
        }
    }

    /// get a value at the given index
    /// returns Some(Value) if index is valid and value is not NULL
    /// returns Some(Value::Null) if index is valid but value is NULL
    /// returns None if index is out of bounds
    pub fn get(&self, index: usize) -> Option<Value> {
        match self {
            Vector::Integer { data, validity } => {
                if index >= data.len() {
                    return None;
                }
                if validity.is_valid(index) {
                    Some(Value::Integer(data[index]))
                } else {
                    Some(Value::Null)
                }
            }
            Vector::Float { data, validity } => {
                if index >= data.len() {
                    return None;
                }
                if validity.is_valid(index) {
                    Some(Value::Float(data[index]))
                } else {
                    Some(Value::Null)
                }
            }
            Vector::Boolean { data, validity } => {
                if index >= data.len() {
                    return None;
                }
                if validity.is_valid(index) {
                    Some(Value::Boolean(data[index]))
                } else {
                    Some(Value::Null)
                }
            }
            Vector::Varchar { data, validity } => {
                if index >= data.len() {
                    return None;
                }
                if validity.is_valid(index) {
                    Some(Value::Varchar(data[index].clone()))
                } else {
                    Some(Value::Null)
                }
            }
        }
    }

    /// push a value to the vector
    pub fn push(&mut self, value: Value) {
        match (self, value) {
            (Vector::Integer { data, validity }, Value::Integer(i)) => {
                data.push(i);
                let index = data.len() - 1;
                validity.resize(data.len());
                validity.set_valid(index);
            }
            (Vector::Integer { data, validity }, Value::Null) => {
                data.push(0); // push garbage for NULL (won't be read)
                let index = data.len() - 1;
                validity.resize(data.len());
                validity.set_null(index);
            }
            (Vector::Float { data, validity }, Value::Float(f)) => {
                data.push(f);
                let index = data.len() - 1;
                validity.resize(data.len());
                validity.set_valid(index);
            }
            (Vector::Float { data, validity }, Value::Null) => {
                data.push(0.0); // push garbage for NULL
                let index = data.len() - 1;
                validity.resize(data.len());
                validity.set_null(index);
            }
            (Vector::Boolean { data, validity }, Value::Boolean(b)) => {
                data.push(b);
                let index = data.len() - 1;
                validity.resize(data.len());
                validity.set_valid(index);
            }
            (Vector::Boolean { data, validity }, Value::Null) => {
                data.push(false); // push garbage for NULL
                let index = data.len() - 1;
                validity.resize(data.len());
                validity.set_null(index);
            }
            (Vector::Varchar { data, validity }, Value::Varchar(s)) => {
                data.push(s);
                let index = data.len() - 1;
                validity.resize(data.len());
                validity.set_valid(index);
            }
            (Vector::Varchar { data, validity }, Value::Null) => {
                data.push(String::new()); // push empty string for NULL
                let index = data.len() - 1;
                validity.resize(data.len());
                validity.set_null(index);
            }
            _ => panic!("Type mismatch when pushing value to vector"),
        }
    }

    /// clear the vector (keep capacity)
    pub fn clear(&mut self) {
        match self {
            Vector::Integer { data, validity } => {
                data.clear();
                validity.resize(0);
            }
            Vector::Float { data, validity } => {
                data.clear();
                validity.resize(0);
            }
            Vector::Boolean { data, validity } => {
                data.clear();
                validity.resize(0);
            }
            Vector::Varchar { data, validity } => {
                data.clear();
                validity.resize(0);
            }
        }
    }

    /// get the column type of this vector
    pub fn column_type(&self) -> ColumnType {
        match self {
            Vector::Integer { .. } => ColumnType::Integer,
            Vector::Float { .. } => ColumnType::Float,
            Vector::Boolean { .. } => ColumnType::Boolean,
            Vector::Varchar { .. } => ColumnType::Varchar,
        }
    }
}

/// a batch of rows in columnar format
/// each column is stored as a separate Vector
/// represents a horizontal slice of a table (e.g., 2048 rows)
#[derive(Debug, Clone)]
pub struct DataChunk {
    /// column vectors
    pub columns: Vec<Vector>,
    /// number of valid rows in this chunk (≤ capacity)
    pub count: usize,
    /// maximum capacity (typically 2048)
    pub capacity: usize,
    /// optional selection vector for zero-copy filtering
    /// if present, only rows at sel.indices are valid
    pub selection: Option<SelectionVector>,
}

impl DataChunk {
    /// standard vector size for columnar batching
    pub const STANDARD_VECTOR_SIZE: usize = 2048;

    /// create a new DataChunk with given schema and capacity
    pub fn new(column_types: Vec<ColumnType>, capacity: usize) -> Self {
        let columns = column_types
            .iter()
            .map(|col_type| Vector::new(col_type, capacity))
            .collect();

        DataChunk {
            columns,
            count: 0,
            capacity,
            selection: None,
        }
    }

    /// create an empty DataChunk (for sources that don't take input)
    pub fn empty() -> Self {
        DataChunk {
            columns: Vec::new(),
            count: 0,
            capacity: 0,
            selection: None,
        }
    }

    /// reset the chunk (clear all vectors, set count to 0)
    /// keeps allocated memory for reuse
    pub fn reset(&mut self) {
        for col in &mut self.columns {
            col.clear();
        }
        self.count = 0;
        self.selection = None;
    }

    /// check if the chunk is empty
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// get the number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// get a value at (column_idx, row_idx)
    /// if selection vector is present, row_idx is into the selection, not the raw data
    pub fn get_value(&self, column_idx: usize, row_idx: usize) -> Option<Value> {
        let actual_row = if let Some(ref sel) = self.selection {
            if row_idx >= sel.count() {
                return None;
            }
            sel.get(row_idx)
        } else {
            if row_idx >= self.count {
                return None;
            }
            row_idx
        };

        self.columns
            .get(column_idx)
            .and_then(|col| col.get(actual_row))
    }

    /// get the effective row count (selection count if present, else count)
    pub fn selected_count(&self) -> usize {
        if let Some(ref sel) = self.selection {
            sel.count()
        } else {
            self.count
        }
    }

    /// set a selection vector (for zero-copy filtering)
    pub fn set_selection(&mut self, selection: SelectionVector) {
        self.selection = Some(selection);
    }

    /// clear the selection vector (return to full chunk)
    pub fn clear_selection(&mut self) {
        self.selection = None;
    }

    /// append a row to the chunk
    /// panics if row doesn't match schema
    pub fn append_row(&mut self, row: Vec<Value>) {
        assert_eq!(row.len(), self.columns.len(), "Row length mismatch");
        assert!(self.count < self.capacity, "DataChunk is full");

        for (i, value) in row.into_iter().enumerate() {
            self.columns[i].push(value);
        }
        self.count += 1;
    }

    /// remove the first `n` rows by updating the selection vector
    /// used for OFFSET implementation
    pub fn apply_offset(&mut self, n: usize) {
        if n == 0 {
            return;
        }

        if let Some(ref mut sel) = self.selection {
            // already have selection vector - remove first n indices
            if n >= sel.count() {
                // skip entire chunk
                sel.indices.clear();
            } else {
                // remove first n elements
                sel.indices.drain(0..n);
            }
        } else {
            // no selection vector - create one excluding first n rows
            if n >= self.count {
                // skip entire chunk
                self.selection = Some(SelectionVector::new(0));
            } else {
                // create selection vector with rows n..count
                let remaining = self.count - n;
                let mut sel = SelectionVector::new(remaining);
                for i in n..self.count {
                    sel.push(i as u16);
                }
                self.selection = Some(sel);
            }
        }
    }

    /// keep only the first `n` rows (truncate the rest)
    /// used for LIMIT implementation
    pub fn truncate(&mut self, n: usize) {
        let current_len = self.selected_count();
        if n >= current_len {
            return; // already smaller or equal
        }

        if let Some(ref mut sel) = self.selection {
            // already have selection vector - just truncate it
            sel.indices.truncate(n);
        } else {
            // no selection vector - create one with first n rows
            let mut sel = SelectionVector::new(n);
            for i in 0..n {
                sel.push(i as u16);
            }
            self.selection = Some(sel);
        }
    }
}
