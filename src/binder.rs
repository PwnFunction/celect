use crate::parser::{AggregateFunction, Expression, LiteralValue, Query, SelectColumn};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq)]
pub struct BinderError {
    pub message: String,
}

pub type BindResult<T> = Result<T, BinderError>;

#[derive(Debug, Clone, PartialEq)]
pub struct BoundQuery {
    pub select_columns: Vec<Column>, // validated and bound columns
    pub file_path: PathBuf,
    pub schema: Schema,
    pub where_clause: Option<BoundExpression>, // bound expression instead of raw
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub aggregates: Vec<BoundAggregateExpression>, // aggregate functions in SELECT
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundAggregateExpression {
    CountStar,
    Count {
        column: Column, // column to count non-NULL values
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub type_: ColumnType,
    pub index: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Integer,
    Float,
    Boolean,
    Varchar,
    Null, // if column is all NULL
}

/// bound expression with metadata attached (column indices, types, etc.)
#[derive(Debug, Clone, PartialEq)]
pub enum BoundExpression {
    // logical operators (in precedence order: OR < AND < NOT)
    Or(Box<BoundExpression>, Box<BoundExpression>),
    And(Box<BoundExpression>, Box<BoundExpression>),
    Not(Box<BoundExpression>),

    // column reference with metadata
    ColumnRef {
        name: String,
        index: usize,      // column index for execution
        type_: ColumnType, // column type
    },

    // literal with type attached
    Literal {
        value: LiteralValue,
        type_: ColumnType, // type of the literal
    },

    // comparison operators (all return Boolean)
    Equal(Box<BoundExpression>, Box<BoundExpression>),
    NotEqual(Box<BoundExpression>, Box<BoundExpression>),
    GreaterThan(Box<BoundExpression>, Box<BoundExpression>),
    GreaterThanOrEqual(Box<BoundExpression>, Box<BoundExpression>),
    LessThan(Box<BoundExpression>, Box<BoundExpression>),
    LessThanOrEqual(Box<BoundExpression>, Box<BoundExpression>),
}

pub struct Binder;

impl Binder {
    pub fn new() -> Self {
        Self
    }

    /// binds a parsed Query to create a BoundQuery with all metadata attached.
    /// this performs validation and binding in one step.
    pub fn bind(&self, query: Query) -> BindResult<BoundQuery> {
        // step 1: Resolve file name
        let file_path = self.resolve_file_name(&query.from.file)?;

        // step 2: Read headers (assume has_header=true for now)
        let mut schema = self.read_csv_headers(&file_path)?;

        // step 3: Infer types
        self.infer_column_types(&file_path, &mut schema, true)?;

        // step 4: Validate and bind SELECT columns and aggregates
        let (select_columns, aggregates) =
            self.validate_select_columns(&query.select.columns, &schema)?;

        // step 5: Validate and bind WHERE clause (if present)
        let where_clause = if let Some(where_clause) = query.where_clause {
            // validate first
            self.validate_where_expression(&where_clause.condition, &schema)?;
            // then bind
            Some(self.bind_expression(&where_clause.condition, &schema)?)
        } else {
            None
        };

        Ok(BoundQuery {
            select_columns,
            file_path,
            schema,
            where_clause,
            limit: query.limit,
            offset: query.offset,
            aggregates,
        })
    }

    /// validates SELECT columns against the schema.
    /// - Expands `SELECT *` to all columns
    /// - Validates that specified columns exist in the schema
    /// - Returns error if any column doesn't exist
    /// - Returns both regular columns and aggregates
    pub fn validate_select_columns(
        &self,
        select_columns: &[SelectColumn],
        schema: &Schema,
    ) -> BindResult<(Vec<Column>, Vec<BoundAggregateExpression>)> {
        let mut validated_columns = Vec::new();
        let mut aggregates = Vec::new();

        for col in select_columns {
            match col {
                SelectColumn::All => {
                    // expand * to all columns
                    validated_columns.extend(schema.columns.clone());
                }
                SelectColumn::Column(name) => {
                    // find column in schema
                    let found_column =
                        schema
                            .columns
                            .iter()
                            .find(|c| c.name == *name)
                            .ok_or_else(|| BinderError {
                                message: format!("Column '{}' not found in schema", name),
                            })?;
                    validated_columns.push(found_column.clone());
                }
                SelectColumn::Aggregate(agg_func) => {
                    // bind aggregate function
                    let bound_agg = self.bind_aggregate_function(agg_func, schema)?;
                    aggregates.push(bound_agg);
                }
            }
        }

        Ok((validated_columns, aggregates))
    }

    /// binds an aggregate function and validates column references
    fn bind_aggregate_function(
        &self,
        agg_func: &AggregateFunction,
        schema: &Schema,
    ) -> BindResult<BoundAggregateExpression> {
        match agg_func {
            AggregateFunction::CountStar => Ok(BoundAggregateExpression::CountStar),
            AggregateFunction::Count(column_name) => {
                // find column in schema
                let found_column = schema
                    .columns
                    .iter()
                    .find(|c| c.name == *column_name)
                    .ok_or_else(|| BinderError {
                        message: format!("Column '{}' not found in schema", column_name),
                    })?;

                Ok(BoundAggregateExpression::Count {
                    column: found_column.clone(),
                })
            }
        }
    }

    /// validates WHERE clause expressions:
    /// - Validates that column references exist in schema
    /// - Validates that comparison types are compatible (strict - no auto-casting)
    /// - Returns error if column doesn't exist or types are incompatible
    pub fn validate_where_expression(
        &self,
        expression: &Expression,
        schema: &Schema,
    ) -> BindResult<()> {
        match expression {
            Expression::Or(left, right) => {
                // or requires both operands to be boolean
                self.validate_where_expression(left, schema)?;
                self.validate_where_expression(right, schema)
            }
            Expression::And(left, right) => {
                // and requires both operands to be boolean
                self.validate_where_expression(left, schema)?;
                self.validate_where_expression(right, schema)
            }
            Expression::Not(inner) => {
                // not requires its operand to be boolean
                // recursively validate the inner expression
                self.validate_where_expression(inner, schema)
            }
            Expression::Equal(left, right)
            | Expression::NotEqual(left, right)
            | Expression::GreaterThan(left, right)
            | Expression::GreaterThanOrEqual(left, right)
            | Expression::LessThan(left, right)
            | Expression::LessThanOrEqual(left, right) => {
                // validate both sides
                let left_type = self.get_expression_type(left, schema)?;
                let right_type = self.get_expression_type(right, schema)?;

                // check type compatibility (strict - must match exactly or be compatible)
                if !self.are_types_compatible(&left_type, &right_type) {
                    return Err(BinderError {
                        message: format!(
                            "Cannot compare {} and {} - types must match",
                            self.type_to_string(&left_type),
                            self.type_to_string(&right_type)
                        ),
                    });
                }

                Ok(())
            }
            Expression::Column(name) => {
                // validate column exists
                schema
                    .columns
                    .iter()
                    .find(|c| c.name == *name)
                    .ok_or_else(|| BinderError {
                        message: format!("Column '{}' not found in schema", name),
                    })?;
                Ok(())
            }
            Expression::Literal(_) => {
                // literals are always valid
                Ok(())
            }
        }
    }

    /// gets the type of an expression.
    fn get_expression_type(&self, expr: &Expression, schema: &Schema) -> BindResult<ColumnType> {
        match expr {
            Expression::Or(_, _) | Expression::And(_, _) | Expression::Not(_) => {
                // logical operators return boolean
                Ok(ColumnType::Boolean)
            }
            Expression::Column(name) => {
                let col = schema
                    .columns
                    .iter()
                    .find(|c| c.name == *name)
                    .ok_or_else(|| BinderError {
                        message: format!("Column '{}' not found in schema", name),
                    })?;
                Ok(col.type_.clone())
            }
            Expression::Literal(lit) => Ok(match lit {
                LiteralValue::Integer(_) => ColumnType::Integer,
                LiteralValue::Float(_) => ColumnType::Float,
                LiteralValue::String(_) => ColumnType::Varchar,
                LiteralValue::Boolean(_) => ColumnType::Boolean,
                LiteralValue::Null => ColumnType::Null,
            }),
            Expression::Equal(_, _)
            | Expression::NotEqual(_, _)
            | Expression::GreaterThan(_, _)
            | Expression::GreaterThanOrEqual(_, _)
            | Expression::LessThan(_, _)
            | Expression::LessThanOrEqual(_, _) => {
                // comparison expressions return boolean
                Ok(ColumnType::Boolean)
            }
        }
    }

    /// checks if two types are compatible for comparison (strict).
    /// only allows:
    /// - Same types
    /// - Integer and Float (numeric comparison)
    fn are_types_compatible(&self, left: &ColumnType, right: &ColumnType) -> bool {
        match (left, right) {
            // same types are always compatible
            (l, r) if l == r => true,

            // integer and Float are compatible (numeric comparison)
            (ColumnType::Integer, ColumnType::Float) => true,
            (ColumnType::Float, ColumnType::Integer) => true,

            // null is compatible with any type (for IS NULL checks, but we don't have that yet)
            (ColumnType::Null, _) => true,
            (_, ColumnType::Null) => true,

            // everything else is incompatible
            _ => false,
        }
    }

    /// converts ColumnType to string for error messages.
    fn type_to_string(&self, ty: &ColumnType) -> &str {
        match ty {
            ColumnType::Integer => "Integer",
            ColumnType::Float => "Float",
            ColumnType::Boolean => "Boolean",
            ColumnType::Varchar => "Varchar",
            ColumnType::Null => "Null",
        }
    }

    /// binds an expression to create a BoundExpression with metadata.
    /// recursively walks the Expression tree and attaches:
    /// - Column indices and types for column references
    /// - Types for literals
    /// - Types for comparison expressions (always Boolean)
    pub fn bind_expression(
        &self,
        expr: &Expression,
        schema: &Schema,
    ) -> BindResult<BoundExpression> {
        match expr {
            Expression::Or(left, right) => {
                let bound_left = self.bind_expression(left, schema)?;
                let bound_right = self.bind_expression(right, schema)?;
                Ok(BoundExpression::Or(
                    Box::new(bound_left),
                    Box::new(bound_right),
                ))
            }
            Expression::And(left, right) => {
                let bound_left = self.bind_expression(left, schema)?;
                let bound_right = self.bind_expression(right, schema)?;
                Ok(BoundExpression::And(
                    Box::new(bound_left),
                    Box::new(bound_right),
                ))
            }
            Expression::Not(inner) => {
                let bound_inner = self.bind_expression(inner, schema)?;
                Ok(BoundExpression::Not(Box::new(bound_inner)))
            }
            Expression::Column(name) => {
                // look up column in schema
                let col = schema
                    .columns
                    .iter()
                    .find(|c| c.name == *name)
                    .ok_or_else(|| BinderError {
                        message: format!("Column '{}' not found in schema", name),
                    })?;

                Ok(BoundExpression::ColumnRef {
                    name: name.clone(),
                    index: col.index,
                    type_: col.type_.clone(),
                })
            }

            Expression::Literal(value) => {
                // infer type from literal value
                let type_ = match value {
                    LiteralValue::Integer(_) => ColumnType::Integer,
                    LiteralValue::Float(_) => ColumnType::Float,
                    LiteralValue::String(_) => ColumnType::Varchar,
                    LiteralValue::Boolean(_) => ColumnType::Boolean,
                    LiteralValue::Null => ColumnType::Null,
                };

                Ok(BoundExpression::Literal {
                    value: value.clone(),
                    type_,
                })
            }

            Expression::Equal(left, right) => {
                let bound_left = self.bind_expression(left, schema)?;
                let bound_right = self.bind_expression(right, schema)?;
                Ok(BoundExpression::Equal(
                    Box::new(bound_left),
                    Box::new(bound_right),
                ))
            }

            Expression::NotEqual(left, right) => {
                let bound_left = self.bind_expression(left, schema)?;
                let bound_right = self.bind_expression(right, schema)?;
                Ok(BoundExpression::NotEqual(
                    Box::new(bound_left),
                    Box::new(bound_right),
                ))
            }

            Expression::GreaterThan(left, right) => {
                let bound_left = self.bind_expression(left, schema)?;
                let bound_right = self.bind_expression(right, schema)?;
                Ok(BoundExpression::GreaterThan(
                    Box::new(bound_left),
                    Box::new(bound_right),
                ))
            }

            Expression::GreaterThanOrEqual(left, right) => {
                let bound_left = self.bind_expression(left, schema)?;
                let bound_right = self.bind_expression(right, schema)?;
                Ok(BoundExpression::GreaterThanOrEqual(
                    Box::new(bound_left),
                    Box::new(bound_right),
                ))
            }

            Expression::LessThan(left, right) => {
                let bound_left = self.bind_expression(left, schema)?;
                let bound_right = self.bind_expression(right, schema)?;
                Ok(BoundExpression::LessThan(
                    Box::new(bound_left),
                    Box::new(bound_right),
                ))
            }

            Expression::LessThanOrEqual(left, right) => {
                let bound_left = self.bind_expression(left, schema)?;
                let bound_right = self.bind_expression(right, schema)?;
                Ok(BoundExpression::LessThanOrEqual(
                    Box::new(bound_left),
                    Box::new(bound_right),
                ))
            }
        }
    }

    /// validates a CSV file and infers its schema (headers + types).
    /// also validates SELECT columns and WHERE expressions against the schema.
    /// this is a convenience method that combines:
    /// - File name resolution
    /// - Header reading (or generating if has_header=false)
    /// - Type inference
    /// - SELECT column validation
    /// - WHERE expression validation (if provided)
    pub fn validate(
        &self,
        file_name: &str,
        has_header: bool,
        select_columns: &[SelectColumn],
        where_clause: Option<&Expression>,
    ) -> BindResult<(PathBuf, Schema, Vec<Column>)> {
        // step 1: Resolve file name
        let file_path = self.resolve_file_name(file_name)?;

        // step 2: Read headers or generate column names
        let mut schema = if has_header {
            self.read_csv_headers(&file_path)?
        } else {
            self.read_csv_without_headers(&file_path)?
        };

        // step 3: Infer types (skip header row only if has_header=true)
        self.infer_column_types(&file_path, &mut schema, has_header)?;

        // step 4: Validate SELECT columns (ignore aggregates for old API compatibility)
        let (validated_select_columns, _aggregates) =
            self.validate_select_columns(select_columns, &schema)?;

        // step 5: Validate WHERE expression (if present)
        if let Some(where_expr) = where_clause {
            self.validate_where_expression(where_expr, &schema)?;
        }

        Ok((file_path, schema, validated_select_columns))
    }

    /// resolves a file name from the AST to an actual CSV file path.
    /// validates that the file exists.
    pub fn resolve_file_name(&self, file_name: &str) -> BindResult<PathBuf> {
        let path = Path::new(file_name);

        // resolve relative paths to absolute
        let resolved_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|e| BinderError {
                    message: format!("Failed to get current directory: {}", e),
                })?
                .join(path)
        };

        // validate file exists
        if !resolved_path.exists() {
            return Err(BinderError {
                message: format!("File not found: {}", resolved_path.display()),
            });
        }

        Ok(resolved_path)
    }

    /// reads CSV file headers (first row) and returns column names.
    /// assumes the first row contains column headers.
    pub fn read_csv_headers(&self, file_path: &PathBuf) -> BindResult<Schema> {
        // read first line of file
        let content = fs::read_to_string(file_path).map_err(|e| BinderError {
            message: format!("Failed to read file: {}", e),
        })?;

        // get first line
        let first_line = content.lines().next().ok_or_else(|| BinderError {
            message: "CSV file is empty".to_string(),
        })?;

        // parse CSV header: split by comma and trim whitespace
        let column_names: Vec<String> = first_line
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        if column_names.is_empty() {
            return Err(BinderError {
                message: "CSV header has no columns".to_string(),
            });
        }

        // create columns with indices (types will be inferred separately)
        let columns: Vec<Column> = column_names
            .into_iter()
            .enumerate()
            .map(|(index, name)| Column {
                name,
                type_: ColumnType::Varchar, // temporary, will be inferred
                index,
            })
            .collect();

        Ok(Schema { columns })
    }

    /// reads CSV file without headers and generates column names (column1, column2, etc.).
    pub fn read_csv_without_headers(&self, file_path: &PathBuf) -> BindResult<Schema> {
        let content = fs::read_to_string(file_path).map_err(|e| BinderError {
            message: format!("Failed to read file: {}", e),
        })?;

        // get first line to determine number of columns
        let first_line = content.lines().next().ok_or_else(|| BinderError {
            message: "CSV file is empty".to_string(),
        })?;

        // parse first line to count columns
        let column_count = first_line.split(',').map(|s| s.trim()).count();

        if column_count == 0 {
            return Err(BinderError {
                message: "CSV file has no columns".to_string(),
            });
        }

        // generate column names: column1, column2, etc.
        let columns: Vec<Column> = (0..column_count)
            .map(|index| Column {
                name: format!("column{}", index + 1),
                type_: ColumnType::Varchar, // temporary, will be inferred
                index,
            })
            .collect();

        Ok(Schema { columns })
    }

    /// infers column types by reading sample data rows.
    /// reads first 20 rows (excluding header if has_header=true) and tries casting to types in order:
    /// integer → FLOAT → BOOLEAN → VARCHAR (fallback)
    pub fn infer_column_types(
        &self,
        file_path: &PathBuf,
        schema: &mut Schema,
        has_header: bool,
    ) -> BindResult<()> {
        let content = fs::read_to_string(file_path).map_err(|e| BinderError {
            message: format!("Failed to read file: {}", e),
        })?;

        let lines: Vec<&str> = content.lines().collect();
        if lines.len() < 2 {
            // only header, no data rows - all columns remain VARCHAR
            return Ok(());
        }

        // read sample rows (skip header only if has_header=true, max 20 rows)
        let skip_count = if has_header { 1 } else { 0 };
        let sample_rows: Vec<&str> = lines
            .iter()
            .skip(skip_count) // skip header only if has_header=true
            .take(20) // sample first 20 rows
            .copied()
            .collect();

        if sample_rows.is_empty() {
            return Ok(());
        }

        // infer type for each column
        for col in &mut schema.columns {
            let inferred_type = self.infer_type_for_column(&sample_rows, col.index)?;
            col.type_ = inferred_type;
        }

        Ok(())
    }

    /// infers the type for a single column by trying casts in order.
    fn infer_type_for_column(
        &self,
        sample_rows: &[&str],
        col_index: usize,
    ) -> BindResult<ColumnType> {
        // try types in order: INTEGER → FLOAT → BOOLEAN → VARCHAR (fallback)
        let mut all_null = true;

        // try INTEGER first
        let mut all_integer = true;
        let mut has_valid_value = false;
        for row in sample_rows {
            let values: Vec<&str> = row.split(',').map(|s| s.trim()).collect();
            if col_index >= values.len() {
                continue; // skip rows with missing columns
            }
            let value = values[col_index];
            if value.is_empty() || value.eq_ignore_ascii_case("null") {
                continue; // null doesn't break type detection
            }
            all_null = false;
            has_valid_value = true;
            if value.parse::<i64>().is_err() {
                all_integer = false;
                break;
            }
        }
        if !has_valid_value {
            all_integer = false;
        }
        if all_null {
            return Ok(ColumnType::Null);
        }
        if all_integer {
            return Ok(ColumnType::Integer);
        }

        // try FLOAT
        let mut all_float = true;
        let mut has_valid_value = false;
        for row in sample_rows {
            let values: Vec<&str> = row.split(',').map(|s| s.trim()).collect();
            if col_index >= values.len() {
                continue; // skip rows with missing columns
            }
            let value = values[col_index];
            if value.is_empty() || value.eq_ignore_ascii_case("null") {
                continue;
            }
            has_valid_value = true;
            if value.parse::<f64>().is_err() {
                all_float = false;
                break;
            }
        }
        if !has_valid_value {
            all_float = false;
        }
        if all_float {
            return Ok(ColumnType::Float);
        }

        // try BOOLEAN (only accept explicit "true" or "false")
        let mut all_boolean = true;
        let mut has_valid_value = false;
        for row in sample_rows {
            let values: Vec<&str> = row.split(',').map(|s| s.trim()).collect();
            if col_index >= values.len() {
                continue; // skip rows with missing columns
            }
            let value = values[col_index];
            if value.is_empty() || value.eq_ignore_ascii_case("null") {
                continue;
            }
            has_valid_value = true;
            // only accept explicit "true" or "false" (case-insensitive)
            let lower = value.to_lowercase();
            if lower != "true" && lower != "false" {
                all_boolean = false;
                break;
            }
        }
        if !has_valid_value {
            all_boolean = false;
        }
        if all_boolean {
            return Ok(ColumnType::Boolean);
        }

        // fallback to VARCHAR
        Ok(ColumnType::Varchar)
    }
}
