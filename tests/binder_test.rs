use celect::binder::{BoundAggregateExpression, BoundExpression, Column, ColumnType, Schema};
use celect::parser::{Expression, LiteralValue, SelectColumn};
use celect::{Binder, Parser};

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    // guard struct that automatically cleans up test files when dropped
    struct TestFileGuard {
        file: String,
    }

    impl TestFileGuard {
        fn new(file: String) -> Self {
            Self { file }
        }

        fn path(&self) -> &str {
            &self.file
        }
    }

    impl Drop for TestFileGuard {
        fn drop(&mut self) {
            if Path::new(&self.file).exists() {
                let _ = fs::remove_file(&self.file);
            }
        }
    }

    fn setup_test_file() -> TestFileGuard {
        // use unique filename for each test to avoid conflicts
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let test_file = format!("testdata_{}", counter);
        // create a simple test CSV file
        fs::write(&test_file, "id,name,age\n1,Alice,30\n2,Bob,25\n").unwrap();
        TestFileGuard::new(test_file)
    }

    #[test]
    fn test_resolve_existing_file() {
        let test_file = setup_test_file();
        // ensure file exists before resolving
        assert!(
            Path::new(test_file.path()).exists(),
            "Test file should exist"
        );

        let binder = Binder::new();
        let result = binder.resolve_file_name(test_file.path());
        assert!(result.is_ok(), "Resolve should succeed, got: {:?}", result);
        let path = result.unwrap();
        assert!(path.exists());
        assert!(path.is_file());
        // file will be automatically cleaned up when test_file goes out of scope
    }

    #[test]
    fn test_resolve_nonexistent_file() {
        let binder = Binder::new();
        let result = binder.resolve_file_name("nonexistent");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.message.contains("File not found"));
    }

    #[test]
    fn test_resolve_relative_path() {
        let test_file = setup_test_file();
        // ensure file exists before resolving
        assert!(
            Path::new(test_file.path()).exists(),
            "Test file should exist"
        );

        let binder = Binder::new();
        let result = binder.resolve_file_name(test_file.path());
        assert!(result.is_ok(), "Resolve should succeed, got: {:?}", result);
        let path = result.unwrap();
        // resolved path should be absolute (relative paths are converted to absolute)
        assert!(path.is_absolute());
        assert!(path.exists());
        // file will be automatically cleaned up when test_file goes out of scope
    }

    #[test]
    fn test_parse_and_resolve_file() {
        let test_file = setup_test_file();
        // ensure file exists before resolving
        assert!(
            Path::new(test_file.path()).exists(),
            "Test file should exist"
        );

        let mut parser = Parser::new();
        let binder = Binder::new();

        // parse SQL to get AST
        let sql = format!("SELECT * FROM {}", test_file.path());
        let parse_result = parser.parse(&sql);
        assert!(
            parse_result.is_ok(),
            "Parse should succeed, got: {:?}",
            parse_result
        );

        let query = parse_result.unwrap();

        // resolve file name from AST
        let resolve_result = binder.resolve_file_name(&query.from.file);
        assert!(
            resolve_result.is_ok(),
            "Resolve should succeed, got: {:?}",
            resolve_result
        );
        let path = resolve_result.unwrap();
        assert!(path.exists());
        // file will be automatically cleaned up when test_file goes out of scope
    }

    #[test]
    fn test_validate() {
        let test_file = setup_test_file();
        assert!(
            Path::new(test_file.path()).exists(),
            "Test file should exist"
        );

        let binder = Binder::new();
        let validate_result = binder.validate(test_file.path(), true, &[], None);
        assert!(validate_result.is_ok(), "Should validate file successfully");

        let (_file_path, schema, _validated_select) = validate_result.unwrap();
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[0].index, 0);
        assert_eq!(schema.columns[1].name, "name");
        assert_eq!(schema.columns[1].index, 1);
        assert_eq!(schema.columns[2].name, "age");
        assert_eq!(schema.columns[2].index, 2);

        // verify types (id and age should be Integer, name should be Varchar)
        use celect::binder::ColumnType;
        assert_eq!(schema.columns[0].type_, ColumnType::Integer); // id
        assert_eq!(schema.columns[1].type_, ColumnType::Varchar); // name
        assert_eq!(schema.columns[2].type_, ColumnType::Integer); // age
        // file will be automatically cleaned up when test_file goes out of scope
    }

    #[test]
    fn test_read_csv_headers_empty_file() {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let test_file = format!("empty_test_{}", counter);
        fs::write(&test_file, "").unwrap();

        let binder = Binder::new();
        let file_path = binder.resolve_file_name(&test_file).unwrap();

        let schema_result = binder.read_csv_headers(&file_path);
        assert!(schema_result.is_err());
        let error = schema_result.unwrap_err();
        assert!(error.message.contains("empty"));

        fs::remove_file(&test_file).unwrap();
    }

    #[test]
    fn test_infer_types_integer() {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let test_file = format!("int_test_{}", counter);
        fs::write(&test_file, "id\n1\n2\n3\n").unwrap();

        let binder = Binder::new();
        let file_path = binder.resolve_file_name(&test_file).unwrap();

        let mut schema = binder.read_csv_headers(&file_path).unwrap();
        binder
            .infer_column_types(&file_path, &mut schema, true)
            .unwrap();

        assert_eq!(schema.columns[0].type_, ColumnType::Integer);

        fs::remove_file(&test_file).unwrap();
    }

    #[test]
    fn test_infer_types_float() {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let test_file = format!("float_test_{}", counter);
        fs::write(&test_file, "price\n1.5\n2.7\n3.14\n").unwrap();

        let binder = Binder::new();
        let file_path = binder.resolve_file_name(&test_file).unwrap();

        let mut schema = binder.read_csv_headers(&file_path).unwrap();
        binder
            .infer_column_types(&file_path, &mut schema, true)
            .unwrap();

        assert_eq!(schema.columns[0].type_, ColumnType::Float);

        fs::remove_file(&test_file).unwrap();
    }

    #[test]
    fn test_infer_types_boolean() {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let test_file = format!("bool_test_{}", counter);
        fs::write(&test_file, "active\ntrue\nfalse\ntrue\n").unwrap();

        let binder = Binder::new();
        let file_path = binder.resolve_file_name(&test_file).unwrap();

        let mut schema = binder.read_csv_headers(&file_path).unwrap();
        binder
            .infer_column_types(&file_path, &mut schema, true)
            .unwrap();

        assert_eq!(schema.columns[0].type_, ColumnType::Boolean);

        fs::remove_file(&test_file).unwrap();
    }

    #[test]
    fn test_infer_types_not_boolean_for_yes_no() {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let test_file = format!("not_bool_test_{}", counter);
        fs::write(&test_file, "flag\nyes\nno\nyes\n").unwrap();

        let binder = Binder::new();
        let file_path = binder.resolve_file_name(&test_file).unwrap();

        let mut schema = binder.read_csv_headers(&file_path).unwrap();
        binder
            .infer_column_types(&file_path, &mut schema, true)
            .unwrap();

        // "yes"/"no" should NOT be detected as boolean, should be Varchar
        assert_eq!(schema.columns[0].type_, ColumnType::Varchar);

        fs::remove_file(&test_file).unwrap();
    }

    #[test]
    fn test_infer_types_not_boolean_for_1_0() {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let test_file = format!("not_bool_num_test_{}", counter);
        fs::write(&test_file, "flag\n1\n0\n1\n").unwrap();

        let binder = Binder::new();
        let file_path = binder.resolve_file_name(&test_file).unwrap();

        let mut schema = binder.read_csv_headers(&file_path).unwrap();
        binder
            .infer_column_types(&file_path, &mut schema, true)
            .unwrap();

        // "1"/"0" should NOT be detected as boolean, should be Integer
        assert_eq!(schema.columns[0].type_, ColumnType::Integer);

        fs::remove_file(&test_file).unwrap();
    }

    #[test]
    fn test_infer_types_varchar_fallback() {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let test_file = format!("varchar_test_{}", counter);
        fs::write(&test_file, "name\nAlice\nBob\n123abc\n").unwrap();

        let binder = Binder::new();
        let file_path = binder.resolve_file_name(&test_file).unwrap();

        let mut schema = binder.read_csv_headers(&file_path).unwrap();
        binder
            .infer_column_types(&file_path, &mut schema, true)
            .unwrap();

        assert_eq!(schema.columns[0].type_, ColumnType::Varchar);

        fs::remove_file(&test_file).unwrap();
    }

    #[test]
    fn test_validate_without_header() {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let test_file = format!("no_header_test_{}", counter);
        // csv without header - first row is data
        fs::write(&test_file, "1,Alice,30\n2,Bob,25\n").unwrap();

        let binder = Binder::new();
        let validate_result = binder.validate(&test_file, false, &[], None); // has_header=false

        assert!(
            validate_result.is_ok(),
            "Should validate file without header"
        );
        let (_file_path, schema, _validated_select) = validate_result.unwrap();

        // should generate column names: column1, column2, column3
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[0].name, "column1");
        assert_eq!(schema.columns[1].name, "column2");
        assert_eq!(schema.columns[2].name, "column3");

        // types should be inferred from first row (which is now data, not header)
        assert_eq!(schema.columns[0].type_, ColumnType::Integer); // 1
        assert_eq!(schema.columns[1].type_, ColumnType::Varchar); // alice
        assert_eq!(schema.columns[2].type_, ColumnType::Integer); // 30

        fs::remove_file(&test_file).unwrap();
    }

    #[test]
    fn test_validate_header_vs_no_header_same_file() {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let test_file = format!("header_comparison_test_{}", counter);
        // create a CSV file
        fs::write(&test_file, "id,name,age\n1,Alice,30\n2,Bob,25\n").unwrap();

        let binder = Binder::new();

        // test with header=true
        let result_with_header = binder.validate(&test_file, true, &[], None).unwrap();
        let (_path1, schema_with_header, _) = result_with_header;
        assert_eq!(schema_with_header.columns[0].name, "id");
        assert_eq!(schema_with_header.columns[1].name, "name");
        assert_eq!(schema_with_header.columns[2].name, "age");

        // test with header=false (same file)
        let result_no_header = binder.validate(&test_file, false, &[], None).unwrap();
        let (_path2, schema_no_header, _) = result_no_header;
        assert_eq!(schema_no_header.columns[0].name, "column1"); // generated name
        assert_eq!(schema_no_header.columns[1].name, "column2");
        assert_eq!(schema_no_header.columns[2].name, "column3");

        // with header=false, first row ("id,name,age") is treated as data, so types should be Varchar
        assert_eq!(schema_no_header.columns[0].type_, ColumnType::Varchar); // "id" as string
        assert_eq!(schema_no_header.columns[1].type_, ColumnType::Varchar); // "name" as string
        assert_eq!(schema_no_header.columns[2].type_, ColumnType::Varchar); // "age" as string

        fs::remove_file(&test_file).unwrap();
    }

    #[test]
    fn test_validate_select_all() {
        let binder = Binder::new();
        let schema = Schema {
            columns: vec![
                Column {
                    name: "id".to_string(),
                    type_: ColumnType::Integer,
                    index: 0,
                },
                Column {
                    name: "name".to_string(),
                    type_: ColumnType::Varchar,
                    index: 1,
                },
                Column {
                    name: "age".to_string(),
                    type_: ColumnType::Integer,
                    index: 2,
                },
            ],
        };

        let select_columns = vec![SelectColumn::All];
        let result = binder.validate_select_columns(&select_columns, &schema);

        assert!(result.is_ok());
        let (validated, aggregates) = result.unwrap();
        assert_eq!(aggregates.len(), 0); // no aggregates
        assert_eq!(validated.len(), 3);
        assert_eq!(validated[0].name, "id");
        assert_eq!(validated[1].name, "name");
        assert_eq!(validated[2].name, "age");
    }

    #[test]
    fn test_validate_select_specific_columns() {
        let binder = Binder::new();
        let schema = Schema {
            columns: vec![
                Column {
                    name: "id".to_string(),
                    type_: ColumnType::Integer,
                    index: 0,
                },
                Column {
                    name: "name".to_string(),
                    type_: ColumnType::Varchar,
                    index: 1,
                },
                Column {
                    name: "age".to_string(),
                    type_: ColumnType::Integer,
                    index: 2,
                },
            ],
        };

        let select_columns = vec![
            SelectColumn::Column("id".to_string()),
            SelectColumn::Column("name".to_string()),
        ];
        let result = binder.validate_select_columns(&select_columns, &schema);

        assert!(result.is_ok());
        let (validated, aggregates) = result.unwrap();
        assert_eq!(aggregates.len(), 0); // no aggregates
        assert_eq!(validated.len(), 2);
        assert_eq!(validated[0].name, "id");
        assert_eq!(validated[1].name, "name");
    }

    #[test]
    fn test_validate_select_nonexistent_column() {
        let binder = Binder::new();
        let schema = Schema {
            columns: vec![
                Column {
                    name: "id".to_string(),
                    type_: ColumnType::Integer,
                    index: 0,
                },
                Column {
                    name: "name".to_string(),
                    type_: ColumnType::Varchar,
                    index: 1,
                },
            ],
        };

        let select_columns = vec![SelectColumn::Column("nonexistent".to_string())];
        let result = binder.validate_select_columns(&select_columns, &schema);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.message.contains("not found"));
        assert!(error.message.contains("nonexistent"));
    }

    #[test]
    fn test_validate_select_mixed_all_and_specific() {
        let binder = Binder::new();
        let schema = Schema {
            columns: vec![
                Column {
                    name: "id".to_string(),
                    type_: ColumnType::Integer,
                    index: 0,
                },
                Column {
                    name: "name".to_string(),
                    type_: ColumnType::Varchar,
                    index: 1,
                },
            ],
        };

        // select *, name - should expand * to all columns, then add name again
        let select_columns = vec![SelectColumn::All, SelectColumn::Column("name".to_string())];
        let result = binder.validate_select_columns(&select_columns, &schema);

        assert!(result.is_ok());
        let (validated, aggregates) = result.unwrap();
        assert_eq!(aggregates.len(), 0); // no aggregates
        // should have id, name (from *), then name again
        assert_eq!(validated.len(), 3);
        assert_eq!(validated[0].name, "id");
        assert_eq!(validated[1].name, "name");
        assert_eq!(validated[2].name, "name");
    }

    #[test]
    fn test_validate_where_column_exists() {
        let binder = Binder::new();
        let schema = Schema {
            columns: vec![
                Column {
                    name: "id".to_string(),
                    type_: ColumnType::Integer,
                    index: 0,
                },
                Column {
                    name: "name".to_string(),
                    type_: ColumnType::Varchar,
                    index: 1,
                },
            ],
        };

        let expr = Expression::Column("id".to_string());
        let result = binder.validate_where_expression(&expr, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_where_column_not_exists() {
        let binder = Binder::new();
        let schema = Schema {
            columns: vec![Column {
                name: "id".to_string(),
                type_: ColumnType::Integer,
                index: 0,
            }],
        };

        let expr = Expression::Column("nonexistent".to_string());
        let result = binder.validate_where_expression(&expr, &schema);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.message.contains("not found"));
    }

    #[test]
    fn test_validate_where_compatible_types() {
        let binder = Binder::new();
        let schema = Schema {
            columns: vec![Column {
                name: "age".to_string(),
                type_: ColumnType::Integer,
                index: 0,
            }],
        };

        // age > 18 (Integer > Integer) - should work
        let expr = Expression::GreaterThan(
            Box::new(Expression::Column("age".to_string())),
            Box::new(Expression::Literal(LiteralValue::Integer(18))),
        );
        let result = binder.validate_where_expression(&expr, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_where_incompatible_types() {
        let binder = Binder::new();
        let schema = Schema {
            columns: vec![Column {
                name: "name".to_string(),
                type_: ColumnType::Varchar,
                index: 0,
            }],
        };

        // name > 1 (Varchar > Integer) - should fail
        let expr = Expression::GreaterThan(
            Box::new(Expression::Column("name".to_string())),
            Box::new(Expression::Literal(LiteralValue::Integer(1))),
        );
        let result = binder.validate_where_expression(&expr, &schema);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.message.contains("Cannot compare"));
        assert!(error.message.contains("Varchar"));
        assert!(error.message.contains("Integer"));
    }

    #[test]
    fn test_validate_where_integer_float_compatible() {
        let binder = Binder::new();
        let schema = Schema {
            columns: vec![Column {
                name: "age".to_string(),
                type_: ColumnType::Integer,
                index: 0,
            }],
        };

        // age > 18.5 (Integer > Float) - should work (numeric comparison)
        let expr = Expression::GreaterThan(
            Box::new(Expression::Column("age".to_string())),
            Box::new(Expression::Literal(LiteralValue::Float(18.5))),
        );
        let result = binder.validate_where_expression(&expr, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_bind_expression_column_ref() {
        let binder = Binder::new();
        let schema = Schema {
            columns: vec![
                Column {
                    name: "age".to_string(),
                    type_: ColumnType::Integer,
                    index: 2,
                },
                Column {
                    name: "name".to_string(),
                    type_: ColumnType::Varchar,
                    index: 0,
                },
            ],
        };

        // bind a column reference
        let expr = Expression::Column("age".to_string());
        let bound = binder.bind_expression(&expr, &schema).unwrap();

        match bound {
            BoundExpression::ColumnRef { name, index, type_ } => {
                assert_eq!(name, "age");
                assert_eq!(index, 2);
                assert_eq!(type_, ColumnType::Integer);
            }
            _ => panic!("Expected ColumnRef"),
        }
    }

    #[test]
    fn test_bind_expression_literal() {
        let binder = Binder::new();
        let schema = Schema { columns: vec![] };

        // bind an integer literal
        let expr = Expression::Literal(LiteralValue::Integer(42));
        let bound = binder.bind_expression(&expr, &schema).unwrap();

        match bound {
            BoundExpression::Literal { value, type_ } => {
                assert_eq!(value, LiteralValue::Integer(42));
                assert_eq!(type_, ColumnType::Integer);
            }
            _ => panic!("Expected Literal"),
        }
    }

    #[test]
    fn test_bind_expression_comparison() {
        let binder = Binder::new();
        let schema = Schema {
            columns: vec![Column {
                name: "age".to_string(),
                type_: ColumnType::Integer,
                index: 0,
            }],
        };

        // bind a comparison: age > 25
        let expr = Expression::GreaterThan(
            Box::new(Expression::Column("age".to_string())),
            Box::new(Expression::Literal(LiteralValue::Integer(25))),
        );
        let bound = binder.bind_expression(&expr, &schema).unwrap();

        match bound {
            BoundExpression::GreaterThan(left, right) => {
                // check left side (column)
                match *left {
                    BoundExpression::ColumnRef { name, index, type_ } => {
                        assert_eq!(name, "age");
                        assert_eq!(index, 0);
                        assert_eq!(type_, ColumnType::Integer);
                    }
                    _ => panic!("Expected ColumnRef on left"),
                }
                // check right side (literal)
                match *right {
                    BoundExpression::Literal { value, type_ } => {
                        assert_eq!(value, LiteralValue::Integer(25));
                        assert_eq!(type_, ColumnType::Integer);
                    }
                    _ => panic!("Expected Literal on right"),
                }
            }
            _ => panic!("Expected GreaterThan"),
        }
    }

    #[test]
    fn test_bind_expression_nested_comparison() {
        let binder = Binder::new();
        let schema = Schema {
            columns: vec![
                Column {
                    name: "age".to_string(),
                    type_: ColumnType::Integer,
                    index: 0,
                },
                Column {
                    name: "score".to_string(),
                    type_: ColumnType::Float,
                    index: 1,
                },
            ],
        };

        // bind: age >= score (Integer >= Float)
        let expr = Expression::GreaterThanOrEqual(
            Box::new(Expression::Column("age".to_string())),
            Box::new(Expression::Column("score".to_string())),
        );
        let bound = binder.bind_expression(&expr, &schema).unwrap();

        match bound {
            BoundExpression::GreaterThanOrEqual(left, right) => {
                // check left side
                match *left {
                    BoundExpression::ColumnRef { name, index, type_ } => {
                        assert_eq!(name, "age");
                        assert_eq!(index, 0);
                        assert_eq!(type_, ColumnType::Integer);
                    }
                    _ => panic!("Expected ColumnRef on left"),
                }
                // check right side
                match *right {
                    BoundExpression::ColumnRef { name, index, type_ } => {
                        assert_eq!(name, "score");
                        assert_eq!(index, 1);
                        assert_eq!(type_, ColumnType::Float);
                    }
                    _ => panic!("Expected ColumnRef on right"),
                }
            }
            _ => panic!("Expected GreaterThanOrEqual"),
        }
    }

    #[test]
    fn test_bind_full_query() {
        // create a test CSV file
        let test_file = format!(
            "test_bind_{}.csv",
            TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
        );
        let _guard = TestFileGuard::new(test_file.clone());

        fs::write(&test_file, "id,name,age\n1,Alice,30\n2,Bob,25").unwrap();

        // parse a query
        let mut parser = Parser::new();
        let query = parser
            .parse(&format!("SELECT * FROM '{}' WHERE age > 25", test_file))
            .unwrap();

        // bind the query
        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        // verify bound query structure
        assert_eq!(bound_query.select_columns.len(), 3); // id, name, age
        assert_eq!(bound_query.select_columns[0].name, "id");
        assert_eq!(bound_query.select_columns[1].name, "name");
        assert_eq!(bound_query.select_columns[2].name, "age");

        // verify WHERE clause is bound
        assert!(bound_query.where_clause.is_some());
        match &bound_query.where_clause {
            Some(BoundExpression::GreaterThan(left, _right)) => {
                match left.as_ref() {
                    BoundExpression::ColumnRef { name, index, type_ } => {
                        assert_eq!(name, "age");
                        assert_eq!(*index, 2); // age is at index 2
                        assert_eq!(*type_, ColumnType::Integer);
                    }
                    _ => panic!("Expected ColumnRef in WHERE clause"),
                }
            }
            _ => panic!("Expected GreaterThan in WHERE clause"),
        }
    }

    // ===== LIMIT/OFFSET Binding Tests =====

    #[test]
    fn test_bind_query_with_limit() {
        let test_file = format!(
            "test_bind_limit_{}.csv",
            TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
        );
        let _guard = TestFileGuard::new(test_file.clone());
        fs::write(&test_file, "id,name\n1,Alice\n2,Bob").unwrap();

        let mut parser = Parser::new();
        let query = parser
            .parse(&format!("SELECT * FROM '{}' LIMIT 10", test_file))
            .unwrap();

        let binder = Binder::new();
        let result = binder.bind(query);
        assert!(result.is_ok());
        let bound = result.unwrap();
        assert_eq!(bound.limit, Some(10));
        assert_eq!(bound.offset, None);
    }

    #[test]
    fn test_bind_query_with_offset() {
        let test_file = format!(
            "test_bind_offset_{}.csv",
            TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
        );
        let _guard = TestFileGuard::new(test_file.clone());
        fs::write(&test_file, "id,name\n1,Alice\n2,Bob").unwrap();

        let mut parser = Parser::new();
        let query = parser
            .parse(&format!("SELECT * FROM '{}' OFFSET 5", test_file))
            .unwrap();

        let binder = Binder::new();
        let result = binder.bind(query);
        assert!(result.is_ok());
        let bound = result.unwrap();
        assert_eq!(bound.limit, None);
        assert_eq!(bound.offset, Some(5));
    }

    #[test]
    fn test_bind_query_with_limit_and_offset() {
        let test_file = format!(
            "test_bind_limit_offset_{}.csv",
            TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
        );
        let _guard = TestFileGuard::new(test_file.clone());
        fs::write(&test_file, "id,name\n1,Alice\n2,Bob").unwrap();

        let mut parser = Parser::new();
        let query = parser
            .parse(&format!("SELECT * FROM '{}' LIMIT 20 OFFSET 10", test_file))
            .unwrap();

        let binder = Binder::new();
        let result = binder.bind(query);
        assert!(result.is_ok());
        let bound = result.unwrap();
        assert_eq!(bound.limit, Some(20));
        assert_eq!(bound.offset, Some(10));
    }

    // ===== COUNT Aggregate Binding Tests =====

    #[test]
    fn test_bind_count_star() {
        let test_file = format!(
            "test_bind_count_star_{}.csv",
            TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
        );
        let _guard = TestFileGuard::new(test_file.clone());
        fs::write(&test_file, "id,name\n1,Alice\n2,Bob").unwrap();

        let mut parser = Parser::new();
        let query = parser
            .parse(&format!("SELECT COUNT(*) FROM '{}'", test_file))
            .unwrap();

        let binder = Binder::new();
        let result = binder.bind(query);
        assert!(result.is_ok());
        let bound = result.unwrap();
        assert_eq!(bound.aggregates.len(), 1);
        assert_eq!(bound.select_columns.len(), 0); // no regular columns

        match &bound.aggregates[0] {
            BoundAggregateExpression::CountStar => {
                // success
            }
            _ => panic!("Expected CountStar"),
        }
    }

    #[test]
    fn test_bind_count_column_valid() {
        let test_file = format!(
            "test_bind_count_col_{}.csv",
            TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
        );
        let _guard = TestFileGuard::new(test_file.clone());
        fs::write(&test_file, "id,name\n1,Alice\n2,Bob").unwrap();

        let mut parser = Parser::new();
        let query = parser
            .parse(&format!("SELECT COUNT(name) FROM '{}'", test_file))
            .unwrap();

        let binder = Binder::new();
        let result = binder.bind(query);
        assert!(result.is_ok());
        let bound = result.unwrap();
        assert_eq!(bound.aggregates.len(), 1);

        match &bound.aggregates[0] {
            BoundAggregateExpression::Count { column } => {
                assert_eq!(column.name, "name");
            }
            _ => panic!("Expected Count"),
        }
    }

    #[test]
    fn test_bind_count_column_nonexistent() {
        let test_file = format!(
            "test_bind_count_invalid_{}.csv",
            TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
        );
        let _guard = TestFileGuard::new(test_file.clone());
        fs::write(&test_file, "id,name\n1,Alice\n2,Bob").unwrap();

        let mut parser = Parser::new();
        let query = parser
            .parse(&format!("SELECT COUNT(nonexistent) FROM '{}'", test_file))
            .unwrap();

        let binder = Binder::new();
        let result = binder.bind(query);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.message.contains("not found"));
        assert!(error.message.contains("nonexistent"));
    }

    #[test]
    fn test_bind_multiple_aggregates() {
        let test_file = format!(
            "test_bind_multi_agg_{}.csv",
            TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
        );
        let _guard = TestFileGuard::new(test_file.clone());
        fs::write(&test_file, "id,name\n1,Alice\n2,Bob").unwrap();

        // for now, we test binding when manually constructing multiple aggregates
        // parser might not support multiple aggregates yet, but binder should
        let mut parser = Parser::new();

        // parse two separate queries to validate the binding logic
        let query1 = parser
            .parse(&format!("SELECT COUNT(*) FROM '{}'", test_file))
            .unwrap();
        let query2 = parser
            .parse(&format!("SELECT COUNT(name) FROM '{}'", test_file))
            .unwrap();

        let binder = Binder::new();
        let result1 = binder.bind(query1);
        let result2 = binder.bind(query2);

        assert!(result1.is_ok());
        assert!(result2.is_ok());

        // both should have exactly 1 aggregate
        assert_eq!(result1.unwrap().aggregates.len(), 1);
        assert_eq!(result2.unwrap().aggregates.len(), 1);
    }

    #[test]
    fn test_bind_count_with_where() {
        let test_file = format!(
            "test_bind_count_where_{}.csv",
            TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
        );
        let _guard = TestFileGuard::new(test_file.clone());
        fs::write(&test_file, "id,name\n1,Alice\n2,Bob\n10,Charlie").unwrap();

        let mut parser = Parser::new();
        let query = parser
            .parse(&format!(
                "SELECT COUNT(id) FROM '{}' WHERE id > 5",
                test_file
            ))
            .unwrap();

        let binder = Binder::new();
        let result = binder.bind(query);
        assert!(result.is_ok());
        let bound = result.unwrap();
        assert_eq!(bound.aggregates.len(), 1);
        assert!(bound.where_clause.is_some());
    }

    #[test]
    fn test_bind_count_with_limit() {
        let test_file = format!(
            "test_bind_count_limit_{}.csv",
            TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
        );
        let _guard = TestFileGuard::new(test_file.clone());
        fs::write(&test_file, "id,name\n1,Alice\n2,Bob").unwrap();

        let mut parser = Parser::new();
        let query = parser
            .parse(&format!("SELECT COUNT(*) FROM '{}' LIMIT 1", test_file))
            .unwrap();

        let binder = Binder::new();
        let result = binder.bind(query);
        assert!(result.is_ok());
        let bound = result.unwrap();
        assert_eq!(bound.aggregates.len(), 1);
        assert_eq!(bound.limit, Some(1));
    }
}
