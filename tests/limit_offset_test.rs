use celect::{Binder, Optimizer, Parser, Planner};
use celect::{PhysicalPlanner, PipelineExecutor};
use std::fs;
use std::io::Write;

struct TestFile {
    path: String,
}

impl TestFile {
    fn new(name: &str, contents: &str) -> Self {
        let path = format!("test_{}.csv", name);
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        TestFile { path }
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[test]
fn test_limit_only() {
    let test_file = TestFile::new(
        "limit_only",
        "name,age\nAlice,25\nBob,30\nCharlie,35\nDiana,40\nEve,45\n",
    );

    let sql = format!("SELECT name, age FROM '{}' LIMIT 3", test_file.path);
    let result = execute_query(&sql);
    assert!(result.is_ok(), "Query should execute successfully");

    let chunks = result.unwrap();
    let total_rows: usize = chunks.iter().map(|c| c.selected_count()).sum();
    assert_eq!(total_rows, 3, "Should return exactly 3 rows");
}

#[test]
fn test_limit_zero() {
    let test_file = TestFile::new("limit_zero", "name,age\nAlice,25\nBob,30\n");

    let sql = format!("SELECT name, age FROM '{}' LIMIT 0", test_file.path);
    let result = execute_query(&sql);
    assert!(result.is_ok(), "Query should execute successfully");

    let chunks = result.unwrap();
    let total_rows: usize = chunks.iter().map(|c| c.selected_count()).sum();
    assert_eq!(total_rows, 0, "Should return 0 rows with LIMIT 0");
}

#[test]
fn test_limit_larger_than_data() {
    let test_file = TestFile::new("limit_large", "name,age\nAlice,25\nBob,30\nCharlie,35\n");

    let sql = format!("SELECT name, age FROM '{}' LIMIT 10000", test_file.path);
    let result = execute_query(&sql);
    assert!(result.is_ok(), "Query should execute successfully");

    let chunks = result.unwrap();
    let total_rows: usize = chunks.iter().map(|c| c.selected_count()).sum();
    assert_eq!(
        total_rows, 3,
        "Should return all 3 rows when LIMIT exceeds data"
    );
}

#[test]
fn test_offset_only() {
    let test_file = TestFile::new(
        "offset_only",
        "name,age\nAlice,25\nBob,30\nCharlie,35\nDiana,40\nEve,45\nFrank,50\nGrace,55\nHenry,60\nIvy,65\nJack,70\n",
    );

    let sql = format!("SELECT name, age FROM '{}' OFFSET 5", test_file.path);
    let result = execute_query(&sql);
    assert!(result.is_ok(), "Query should execute successfully");

    let chunks = result.unwrap();
    let total_rows: usize = chunks.iter().map(|c| c.selected_count()).sum();
    assert_eq!(total_rows, 5, "Should return 5 rows after skipping first 5");
}

#[test]
fn test_offset_zero() {
    let test_file = TestFile::new("offset_zero", "name,age\nAlice,25\nBob,30\nCharlie,35\n");

    let sql = format!("SELECT name, age FROM '{}' OFFSET 0", test_file.path);
    let result = execute_query(&sql);
    assert!(result.is_ok(), "Query should execute successfully");

    let chunks = result.unwrap();
    let total_rows: usize = chunks.iter().map(|c| c.selected_count()).sum();
    assert_eq!(total_rows, 3, "OFFSET 0 should return all 3 rows");
}

#[test]
fn test_offset_larger_than_data() {
    let test_file = TestFile::new("offset_large", "name,age\nAlice,25\nBob,30\n");

    let sql = format!("SELECT name, age FROM '{}' OFFSET 10000", test_file.path);
    let result = execute_query(&sql);
    assert!(result.is_ok(), "Query should execute successfully");

    let chunks = result.unwrap();
    let total_rows: usize = chunks.iter().map(|c| c.selected_count()).sum();
    assert_eq!(
        total_rows, 0,
        "Should return 0 rows when offset exceeds data"
    );
}

#[test]
fn test_limit_and_offset() {
    let test_file = TestFile::new(
        "limit_offset",
        "name,age\nAlice,25\nBob,30\nCharlie,35\nDiana,40\nEve,45\nFrank,50\n",
    );

    let sql = format!(
        "SELECT name, age FROM '{}' LIMIT 3 OFFSET 2",
        test_file.path
    );
    let result = execute_query(&sql);
    assert!(result.is_ok(), "Query should execute successfully");

    let chunks = result.unwrap();
    let total_rows: usize = chunks.iter().map(|c| c.selected_count()).sum();
    assert_eq!(total_rows, 3, "Should return exactly 3 rows (LIMIT 3)");
}

#[test]
fn test_limit_and_offset_pagination() {
    let test_file = TestFile::new(
        "pagination",
        "name,age\nAlice,25\nBob,30\nCharlie,35\nDiana,40\nEve,45\nFrank,50\nGrace,55\nHenry,60\nIvy,65\n",
    );

    // test pagination scenario: Page 1 (rows 0-2)
    let sql_page1 = format!("SELECT name FROM '{}' LIMIT 3 OFFSET 0", test_file.path);
    let result1 = execute_query(&sql_page1);
    assert!(result1.is_ok());
    let chunks1 = result1.unwrap();
    let rows1: usize = chunks1.iter().map(|c| c.selected_count()).sum();
    assert_eq!(rows1, 3, "Page 1 should have 3 rows");

    // page 2 (rows 3-5)
    let sql_page2 = format!("SELECT name FROM '{}' LIMIT 3 OFFSET 3", test_file.path);
    let result2 = execute_query(&sql_page2);
    assert!(result2.is_ok());
    let chunks2 = result2.unwrap();
    let rows2: usize = chunks2.iter().map(|c| c.selected_count()).sum();
    assert_eq!(rows2, 3, "Page 2 should have 3 rows");

    // page 3 (rows 6-8)
    let sql_page3 = format!("SELECT name FROM '{}' LIMIT 3 OFFSET 6", test_file.path);
    let result3 = execute_query(&sql_page3);
    assert!(result3.is_ok());
    let chunks3 = result3.unwrap();
    let rows3: usize = chunks3.iter().map(|c| c.selected_count()).sum();
    assert_eq!(rows3, 3, "Page 3 should have 3 rows");
}

#[test]
fn test_limit_with_where_clause() {
    let test_file = TestFile::new(
        "limit_where",
        "name,age\nAlice,25\nBob,55\nCharlie,60\nDiana,65\nEve,70\nFrank,40\nGrace,75\n",
    );

    let sql = format!(
        "SELECT name, age FROM '{}' WHERE age > 50 LIMIT 3",
        test_file.path
    );
    let result = execute_query(&sql);
    assert!(result.is_ok(), "Query with WHERE and LIMIT should execute");

    let chunks = result.unwrap();
    let total_rows: usize = chunks.iter().map(|c| c.selected_count()).sum();
    assert_eq!(
        total_rows, 3,
        "Should return exactly 3 rows from filtered results"
    );
}

#[test]
fn test_offset_with_where_clause() {
    let test_file = TestFile::new(
        "offset_where",
        "name,age\nAlice,25\nBob,55\nCharlie,60\nDiana,65\nEve,70\nFrank,40\n",
    );

    let sql = format!(
        "SELECT name, age FROM '{}' WHERE age > 50 OFFSET 2",
        test_file.path
    );
    let result = execute_query(&sql);
    assert!(result.is_ok(), "Query with WHERE and OFFSET should execute");

    let chunks = result.unwrap();
    let total_rows: usize = chunks.iter().map(|c| c.selected_count()).sum();
    assert_eq!(
        total_rows, 2,
        "Should return 2 rows after skipping first 2 of 4 filtered results"
    );
}

#[test]
fn test_limit_offset_with_where_clause() {
    let test_file = TestFile::new(
        "all_three",
        "name,age\nAlice,25\nBob,55\nCharlie,60\nDiana,65\nEve,70\nFrank,75\n",
    );

    let sql = format!(
        "SELECT name, age FROM '{}' WHERE age > 50 LIMIT 2 OFFSET 1",
        test_file.path
    );
    let result = execute_query(&sql);
    assert!(
        result.is_ok(),
        "Query with WHERE, LIMIT, and OFFSET should execute"
    );

    let chunks = result.unwrap();
    let total_rows: usize = chunks.iter().map(|c| c.selected_count()).sum();
    assert_eq!(
        total_rows, 2,
        "Should return 2 rows: skip 1, take 2 from 5 filtered results"
    );
}

#[test]
fn test_limit_offset_all_columns() {
    let test_file = TestFile::new(
        "select_star",
        "name,age\nAlice,25\nBob,30\nCharlie,35\nDiana,40\nEve,45\nFrank,50\nGrace,55\n",
    );

    let sql = format!("SELECT * FROM '{}' LIMIT 5 OFFSET 2", test_file.path);
    let result = execute_query(&sql);
    assert!(
        result.is_ok(),
        "Query with SELECT * should work with LIMIT/OFFSET"
    );

    let chunks = result.unwrap();
    let total_rows: usize = chunks.iter().map(|c| c.selected_count()).sum();
    assert_eq!(total_rows, 5, "Should return exactly 5 rows");
}

#[test]
fn test_limit_one() {
    let test_file = TestFile::new("limit_one", "name,age\nAlice,25\nBob,30\n");

    let sql = format!("SELECT name FROM '{}' LIMIT 1", test_file.path);
    let result = execute_query(&sql);
    assert!(result.is_ok(), "LIMIT 1 should work");

    let chunks = result.unwrap();
    let total_rows: usize = chunks.iter().map(|c| c.selected_count()).sum();
    assert_eq!(total_rows, 1, "Should return exactly 1 row");
}

#[test]
fn test_offset_equals_total_rows() {
    let test_file = TestFile::new("offset_exact", "name,age\nAlice,25\nBob,30\nCharlie,35\n");

    let sql = format!("SELECT name FROM '{}' OFFSET 3", test_file.path);
    let result = execute_query(&sql);
    assert!(result.is_ok(), "OFFSET equal to total rows should succeed");

    let chunks = result.unwrap();
    let total_rows: usize = chunks.iter().map(|c| c.selected_count()).sum();
    assert_eq!(
        total_rows, 0,
        "OFFSET equal to total rows should return empty result"
    );
}

#[test]
fn test_limit_offset_boundary() {
    let test_file = TestFile::new(
        "boundary",
        "name,age\nAlice,25\nBob,30\nCharlie,35\nDiana,40\nEve,45\nFrank,50\nGrace,55\nHenry,60\nIvy,65\nJack,70\n",
    );

    // test boundary: get last row only
    let sql = format!("SELECT name FROM '{}' LIMIT 1 OFFSET 9", test_file.path);
    let result = execute_query(&sql);
    assert!(result.is_ok(), "Boundary test should succeed");

    let chunks = result.unwrap();
    let total_rows: usize = chunks.iter().map(|c| c.selected_count()).sum();
    assert_eq!(total_rows, 1, "Should return the last row");
}

#[test]
fn test_limit_offset_complex_filter() {
    let test_file = TestFile::new(
        "complex",
        "name,age\nAlice,25\nBob,35\nCharlie,40\nAlice,45\nDiana,50\nEve,55\n",
    );

    let sql = format!(
        "SELECT name, age FROM '{}' WHERE age > 30 LIMIT 2 OFFSET 1",
        test_file.path
    );
    let result = execute_query(&sql);
    assert!(
        result.is_ok(),
        "Complex query with LIMIT/OFFSET should work"
    );

    let chunks = result.unwrap();
    let total_rows: usize = chunks.iter().map(|c| c.selected_count()).sum();
    assert_eq!(total_rows, 2, "Should return 2 rows from filtered results");
}

// helper function to execute a query and return chunks
fn execute_query(sql: &str) -> Result<Vec<celect::DataChunk>, String> {
    // parse
    let mut parser = Parser::new();
    let query = parser.parse(sql).map_err(|e| e.message)?;

    // bind
    let binder = Binder::new();
    let bound_query = binder.bind(query).map_err(|e| e.message)?;

    // plan
    let planner = Planner::new();
    let logical_plan = planner.plan(bound_query);

    // optimize
    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(logical_plan);

    // physical plan
    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);

    // execute
    let mut executor = PipelineExecutor::new(operators, schemas);
    Ok(executor.execute())
}
