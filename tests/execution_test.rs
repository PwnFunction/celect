use celect::Value;
use celect::{Binder, Optimizer, Parser, PhysicalPlanner, PipelineExecutor, Planner};
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
fn test_simple_select_all() {
    let test_file = TestFile::new("simple_select", "id,name,age\n1,Alice,30\n2,Bob,25\n");

    let sql = format!("SELECT id, name, age FROM '{}'", test_file.path);
    let mut parser = Parser::new();
    let query = parser.parse(&sql).unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let logical_plan = planner.plan(bound_query);

    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(logical_plan);

    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);

    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].count, 2);

    // check first row
    assert_eq!(results[0].get_value(0, 0), Some(Value::Integer(1)));
    assert_eq!(
        results[0].get_value(1, 0),
        Some(Value::Varchar("Alice".to_string()))
    );
    assert_eq!(results[0].get_value(2, 0), Some(Value::Integer(30)));

    // check second row
    assert_eq!(results[0].get_value(0, 1), Some(Value::Integer(2)));
    assert_eq!(
        results[0].get_value(1, 1),
        Some(Value::Varchar("Bob".to_string()))
    );
    assert_eq!(results[0].get_value(2, 1), Some(Value::Integer(25)));
}

#[test]
fn test_select_with_projection() {
    let test_file = TestFile::new("projection", "id,name,age\n1,Alice,30\n2,Bob,25\n");

    let sql = format!("SELECT name FROM '{}'", test_file.path);
    let mut parser = Parser::new();
    let query = parser.parse(&sql).unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let logical_plan = planner.plan(bound_query);

    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(logical_plan);

    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);

    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].count, 2);
    assert_eq!(results[0].column_count(), 1); // only 'name' column

    assert_eq!(
        results[0].get_value(0, 0),
        Some(Value::Varchar("Alice".to_string()))
    );
    assert_eq!(
        results[0].get_value(0, 1),
        Some(Value::Varchar("Bob".to_string()))
    );
}

#[test]
fn test_simple_filter() {
    let test_file = TestFile::new(
        "filter",
        "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n",
    );

    let sql = format!("SELECT name, age FROM '{}' WHERE age > 25", test_file.path);
    let mut parser = Parser::new();
    let query = parser.parse(&sql).unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let logical_plan = planner.plan(bound_query);

    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(logical_plan);

    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);

    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].count, 2); // alice and Charlie

    assert_eq!(
        results[0].get_value(0, 0),
        Some(Value::Varchar("Alice".to_string()))
    );
    assert_eq!(results[0].get_value(1, 0), Some(Value::Integer(30)));

    assert_eq!(
        results[0].get_value(0, 1),
        Some(Value::Varchar("Charlie".to_string()))
    );
    assert_eq!(results[0].get_value(1, 1), Some(Value::Integer(35)));
}

#[test]
fn test_filter_with_equality() {
    let test_file = TestFile::new(
        "eq_filter",
        "id,name,age\n1,Alice,30\n2,Bob,25\n3,Alice,35\n",
    );

    let sql = format!("SELECT age FROM '{}' WHERE name = 'Alice'", test_file.path);
    let mut parser = Parser::new();
    let query = parser.parse(&sql).unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let logical_plan = planner.plan(bound_query);

    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(logical_plan);

    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);

    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].count, 2); // two Alices

    assert_eq!(results[0].get_value(0, 0), Some(Value::Integer(30)));
    assert_eq!(results[0].get_value(0, 1), Some(Value::Integer(35)));
}

#[test]
fn test_filter_with_and() {
    let test_file = TestFile::new(
        "and_filter",
        "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n",
    );

    let sql = format!(
        "SELECT name FROM '{}' WHERE age > 25 AND age < 35",
        test_file.path
    );
    let mut parser = Parser::new();
    let query = parser.parse(&sql).unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let logical_plan = planner.plan(bound_query);

    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(logical_plan);

    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);

    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].count, 1); // only Alice (30)

    assert_eq!(
        results[0].get_value(0, 0),
        Some(Value::Varchar("Alice".to_string()))
    );
}

#[test]
fn test_filter_with_or() {
    let test_file = TestFile::new(
        "or_filter",
        "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n",
    );

    let sql = format!(
        "SELECT name FROM '{}' WHERE age = 25 OR age = 35",
        test_file.path
    );
    let mut parser = Parser::new();
    let query = parser.parse(&sql).unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let logical_plan = planner.plan(bound_query);

    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(logical_plan);

    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);

    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].count, 2); // bob and Charlie

    assert_eq!(
        results[0].get_value(0, 0),
        Some(Value::Varchar("Bob".to_string()))
    );
    assert_eq!(
        results[0].get_value(0, 1),
        Some(Value::Varchar("Charlie".to_string()))
    );
}

#[test]
fn test_filter_with_not() {
    let test_file = TestFile::new(
        "not_filter",
        "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n",
    );

    let sql = format!("SELECT name FROM '{}' WHERE NOT (age = 25)", test_file.path);
    let mut parser = Parser::new();
    let query = parser.parse(&sql).unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let logical_plan = planner.plan(bound_query);

    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(logical_plan);

    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);

    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].count, 2); // alice and Charlie (not Bob)

    assert_eq!(
        results[0].get_value(0, 0),
        Some(Value::Varchar("Alice".to_string()))
    );
    assert_eq!(
        results[0].get_value(0, 1),
        Some(Value::Varchar("Charlie".to_string()))
    );
}

#[test]
fn test_empty_result() {
    let test_file = TestFile::new("empty", "id,name,age\n1,Alice,30\n2,Bob,25\n");

    let sql = format!("SELECT name FROM '{}' WHERE age > 100", test_file.path);
    let mut parser = Parser::new();
    let query = parser.parse(&sql).unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let logical_plan = planner.plan(bound_query);

    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(logical_plan);

    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);

    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();

    assert_eq!(results.len(), 0); // no results
}

#[test]
fn test_multiple_types() {
    let test_file = TestFile::new(
        "types",
        "id,name,salary,active\n1,Alice,50000.5,true\n2,Bob,45000.0,false\n",
    );

    let sql = format!(
        "SELECT name, salary, active FROM '{}' WHERE active = true",
        test_file.path
    );
    let mut parser = Parser::new();
    let query = parser.parse(&sql).unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let logical_plan = planner.plan(bound_query);

    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(logical_plan);

    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);

    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].count, 1); // only Alice

    assert_eq!(
        results[0].get_value(0, 0),
        Some(Value::Varchar("Alice".to_string()))
    );
    assert_eq!(results[0].get_value(1, 0), Some(Value::Float(50000.5)));
    assert_eq!(results[0].get_value(2, 0), Some(Value::Boolean(true)));
}

#[test]
fn test_large_batch() {
    // create a CSV with more than 2048 rows to test batching
    let mut csv_content = String::from("id,value\n");
    for i in 0..3000 {
        csv_content.push_str(&format!("{},{}\n", i, i * 2));
    }
    let test_file = TestFile::new("large", &csv_content);

    let sql = format!("SELECT value FROM '{}' WHERE id < 10", test_file.path);
    let mut parser = Parser::new();
    let query = parser.parse(&sql).unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let logical_plan = planner.plan(bound_query);

    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(logical_plan);

    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);

    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();

    // should have results from multiple batches
    let total_rows: usize = results.iter().map(|chunk| chunk.count).sum();
    assert_eq!(total_rows, 10);

    // verify first few values
    assert_eq!(results[0].get_value(0, 0), Some(Value::Integer(0)));
    assert_eq!(results[0].get_value(0, 1), Some(Value::Integer(2)));
    assert_eq!(results[0].get_value(0, 2), Some(Value::Integer(4)));
}

#[test]
fn test_string_comparison() {
    let test_file = TestFile::new("string_cmp", "id,name\n1,Alice\n2,Bob\n3,Charlie\n");

    let sql = format!("SELECT name FROM '{}' WHERE name >= 'Bob'", test_file.path);
    let mut parser = Parser::new();
    let query = parser.parse(&sql).unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let logical_plan = planner.plan(bound_query);

    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(logical_plan);

    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);

    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].count, 2); // bob and Charlie

    assert_eq!(
        results[0].get_value(0, 0),
        Some(Value::Varchar("Bob".to_string()))
    );
    assert_eq!(
        results[0].get_value(0, 1),
        Some(Value::Varchar("Charlie".to_string()))
    );
}

#[test]
fn test_complex_expression() {
    let test_file = TestFile::new(
        "complex",
        "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n4,Diana,28\n",
    );

    let sql = format!(
        "SELECT name FROM '{}' WHERE (age > 25 AND age < 35) OR name = 'Bob'",
        test_file.path
    );
    let mut parser = Parser::new();
    let query = parser.parse(&sql).unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let logical_plan = planner.plan(bound_query);

    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(logical_plan);

    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);

    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].count, 3); // alice (30), Bob (25), Diana (28)

    assert_eq!(
        results[0].get_value(0, 0),
        Some(Value::Varchar("Alice".to_string()))
    );
    assert_eq!(
        results[0].get_value(0, 1),
        Some(Value::Varchar("Bob".to_string()))
    );
    assert_eq!(
        results[0].get_value(0, 2),
        Some(Value::Varchar("Diana".to_string()))
    );
}

#[test]
fn test_dead_code_elimination_in_execution() {
    let test_file = TestFile::new("dead_code", "id,name,age\n1,Alice,30\n2,Bob,25\n");

    // this WHERE clause simplifies to just "age > 25"
    let sql = format!(
        "SELECT name FROM '{}' WHERE (1 = 1) AND age > 25",
        test_file.path
    );
    let mut parser = Parser::new();
    let query = parser.parse(&sql).unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let logical_plan = planner.plan(bound_query);

    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(logical_plan);

    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);

    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].count, 1); // only Alice
    assert_eq!(
        results[0].get_value(0, 0),
        Some(Value::Varchar("Alice".to_string()))
    );
}
