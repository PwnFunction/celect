use celect::{Binder, Optimizer, Parser, Planner};
use celect::{DataChunk, PhysicalPlanner, PipelineExecutor};
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;

fn create_test_csv(name: &str, content: &str) -> PathBuf {
    let file_path = std::env::temp_dir().join(format!("celect_test_{}.csv", name));
    let mut file = File::create(&file_path).unwrap();
    file.write_all(content.as_bytes()).unwrap();
    file_path
}

fn cleanup_test_csv(path: &PathBuf) {
    let _ = fs::remove_file(path);
}

fn execute_count_query(sql: &str) -> DataChunk {
    let mut parser = Parser::new();
    let query = parser.parse(sql).unwrap();

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

    // for aggregates, we should get a single chunk with one row
    assert_eq!(
        results.len(),
        1,
        "Expected single result chunk for aggregate"
    );
    results.into_iter().next().unwrap()
}

#[test]
fn test_count_star_basic() {
    let csv_content = "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n4,David,40\n5,Eve,28\n";
    let file_path = create_test_csv("count_star_basic", csv_content);

    let sql = format!("SELECT COUNT(*) FROM '{}'", file_path.display());
    let result = execute_count_query(&sql);

    assert_eq!(result.selected_count(), 1);
    if let Some(celect::Value::Integer(count)) = result.get_value(0, 0) {
        assert_eq!(count, 5);
    } else {
        panic!("Expected integer count");
    }

    cleanup_test_csv(&file_path);
}

#[test]
fn test_count_star_empty_table() {
    let csv_content = "id,name,age\n";
    let file_path = create_test_csv("count_star_empty", csv_content);

    let sql = format!("SELECT COUNT(*) FROM '{}'", file_path.display());
    let result = execute_count_query(&sql);

    assert_eq!(result.selected_count(), 1);
    if let Some(celect::Value::Integer(count)) = result.get_value(0, 0) {
        assert_eq!(count, 0);
    } else {
        panic!("Expected integer count");
    }

    cleanup_test_csv(&file_path);
}

#[test]
fn test_count_column_no_nulls() {
    let csv_content = "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n";
    let file_path = create_test_csv("count_no_nulls", csv_content);

    let sql = format!("SELECT COUNT(age) FROM '{}'", file_path.display());
    let result = execute_count_query(&sql);

    assert_eq!(result.selected_count(), 1);
    if let Some(celect::Value::Integer(count)) = result.get_value(0, 0) {
        assert_eq!(count, 3);
    } else {
        panic!("Expected integer count");
    }

    cleanup_test_csv(&file_path);
}

#[test]
fn test_count_column_with_nulls() {
    let csv_content = "id,name,age\n1,Alice,30\n2,Bob,\n3,Charlie,35\n4,David,\n5,Eve,28\n";
    let file_path = create_test_csv("count_with_nulls", csv_content);

    let sql = format!("SELECT COUNT(age) FROM '{}'", file_path.display());
    let result = execute_count_query(&sql);

    assert_eq!(result.selected_count(), 1);
    if let Some(celect::Value::Integer(count)) = result.get_value(0, 0) {
        assert_eq!(count, 3); // only non-NULL ages
    } else {
        panic!("Expected integer count");
    }

    cleanup_test_csv(&file_path);
}

#[test]
fn test_count_star_vs_count_column() {
    let csv_content = "id,name,age\n1,Alice,30\n2,Bob,\n3,Charlie,35\n4,David,\n5,Eve,28\n";
    let file_path = create_test_csv("count_star_vs_col", csv_content);

    // count(*) should count all rows
    let sql_star = format!("SELECT COUNT(*) FROM '{}'", file_path.display());
    let result_star = execute_count_query(&sql_star);
    if let Some(celect::Value::Integer(count)) = result_star.get_value(0, 0) {
        assert_eq!(count, 5);
    } else {
        panic!("Expected integer count");
    }

    // count(age) should only count non-NULL ages
    let sql_col = format!("SELECT COUNT(age) FROM '{}'", file_path.display());
    let result_col = execute_count_query(&sql_col);
    if let Some(celect::Value::Integer(count)) = result_col.get_value(0, 0) {
        assert_eq!(count, 3);
    } else {
        panic!("Expected integer count");
    }

    cleanup_test_csv(&file_path);
}

#[test]
fn test_count_star_large_dataset() {
    // create a CSV with 10,000 rows
    let mut csv_content = String::from("id,value\n");
    for i in 0..10000 {
        csv_content.push_str(&format!("{},{}\n", i, i * 2));
    }
    let file_path = create_test_csv("count_large", &csv_content);

    let sql = format!("SELECT COUNT(*) FROM '{}'", file_path.display());
    let result = execute_count_query(&sql);

    assert_eq!(result.selected_count(), 1);
    if let Some(celect::Value::Integer(count)) = result.get_value(0, 0) {
        assert_eq!(count, 10000);
    } else {
        panic!("Expected integer count");
    }

    cleanup_test_csv(&file_path);
}

#[test]
fn test_count_column_all_nulls() {
    let csv_content = "id,name,age\n1,Alice,\n2,Bob,\n3,Charlie,\n";
    let file_path = create_test_csv("count_all_nulls", csv_content);

    let sql = format!("SELECT COUNT(age) FROM '{}'", file_path.display());
    let result = execute_count_query(&sql);

    assert_eq!(result.selected_count(), 1);
    if let Some(celect::Value::Integer(count)) = result.get_value(0, 0) {
        assert_eq!(count, 0); // all NULL values
    } else {
        panic!("Expected integer count");
    }

    cleanup_test_csv(&file_path);
}

#[test]
fn test_count_with_where_clause() {
    let csv_content = "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n4,David,40\n5,Eve,28\n";
    let file_path = create_test_csv("count_with_where", csv_content);

    let sql = format!(
        "SELECT COUNT(*) FROM '{}' WHERE age > 30",
        file_path.display()
    );
    let result = execute_count_query(&sql);

    assert_eq!(result.selected_count(), 1);
    if let Some(celect::Value::Integer(count)) = result.get_value(0, 0) {
        assert_eq!(count, 2); // charlie (35) and David (40)
    } else {
        panic!("Expected integer count");
    }

    cleanup_test_csv(&file_path);
}

#[test]
fn test_count_column_with_where_clause() {
    let csv_content = "id,name,age\n1,Alice,30\n2,Bob,\n3,Charlie,35\n4,David,\n5,Eve,28\n";
    let file_path = create_test_csv("count_col_where", csv_content);

    // count non-NULL ages where id > 2
    let sql = format!(
        "SELECT COUNT(age) FROM '{}' WHERE id > 2",
        file_path.display()
    );
    let result = execute_count_query(&sql);

    assert_eq!(result.selected_count(), 1);
    if let Some(celect::Value::Integer(count)) = result.get_value(0, 0) {
        assert_eq!(count, 2); // charlie (35) and Eve (28), David has NULL
    } else {
        panic!("Expected integer count");
    }

    cleanup_test_csv(&file_path);
}
