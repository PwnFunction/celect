use celect::binder::{BoundAggregateExpression, BoundExpression};
use celect::{Binder, LogicalOperator, Parser, Planner};
use std::fs;
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
}

impl Drop for TestFileGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.file);
    }
}

#[test]
fn test_plan_simple_select() {
    let test_file = format!(
        "test_plan_simple_{}.csv",
        TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
    );
    let _guard = TestFileGuard::new(test_file.clone());
    fs::write(&test_file, "id,name\n1,Alice").unwrap();

    let mut parser = Parser::new();
    let query = parser
        .parse(&format!("SELECT name FROM '{}'", test_file))
        .unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let plan = planner.plan(bound_query);

    // expected plan: Projection -> Get
    match plan {
        LogicalOperator::Projection(proj) => {
            assert_eq!(proj.expressions.len(), 1);
            match &proj.expressions[0] {
                BoundExpression::ColumnRef { name, .. } => assert_eq!(name, "name"),
                _ => panic!("Expected ColumnRef in projection"),
            }

            match *proj.child {
                LogicalOperator::Get(get) => {
                    assert!(get.file_path.to_string_lossy().contains(&test_file));
                    assert_eq!(get.columns.len(), 2); // id, name
                }
                _ => panic!("Expected Get as child of Projection"),
            }
        }
        _ => panic!("Expected Projection as root"),
    }
}

#[test]
fn test_plan_select_where() {
    let test_file = format!(
        "test_plan_where_{}.csv",
        TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
    );
    let _guard = TestFileGuard::new(test_file.clone());
    fs::write(&test_file, "id,age\n1,30").unwrap();

    let mut parser = Parser::new();
    let query = parser
        .parse(&format!("SELECT id FROM '{}' WHERE age > 20", test_file))
        .unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let plan = planner.plan(bound_query);

    // expected plan: Projection -> Filter -> Get
    match plan {
        LogicalOperator::Projection(proj) => {
            match *proj.child {
                LogicalOperator::Filter(filter) => {
                    // check filter condition
                    match filter.expression {
                        BoundExpression::GreaterThan(left, right) => match (*left, *right) {
                            (
                                BoundExpression::ColumnRef { name, .. },
                                BoundExpression::Literal { .. },
                            ) => {
                                assert_eq!(name, "age");
                            }
                            _ => panic!("Expected Column > Literal"),
                        },
                        _ => panic!("Expected GreaterThan expression"),
                    }

                    match *filter.child {
                        LogicalOperator::Get(get) => {
                            assert!(get.file_path.to_string_lossy().contains(&test_file));
                        }
                        _ => panic!("Expected Get as child of Filter"),
                    }
                }
                _ => panic!("Expected Filter as child of Projection"),
            }
        }
        _ => panic!("Expected Projection as root"),
    }
}

// ===== LIMIT/OFFSET Planning Tests =====

#[test]
fn test_plan_with_limit() {
    let test_file = format!(
        "test_plan_limit_{}.csv",
        TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
    );
    let _guard = TestFileGuard::new(test_file.clone());
    fs::write(&test_file, "id,name\n1,Alice\n2,Bob").unwrap();

    let mut parser = Parser::new();
    let query = parser
        .parse(&format!("SELECT name FROM '{}' LIMIT 10", test_file))
        .unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let plan = planner.plan(bound_query);

    // expected plan: Limit -> Projection -> Get
    match plan {
        LogicalOperator::Limit(limit_op) => {
            assert_eq!(limit_op.limit, Some(10));
            assert_eq!(limit_op.offset, None);
            assert!(matches!(*limit_op.child, LogicalOperator::Projection(_)));
        }
        _ => panic!("Expected Limit as root"),
    }
}

#[test]
fn test_plan_with_offset() {
    let test_file = format!(
        "test_plan_offset_{}.csv",
        TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
    );
    let _guard = TestFileGuard::new(test_file.clone());
    fs::write(&test_file, "id,name\n1,Alice\n2,Bob").unwrap();

    let mut parser = Parser::new();
    let query = parser
        .parse(&format!("SELECT name FROM '{}' OFFSET 5", test_file))
        .unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let plan = planner.plan(bound_query);

    // expected plan: Limit -> Projection -> Get
    match plan {
        LogicalOperator::Limit(limit_op) => {
            assert_eq!(limit_op.limit, None);
            assert_eq!(limit_op.offset, Some(5));
        }
        _ => panic!("Expected Limit as root"),
    }
}

#[test]
fn test_plan_with_limit_and_offset() {
    let test_file = format!(
        "test_plan_limit_offset_{}.csv",
        TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
    );
    let _guard = TestFileGuard::new(test_file.clone());
    fs::write(&test_file, "id,name\n1,Alice\n2,Bob").unwrap();

    let mut parser = Parser::new();
    let query = parser
        .parse(&format!(
            "SELECT name FROM '{}' LIMIT 10 OFFSET 5",
            test_file
        ))
        .unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let plan = planner.plan(bound_query);

    // expected plan: Limit -> Projection -> Get
    match plan {
        LogicalOperator::Limit(limit_op) => {
            assert_eq!(limit_op.limit, Some(10));
            assert_eq!(limit_op.offset, Some(5));
        }
        _ => panic!("Expected Limit as root"),
    }
}

// ===== COUNT Aggregate Planning Tests =====

#[test]
fn test_plan_count_star() {
    let test_file = format!(
        "test_plan_count_star_{}.csv",
        TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
    );
    let _guard = TestFileGuard::new(test_file.clone());
    fs::write(&test_file, "id,name\n1,Alice\n2,Bob").unwrap();

    let mut parser = Parser::new();
    let query = parser
        .parse(&format!("SELECT COUNT(*) FROM '{}'", test_file))
        .unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let plan = planner.plan(bound_query);

    // expected plan: Aggregate -> Get
    match plan {
        LogicalOperator::Aggregate(agg) => {
            assert_eq!(agg.aggregates.len(), 1);
            assert!(matches!(
                agg.aggregates[0],
                BoundAggregateExpression::CountStar
            ));
            assert!(matches!(*agg.child, LogicalOperator::Get(_)));
        }
        _ => panic!("Expected Aggregate as root"),
    }
}

#[test]
fn test_plan_count_column() {
    let test_file = format!(
        "test_plan_count_col_{}.csv",
        TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
    );
    let _guard = TestFileGuard::new(test_file.clone());
    fs::write(&test_file, "id,name\n1,Alice\n2,Bob").unwrap();

    let mut parser = Parser::new();
    let query = parser
        .parse(&format!("SELECT COUNT(name) FROM '{}'", test_file))
        .unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let plan = planner.plan(bound_query);

    // expected plan: Aggregate -> Get
    match plan {
        LogicalOperator::Aggregate(agg) => {
            assert_eq!(agg.aggregates.len(), 1);
            match &agg.aggregates[0] {
                BoundAggregateExpression::Count { column } => {
                    assert_eq!(column.name, "name");
                }
                _ => panic!("Expected Count aggregate"),
            }
            assert!(matches!(*agg.child, LogicalOperator::Get(_)));
        }
        _ => panic!("Expected Aggregate as root"),
    }
}

#[test]
fn test_plan_count_with_where() {
    let test_file = format!(
        "test_plan_count_where_{}.csv",
        TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
    );
    let _guard = TestFileGuard::new(test_file.clone());
    fs::write(&test_file, "id,age\n1,25\n2,35").unwrap();

    let mut parser = Parser::new();
    let query = parser
        .parse(&format!(
            "SELECT COUNT(*) FROM '{}' WHERE age > 30",
            test_file
        ))
        .unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let plan = planner.plan(bound_query);

    // expected plan: Aggregate -> Filter -> Get
    match plan {
        LogicalOperator::Aggregate(agg) => {
            assert_eq!(agg.aggregates.len(), 1);
            match *agg.child {
                LogicalOperator::Filter(filter) => {
                    assert!(matches!(
                        filter.expression,
                        BoundExpression::GreaterThan(_, _)
                    ));
                    assert!(matches!(*filter.child, LogicalOperator::Get(_)));
                }
                _ => panic!("Expected Filter as child of Aggregate"),
            }
        }
        _ => panic!("Expected Aggregate as root"),
    }
}

#[test]
fn test_plan_count_with_limit() {
    let test_file = format!(
        "test_plan_count_limit_{}.csv",
        TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
    );
    let _guard = TestFileGuard::new(test_file.clone());
    fs::write(&test_file, "id,name\n1,Alice\n2,Bob").unwrap();

    let mut parser = Parser::new();
    let query = parser
        .parse(&format!("SELECT COUNT(*) FROM '{}' LIMIT 1", test_file))
        .unwrap();

    let binder = Binder::new();
    let bound_query = binder.bind(query).unwrap();

    let planner = Planner::new();
    let plan = planner.plan(bound_query);

    // expected plan: Limit -> Aggregate -> Get
    match plan {
        LogicalOperator::Limit(limit_op) => {
            assert_eq!(limit_op.limit, Some(1));
            match *limit_op.child {
                LogicalOperator::Aggregate(agg) => {
                    assert_eq!(agg.aggregates.len(), 1);
                    assert!(matches!(*agg.child, LogicalOperator::Get(_)));
                }
                _ => panic!("Expected Aggregate as child of Limit"),
            }
        }
        _ => panic!("Expected Limit as root"),
    }
}

#[test]
fn test_plan_multiple_aggregates() {
    let test_file = format!(
        "test_plan_multi_agg_{}.csv",
        TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
    );
    let _guard = TestFileGuard::new(test_file.clone());
    fs::write(&test_file, "id,name,age\n1,Alice,30\n2,Bob,25").unwrap();

    // note: This would need parser support for multiple aggregates, testing the planner's capability
    let planner = Planner::new();

    // create a bound query manually with multiple aggregates
    use celect::binder::BoundAggregateExpression;
    use celect::{BoundQuery, Column, ColumnType, Schema};
    use std::path::PathBuf;

    let id_column = Column {
        name: "id".to_string(),
        type_: ColumnType::Integer,
        index: 0,
    };

    let bound_query = BoundQuery {
        select_columns: vec![],
        file_path: PathBuf::from(&test_file),
        schema: Schema {
            columns: vec![
                id_column.clone(),
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
        },
        where_clause: None,
        limit: None,
        offset: None,
        aggregates: vec![
            BoundAggregateExpression::CountStar,
            BoundAggregateExpression::Count { column: id_column },
        ],
    };

    let plan = planner.plan(bound_query);

    // expected plan: Aggregate -> Get
    match plan {
        LogicalOperator::Aggregate(agg) => {
            assert_eq!(agg.aggregates.len(), 2);
            assert!(matches!(
                agg.aggregates[0],
                BoundAggregateExpression::CountStar
            ));
            assert!(matches!(
                agg.aggregates[1],
                BoundAggregateExpression::Count { .. }
            ));
        }
        _ => panic!("Expected Aggregate as root"),
    }
}
