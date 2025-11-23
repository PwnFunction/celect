use celect::parser::LiteralValue;
use celect::planner::LogicalOperator;
use celect::{Binder, BoundExpression, Optimizer, Parser, Planner};
use std::fs;
use std::io::Write;
use std::path::PathBuf;

struct TestFileGuard {
    path: PathBuf,
}

impl TestFileGuard {
    fn new(name: &str, content: &str) -> Self {
        let path = PathBuf::from(name);
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        Self { path }
    }
}

impl Drop for TestFileGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_column_pruning() {
        // query: SELECT name FROM ... WHERE age > 25
        // should only read columns: name (index 1), age (index 2)

        let _guard = TestFileGuard::new(
            "test_optimizer_simple.csv",
            "id,name,age,alive\n1,Alice,30,true\n2,Bob,25,false\n",
        );

        let sql = "SELECT name FROM 'test_optimizer_simple.csv' WHERE age > 25";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        // before optimization: Get should have all 4 columns
        if let LogicalOperator::Projection(ref proj) = plan {
            if let LogicalOperator::Filter(ref filter) = *proj.child {
                if let LogicalOperator::Get(ref get) = *filter.child {
                    assert_eq!(
                        get.columns.len(),
                        4,
                        "Before optimization: should have all 4 columns"
                    );
                }
            }
        }

        // apply optimization
        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // after optimization: Get should only have 2 columns (name, age)
        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                if let LogicalOperator::Get(get) = *filter.child {
                    assert_eq!(
                        get.columns.len(),
                        2,
                        "After optimization: should have 2 columns"
                    );

                    // verify it's name and age
                    let names: Vec<&str> = get.columns.iter().map(|c| c.name.as_str()).collect();
                    assert!(names.contains(&"name"));
                    assert!(names.contains(&"age"));
                    assert!(!names.contains(&"id"));
                    assert!(!names.contains(&"alive"));
                } else {
                    panic!("Expected Get operator");
                }
            } else {
                panic!("Expected Filter operator");
            }
        } else {
            panic!("Expected Projection operator");
        }
    }

    #[test]
    fn test_complex_where_column_collection() {
        // query with complex WHERE: (age > 25 AND alive = true) OR NOT (id = 1)
        // should read: name, age (from SELECT), age, alive, id (from WHERE) = all 4 columns

        let _guard = TestFileGuard::new(
            "test_optimizer_complex.csv",
            "id,name,age,alive\n1,Alice,30,true\n2,Bob,25,false\n",
        );

        let sql = "SELECT name, age FROM 'test_optimizer_complex.csv' WHERE (age > 25 AND alive = true) OR NOT (id = 1)";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // should still have all 4 columns (all referenced in query)
        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                if let LogicalOperator::Get(get) = *filter.child {
                    assert_eq!(get.columns.len(), 4, "Complex WHERE references all columns");
                } else {
                    panic!("Expected Get operator");
                }
            } else {
                panic!("Expected Filter operator");
            }
        } else {
            panic!("Expected Projection operator");
        }
    }

    #[test]
    fn test_select_star_no_pruning() {
        // query: SELECT * FROM ... WHERE name = 'Alice'
        // should read all columns (SELECT * requires all)

        let _guard = TestFileGuard::new(
            "test_optimizer_star.csv",
            "id,name,age,alive\n1,Alice,30,true\n",
        );

        let sql = "SELECT * FROM 'test_optimizer_star.csv' WHERE name = 'Alice'";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // should have all 4 columns
        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                if let LogicalOperator::Get(get) = *filter.child {
                    assert_eq!(get.columns.len(), 4, "SELECT * requires all columns");
                } else {
                    panic!("Expected Get operator");
                }
            } else {
                panic!("Expected Filter operator");
            }
        } else {
            panic!("Expected Projection operator");
        }
    }

    #[test]
    fn test_no_where_clause_pruning() {
        // query: SELECT id, name FROM ...
        // should only read id and name

        let _guard = TestFileGuard::new(
            "test_optimizer_no_where.csv",
            "id,name,age,alive\n1,Alice,30,true\n",
        );

        let sql = "SELECT id, name FROM 'test_optimizer_no_where.csv'";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // should have 2 columns
        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Get(get) = *proj.child {
                assert_eq!(get.columns.len(), 2, "Should only have id and name");

                let names: Vec<&str> = get.columns.iter().map(|c| c.name.as_str()).collect();
                assert!(names.contains(&"id"));
                assert!(names.contains(&"name"));
            } else {
                panic!("Expected Get operator");
            }
        } else {
            panic!("Expected Projection operator");
        }
    }

    #[test]
    fn test_maximal_pruning() {
        // query: SELECT name FROM ... WHERE name = 'Alice'
        // should only read name (same column in SELECT and WHERE)

        let _guard = TestFileGuard::new(
            "test_optimizer_maximal.csv",
            "id,name,age,alive\n1,Alice,30,true\n",
        );

        let sql = "SELECT name FROM 'test_optimizer_maximal.csv' WHERE name = 'Alice'";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // should have only 1 column (name)
        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                if let LogicalOperator::Get(get) = *filter.child {
                    assert_eq!(
                        get.columns.len(),
                        1,
                        "Maximal pruning: only 1 column needed"
                    );
                    assert_eq!(get.columns[0].name, "name");
                } else {
                    panic!("Expected Get operator");
                }
            } else {
                panic!("Expected Filter operator");
            }
        } else {
            panic!("Expected Projection operator");
        }
    }

    #[test]
    fn test_dead_code_true_and_expression() {
        // where true AND age > 25 → WHERE age > 25
        let _guard = TestFileGuard::new("test_dead_code_and.csv", "id,name,age\n1,Alice,30\n");

        let sql = "SELECT name FROM 'test_dead_code_and.csv' WHERE true AND age > 25";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // filter should still exist but with simplified expression (no AND with true)
        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                // should be just age > 25, not (true AND age > 25)
                match filter.expression {
                    BoundExpression::GreaterThan(_, _) => {
                        // correct - simplified to just the comparison
                    }
                    BoundExpression::And(_, _) => {
                        panic!("AND with true should have been eliminated");
                    }
                    _ => panic!("Unexpected expression type"),
                }
            } else {
                panic!("Expected Filter operator");
            }
        } else {
            panic!("Expected Projection operator");
        }
    }

    #[test]
    fn test_dead_code_false_or_expression() {
        // where false OR name = 'Alice' → WHERE name = 'Alice'
        let _guard = TestFileGuard::new("test_dead_code_or.csv", "id,name,age\n1,Alice,30\n");

        let sql = "SELECT name FROM 'test_dead_code_or.csv' WHERE false OR name = 'Alice'";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // should be simplified to just name = 'Alice'
        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                match filter.expression {
                    BoundExpression::Equal(_, _) => {
                        // correct - simplified to just the comparison
                    }
                    BoundExpression::Or(_, _) => {
                        panic!("OR with false should have been eliminated");
                    }
                    _ => panic!("Unexpected expression type"),
                }
            } else {
                panic!("Expected Filter operator");
            }
        } else {
            panic!("Expected Projection operator");
        }
    }

    #[test]
    fn test_dead_code_not_true() {
        // where NOT true → WHERE false
        let _guard = TestFileGuard::new("test_dead_code_not.csv", "id,name,age\n1,Alice,30\n");

        let sql = "SELECT name FROM 'test_dead_code_not.csv' WHERE NOT true";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // should be simplified to false literal
        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                match filter.expression {
                    BoundExpression::Literal {
                        value: LiteralValue::Boolean(false),
                        ..
                    } => {
                        // correct - simplified to false
                    }
                    _ => panic!("NOT true should become false literal"),
                }
            } else {
                panic!("Expected Filter operator");
            }
        } else {
            panic!("Expected Projection operator");
        }
    }

    #[test]
    fn test_dead_code_filter_always_true() {
        // where true → Filter removed entirely!
        let _guard = TestFileGuard::new("test_dead_code_true.csv", "id,name,age\n1,Alice,30\n");

        let sql = "SELECT name FROM 'test_dead_code_true.csv' WHERE true";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // filter should be completely removed!
        if let LogicalOperator::Projection(proj) = optimized_plan {
            match *proj.child {
                LogicalOperator::Get(_) => {
                    // correct - Filter was removed, goes straight to Get
                }
                LogicalOperator::Filter(_) => {
                    panic!("Filter with constant true should have been removed");
                }
                _ => panic!("Unexpected operator"),
            }
        } else {
            panic!("Expected Projection operator");
        }
    }

    #[test]
    fn test_dead_code_complex_simplification() {
        // where (true AND age > 25) OR false → WHERE age > 25
        let _guard = TestFileGuard::new("test_dead_code_complex.csv", "id,name,age\n1,Alice,30\n");

        let sql =
            "SELECT name FROM 'test_dead_code_complex.csv' WHERE (true AND age > 25) OR false";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // should be simplified to just age > 25
        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                match filter.expression {
                    BoundExpression::GreaterThan(_, _) => {
                        // correct - fully simplified
                    }
                    _ => panic!("Should be simplified to just age > 25"),
                }
            } else {
                panic!("Expected Filter operator");
            }
        } else {
            panic!("Expected Projection operator");
        }
    }

    #[test]
    fn test_dead_code_false_and_anything() {
        // where false AND age > 25 → WHERE false
        let _guard =
            TestFileGuard::new("test_dead_code_false_and.csv", "id,name,age\n1,Alice,30\n");

        let sql = "SELECT name FROM 'test_dead_code_false_and.csv' WHERE false AND age > 25";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // should be simplified to false literal
        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                match filter.expression {
                    BoundExpression::Literal {
                        value: LiteralValue::Boolean(false),
                        ..
                    } => {
                        // correct - simplified to false (short-circuit)
                    }
                    _ => panic!("false AND anything should become false"),
                }
            } else {
                panic!("Expected Filter operator");
            }
        } else {
            panic!("Expected Projection operator");
        }
    }

    #[test]
    fn test_dead_code_expression_and_true() {
        // where age > 25 AND true → WHERE age > 25
        let _guard = TestFileGuard::new("test_dead_code_and_true.csv", "id,name,age\n1,Alice,30\n");

        let sql = "SELECT name FROM 'test_dead_code_and_true.csv' WHERE age > 25 AND true";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                match filter.expression {
                    BoundExpression::GreaterThan(_, _) => {
                        // correct - AND with true eliminated
                    }
                    _ => panic!("x AND true should simplify to x"),
                }
            }
        }
    }

    #[test]
    fn test_dead_code_expression_and_false() {
        // where age > 25 AND false → WHERE false
        let _guard =
            TestFileGuard::new("test_dead_code_and_false.csv", "id,name,age\n1,Alice,30\n");

        let sql = "SELECT name FROM 'test_dead_code_and_false.csv' WHERE age > 25 AND false";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                match filter.expression {
                    BoundExpression::Literal {
                        value: LiteralValue::Boolean(false),
                        ..
                    } => {
                        // correct - x AND false becomes false
                    }
                    _ => panic!("x AND false should simplify to false"),
                }
            }
        }
    }

    #[test]
    fn test_dead_code_true_or_expression() {
        // where true OR age > 25 → WHERE true
        let _guard = TestFileGuard::new("test_dead_code_true_or.csv", "id,name,age\n1,Alice,30\n");

        let sql = "SELECT name FROM 'test_dead_code_true_or.csv' WHERE true OR age > 25";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // true OR x → true, then filter removed
        if let LogicalOperator::Projection(proj) = optimized_plan {
            match *proj.child {
                LogicalOperator::Get(_) => {
                    // correct - true OR x becomes true, filter removed
                }
                _ => panic!("true OR x should become true and filter removed"),
            }
        }
    }

    #[test]
    fn test_dead_code_expression_or_true() {
        // where age > 25 OR true → WHERE true → filter removed
        let _guard = TestFileGuard::new("test_dead_code_or_true.csv", "id,name,age\n1,Alice,30\n");

        let sql = "SELECT name FROM 'test_dead_code_or_true.csv' WHERE age > 25 OR true";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // x OR true → true, then filter removed
        if let LogicalOperator::Projection(proj) = optimized_plan {
            match *proj.child {
                LogicalOperator::Get(_) => {
                    // correct - x OR true becomes true, filter removed
                }
                _ => panic!("x OR true should become true and filter removed"),
            }
        }
    }

    #[test]
    fn test_dead_code_expression_or_false() {
        // where age > 25 OR false → WHERE age > 25
        let _guard = TestFileGuard::new("test_dead_code_or_false.csv", "id,name,age\n1,Alice,30\n");

        let sql = "SELECT name FROM 'test_dead_code_or_false.csv' WHERE age > 25 OR false";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                match filter.expression {
                    BoundExpression::GreaterThan(_, _) => {
                        // correct - x OR false becomes x
                    }
                    _ => panic!("x OR false should simplify to x"),
                }
            }
        }
    }

    #[test]
    fn test_dead_code_not_false() {
        // where NOT false → WHERE true → filter removed
        let _guard =
            TestFileGuard::new("test_dead_code_not_false.csv", "id,name,age\n1,Alice,30\n");

        let sql = "SELECT name FROM 'test_dead_code_not_false.csv' WHERE NOT false";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // not false → true, filter removed
        if let LogicalOperator::Projection(proj) = optimized_plan {
            match *proj.child {
                LogicalOperator::Get(_) => {
                    // correct - NOT false becomes true, filter removed
                }
                _ => panic!("NOT false should become true and filter removed"),
            }
        }
    }

    #[test]
    fn test_constant_folding_integer_equal_true() {
        // where 1 = 1 → WHERE true → filter removed
        let _guard = TestFileGuard::new("test_const_int_eq_true.csv", "id,name\n1,Alice\n");

        let sql = "SELECT name FROM 'test_const_int_eq_true.csv' WHERE 1 = 1";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // 1 = 1 evaluates to true, filter removed
        if let LogicalOperator::Projection(proj) = optimized_plan {
            match *proj.child {
                LogicalOperator::Get(_) => {
                    // correct - constant comparison evaluated, filter removed
                }
                _ => panic!("WHERE 1 = 1 should evaluate to true and remove filter"),
            }
        }
    }

    #[test]
    fn test_constant_folding_integer_equal_false() {
        // where 1 = 2 → WHERE false
        let _guard = TestFileGuard::new("test_const_int_eq_false.csv", "id,name\n1,Alice\n");

        let sql = "SELECT name FROM 'test_const_int_eq_false.csv' WHERE 1 = 2";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // 1 = 2 evaluates to false
        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                match filter.expression {
                    BoundExpression::Literal {
                        value: LiteralValue::Boolean(false),
                        ..
                    } => {
                        // correct - constant comparison evaluated to false
                    }
                    _ => panic!("WHERE 1 = 2 should evaluate to false"),
                }
            }
        }
    }

    #[test]
    fn test_constant_folding_integer_greater_than() {
        // where 5 > 3 → WHERE true
        let _guard = TestFileGuard::new("test_const_int_gt.csv", "id,name\n1,Alice\n");

        let sql = "SELECT name FROM 'test_const_int_gt.csv' WHERE 5 > 3";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // 5 > 3 evaluates to true, filter removed
        if let LogicalOperator::Projection(proj) = optimized_plan {
            match *proj.child {
                LogicalOperator::Get(_) => {}
                _ => panic!("WHERE 5 > 3 should evaluate to true and remove filter"),
            }
        }
    }

    #[test]
    fn test_constant_folding_string_equal() {
        // where 'hello' = 'hello' → WHERE true
        let _guard = TestFileGuard::new("test_const_str_eq.csv", "id,name\n1,Alice\n");

        let sql = "SELECT name FROM 'test_const_str_eq.csv' WHERE 'hello' = 'hello'";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // 'hello' = 'hello' evaluates to true, filter removed
        if let LogicalOperator::Projection(proj) = optimized_plan {
            match *proj.child {
                LogicalOperator::Get(_) => {}
                _ => panic!("WHERE 'hello' = 'hello' should evaluate to true and remove filter"),
            }
        }
    }

    #[test]
    fn test_constant_folding_string_less_than() {
        // where 'apple' < 'banana' → WHERE true (lexicographic)
        let _guard = TestFileGuard::new("test_const_str_lt.csv", "id,name\n1,Alice\n");

        let sql = "SELECT name FROM 'test_const_str_lt.csv' WHERE 'apple' < 'banana'";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // 'apple' < 'banana' evaluates to true
        if let LogicalOperator::Projection(proj) = optimized_plan {
            match *proj.child {
                LogicalOperator::Get(_) => {}
                _ => panic!("WHERE 'apple' < 'banana' should evaluate to true and remove filter"),
            }
        }
    }

    #[test]
    fn test_constant_folding_with_and() {
        // where (1 = 1) AND age > 25 → WHERE true AND age > 25 → WHERE age > 25
        let _guard = TestFileGuard::new("test_const_fold_and.csv", "id,name,age\n1,Alice,30\n");

        let sql = "SELECT name FROM 'test_const_fold_and.csv' WHERE (1 = 1) AND age > 25";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // (1 = 1) becomes true, then true AND x becomes x
        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                match filter.expression {
                    BoundExpression::GreaterThan(_, _) => {
                        // correct - fully simplified to just age > 25
                    }
                    _ => panic!("Should simplify to just age > 25"),
                }
            }
        }
    }

    #[test]
    fn test_constant_folding_with_or() {
        // where (10 < 5) OR name = 'Alice' → WHERE false OR name = 'Alice' → WHERE name = 'Alice'
        let _guard = TestFileGuard::new("test_const_fold_or.csv", "id,name\n1,Alice\n");

        let sql = "SELECT name FROM 'test_const_fold_or.csv' WHERE (10 < 5) OR name = 'Alice'";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // (10 < 5) becomes false, then false OR x becomes x
        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                match filter.expression {
                    BoundExpression::Equal(_, _) => {
                        // correct - simplified to just name = 'Alice'
                    }
                    _ => panic!("Should simplify to just name = 'Alice'"),
                }
            }
        }
    }

    #[test]
    fn test_constant_folding_complex_chain() {
        // where (5 > 3) AND (2 < 10) OR (1 = 2)
        // → WHERE true AND true OR false
        // → WHERE true OR false
        // → WHERE true → filter removed
        let _guard = TestFileGuard::new("test_const_fold_complex.csv", "id,name\n1,Alice\n");

        let sql =
            "SELECT name FROM 'test_const_fold_complex.csv' WHERE (5 > 3) AND (2 < 10) OR (1 = 2)";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // complex chain simplifies to true, filter removed
        if let LogicalOperator::Projection(proj) = optimized_plan {
            match *proj.child {
                LogicalOperator::Get(_) => {
                    // correct - all constants evaluated and simplified to true
                }
                _ => {
                    panic!("Complex constant expression should evaluate to true and remove filter")
                }
            }
        }
    }

    #[test]
    fn test_constant_folding_null_equals_null() {
        // where NULL = NULL → WHERE false (SQL semantics)
        let _guard = TestFileGuard::new("test_const_null_eq.csv", "id,name\n1,Alice\n");

        let sql = "SELECT name FROM 'test_const_null_eq.csv' WHERE NULL = NULL";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // null = NULL evaluates to false in SQL
        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                match filter.expression {
                    BoundExpression::Literal {
                        value: LiteralValue::Boolean(false),
                        ..
                    } => {
                        // correct - NULL = NULL is false
                    }
                    _ => panic!("NULL = NULL should evaluate to false"),
                }
            } else {
                panic!("Expected Filter operator");
            }
        } else {
            panic!("Expected Projection operator");
        }
    }

    #[test]
    fn test_double_not_elimination() {
        // where NOT NOT (age > 25) → WHERE age > 25
        let _guard = TestFileGuard::new("test_double_not.csv", "id,name,age\n1,Alice,30\n");

        let sql = "SELECT name FROM 'test_double_not.csv' WHERE NOT NOT (age > 25)";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // not NOT x should be eliminated to just x
        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                match filter.expression {
                    BoundExpression::GreaterThan(_, _) => {
                        // correct - double NOT eliminated
                    }
                    _ => panic!("NOT NOT (age > 25) should simplify to age > 25"),
                }
            }
        }
    }

    #[test]
    fn test_triple_not_elimination() {
        // where NOT NOT NOT (age > 25) → WHERE NOT (age > 25)
        let _guard = TestFileGuard::new("test_triple_not.csv", "id,name,age\n1,Alice,30\n");

        let sql = "SELECT name FROM 'test_triple_not.csv' WHERE NOT NOT NOT (age > 25)";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // not NOT NOT x → NOT x
        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                match filter.expression {
                    BoundExpression::Not(inner) => {
                        // should have single NOT with age > 25 inside
                        match *inner {
                            BoundExpression::GreaterThan(_, _) => {
                                // correct - triple NOT reduced to single NOT
                            }
                            _ => panic!("Expected GreaterThan inside single NOT"),
                        }
                    }
                    _ => panic!("NOT NOT NOT x should simplify to NOT x"),
                }
            }
        }
    }

    #[test]
    fn test_quadruple_not_elimination() {
        // where NOT NOT NOT NOT (age > 25) → WHERE age > 25
        let _guard = TestFileGuard::new("test_quad_not.csv", "id,name,age\n1,Alice,30\n");

        let sql = "SELECT name FROM 'test_quad_not.csv' WHERE NOT NOT NOT NOT (age > 25)";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // not NOT NOT NOT x → x (even number of NOTs cancel out)
        if let LogicalOperator::Projection(proj) = optimized_plan {
            if let LogicalOperator::Filter(filter) = *proj.child {
                match filter.expression {
                    BoundExpression::GreaterThan(_, _) => {
                        // correct - all NOTs eliminated
                    }
                    _ => panic!("NOT NOT NOT NOT x should simplify to x"),
                }
            }
        }
    }

    #[test]
    fn test_double_not_with_constant() {
        // where NOT NOT true → WHERE true → filter removed
        let _guard = TestFileGuard::new("test_double_not_const.csv", "id,name\n1,Alice\n");

        let sql = "SELECT name FROM 'test_double_not_const.csv' WHERE NOT NOT true";

        let mut parser = Parser::new();
        let query = parser.parse(sql).unwrap();

        let binder = Binder::new();
        let bound_query = binder.bind(query).unwrap();

        let planner = Planner::new();
        let plan = planner.plan(bound_query);

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        // not NOT true → true, filter removed
        if let LogicalOperator::Projection(proj) = optimized_plan {
            match *proj.child {
                LogicalOperator::Get(_) => {
                    // correct - simplified to true, filter removed
                }
                _ => panic!("NOT NOT true should become true and remove filter"),
            }
        }
    }
}
