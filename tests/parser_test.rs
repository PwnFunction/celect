use celect::Parser;
use celect::parser::{Expression, LiteralValue, SelectColumn};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        assert_eq!(query.select.columns, vec![SelectColumn::All]);
        assert_eq!(query.from.file, "users");
        assert_eq!(query.where_clause, None);
    }

    #[test]
    fn test_select_columns() {
        let mut parser = Parser::new();
        let sql = "SELECT id, name, email FROM users";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        assert_eq!(
            query.select.columns,
            vec![
                SelectColumn::Column("id".to_string()),
                SelectColumn::Column("name".to_string()),
                SelectColumn::Column("email".to_string()),
            ]
        );
        assert_eq!(query.from.file, "users");
        assert_eq!(query.where_clause, None);
    }

    #[test]
    fn test_select_with_where() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE id = 1";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        assert_eq!(query.select.columns, vec![SelectColumn::All]);
        assert_eq!(query.from.file, "users");
        assert!(query.where_clause.is_some());
        let where_clause = query.where_clause.unwrap();
        assert!(matches!(where_clause.condition, Expression::Equal(_, _)));
    }

    #[test]
    fn test_select_columns_with_where() {
        let mut parser = Parser::new();
        let sql = "SELECT name, email FROM users WHERE age > 18";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        assert_eq!(
            query.select.columns,
            vec![
                SelectColumn::Column("name".to_string()),
                SelectColumn::Column("email".to_string()),
            ]
        );
        assert_eq!(query.from.file, "users");
        assert!(query.where_clause.is_some());
        let where_clause = query.where_clause.unwrap();
        assert!(matches!(
            where_clause.condition,
            Expression::GreaterThan(_, _)
        ));
    }

    #[test]
    fn test_where_with_comparison_operators() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE age >= 18";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        let where_clause = query.where_clause.unwrap();
        assert!(matches!(
            where_clause.condition,
            Expression::GreaterThanOrEqual(_, _)
        ));
    }

    #[test]
    fn test_where_with_string_literal() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE name = 'John'";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::Equal(left, right) = where_clause.condition {
            assert!(matches!(*left, Expression::Column(_)));
            assert!(matches!(
                *right,
                Expression::Literal(LiteralValue::String(_))
            ));
            if let Expression::Literal(LiteralValue::String(s)) = *right {
                assert_eq!(s, "John");
            }
        } else {
            panic!("Expected Equal expression");
        }
    }

    #[test]
    fn test_where_with_null_literal() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE email = NULL";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::Equal(left, right) = where_clause.condition {
            assert!(matches!(*left, Expression::Column(_)));
            assert!(matches!(*right, Expression::Literal(LiteralValue::Null)));
        } else {
            panic!("Expected Equal expression");
        }
    }

    #[test]
    fn test_where_with_not_equal() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE id != 1";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        let where_clause = query.where_clause.unwrap();
        assert!(matches!(where_clause.condition, Expression::NotEqual(_, _)));
    }

    #[test]
    fn test_where_with_not_equal_alt() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE id <> 1";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        let where_clause = query.where_clause.unwrap();
        assert!(matches!(where_clause.condition, Expression::NotEqual(_, _)));
    }

    #[test]
    fn test_where_with_less_than() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE age < 18";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        let where_clause = query.where_clause.unwrap();
        assert!(matches!(where_clause.condition, Expression::LessThan(_, _)));
    }

    #[test]
    fn test_where_with_float_literal() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM products WHERE price = 99.99";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::Equal(left, right) = where_clause.condition {
            assert!(matches!(*left, Expression::Column(_)));
            assert!(matches!(
                *right,
                Expression::Literal(LiteralValue::Float(_))
            ));
            if let Expression::Literal(LiteralValue::Float(f)) = *right {
                assert_eq!(f, 99.99);
            }
        } else {
            panic!("Expected Equal expression");
        }
    }

    #[test]
    fn test_where_with_negative_number() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE balance = -100";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::Equal(left, right) = where_clause.condition {
            assert!(matches!(*left, Expression::Column(_)));
            assert!(matches!(
                *right,
                Expression::Literal(LiteralValue::Integer(_))
            ));
            if let Expression::Literal(LiteralValue::Integer(i)) = *right {
                assert_eq!(i, -100);
            }
        } else {
            panic!("Expected Equal expression");
        }
    }

    #[test]
    fn test_where_with_negative_float() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM products WHERE price = -99.99";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::Equal(left, right) = where_clause.condition {
            assert!(matches!(*left, Expression::Column(_)));
            assert!(matches!(
                *right,
                Expression::Literal(LiteralValue::Float(_))
            ));
            if let Expression::Literal(LiteralValue::Float(f)) = *right {
                assert_eq!(f, -99.99);
            }
        } else {
            panic!("Expected Equal expression");
        }
    }

    #[test]
    fn test_multiple_comparison_operators() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE age > 18";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        let where_clause = query.where_clause.unwrap();
        assert!(matches!(
            where_clause.condition,
            Expression::GreaterThan(_, _)
        ));
    }

    // bad cases - should fail parsing

    #[test]
    fn test_missing_from() {
        let mut parser = Parser::new();
        let sql = "SELECT * WHERE id = 1";
        let result = parser.parse(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_select() {
        let mut parser = Parser::new();
        let sql = "FROM users WHERE id = 1";
        let result = parser.parse(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_table_name() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM WHERE id = 1";
        let result = parser.parse(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_incomplete_where() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE";
        let result = parser.parse(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_where_without_condition() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE = 1";
        let result = parser.parse(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_unclosed_string() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE name = 'John";
        let result = parser.parse(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_column_list() {
        let mut parser = Parser::new();
        let sql = "SELECT , FROM users";
        let result = parser.parse(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_comma_in_column_list() {
        let mut parser = Parser::new();
        let sql = "SELECT id name FROM users";
        let result = parser.parse(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_query() {
        let mut parser = Parser::new();
        let sql = "";
        let result = parser.parse(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_just_select() {
        let mut parser = Parser::new();
        let sql = "SELECT";
        let result = parser.parse(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_where_before_from() {
        let mut parser = Parser::new();
        let sql = "SELECT * WHERE id = 1 FROM users";
        let result = parser.parse(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_parenthesized_where() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE (age > 18)";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        // parentheses should be transparent - expression should be unwrapped
        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::GreaterThan(left, right) => match (*left, *right) {
                (Expression::Column(name), Expression::Literal(LiteralValue::Integer(val))) => {
                    assert_eq!(name, "age");
                    assert_eq!(val, 18);
                }
                _ => panic!("Expected Column > Integer"),
            },
            _ => panic!("Expected GreaterThan expression"),
        }
    }

    #[test]
    fn test_nested_parentheses() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE ((age > 18))";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        // nested parentheses should also be unwrapped
        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::GreaterThan(left, right) => match (*left, *right) {
                (Expression::Column(name), Expression::Literal(LiteralValue::Integer(val))) => {
                    assert_eq!(name, "age");
                    assert_eq!(val, 18);
                }
                _ => panic!("Expected Column > Integer"),
            },
            _ => panic!("Expected GreaterThan expression"),
        }
    }

    #[test]
    fn test_select_with_parentheses() {
        let mut parser = Parser::new();
        let sql = "SELECT (name), (age) FROM users";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        // parentheses should be transparent - unwrapped to column names
        assert_eq!(
            query.select.columns,
            vec![
                SelectColumn::Column("name".to_string()),
                SelectColumn::Column("age".to_string()),
            ]
        );
    }

    #[test]
    fn test_select_single_column_with_parentheses() {
        let mut parser = Parser::new();
        let sql = "SELECT (id) FROM users";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        assert_eq!(
            query.select.columns,
            vec![SelectColumn::Column("id".to_string()),]
        );
    }

    #[test]
    fn test_select_multiple_columns_in_parentheses_fails() {
        // select (a, b) should fail - we don't support ROW constructors
        let mut parser = Parser::new();
        let sql = "SELECT (name, age) FROM users";
        let result = parser.parse(sql);
        assert!(
            result.is_err(),
            "SELECT (a, b) should fail - ROW constructors not supported"
        );
    }

    #[test]
    fn test_select_star_in_parentheses_fails() {
        // select (*) should fail - doesn't make semantic sense
        let mut parser = Parser::new();
        let sql = "SELECT (*) FROM users";
        let result = parser.parse(sql);
        assert!(result.is_err(), "SELECT (*) should fail - invalid syntax");
    }

    #[test]
    fn test_select_mixed_parenthesized_and_plain() {
        let mut parser = Parser::new();
        let sql = "SELECT id, (name), age FROM users";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        // all should be unwrapped to plain column names
        assert_eq!(
            query.select.columns,
            vec![
                SelectColumn::Column("id".to_string()),
                SelectColumn::Column("name".to_string()),
                SelectColumn::Column("age".to_string()),
            ]
        );
    }

    #[test]
    fn test_not_expression() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE NOT (age > 18)";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::Not(inner) => match *inner {
                Expression::GreaterThan(left, right) => match (*left, *right) {
                    (Expression::Column(name), Expression::Literal(LiteralValue::Integer(val))) => {
                        assert_eq!(name, "age");
                        assert_eq!(val, 18);
                    }
                    _ => panic!("Expected Column > Integer inside NOT"),
                },
                _ => panic!("Expected GreaterThan inside NOT"),
            },
            _ => panic!("Expected Not expression"),
        }
    }

    #[test]
    fn test_not_without_parentheses() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE NOT age > 18";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        // not should bind to the comparison
        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::Not(_) => {
                // should be NOT (age > 18)
            }
            _ => panic!("Expected Not expression"),
        }
    }

    #[test]
    fn test_nested_not() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE NOT (NOT (age > 18))";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::Not(outer) => {
                match *outer {
                    Expression::Not(inner) => {
                        match *inner {
                            Expression::GreaterThan(_, _) => {
                                // double NOT
                            }
                            _ => panic!("Expected GreaterThan inside inner NOT"),
                        }
                    }
                    _ => panic!("Expected inner NOT"),
                }
            }
            _ => panic!("Expected outer NOT"),
        }
    }

    #[test]
    fn test_and_expression() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE age > 18 AND name = 'Alice'";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::And(left, right) => {
                match (*left, *right) {
                    (Expression::GreaterThan(_, _), Expression::Equal(_, _)) => {
                        // correct structure
                    }
                    _ => panic!("Expected GreaterThan AND Equal"),
                }
            }
            _ => panic!("Expected And expression"),
        }
    }

    #[test]
    fn test_or_expression() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE age < 18 OR age > 65";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::Or(left, right) => {
                match (*left, *right) {
                    (Expression::LessThan(_, _), Expression::GreaterThan(_, _)) => {
                        // correct structure
                    }
                    _ => panic!("Expected LessThan OR GreaterThan"),
                }
            }
            _ => panic!("Expected Or expression"),
        }
    }

    #[test]
    fn test_and_or_precedence() {
        let mut parser = Parser::new();
        // and has higher precedence than OR
        // a OR b AND c should parse as a OR (b AND c)
        let sql = "SELECT * FROM users WHERE a = 1 OR b = 2 AND c = 3";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::Or(left, right) => {
                // left should be a = 1
                match *left {
                    Expression::Equal(_, _) => {}
                    _ => panic!("Expected Equal on left of OR"),
                }
                // right should be (b = 2 AND c = 3)
                match *right {
                    Expression::And(_, _) => {}
                    _ => panic!("Expected And on right of OR"),
                }
            }
            _ => panic!("Expected Or expression at root"),
        }
    }

    #[test]
    fn test_complex_and_or_not() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE (age > 25 AND alive = true) OR NOT (id = 1)";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::Or(left, right) => {
                // left: (age > 25 AND alive = true)
                match *left {
                    Expression::And(_, _) => {}
                    _ => panic!("Expected And on left of OR"),
                }
                // right: NOT (id = 1)
                match *right {
                    Expression::Not(inner) => match *inner {
                        Expression::Equal(_, _) => {}
                        _ => panic!("Expected Equal inside NOT"),
                    },
                    _ => panic!("Expected Not on right of OR"),
                }
            }
            _ => panic!("Expected Or expression"),
        }
    }

    #[test]
    fn test_multiple_and() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE a = 1 AND b = 2 AND c = 3";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        // should be right-associative: a = 1 AND (b = 2 AND c = 3)
        match where_clause.condition {
            Expression::And(_, _) => {}
            _ => panic!("Expected And expression"),
        }
    }

    #[test]
    fn test_multiple_or() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE a = 1 OR b = 2 OR c = 3";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        // should be right-associative: a = 1 OR (b = 2 OR c = 3)
        match where_clause.condition {
            Expression::Or(_, _) => {}
            _ => panic!("Expected Or expression"),
        }
    }

    #[test]
    fn test_not_and_precedence() {
        let mut parser = Parser::new();
        // not has higher precedence than AND
        // not a AND b should parse as (NOT a) AND b
        let sql = "SELECT * FROM users WHERE NOT a = 1 AND b = 2";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::And(left, right) => {
                // left should be NOT (a = 1)
                match *left {
                    Expression::Not(_) => {}
                    _ => panic!("Expected Not on left of AND"),
                }
                // right should be b = 2
                match *right {
                    Expression::Equal(_, _) => {}
                    _ => panic!("Expected Equal on right of AND"),
                }
            }
            _ => panic!("Expected And expression"),
        }
    }

    #[test]
    fn test_parentheses_override_precedence() {
        let mut parser = Parser::new();
        // (a OR b) AND c should parse as (a OR b) AND c
        let sql = "SELECT * FROM users WHERE (a = 1 OR b = 2) AND c = 3";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::And(left, right) => {
                // left should be (a = 1 OR b = 2)
                match *left {
                    Expression::Or(_, _) => {}
                    _ => panic!(
                        "Expected Or on left of AND (parentheses should override precedence)"
                    ),
                }
                // right should be c = 3
                match *right {
                    Expression::Equal(_, _) => {}
                    _ => panic!("Expected Equal on right of AND"),
                }
            }
            _ => panic!("Expected And expression"),
        }
    }

    #[test]
    fn test_deeply_nested_parentheses() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE ((a = 1 OR b = 2) AND c = 3)";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::And(left, right) => {
                match *left {
                    Expression::Or(_, _) => {}
                    _ => panic!("Expected Or inside nested parentheses"),
                }
                match *right {
                    Expression::Equal(_, _) => {}
                    _ => panic!("Expected Equal on right"),
                }
            }
            _ => panic!("Expected And expression"),
        }
    }

    #[test]
    fn test_triple_nested_parentheses() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE (((a = 1)))";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::Equal(_, _) => {}
            _ => panic!("Expected Equal (parentheses should unwrap)"),
        }
    }

    #[test]
    fn test_complex_bodmas_precedence() {
        let mut parser = Parser::new();
        // not > AND > OR (like !, &&, || in programming)
        // a OR b AND NOT c should parse as: a OR (b AND (NOT c))
        let sql = "SELECT * FROM users WHERE a = 1 OR b = 2 AND NOT c = 3";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::Or(left, right) => {
                // left: a = 1
                match *left {
                    Expression::Equal(_, _) => {}
                    _ => panic!("Expected Equal on left of OR"),
                }
                // right: (b = 2 AND NOT c = 3)
                match *right {
                    Expression::And(left_and, right_and) => {
                        // left of AND: b = 2
                        match *left_and {
                            Expression::Equal(_, _) => {}
                            _ => panic!("Expected Equal on left of AND"),
                        }
                        // right of AND: NOT c = 3
                        match *right_and {
                            Expression::Not(_) => {}
                            _ => panic!("Expected Not on right of AND"),
                        }
                    }
                    _ => panic!("Expected And on right of OR"),
                }
            }
            _ => panic!("Expected Or at root"),
        }
    }

    #[test]
    fn test_mixed_nested_with_parentheses() {
        let mut parser = Parser::new();
        // (a OR b) AND (c OR d)
        let sql = "SELECT * FROM users WHERE (a = 1 OR b = 2) AND (c = 3 OR d = 4)";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::And(left, right) => {
                // both sides should be OR
                match (*left, *right) {
                    (Expression::Or(_, _), Expression::Or(_, _)) => {}
                    _ => panic!("Expected Or on both sides of AND"),
                }
            }
            _ => panic!("Expected And expression"),
        }
    }

    #[test]
    fn test_deeply_nested_logical_ops() {
        let mut parser = Parser::new();
        // ((a OR b) AND (c OR d)) OR e
        let sql = "SELECT * FROM users WHERE ((a = 1 OR b = 2) AND (c = 3 OR d = 4)) OR e = 5";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::Or(left, right) => {
                // left: ((a OR b) AND (c OR d))
                match *left {
                    Expression::And(_, _) => {}
                    _ => panic!("Expected And on left of outer OR"),
                }
                // right: e = 5
                match *right {
                    Expression::Equal(_, _) => {}
                    _ => panic!("Expected Equal on right of outer OR"),
                }
            }
            _ => panic!("Expected Or at root"),
        }
    }

    #[test]
    fn test_not_with_nested_and_or() {
        let mut parser = Parser::new();
        // not (a OR b AND c) should parse as: NOT ((a OR (b AND c)))
        let sql = "SELECT * FROM users WHERE NOT (a = 1 OR b = 2 AND c = 3)";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::Not(inner) => {
                // inner should be OR
                match *inner {
                    Expression::Or(left, right) => {
                        // left: a = 1
                        match *left {
                            Expression::Equal(_, _) => {}
                            _ => panic!("Expected Equal on left of OR"),
                        }
                        // right: (b = 2 AND c = 3)
                        match *right {
                            Expression::And(_, _) => {}
                            _ => panic!("Expected And on right of OR"),
                        }
                    }
                    _ => panic!("Expected Or inside NOT"),
                }
            }
            _ => panic!("Expected Not at root"),
        }
    }

    #[test]
    fn test_chained_precedence() {
        let mut parser = Parser::new();
        // a AND b OR c AND d should parse as: (a AND b) OR (c AND d)
        let sql = "SELECT * FROM users WHERE a = 1 AND b = 2 OR c = 3 AND d = 4";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        match where_clause.condition {
            Expression::Or(left, right) => {
                // both sides should be AND
                match (*left, *right) {
                    (Expression::And(_, _), Expression::And(_, _)) => {}
                    _ => panic!("Expected And on both sides of OR"),
                }
            }
            _ => panic!("Expected Or at root"),
        }
    }

    #[test]
    fn test_parentheses_change_evaluation_order() {
        let mut parser = Parser::new();
        // a AND (b OR c) AND d
        let sql = "SELECT * FROM users WHERE a = 1 AND (b = 2 OR c = 3) AND d = 4";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        let where_clause = query.where_clause.unwrap();
        // root should be AND
        match where_clause.condition {
            Expression::And(_, right) => {
                // should have nested structure with OR in the middle
                match *right {
                    Expression::And(left_inner, _) => {
                        // left_inner should be the (b OR c) part
                        match *left_inner {
                            Expression::Or(_, _) => {}
                            _ => panic!("Expected Or in middle position"),
                        }
                    }
                    _ => panic!("Expected nested And"),
                }
            }
            _ => panic!("Expected And at root"),
        }
    }

    #[test]
    fn test_insanely_complex_nested_query() {
        let mut parser = Parser::new();
        // the craziest query: triple nested, multiple AND/OR/NOT, all comparison operators
        let sql = "SELECT name, age FROM users WHERE \
            ((age > 25 AND alive = true) OR (age < 18 AND NOT (id = 1))) \
            AND (name != 'Bob' OR (id >= 2 AND id <= 4)) \
            OR NOT ((age = 30 OR age = 35) AND alive = false)";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();

        // just verify it parses correctly and has WHERE clause
        assert!(query.where_clause.is_some());
        let where_clause = query.where_clause.unwrap();

        // root should be OR (outermost OR in the expression)
        match where_clause.condition {
            Expression::Or(_, _) => {
                // successfully parsed the insanely complex query!
            }
            _ => panic!("Expected Or at root of complex expression"),
        }
    }

    #[test]
    fn test_case_insensitive_keywords() {
        let mut parser = Parser::new();
        let sql = "select * from users where age > 25";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        assert_eq!(query.select.columns, vec![SelectColumn::All]);
        assert_eq!(query.from.file, "users");
        assert!(query.where_clause.is_some());
    }

    #[test]
    fn test_mixed_case_keywords() {
        let mut parser = Parser::new();
        let sql = "SeLeCt name, age FrOm 'data.csv' WhErE active = TrUe";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        assert_eq!(
            query.select.columns,
            vec![
                SelectColumn::Column("name".to_string()),
                SelectColumn::Column("age".to_string()),
            ]
        );
        assert_eq!(query.from.file, "data.csv");
        assert!(query.where_clause.is_some());
    }

    #[test]
    fn test_lowercase_logical_operators() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE active = true and age > 18 or admin = true";
        let result = parser.parse(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_lowercase_not_operator() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE not active = false";
        let result = parser.parse(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_semicolon_optional() {
        let mut parser = Parser::new();
        let sql_without = "SELECT * FROM users";
        let sql_with = "SELECT * FROM users;";
        
        let result_without = parser.parse(sql_without);
        assert!(result_without.is_ok());
        
        let mut parser2 = Parser::new();
        let result_with = parser2.parse(sql_with);
        assert!(result_with.is_ok());
        
        // both should produce same query structure
        let query_without = result_without.unwrap();
        let query_with = result_with.unwrap();
        assert_eq!(query_without.from.file, query_with.from.file);
    }

    #[test]
    fn test_semicolon_with_where() {
        let mut parser = Parser::new();
        let sql = "SELECT name FROM 'data.csv' WHERE age > 25;";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        assert_eq!(
            query.select.columns,
            vec![SelectColumn::Column("name".to_string())]
        );
        assert_eq!(query.from.file, "data.csv");
        assert!(query.where_clause.is_some());
    }

    #[test]
    fn test_semicolon_with_limit() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users LIMIT 10;";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        assert_eq!(query.limit, Some(10));
    }

    #[test]
    fn test_double_quotes_in_strings() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM \"data.csv\" WHERE name = \"Alice\"";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        assert_eq!(query.from.file, "data.csv");
    }

    #[test]
    fn test_mixed_quotes() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM 'data.csv' WHERE name = \"Bob\"";
        let result = parser.parse(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_lowercase_count() {
        let mut parser = Parser::new();
        let sql = "select count(*) from users";
        let result = parser.parse(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_lowercase_limit_offset() {
        let mut parser = Parser::new();
        let sql = "select * from users limit 10 offset 5";
        let result = parser.parse(sql);
        assert!(result.is_ok());
        let query = result.unwrap();
        assert_eq!(query.limit, Some(10));
        assert_eq!(query.offset, Some(5));
    }

    #[test]
    fn test_lowercase_null() {
        let mut parser = Parser::new();
        let sql = "SELECT * FROM users WHERE name = null";
        let result = parser.parse(sql);
        assert!(result.is_ok());
    }
}
