use tree_sitter::{Language, Node, Parser as TreeSitterParser, Tree};

unsafe extern "C" {
    fn tree_sitter_sql() -> Language;
}

#[derive(Debug, Clone)]
pub struct ParseError {
    pub message: String,
    pub offset: usize,
}

pub type ParseResult<T> = Result<T, ParseError>;

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub select: SelectClause,
    pub from: FromClause,
    pub where_clause: Option<WhereClause>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectClause {
    pub columns: Vec<SelectColumn>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectColumn {
    All,                          // select *
    Column(String),               // select column_name
    Aggregate(AggregateFunction), // select COUNT(*)
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    CountStar,
    Count(String), // column name
}

#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    pub file: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub condition: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    // logical operators (in precedence order: OR < AND < NOT)
    Or(Box<Expression>, Box<Expression>),
    And(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),

    // comparison operators
    Equal(Box<Expression>, Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    GreaterThanOrEqual(Box<Expression>, Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
    LessThanOrEqual(Box<Expression>, Box<Expression>),

    // leaf nodes
    Column(String),
    Literal(LiteralValue),
}

#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

pub struct Parser {
    parser: TreeSitterParser,
}

impl Parser {
    pub fn new() -> Self {
        let mut parser = TreeSitterParser::new();
        let language = unsafe { tree_sitter_sql() };
        parser
            .set_language(&language)
            .expect("Failed to load SQL grammar");

        Self { parser }
    }

    pub fn parse(&mut self, sql: &str) -> ParseResult<Query> {
        let tree = self.parser.parse(sql, None).ok_or_else(|| ParseError {
            message: "Failed to parse query".to_string(),
            offset: 0,
        })?;

        // check for parse errors
        if self.has_parse_errors(&tree, sql) {
            return Err(ParseError {
                message: "Parse error: invalid SQL syntax".to_string(),
                offset: 0,
            });
        }

        let root_node = tree.root_node();
        self.transform_tree(&root_node, sql)
    }

    fn has_parse_errors(&self, tree: &Tree, source: &str) -> bool {
        let root = tree.root_node();

        // check if root node has errors
        if root.has_error() {
            return true;
        }

        // check if all input was consumed
        if root.end_byte() < source.len() {
            return true;
        }

        // recursively check for error nodes
        self.check_node_for_errors(&root)
    }

    fn check_node_for_errors(&self, node: &Node) -> bool {
        // check if this node is an error node
        if node.is_error() || node.kind() == "ERROR" {
            return true;
        }

        // recursively check children
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                if self.check_node_for_errors(&child) {
                    return true;
                }
            }
        }

        false
    }

    fn transform_tree(&self, node: &Node, source: &str) -> ParseResult<Query> {
        match node.kind() {
            "source_file" => {
                let child = node.child(0).ok_or_else(|| ParseError {
                    message: "Expected select_statement".to_string(),
                    offset: node.start_byte(),
                })?;
                self.transform_tree(&child, source)
            }
            "select_statement" => {
                let mut select_list_node = None;
                let mut file_name_node = None;
                let mut where_clause_node = None;
                let mut limit_clause_node = None;
                let mut offset_clause_node = None;

                for i in 0..node.child_count() {
                    if let Some(child) = node.child(i) {
                        match child.kind() {
                            "select_list" => select_list_node = Some(child),
                            "file_name" => file_name_node = Some(child),
                            "where_clause" => where_clause_node = Some(child),
                            "limit_clause" => limit_clause_node = Some(child),
                            "offset_clause" => offset_clause_node = Some(child),
                            _ => {} // skip keywords like SELECT, FROM, WHERE, LIMIT, OFFSET
                        }
                    }
                }

                let select = select_list_node
                    .ok_or_else(|| ParseError {
                        message: "Missing select_list".to_string(),
                        offset: node.start_byte(),
                    })
                    .and_then(|n| self.transform_select_list(&n, source))?;

                let from = file_name_node
                    .ok_or_else(|| ParseError {
                        message: "Missing file_name".to_string(),
                        offset: node.start_byte(),
                    })
                    .and_then(|n| self.transform_file_name(&n, source))?;

                let where_clause = if let Some(n) = where_clause_node {
                    Some(self.transform_where_clause(&n, source)?)
                } else {
                    None
                };

                let limit = if let Some(n) = limit_clause_node {
                    Some(self.extract_number_from_clause(&n, source)?)
                } else {
                    None
                };

                let offset = if let Some(n) = offset_clause_node {
                    Some(self.extract_number_from_clause(&n, source)?)
                } else {
                    None
                };

                Ok(Query {
                    select,
                    from,
                    where_clause,
                    limit,
                    offset,
                })
            }
            _ => Err(ParseError {
                message: format!("Unexpected node type: {}", node.kind()),
                offset: node.start_byte(),
            }),
        }
    }

    fn transform_select_list(&self, node: &Node, source: &str) -> ParseResult<SelectClause> {
        // check if it's SELECT *
        if node.child_count() == 0 {
            // check if the text is "*"
            let text = &source[node.start_byte()..node.end_byte()];
            if text.trim() == "*" {
                return Ok(SelectClause {
                    columns: vec![SelectColumn::All],
                });
            }
        }

        // check for column_list child
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                if child.kind() == "column_list" {
                    return self.transform_column_list(&child, source);
                }
            }
        }

        // if no column_list, might be just *
        let text = &source[node.start_byte()..node.end_byte()];
        if text.trim() == "*" {
            return Ok(SelectClause {
                columns: vec![SelectColumn::All],
            });
        }

        Err(ParseError {
            message: "Invalid select_list".to_string(),
            offset: node.start_byte(),
        })
    }

    fn transform_column_list(&self, node: &Node, source: &str) -> ParseResult<SelectClause> {
        let mut columns = Vec::new();

        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                match child.kind() {
                    "select_expression" => {
                        // handle select_expression which can be column_name, (column_name), or aggregate_function
                        let select_col = self.transform_select_expression(&child, source)?;
                        columns.push(select_col);
                    }
                    "column_name" => {
                        // fallback for direct column_name (if still present)
                        let name = self.get_node_text(&child, source)?;
                        columns.push(SelectColumn::Column(name));
                    }
                    _ => {
                        // skip commas and other tokens
                    }
                }
            }
        }

        Ok(SelectClause { columns })
    }

    fn transform_select_expression(&self, node: &Node, source: &str) -> ParseResult<SelectColumn> {
        // select_expression can be:
        // 1. column_name
        // 2. ( column_name )
        // 3. aggregate_function

        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                match child.kind() {
                    "column_name" => {
                        let name = self.get_node_text(&child, source)?;
                        return Ok(SelectColumn::Column(name));
                    }
                    "aggregate_function" => {
                        return self.transform_aggregate_function(&child, source);
                    }
                    _ => {}
                }
            }
        }

        // if no child found, try to get text directly as column name
        let name = self.get_node_text(node, source)?;
        Ok(SelectColumn::Column(name))
    }

    fn transform_aggregate_function(&self, node: &Node, source: &str) -> ParseResult<SelectColumn> {
        // aggregate_function can be:
        // 1. COUNT ( * )
        // 2. COUNT ( column_name )

        let mut is_count_star = false;
        let mut column_name: Option<String> = None;

        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                match child.kind() {
                    "*" => {
                        is_count_star = true;
                    }
                    "column_name" => {
                        column_name = Some(self.get_node_text(&child, source)?);
                    }
                    _ => {}
                }
            }
        }

        if is_count_star {
            Ok(SelectColumn::Aggregate(AggregateFunction::CountStar))
        } else if let Some(col) = column_name {
            Ok(SelectColumn::Aggregate(AggregateFunction::Count(col)))
        } else {
            Err(ParseError {
                message: "Invalid aggregate function".to_string(),
                offset: node.start_byte(),
            })
        }
    }

    fn transform_file_name(&self, node: &Node, source: &str) -> ParseResult<FromClause> {
        let name = self.get_node_text(node, source)?;
        // if it's a string literal, strip the quotes
        let file_name = if name.starts_with("'") && name.ends_with("'") {
            name[1..name.len() - 1].to_string()
        } else {
            name
        };
        Ok(FromClause { file: file_name })
    }

    fn transform_where_clause(&self, node: &Node, source: &str) -> ParseResult<WhereClause> {
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                if child.kind() == "expression" {
                    let condition = self.transform_expression(&child, source)?;
                    return Ok(WhereClause { condition });
                }
            }
        }

        Err(ParseError {
            message: "Missing expression in where_clause".to_string(),
            offset: node.start_byte(),
        })
    }

    fn transform_expression(&self, node: &Node, source: &str) -> ParseResult<Expression> {
        match node.kind() {
            "or_expression" => self.transform_or(&node, source),
            "and_expression" => self.transform_and(&node, source),
            "not_expression" => self.transform_not(&node, source),
            "primary_expression" => self.transform_primary(&node, source),
            "comparison_expression" => self.transform_comparison(&node, source),
            "column_name" => {
                let name = self.get_node_text(node, source)?;
                Ok(Expression::Column(name))
            }
            "literal" => {
                // find the actual literal child
                for i in 0..node.child_count() {
                    if let Some(child) = node.child(i) {
                        match child.kind() {
                            "string_literal" => {
                                let text = self.get_node_text(&child, source)?;
                                // remove quotes
                                let text = text.trim_matches('\'');
                                return Ok(Expression::Literal(LiteralValue::String(
                                    text.to_string(),
                                )));
                            }
                            "number_literal" => {
                                let text = self.get_node_text(&child, source)?;
                                if let Ok(i) = text.parse::<i64>() {
                                    return Ok(Expression::Literal(LiteralValue::Integer(i)));
                                } else if let Ok(f) = text.parse::<f64>() {
                                    return Ok(Expression::Literal(LiteralValue::Float(f)));
                                }
                            }
                            "boolean_literal" => {
                                let text = self.get_node_text(&child, source)?;
                                let b = text.trim().to_lowercase() == "true";
                                return Ok(Expression::Literal(LiteralValue::Boolean(b)));
                            }
                            _ => {}
                        }
                    }
                }
                // check for NULL keyword
                let text = self.get_node_text(node, source)?;
                if text.trim().to_uppercase() == "NULL" {
                    return Ok(Expression::Literal(LiteralValue::Null));
                }
                Err(ParseError {
                    message: "Invalid literal".to_string(),
                    offset: node.start_byte(),
                })
            }
            _ => {
                // try to find operator in children
                let children: Vec<Node> = (0..node.child_count())
                    .filter_map(|i| node.child(i))
                    .collect();

                // check for comparison operators
                for (i, child) in children.iter().enumerate() {
                    let op_text = self.get_node_text(child, source)?;
                    match op_text.trim() {
                        "=" => {
                            if i > 0 && i < children.len() - 1 {
                                let left = self.transform_expression(&children[i - 1], source)?;
                                let right = self.transform_expression(&children[i + 1], source)?;
                                return Ok(Expression::Equal(Box::new(left), Box::new(right)));
                            }
                        }
                        "!=" | "<>" => {
                            if i > 0 && i < children.len() - 1 {
                                let left = self.transform_expression(&children[i - 1], source)?;
                                let right = self.transform_expression(&children[i + 1], source)?;
                                return Ok(Expression::NotEqual(Box::new(left), Box::new(right)));
                            }
                        }
                        ">" => {
                            if i > 0 && i < children.len() - 1 {
                                let left = self.transform_expression(&children[i - 1], source)?;
                                let right = self.transform_expression(&children[i + 1], source)?;
                                return Ok(Expression::GreaterThan(
                                    Box::new(left),
                                    Box::new(right),
                                ));
                            }
                        }
                        ">=" => {
                            if i > 0 && i < children.len() - 1 {
                                let left = self.transform_expression(&children[i - 1], source)?;
                                let right = self.transform_expression(&children[i + 1], source)?;
                                return Ok(Expression::GreaterThanOrEqual(
                                    Box::new(left),
                                    Box::new(right),
                                ));
                            }
                        }
                        "<" => {
                            if i > 0 && i < children.len() - 1 {
                                let left = self.transform_expression(&children[i - 1], source)?;
                                let right = self.transform_expression(&children[i + 1], source)?;
                                return Ok(Expression::LessThan(Box::new(left), Box::new(right)));
                            }
                        }
                        "<=" => {
                            if i > 0 && i < children.len() - 1 {
                                let left = self.transform_expression(&children[i - 1], source)?;
                                let right = self.transform_expression(&children[i + 1], source)?;
                                return Ok(Expression::LessThanOrEqual(
                                    Box::new(left),
                                    Box::new(right),
                                ));
                            }
                        }
                        _ => {}
                    }
                }

                // if no operator found, try recursing into first child
                if let Some(first_child) = children.first() {
                    self.transform_expression(first_child, source)
                } else {
                    Err(ParseError {
                        message: format!("Cannot transform expression: {}", node.kind()),
                        offset: node.start_byte(),
                    })
                }
            }
        }
    }

    fn transform_or(&self, node: &Node, source: &str) -> ParseResult<Expression> {
        // or_expression: and_expression | and_expression OR or_expression
        let mut and_expr = None;
        let mut or_expr = None;

        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                match child.kind() {
                    "and_expression" => {
                        if and_expr.is_none() {
                            and_expr = Some(self.transform_expression(&child, source)?);
                        }
                    }
                    "or_expression" => {
                        or_expr = Some(self.transform_expression(&child, source)?);
                    }
                    _ => {} // skip operators
                }
            }
        }

        match (and_expr, or_expr) {
            (Some(left), Some(right)) => Ok(Expression::Or(Box::new(left), Box::new(right))),
            (Some(expr), None) => Ok(expr), // just and_expression
            _ => Err(ParseError {
                message: "Invalid OR expression".to_string(),
                offset: node.start_byte(),
            }),
        }
    }

    fn transform_and(&self, node: &Node, source: &str) -> ParseResult<Expression> {
        // and_expression: not_expression | not_expression AND and_expression
        let mut not_expr = None;
        let mut and_expr = None;

        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                match child.kind() {
                    "not_expression" => {
                        if not_expr.is_none() {
                            not_expr = Some(self.transform_expression(&child, source)?);
                        }
                    }
                    "and_expression" => {
                        and_expr = Some(self.transform_expression(&child, source)?);
                    }
                    _ => {} // skip operators
                }
            }
        }

        match (not_expr, and_expr) {
            (Some(left), Some(right)) => Ok(Expression::And(Box::new(left), Box::new(right))),
            (Some(expr), None) => Ok(expr), // just not_expression
            _ => Err(ParseError {
                message: "Invalid AND expression".to_string(),
                offset: node.start_byte(),
            }),
        }
    }

    fn transform_not(&self, node: &Node, source: &str) -> ParseResult<Expression> {
        // not_expression: NOT not_expression | primary_expression
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                match child.kind() {
                    "not_expression" => {
                        // not not_expression
                        let inner = self.transform_expression(&child, source)?;
                        return Ok(Expression::Not(Box::new(inner)));
                    }
                    "primary_expression" => {
                        // just primary_expression
                        return self.transform_expression(&child, source);
                    }
                    _ => {} // skip NOT keyword
                }
            }
        }
        Err(ParseError {
            message: "Invalid NOT expression".to_string(),
            offset: node.start_byte(),
        })
    }

    fn transform_primary(&self, node: &Node, source: &str) -> ParseResult<Expression> {
        // primary_expression: comparison_expression | column_name | literal | '(' expression ')'
        // check for parenthesized expression first
        if node.child_count() == 3 {
            if let (Some(first), Some(middle), Some(last)) =
                (node.child(0), node.child(1), node.child(2))
            {
                let first_text = self.get_node_text(&first, source).unwrap_or_default();
                let last_text = self.get_node_text(&last, source).unwrap_or_default();
                if first_text.trim() == "("
                    && last_text.trim() == ")"
                    && middle.kind() == "expression"
                {
                    return self.transform_expression(&middle, source);
                }
            }
        }

        // otherwise, find the first child and transform it
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                match child.kind() {
                    "comparison_expression" => return self.transform_comparison(&child, source),
                    "column_name" => {
                        let name = self.get_node_text(&child, source)?;
                        return Ok(Expression::Column(name));
                    }
                    "literal" => return self.transform_literal(&child, source),
                    "expression" => return self.transform_expression(&child, source),
                    _ => {}
                }
            }
        }
        Err(ParseError {
            message: "Invalid primary expression".to_string(),
            offset: node.start_byte(),
        })
    }

    fn transform_literal(&self, node: &Node, source: &str) -> ParseResult<Expression> {
        // find the actual literal child
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                match child.kind() {
                    "string_literal" => {
                        let text = self.get_node_text(&child, source)?;
                        let text = text.trim_matches('\'');
                        return Ok(Expression::Literal(LiteralValue::String(text.to_string())));
                    }
                    "number_literal" => {
                        let text = self.get_node_text(&child, source)?;
                        if let Ok(i) = text.parse::<i64>() {
                            return Ok(Expression::Literal(LiteralValue::Integer(i)));
                        } else if let Ok(f) = text.parse::<f64>() {
                            return Ok(Expression::Literal(LiteralValue::Float(f)));
                        }
                    }
                    "boolean_literal" => {
                        let text = self.get_node_text(&child, source)?;
                        let b = text.trim().to_lowercase() == "true";
                        return Ok(Expression::Literal(LiteralValue::Boolean(b)));
                    }
                    _ => {}
                }
            }
        }
        // check for NULL
        let text = self.get_node_text(node, source)?;
        if text.trim().to_uppercase() == "NULL" {
            return Ok(Expression::Literal(LiteralValue::Null));
        }
        Err(ParseError {
            message: "Invalid literal".to_string(),
            offset: node.start_byte(),
        })
    }

    fn transform_comparison(&self, node: &Node, source: &str) -> ParseResult<Expression> {
        let children: Vec<Node> = (0..node.child_count())
            .filter_map(|i| node.child(i))
            .collect();

        // find the operator
        for (i, child) in children.iter().enumerate() {
            let text = self.get_node_text(child, source)?;
            let op = text.trim();
            if matches!(op, "=" | "!=" | "<>" | ">" | ">=" | "<" | "<=") {
                if i > 0 && i < children.len() - 1 {
                    let left = self.transform_expression(&children[i - 1], source)?;
                    let right = self.transform_expression(&children[i + 1], source)?;
                    return match op {
                        "=" => Ok(Expression::Equal(Box::new(left), Box::new(right))),
                        "!=" | "<>" => Ok(Expression::NotEqual(Box::new(left), Box::new(right))),
                        ">" => Ok(Expression::GreaterThan(Box::new(left), Box::new(right))),
                        ">=" => Ok(Expression::GreaterThanOrEqual(
                            Box::new(left),
                            Box::new(right),
                        )),
                        "<" => Ok(Expression::LessThan(Box::new(left), Box::new(right))),
                        "<=" => Ok(Expression::LessThanOrEqual(Box::new(left), Box::new(right))),
                        _ => unreachable!(),
                    };
                }
            }
        }

        Err(ParseError {
            message: "Invalid comparison_expression".to_string(),
            offset: node.start_byte(),
        })
    }

    fn get_node_text(&self, node: &Node, source: &str) -> ParseResult<String> {
        Ok(source[node.start_byte()..node.end_byte()].to_string())
    }

    fn extract_number_from_clause(&self, node: &Node, source: &str) -> ParseResult<usize> {
        // extract number from limit_clause or offset_clause
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                if child.kind() == "number_literal" {
                    let text = self.get_node_text(&child, source)?;
                    return text.parse::<usize>().map_err(|_| ParseError {
                        message: format!("Invalid number: {}", text),
                        offset: child.start_byte(),
                    });
                }
            }
        }
        Err(ParseError {
            message: "Missing number in clause".to_string(),
            offset: node.start_byte(),
        })
    }
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_creation() {
        let _parser = Parser::new();
    }
}
