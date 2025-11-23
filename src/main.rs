use celect::binder::BoundAggregateExpression;
use celect::parser::LiteralValue;
use celect::{Binder, BoundExpression, LogicalOperator, Optimizer, Parser, Planner};
use celect::{DataChunk, PhysicalPlanner, PipelineExecutor, Value};
use colored::*;
use std::time::Instant;

fn main() {
    let total_start = Instant::now();

    // example query
    let sql = "SELECT name, age, city FROM 'data.csv' WHERE age > 50 AND active = true LIMIT 5";

    println!("{}", "Query:".bright_cyan().bold());
    println!("  {}", sql.bright_white());
    println!();

    println!("{}", "=== PARSING ===".bright_cyan().bold());

    // step 1: Parse SQL to AST
    let mut parser = Parser::new();
    let parse_start = Instant::now();
    let parse_result = parser.parse(sql);
    let parse_duration = parse_start.elapsed();

    let query = match parse_result {
        Ok(query) => {
            println!(
                "{} {}",
                "Parse successful".green().bold(),
                format!("({})", format_duration(parse_duration)).bright_black()
            );
            query
        }
        Err(e) => {
            eprintln!(
                "{}",
                format!("Parse failed: {} (offset: {})", e.message, e.offset)
                    .red()
                    .bold()
            );
            return;
        }
    };

    println!();
    println!("{}", "=== BINDING ===".bright_cyan().bold());

    // step 2: binder - resolve file, read headers, infer types, validate columns
    let binder = Binder::new();
    let bind_start = Instant::now();
    let bind_result = binder.bind(query);
    let bind_duration = bind_start.elapsed();

    let bound_query = match bind_result {
        Ok(bound) => {
            println!(
                "{} {}",
                "Binding successful".green().bold(),
                format!("({})", format_duration(bind_duration)).bright_black()
            );
            println!("Resolved path: {}", bound.file_path.display());

            println!("Schema columns ({}):", bound.schema.columns.len());
            for col in &bound.schema.columns {
                println!("  [{}] {} ({:?})", col.index, col.name, col.type_);
            }

            println!("Selected columns ({}):", bound.select_columns.len());
            for col in &bound.select_columns {
                println!("  [{}] {} ({:?})", col.index, col.name, col.type_);
            }

            if let Some(where_clause) = &bound.where_clause {
                println!("WHERE clause bound: {}", format_expression(where_clause));
            }

            bound
        }
        Err(e) => {
            eprintln!("{}", format!("Binding failed: {}", e.message).red().bold());
            return;
        }
    };

    println!();
    println!("{}", "=== PLANNING ===".bright_cyan().bold());

    // step 3: Plan - generate logical plan
    let planner = Planner::new();
    let plan_start = Instant::now();
    let plan = planner.plan(bound_query);
    let plan_duration = plan_start.elapsed();

    println!(
        "{} {}",
        "Plan generation successful".green().bold(),
        format!("({})", format_duration(plan_duration)).bright_black()
    );
    println!("Before optimization:");
    print_plan(&plan, 0);

    println!();
    println!("{}", "=== OPTIMIZATION ===".bright_cyan().bold());

    // step 4: Optimize - apply projection pushdown
    let optimizer = Optimizer::new();
    let opt_start = Instant::now();
    let optimized_plan = optimizer.optimize(plan);
    let opt_duration = opt_start.elapsed();

    println!(
        "{} {}",
        "Optimization successful".green().bold(),
        format!("({})", format_duration(opt_duration)).bright_black()
    );
    println!("After optimization:");
    print_plan(&optimized_plan, 0);

    println!();
    println!("{}", "=== EXECUTION ===".bright_cyan().bold());

    // step 5: Physical Planning - convert logical plan to physical operators
    let physical_planner = PhysicalPlanner::new();
    let phys_plan_start = Instant::now();
    let (operators, schemas) = physical_planner.plan(optimized_plan);
    let phys_plan_duration = phys_plan_start.elapsed();

    println!(
        "{} {}",
        "Physical plan generated".green().bold(),
        format!("({})", format_duration(phys_plan_duration)).bright_black()
    );

    // step 6: Execute the pipeline
    let mut executor = PipelineExecutor::new(operators, schemas);
    let exec_start = Instant::now();
    let results = executor.execute();
    let exec_duration = exec_start.elapsed();

    println!(
        "{} {}",
        "Execution successful".green().bold(),
        format!("({})", format_duration(exec_duration)).bright_black()
    );
    println!();

    // print results
    print_results(&results);

    println!();
    let total_duration = total_start.elapsed();
    println!(
        "{} {}",
        "Total execution time:".bright_magenta().bold(),
        format!("{}", format_duration(total_duration)).bright_magenta()
    );
}

fn print_results(chunks: &[DataChunk]) {
    if chunks.is_empty() {
        println!("{}", "No results".yellow());
        return;
    }

    let total_rows: usize = chunks.iter().map(|chunk| chunk.selected_count()).sum();

    println!("{}", "=== RESULTS ===".bright_cyan().bold());
    println!("Total rows: {}", total_rows.to_string().bright_yellow());
    println!();

    // determine column count from first chunk
    let col_count = chunks[0].column_count();

    // print table header
    for col_idx in 0..col_count {
        print!(
            "{:<20} ",
            format!("Column {}", col_idx).bright_cyan().bold()
        );
    }
    println!();
    println!("{}", "─".repeat(col_count * 21).bright_black());

    // print rows
    for chunk in chunks {
        for row_idx in 0..chunk.selected_count() {
            for col_idx in 0..chunk.column_count() {
                if let Some(value) = chunk.get_value(col_idx, row_idx) {
                    print!("{:<20} ", format_value(&value).bright_white());
                } else {
                    print!("{:<20} ", "NULL".bright_black());
                }
            }
            println!();
        }
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Varchar(s) => format!("'{}'", s),
        Value::Null => "NULL".to_string(),
    }
}

fn format_duration(duration: std::time::Duration) -> String {
    let micros = duration.as_micros();
    if micros < 1000 {
        format!("{}µs", micros)
    } else if micros < 1_000_000 {
        format!("{:.2}ms", micros as f64 / 1000.0)
    } else {
        format!("{:.2}s", micros as f64 / 1_000_000.0)
    }
}

fn format_expression(expr: &BoundExpression) -> String {
    match expr {
        BoundExpression::Or(left, right) => format!(
            "{} OR {}",
            format_expression(left),
            format_expression(right)
        ),
        BoundExpression::And(left, right) => format!(
            "{} AND {}",
            format_expression(left),
            format_expression(right)
        ),
        BoundExpression::Not(inner) => format!("NOT ({})", format_expression(inner)),
        BoundExpression::ColumnRef { name, .. } => name.clone(),
        BoundExpression::Literal { value, .. } => match value {
            LiteralValue::Integer(v) => v.to_string(),
            LiteralValue::Float(v) => v.to_string(),
            LiteralValue::String(v) => format!("'{}'", v),
            LiteralValue::Boolean(v) => v.to_string(),
            LiteralValue::Null => "NULL".to_string(),
        },
        BoundExpression::Equal(l, r) => {
            format!("{} = {}", format_expression(l), format_expression(r))
        }
        BoundExpression::NotEqual(l, r) => {
            format!("{} != {}", format_expression(l), format_expression(r))
        }
        BoundExpression::GreaterThan(l, r) => {
            format!("{} > {}", format_expression(l), format_expression(r))
        }
        BoundExpression::GreaterThanOrEqual(l, r) => {
            format!("{} >= {}", format_expression(l), format_expression(r))
        }
        BoundExpression::LessThan(l, r) => {
            format!("{} < {}", format_expression(l), format_expression(r))
        }
        BoundExpression::LessThanOrEqual(l, r) => {
            format!("{} <= {}", format_expression(l), format_expression(r))
        }
    }
}

fn print_plan(operator: &LogicalOperator, indent: usize) {
    let indent_str = if indent > 0 {
        " ".repeat(indent) + "└── "
    } else {
        "".to_string()
    };

    match operator {
        LogicalOperator::Projection(proj) => {
            let columns: Vec<String> = proj.expressions.iter().map(format_expression).collect();
            println!(
                "{}LogicalProjection (Output: {})",
                indent_str,
                columns.join(", ")
            );
            print_plan(&proj.child, indent + 2);
        }
        LogicalOperator::Filter(filter) => {
            println!(
                "{}LogicalFilter (Condition: {})",
                indent_str,
                format_expression(&filter.expression)
            );
            print_plan(&filter.child, indent + 5); // increase indent to align under "LogicalFilter"
        }
        LogicalOperator::Get(get) => {
            let columns: Vec<String> = get.columns.iter().map(|c| c.name.clone()).collect();
            println!(
                "{}LogicalGet (File: {}, Schema: [{}])",
                indent_str,
                get.file_path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy(),
                columns.join(", ")
            );
        }
        LogicalOperator::Limit(limit) => {
            let limit_str = limit
                .limit
                .map(|l| format!("LIMIT {}", l))
                .unwrap_or_default();
            let offset_str = limit
                .offset
                .map(|o| format!("OFFSET {}", o))
                .unwrap_or_default();
            let clause = format!("{} {}", limit_str, offset_str).trim().to_string();
            println!("{}LogicalLimit ({})", indent_str, clause);
            print_plan(&limit.child, indent + 2);
        }
        LogicalOperator::Aggregate(agg) => {
            let agg_names: Vec<String> = agg
                .aggregates
                .iter()
                .map(|a| match a {
                    BoundAggregateExpression::CountStar => "COUNT(*)".to_string(),
                    BoundAggregateExpression::Count { column } => format!("COUNT({})", column.name),
                })
                .collect();
            println!(
                "{}LogicalAggregate (Aggregates: [{}])",
                indent_str,
                agg_names.join(", ")
            );
            print_plan(&agg.child, indent + 2);
        }
    }
}
