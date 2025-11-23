use celect::{Binder, Optimizer, Parser, PhysicalPlanner, PipelineExecutor, Planner};
use colored::*;
use std::time::Instant;

fn main() {
    println!("{}", "Celect Benchmark Suite".bright_cyan().bold());
    println!();
    
    // benchmark 1: complex query with LIMIT (early termination)
    benchmark_with_limit();
    
    // benchmark 2: full table scan with filters (no LIMIT)
    benchmark_full_scan();
    
    // benchmark 3: COUNT(*) aggregate
    benchmark_count_star();
    
    // benchmark 4: COUNT(column) with WHERE clause
    benchmark_count_with_filter();
}

fn benchmark_with_limit() {
    println!("{}", "=== BENCHMARK 1: LIMIT Query (Early Termination) ===".yellow().bold());
    println!("{}","Query:".dimmed());
    println!("  SELECT name, age, score, city, department FROM 'data.csv'");
    println!("  WHERE age > 30 AND age < 70 AND active = true AND score > 50.0");
    println!("  LIMIT 1000");
    println!();
    
    let sql = "SELECT name, age, score, city, department FROM 'data.csv' WHERE age > 30 AND age < 70 AND active = true AND score > 50.0 LIMIT 1000";
    let start = Instant::now();
    
    let mut parser = Parser::new();
    let query = parser.parse(sql).expect("parse failed");
    let binder = Binder::new();
    let bound_query = binder.bind(query).expect("binding failed");
    let planner = Planner::new();
    let plan = planner.plan(bound_query);
    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(plan);
    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);
    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();
    
    let duration = start.elapsed();
    let rows: usize = results.iter().map(|chunk| chunk.selected_count()).sum();
    
    println!("{} {} rows in {}", "Result:".green().bold(), rows, format!("{:.2}ms", duration.as_secs_f64() * 1000.0).cyan());
    println!();
}

fn benchmark_full_scan() {
    println!("{}", "=== BENCHMARK 2: Full Scan with Filters ===".yellow().bold());
    println!("{}", "Query:".dimmed());
    println!("  SELECT name, age, score, city, department FROM 'data.csv'");
    println!("  WHERE age > 30 AND age < 70 AND active = true AND score > 50.0");
    println!("        AND NOT (active = false) AND NOT NOT (age > 25)");
    println!("  {}", "(includes dead filters for optimizer testing)".dimmed());
    println!();
    
    let sql = "SELECT name, age, score, city, department FROM 'data.csv' WHERE age > 30 AND age < 70 AND active = true AND score > 50.0 AND NOT (active = false) AND NOT NOT (age > 25)";
    let start = Instant::now();
    
    let mut parser = Parser::new();
    let query = parser.parse(sql).expect("parse failed");
    let binder = Binder::new();
    let bound_query = binder.bind(query).expect("binding failed");
    let planner = Planner::new();
    let plan = planner.plan(bound_query);
    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(plan);
    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);
    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();
    
    let duration = start.elapsed();
    let rows: usize = results.iter().map(|chunk| chunk.selected_count()).sum();

    // calculate throughput (rows/sec)
    let total_rows = 1_000_000; // data.csv has 1M rows
    let throughput = (total_rows as f64 / duration.as_secs_f64()) as u64;
    
    println!("{} {} rows in {}", "Result:".green().bold(), rows, format!("{:.2}ms", duration.as_secs_f64() * 1000.0).cyan());
    println!("{} {}", "Throughput:".green().bold(), format!("{} rows/sec", throughput).cyan());
    println!();
}

fn benchmark_count_star() {
    println!("{}", "=== BENCHMARK 3: COUNT(*) Aggregate ===".yellow().bold());
    println!("{}", "Query:".dimmed());
    println!("  SELECT COUNT(*) FROM 'data.csv'");
    println!();
    
    let sql = "SELECT COUNT(*) FROM 'data.csv'";
    let start = Instant::now();
    
    let mut parser = Parser::new();
    let query = parser.parse(sql).expect("parse failed");
    let binder = Binder::new();
    let bound_query = binder.bind(query).expect("binding failed");
    let planner = Planner::new();
    let plan = planner.plan(bound_query);
    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(plan);
    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);
    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();
    
    let duration = start.elapsed();
    
    let count = if let Some(chunk) = results.first() {
        if let Some(celect::Value::Integer(n)) = chunk.get_value(0, 0) {
            n
        } else {
            0
        }
    } else {
        0
    };
    
    // calculate throughput (rows processed per second)
    let total_rows = 10_000_000;
    let throughput = (total_rows as f64 / duration.as_secs_f64()) as u64;
    
    println!("{} count = {} in {}", "Result:".green().bold(), count, format!("{:.2}ms", duration.as_secs_f64() * 1000.0).cyan());
    println!("{} {}", "Throughput:".green().bold(), format!("{} rows/sec", throughput).cyan());
    println!();
}

fn benchmark_count_with_filter() {
    println!("{}", "=== BENCHMARK 4: COUNT(column) with Filter ===".yellow().bold());
    println!("{}", "Query:".dimmed());
    println!("  SELECT COUNT(age) FROM 'data.csv'");
    println!("  WHERE age > 30 AND active = true");
    println!();
    
    let sql = "SELECT COUNT(age) FROM 'data.csv' WHERE age > 30 AND active = true";
    let start = Instant::now();
    
    let mut parser = Parser::new();
    let query = parser.parse(sql).expect("parse failed");
    let binder = Binder::new();
    let bound_query = binder.bind(query).expect("binding failed");
    let planner = Planner::new();
    let plan = planner.plan(bound_query);
    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(plan);
    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);
    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();
    
    let duration = start.elapsed();
    
    let count = if let Some(chunk) = results.first() {
        if let Some(celect::Value::Integer(n)) = chunk.get_value(0, 0) {
            n
        } else {
            0
        }
    } else {
        0
    };
    
    // calculate throughput (rows processed per second)
    let total_rows = 10_000_000;
    let throughput = (total_rows as f64 / duration.as_secs_f64()) as u64;
    
    println!("{} count = {} in {}", "Result:".green().bold(), count, format!("{:.2}ms", duration.as_secs_f64() * 1000.0).cyan());
    println!("{} {}", "Throughput:".green().bold(), format!("{} rows/sec", throughput).cyan());
    println!();
}
