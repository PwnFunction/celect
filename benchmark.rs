use celect::{Binder, Optimizer, Parser, PhysicalPlanner, PipelineExecutor, Planner};
use std::time::Instant;

fn main() {
    println!("========================================");
    println!("Celect Benchmark");
    println!("========================================");
    println!();
    
    // benchmark 1: complex query with LIMIT (early termination)
    benchmark_with_limit();
    println!();
    
    // benchmark 2: full table scan with filters (no LIMIT)
    benchmark_full_scan();
    println!();
    
    // benchmark 3: COUNT(*) aggregate
    benchmark_count_star();
    println!();
    
    // benchmark 4: COUNT(column) with WHERE clause
    benchmark_count_with_filter();
}

fn benchmark_with_limit() {
    let sql = "SELECT name, age, score, city, department FROM 'data.csv' WHERE age > 30 AND age < 70 AND active = true AND score > 50.0 LIMIT 1000";
    
    println!("Benchmark 1: LIMIT query (early termination test)");
    println!(
        "Query: SELECT ... WHERE age > 30 AND age < 70 AND active = true AND score > 50.0 LIMIT 1000"
    );
    println!();
    
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
    let ms = duration.as_millis();
    let rows: usize = results.iter().map(|chunk| chunk.selected_count()).sum();
    
    println!("Completed: {}ms ({} rows)", ms, rows);
    println!("========================================");
}

fn benchmark_full_scan() {
    let sql = "SELECT name, age, score, city, department FROM 'data.csv' WHERE age > 30 AND age < 70 AND active = true AND score > 50.0 AND NOT (active = false) AND NOT NOT (age > 25)";
    
    println!("Benchmark 2: full scan query (no LIMIT)");
    println!("Query: SELECT name, age, score, city, department");
    println!("        WHERE age > 30 AND age < 70 AND active = true AND score > 50.0");
    println!("              AND NOT (active = false) AND NOT NOT (age > 25)");
    println!("        (includes dead filters for optimizer testing)");
    println!();
    
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
    let ms = duration.as_millis();
    let rows: usize = results.iter().map(|chunk| chunk.selected_count()).sum();

    // calculate throughput (rows/sec)
    let total_rows = 1_000_000; // data.csv has 1M rows
    let throughput = (total_rows as f64 / (ms as f64 / 1000.0)) as u64;
    
    println!("Completed: {}ms ({} rows)", ms, rows);
    println!("Throughput: {} rows/sec", throughput);
    println!("========================================");
}

fn benchmark_count_star() {
    let sql = "SELECT COUNT(*) FROM 'data.csv'";
    
    println!("Benchmark 3: COUNT(*) aggregate");
    println!("Query: SELECT COUNT(*) FROM 'data.csv'");
    println!();
    
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
    let ms = duration.as_millis();
    
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
    let throughput = (total_rows as f64 / (ms as f64 / 1000.0)) as u64;
    
    println!("Completed: {}ms (count: {})", ms, count);
    println!("Throughput: {} rows/sec", throughput);
    println!("========================================");
}

fn benchmark_count_with_filter() {
    let sql = "SELECT COUNT(age) FROM 'data.csv' WHERE age > 30 AND active = true";
    
    println!("Benchmark 4: COUNT(column) with filter");
    println!("Query: SELECT COUNT(age) FROM 'data.csv'");
    println!("       WHERE age > 30 AND active = true");
    println!();
    
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
    let ms = duration.as_millis();
    
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
    let throughput = (total_rows as f64 / (ms as f64 / 1000.0)) as u64;
    
    println!("Completed: {}ms (count: {})", ms, count);
    println!("Throughput: {} rows/sec", throughput);
    println!("========================================");
}
