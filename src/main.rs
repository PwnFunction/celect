use celect::config::VERSION;
use celect::{Binder, Optimizer, Parser, PhysicalPlanner, PipelineExecutor, Planner, Value};
use colored::*;
use comfy_table::{Table, Cell, ContentArrangement, presets::ASCII_FULL};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

fn main() {
    println!("{} {}", "Celect SQL Engine".bright_cyan().bold(), format!("v{}", VERSION).dimmed());
    println!("Press Ctrl+D to exit, {} for help\n", ".help".green());
    
    // setup Ctrl+C handler
    let interrupted = Arc::new(AtomicBool::new(false));
    let interrupted_clone = interrupted.clone();
    
    ctrlc::set_handler(move || {
        interrupted_clone.store(true, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    let mut rl = match DefaultEditor::new() {
        Ok(editor) => editor,
        Err(e) => {
            eprintln!("Failed to initialize editor: {}", e);
            return;
        }
    };
    
    let mut ctrl_c_count = 0;
    
    loop {
        // reset interrupt flag before each command
        interrupted.store(false, Ordering::SeqCst);
        
        let readline = rl.readline("celect> ");

        match readline {
            Ok(line) => {
                let sql = line.trim();

                // reset Ctrl+C counter on successful input
                ctrl_c_count = 0;

                // skip empty lines
                if sql.is_empty() {
                    continue;
                }

                // add to history
                let _ = rl.add_history_entry(sql);

                // handle meta commands
                if sql.starts_with('.') {
                    handle_meta_command(sql);
                    continue;
                }

                // execute query
                if execute_query(sql, &interrupted) {
                    // query completed normally
                    println!();
                } else {
                    // query was interrupted
                    eprintln!("{}", "Query interrupted".yellow());
                    println!();
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl+C at prompt - increment counter
                ctrl_c_count += 1;
                
                if ctrl_c_count >= 2 {
                    eprintln!("{}", "To exit, press Ctrl+D".dimmed());
                }
                continue;
            }
            Err(ReadlineError::Eof) => {
                // Ctrl+D - exit
                println!("exit");
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }
}

fn execute_query(sql: &str, interrupted: &Arc<AtomicBool>) -> bool {
    let start_time = Instant::now();
    
    // check for interrupt
    if interrupted.load(Ordering::SeqCst) {
        return false;
    }

    // step 1: parse
    let mut parser = Parser::new();
    let query = match parser.parse(sql) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e.message);
            if e.offset > 0 {
                eprintln!("  {} offset {}", "at".dimmed(), e.offset);
            }
            eprintln!("  {} SELECT ... FROM '...' [WHERE ...] [LIMIT ...]", "hint:".dimmed());
            return true;
        }
    };

    // check for interrupt
    if interrupted.load(Ordering::SeqCst) {
        return false;
            }
            
    // step 2: bind
    let binder = Binder::new();
    let bound_query = match binder.bind(query) {
        Ok(bq) => bq,
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e.message);
            return true;
        }
    };

    // check for interrupt
    if interrupted.load(Ordering::SeqCst) {
        return false;
    }

    // extract column names for display
    let column_names: Vec<String> = bound_query
        .select_columns
        .iter()
        .map(|col| col.name.clone())
        .collect();

    // step 3: plan
    let planner = Planner::new();
    let logical_plan = planner.plan(bound_query);
    
    // step 4: optimize
    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(logical_plan);

    // step 5: physical plan
    let physical_planner = PhysicalPlanner::new();
    let (operators, schemas) = physical_planner.plan(optimized_plan);

    // check for interrupt before execution
    if interrupted.load(Ordering::SeqCst) {
        return false;
    }

    // step 6: execute
    let mut executor = PipelineExecutor::new(operators, schemas);
    let results = executor.execute();
    
    // check for interrupt after execution
    if interrupted.load(Ordering::SeqCst) {
        return false;
    }

    let duration = start_time.elapsed();
    let time_str = if duration.as_secs() > 0 {
        format!("{:.2}s", duration.as_secs_f64())
    } else {
        format!("{}ms", duration.as_millis())
    };

    // display results
    if results.is_empty() {
        println!("\n{}", format!("(0 rows in {})", time_str).dimmed());
        return true;
    }

    let total_rows: usize = results.iter().map(|chunk| chunk.count).sum();

    // create table
    println!();  // blank line before table
    if let Some(first_chunk) = results.first() {
        let mut table = Table::new();
        table
            .load_preset(ASCII_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic);

        // add header with actual column names
        let headers: Vec<Cell> = if column_names.is_empty() {
            // fallback to col0, col1, ... if no column names available
            (0..first_chunk.columns.len())
                .map(|i| Cell::new(format!("col{}", i)).fg(comfy_table::Color::Cyan))
                .collect()
        } else {
            column_names
                .iter()
                .map(|name| Cell::new(name).fg(comfy_table::Color::Cyan))
                .collect()
        };
        table.set_header(headers);

        // add rows
        for chunk in &results {
            for row_idx in 0..chunk.count {
                // check for interrupt while printing
                if interrupted.load(Ordering::SeqCst) {
                    return false;
                }
                
                let row: Vec<Cell> = chunk
                    .columns
                    .iter()
                    .map(|col| match col.get(row_idx) {
                        Some(value) => Cell::new(format_value(&value)),
                        None => Cell::new("NULL").fg(comfy_table::Color::DarkGrey),
                    })
                    .collect();
                table.add_row(row);
            }
        }
        
        println!("{}", table);
    }
    
    println!("{}", format!("({} rows in {})", total_rows, time_str).dimmed());
    true
}

fn handle_meta_command(cmd: &str) {
    match cmd.trim() {
        ".help" => {
            println!("\n{}", "Meta Commands:".bright_cyan().bold());
            println!("  {} - Show this help message", ".help".green());
            println!("  {} - Exit the REPL", ".exit".green());
            
            println!("\n{}", "SQL Syntax:".bright_cyan().bold());
            println!("  {}", "SELECT column1, column2, ... FROM 'file.csv' [WHERE condition] [LIMIT n] [OFFSET n]".dimmed());
            
            println!("\n{}", "Operators:".bright_cyan().bold());
            println!("  {} =, <>, <, >, <=, >=", "Comparison:".dimmed());
            println!("  {} AND, OR, NOT", "Logical:".dimmed());
            println!("  {} COUNT(*), COUNT(column)", "Aggregates:".dimmed());
            
            println!("\n{}", "Examples:".bright_cyan().bold());
            println!("  {}", "-- Select all columns from a file".dimmed());
            println!("  {}", "SELECT * FROM 'data.csv' LIMIT 10".yellow());
            println!();
            println!("  {}", "-- Filter with conditions".dimmed());
            println!("  {}", "SELECT name, age FROM 'users.csv' WHERE age > 25 AND active = true".yellow());
            println!();
            println!("  {}", "-- Count rows matching a condition".dimmed());
            println!("  {}", "SELECT COUNT(*) FROM 'sales.csv' WHERE region = 'West'".yellow());
            println!();
        }
        ".exit" | ".quit" => {
            println!("exit");
            std::process::exit(0);
        }
        _ => {
            eprintln!("{} {}", "Unknown command:".red().bold(), cmd);
            eprintln!("  {} Type .help for available commands", "hint:".dimmed());
        }
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Varchar(s) => s.clone(),
        Value::Null => "NULL".dimmed().to_string(),
    }
}

