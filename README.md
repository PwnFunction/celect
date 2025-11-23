# Celect

A pretty fast columnar SQL query engine for CSV files.

## Features

- SELECT, WHERE, LIMIT, OFFSET, COUNT queries
- Automatic type inference for CSV data
- Columnar storage with validity bitmaps
- Selection vectors for zero-copy filtering
- Parallel CSV scanning with byte-range splitting
- Query optimization and push-based execution

## Quick Start

Create a sample CSV file:

```bash
cat > users.csv << EOF
id,name,age,salary,active
1,Alice,30,75000,true
2,Bob,25,65000,false
3,Charlie,35,85000,true
4,Diana,28,70000,true
EOF
```

Start the REPL and run queries:

```bash
$ cargo run --bin celect

Celect SQL Engine v0.0.2
Press Ctrl+D to exit, .help for help

celect> SELECT name, age FROM 'users.csv' WHERE age >= 30

+---------+-----+
| name    | age |
+===============+
| Alice   | 30  |
|---------+-----|
| Charlie | 35  |
+---------+-----+
(2 rows in 1ms)
```

## Usage

```bash
# interactive REPL
cargo run --bin celect

# see query execution breakdown
cargo run --bin breakdown

# run performance benchmark
cargo run --release --bin benchmark
```

