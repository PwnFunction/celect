# Celect

A pretty fast columnar SQL query engine for CSV files.

## Features

- SELECT, WHERE, LIMIT, OFFSET, COUNT queries
- Automatic type inference for CSV data
- Columnar storage with validity bitmaps
- Selection vectors for zero-copy filtering
- Parallel CSV scanning with byte-range splitting
- Query optimization and push-based execution

## Example

```bash
echo "id,name,age,active" > data.csv
echo "1,Alice,30,true" >> data.csv
echo "2,Bob,20,false" >> data.csv
echo "3,Charlie,35,true" >> data.csv
```

```sql
SELECT name, age 
FROM 'data.csv' 
WHERE (age > 25 AND active = true) OR name = 'Bob' 
LIMIT 10
```

## Usage

```bash
# see query execution breakdown
cargo run --bin breakdown

# run performance benchmark
cargo run --release --bin benchmark
```

