# Celect

A pretty fast columnar SQL query engine for CSV files.

## Features

- SELECT with WHERE, LIMIT, OFFSET
- COUNT(*) and COUNT(column) aggregation
- Comparison operators (=, <>, <, >, <=, >=)
- Logical operators (AND, OR, NOT)
- Automatic type inference
- Query optimization (projection pushdown, limit pushdown, constant folding, dead code elimination)
- Parallel CSV scanning with validity bitmasks
- Zero-copy filtering with selection vectors

## Example

```bash
echo -e "id,name,age,active\n1,Alice,30,true\n2,Bob,20,false\n3,Charlie,35,true" > data.csv
```

```sql
SELECT name, age FROM 'data.csv' WHERE (age > 25 AND active = true) OR name = 'Bob' LIMIT 10
```

