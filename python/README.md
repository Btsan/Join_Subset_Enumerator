# PostgreSQL Join Enumerator

A Python implementation of PostgreSQL-style join enumeration.
Maintains **exact ordering** to align with PostgreSQL optimizer behavior.

---

## 🎯 Features

✅ **SQLglot-based SQL parsing** - Robust parsing with multiple dialect support
✅ **DP enumeration** - PostgreSQL-style level-by-level enumeration
✅ **Streaming CSV output** - Memory-efficient for large workloads

---

## 📦 Installation

```bash
# Install dependencies
pip install sqlglot tqdm
```

---

## 🚀 Usage

### Command Line

```bash
# Basic usage (one query per line)
python main.py queries.sql

# Semicolon-separated queries
python main.py benchmark.sql --semicolon-separated --output results.csv

# Stop on first error
python main.py queries.sql --stop-on-error --verbose

# Limit enumeration depth
python main.py queries.sql --max-level 10
```

### Full Options

```
usage: main.py [-h] [--output OUTPUT] [--semicolon-separated] 
               [--stop-on-error] [--dialect DIALECT] [--verbose]
               [--max-level MAX_LEVEL]
               input_file

positional arguments:
  input_file            Input SQL file

optional arguments:
  --output, -o          Output CSV file (default: output.csv)
  --semicolon-separated Input file has semicolon-separated queries
  --stop-on-error       Stop processing on first error (default: continue)
  --dialect             SQL dialect (default: postgres)
  --verbose, -v         Verbose output
  --max-level           Maximum enumeration level (default: 20)
```

### Programmatic Usage

```python
from parser import parse_sql
from enumerator import PostgreSQLJoinEnumerator
from sql_generator import SubqueryGenerator
from utils import format_subset

# Parse query
sql = "SELECT * FROM A, B WHERE A.x = B.y"
parsed = parse_sql(sql)

# Enumerate subsets
enumerator = PostgreSQLJoinEnumerator(parsed.join_graph)
result = enumerator.enumerate_subsets(parsed.tables, max_level=20)

# Generate SQL
generator = SubqueryGenerator(parsed.aliases, parsed.classifier, parsed.join_graph)

for plan in result.all_plans:
    subset_str = format_subset(plan.subset)
    sql_query = generator.generate_subquery(plan.subset, plan.left, plan.right)
    print(f"{subset_str}: {sql_query}")
```

---

## 📊 Example Input/Output

### Input (queries.sql)

```sql
-- Simple join query
SELECT * FROM A, B WHERE A.x = B.y AND A.z > 10;

-- Chain query
SELECT * FROM C, D, E WHERE C.a = D.b AND D.b = E.c;
```

### Output (results.csv)

```csv
query_id,subset,query
1,"{A}","SELECT * FROM A WHERE A.z > 10;"
1,"{B}","SELECT * FROM B;"
1,"{A, B}","SELECT * FROM A JOIN B ON A.x = B.y WHERE A.z > 10;"
2,"{C}","SELECT * FROM C;"
2,"{D}","SELECT * FROM D;"
2,"{E}","SELECT * FROM E;"
2,"{C, D}","SELECT * FROM C JOIN D ON C.a = D.b;"
2,"{D, E}","SELECT * FROM D JOIN E ON D.b = E.c;"
2,"{C, E}","SELECT * FROM C JOIN E ON C.a = E.c;"
2,"{C, D, E}","SELECT * FROM C JOIN D ON C.a = D.b JOIN E ON D.b = E.c;"
```

### Console Output

```
Processing queries: 100%|████████████| 2/2 [00:01<00:00,  1.5 queries/s]

Completed: 2/2 queries
Output written to: results.csv
```

---

## 🐛 Troubleshooting

**1. "No queries found in file"**
- Check that queries contain SELECT keyword
- Use --semicolon-separated if queries span multiple lines

**2. "No tables found in query"**
- Ensure query has FROM clause

**3. Enumeration takes too long**
- Use --max-level to limit depth (default: 20)
- Large queries with 15+ tables may take several seconds–minutes

---